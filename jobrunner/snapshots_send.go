package jobrunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/vansante/go-zfs"
	zfshttp "github.com/vansante/go-zfs/http"
)

var (
	errDatasetNotFound = errors.New("dataset not found")
)

type snapshotSend struct {
	// If set, send incrementally from this snapshot
	baseSnapshot *zfs.Dataset
	// The snapshot to send
	snapshot *zfs.Dataset
}

func (s *snapshotSend) sendToServer(ctx context.Context, server, authToken string) error {
	if s.baseSnapshot == nil {
		return s.sendFull(ctx, server, authToken)
	}
	return s.sendWithBase(ctx, server, authToken)
}

func (s *snapshotSend) sendWithBase(ctx context.Context, server, authToken string) error {

}

func (s *snapshotSend) sendFull(ctx context.Context, server, authToken string) error {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	pipeRdr, pipeWrtr := io.Pipe()

	req, err := requestToServer(ctx, server, authToken, http.MethodPut, fmt.Sprintf(
		"filesystems/%s/snapshots/%s",
		datasetName(s.snapshot.Name, true),
		snapshotName(s.snapshot.Name),
	), pipeRdr)
	if err != nil {
		return fmt.Errorf("error creating send snapshot request: %w", err)
	}

	err := s.snapshot.SendSnapshot(pipeWrtr, true)
	if err != nil {
		return fmt.Errorf("error sending snapshot %s: %w", s.snapshot.Name, err)
	}

}

func (r *Runner) sendSnapshots() error {
	datasets, err := zfs.ListWithProperty(r.config.DatasetType, r.config.ParentDataset, r.config.Properties.SnapshotSendTo)
	if err != nil {
		return fmt.Errorf("error finding snapshottable datasets: %w", err)
	}

	for dataset := range datasets {
		ds, err := zfs.GetDataset(dataset, []string{
			r.config.Properties.SnapshotSendTo,
		})
		if err != nil {
			return fmt.Errorf("error retrieving snapshottable dataset %s: %w", dataset, err)
		}
		err = r.sendSnapshotsForDataset(ds)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Runner) listRemoteSnapshotsForDataset(ds *zfs.Dataset) ([]*zfs.Dataset, error) {
	targetServer := ds.ExtraProps[r.config.Properties.SnapshotSendTo]

	ctx, cancel := context.WithTimeout(r.ctx, requestTimeout)
	defer cancel()

	req, err := requestToServer(ctx, targetServer, r.config.AuthorisationToken,
		http.MethodGet, fmt.Sprintf("filesystems/%s/snapshots?%s=%s",
			datasetName(ds.Name, true),
			zfshttp.GETParamExtraProperties, strings.Join([]string{
				r.config.Properties.SnapshotCreatedAt,
				zfs.PropertyReceiveResumeToken,
			}, ","),
		), nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error requesting remote snapshots: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// Continue
	case http.StatusNotFound:
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected status %d requesting remote snapshots: %w", resp.StatusCode, err)
	}

	var datasets []*zfs.Dataset
	err = json.NewDecoder(resp.Body).Decode(&datasets)
	return datasets, err
}

func (r *Runner) reconcileSnapshots(local, remote []*zfs.Dataset) ([]snapshotSend, error) {
	var err error
	local, err = orderSnapshotsByCreated(local, r.config.Properties.SnapshotCreatedAt)
	if err != nil {
		return nil, err
	}
	remote, err = orderSnapshotsByCreated(remote, r.config.Properties.SnapshotCreatedAt)
	if err != nil {
		return nil, err
	}

	toSend := make([]snapshotSend, 0, 8)
	var prevRemoteSnap *zfs.Dataset
	for _, snap := range local {
		remoteExists := snapshotsContain(remote, datasetName(snap.Name, true), snapshotName(snap.Name))
		localSent := snap.ExtraProps[r.config.Properties.SnapshotSentAt] != zfs.PropertyUnset

		logger := r.logger.WithFields(logrus.Fields{
			"dataset":  datasetName(snap.Name, true),
			"snapshot": snapshotName(snap.Name),
		})

		if remoteExists {
			prevRemoteSnap = snap
			if localSent {
				continue // Nothing to do!
			}
			val := time.Now().Format(dateTimeFormat)
			setErr := snap.SetProperty(r.config.Properties.SnapshotSentAt, val)
			if setErr != nil {
				logger.WithError(setErr).Errorf("jobrunner.reconcileSnapshots: Error setting %s after property was missing",
					r.config.Properties.SnapshotSentAt,
				)
			} else {
				logger.WithError(setErr).WithField("value", val).Warnf("jobrunner.reconcileSnapshots: Set %s after property was missing",
					r.config.Properties.SnapshotSentAt,
				)
			}
			continue // No more to do
		}

		toSend = append(toSend, snapshotSend{
			baseSnapshot: prevRemoteSnap,
			snapshot:     snap,
		})
	}
	return toSend, nil
}

func (r *Runner) sendSnapshotsForDataset(ds *zfs.Dataset) error {
	createdProp := r.config.Properties.SnapshotCreatedAt
	sentProp := r.config.Properties.SnapshotSentAt
	localSnaps, err := zfs.ListByType(zfs.DatasetSnapshot, ds.Name, []string{createdProp, sentProp})
	if err != nil {
		return fmt.Errorf("error listing existing snapshots: %w", err)
	}

	snapsUnsent := false
	for _, snap := range localSnaps {
		if r.config.IgnoreSnapshotsWithoutCreatedProperty && snap.ExtraProps[createdProp] == zfs.PropertyUnset {
			continue
		}
		if snap.ExtraProps[sentProp] == zfs.PropertyUnset {
			snapsUnsent = true
		}
	}
	if !snapsUnsent {
		return nil // Nothing to do, everything has been sent
	}

	remoteSnaps, err := r.listRemoteSnapshotsForDataset(ds)
	if err != nil {
		return err
	}

	localSnaps = filterSnapshotsWithProp(localSnaps, createdProp)
	remoteSnaps = filterSnapshotsWithProp(remoteSnaps, createdProp)

	toSend, err := r.reconcileSnapshots(localSnaps, remoteSnaps)
	if err != nil {
		return fmt.Errorf("error reconciling snapshots: %w", err)
	}

	for _, send := range toSend {
		if send.baseSnapshot == nil {

		}
	}

	return nil
}
