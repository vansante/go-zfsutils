package jobrunner

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/vansante/go-zfs"
	zfshttp "github.com/vansante/go-zfs/http"
)

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

	client := zfshttp.NewClient(ds.ExtraProps[r.config.Properties.SnapshotSendTo], r.config.AuthorisationToken)
	remoteDataset := datasetName(ds.Name, true)

	ctx, cancel := context.WithTimeout(r.ctx, requestTimeout)
	defer cancel()

	resumeToken, err := client.ResumableSendToken(ctx, remoteDataset)
	if err != nil {
		return fmt.Errorf("error checking resumable sends: %w", err)
	}
	if resumeToken != "" {
		ctx, cancel := context.WithTimeout(r.ctx, time.Duration(r.config.MaximumSendTimeMinutes)*time.Minute)
		err = client.ResumeSend(ctx, datasetName(ds.Name, true), resumeToken)
		if err != nil {
			cancel()
			return fmt.Errorf("error resuming send: %w", err)
		}
		cancel()
	}

	ctx, cancel = context.WithTimeout(r.ctx, requestTimeout)
	defer cancel()
	remoteSnaps, err := client.DatasetSnapshots(ctx, remoteDataset, []string{createdProp})
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
		ctx, cancel := context.WithTimeout(r.ctx, time.Duration(r.config.MaximumSendTimeMinutes)*time.Minute)
		err = client.Send(ctx, send)
		if err != nil {
			cancel()
			return fmt.Errorf("error sending %s/%s: %w", send.DatasetName, send.SnapshotName, err)
		}
		cancel()
	}

	return nil
}

func (r *Runner) reconcileSnapshots(local, remote []*zfs.Dataset) ([]zfshttp.SnapshotSend, error) {
	var err error
	local, err = orderSnapshotsByCreated(local, r.config.Properties.SnapshotCreatedAt)
	if err != nil {
		return nil, err
	}
	remote, err = orderSnapshotsByCreated(remote, r.config.Properties.SnapshotCreatedAt)
	if err != nil {
		return nil, err
	}

	toSend := make([]zfshttp.SnapshotSend, 0, 8)
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

		toSend = append(toSend, zfshttp.SnapshotSend{
			DatasetName:  datasetName(snap.Name, true),
			SnapshotName: snapshotName(snap.Name),
			Snapshot:     snap,
			SendOptions: zfs.SendOptions{
				Raw:             true,
				Props:           true,
				IncrementalBase: prevRemoteSnap,
			},
		})

		// Once we have sent the first snapshot, the next one can be incremental upon that one
		prevRemoteSnap = snap
	}
	return toSend, nil
}
