package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vansante/go-zfs"
	zfshttp "github.com/vansante/go-zfs/http"
)

func (r *Runner) sendSnapshots() error {
	sendToProp := r.config.Properties.snapshotSendTo()
	datasets, err := zfs.ListWithProperty(r.ctx, r.config.DatasetType, r.config.ParentDataset, sendToProp)
	if err != nil {
		return fmt.Errorf("error finding snapshottable datasets: %w", err)
	}

	for dataset := range datasets {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}

		ds, err := zfs.GetDataset(r.ctx, dataset, sendToProp)
		if err != nil {
			return fmt.Errorf("error retrieving snapshottable dataset %s: %w", dataset, err)
		}

		err = r.sendDatasetSnapshots(ds)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) sendDatasetSnapshots(ds *zfs.Dataset) error {
	locked, unlock := r.sendLock(datasetName(ds.Name, true))
	if !locked {
		return nil // Some other goroutine is sending this dataset already, continue to next.
	}
	defer func() {
		// Unlock the send for this dataset again
		unlock()
	}()

	createdProp := r.config.Properties.snapshotCreatedAt()
	sentProp := r.config.Properties.snapshotSentAt()
	sendToProp := r.config.Properties.snapshotSendTo()
	localSnaps, err := zfs.ListByType(r.ctx, zfs.DatasetSnapshot, ds.Name, createdProp, sentProp)
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

	server := ds.ExtraProps[sendToProp]
	client := zfshttp.NewClient(server, r.config.AuthorisationToken, r.logger)
	remoteDataset := datasetName(ds.Name, true)

	ctx, cancel := context.WithTimeout(r.ctx, requestTimeout)
	defer cancel()

	resumeToken, err := client.ResumableSendToken(ctx, remoteDataset)
	switch {
	case errors.Is(err, zfshttp.ErrDatasetNotFound):
		// Nothing to do.
	case err != nil:
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
	switch {
	case errors.Is(err, zfshttp.ErrDatasetNotFound):
		// Nothing to do.
	case err != nil:
		return fmt.Errorf("error listing remote snapshots: %w", err)
	}

	localSnaps = filterSnapshotsWithProp(localSnaps, createdProp)
	remoteSnaps = filterSnapshotsWithProp(remoteSnaps, createdProp)

	toSend, err := r.reconcileSnapshots(localSnaps, remoteSnaps)
	if err != nil {
		return fmt.Errorf("error reconciling snapshots: %w", err)
	}

	for _, send := range toSend {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}

		r.EmitEvent(SendingSnapshotEvent, send.Snapshot.Name, server, send.DatasetName, send.SnapshotName)

		ctx, cancel := context.WithTimeout(r.ctx, time.Duration(r.config.MaximumSendTimeMinutes)*time.Minute)
		err = client.Send(ctx, send)
		if err != nil {
			cancel()
			return fmt.Errorf("error sending %s/%s: %w", send.DatasetName, send.SnapshotName, err)
		}
		cancel()

		r.EmitEvent(SentSnapshotEvent, send.Snapshot.Name, server, send.DatasetName, send.SnapshotName)
	}

	return nil
}

func (r *Runner) reconcileSnapshots(local, remote []zfs.Dataset) ([]zfshttp.SnapshotSend, error) {
	createdProp := r.config.Properties.snapshotCreatedAt()
	sentProp := r.config.Properties.snapshotSentAt()
	var err error
	local, err = orderSnapshotsByCreated(local, createdProp)
	if err != nil {
		return nil, err
	}
	remote, err = orderSnapshotsByCreated(remote, createdProp)
	if err != nil {
		return nil, err
	}

	toSend := make([]zfshttp.SnapshotSend, 0, 8)
	var prevRemoteSnap *zfs.Dataset
	for i := range local {
		snap := &local[i]
		remoteExists := snapshotsContain(remote, datasetName(snap.Name, true), snapshotName(snap.Name))
		localSent := snap.ExtraProps[sentProp] != zfs.PropertyUnset

		logger := r.logger.WithFields(map[string]interface{}{
			"dataset":  datasetName(snap.Name, true),
			"snapshot": snapshotName(snap.Name),
		})

		if remoteExists {
			prevRemoteSnap = snap
			if localSent {
				continue // Nothing to do!
			}
			val := time.Now().Format(dateTimeFormat)
			setErr := snap.SetProperty(r.ctx, sentProp, val)
			if setErr != nil {
				logger.WithError(setErr).Errorf(
					"zfs.job.Runner.reconcileSnapshots: Error setting %s after property was missing", sentProp,
				)
			} else {
				logger.WithError(setErr).WithField("value", val).Infof(
					"zfs.job.Runner.reconcileSnapshots: Set %s after property was missing", sentProp,
				)
			}
			continue // No more to do
		}

		props := make(map[string]string, len(r.config.SendSetProperties)+len(r.config.SendCopyProperties))
		for k, v := range r.config.SendSetProperties {
			props[k] = v
		}
		for _, prop := range r.config.SendCopyProperties {
			val, err := snap.GetProperty(r.ctx, prop)
			if err != nil {
				return nil, fmt.Errorf("error getting prop %s copy value for %s: %w", prop, snap.Name, err)
			}
			if val == zfs.PropertyUnset {
				continue
			}
			props[prop] = val
		}

		toSend = append(toSend, zfshttp.SnapshotSend{
			DatasetName:  datasetName(snap.Name, true),
			SnapshotName: snapshotName(snap.Name),
			Snapshot:     snap,
			SendOptions: zfs.SendOptions{
				BytesPerSecond:    r.config.SendSpeedBytesPerSecond,
				Raw:               r.config.SendRaw,
				IncludeProperties: r.config.SendIncludeProperties,
				IncrementalBase:   prevRemoteSnap,
			},
			Properties: props,
		})

		// Once we have sent the first snapshot, the next one can be incremental upon that one
		prevRemoteSnap = snap
	}
	return toSend, nil
}
