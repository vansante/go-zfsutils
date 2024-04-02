package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	zfs "github.com/vansante/go-zfsutils"
	zfshttp "github.com/vansante/go-zfsutils/http"
)

var (
	ErrNoCommonSnapshots = errors.New("local and remote datasets have no common snapshot")
	ErrNoLocalSnapshots  = errors.New("no local snapshots to send")
)

func (r *Runner) sendSnapshots(routineID int) error {
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

		server := ds.ExtraProps[sendToProp]
		if !propertyIsSet(server) {
			r.logger.Debug("zfs.job.Runner.sendSnapshots: No server specified",
				"routineID", routineID,
				"dataset", dataset,
			)
			continue // Dont know where to send this one ¯\_(ツ)_/¯
		}

		err = r.sendDatasetSnapshots(routineID, ds)
		switch {
		case isContextError(err):
			r.logger.Info("zfs.job.Runner.sendSnapshots: Send snapshot job interrupted",
				"error", err,
				"routineID", routineID,
				"dataset", dataset,
			)
			return nil // Return no error
		case err != nil:
			r.logger.Error("zfs.job.Runner.sendSnapshots: Error sending snapshot",
				"error", err,
				"routineID", routineID,
				"dataset", dataset,
			)
			continue // on to the next dataset :-/
		}
	}
	return nil
}

func (r *Runner) sendDatasetSnapshots(routineID int, ds *zfs.Dataset) error {
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

	localSnaps, err := zfs.ListSnapshots(r.ctx, zfs.ListOptions{
		ParentDataset:   ds.Name,
		ExtraProperties: []string{createdProp, sentProp},
	})
	if err != nil {
		return fmt.Errorf("error listing existing snapshots: %w", err)
	}

	snapsUnsent := false
	for _, snap := range localSnaps {
		if r.config.IgnoreSnapshotsWithoutCreatedProperty && !propertyIsSet(snap.ExtraProps[createdProp]) {
			continue
		}
		if !propertyIsSet(snap.ExtraProps[sentProp]) {
			snapsUnsent = true
		}
	}
	if !snapsUnsent {
		return nil // Nothing to do, everything has been sent
	}

	server := ds.ExtraProps[sendToProp]
	client := zfshttp.NewClient(server, r.logger)
	for hdr := range r.config.HTTPHeaders {
		client.SetHeader(hdr, r.config.HTTPHeaders[hdr])
	}
	remoteDataset := datasetName(ds.Name, true)

	hasSent, err := r.resumeSendSnapshot(client, ds, remoteDataset)
	if err != nil {
		// TODO:FIXME We should probably force a full re-send after throwing away the partial data on the remote server here
		return err
	}
	if hasSent {
		return nil
	}

	ctx, cancel := context.WithTimeout(r.ctx, requestTimeout)
	remoteSnaps, err := client.DatasetSnapshots(ctx, remoteDataset, []string{createdProp})
	cancel()
	switch {
	case errors.Is(err, zfshttp.ErrDatasetNotFound):
		// Nothing to do.
	case err != nil:
		return fmt.Errorf("error listing remote snapshots: %w", err)
	}

	localSnaps = filterSnapshotsWithProp(localSnaps, createdProp)

	toSend, err := r.reconcileSnapshots(routineID, localSnaps, remoteSnaps, server)
	if err != nil {
		return fmt.Errorf("error reconciling snapshots: %w", err)
	}

	for _, send := range toSend {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}

		err := r.sendSnapshot(client, send)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) resumeSendSnapshot(client *zfshttp.Client, ds *zfs.Dataset, remoteDataset string) (bool, error) {
	ctx, cancel := context.WithTimeout(r.ctx, requestTimeout)
	resumeToken, err := client.ResumableSendToken(ctx, remoteDataset)
	cancel()
	switch {
	case isContextError(err):
		return false, nil // context expired, no problem
	case errors.Is(err, zfshttp.ErrDatasetNotFound):
		// Nothing to do.
	case err != nil:
		return false, fmt.Errorf("error checking resumable sends: %w", err)
	}
	if resumeToken == "" {
		return false, nil
	}

	r.logger.Debug("zfs.job.Runner.resumeSendSnapshot: Resuming sending snapshot",
		"snapshot", ds.Name,
		"server", client.Server(),
		"sendSnapshotName", remoteDataset,
	)

	// TODO: FIXME: When resuming a send, we only get the dataset name, not including the @<snapshot> part, due to the
	// way resumption works
	r.EmitEvent(ResumeSendingSnapshotEvent, ds.Name, client.Server())

	ctx, cancel = context.WithTimeout(r.ctx, time.Duration(r.config.MaximumSendTimeMinutes)*time.Minute)
	result, err := client.ResumeSend(ctx, datasetName(ds.Name, true), resumeToken, zfshttp.ResumeSendOptions{
		ResumeSendOptions: zfs.ResumeSendOptions{
			BytesPerSecond:   r.config.SendSpeedBytesPerSecond,
			CompressionLevel: r.config.SendCompressionLevel,
		},
		ProgressEvery: r.config.SendProgressEventInterval,
		ProgressFn: func(bytes int64) {
			// TODO: FIXME: When resuming a send, we only get the dataset name, not including the @<snapshot> part, due to the
			// way resumption works
			r.EmitEvent(SnapshotSendingProgressEvent, ds.Name, client.Server(), bytes)
		},
	})
	cancel()
	if err != nil {
		return false, fmt.Errorf("error resuming send of %s (sent %d bytes in %s): %w",
			ds.Name, result.BytesSent, result.TimeTaken, err,
		)
	}

	r.logger.Debug("zfs.job.Runner.resumeSendSnapshot: Sent snapshot",
		"snapshot", ds.Name,
		"server", client.Server(),
		"sendSnapshotName", remoteDataset,
		"bytesSent", result.BytesSent,
		"timeTaken", result.TimeTaken.String(),
	)

	// TODO: FIXME: When resuming a send, we only get the dataset name, not including the @<snapshot> part, due to the
	// way resumption works
	r.EmitEvent(SentSnapshotEvent, ds.Name, client.Server(), result.BytesSent, result.TimeTaken)
	return true, nil
}

func (r *Runner) sendSnapshot(client *zfshttp.Client, send zfshttp.SnapshotSendOptions) error {
	r.logger.Debug("zfs.job.Runner.sendDatasetSnapshots: Sending snapshot",
		"snapshot", send.Snapshot.Name,
		"server", client.Server(),
		"sendSnapshotName", send.SnapshotName,
	)

	r.EmitEvent(StartSendingSnapshotEvent, send.Snapshot.Name, client.Server())

	ctx, cancel := context.WithTimeout(r.ctx, time.Duration(r.config.MaximumSendTimeMinutes)*time.Minute)
	result, err := client.Send(ctx, send)
	cancel()
	if err != nil {
		return fmt.Errorf("error sending %s@%s (sent %d bytes in %s): %w",
			send.DatasetName, send.SnapshotName, result.BytesSent, result.TimeTaken, err,
		)
	}

	r.logger.Debug("zfs.job.Runner.sendDatasetSnapshots: Snapshot sent",
		"snapshot", send.Snapshot.Name,
		"server", client.Server(),
		"sendSnapshotName", send.SnapshotName,
		"bytesSent", result.BytesSent,
		"timeTaken", result.TimeTaken.String(),
	)

	r.EmitEvent(SentSnapshotEvent, send.Snapshot.Name, client.Server(), result.BytesSent, result.TimeTaken)
	return nil
}

func (r *Runner) reconcileSnapshots(routineID int, local, remote []zfs.Dataset, server string) ([]zfshttp.SnapshotSendOptions, error) {
	createdProp := r.config.Properties.snapshotCreatedAt()
	sentProp := r.config.Properties.snapshotSentAt()

	if len(local) == 0 {
		return nil, ErrNoLocalSnapshots
	}

	local, err := orderSnapshotsByCreated(local, createdProp)
	if err != nil {
		return nil, err
	}

	toSend := make([]zfshttp.SnapshotSendOptions, 0, 8)
	var prevRemoteSnap *zfs.Dataset
	for i := range local {
		snap := &local[i]
		remoteExists := snapshotsContain(remote, datasetName(snap.Name, true), snapshotName(snap.Name))
		localSent := propertyIsSet(snap.ExtraProps[sentProp])

		logger := r.logger.With(
			"routineID", routineID,
			"dataset", datasetName(snap.Name, true),
			"snapshot", snapshotName(snap.Name),
		)

		if remoteExists {
			prevRemoteSnap = snap
			if localSent {
				continue // Nothing to do!
			}
			val := time.Now().Format(dateTimeFormat)
			setErr := snap.SetProperty(r.ctx, sentProp, val)
			if setErr != nil {
				logger.Error(
					"zfs.job.Runner.reconcileSnapshots: Error setting sent property after property was missing",
					"error", setErr,
					"property", sentProp,
				)
			} else {
				logger.Info(
					"zfs.job.Runner.reconcileSnapshots: Set sent property after property was missing",
					"value", val,
					"property", sentProp,
				)
			}
			continue // No more to do
		}

		if len(remote) > 0 && prevRemoteSnap == nil {
			// If remote has snapshots, but we haven't found the common snapshot yet, continue
			continue
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
			if propertyIsSet(val) {
				continue
			}
			props[prop] = val
		}

		toSend = append(toSend, zfshttp.SnapshotSendOptions{
			DatasetName:  datasetName(snap.Name, true),
			SnapshotName: snapshotName(snap.Name),
			Snapshot:     snap,
			SendOptions: zfs.SendOptions{
				CompressionLevel:  r.config.SendCompressionLevel,
				BytesPerSecond:    r.config.SendSpeedBytesPerSecond,
				Raw:               r.config.SendRaw,
				IncludeProperties: r.config.SendIncludeProperties,
				IncrementalBase:   prevRemoteSnap,
			},
			Properties:    props,
			ProgressEvery: r.config.SendProgressEventInterval,
			ProgressFn: func(bytes int64) {
				r.EmitEvent(SnapshotSendingProgressEvent, snap.Name, server, bytes)
			},
		})

		// Once we have sent the first snapshot, the next one can be incremental upon that one
		prevRemoteSnap = snap
	}

	if len(remote) > 0 && prevRemoteSnap == nil {
		return toSend, fmt.Errorf("%w: %s", ErrNoCommonSnapshots, datasetName(local[0].Name, true))
	}

	return toSend, nil
}
