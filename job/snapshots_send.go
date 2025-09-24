package job

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	zfs "github.com/vansante/go-zfsutils"
	zfshttp "github.com/vansante/go-zfsutils/http"
)

const stopSendingBeforeDeleteDuration = 24 * time.Hour

var ErrNoCommonSnapshots = errors.New("local and remote datasets have no common snapshot")

func (r *Runner) sendSnapshots(routineID int) error {
	sendToProp := r.config.Properties.snapshotSendTo()

	datasets, err := zfs.ListWithProperty(r.ctx, sendToProp, zfs.ListWithPropertyOptions{
		ParentDataset:   r.config.ParentDataset,
		DatasetType:     r.config.DatasetType,
		PropertySources: []zfs.PropertySource{zfs.PropertySourceLocal},
	})
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("error finding snapshottable datasets: %w", err)
	}

	for dataset := range datasets {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}

		err := r.sendDatasetSnapshotsByName(routineID, dataset)
		switch {
		case isContextError(err):
			return err
		case err != nil:
			// Errors are already logged, we do want to continue sending other dataset snapshots
			continue
		}
	}
	return nil
}

func (r *Runner) sendDatasetSnapshotsByName(routineID int, dataset string) error {
	sendToProp := r.config.Properties.snapshotSendTo()
	sendingProp := r.config.Properties.snapshotSending()
	deleteProp := r.config.Properties.deleteAt()

	ds, err := zfs.GetDataset(r.ctx, dataset, sendToProp, sendingProp, deleteProp)
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		return nil // Dataset was removed meanwhile, continue with the next one
	case err != nil:
		return fmt.Errorf("error retrieving sendable dataset %s: %w", dataset, err)
	}

	if ds.Type == zfs.DatasetSnapshot {
		// We dont send individual snapshots
		return nil
	}

	server := ds.ExtraProps[sendToProp]
	if !propertyIsSet(server) {
		r.logger.Debug("zfs.job.Runner.sendDatasetSnapshotsByName: No server specified",
			"routineID", routineID,
			"dataset", dataset,
		)
		return nil // Dont know where to send this one ¯\_(ツ)_/¯
	}

	deleteAtStopSend := time.Now().Add(stopSendingBeforeDeleteDuration)
	if propertyIsSet(ds.ExtraProps[deleteProp]) && propertyIsBefore(ds.ExtraProps[deleteProp], deleteAtStopSend) {
		r.logger.Debug("zfs.job.Runner.sendDatasetSnapshotsByName: Dataset will be deleted, skipping",
			"routineID", routineID,
			"dataset", dataset,
		)
		return nil
	}

	err = r.sendDatasetSnapshots(ds)
	switch {
	case isContextError(err):
		r.logger.Info("zfs.job.Runner.sendDatasetSnapshotsByName: Send snapshot job interrupted",
			"error", err,
			"routineID", routineID,
			"dataset", dataset,
		)
		return err
	case err != nil:
		r.logger.Error("zfs.job.Runner.sendDatasetSnapshotsByName: Error sending snapshot",
			"error", err,
			"routineID", routineID,
			"dataset", dataset,
		)
		return err
	}
	return nil
}

func (r *Runner) sendDatasetSnapshots(ds *zfs.Dataset) error {
	locked, unlock := r.lockDataset(ds.Name)
	if !locked {
		return nil // Some other goroutine is doing something with this dataset already, continue to next.
	}
	defer func() {
		// Unlock this dataset again
		unlock()
	}()

	createdProp := r.config.Properties.snapshotCreatedAt()
	sendToProp := r.config.Properties.snapshotSendTo()
	sendingProp := r.config.Properties.snapshotSending()
	sentProp := r.config.Properties.snapshotSentAt()
	ignoreProp := r.config.Properties.snapshotIgnoreSend()

	localSnaps, err := zfs.ListSnapshots(r.ctx, zfs.ListOptions{
		ParentDataset:   ds.Name,
		ExtraProperties: []string{createdProp, ignoreProp},
	})
	if err != nil {
		return fmt.Errorf("error listing local %s snapshots: %w", ds.Name, err)
	}

	if len(localSnaps) == 0 {
		// Nothing to do
		return nil
	}

	server := ds.ExtraProps[sendToProp]
	client := r.getServerClient(server)
	remoteDataset := datasetName(ds.Name, true)

	// If we have a sending property, its worth checking whether we can resume a transfer
	if propertyIsSet(ds.ExtraProps[sendingProp]) {
		resumable, err := r.resumeSendSnapshot(client, ds, remoteDataset, ds.ExtraProps[sendingProp])
		if err != nil {
			// TODO:FIXME We should probably force a full re-send after throwing away the partial data on the remote server here
			return err
		}
		if resumable {
			// Clear remote cache, because we have resumed snapshots, its no longer correct
			r.clearRemoteDatasetCache(client.Server(), remoteDataset)
			return nil
		}
	}

	remoteSnaps, err := r.remoteDatasetSnapshots(client, remoteDataset)
	if err != nil {
		return err
	}

	// Filter out snapshots with the ignore property set
	localSnaps = filterSnapshotsWithProp(localSnaps, ignoreProp)

	toSend, err := r.reconcileSnapshots(localSnaps, remoteSnaps, server)
	if err != nil {
		return fmt.Errorf("error reconciling %s snapshots: %w", ds.Name, err)
	}

	for _, send := range toSend {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}

		err := r.sendSnapshot(client, send)
		if err != nil {
			return err
		}

		err = r.setSendSnapshotProperties(client, send.Snapshot.Name)
		if err != nil {
			r.logger.Error("zfs.job.Runner.resumeSendSnapshot: Error setting snapshot properties",
				"error", err, "snapshot", send.Snapshot.Name)
		}

		err = send.Snapshot.SetProperty(r.ctx, sentProp, time.Now().Format(dateTimeFormat))
		switch {
		case errors.Is(err, zfs.ErrDatasetNotFound):
			r.logger.Warn("zfs.job.Runner.sendDatasetSnapshots: Dataset not found, did not set sent property",
				"snapshot", send.Snapshot.Name, "property", sentProp,
			)
			continue
		case err != nil:
			return fmt.Errorf("error setting %s property on %s after send: %w", sentProp, send.Snapshot.Name, err)
		}

		// Clear remote cache, because we are sending snapshots, its no longer correct
		r.clearRemoteDatasetCache(client.Server(), remoteDataset)
	}
	return nil
}

func (r *Runner) resumeSendSnapshot(client *zfshttp.Client, ds *zfs.Dataset, remoteDataset, sendingSnapName string) (bool, error) {
	ctx, cancel := context.WithTimeout(r.ctx, requestTimeout)
	resumeToken, curBytes, err := client.ResumableSendToken(ctx, remoteDataset)
	cancel()
	switch {
	case isContextError(err):
		return false, nil // context expired, no problem
	case errors.Is(err, zfs.ErrDatasetNotFound):
		// Nothing to do.
	case err != nil:
		return false, fmt.Errorf("error checking resumable sends: %w", err)
	}
	if resumeToken == "" {
		return false, nil
	}

	// Due to the way resumption works, the dataset does not contain the @<snapname> part, so we manually make the name here:
	fullSnapName := fmt.Sprintf("%s@%s", ds.Name, sendingSnapName)

	r.logger.Debug("zfs.job.Runner.resumeSendSnapshot: Resuming sending snapshot",
		"dataset", ds.Name,
		"server", client.Server(),
		"snapshotName", sendingSnapName,
		"snapshot", fullSnapName,
		"curBytes", curBytes,
	)

	now := time.Now()
	ctx, cancel = context.WithTimeout(r.ctx, r.config.maximumSendTime())
	sending := &zfsSend{
		dataset: fullSnapName,
		server:  client.Server(),
		updated: now,
		started: now,
		cancel:  cancel,
	}

	r.setSendingState(sending)
	defer func() {
		r.clearSendingState(sending)
	}()

	r.EmitEvent(ResumeSendingSnapshotEvent, fullSnapName, client.Server(), curBytes)

	result, err := client.ResumeSend(ctx, datasetName(ds.Name, true), resumeToken, zfshttp.ResumeSendOptions{
		ResumeSendOptions: zfs.ResumeSendOptions{
			BytesPerSecond:   r.config.SendSpeedBytesPerSecond,
			CompressionLevel: r.config.SendCompressionLevel,
		},
		ProgressEvery: r.config.sendProgressInterval(),
		ProgressFn: func(bytes int64) {
			r.EmitEvent(SnapshotSendingProgressEvent, fullSnapName, client.Server(), int64(curBytes)+bytes)
		},
	})
	cancel()
	result.BytesSent += int64(curBytes)
	switch {
	case errors.Is(err, zfshttp.ErrTooManyRequests):
		r.logger.Info("zfs.job.Runner.resumeSendSnapshot: Too many receives, delaying",
			"error", err,
			"snapshot", ds.Name,
			"server", client.Server(),
			"snapshotName", sendingSnapName,
			"snapshot", fullSnapName,
		)
		return true, nil
	case err != nil:
		r.EmitEvent(SendSnapshotErrorEvent, fullSnapName, client.Server(), err)

		return false, fmt.Errorf("error resuming send of %s (sent %d bytes in %s): %w",
			fullSnapName, result.BytesSent, result.TimeTaken, err,
		)
	}

	err = r.setSendSnapshotProperties(client, fullSnapName)
	if err != nil {
		r.logger.Error("zfs.job.Runner.resumeSendSnapshot: Error setting snapshot properties", "error", err, "snapshot", fullSnapName)
	}

	r.logger.Debug("zfs.job.Runner.resumeSendSnapshot: Sent snapshot",
		"snapshot", ds.Name,
		"server", client.Server(),
		"snapshotName", sendingSnapName,
		"snapshot", fullSnapName,
		"bytesSent", result.BytesSent,
		"timeTaken", result.TimeTaken.String(),
	)

	r.EmitEvent(SentSnapshotEvent, fullSnapName, client.Server(), result.BytesSent, result.TimeTaken)
	return true, nil
}

func (r *Runner) sendSnapshot(client *zfshttp.Client, send zfshttp.SnapshotSendOptions) error {
	r.logger.Debug("zfs.job.Runner.sendDatasetSnapshots: Sending snapshot",
		"snapshot", send.Snapshot.Name,
		"server", client.Server(),
		"sendSnapshotName", send.SnapshotName,
	)

	now := time.Now()
	ctx, cancel := context.WithTimeout(r.ctx, r.config.maximumSendTime())
	sending := &zfsSend{
		dataset: send.Snapshot.Name,
		server:  client.Server(),
		updated: now,
		started: now,
		cancel:  cancel,
	}

	r.setSendingState(sending)
	defer func() {
		r.clearSendingState(sending)
	}()

	r.EmitEvent(StartSendingSnapshotEvent, send.Snapshot.Name, client.Server())

	result, err := client.Send(ctx, send)
	cancel()
	switch {
	case errors.Is(err, zfs.ErrDatasetExists):
		r.logger.Warn("zfs.job.Runner.sendDatasetSnapshots: Dataset exists",
			"error", err,
			"snapshot", send.Snapshot.Name,
			"server", client.Server(),
			"sendSnapshotName", send.SnapshotName,
		)
		r.clearRemoteDatasetCache(client.Server(), datasetName(send.Snapshot.Name, true))
		return nil
	case errors.Is(err, zfshttp.ErrTooManyRequests):
		r.logger.Info("zfs.job.Runner.sendDatasetSnapshots: Too many receives, delaying",
			"error", err,
			"snapshot", send.Snapshot.Name,
			"server", client.Server(),
			"sendSnapshotName", send.SnapshotName,
		)
		return nil
	case err != nil:
		r.EmitEvent(SendSnapshotErrorEvent, send.Snapshot.Name, client.Server(), err)

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

func (r *Runner) setSendSnapshotProperties(client *zfshttp.Client, snapName string) error {
	snapProps, err := r.getSendSnapshotProperties(snapName)
	if err != nil {
		return fmt.Errorf("error getting properties for snapshot %s: %w", snapName, err)
	}

	if len(snapProps) == 0 {
		return nil // Nothing to do!
	}

	setProps := zfshttp.SetProperties{
		Set: snapProps,
	}

	err = client.SetSnapshotProperties(r.ctx, datasetName(snapName, true), snapshotName(snapName), setProps)
	if err != nil {
		return fmt.Errorf("error setting snapshot properties for snapshot %s: %w", snapName, err)
	}
	return nil
}

func (r *Runner) reconcileSnapshots(local, remote []zfs.Dataset, server string) ([]zfshttp.SnapshotSendOptions, error) {
	toSend := make([]zfshttp.SnapshotSendOptions, 0, 8)
	var prevRemoteSnap *zfs.Dataset
	for i := range local {
		snap := &local[i]
		remoteExists := snapshotsContain(remote, datasetName(snap.Name, true), snapshotName(snap.Name))
		if remoteExists {
			prevRemoteSnap = snap
			continue // No more to do
		}

		if len(remote) > 0 && prevRemoteSnap == nil {
			// If remote has snapshots, but we haven't found the common snapshot yet, continue
			continue
		}

		dsProps, err := r.getSendDatasetProperties(stripDatasetSnapshot(snap.Name))
		if err != nil {
			return nil, fmt.Errorf("error getting properties for dataset %s: %w", stripDatasetSnapshot(snap.Name), err)
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
			Resumable:            r.config.SendResumable,
			ReceiveForceRollback: r.config.SendReceiveForceRollback,
			Properties:           dsProps,
			ProgressEvery:        r.config.sendProgressInterval(),
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

func (r *Runner) setSendingState(sending *zfsSend) {
	r.sendLock.Lock()
	defer r.sendLock.Unlock()

	r.sends = append(r.sends, sending)
}

func (r *Runner) updateSendingState(datasetName string, updater func(*zfsSend)) {
	r.sendLock.RLock()
	defer r.sendLock.RUnlock()

	for i := range r.sends {
		send := r.sends[i]
		if send.dataset != datasetName {
			continue
		}

		updater(send)
		return
	}
}

func (r *Runner) clearSendingState(sending *zfsSend) {
	r.sendLock.Lock()
	defer r.sendLock.Unlock()

	r.sends = slices.DeleteFunc(r.sends, func(s *zfsSend) bool {
		return sending == s
	})
}

func (r *Runner) getSendDatasetProperties(datasetName string) (map[string]string, error) {
	props := r.config.sendSetProperties()
	ds, err := zfs.GetDataset(r.ctx, datasetName, r.config.SendCopyProperties...)
	if err != nil {
		return nil, err
	}

	for _, prop := range r.config.SendCopyProperties {
		if !propertyIsSet(ds.ExtraProps[prop]) {
			continue
		}
		props[prop] = ds.ExtraProps[prop]
	}
	return props, nil
}

func (r *Runner) getSendSnapshotProperties(datasetName string) (map[string]string, error) {
	props := r.config.sendSetSnapshotProperties()
	ds, err := zfs.GetDataset(r.ctx, datasetName, r.config.SendCopySnapshotProperties...)
	if err != nil {
		return nil, err
	}

	for _, prop := range r.config.SendCopySnapshotProperties {
		if !propertyIsSet(ds.ExtraProps[prop]) {
			continue
		}
		props[prop] = ds.ExtraProps[prop]
	}
	return props, nil
}
