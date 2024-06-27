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

const deleteAfter = time.Minute * 5

func (r *Runner) markPrunableSnapshots() error {
	err := r.markPrunableExcessSnapshots()
	if err != nil {
		return err
	}
	return r.markPrunableSnapshotsByAge()
}

func (r *Runner) markPrunableExcessSnapshots() error {
	countProp := r.config.Properties.snapshotRetentionCount()

	datasets, err := zfs.ListWithProperty(r.ctx, countProp, zfs.ListWithPropertyOptions{
		ParentDataset:   r.config.ParentDataset,
		DatasetType:     r.config.DatasetType,
		PropertySources: []zfs.PropertySource{zfs.PropertySourceLocal},
	})
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("error finding retention count datasets: %w", err)
	}

	for dataset := range datasets {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}

		ds, err := zfs.GetDataset(r.ctx, dataset, countProp)
		if err != nil {
			return fmt.Errorf("error retrieving count retention dataset %s: %w", dataset, err)
		}

		retentionCount, err := parseDatasetIntProperty(ds, countProp)
		if err != nil {
			return fmt.Errorf("error parsing %s property for %s: %w", countProp, dataset, err)
		}

		if retentionCount <= 0 { // Zero or less is considered to be Off.
			continue
		}

		err = r.markExcessDatasetSnapshots(ds, retentionCount)
		switch {
		case isContextError(err):
			r.logger.Info("zfs.job.Runner.markPrunableExcessSnapshots: Mark snapshot job interrupted", "error", err, "dataset", dataset)
			return nil // Return no error
		case err != nil:
			r.logger.Error("zfs.job.Runner.markPrunableExcessSnapshots: Error marking snapshots", "error", err, "dataset", dataset)
			continue // on to the next dataset :-/
		}
	}

	return nil
}

func (r *Runner) markExcessDatasetSnapshots(ds *zfs.Dataset, maxCount int64) error {
	locked, unlock := r.lockDataset(ds.Name)
	if !locked {
		return nil // Some other goroutine is doing something with this dataset already, continue to next.
	}
	defer func() {
		// Unlock this dataset again
		unlock()
	}()

	createdProp := r.config.Properties.snapshotCreatedAt()
	deleteProp := r.config.Properties.deleteAt()
	serverProp := r.config.Properties.snapshotSendTo()

	snaps, err := ds.Snapshots(r.ctx, zfs.ListOptions{
		ExtraProperties: []string{createdProp, deleteProp},
	})
	if err != nil {
		return fmt.Errorf("error retrieving snapshots for %s: %w", ds.Name, err)
	}

	// Snapshots are always retrieved with the newest last, so reverse the list:
	slices.Reverse(snaps)

	currentFound := int64(0)
	deleteAt := time.Now().Add(deleteAfter)
	for i := range snaps {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}
		snap := &snaps[i]

		if r.config.IgnoreSnapshotsWithoutCreatedProperty && !propertyIsSet(snap.ExtraProps[createdProp]) {
			continue // Ignore
		}
		currentFound++

		if propertyIsSet(snap.ExtraProps[deleteProp]) && propertyIsBefore(snap.ExtraProps[deleteProp], deleteAt) {
			continue // Already being deleted, sooner than we would
		}

		if currentFound <= maxCount {
			continue // Not at the max yet
		}

		err = snap.SetProperty(r.ctx, deleteProp, deleteAt.Format(dateTimeFormat))
		if err != nil {
			return fmt.Errorf("error setting %s property for %s: %w", deleteProp, snap.Name, err)
		}

		err = r.markRemoteDatasetSnapshot(snap, snap.ExtraProps[serverProp], deleteProp, deleteAt)
		if err != nil {
			r.logger.Error("zfs.job.Runner.markAgingDatasetSnapshots: Remote mark failed",
				"error", err,
				"snapshot", snap.Name,
				"deleteAt", deleteAt.Format(dateTimeFormat),
				"maxCount", maxCount,
				"server", snap.ExtraProps[serverProp],
			)
		}

		r.logger.Debug("zfs.job.Runner.markExcessDatasetSnapshots: Snapshot marked",
			"snapshot", snap.Name,
			"snapshotIndex", currentFound,
			"deleteAt", deleteAt.Format(dateTimeFormat),
			"maxCount", maxCount,
			"remoteMarked", r.config.EnableSnapshotMarkRemote,
			"server", snap.ExtraProps[serverProp],
		)

		r.EmitEvent(MarkSnapshotDeletionEvent, snap.Name, datasetName(snap.Name, true), snapshotName(snap.Name))
	}

	return nil
}

func (r *Runner) markPrunableSnapshotsByAge() error {
	retentionProp := r.config.Properties.snapshotRetentionMinutes()

	datasets, err := zfs.ListWithProperty(r.ctx, retentionProp, zfs.ListWithPropertyOptions{
		ParentDataset:   r.config.ParentDataset,
		DatasetType:     r.config.DatasetType,
		PropertySources: []zfs.PropertySource{zfs.PropertySourceLocal},
	})
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("error finding retention time datasets: %w", err)
	}

	for dataset := range datasets {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}

		ds, err := zfs.GetDataset(r.ctx, dataset, retentionProp)
		if err != nil {
			return fmt.Errorf("error retrieving time retention dataset %s: %w", dataset, err)
		}

		retentionMinutes, err := parseDatasetIntProperty(ds, retentionProp)
		if err != nil {
			return fmt.Errorf("error parsing %s property for %s: %w", retentionProp, dataset, err)
		}

		if retentionMinutes <= 0 { // Zero or less is considered to be Off.
			continue
		}

		err = r.markAgingDatasetSnapshots(ds, time.Duration(retentionMinutes)*time.Minute)
		if err != nil {
			return fmt.Errorf("error marking counted snapshots for %s: %w", dataset, err)
		}
	}

	return nil
}

func (r *Runner) markAgingDatasetSnapshots(ds *zfs.Dataset, duration time.Duration) error {
	locked, unlock := r.lockDataset(ds.Name)
	if !locked {
		return nil // Some other goroutine is doing something with this dataset already, continue to next.
	}
	defer func() {
		// Unlock this dataset again
		unlock()
	}()

	createdProp := r.config.Properties.snapshotCreatedAt()
	deleteProp := r.config.Properties.deleteAt()
	serverProp := r.config.Properties.snapshotSendTo()

	snaps, err := ds.Snapshots(r.ctx, zfs.ListOptions{
		ExtraProperties: []string{createdProp, deleteProp, serverProp},
	})
	if err != nil {
		return fmt.Errorf("error retrieving snapshots for %s: %w", ds.Name, err)
	}

	now := time.Now()
	deleteAt := now.Add(deleteAfter)
	for i := range snaps {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}
		snap := &snaps[i]

		if !propertyIsSet(snap.ExtraProps[createdProp]) {
			continue // Cannot determine age
		}
		if propertyIsSet(snap.ExtraProps[deleteProp]) && propertyIsBefore(snap.ExtraProps[deleteProp], deleteAt) {
			continue // Already being deleted, sooner than we would
		}

		createdAt, err := parseDatasetTimeProperty(snap, createdProp)
		if err != nil {
			return fmt.Errorf("error parsing %s property for %s: %w", createdProp, snap.Name, err)
		}

		if createdAt.Add(duration).After(now) {
			continue // Retention period has not passed yet.
		}

		err = snap.SetProperty(r.ctx, deleteProp, deleteAt.Format(dateTimeFormat))
		if err != nil {
			return fmt.Errorf("error setting %s property for %s: %w", deleteProp, snap.Name, err)
		}

		err = r.markRemoteDatasetSnapshot(snap, snap.ExtraProps[serverProp], deleteProp, deleteAt)
		if err != nil {
			r.logger.Warn("zfs.job.Runner.markAgingDatasetSnapshots: Remote mark failed",
				"error", err,
				"snapshot", snap.Name,
				"createdAt", createdAt,
				"deleteAt", deleteAt.Format(dateTimeFormat),
				"deleteAfter", duration,
				"server", snap.ExtraProps[serverProp],
			)
		}

		r.logger.Debug("zfs.job.Runner.markAgingDatasetSnapshots: Snapshot marked",
			"snapshot", snap.Name,
			"createdAt", createdAt,
			"deleteAt", deleteAt.Format(dateTimeFormat),
			"deleteAfter", duration,
			"remoteMarked", r.config.EnableSnapshotMarkRemote,
			"server", snap.ExtraProps[serverProp],
		)

		r.EmitEvent(MarkSnapshotDeletionEvent, snap.Name, datasetName(snap.Name, true), snapshotName(snap.Name))
	}
	return nil
}

func (r *Runner) markRemoteDatasetSnapshot(localSnap *zfs.Dataset, server, deleteProp string, deleteAt time.Time) error {
	if !r.config.EnableSnapshotMarkRemote || !propertyIsSet(server) {
		return nil
	}

	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Minute)
	defer cancel()

	client := r.getServerClient(server)
	return client.SetSnapshotProperties(ctx, datasetName(localSnap.Name, true), snapshotName(localSnap.Name), zfshttp.SetProperties{
		Set: map[string]string{
			deleteProp: deleteAt.Format(dateTimeFormat),
		},
	})
}
