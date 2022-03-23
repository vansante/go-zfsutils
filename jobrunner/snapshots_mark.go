package jobrunner

import (
	"fmt"
	"github.com/vansante/go-zfs"
	"time"
)

func (r *Runner) markPrunableSnapshots() error {
	err := r.markPrunableExcessSnapshots()
	if err != nil {
		return err
	}
	return r.markPrunableSnapshotsByAge()
}

func (r *Runner) markPrunableExcessSnapshots() error {
	countProp := r.config.Properties.SnapshotRetentionCount
	datasets, err := zfs.ListWithProperty(r.config.DatasetType, r.config.ParentDataset, countProp)
	if err != nil {
		return fmt.Errorf("error finding retention count datasets: %w", err)
	}

	for dataset := range datasets {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}

		ds, err := zfs.GetDataset(dataset, []string{countProp})
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
		if err != nil {
			return fmt.Errorf("error marking excess snapshots for %s: %w", dataset, err)
		}
	}

	return nil
}

func (r *Runner) markExcessDatasetSnapshots(ds *zfs.Dataset, maxCount int64) error {
	if r.ctx.Err() != nil {
		return r.ctx.Err()
	}

	createdProp := r.config.Properties.SnapshotCreatedAt
	deleteProp := r.config.Properties.DeleteAt

	snaps, err := ds.Snapshots([]string{createdProp, deleteProp})
	if err != nil {
		return fmt.Errorf("error retrieving snapshots for %s: %w", ds.Name, err)
	}

	// Snapshots are always retrieved with the newest last, so reverse the list:
	reverse(snaps)
	currentFound := int64(0)
	now := time.Now()
	for _, snap := range snaps {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}

		if snap.ExtraProps[createdProp] == zfs.PropertyUnset || snap.ExtraProps[deleteProp] != zfs.PropertyUnset {
			continue // Ignore
		}

		currentFound++
		if currentFound <= maxCount {
			continue // Not at the max yet
		}

		err = snap.SetProperty(deleteProp, now.Format(dateTimeFormat))
		if err != nil {
			return fmt.Errorf("error setting %s property for %s: %w", deleteProp, snap.Name, err)
		}

		r.EmitEvent(MarkSnapshotDeletionEvent, snap.Name, datasetName(snap.Name, true), snapshotName(snap.Name))
	}

	return nil
}

func (r *Runner) markPrunableSnapshotsByAge() error {
	retentionProp := r.config.Properties.SnapshotMaxRetentionMinutes
	datasets, err := zfs.ListWithProperty(r.config.DatasetType, r.config.ParentDataset, retentionProp)
	if err != nil {
		return fmt.Errorf("error finding retention time datasets: %w", err)
	}

	for dataset := range datasets {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}

		ds, err := zfs.GetDataset(dataset, []string{retentionProp})
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
	if r.ctx.Err() != nil {
		return r.ctx.Err()
	}

	createdProp := r.config.Properties.SnapshotCreatedAt
	deleteProp := r.config.Properties.DeleteAt

	snaps, err := ds.Snapshots([]string{createdProp, deleteProp})
	if err != nil {
		return fmt.Errorf("error retrieving snapshots for %s: %w", ds.Name, err)
	}

	now := time.Now()
	for _, snap := range snaps {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}

		if snap.ExtraProps[createdProp] == zfs.PropertyUnset || snap.ExtraProps[deleteProp] != zfs.PropertyUnset {
			continue // Ignore
		}

		createdAt, err := parseDatasetTimeProperty(snap, createdProp)
		if err != nil {
			return fmt.Errorf("error parsing %s property for %s: %w", createdProp, snap.Name, err)
		}

		if createdAt.Add(duration).After(now) {
			continue // Retention period has not passed yet.
		}

		err = snap.SetProperty(deleteProp, now.Format(dateTimeFormat))
		if err != nil {
			return fmt.Errorf("error setting %s property for %s: %w", deleteProp, snap.Name, err)
		}

		r.EmitEvent(MarkSnapshotDeletionEvent, snap.Name, datasetName(snap.Name, true), snapshotName(snap.Name))
	}

	return nil
}
