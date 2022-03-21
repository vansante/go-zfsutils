package jobrunner

import (
	"fmt"
	"github.com/vansante/go-zfs"
	"strconv"
	"strings"
	"time"
)

func (r *Runner) createSnapshots() error {
	datasets, err := zfs.ListWithProperty(r.config.DatasetType, r.config.ParentDataset, r.config.Properties.SnapshotIntervalMinutes)
	if err != nil {
		return fmt.Errorf("error finding snapshottable datasets: %w", err)
	}

	for dataset := range datasets {
		ds, err := zfs.GetDataset(dataset, []string{
			r.config.Properties.SnapshotIntervalMinutes,
		})
		if err != nil {
			return fmt.Errorf("error retrieving snapshottable dataset %s: %w", dataset, err)
		}
		err = r.createSnapshotsForDataset(ds)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Runner) snapshotName(tm time.Time) string {
	name := r.config.SnapshotNameTemplate
	name = strings.ReplaceAll(name, "%UNIXTIME%", strconv.FormatInt(tm.Unix(), 10))
	// TODO: FIXME: Some other constant replacement could be added here?
	return name
}

func (r *Runner) createSnapshotsForDataset(ds *zfs.Dataset) error {
	intervalMinsProp := r.config.Properties.SnapshotIntervalMinutes
	intervalMins, err := strconv.ParseInt(ds.ExtraProps[intervalMinsProp], 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing %s property: %w", intervalMinsProp, err)
	}

	createdProp := r.config.Properties.SnapshotCreatedAt
	snapshots, err := zfs.ListByType(zfs.DatasetSnapshot, ds.Name, []string{createdProp})
	if err != nil {
		return fmt.Errorf("error listing existing snapshots: %w", err)
	}
	latestSnap := time.Unix(1, 0) // A long, long time ago...

	for _, snap := range snapshots {
		if r.config.IgnoreSnapshotsWithoutCreatedProperty && snap.ExtraProps[createdProp] == zfs.PropertyUnset {
			continue
		}

		created, err := parseDatasetTimeProperty(snap, createdProp)
		if err != nil {
			return fmt.Errorf("error parsing %s on snapshot %s: %w", createdProp, snap.Name, err)
		}
		if created.After(latestSnap) {
			created = latestSnap
		}
	}

	if time.Since(latestSnap) < time.Minute*time.Duration(intervalMins) {
		return nil // The snapshot interval since last snapshot has not elapsed
	}

	tm := time.Now()
	name := r.snapshotName(tm)
	snap, err := ds.Snapshot(name, false)
	if err != nil {
		return fmt.Errorf("error creating snapshot %s for %s: %w", name, ds.Name, err)
	}

	err = snap.SetProperty(createdProp, tm.Format(dateTimeFormat))
	if err != nil {
		return fmt.Errorf("error setting %s on snapshot %s: %w", createdProp, snap.Name, err)
	}

	r.Emitter.EmitEvent(CreatedSnapshotEvent, ds.Name, name, tm)
	return nil
}
