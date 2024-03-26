package job

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	zfs "github.com/vansante/go-zfsutils"
)

var earliestSnapshot = time.Unix(1, 0)

func (r *Runner) createSnapshots() error {
	intervalProp := r.config.Properties.snapshotIntervalMinutes()
	datasets, err := zfs.ListWithProperty(r.ctx, r.config.DatasetType, r.config.ParentDataset, intervalProp)
	if err != nil {
		return fmt.Errorf("error finding snapshottable datasets: %w", err)
	}

	for dataset := range datasets {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}

		ds, err := zfs.GetDataset(r.ctx, dataset, intervalProp)
		if err != nil {
			return fmt.Errorf("error retrieving snapshottable dataset %s: %w", dataset, err)
		}
		err = r.createDatasetSnapshot(ds)
		switch {
		case isContextError(err):
			r.logger.Info("zfs.job.Runner.createSnapshots: Create snapshot job interrupted", "error", err, "dataset", dataset)
			return nil // Return no error
		case err != nil:
			r.logger.Error("zfs.job.Runner.createSnapshots: Error creating snapshot", "error", err, "dataset", dataset)
			continue // on to the next dataset :-/
		}
	}

	return nil
}

func (r *Runner) snapshotName(tm time.Time) string {
	name := r.config.SnapshotNameTemplate
	name = strings.ReplaceAll(name, "%UNIXTIME%", strconv.FormatInt(tm.Unix(), 10))
	name = strings.ReplaceAll(name, "%RFC3339%", tm.Format(time.RFC3339))
	// TODO: FIXME: Some other constant replacement could be added here?
	return name
}

func (r *Runner) createDatasetSnapshot(ds *zfs.Dataset) error {
	intervalMinsProp := r.config.Properties.snapshotIntervalMinutes()
	intervalMins, err := strconv.ParseInt(ds.ExtraProps[intervalMinsProp], 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing %s property: %w", intervalMinsProp, err)
	}

	createdProp := r.config.Properties.snapshotCreatedAt()
	snapshots, err := ds.Snapshots(r.ctx, zfs.ListOptions{
		ExtraProperties: []string{createdProp},
	})
	if err != nil {
		return fmt.Errorf("error listing existing snapshots: %w", err)
	}
	latestSnap := earliestSnapshot // A long, long time ago...

	for i := range snapshots {
		snap := &snapshots[i]
		if r.config.IgnoreSnapshotsWithoutCreatedProperty && !propertyIsSet(snap.ExtraProps[createdProp]) {
			continue
		}

		created, err := parseDatasetTimeProperty(snap, createdProp)
		if err != nil {
			return fmt.Errorf("error parsing %s on snapshot %s: %w", createdProp, snap.Name, err)
		}
		if created.After(latestSnap) {
			latestSnap = created
		}
	}

	interval := time.Duration(intervalMins) * time.Minute
	if time.Since(latestSnap) < interval {
		return nil // The snapshot interval since last snapshot has not elapsed
	}

	// Log an error whenever more than twice the interval time has passed without a snapshot
	if !latestSnap.Equal(earliestSnapshot) && time.Since(latestSnap) >= 2*interval {
		r.logger.Error("zfs.job.Runner.createDatasetSnapshot: Snapshot creation running behind",
			"dataset", ds.Name,
			"previousSnapshot", latestSnap,
			"interval", interval,
		)
	}

	tm := time.Now()
	name := r.snapshotName(tm)
	snap, err := ds.Snapshot(r.ctx, name, zfs.SnapshotOptions{})
	if err != nil {
		return fmt.Errorf("error creating snapshot %s for %s: %w", name, ds.Name, err)
	}

	// Deliberately using context.Background here, because I always want to set the property if the snapshot was made
	err = snap.SetProperty(context.Background(), createdProp, tm.Format(dateTimeFormat))
	if err != nil {
		return fmt.Errorf("error setting %s on snapshot %s: %w", createdProp, snap.Name, err)
	}

	r.logger.Debug("zfs.job.Runner.createDatasetSnapshot: Snapshot created",
		"snapshot", snap.Name,
		"previousSnapshot", latestSnap,
		"interval", interval,
	)
	r.Emitter.EmitEvent(CreatedSnapshotEvent, ds.Name, name, tm)
	return nil
}
