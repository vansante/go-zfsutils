package job

import (
	"fmt"
	"time"

	zfs "github.com/vansante/go-zfsutils"
)

func (r *Runner) pruneSnapshots() error {
	deleteProp := r.config.Properties.deleteAt()

	snapshots, err := zfs.ListWithProperty(r.ctx, zfs.DatasetSnapshot, r.config.ParentDataset, deleteProp)
	if err != nil {
		return fmt.Errorf("error finding prunable datasets: %w", err)
	}

	for snapshot := range snapshots {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}

		err = r.pruneAgedSnapshot(snapshot)
		switch {
		case isContextError(err):
			r.logger.Info("zfs.job.Runner.pruneSnapshots: Prune snapshot job interrupted",
				"error", err,
				"dataset", datasetName(snapshot, true),
				"snapshot", snapshotName(snapshot),
				"full", snapshot,
			)
			return nil // Return no error
		case err != nil:
			r.logger.Error("zfs.job.Runner.pruneSnapshots: Error pruning snapshot",
				"error", err,
				"dataset", datasetName(snapshot, true),
				"snapshot", snapshotName(snapshot),
				"full", snapshot,
			)
			continue // on to the next dataset :-/
		}
	}

	return nil
}

func (r *Runner) pruneAgedSnapshot(snapshot string) error {
	deleteProp := r.config.Properties.deleteAt()

	snap, err := zfs.GetDataset(r.ctx, snapshot, deleteProp)
	if err != nil {
		return fmt.Errorf("error getting snapshot %s: %w", snapshot, err)
	}

	if snap.Type != zfs.DatasetSnapshot {
		return fmt.Errorf("unexpected dataset type %s for %s", snap.Type, snap.Name)
	}

	if !propertyIsSet(snap.ExtraProps[deleteProp]) {
		return nil
	}

	deleteAt, err := parseDatasetTimeProperty(snap, deleteProp)
	if err != nil {
		return fmt.Errorf("error parsing %s for %s: %w", deleteProp, snap.Name, err)
	}

	if deleteAt.After(time.Now()) {
		return nil // Not due for removal yet
	}

	// TODO: FIXME: Do we want deferred destroy?
	err = snap.Destroy(r.ctx, zfs.DestroyOptions{})
	if err != nil {
		return fmt.Errorf("error destroying %s: %w", snap.Name, err)
	}

	r.logger.Debug("zfs.job.Runner.pruneAgedSnapshot: Snapshot pruned",
		"snapshot", snap.Name,
		"deleteAt", deleteAt.Format(dateTimeFormat),
	)

	r.EmitEvent(DeletedSnapshotEvent, snap.Name, datasetName(snap.Name, true), snapshotName(snap.Name))

	return nil
}
