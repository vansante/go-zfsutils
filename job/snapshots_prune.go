package job

import (
	"fmt"
	"time"

	"github.com/vansante/go-zfs"
)

func (r *Runner) pruneSnapshots() error {
	deleteProp := r.config.Properties.deleteAt()

	snapshots, err := zfs.ListWithProperty(r.ctx, zfs.DatasetSnapshot, r.config.ParentDataset, deleteProp)
	if err != nil {
		return fmt.Errorf("error finding prunable datasets: %w", err)
	}

	now := time.Now()
	for snapshot := range snapshots {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}

		snap, err := zfs.GetDataset(r.ctx, snapshot, deleteProp)
		if err != nil {
			return fmt.Errorf("error getting snapshot %s: %w", snapshot, err)
		}

		if snap.Type != zfs.DatasetSnapshot {
			return fmt.Errorf("unexpected dataset type %s for %s: %w", snap.Type, snapshot, err)
		}

		deleteAt, err := parseDatasetTimeProperty(snap, deleteProp)
		if err != nil {
			return fmt.Errorf("error parsing %s for %s: %s", deleteProp, snapshot, err)
		}

		if deleteAt.After(now) {
			continue // Not due for removal yet
		}

		// TODO: FIXME: Do we want deferred destroy?
		err = snap.Destroy(r.ctx, zfs.DestroyDefault)
		if err != nil {
			return fmt.Errorf("error destroying %s: %s", snapshot, err)
		}

		r.EmitEvent(DeletedSnapshotEvent, snapshot, datasetName(snapshot, true), snapshotName(snapshot))
	}

	return nil
}
