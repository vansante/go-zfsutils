package jobrunner

import (
	"fmt"
	"time"

	"github.com/vansante/go-zfs"
)

func (r *Runner) pruneFilesystems() error {
	deleteProp := r.config.Properties.deleteAt()

	filesystems, err := zfs.ListWithProperty(r.ctx, zfs.DatasetFilesystem, r.config.ParentDataset, deleteProp)
	if err != nil {
		return fmt.Errorf("error finding prunable filesystems: %w", err)
	}

	now := time.Now()
	for filesystem := range filesystems {
		if r.ctx.Err() != nil {
			return r.ctx.Err()
		}

		fs, err := zfs.GetDataset(r.ctx, filesystem, []string{deleteProp})
		if err != nil {
			return fmt.Errorf("error getting filesystem %s: %w", filesystem, err)
		}

		if fs.Type != zfs.DatasetFilesystem {
			return fmt.Errorf("unexpected dataset type %s for %s: %w", fs.Type, filesystem, err)
		}

		deleteAt, err := parseDatasetTimeProperty(fs, deleteProp)
		if err != nil {
			return fmt.Errorf("error parsing %s for %s: %s", deleteProp, filesystem, err)
		}

		if deleteAt.After(now) {
			continue // Not due for removal yet
		}

		children, err := fs.Children(r.ctx, 0, nil)
		if err != nil {
			return fmt.Errorf("error listing %s children: %w", filesystem, err)
		}
		if len(children) > 0 {
			// TODO: FIXME: Maybe add a recursive delete option in the future?
			continue // We are not deleting recursively.
		}

		// TODO: FIXME: Do we want deferred destroy?
		err = fs.Destroy(r.ctx, zfs.DestroyDefault)
		if err != nil {
			return fmt.Errorf("error destroying %s: %s", filesystem, err)
		}

		r.EmitEvent(DeletedFilesystemEvent, filesystem, datasetName(filesystem, true))
	}

	return nil
}
