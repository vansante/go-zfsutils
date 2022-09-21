package job

import (
	"fmt"
	"time"

	zfs "github.com/vansante/go-zfsutils"
)

func (r *Runner) pruneFilesystems() error {
	deleteProp := r.config.Properties.deleteAt()

	filesystems, err := zfs.ListWithProperty(r.ctx, zfs.DatasetFilesystem, r.config.ParentDataset, deleteProp)
	if err != nil {
		return fmt.Errorf("error finding prunable filesystems: %w", err)
	}

	for filesystem := range filesystems {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}

		err = r.pruneAgedFilesystem(filesystem)
		switch {
		case isContextError(err):
			r.logger.WithFields(map[string]interface{}{
				"dataset": filesystem,
			}).WithError(err).Info("zfs.job.Runner.pruneFilesystems: Prune filesystem job interrupted")
			return nil // Return no error
		case err != nil:
			r.logger.WithFields(map[string]interface{}{
				"dataset": filesystem,
			}).WithError(err).Error("zfs.job.Runner.pruneFilesystems: Error pruning filesystem")
			continue // on to the next dataset :-/
		}
	}

	return nil
}

func (r *Runner) pruneAgedFilesystem(filesystem string) error {
	deleteProp := r.config.Properties.deleteAt()

	fs, err := zfs.GetDataset(r.ctx, filesystem, deleteProp)
	if err != nil {
		return fmt.Errorf("error getting filesystem %s: %w", filesystem, err)
	}

	if fs.Type != zfs.DatasetFilesystem {
		return fmt.Errorf("unexpected dataset type %s for %s: %w", fs.Type, filesystem, err)
	}

	deleteAt, err := parseDatasetTimeProperty(fs, deleteProp)
	if err != nil {
		return fmt.Errorf("error parsing %s for %s: %w", deleteProp, filesystem, err)
	}

	if deleteAt.After(time.Now()) {
		return nil // Not due for removal yet
	}

	children, err := fs.Children(r.ctx, 0)
	if err != nil {
		return fmt.Errorf("error listing %s children: %w", filesystem, err)
	}
	if len(children) > 0 {
		// TODO: FIXME: Maybe add a recursive delete option in the future?
		return nil // We are not deleting recursively.
	}

	// TODO: FIXME: Do we want deferred destroy?
	err = fs.Destroy(r.ctx, zfs.DestroyDefault)
	if err != nil {
		return fmt.Errorf("error destroying %s: %w", filesystem, err)
	}

	r.EmitEvent(DeletedFilesystemEvent, filesystem, datasetName(filesystem, true))

	return nil
}
