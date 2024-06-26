package job

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	zfs "github.com/vansante/go-zfsutils"
)

func (r *Runner) pruneFilesystems() error {
	deleteProp := r.config.Properties.deleteAt()

	filesystems, err := zfs.ListWithProperty(r.ctx, deleteProp, zfs.ListWithPropertyOptions{
		ParentDataset:   r.config.ParentDataset,
		DatasetType:     zfs.DatasetFilesystem,
		PropertySources: []zfs.PropertySource{zfs.PropertySourceLocal},
	})
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("error finding prunable old filesystems: %w", err)
	}

	for filesystem := range filesystems {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}

		err = r.pruneAgedFilesystem(filesystem)
		switch {
		case isContextError(err):
			r.logger.Info("zfs.job.Runner.pruneFilesystems: Prune filesystem job interrupted", "error", err, "dataset", filesystem)
			return nil // Return no error
		case err != nil:
			r.logger.Error("zfs.job.Runner.pruneFilesystems: Error pruning aged filesystems", "error", err, "dataset", filesystem)
			continue // on to the next dataset :-/
		}
	}

	deleteWithoutSnaps := r.config.Properties.deleteWithoutSnapshots()
	filesystems, err = zfs.ListWithProperty(r.ctx, deleteWithoutSnaps, zfs.ListWithPropertyOptions{
		ParentDataset:   r.config.ParentDataset,
		DatasetType:     r.config.DatasetType,
		PropertySources: []zfs.PropertySource{zfs.PropertySourceLocal},
	})
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("error finding prunable filesystems without snapshots: %w", err)
	}

	for filesystem := range filesystems {
		if r.ctx.Err() != nil {
			return nil // context expired, no problem
		}

		err = r.pruneFilesystemWithoutSnapshots(filesystem)
		switch {
		case isContextError(err):
			r.logger.Info("zfs.job.Runner.pruneFilesystems: Prune filesystem job interrupted", "error", err, "dataset", filesystem)
			return nil // Return no error
		case err != nil:
			r.logger.Error("zfs.job.Runner.pruneFilesystems: Error pruning filesystems without snapshots", "error", err, "dataset", filesystem)
			continue // on to the next dataset :-/
		}
	}

	return nil
}

func (r *Runner) pruneAgedFilesystem(filesystem string) error {
	locked, unlock := r.lockDataset(filesystem)
	if !locked {
		return nil // Some other goroutine is doing something with this dataset already, continue to next.
	}
	defer func() {
		// Unlock this dataset again
		unlock()
	}()

	deleteProp := r.config.Properties.deleteAt()

	fs, err := zfs.GetDataset(r.ctx, filesystem, deleteProp)
	if err != nil {
		return fmt.Errorf("error getting filesystem %s: %w", filesystem, err)
	}

	if fs.Type != zfs.DatasetFilesystem {
		return fmt.Errorf("unexpected dataset type %s for %s: %w", fs.Type, filesystem, err)
	}

	if !propertyIsSet(fs.ExtraProps[deleteProp]) {
		return nil
	}

	deleteAt, err := parseDatasetTimeProperty(fs, deleteProp)
	if err != nil {
		return fmt.Errorf("error parsing %s for %s: %w", deleteProp, filesystem, err)
	}

	if deleteAt.After(time.Now()) {
		return nil // Not due for removal yet
	}

	children, err := fs.Children(r.ctx, zfs.ListOptions{})
	if err != nil {
		return fmt.Errorf("error listing %s children: %w", filesystem, err)
	}
	if len(children) > 0 {
		// TODO: FIXME: Maybe add a recursive delete option in the future?
		return nil // We are not deleting recursively.
	}

	// TODO: FIXME: Do we want deferred destroy?
	err = fs.Destroy(r.ctx, zfs.DestroyOptions{})
	if err != nil {
		return fmt.Errorf("error destroying %s: %w", filesystem, err)
	}

	r.logger.Debug("zfs.job.Runner.pruneAgedFilesystem: Filesystem pruned",
		"filesystem", fs.Name,
		"deleteAt", deleteAt.Format(dateTimeFormat),
	)

	r.EmitEvent(DeletedFilesystemEvent, filesystem, datasetName(filesystem, true))

	return nil
}

func (r *Runner) pruneFilesystemWithoutSnapshots(filesystem string) error {
	locked, unlock := r.lockDataset(filesystem)
	if !locked {
		return nil // Some other goroutine is doing something with this dataset already, continue to next.
	}
	defer func() {
		// Unlock this dataset again
		unlock()
	}()

	deleteWithoutSnaps := r.config.Properties.deleteWithoutSnapshots()

	fs, err := zfs.GetDataset(r.ctx, filesystem, deleteWithoutSnaps)
	if err != nil {
		return fmt.Errorf("error getting filesystem %s: %w", filesystem, err)
	}

	if fs.Type != zfs.DatasetFilesystem {
		return fmt.Errorf("unexpected dataset type %s for %s: %w", fs.Type, filesystem, err)
	}

	if !propertyIsSet(fs.ExtraProps[deleteWithoutSnaps]) {
		return nil
	}

	shouldDelete, _ := strconv.ParseBool(fs.ExtraProps[deleteWithoutSnaps])
	if !shouldDelete {
		return nil
	}

	children, err := fs.Children(r.ctx, zfs.ListOptions{})
	if err != nil {
		return fmt.Errorf("error listing %s children: %w", filesystem, err)
	}
	if len(children) > 0 {
		return nil
	}

	err = fs.Destroy(r.ctx, zfs.DestroyOptions{})
	if err != nil {
		return fmt.Errorf("error destroying %s: %w", filesystem, err)
	}

	r.logger.Debug("zfs.job.Runner.pruneFilesystemWithoutSnapshots: Filesystem pruned",
		"filesystem", fs.Name,
	)

	r.EmitEvent(DeletedFilesystemEvent, filesystem, datasetName(filesystem, true))

	return nil
}
