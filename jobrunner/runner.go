package jobrunner

import (
	"context"

	eventemitter "github.com/vansante/go-event-emitter"

	"github.com/vansante/go-zfs"
)

type Runner struct {
	eventemitter.Emitter

	config Config
	logger zfs.Logger
	ctx    context.Context
}

func (r *Runner) Run() error {
	return nil
}

func (r *Runner) findSnapshotDatasets() ([]string, error) {
	datasets, err := zfs.ListDatasetWithProperty(r.config.ParentDataset, r.config.SnapshotIntervalProperty)
}
