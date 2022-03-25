package jobrunner

import (
	"context"
	"time"

	zfshttp "github.com/vansante/go-zfs/http"

	eventemitter "github.com/vansante/go-event-emitter"

	"github.com/vansante/go-zfs"
)

const (
	dateTimeFormat = time.RFC3339

	requestTimeout = time.Second * 20

	createSnapshotInterval = time.Minute
)

type Runner struct {
	eventemitter.Emitter

	config    Config
	sendQueue chan zfshttp.SnapshotSend

	logger zfs.Logger
	ctx    context.Context
}

func (r *Runner) runCreateSnapshots() {
	ticker := time.NewTicker(createSnapshotInterval)
	defer ticker.Stop()

	r.logger.Infof("zfs.jobrunner.runCreateSnapshots: Running every %v", createSnapshotInterval)
	defer r.logger.Info("zfs.jobrunner.runCreateSnapshots: Stopped")

	for {
		select {
		case <-ticker.C:

		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Runner) Run() error {
	err := r.createSnapshots()
	if err != nil {
		return err
	}
	err = r.sendSnapshots()
	if err != nil {
		return err
	}
	err = r.markPrunableSnapshots()
	if err != nil {
		return err
	}

	err = r.pruneSnapshots()
	if err != nil {
		return err
	}
	err = r.pruneFilesystems()
	if err != nil {
		return err
	}
	return nil
}
