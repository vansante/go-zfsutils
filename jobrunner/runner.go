package jobrunner

import (
	"context"
	"time"

	eventemitter "github.com/vansante/go-event-emitter"

	"github.com/vansante/go-zfs"
)

const (
	dateTimeFormat = time.RFC3339

	requestTimeout = time.Second * 20
)

type Runner struct {
	*eventemitter.Emitter

	config Config
	logger zfs.Logger
	ctx    context.Context
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

func (r *Runner) pruneFilesystems() error {
	return nil
}
