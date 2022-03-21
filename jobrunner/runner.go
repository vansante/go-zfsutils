package jobrunner

import (
	"context"
	"time"

	eventemitter "github.com/vansante/go-event-emitter"
	"github.com/vansante/go-zfs"
)

const (
	dateTimeFormat = time.RFC3339
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

func (r *Runner) sendSnapshots() error {
	return nil
}

func (r *Runner) markPrunableSnapshots() error {
	return nil
}

func (r *Runner) pruneSnapshots() error {
	return nil
}

func (r *Runner) pruneFilesystems() error {
	return nil
}
