package job

import (
	"context"
	"log/slog"
	"sync"
	"time"

	eventemitter "github.com/vansante/go-event-emitter"
)

const (
	dateTimeFormat = time.RFC3339

	requestTimeout = time.Second * 20

	createSnapshotInterval  = time.Minute
	sendSnapshotInterval    = time.Minute
	markSnapshotInterval    = time.Minute
	pruneSnapshotInterval   = time.Minute
	pruneFilesystemInterval = time.Minute
)

// NewRunner creates a new job runner
func NewRunner(ctx context.Context, conf Config, logger *slog.Logger) *Runner {
	return &Runner{
		config:      conf,
		datasetLock: make(map[string]struct{}),
		logger:      logger,
		ctx:         ctx,
	}
}

// Runner runs Create, Send and Prune snapshot jobs. Additionally, it can prune filesystems.
type Runner struct {
	eventemitter.Emitter

	config      Config
	mapLock     sync.Mutex
	datasetLock map[string]struct{}

	logger *slog.Logger
	ctx    context.Context
}

func (r *Runner) lockDataset(dataset string) (succeeded bool, unlock func()) {
	r.mapLock.Lock()
	_, ok := r.datasetLock[dataset]
	if ok {
		// Entry found, already locked.
		r.mapLock.Unlock()
		return false, func() {} // Noop unlock
	}
	// Set the lock!
	r.datasetLock[dataset] = struct{}{}
	r.mapLock.Unlock()

	return true, func() {
		// Simple unlock function removes entry from map:
		r.mapLock.Lock()
		delete(r.datasetLock, dataset)
		r.mapLock.Unlock()
	}
}

// Run starts the goroutines for the different types of jobs
func (r *Runner) Run() {
	if r.config.EnableSnapshotCreate {
		go r.runCreateSnapshots()
	}

	if r.config.EnableSnapshotSend {
		// Start as many go routines as configured
		for i := 1; i <= r.config.SendRoutines; i++ {
			go r.runSendSnapshotRoutine(i)
		}
	}

	if r.config.EnableSnapshotMark {
		go r.runMarkSnapshots()
	}

	if r.config.EnableSnapshotPrune {
		go r.runPruneSnapshots()
	}

	if r.config.EnableFilesystemPrune {
		go r.runPruneFilesystems()
	}
}

func (r *Runner) runCreateSnapshots() {
	dur := randomizeDuration(createSnapshotInterval)
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	r.logger.Info("zfs.job.Runner.runCreateSnapshots: Running", "interval", dur)
	defer r.logger.Info("zfs.job.Runner.runCreateSnapshots: Stopped")

	for {
		select {
		case <-ticker.C:
			err := r.createSnapshots()
			switch {
			case isContextError(err):
				r.logger.Info("zfs.job.Runner.runCreateSnapshots: Job interrupted", "error", err)
			case err != nil:
				r.logger.Error("zfs.job.Runner.runCreateSnapshots: Error making snapshots", "error", err)
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Runner) runSendSnapshotRoutine(id int) {
	// Add some sleep, so not all send routines start at the same time:
	sleepTime := time.Duration(int(sendSnapshotInterval) / r.config.SendRoutines * (id - 1))
	time.Sleep(sleepTime)

	dur := randomizeDuration(sendSnapshotInterval)
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	r.logger.Info("zfs.job.Runner.runSendSnapshotRoutine: Running", "interval", dur, "routineID", id)
	defer r.logger.Info("zfs.job.Runner.runSendSnapshotRoutine: Stopped", "interval", dur, "routineID", id)

	for {
		select {
		case <-ticker.C:
			err := r.sendSnapshots(id)
			switch {
			case isContextError(err):
				r.logger.Info("zfs.job.Runner.runSendSnapshots: Job interrupted", "error", err)
			case err != nil:
				r.logger.Error("zfs.job.Runner.runSendSnapshots: Error sending snapshots", "error", err)
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Runner) runMarkSnapshots() {
	dur := randomizeDuration(markSnapshotInterval)
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	r.logger.Info("zfs.job.Runner.runMarkSnapshots: Running", "interval", dur)
	defer r.logger.Info("zfs.job.Runner.runMarkSnapshots: Stopped")

	for {
		select {
		case <-ticker.C:
			err := r.markPrunableSnapshots()
			switch {
			case isContextError(err):
				r.logger.Info("zfs.job.Runner.runMarkSnapshots: Job interrupted", "error", err)
			case err != nil:
				r.logger.Error("zfs.job.Runner.runMarkSnapshots: Error marking snapshots", "error", err)
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Runner) runPruneSnapshots() {
	dur := randomizeDuration(pruneSnapshotInterval)
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	r.logger.Info("zfs.job.Runner.runPruneSnapshots: Running", "interval", dur)
	defer r.logger.Info("zfs.job.Runner.runPruneSnapshots: Stopped")

	for {
		select {
		case <-ticker.C:
			err := r.pruneSnapshots()
			switch {
			case isContextError(err):
				r.logger.Info("zfs.job.Runner.runPruneSnapshots: Job interrupted", "error", err)
			case err != nil:
				r.logger.Error("zfs.job.Runner.runPruneSnapshots: Error pruning snapshots", "error", err)
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Runner) runPruneFilesystems() {
	dur := randomizeDuration(pruneFilesystemInterval)
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	r.logger.Info("zfs.job.Runner.runPruneFilesystems: Running", "interval", dur)
	defer r.logger.Info("zfs.job.Runner.runPruneFilesystems: Stopped")

	for {
		select {
		case <-ticker.C:
			err := r.pruneFilesystems()
			switch {
			case isContextError(err):
				r.logger.Info("zfs.job.Runner.runPruneFilesystems: Job interrupted", "error", err)
			case err != nil:
				r.logger.Error("zfs.job.Runner.runPruneFilesystems: Error pruning filesystems", "error", err)
			}
		case <-r.ctx.Done():
			return
		}
	}
}
