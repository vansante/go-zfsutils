package job

import (
	"context"
	"sync"
	"time"

	eventemitter "github.com/vansante/go-event-emitter"

	"github.com/vansante/go-zfs"
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
func NewRunner(ctx context.Context, conf Config, logger zfs.Logger) *Runner {
	return &Runner{
		config:          conf,
		datasetSendLock: make(map[string]struct{}),
		logger:          logger,
		ctx:             ctx,
	}
}

// Runner runs Create, Send and Prune snapshot jobs. Additionally, it can prune filesystems.
type Runner struct {
	eventemitter.Emitter

	config          Config
	mapLock         sync.Mutex
	datasetSendLock map[string]struct{}

	logger zfs.Logger
	ctx    context.Context
}

func (r *Runner) sendLock(dataset string) (succeeded bool, unlock func()) {
	r.mapLock.Lock()
	_, ok := r.datasetSendLock[dataset]
	if ok {
		// Entry found, already locked.
		r.mapLock.Unlock()
		return false, func() {} // Noop unlock
	}
	// Set the lock!
	r.datasetSendLock[dataset] = struct{}{}
	r.mapLock.Unlock()

	return true, func() {
		// Simple unlock function removes entry from map:
		r.mapLock.Lock()
		delete(r.datasetSendLock, dataset)
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

	r.logger.Infof("zfs.job.Runner.runCreateSnapshots: Running every %v", dur)
	defer r.logger.Info("zfs.job.Runner.runCreateSnapshots: Stopped")

	for {
		select {
		case <-ticker.C:
			err := r.createSnapshots()
			switch {
			case isContextError(err):
				r.logger.WithError(err).Info("zfs.job.Runner.runCreateSnapshots: Job interrupted")
			case err != nil:
				r.logger.WithError(err).Error("zfs.job.Runner.runCreateSnapshots: Error making snapshots")
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

	r.logger.WithField("routineID", id).Infof("zfs.job.Runner.runSendSnapshotRoutine: Running every %v", dur)
	defer r.logger.WithField("routineID", id).Info("zfs.job.Runner.runSendSnapshotRoutine: Stopped")

	for {
		select {
		case <-ticker.C:
			err := r.sendSnapshots()
			switch {
			case isContextError(err):
				r.logger.WithError(err).Info("zfs.job.Runner.runSendSnapshots: Job interrupted")
			case err != nil:
				r.logger.WithError(err).Error("zfs.job.Runner.runSendSnapshots: Error sending snapshots")
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

	r.logger.Infof("zfs.job.Runner.runMarkSnapshots: Running every %v", dur)
	defer r.logger.Info("zfs.job.Runner.runMarkSnapshots: Stopped")

	for {
		select {
		case <-ticker.C:
			err := r.markPrunableSnapshots()
			switch {
			case isContextError(err):
				r.logger.WithError(err).Info("zfs.job.Runner.runMarkSnapshots: Job interrupted")
			case err != nil:
				r.logger.WithError(err).Error("zfs.job.Runner.runMarkSnapshots: Error marking snapshots")
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

	r.logger.Infof("zfs.job.Runner.runPruneSnapshots: Running every %v", dur)
	defer r.logger.Info("zfs.job.Runner.runPruneSnapshots: Stopped")

	for {
		select {
		case <-ticker.C:
			err := r.pruneSnapshots()
			switch {
			case isContextError(err):
				r.logger.WithError(err).Info("zfs.job.Runner.runPruneSnapshots: Job interrupted")
			case err != nil:
				r.logger.WithError(err).Error("zfs.job.Runner.runPruneSnapshots: Error pruning snapshots")
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

	r.logger.Infof("zfs.job.Runner.runPruneFilesystems: Running every %v", dur)
	defer r.logger.Info("zfs.job.Runner.runPruneFilesystems: Stopped")

	for {
		select {
		case <-ticker.C:
			err := r.pruneFilesystems()
			switch {
			case isContextError(err):
				r.logger.WithError(err).Info("zfs.job.Runner.runPruneFilesystems: Job interrupted")
			case err != nil:
				r.logger.WithError(err).Error("zfs.job.Runner.runPruneFilesystems: Error pruning filesystems")
			}
		case <-r.ctx.Done():
			return
		}
	}
}
