package job

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	eventemitter "github.com/vansante/go-event-emitter"
	zfs "github.com/vansante/go-zfsutils"
	zfshttp "github.com/vansante/go-zfsutils/http"
)

const (
	dateTimeFormat = time.RFC3339

	requestTimeout = time.Second * 20

	createSnapshotInterval   = 5 * time.Minute
	sendSnapshotInterval     = 15 * time.Minute // Effectively divided by the amount of send routines configured (default 3)
	pruneRemoteCacheInterval = 5 * time.Minute
	markSnapshotInterval     = 10 * time.Minute
	pruneSnapshotInterval    = 10 * time.Minute
	pruneFilesystemInterval  = 10 * time.Minute
)

// NewRunner creates a new job runner
func NewRunner(ctx context.Context, conf Config, logger *slog.Logger) *Runner {
	r := &Runner{
		Emitter:     eventemitter.NewEmitter(false),
		config:      conf,
		datasetLock: make(map[string]struct{}),
		remoteCache: make(map[string]map[string]*datasetCache),
		sendChan:    make(chan string),
		logger:      logger,
		ctx:         ctx,
	}
	r.attachListeners()
	return r
}

// Runner runs Create, ZFSSending and Prune snapshot jobs. Additionally, it can prune filesystems.
type Runner struct {
	*eventemitter.Emitter

	config Config

	datasetLock map[string]struct{}
	dsLock      sync.Mutex

	remoteCache map[string]map[string]*datasetCache // Snapshots indexed by server, then dataset name
	cacheLock   sync.RWMutex

	sendChan chan string
	sends    []*zfsSend
	sendLock sync.RWMutex

	logger *slog.Logger
	ctx    context.Context
}

func (r *Runner) getServerClient(server string) *zfshttp.Client {
	client := zfshttp.NewClient(server, r.logger)
	for hdr := range r.config.HTTPHeaders {
		client.SetHeader(hdr, r.config.HTTPHeaders[hdr])
	}
	return client
}

func (r *Runner) attachListeners() {
	r.AddListener(StartSendingSnapshotEvent, func(args ...any) {
		snapName := args[0].(string)
		r.onSendStart(snapName)
	})

	r.AddListener(SentSnapshotEvent, func(args ...any) {
		snapName := args[0].(string)
		r.onSendComplete(snapName)
	})

	r.AddListener(SnapshotSendingProgressEvent, func(args ...any) {
		snapName := args[0].(string)

		r.updateSendingState(snapName, func(sending *zfsSend) {
			sending.bytesSent = args[2].(int64)
			sending.updated = time.Now()
		})
	})
}

func (r *Runner) onSendStart(snapName string) {
	dsName := r.fullDatasetName(datasetName(snapName, true))
	ds, err := zfs.GetDataset(r.ctx, dsName)
	if err != nil {
		r.logger.Error("zfs.job.runner.onSendStart: Error retrieving dataset", "error", err, "snapName", snapName)
		return
	}
	err = ds.SetProperty(r.ctx, r.config.Properties.snapshotSending(), snapshotName(snapName))
	if err != nil {
		r.logger.Error("zfs.job.runner.onSendStart: Error setting dataset property",
			"error", err, "dataset", ds.Name, "property", r.config.Properties.snapshotSending(),
		)
		return
	}
	r.logger.Debug("zfs.job.runner.onSendStart: Snapshot sending property set")
}

func (r *Runner) onSendComplete(snapName string) {
	dsName := r.fullDatasetName(datasetName(snapName, true))
	ds, err := zfs.GetDataset(r.ctx, dsName)
	if err != nil {
		r.logger.Error("zfs.job.runner.onSendComplete: Error retrieving dataset", "error", err, "snapName", snapName)
		return
	}
	err = ds.InheritProperty(r.ctx, r.config.Properties.snapshotSending())
	if err != nil {
		r.logger.Error("zfs.job.runner.onSendComplete: Error inheriting dataset property",
			"error", err, "dataset", ds.Name, "property", r.config.Properties.snapshotSending(),
		)
		return
	}
	r.logger.Debug("zfs.job.runner.onSendStart: Snapshot sending property removed")
}

func (r *Runner) fullDatasetName(dataset string) string {
	if r.config.ParentDataset == "" {
		return dataset
	}
	return fmt.Sprintf("%s/%s", strings.TrimRight(r.config.ParentDataset, "/"), dataset)
}

func (r *Runner) datasetHasLockProperty(dataset string) bool {
	prop := r.config.Properties.datasetLocked()

	ds, err := zfs.GetDataset(r.ctx, dataset, prop)
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		r.logger.Warn("zfs.job.runner.datasetHasLockProperty: Dataset gone", "dataset", dataset, "error", err)
		return true // Lets not proceed by locking a nonexisting dataset
	case err != nil:
		r.logger.Error("zfs.job.runner.datasetHasLockProperty: Error retrieving dataset", "dataset", dataset, "error", err)
		return true // Lets assume it is locked then!
	}
	return propertyIsSet(ds.ExtraProps[prop])
}

func (r *Runner) lockDataset(dataset string) (succeeded bool, unlock func()) {
	if r.datasetHasLockProperty(dataset) {
		// The dataset has been locked by property
		return false, func() {} // Noop unlock
	}
	r.dsLock.Lock()
	_, ok := r.datasetLock[dataset]
	if ok {
		// Entry found, already locked.
		r.dsLock.Unlock()
		return false, func() {} // Noop unlock
	}
	// Set the lock!
	r.datasetLock[dataset] = struct{}{}
	r.dsLock.Unlock()

	return true, func() {
		// Simple unlock function removes entry from map:
		r.dsLock.Lock()
		delete(r.datasetLock, dataset)
		r.dsLock.Unlock()
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

		go r.runPruneRemoteCache()
	}

	if r.config.EnableSnapshotMark {
		go r.runMarkSnapshots(time.Minute)
	}

	if r.config.EnableSnapshotPrune {
		go r.runPruneSnapshots(time.Minute * 2)
	}

	if r.config.EnableFilesystemPrune {
		go r.runPruneFilesystems(time.Minute * 3)
	}
}

// ListCurrentSends returns a list of current ZFS sends in progress
func (r *Runner) ListCurrentSends() []ZFSSend {
	r.sendLock.RLock()
	lst := make([]ZFSSend, len(r.sends))
	for i := range r.sends {
		lst[i] = *r.sends[i]
	}
	r.sendLock.RUnlock()
	return lst
}

// SendDataset can be used to trigger send for a specific dataset.
// Do not include the snapshot part of the dataset.
// Blocking call, will block until one of the send goroutines has picked
// up the call. If sending is disabled, will block forever.
func (r *Runner) SendDataset(dataset string) {
	r.sendChan <- dataset
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
			case errors.Is(err, zfs.ErrPoolIOSuspended), errors.Is(err, zfs.ErrDatasetNotFound):
				r.logger.Warn("zfs.job.Runner.runCreateSnapshots: Cannot query datasets", "error", err)
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
	initDelay := time.Duration(int(sendSnapshotInterval) / r.config.SendRoutines * (id - 1))
	time.Sleep(initDelay)

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
			case errors.Is(err, zfs.ErrPoolIOSuspended), errors.Is(err, zfs.ErrDatasetNotFound):
				r.logger.Warn("zfs.job.Runner.runSendSnapshots: Cannot query datasets", "error", err)
			case err != nil:
				r.logger.Error("zfs.job.Runner.runSendSnapshots: Error sending snapshots", "error", err)
			}
		case dataset := <-r.sendChan:
			// Errors are already logged
			_ = r.sendDatasetSnapshotsByName(id, dataset)
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Runner) runPruneRemoteCache() {
	dur := randomizeDuration(pruneRemoteCacheInterval)
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.pruneRemoteDatasetCache()
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Runner) runMarkSnapshots(initDelay time.Duration) {
	time.Sleep(initDelay)

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
			case errors.Is(err, zfs.ErrPoolIOSuspended), errors.Is(err, zfs.ErrDatasetNotFound):
				r.logger.Warn("zfs.job.Runner.runCreateSnapshots: Cannot query datasets", "error", err)
			case err != nil:
				r.logger.Error("zfs.job.Runner.runMarkSnapshots: Error marking snapshots", "error", err)
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Runner) runPruneSnapshots(initDelay time.Duration) {
	time.Sleep(initDelay)

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
			case errors.Is(err, zfs.ErrPoolIOSuspended), errors.Is(err, zfs.ErrDatasetNotFound):
				r.logger.Warn("zfs.job.Runner.runPruneSnapshots: Cannot query datasets", "error", err)
			case err != nil:
				r.logger.Error("zfs.job.Runner.runPruneSnapshots: Error pruning snapshots", "error", err)
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Runner) runPruneFilesystems(initDelay time.Duration) {
	time.Sleep(initDelay)

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
			case errors.Is(err, zfs.ErrPoolIOSuspended), errors.Is(err, zfs.ErrDatasetNotFound):
				r.logger.Warn("zfs.job.Runner.runPruneFilesystems: Cannot query datasets", "error", err)
			case err != nil:
				r.logger.Error("zfs.job.Runner.runPruneFilesystems: Error pruning filesystems", "error", err)
			}
		case <-r.ctx.Done():
			return
		}
	}
}
