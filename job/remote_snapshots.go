package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	zfs "github.com/vansante/go-zfsutils"
	zfshttp "github.com/vansante/go-zfsutils/http"
)

const (
	maximumCacheAge = 2 * time.Minute * 60
)

type datasetCache struct {
	cachedAt  time.Time
	snapshots []zfs.Dataset
}

// remoteDatasetSnapshots retrieves the remote datasets snapshots, but caches that data for a time
func (r *Runner) remoteDatasetSnapshots(client *zfshttp.Client, remoteDataset string) ([]zfs.Dataset, error) {
	r.cacheLock.RLock()
	serverCache, ok := r.remoteCache[client.Server()]
	if ok {
		dsCache, ok := serverCache[remoteDataset]
		if ok && time.Since(dsCache.cachedAt) < maximumCacheAge {
			r.cacheLock.RUnlock()
			return dsCache.snapshots, nil
		}
	}
	r.cacheLock.RUnlock()

	ctx, cancel := context.WithTimeout(r.ctx, requestTimeout)
	remoteSnaps, err := client.DatasetSnapshots(ctx, remoteDataset, []string{r.config.Properties.snapshotCreatedAt()})
	cancel()
	switch {
	case errors.Is(err, zfshttp.ErrDatasetNotFound):
		// Not an error, just means we have to send everything
	case err != nil:
		return nil, fmt.Errorf("error listing remote %s snapshots for %s: %w", client.Server(), remoteDataset, err)
	}

	r.setRemoteDatasetCache(client.Server(), remoteDataset, remoteSnaps)

	return remoteSnaps, nil
}

func (r *Runner) setRemoteDatasetCache(server, remoteDataset string, snapshots []zfs.Dataset) {
	r.cacheLock.Lock()
	defer r.cacheLock.Unlock()

	serverCache, ok := r.remoteCache[server]
	if !ok {
		serverCache = make(map[string]datasetCache)
		r.remoteCache[server] = serverCache
	}

	dsCache, ok := serverCache[remoteDataset]
	if !ok {
		dsCache = datasetCache{}
		serverCache[remoteDataset] = dsCache
	}
	dsCache.cachedAt = time.Now()
	dsCache.snapshots = snapshots
}

func (r *Runner) clearRemoteDatasetCache(server, remoteDataset string) {
	r.cacheLock.Lock()
	defer r.cacheLock.Unlock()

	serverCache, ok := r.remoteCache[server]
	if !ok {
		return
	}
	delete(serverCache, remoteDataset)
}

func (r *Runner) pruneRemoteDatasetCache() {
	r.cacheLock.Lock()
	defer r.cacheLock.Unlock()

	for _, serverCache := range r.remoteCache {
		for remoteDataset, dsCache := range serverCache {
			if time.Since(dsCache.cachedAt) >= maximumCacheAge {
				delete(serverCache, remoteDataset)
			}
		}
	}

	for server, serverCache := range r.remoteCache {
		if len(serverCache) == 0 {
			delete(r.remoteCache, server)
		}
	}
}
