package job

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	zfs "github.com/vansante/go-zfsutils"
	zfshttp "github.com/vansante/go-zfsutils/http"
)

// cacheRunner creates a Runner with just enough state for the remote cache, no zpool needed
func cacheRunner(t *testing.T, cacheAgeSeconds int64) *Runner {
	t.Helper()

	return &Runner{
		remoteCache: make(map[string]map[string]*datasetCache),
		zfsClient:   zfshttp.NewClient(nil, slog.Default()),
		config: Config{
			MaximumRemoteSnapshotCacheAgeSeconds: cacheAgeSeconds,
		},
		logger: slog.Default(),
		ctx:    t.Context(),
	}
}

func TestRunner_setRemoteDatasetCache(t *testing.T) {
	r := cacheRunner(t, 60)

	snaps := []zfs.Dataset{{Name: "testfs1@snap1"}}
	r.setRemoteDatasetCache("http://host1", "testfs1", snaps)

	require.Len(t, r.remoteCache, 1)
	require.Equal(t, snaps, r.remoteCache["http://host1"]["testfs1"].snapshots)
	require.WithinDuration(t, time.Now(), r.remoteCache["http://host1"]["testfs1"].cachedAt, time.Minute)

	// Overwriting an existing entry must reuse the same cache object
	dsCache := r.remoteCache["http://host1"]["testfs1"]
	nwSnaps := []zfs.Dataset{{Name: "testfs1@snap1"}, {Name: "testfs1@snap2"}}
	r.setRemoteDatasetCache("http://host1", "testfs1", nwSnaps)
	require.Same(t, dsCache, r.remoteCache["http://host1"]["testfs1"])
	require.Equal(t, nwSnaps, dsCache.snapshots)
}

func TestRunner_clearRemoteDatasetCache(t *testing.T) {
	r := cacheRunner(t, 60)

	r.setRemoteDatasetCache("http://host1", "testfs1", []zfs.Dataset{{Name: "testfs1@snap1"}})
	r.setRemoteDatasetCache("http://host1", "testfs2", []zfs.Dataset{{Name: "testfs2@snap1"}})

	r.clearRemoteDatasetCache("http://host1", "testfs1")
	require.NotContains(t, r.remoteCache["http://host1"], "testfs1")
	require.Contains(t, r.remoteCache["http://host1"], "testfs2")

	// Clearing unknown entries is a no-op
	r.clearRemoteDatasetCache("http://host2", "testfs1")
	r.clearRemoteDatasetCache("http://host1", "testfs3")
	require.Len(t, r.remoteCache, 1)
	require.Len(t, r.remoteCache["http://host1"], 1)
}

func TestRunner_pruneRemoteDatasetCache(t *testing.T) {
	r := cacheRunner(t, 60)

	r.setRemoteDatasetCache("http://host1", "expired", []zfs.Dataset{{Name: "expired@snap1"}})
	r.setRemoteDatasetCache("http://host1", "fresh", []zfs.Dataset{{Name: "fresh@snap1"}})
	r.setRemoteDatasetCache("http://host2", "expired", []zfs.Dataset{{Name: "expired@snap1"}})

	r.remoteCache["http://host1"]["expired"].cachedAt = time.Now().Add(-2 * time.Minute)
	r.remoteCache["http://host2"]["expired"].cachedAt = time.Now().Add(-2 * time.Minute)

	r.pruneRemoteDatasetCache()

	require.Len(t, r.remoteCache, 1, "server without cache entries should be removed")
	require.Len(t, r.remoteCache["http://host1"], 1)
	require.Contains(t, r.remoteCache["http://host1"], "fresh")
}

func TestRunner_remoteDatasetSnapshots(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests++
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]zfs.Dataset{{Name: "testfs1@remote"}})
	}))
	defer server.Close()

	r := cacheRunner(t, 60)
	r.config.Properties.ApplyDefaults()

	cached := []zfs.Dataset{{Name: "testfs1@cached"}}
	r.setRemoteDatasetCache(server.URL, "testfs1", cached)

	// Within the cache age, the cached value is returned without a request
	snaps, err := r.remoteDatasetSnapshots(server.URL, "testfs1")
	require.NoError(t, err)
	require.Equal(t, cached, snaps)
	require.Zero(t, requests)

	// An unknown dataset is a cache miss
	snaps, err = r.remoteDatasetSnapshots(server.URL, "testfs2")
	require.NoError(t, err)
	require.Equal(t, []zfs.Dataset{{Name: "testfs1@remote"}}, snaps)
	require.Equal(t, 1, requests)
	require.Contains(t, r.remoteCache[server.URL], "testfs2")

	// Expiring the entry causes a refresh
	r.remoteCache[server.URL]["testfs1"].cachedAt = time.Now().Add(-2 * time.Minute)
	snaps, err = r.remoteDatasetSnapshots(server.URL, "testfs1")
	require.NoError(t, err)
	require.Equal(t, []zfs.Dataset{{Name: "testfs1@remote"}}, snaps)
	require.Equal(t, 2, requests)
}

func TestRunner_remoteDatasetSnapshots_notFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	r := cacheRunner(t, 60)
	r.config.Properties.ApplyDefaults()

	// A missing remote dataset is not an error, it just means everything has to be sent
	snaps, err := r.remoteDatasetSnapshots(server.URL, "testfs1")
	require.NoError(t, err)
	require.Empty(t, snaps)
	require.Contains(t, r.remoteCache[server.URL], "testfs1")
}

func TestRunner_remoteDatasetSnapshots_error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	r := cacheRunner(t, 60)
	r.config.Properties.ApplyDefaults()

	_, err := r.remoteDatasetSnapshots(server.URL, "testfs1")
	require.ErrorContains(t, err, "error listing remote")
	require.Empty(t, r.remoteCache)
}
