package job

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	eventemitter "github.com/vansante/go-event-emitter"

	"github.com/stretchr/testify/require"

	zfs "github.com/vansante/go-zfsutils"
	zfshttp "github.com/vansante/go-zfsutils/http"
)

const (
	testZPool      = "go-test-zpool-runner"
	testHTTPZPool  = "go-test-zpool-runner-http"
	testPrefix     = "/test-world"
	testFilesystem = testZPool + "/testfs1"
)

func runnerTest(t *testing.T, fn func(url string, runner *Runner)) {
	t.Helper()

	zfshttp.TestHTTPZPool(testHTTPZPool, testPrefix, "", func(server *httptest.Server) {
		// Create another zpool as 'source':
		zfs.TestZPool(testZPool, func() {
			slog.SetLogLoggerLevel(slog.LevelDebug)
			r := &Runner{
				Emitter:     eventemitter.NewEmitter(false),
				datasetLock: make(map[string]struct{}),
				remoteCache: make(map[string]map[string]*datasetCache),
				sendChan:    make(chan string),
				zfsClient:   zfshttp.NewClient(nil, slog.Default()),
				config: Config{
					ParentDataset: testZPool,
					DatasetType:   zfs.DatasetFilesystem,
				},
				logger: slog.Default(),
				ctx:    t.Context(),
			}
			r.attachListeners()

			r.config.ApplyDefaults()
			r.config.MaximumSendTimeSeconds = 30
			r.config.SendSetProperties = map[string]string{
				zfs.PropertyCanMount: zfs.ValueOff,
			}
			r.config.SendCopyProperties = []string{
				defaultNamespace + ":" + defaultSnapshotCreatedAtProperty,
			}

			_, err := zfs.CreateFilesystem(t.Context(), testFilesystem, zfs.CreateFilesystemOptions{
				Properties: map[string]string{zfs.PropertyCanMount: zfs.ValueOff},
			})
			if err != nil {
				panic(err)
			}

			r.AddCapturer(func(event eventemitter.EventType, arguments ...any) {
				t.Logf("EVENT: %s %#v", event, arguments)
			})

			fn(server.URL+testPrefix, r)
		})
	})
}

func TestNewRunner(t *testing.T) {
	var gotHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotHeader = req.Header.Get("X-Test")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("[]"))
	}))
	defer server.Close()

	conf := Config{}
	conf.ApplyDefaults()
	conf.ParentDataset = testZPool
	conf.HTTPHeaders = map[string]string{"X-Test": "hello"}

	r := NewRunner(t.Context(), conf, slog.Default())
	require.NotNil(t, r.Emitter)
	require.NotNil(t, r.datasetLock)
	require.NotNil(t, r.remoteCache)
	require.NotNil(t, r.sendChan)
	require.NotNil(t, r.zfsClient)
	require.Equal(t, testZPool, r.config.ParentDataset)
	require.Equal(t, defaultSendRoutines, r.config.SendRoutines)
	require.Equal(t, defaultNamespace, r.config.Properties.Namespace)
	require.Empty(t, r.ListCurrentSends())

	// The configured HTTP headers must be wired into the send client
	snaps, err := r.zfsClient.DatasetSnapshots(t.Context(), server.URL, "testfs1", nil)
	require.NoError(t, err)
	require.Empty(t, snaps)
	require.Equal(t, "hello", gotHeader)
}

func TestRunner_Run(t *testing.T) {
	conf := Config{}
	conf.ApplyDefaults()
	conf.EnableFilesystemPrune = true
	conf.SendRoutines = 1

	ctx, cancel := context.WithCancel(t.Context())
	r := NewRunner(ctx, conf, slog.Default())

	// All job intervals are minutes, so none of the routines does any work before the cancel
	r.Run()
	require.Empty(t, r.ListCurrentSends())

	cancel()
	time.Sleep(time.Millisecond * 100)
	require.Empty(t, r.ListCurrentSends())
}

func TestRunner_fullDatasetName(t *testing.T) {
	tests := []struct {
		parent  string
		dataset string
		want    string
	}{
		{"", "testfs1", "testfs1"},
		{"testpool", "testfs1", "testpool/testfs1"},
		{"testpool/", "testfs1", "testpool/testfs1"},
		{"testpool/nested", "testfs1", "testpool/nested/testfs1"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			r := &Runner{config: Config{ParentDataset: tt.parent}}
			require.Equal(t, tt.want, r.fullDatasetName(tt.dataset))
		})
	}
}

func TestRunner_lockDataset(t *testing.T) {
	runnerTest(t, func(url string, runner *Runner) {
		locked, unlock := runner.lockDataset(testFilesystem)
		require.True(t, locked)
		require.Contains(t, runner.datasetLock, testFilesystem)

		// A second lock of the same dataset must not succeed
		otherLocked, otherUnlock := runner.lockDataset(testFilesystem)
		require.False(t, otherLocked)
		otherUnlock() // Noop unlock, must not release the lock
		require.Contains(t, runner.datasetLock, testFilesystem)

		unlock()
		require.NotContains(t, runner.datasetLock, testFilesystem)

		locked, unlock = runner.lockDataset(testFilesystem)
		require.True(t, locked)
		unlock()

		// A dataset locked by property cannot be locked
		ds, err := zfs.GetDataset(t.Context(), testFilesystem)
		require.NoError(t, err)
		require.NoError(t, ds.SetProperty(t.Context(), runner.config.Properties.datasetLocked(), "true"))

		require.True(t, runner.datasetHasLockProperty(testFilesystem))
		locked, _ = runner.lockDataset(testFilesystem)
		require.False(t, locked)
		require.NotContains(t, runner.datasetLock, testFilesystem)

		// A nonexistent dataset is considered locked
		require.True(t, runner.datasetHasLockProperty(testZPool+"/doesnotexist"))
	})
}
