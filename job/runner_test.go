package job

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"testing"

	eventemitter "github.com/vansante/go-event-emitter"

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
			r := &Runner{
				Emitter:     eventemitter.NewEmitter(false),
				datasetLock: make(map[string]struct{}),
				remoteCache: make(map[string]map[string]datasetCache),
				sendChan:    make(chan string),
				config: Config{
					ParentDataset: testZPool,
					DatasetType:   zfs.DatasetFilesystem,
				},
				logger: slog.Default(),
				ctx:    context.Background(),
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

			_, err := zfs.CreateFilesystem(context.Background(), testFilesystem, zfs.CreateFilesystemOptions{
				Properties: map[string]string{zfs.PropertyCanMount: zfs.ValueOff},
			})
			if err != nil {
				panic(err)
			}

			r.AddCapturer(func(event eventemitter.EventType, arguments ...interface{}) {
				t.Logf("EVENT: %s %#v", event, arguments)
			})

			fn(server.URL+testPrefix, r)
		})
	})
}
