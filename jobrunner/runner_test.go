package jobrunner

import (
	"context"
	"net/http/httptest"
	"testing"

	zfshttp "github.com/vansante/go-zfs/http"

	eventemitter "github.com/vansante/go-event-emitter"

	"github.com/vansante/go-zfs"
)

const (
	testZPool      = "go-test-zpool-runner"
	testHTTPZPool  = "go-test-zpool-runner-http"
	testToken      = "blaat"
	testFilesystem = testZPool + "/testfs1"
)

func runnerTest(t *testing.T, fn func(server *httptest.Server, runner *Runner)) {
	t.Helper()

	zfshttp.TestHTTPZPool(testHTTPZPool, testToken, "", zfs.NewTestLogger(t), func(server *httptest.Server) {
		// Create another zpool as 'source':
		zfs.TestZPool(testZPool, func() {
			r := &Runner{
				Emitter: eventemitter.NewEmitter(false),
				config: Config{
					ParentDataset:                         testZPool,
					DatasetType:                           zfs.DatasetFilesystem,
					AuthorisationToken:                    testToken,
					IgnoreSnapshotsWithoutCreatedProperty: true,
					DeleteFilesystems:                     true,
				},
				logger: zfs.NewTestLogger(t),
				ctx:    context.Background(),
			}

			r.config.ApplyDefaults()
			r.config.MaximumSendTimeMinutes = 1

			_, err := zfs.CreateFilesystem(context.Background(), testFilesystem, map[string]string{zfs.PropertyCanMount: zfs.PropertyOff}, nil)
			if err != nil {
				panic(err)
			}

			r.AddCapturer(func(event eventemitter.EventType, arguments ...interface{}) {
				t.Logf("EVENT: %s %#v", event, arguments)
			})

			fn(server, r)
		})
	})
}
