package jobrunner

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	eventemitter "github.com/vansante/go-event-emitter"

	"github.com/vansante/go-zfs"
)

const testZPool = "go-test-zpool-runner"

func runnerTest(t *testing.T, fn func(runner *Runner)) {
	t.Helper()
	zfs.TestZPool(testZPool, func() {
		r := &Runner{
			Emitter: eventemitter.NewEmitter(false),
			config: Config{
				ParentDataset:                         testZPool,
				DatasetType:                           zfs.DatasetFilesystem,
				AuthorisationToken:                    "blaat",
				IgnoreSnapshotsWithoutCreatedProperty: true,
				DeleteFilesystems:                     true,
				MaximumSendTimeMinutes:                1,
			},
			logger: logrus.WithField("test", 1),
			ctx:    context.Background(),
		}

		r.config.ApplyDefaults()

		fn(r)
	})
}
