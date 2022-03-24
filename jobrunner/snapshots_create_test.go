package jobrunner

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vansante/go-zfs"
)

func TestRunner_createSnapshots(t *testing.T) {
	runnerTest(t, func(server *httptest.Server, runner *Runner) {
		const fsName = "test"
		intervalProp := runner.config.Properties.snapshotIntervalMinutes()
		createProp := runner.config.Properties.snapshotCreatedAt()

		ds, err := zfs.CreateFilesystem(context.Background(), testZPool+"/"+fsName, map[string]string{
			intervalProp:         "1",
			zfs.PropertyCanMount: zfs.PropertyOff,
		}, nil)
		require.NoError(t, err)

		emitCount := 0
		runner.Emitter.AddListener(CreatedSnapshotEvent, func(arguments ...interface{}) {
			emitCount++

			require.Len(t, arguments, 3)
			require.Equal(t, testZPool+"/"+fsName, arguments[0])

			tm := time.Now()
			name := runner.snapshotName(tm)
			require.Equal(t, runner.snapshotName(time.Now()), arguments[1])
			createTm := arguments[2].(time.Time)
			require.WithinDuration(t, tm, createTm, time.Second)

			snaps, err := ds.Snapshots(context.Background(), []string{createProp})
			require.NoError(t, err)
			require.Len(t, snaps, 1)
			require.Equal(t, snaps[0].Name, testZPool+"/"+fsName+"@"+name)
			require.Equal(t, snaps[0].ExtraProps[createProp], tm.Format(dateTimeFormat))
		})

		err = runner.createSnapshots()
		require.NoError(t, err)

		require.Equal(t, 1, emitCount)
	})
}
