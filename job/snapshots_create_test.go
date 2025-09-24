package job

import (
	"testing"
	"time"

	zfs "github.com/vansante/go-zfsutils"

	"github.com/stretchr/testify/require"
)

func TestRunner_createSnapshots(t *testing.T) {
	runnerTest(t, func(url string, runner *Runner) {
		const fsName = "test"
		intervalProp := runner.config.Properties.snapshotIntervalMinutes()
		createProp := runner.config.Properties.snapshotCreatedAt()

		ds, err := zfs.CreateFilesystem(t.Context(), testZPool+"/"+fsName, zfs.CreateFilesystemOptions{
			Properties: map[string]string{
				intervalProp:         "1",
				zfs.PropertyCanMount: zfs.ValueOff,
			},
		})
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

			snaps, err := ds.Snapshots(t.Context(), zfs.ListOptions{ExtraProperties: []string{createProp}})
			require.NoError(t, err)
			require.Len(t, snaps, 1)
			require.Equal(t, snaps[0].Name, testZPool+"/"+fsName+"@"+name)
			require.Equal(t, snaps[0].ExtraProps[createProp], tm.Format(dateTimeFormat))
		})

		err = runner.createSnapshots()
		require.NoError(t, err)

		require.Equal(t, 1, emitCount)

		// Run again to check we don't make another snapshot
		err = runner.createSnapshots()
		require.NoError(t, err)

		// We expect not another snapshot to be made
		require.Equal(t, 1, emitCount)
	})
}
