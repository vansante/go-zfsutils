package jobrunner

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vansante/go-zfs"
)

func TestRunner_pruneFilesystems(t *testing.T) {
	runnerTest(t, func(server *httptest.Server, runner *Runner) {
		fs, err := zfs.GetDataset(testFilesystem, nil)
		require.NoError(t, err)
		require.NoError(t, fs.SetProperty(defaultDeleteAtProperty, time.Now().Add(-time.Minute).Format(dateTimeFormat)))

		const otherFs, fsWithoutDel, fsWithSnap, otherVol = "test1", "test2", "test3", "test4"
		fs, err = zfs.CreateFilesystem(testZPool+"/"+otherFs, map[string]string{zfs.PropertyCanMount: zfs.PropertyOff}, nil)
		require.NoError(t, err)
		require.NoError(t, fs.SetProperty(defaultDeleteAtProperty, time.Now().Add(-time.Minute).Format(dateTimeFormat)))

		fs, err = zfs.CreateFilesystem(testZPool+"/"+fsWithoutDel, map[string]string{zfs.PropertyCanMount: zfs.PropertyOff}, nil)
		require.NoError(t, err)

		fs, err = zfs.CreateFilesystem(testZPool+"/"+fsWithSnap, map[string]string{zfs.PropertyCanMount: zfs.PropertyOff}, nil)
		require.NoError(t, err)
		require.NoError(t, fs.SetProperty(defaultDeleteAtProperty, time.Now().Add(-time.Minute).Format(dateTimeFormat)))

		const snap = "snappie"
		_, err = fs.Snapshot(snap, false)
		require.NoError(t, err)

		vol, err := zfs.CreateVolume(testZPool+"/"+otherVol, 10_000, nil, nil)
		require.NoError(t, err)
		time.Sleep(time.Second / 3)
		require.NoError(t, vol.SetProperty(defaultDeleteAtProperty, time.Now().Add(-time.Minute).Format(dateTimeFormat)))

		events := 0
		runner.AddListener(DeletedFilesystemEvent, func(arguments ...interface{}) {
			events++

			require.Len(t, arguments, 2)
			switch events {
			case 1:
				require.Equal(t, fmt.Sprintf("%s/%s", testZPool, otherFs), arguments[0])
				require.Equal(t, otherFs, arguments[1])
			case 2:
				require.Equal(t, testFilesystem, arguments[0])
				require.Equal(t, datasetName(testFilesystem, true), arguments[1])
			}
		})

		err = runner.pruneFilesystems()
		require.NoError(t, err)
		require.Equal(t, 2, events)

		ds, err := zfs.GetDataset(testZPool, nil)
		require.NoError(t, err)

		datasets, err := ds.Children(0, nil)
		require.NoError(t, err)
		require.Len(t, datasets, 4)
		require.Equal(t, fmt.Sprintf("%s/%s", testZPool, fsWithoutDel), datasets[0].Name)
		require.Equal(t, fmt.Sprintf("%s/%s", testZPool, fsWithSnap), datasets[1].Name)
		require.Equal(t, fmt.Sprintf("%s/%s@%s", testZPool, fsWithSnap, snap), datasets[2].Name)
		require.Equal(t, fmt.Sprintf("%s/%s", testZPool, otherVol), datasets[3].Name)
	})
}
