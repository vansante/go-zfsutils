package job

import (
	"fmt"
	"testing"
	"time"

	zfs "github.com/vansante/go-zfsutils"

	"github.com/stretchr/testify/require"
)

func TestRunner_pruneFilesystems(t *testing.T) {
	runnerTest(t, func(url string, runner *Runner) {
		delProp := runner.config.Properties.deleteAt()
		fs, err := zfs.GetDataset(t.Context(), testFilesystem)
		require.NoError(t, err)
		require.NoError(t, fs.SetProperty(t.Context(), delProp, time.Now().Add(-time.Minute).Format(dateTimeFormat)))

		createOpts := zfs.CreateFilesystemOptions{
			Properties: map[string]string{zfs.PropertyCanMount: zfs.ValueOff},
		}

		const otherFs, fsWithoutDel, fsWithSnap, otherVol, deleteLater = "test1", "test2", "test3", "test4", "test5"
		fs, err = zfs.CreateFilesystem(t.Context(), testZPool+"/"+otherFs, createOpts)
		require.NoError(t, err)
		require.NoError(t, fs.SetProperty(t.Context(), delProp, time.Now().Add(-time.Minute).Format(dateTimeFormat)))

		fs, err = zfs.CreateFilesystem(t.Context(), testZPool+"/"+fsWithoutDel, createOpts)
		require.NoError(t, err)

		fs, err = zfs.CreateFilesystem(t.Context(), testZPool+"/"+fsWithSnap, createOpts)
		require.NoError(t, err)
		require.NoError(t, fs.SetProperty(t.Context(), delProp, time.Now().Add(-time.Minute).Format(dateTimeFormat)))

		const snap = "snappie"
		_, err = fs.Snapshot(t.Context(), snap, zfs.SnapshotOptions{})
		require.NoError(t, err)

		vol, err := zfs.CreateVolume(t.Context(), testZPool+"/"+otherVol, 10_000, zfs.CreateVolumeOptions{})
		require.NoError(t, err)
		time.Sleep(time.Second / 3)
		require.NoError(t, vol.SetProperty(t.Context(), delProp, time.Now().Add(-time.Minute).Format(dateTimeFormat)))

		fs, err = zfs.CreateFilesystem(t.Context(), testZPool+"/"+deleteLater, createOpts)
		require.NoError(t, err)
		require.NoError(t, fs.SetProperty(t.Context(), delProp, time.Now().Add(time.Second*3).Format(dateTimeFormat)))

		events := 0
		runner.AddListener(DeletedFilesystemEvent, func(arguments ...interface{}) {
			events++

			require.Len(t, arguments, 2)
			switch arguments[0] {
			case fmt.Sprintf("%s/%s", testZPool, otherFs):
				require.Equal(t, otherFs, arguments[1])
			case testFilesystem:
				require.Equal(t, datasetName(testFilesystem, true), arguments[1])
			default:
				t.Errorf("Unexpected filesystem: %s", arguments[0])
				t.Fail()
			}
		})

		err = runner.pruneFilesystems()
		require.NoError(t, err)
		require.Equal(t, 2, events)

		ds, err := zfs.GetDataset(t.Context(), testZPool)
		require.NoError(t, err)

		datasets, err := ds.Children(t.Context(), zfs.ListOptions{})
		require.NoError(t, err)
		require.Len(t, datasets, 5)
		require.Equal(t, fmt.Sprintf("%s/%s", testZPool, fsWithoutDel), datasets[0].Name)
		require.Equal(t, fmt.Sprintf("%s/%s", testZPool, fsWithSnap), datasets[1].Name)
		require.Equal(t, fmt.Sprintf("%s/%s@%s", testZPool, fsWithSnap, snap), datasets[2].Name)
		require.Equal(t, fmt.Sprintf("%s/%s", testZPool, otherVol), datasets[3].Name)
		require.Equal(t, fmt.Sprintf("%s/%s", testZPool, deleteLater), datasets[4].Name)
	})
}
