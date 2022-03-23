package jobrunner

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vansante/go-zfs"
)

func TestRunner_pruneSnapshots(t *testing.T) {
	runnerTest(t, func(server *httptest.Server, runner *Runner) {
		ds, err := zfs.GetDataset(testFilesystem, nil)
		require.NoError(t, err)

		const snap1, snap2, snap3, snap4 = "s1", "s2", "s3", "s4"
		now := time.Now()

		snap, err := ds.Snapshot(snap1, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(defaultDeleteAtProperty, now.Add(-time.Minute*2).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(snap2, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(defaultDeleteAtProperty, now.Add(-time.Second*6).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(snap3, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(defaultSnapshotCreatedAtProperty, now.Add(time.Second).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(snap4, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(defaultSnapshotCreatedAtProperty, now.Add(time.Minute).Format(dateTimeFormat)))

		events := 0
		runner.AddListener(DeletedSnapshotEvent, func(arguments ...interface{}) {
			events++

			require.Len(t, arguments, 3)
			require.Equal(t, fmt.Sprintf("%s@s%d", testFilesystem, events), arguments[0])
			require.Equal(t, datasetName(testFilesystem, true), arguments[1])
			require.Equal(t, fmt.Sprintf("s%d", events), arguments[2])
		})

		err = runner.pruneSnapshots()
		require.NoError(t, err)
		require.Equal(t, 2, events)

		snaps, err := ds.Snapshots(nil)
		require.NoError(t, err)
		require.Len(t, snaps, 2)
		require.Equal(t, snaps[0].Name, fmt.Sprintf("%s@%s", testFilesystem, snap3))
		require.Equal(t, snaps[1].Name, fmt.Sprintf("%s@%s", testFilesystem, snap4))
	})
}
