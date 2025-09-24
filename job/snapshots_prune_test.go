package job

import (
	"fmt"
	"testing"
	"time"

	zfs "github.com/vansante/go-zfsutils"

	"github.com/stretchr/testify/require"
)

func TestRunner_pruneSnapshots(t *testing.T) {
	runnerTest(t, func(url string, runner *Runner) {
		createdProp := runner.config.Properties.snapshotCreatedAt()
		deleteProp := runner.config.Properties.deleteAt()

		ds, err := zfs.GetDataset(t.Context(), testFilesystem)
		require.NoError(t, err)

		const snap1, snap2, snap3, snap4 = "s1", "s2", "s3", "s4"
		now := time.Now()

		snap, err := ds.Snapshot(t.Context(), snap1, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), deleteProp, now.Add(-time.Minute*2).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(t.Context(), snap2, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), deleteProp, now.Add(-time.Second*6).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(t.Context(), snap3, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), createdProp, now.Add(time.Second).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(t.Context(), snap4, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), createdProp, now.Add(time.Minute).Format(dateTimeFormat)))

		events := 0
		runner.AddListener(DeletedSnapshotEvent, func(arguments ...interface{}) {
			events++

			require.Len(t, arguments, 3)
			require.Equal(t, datasetName(testFilesystem, true), arguments[1])

			switch arguments[0] {
			case fmt.Sprintf("%s@s1", testFilesystem):
				require.Equal(t, "s1", arguments[2])
			case fmt.Sprintf("%s@s2", testFilesystem):
				require.Equal(t, "s2", arguments[2])
			default:
				t.Logf("unexpected snapshot: %s", arguments[0])
				t.Fail()
			}
		})

		err = runner.pruneSnapshots()
		require.NoError(t, err)
		require.Equal(t, 2, events)

		snaps, err := ds.Snapshots(t.Context(), zfs.ListOptions{})
		require.NoError(t, err)
		require.Len(t, snaps, 2)
		require.Equal(t, snaps[0].Name, fmt.Sprintf("%s@%s", testFilesystem, snap3))
		require.Equal(t, snaps[1].Name, fmt.Sprintf("%s@%s", testFilesystem, snap4))
	})
}
