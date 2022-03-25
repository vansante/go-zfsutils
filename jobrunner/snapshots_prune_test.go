package jobrunner

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vansante/go-zfs"
)

func TestRunner_pruneSnapshots(t *testing.T) {
	runnerTest(t, func(server *httptest.Server, runner *Runner) {
		createdProp := runner.config.Properties.snapshotCreatedAt()
		deleteProp := runner.config.Properties.deleteAt()

		ds, err := zfs.GetDataset(context.Background(), testFilesystem)
		require.NoError(t, err)

		const snap1, snap2, snap3, snap4 = "s1", "s2", "s3", "s4"
		now := time.Now()

		snap, err := ds.Snapshot(context.Background(), snap1, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(context.Background(), deleteProp, now.Add(-time.Minute*2).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(context.Background(), snap2, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(context.Background(), deleteProp, now.Add(-time.Second*6).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(context.Background(), snap3, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(context.Background(), createdProp, now.Add(time.Second).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(context.Background(), snap4, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(context.Background(), createdProp, now.Add(time.Minute).Format(dateTimeFormat)))

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

		snaps, err := ds.Snapshots(context.Background())
		require.NoError(t, err)
		require.Len(t, snaps, 2)
		require.Equal(t, snaps[0].Name, fmt.Sprintf("%s@%s", testFilesystem, snap3))
		require.Equal(t, snaps[1].Name, fmt.Sprintf("%s@%s", testFilesystem, snap4))
	})
}
