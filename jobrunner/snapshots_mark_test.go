package jobrunner

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vansante/go-zfs"
)

func TestRunner_markPrunableExcessSnapshots(t *testing.T) {
	runnerTest(t, func(server *httptest.Server, runner *Runner) {
		ds, err := zfs.GetDataset(testFilesystem, nil)
		require.NoError(t, err)

		err = ds.SetProperty(defaultSnapshotRetentionCountProperty, "2")
		require.NoError(t, err)

		const snap1, snap2, snap3 = "s1", "s2", "s3"
		now := time.Now()

		snap, err := ds.Snapshot(snap1, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(defaultSnapshotCreatedAtProperty, now.Format(dateTimeFormat)))

		snap, err = ds.Snapshot(snap2, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(defaultSnapshotCreatedAtProperty, now.Format(dateTimeFormat)))

		snap, err = ds.Snapshot(snap3, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(defaultSnapshotCreatedAtProperty, now.Format(dateTimeFormat)))

		events := 0
		runner.AddListener(MarkSnapshotDeletionEvent, func(arguments ...interface{}) {
			events++

			require.Len(t, arguments, 3)
			require.Equal(t, testFilesystem+"@"+snap1, arguments[0])
			require.Equal(t, datasetName(testFilesystem, true), arguments[1])
			require.Equal(t, snap1, arguments[2])
		})

		err = runner.markPrunableExcessSnapshots()
		require.NoError(t, err)
		require.Equal(t, 1, events)

		snaps, err := ds.Snapshots([]string{defaultDeleteAtProperty})
		require.NoError(t, err)
		require.Len(t, snaps, 3)

		require.Equal(t, snap1, snapshotName(snaps[0].Name))
		tm, err := parseDatasetTimeProperty(&snaps[0], defaultDeleteAtProperty)
		require.NoError(t, err)
		require.WithinDuration(t, now, tm, time.Second)

		require.Equal(t, snap2, snapshotName(snaps[1].Name))
		require.Equal(t, zfs.PropertyUnset, snaps[1].ExtraProps[defaultDeleteAtProperty])
		require.Equal(t, snap3, snapshotName(snaps[2].Name))
		require.Equal(t, zfs.PropertyUnset, snaps[2].ExtraProps[defaultDeleteAtProperty])
	})
}

func TestRunner_markPrunableSnapshotsByAge(t *testing.T) {
	runnerTest(t, func(server *httptest.Server, runner *Runner) {
		ds, err := zfs.GetDataset(testFilesystem, nil)
		require.NoError(t, err)

		err = ds.SetProperty(defaultSnapshotMaxRetentionMinutesProperty, "2")
		require.NoError(t, err)

		const snap1, snap2, snap3, snap4 = "s1", "s2", "s3", "s4"
		now := time.Now()

		snap, err := ds.Snapshot(snap1, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(defaultSnapshotCreatedAtProperty, now.Add(-time.Minute*3).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(snap2, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(defaultSnapshotCreatedAtProperty, now.Add(-time.Minute).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(snap3, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(defaultSnapshotCreatedAtProperty, now.Add(time.Minute).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(snap4, false)
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(defaultSnapshotCreatedAtProperty, now.Add(time.Minute*3).Format(dateTimeFormat)))

		events := 0
		runner.AddListener(MarkSnapshotDeletionEvent, func(arguments ...interface{}) {
			events++

			require.Len(t, arguments, 3)
			require.Equal(t, testFilesystem+"@"+snap1, arguments[0])
			require.Equal(t, datasetName(testFilesystem, true), arguments[1])
			require.Equal(t, snap1, arguments[2])
		})

		err = runner.markPrunableSnapshotsByAge()
		require.NoError(t, err)
		require.Equal(t, 1, events)

		snaps, err := ds.Snapshots([]string{defaultDeleteAtProperty})
		require.NoError(t, err)
		require.Len(t, snaps, 4)

		require.Equal(t, snap1, snapshotName(snaps[0].Name))
		tm, err := parseDatasetTimeProperty(&snaps[0], defaultDeleteAtProperty)
		require.NoError(t, err)
		require.WithinDuration(t, now, tm, time.Second)

		require.Equal(t, snap2, snapshotName(snaps[1].Name))
		require.Equal(t, zfs.PropertyUnset, snaps[1].ExtraProps[defaultDeleteAtProperty])
		require.Equal(t, snap3, snapshotName(snaps[2].Name))
		require.Equal(t, zfs.PropertyUnset, snaps[2].ExtraProps[defaultDeleteAtProperty])
		require.Equal(t, snap4, snapshotName(snaps[3].Name))
		require.Equal(t, zfs.PropertyUnset, snaps[3].ExtraProps[defaultDeleteAtProperty])
	})
}
