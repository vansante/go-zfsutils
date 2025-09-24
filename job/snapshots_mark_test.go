package job

import (
	"testing"
	"time"

	zfs "github.com/vansante/go-zfsutils"

	"github.com/stretchr/testify/require"
)

func TestRunner_markPrunableExcessSnapshots(t *testing.T) {
	runnerTest(t, func(url string, runner *Runner) {
		retCountProp := runner.config.Properties.snapshotRetentionCount()
		createdProp := runner.config.Properties.snapshotCreatedAt()
		deleteProp := runner.config.Properties.deleteAt()

		ds, err := zfs.GetDataset(t.Context(), testFilesystem)
		require.NoError(t, err)

		err = ds.SetProperty(t.Context(), retCountProp, "2")
		require.NoError(t, err)

		const snap1, snap2, snap3 = "s1", "s2", "s3"
		now := time.Now()

		snap, err := ds.Snapshot(t.Context(), snap1, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), createdProp, now.Format(dateTimeFormat)))

		snap, err = ds.Snapshot(t.Context(), snap2, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), createdProp, now.Format(dateTimeFormat)))

		snap, err = ds.Snapshot(t.Context(), snap3, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), createdProp, now.Format(dateTimeFormat)))

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

		snaps, err := ds.Snapshots(t.Context(), zfs.ListOptions{ExtraProperties: []string{deleteProp}})
		require.NoError(t, err)
		require.Len(t, snaps, 3)

		require.Equal(t, snap1, snapshotName(snaps[0].Name))
		tm, err := parseDatasetTimeProperty(&snaps[0], deleteProp)
		require.NoError(t, err)
		require.WithinDuration(t, now.Add(deleteAfter), tm, time.Second)

		require.Equal(t, snap2, snapshotName(snaps[1].Name))
		require.Equal(t, "", snaps[1].ExtraProps[deleteProp])
		require.Equal(t, snap3, snapshotName(snaps[2].Name))
		require.Equal(t, "", snaps[2].ExtraProps[deleteProp])
	})
}

func TestRunner_markPrunableSnapshotsByAge(t *testing.T) {
	runnerTest(t, func(url string, runner *Runner) {
		retentionProp := runner.config.Properties.snapshotRetentionMinutes()
		createdProp := runner.config.Properties.snapshotCreatedAt()
		deleteProp := runner.config.Properties.deleteAt()

		ds, err := zfs.GetDataset(t.Context(), testFilesystem)
		require.NoError(t, err)

		err = ds.SetProperty(t.Context(), retentionProp, "2")
		require.NoError(t, err)

		const snap1, snap2, snap3, snap4 = "s1", "s2", "s3", "s4"
		now := time.Now()

		snap, err := ds.Snapshot(t.Context(), snap1, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), createdProp, now.Add(-time.Minute*3).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(t.Context(), snap2, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), createdProp, now.Add(-time.Minute).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(t.Context(), snap3, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), createdProp, now.Add(time.Minute).Format(dateTimeFormat)))

		snap, err = ds.Snapshot(t.Context(), snap4, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), createdProp, now.Add(time.Minute*3).Format(dateTimeFormat)))

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

		snaps, err := ds.Snapshots(t.Context(), zfs.ListOptions{ExtraProperties: []string{deleteProp}})
		require.NoError(t, err)
		require.Len(t, snaps, 4)

		require.Equal(t, snap1, snapshotName(snaps[0].Name))
		tm, err := parseDatasetTimeProperty(&snaps[0], deleteProp)
		require.NoError(t, err)
		require.WithinDuration(t, now.Add(deleteAfter), tm, time.Second)

		require.Equal(t, snap2, snapshotName(snaps[1].Name))
		require.Equal(t, "", snaps[1].ExtraProps[deleteProp])
		require.Equal(t, snap3, snapshotName(snaps[2].Name))
		require.Equal(t, "", snaps[2].ExtraProps[deleteProp])
		require.Equal(t, snap4, snapshotName(snaps[3].Name))
		require.Equal(t, "", snaps[3].ExtraProps[deleteProp])
	})
}

func TestRunner_markPrunableSnapshotsWithParentDeleteAt(t *testing.T) {
	runnerTest(t, func(url string, runner *Runner) {
		retentionProp := runner.config.Properties.snapshotRetentionMinutes()
		createdProp := runner.config.Properties.snapshotCreatedAt()
		deleteProp := runner.config.Properties.deleteAt()

		ds, err := zfs.GetDataset(t.Context(), testFilesystem)
		require.NoError(t, err)

		now := time.Now()
		err = ds.SetProperty(t.Context(), retentionProp, "15")
		require.NoError(t, err)
		err = ds.SetProperty(t.Context(), deleteProp, now.Add(time.Minute*60).Format(dateTimeFormat))
		require.NoError(t, err)

		const snap1 = "s1"

		snap, err := ds.Snapshot(t.Context(), snap1, zfs.SnapshotOptions{})
		require.NoError(t, err)
		require.NoError(t, snap.SetProperty(t.Context(), createdProp, now.Add(-time.Minute*20).Format(dateTimeFormat)))

		// Setup done, start

		events := 0
		runner.AddListener(MarkSnapshotDeletionEvent, func(arguments ...interface{}) {
			events++
		})

		err = runner.markPrunableSnapshotsByAge()
		require.NoError(t, err)
		require.Equal(t, 1, events)

		snaps, err := ds.Snapshots(t.Context(), zfs.ListOptions{ExtraProperties: []string{deleteProp}})
		require.NoError(t, err)
		require.Len(t, snaps, 1)

		require.Equal(t, snap1, snapshotName(snaps[0].Name))
		tm, err := parseDatasetTimeProperty(&snaps[0], deleteProp)
		require.NoError(t, err)
		require.WithinDuration(t, now.Add(deleteAfter), tm, time.Second)
	})
}
