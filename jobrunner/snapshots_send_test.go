package jobrunner

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vansante/go-zfs"
)

func TestRunner_sendSnapshots(t *testing.T) {
	runnerTest(t, func(server *httptest.Server, runner *Runner) {
		ds, err := zfs.GetDataset(testFilesystem, nil)
		require.NoError(t, err)

		err = ds.SetProperty(defaultSnapshotSendToProperty, server.URL)
		require.NoError(t, err)

		snapshotTm := time.Now().Add(-time.Minute)
		snapName := runner.snapshotName(snapshotTm)
		snapshot, err := ds.Snapshot(snapName, false)
		require.NoError(t, err)

		err = snapshot.SetProperty(defaultSnapshotCreatedAtProperty, snapshotTm.Format(dateTimeFormat))
		require.NoError(t, err)

		verifyArgs := func(args []interface{}) {
			require.Len(t, args, 4)
			require.Equal(t, testFilesystem+"@"+snapName, args[0])
			require.Equal(t, server.URL, args[1])
			require.Equal(t, datasetName(testFilesystem, true), args[2])
			require.Equal(t, snapName, args[3])
		}

		sendingCount := 0
		runner.AddListener(SendingSnapshotEvent, func(arguments ...interface{}) {
			sendingCount++
			verifyArgs(arguments)
		})

		sentCount := 0
		runner.AddListener(SentSnapshotEvent, func(arguments ...interface{}) {
			sentCount++
			verifyArgs(arguments)
		})

		err = runner.sendSnapshots()
		require.NoError(t, err)

		require.Equal(t, 1, sendingCount)
		require.Equal(t, 1, sentCount)

		snaps, err := zfs.Snapshots(testHTTPZPool+"/"+datasetName(testFilesystem, true), nil)
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		require.Equal(t, testHTTPZPool+"/"+datasetName(testFilesystem, true)+"@"+runner.snapshotName(snapshotTm), snaps[0].Name)
	})
}
