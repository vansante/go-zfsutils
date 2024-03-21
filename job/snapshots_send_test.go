package job

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	zfs "github.com/vansante/go-zfsutils"

	"github.com/stretchr/testify/require"
)

var sendSnaps = []string{
	"snap1",
	"snap2",
	"snap3",
	"snap4",
	"snap5",
}

func sendTest(t *testing.T, fn func(server *httptest.Server, runner *Runner)) {
	runnerTest(t, func(server *httptest.Server, runner *Runner) {
		createdProp := runner.config.Properties.snapshotCreatedAt()
		sendToProp := runner.config.Properties.snapshotSendTo()

		ds, err := zfs.GetDataset(context.Background(), testFilesystem)
		require.NoError(t, err)

		err = ds.SetProperty(context.Background(), sendToProp, server.URL)
		require.NoError(t, err)

		snapshotTm := time.Now().Add(-time.Minute)
		for _, snap := range sendSnaps {
			snapshot, err := ds.Snapshot(context.Background(), snap, zfs.SnapshotOptions{})
			require.NoError(t, err)
			err = snapshot.SetProperty(context.Background(), createdProp, snapshotTm.Format(dateTimeFormat))
			require.NoError(t, err)
		}

		fn(server, runner)
	})
}

func TestRunner_sendSnapshots(t *testing.T) {
	sendTest(t, func(server *httptest.Server, runner *Runner) {
		verifyArgs := func(i int, args []interface{}) {
			require.Len(t, args, 4)
			require.Equal(t, testFilesystem+"@"+sendSnaps[i], args[0])
			require.Equal(t, server.URL, args[1])
			require.Equal(t, datasetName(testFilesystem, true), args[2])
			require.Equal(t, sendSnaps[i], args[3])
		}

		sendingCount := 0
		runner.AddListener(SendingSnapshotEvent, func(arguments ...interface{}) {
			verifyArgs(sendingCount, arguments)
			sendingCount++
		})

		sentCount := 0
		runner.AddListener(SentSnapshotEvent, func(arguments ...interface{}) {
			verifyArgs(sentCount, arguments)
			sentCount++
		})

		err := runner.sendSnapshots(1)
		require.NoError(t, err)

		require.Equal(t, 5, sendingCount)
		require.Equal(t, 5, sentCount)

		snaps, err := zfs.ListSnapshots(context.Background(), zfs.ListOptions{
			ParentDataset: testHTTPZPool + "/" + datasetName(testFilesystem, true),
		})
		require.NoError(t, err)
		require.Len(t, snaps, 5)

		for i, snap := range sendSnaps {
			require.Equal(t, testHTTPZPool+"/"+datasetName(testFilesystem, true)+"@"+snap, snaps[i].Name)
		}
	})
}

func TestRunner_sendPartialSnapshots(t *testing.T) {
	sendTest(t, func(server *httptest.Server, runner *Runner) {
		ds, err := zfs.GetDataset(context.Background(), testFilesystem+"@"+sendSnaps[0])
		require.NoError(t, err)

		pipeRdr, pipeWrtr := io.Pipe()
		go func() {
			err = ds.SendSnapshot(context.Background(), pipeWrtr, zfs.SendOptions{IncludeProperties: true})
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		_, err = zfs.ReceiveSnapshot(context.Background(), pipeRdr, testHTTPZPool+"/"+datasetName(ds.Name, false), zfs.ReceiveOptions{
			Properties: map[string]string{zfs.PropertyCanMount: zfs.PropertyOff},
		})
		require.NoError(t, err)

		verifyArgs := func(i int, args []interface{}) {
			require.Len(t, args, 4)
			require.Equal(t, testFilesystem+"@"+sendSnaps[i+1], args[0])
			require.Equal(t, server.URL, args[1])
			require.Equal(t, datasetName(testFilesystem, true), args[2])
			require.Equal(t, sendSnaps[i+1], args[3])
		}

		sendingCount := 0
		runner.AddListener(SendingSnapshotEvent, func(arguments ...interface{}) {
			verifyArgs(sendingCount, arguments)
			sendingCount++
		})

		sentCount := 0
		runner.AddListener(SentSnapshotEvent, func(arguments ...interface{}) {
			verifyArgs(sentCount, arguments)
			sentCount++
		})

		err = runner.sendSnapshots(1)
		require.NoError(t, err)

		require.Equal(t, 4, sendingCount)
		require.Equal(t, 4, sentCount)

		snaps, err := zfs.ListSnapshots(context.Background(), zfs.ListOptions{
			ParentDataset: testHTTPZPool + "/" + datasetName(testFilesystem, true),
		})
		require.NoError(t, err)
		require.Len(t, snaps, 5)

		for i, snap := range sendSnaps {
			require.Equal(t, testHTTPZPool+"/"+datasetName(testFilesystem, true)+"@"+snap, snaps[i].Name)
		}
	})
}

func TestRunner_sendWithMissingSnapshots(t *testing.T) {
	sendTest(t, func(server *httptest.Server, runner *Runner) {
		ds, err := zfs.GetDataset(context.Background(), testFilesystem+"@"+sendSnaps[2])
		require.NoError(t, err)

		pipeRdr, pipeWrtr := io.Pipe()
		go func() {
			err = ds.SendSnapshot(context.Background(), pipeWrtr, zfs.SendOptions{IncludeProperties: true})
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		_, err = zfs.ReceiveSnapshot(context.Background(), pipeRdr, testHTTPZPool+"/"+datasetName(ds.Name, false), zfs.ReceiveOptions{
			Properties: map[string]string{zfs.PropertyCanMount: zfs.PropertyOff},
		})
		require.NoError(t, err)

		verifyArgs := func(i int, args []interface{}) {
			require.Len(t, args, 4)
			require.Equal(t, testFilesystem+"@"+sendSnaps[i+3], args[0])
			require.Equal(t, server.URL, args[1])
			require.Equal(t, datasetName(testFilesystem, true), args[2])
			require.Equal(t, sendSnaps[i+3], args[3])
		}

		sendingCount := 0
		runner.AddListener(SendingSnapshotEvent, func(arguments ...interface{}) {
			verifyArgs(sendingCount, arguments)
			sendingCount++
		})

		sentCount := 0
		runner.AddListener(SentSnapshotEvent, func(arguments ...interface{}) {
			verifyArgs(sentCount, arguments)
			sentCount++
		})

		err = runner.sendSnapshots(1)
		require.NoError(t, err)

		require.Equal(t, 2, sendingCount)
		require.Equal(t, 2, sentCount)

		snaps, err := zfs.ListSnapshots(context.Background(), zfs.ListOptions{
			ParentDataset: testHTTPZPool + "/" + datasetName(testFilesystem, true),
		})
		require.NoError(t, err)
		require.Len(t, snaps, 3)

		require.Equal(t, testHTTPZPool+"/"+datasetName(testFilesystem, true)+"@snap3", snaps[0].Name)
		require.Equal(t, testHTTPZPool+"/"+datasetName(testFilesystem, true)+"@snap4", snaps[1].Name)
		require.Equal(t, testHTTPZPool+"/"+datasetName(testFilesystem, true)+"@snap5", snaps[2].Name)
	})
}

func TestRunner_sendNoCommonSnapshots(t *testing.T) {
	sendTest(t, func(server *httptest.Server, runner *Runner) {
		ds, err := zfs.GetDataset(context.Background(), testFilesystem+"@"+sendSnaps[2])
		require.NoError(t, err)

		pipeRdr, pipeWrtr := io.Pipe()
		go func() {
			err = ds.SendSnapshot(context.Background(), pipeWrtr, zfs.SendOptions{IncludeProperties: true})
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		ds, err = zfs.ReceiveSnapshot(context.Background(), pipeRdr, testHTTPZPool+"/"+datasetName(ds.Name, true), zfs.ReceiveOptions{
			Properties: map[string]string{zfs.PropertyCanMount: zfs.PropertyOff},
		})
		require.NoError(t, err)

		snap, err := zfs.GetDataset(context.Background(), testHTTPZPool+"/"+datasetName(ds.Name, true)+"@"+sendSnaps[2])
		require.NoError(t, err)

		err = snap.Destroy(context.Background(), zfs.DestroyOptions{})
		require.NoError(t, err)

		_, err = ds.Snapshot(context.Background(), "blaat", zfs.SnapshotOptions{})
		require.NoError(t, err)

		dataset, err := zfs.GetDataset(context.Background(), testFilesystem, runner.config.Properties.snapshotSendTo())
		require.NoError(t, err)

		err = runner.sendDatasetSnapshots(1, dataset)
		require.ErrorIs(t, err, ErrNoCommonSnapshots)
	})
}
