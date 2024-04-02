package job

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

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

func sendTest(t *testing.T, fn func(url string, runner *Runner)) {
	runnerTest(t, func(url string, runner *Runner) {
		createdProp := runner.config.Properties.snapshotCreatedAt()
		sendToProp := runner.config.Properties.snapshotSendTo()

		ds, err := zfs.GetDataset(context.Background(), testFilesystem)
		require.NoError(t, err)

		err = ds.SetProperty(context.Background(), sendToProp, url)
		require.NoError(t, err)

		snapshotTm := time.Now().Add(-time.Minute)
		for i, snap := range sendSnaps {
			snapshot, err := ds.Snapshot(context.Background(), snap, zfs.SnapshotOptions{})
			require.NoError(t, err)

			createdTm := snapshotTm.Add(time.Second * time.Duration(i))
			err = snapshot.SetProperty(context.Background(), createdProp, createdTm.Format(dateTimeFormat))
			require.NoError(t, err)
		}

		fn(url, runner)
	})
}

func testSendSnapshots(t *testing.T, url string, runner *Runner) {
	verifyArgs := func(sent bool, i int, args []interface{}) {
		require.Equal(t, testFilesystem+"@"+sendSnaps[i], args[0])
		require.Equal(t, url, args[1])
		require.Equal(t, datasetName(testFilesystem, true), args[2])
		require.Equal(t, sendSnaps[i], args[3])
		if sent {
			require.NotZero(t, args[4])
			require.NotZero(t, args[5])
			require.Len(t, args, 6)
			t.Logf("send %d bytes in %s", args[4], args[5].(time.Duration).String())
		} else {
			require.Len(t, args, 4)
		}
	}

	sendingCount := 0
	runner.AddListener(StartSendingSnapshotEvent, func(arguments ...interface{}) {
		verifyArgs(false, sendingCount, arguments)
		sendingCount++
	})

	sentCount := 0
	runner.AddListener(SentSnapshotEvent, func(arguments ...interface{}) {
		verifyArgs(true, sentCount, arguments)
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
}

func TestRunner_sendSnapshots(t *testing.T) {
	sendTest(t, func(url string, runner *Runner) {
		testSendSnapshots(t, url, runner)
	})
}

func TestRunner_sendSnapshotsWithSpeedAndCompression(t *testing.T) {
	sendTest(t, func(url string, runner *Runner) {
		runner.config.SendSpeedBytesPerSecond = 10_000
		runner.config.SendCompressionLevel = zstd.SpeedBetterCompression
		testSendSnapshots(t, url, runner)
	})
}

func TestRunner_sendPartialSnapshots(t *testing.T) {
	sendTest(t, func(url string, runner *Runner) {
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

		verifyArgs := func(sent bool, i int, args []interface{}) {
			require.Equal(t, testFilesystem+"@"+sendSnaps[i+1], args[0])
			require.Equal(t, url, args[1])
			require.Equal(t, datasetName(testFilesystem, true), args[2])
			require.Equal(t, sendSnaps[i+1], args[3])
			if sent {
				require.NotZero(t, args[4])
				require.NotZero(t, args[5])
				require.Len(t, args, 6)
				t.Logf("send %d bytes in %s", args[4], args[5].(time.Duration).String())
			} else {
				require.Len(t, args, 4)
			}
		}

		sendingCount := 0
		runner.AddListener(StartSendingSnapshotEvent, func(arguments ...interface{}) {
			verifyArgs(false, sendingCount, arguments)
			sendingCount++
			t.Logf("Sending snapshot %s", arguments[0])
		})

		sentCount := 0
		runner.AddListener(SentSnapshotEvent, func(arguments ...interface{}) {
			verifyArgs(true, sentCount, arguments)
			sentCount++
			t.Logf("Sent snapshot %s", arguments[0])
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
	sendTest(t, func(url string, runner *Runner) {
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

		verifyArgs := func(sent bool, i int, args []interface{}) {
			require.Equal(t, testFilesystem+"@"+sendSnaps[i+3], args[0])
			require.Equal(t, url, args[1])
			require.Equal(t, datasetName(testFilesystem, true), args[2])
			require.Equal(t, sendSnaps[i+3], args[3])
			if sent {
				require.NotZero(t, args[4])
				require.NotZero(t, args[5])
				require.Len(t, args, 6)
				t.Logf("send %d bytes in %s", args[4], args[5].(time.Duration).String())
			} else {
				require.Len(t, args, 4)
			}
		}

		sendingCount := 0
		runner.AddListener(StartSendingSnapshotEvent, func(arguments ...interface{}) {
			verifyArgs(false, sendingCount, arguments)
			sendingCount++
		})

		sentCount := 0
		runner.AddListener(SentSnapshotEvent, func(arguments ...interface{}) {
			verifyArgs(true, sentCount, arguments)
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
	sendTest(t, func(url string, runner *Runner) {
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
