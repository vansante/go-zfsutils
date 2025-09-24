package job

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	zfs "github.com/vansante/go-zfsutils"
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

		ds, err := zfs.GetDataset(t.Context(), testFilesystem)
		require.NoError(t, err)

		err = ds.SetProperty(t.Context(), sendToProp, url)
		require.NoError(t, err)

		snapshotTm := time.Now().Add(-time.Minute)
		for i, snap := range sendSnaps {
			snapshot, err := ds.Snapshot(t.Context(), snap, zfs.SnapshotOptions{})
			require.NoError(t, err)

			createdTm := snapshotTm.Add(time.Second * time.Duration(i))
			err = snapshot.SetProperty(t.Context(), createdProp, createdTm.Format(dateTimeFormat))
			require.NoError(t, err)
		}

		fn(url, runner)
	})
}

func testSendSnapshots(t *testing.T, url string, runner *Runner) {
	verifyArgs := func(sent bool, i int, args []interface{}) {
		require.Equal(t, testFilesystem+"@"+sendSnaps[i], args[0])
		require.Equal(t, url, args[1])
		if sent {
			require.NotZero(t, args[2], "bytes sent should not be zero")
			require.NotZero(t, args[3], "time taken should not be zero")
			require.Len(t, args, 4)
			t.Logf("sent %d bytes in %s", args[2], args[3].(time.Duration).String())
		} else {
			require.Len(t, args, 2)
		}
	}

	wg := sync.WaitGroup{}
	sendingCount := 0
	runner.AddListener(StartSendingSnapshotEvent, func(arguments ...interface{}) {
		verifyArgs(false, sendingCount, arguments)

		wg.Add(1)
		go func() {
			defer wg.Done()

			ds, err := zfs.GetDataset(t.Context(), testFilesystem, runner.config.Properties.snapshotSending())
			require.NoError(t, err)
			require.Equal(t, sendSnaps[sendingCount], ds.ExtraProps[runner.config.Properties.snapshotSending()])

			sends := runner.ListCurrentSends()
			found := false
			for _, send := range sends {
				if send.Dataset() == testFilesystem+"@"+sendSnaps[sendingCount] {
					found = true
					require.Equal(t, arguments[1], send.Server())
					require.NotNil(t, send.CancelSend)

					t.Logf("Found sending struct: %#v", send)
				}
			}
			require.True(t, found)

			sendingCount++
		}()
	})

	sentCount := 0
	runner.AddListener(SentSnapshotEvent, func(arguments ...interface{}) {
		verifyArgs(true, sentCount, arguments)
		sentCount++
	})

	err := runner.sendSnapshots(1)
	require.NoError(t, err)

	wg.Wait()

	require.Equal(t, 5, sendingCount)
	require.Equal(t, 5, sentCount)

	require.Empty(t, runner.ListCurrentSends())

	snaps, err := zfs.ListSnapshots(t.Context(), zfs.ListOptions{
		ParentDataset:   testHTTPZPool + "/" + datasetName(testFilesystem, true),
		ExtraProperties: []string{runner.config.Properties.snapshotCreatedAt()},
	})
	require.NoError(t, err)
	require.Len(t, snaps, 5)

	for i, snap := range sendSnaps {
		require.Equal(t, testHTTPZPool+"/"+datasetName(testFilesystem, true)+"@"+snap, snaps[i].Name)
		require.NotEmpty(t, snaps[i].ExtraProps[runner.config.Properties.snapshotCreatedAt()])
	}
}

func TestRunner_sendSnapshots(t *testing.T) {
	sendTest(t, func(url string, runner *Runner) {
		testSendSnapshots(t, url, runner)
	})
}

func TestRunner_sendSnapshotsSnapshotProps(t *testing.T) {
	const testProp = "nl.vansante:haha"
	const propVal = "hihi"

	const copyProp = "nl.vansante:copythis"
	const copyVal = "yessir"

	sendTest(t, func(url string, runner *Runner) {
		runner.config.SendSetSnapshotProperties = map[string]string{
			testProp: propVal,
		}
		runner.config.SendCopySnapshotProperties = []string{copyProp, runner.config.Properties.snapshotCreatedAt()}
		//runner.config.SendSetProperties = nil
		runner.config.SendCopyProperties = nil

		snap, err := zfs.GetDataset(t.Context(), testFilesystem+"@"+sendSnaps[0])
		require.NoError(t, err)
		err = snap.SetProperty(t.Context(), copyProp, copyVal)
		require.NoError(t, err)

		testSendSnapshots(t, url, runner)

		snap, err = zfs.GetDataset(t.Context(), testHTTPZPool+"/"+datasetName(testFilesystem, true)+"@"+sendSnaps[0], testProp, copyProp)
		require.NoError(t, err)
		require.Equal(t, propVal, snap.ExtraProps[testProp])
		require.Equal(t, copyVal, snap.ExtraProps[copyProp])
	})
}

func TestRunner_sendSnapshotsWithSpeedAndCompression(t *testing.T) {
	sendTest(t, func(url string, runner *Runner) {
		runner.config.SendSpeedBytesPerSecond = 10_000
		runner.config.SendCompressionLevel = zstd.SpeedBetterCompression
		testSendSnapshots(t, url, runner)
	})
}

func TestRunner_sendCancelSnapshots(t *testing.T) {
	sendTest(t, func(url string, runner *Runner) {
		runner.AddListener(StartSendingSnapshotEvent, func(arguments ...interface{}) {
			sends := runner.ListCurrentSends()
			require.Len(t, sends, 1)

			// Cancel!
			sends[0].CancelSend()
		})

		gotErr := false
		runner.AddListener(SendSnapshotErrorEvent, func(args ...interface{}) {
			require.Len(t, args, 3)

			require.Equal(t, testFilesystem+"@"+sendSnaps[0], args[0])
			require.Equal(t, url, args[1])
			require.Error(t, args[2].(error))

			t.Logf("got error: %#v", args[2])
			gotErr = true
		})

		err := runner.sendSnapshots(1)
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, gotErr)
	})
}

func TestRunner_sendPartialSnapshots(t *testing.T) {
	sendTest(t, func(url string, runner *Runner) {
		ds, err := zfs.GetDataset(t.Context(), testFilesystem+"@"+sendSnaps[0])
		require.NoError(t, err)

		pipeRdr, pipeWrtr := io.Pipe()
		go func() {
			err = ds.SendSnapshot(t.Context(), pipeWrtr, zfs.SendOptions{IncludeProperties: true})
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		_, err = zfs.ReceiveSnapshot(t.Context(), pipeRdr, testHTTPZPool+"/"+datasetName(ds.Name, false), zfs.ReceiveOptions{
			Properties: map[string]string{zfs.PropertyCanMount: zfs.ValueOff},
		})
		require.NoError(t, err)

		verifyArgs := func(sent bool, i int, args []interface{}) {
			require.Equal(t, testFilesystem+"@"+sendSnaps[i+1], args[0])
			require.Equal(t, url, args[1])
			if sent {
				require.NotZero(t, args[2], "bytes sent should not be zero")
				require.NotZero(t, args[3], "time taken should not be zero")
				require.Len(t, args, 4)
				t.Logf("sent %d bytes in %s", args[2], args[3].(time.Duration).String())
			} else {
				require.Len(t, args, 2)
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

		err = runner.sendDatasetSnapshotsByName(1, testFilesystem)
		require.NoError(t, err)

		require.Equal(t, 4, sendingCount)
		require.Equal(t, 4, sentCount)

		snaps, err := zfs.ListSnapshots(t.Context(), zfs.ListOptions{
			ParentDataset: testHTTPZPool + "/" + datasetName(testFilesystem, true),
		})
		require.NoError(t, err)
		require.Len(t, snaps, 5)

		for i, snap := range sendSnaps {
			require.Equal(t, testHTTPZPool+"/"+datasetName(testFilesystem, true)+"@"+snap, snaps[i].Name)
		}
	})
}

func TestRunner_sendResumeSnapshot(t *testing.T) {
	sendTest(t, func(url string, runner *Runner) {
		// Setup by doing an interrupted snapshot send
		snap, err := zfs.GetDataset(t.Context(), testFilesystem+"@"+sendSnaps[0])
		require.NoError(t, err)

		pipeRdr, pipeWrtr := io.Pipe()
		go func() {
			err := snap.SendSnapshot(t.Context(), pipeWrtr, zfs.SendOptions{})
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		_, err = zfs.ReceiveSnapshot(
			t.Context(),
			io.LimitReader(pipeRdr, 10*1024),
			testHTTPZPool+"/"+datasetName(snap.Name, false),
			zfs.ReceiveOptions{
				Resumable:  true,
				Properties: map[string]string{zfs.PropertyCanMount: zfs.ValueOff},
			},
		)
		require.Error(t, err)
		var zfsErr *zfs.ResumableStreamError
		require.True(t, errors.As(err, &zfsErr))
		require.NotEmpty(t, zfsErr.ResumeToken(), zfsErr)

		ds, err := zfs.GetDataset(t.Context(), testFilesystem)
		require.NoError(t, err)
		// Manually set the sending property (this will be done by every normal send)
		require.NoError(t, ds.SetProperty(t.Context(), runner.config.Properties.snapshotSending(), sendSnaps[0]))

		// Now start the test by seeing if it resumes
		verifyArgs := func(sent bool, i int, args []interface{}) {
			require.Equal(t, testFilesystem+"@"+sendSnaps[i], args[0])
			require.Equal(t, url, args[1])
			if sent {
				require.NotZero(t, args[2], "bytes sent should not be zero")
				require.NotZero(t, args[3], "time taken should not be zero")
				require.Len(t, args, 4)
				t.Logf("sent %d bytes in %s", args[2], args[3].(time.Duration).String())
			} else {
				require.Len(t, args, 2)
			}
		}

		resumeCount := 0
		runner.AddListener(ResumeSendingSnapshotEvent, func(args ...interface{}) {
			require.Equal(t, testFilesystem+"@"+sendSnaps[0], args[0])
			require.Equal(t, url, args[1])
			require.NotZero(t, args[2])
			require.Len(t, args, 3)
			resumeCount++
			t.Logf("Resuming snapshot %s", args[0])
		})

		sendingCount := 1
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

		// ZFSSending again, because on resume the function stops.
		err = runner.sendSnapshots(1)
		require.NoError(t, err)

		require.Equal(t, 1, resumeCount)
		require.Equal(t, 5, sendingCount)
		require.Equal(t, 5, sentCount)

		snaps, err := zfs.ListSnapshots(t.Context(), zfs.ListOptions{
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
		ds, err := zfs.GetDataset(t.Context(), testFilesystem+"@"+sendSnaps[2])
		require.NoError(t, err)

		pipeRdr, pipeWrtr := io.Pipe()
		go func() {
			err = ds.SendSnapshot(t.Context(), pipeWrtr, zfs.SendOptions{IncludeProperties: true})
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		_, err = zfs.ReceiveSnapshot(t.Context(), pipeRdr, testHTTPZPool+"/"+datasetName(ds.Name, false), zfs.ReceiveOptions{
			Properties: map[string]string{zfs.PropertyCanMount: zfs.ValueOff},
		})
		require.NoError(t, err)

		verifyArgs := func(sent bool, i int, args []interface{}) {
			require.Equal(t, testFilesystem+"@"+sendSnaps[i+3], args[0])
			require.Equal(t, url, args[1])
			if sent {
				require.NotZero(t, args[2])
				require.NotZero(t, args[3])
				require.Len(t, args, 4)
				t.Logf("sent %d bytes in %s", args[2], args[3].(time.Duration).String())
			} else {
				require.Len(t, args, 2)
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

		snaps, err := zfs.ListSnapshots(t.Context(), zfs.ListOptions{
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
		ds, err := zfs.GetDataset(t.Context(), testFilesystem+"@"+sendSnaps[2])
		require.NoError(t, err)

		pipeRdr, pipeWrtr := io.Pipe()
		go func() {
			err = ds.SendSnapshot(t.Context(), pipeWrtr, zfs.SendOptions{IncludeProperties: true})
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		ds, err = zfs.ReceiveSnapshot(t.Context(), pipeRdr, testHTTPZPool+"/"+datasetName(ds.Name, true), zfs.ReceiveOptions{
			Properties: map[string]string{zfs.PropertyCanMount: zfs.ValueOff},
		})
		require.NoError(t, err)

		snap, err := zfs.GetDataset(t.Context(), testHTTPZPool+"/"+datasetName(ds.Name, true)+"@"+sendSnaps[2])
		require.NoError(t, err)

		err = snap.Destroy(t.Context(), zfs.DestroyOptions{})
		require.NoError(t, err)

		_, err = ds.Snapshot(t.Context(), "blaat", zfs.SnapshotOptions{})
		require.NoError(t, err)

		dataset, err := zfs.GetDataset(t.Context(), testFilesystem, runner.config.Properties.snapshotSendTo())
		require.NoError(t, err)

		err = runner.sendDatasetSnapshots(dataset)
		require.ErrorIs(t, err, ErrNoCommonSnapshots)
	})
}

func TestRunner_reconcileSnapshots(t *testing.T) {
	list := []zfs.Dataset{
		{
			Name: "parent/test@1234567",
		},
	}
	runnerTest(t, func(url string, runner *Runner) {
		toSend, err := runner.reconcileSnapshots(list, list, url)
		require.NoError(t, err)
		require.Empty(t, toSend)
	})
}

func TestRunner_sendRoot(t *testing.T) {
	runnerTest(t, func(url string, runner *Runner) {
		datasets, err := zfs.ListDatasets(t.Context(), zfs.ListOptions{
			ParentDataset: testZPool,
			DatasetType:   testFilesystem,
		})
		require.NoError(t, err)

		for _, dataset := range datasets {
			require.NoError(t, dataset.Destroy(t.Context(), zfs.DestroyOptions{Recursive: true}))
		}

		root, err := zfs.GetDataset(t.Context(), testZPool)
		require.NoError(t, err)
		require.NoError(t, root.SetProperty(t.Context(), runner.config.Properties.snapshotSendTo(), url))

		snap, err := root.Snapshot(t.Context(), "hithere", zfs.SnapshotOptions{})
		require.NoError(t, err)

		err = runner.sendDatasetSnapshots(root)
		require.NoError(t, err)

		zfs.ListDatasets(t.Context(), zfs.ListOptions{
			ParentDataset:   testHTTPZPool,
			DatasetType:     zfs.DatasetAll,
			ExtraProperties: nil,
			Recursive:       false,
			Depth:           0,
			FilterSelf:      false,
		})
	})
}
