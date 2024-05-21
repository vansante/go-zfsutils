package http

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	zfs "github.com/vansante/go-zfsutils"

	"github.com/stretchr/testify/require"
)

func clientTest(t *testing.T, fn func(client *Client)) {
	t.Helper()
	TestHTTPZPool(testZPool, testPrefix, testFilesystem, func(server *httptest.Server) {
		c := NewClient(server.URL+testPrefix, slog.Default())
		fn(c)
	})
}

func TestClient_Send(t *testing.T) {
	clientTest(t, func(client *Client) {
		const fsName = testZPool + "/" + testFilesystemName
		ds, err := zfs.GetDataset(context.Background(), fsName)
		require.NoError(t, err)

		snap1, err := ds.Snapshot(context.Background(), "lala1", zfs.SnapshotOptions{})
		require.NoError(t, err)

		snap2, err := ds.Snapshot(context.Background(), "lala2", zfs.SnapshotOptions{})
		require.NoError(t, err)

		const newFs = "testest"
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		const testProp = "nl.vansante:pipo"
		const testPropVal = "clown"

		results, err := client.Send(ctx, SnapshotSendOptions{
			DatasetName: newFs,
			Snapshot:    snap1,
			Properties: ReceiveProperties{
				zfs.PropertyCanMount: zfs.ValueOff,
			},
		})
		require.NoError(t, err)
		require.NotZero(t, results.BytesSent)
		require.NotZero(t, results.TimeTaken)

		results, err = client.Send(ctx, SnapshotSendOptions{
			DatasetName: newFs,
			Snapshot:    snap2,
			Properties: ReceiveProperties{
				zfs.PropertyCanMount: zfs.ValueOff,
				testProp:             testPropVal,
			},
			SendOptions: zfs.SendOptions{
				Raw:               true,
				IncludeProperties: false,
				IncrementalBase:   snap1,
			},
		})
		require.NoError(t, err)
		require.NotZero(t, results.BytesSent)
		require.NotZero(t, results.TimeTaken)

		const fullNewFs = testZPool + "/" + newFs
		ds, err = zfs.GetDataset(context.Background(), fullNewFs, testProp)
		require.NoError(t, err)

		require.Equal(t, "", ds.ExtraProps[testProp])

		snaps, err := ds.Snapshots(context.Background(), zfs.ListOptions{ExtraProperties: []string{testProp}})
		require.NoError(t, err)
		require.Len(t, snaps, 2)
		require.Equal(t, fullNewFs+"@lala1", snaps[0].Name)
		require.Equal(t, fullNewFs+"@lala2", snaps[1].Name)

		require.Equal(t, "", snaps[0].ExtraProps[testProp])
		require.Equal(t, testPropVal, snaps[1].ExtraProps[testProp])
	})
}
