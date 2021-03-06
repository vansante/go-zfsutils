package http

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vansante/go-zfs"
)

func clientTest(t *testing.T, fn func(client *Client)) {
	t.Helper()
	TestHTTPZPool(testZPool, testAuthToken, testFilesystem, zfs.NewTestLogger(t), func(server *httptest.Server) {
		c := NewClient(server.URL, testAuthToken, zfs.NewTestLogger(t))

		fn(c)
	})
}

func TestClient_Send(t *testing.T) {
	clientTest(t, func(client *Client) {
		const fsName = testZPool + "/" + testFilesystemName
		ds, err := zfs.GetDataset(context.Background(), fsName)
		require.NoError(t, err)

		snap1, err := ds.Snapshot(context.Background(), "lala1", false)
		require.NoError(t, err)

		snap2, err := ds.Snapshot(context.Background(), "lala2", false)
		require.NoError(t, err)

		const newFs = "testest"
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		err = client.Send(ctx, SnapshotSend{
			DatasetName: newFs,
			Snapshot:    snap1,
			Properties:  ReceiveProperties{zfs.PropertyCanMount: zfs.PropertyOff},
		})
		require.NoError(t, err)

		err = client.Send(ctx, SnapshotSend{
			DatasetName: newFs,
			Snapshot:    snap2,
			Properties:  ReceiveProperties{zfs.PropertyCanMount: zfs.PropertyOff},
			SendOptions: zfs.SendOptions{
				Raw:               true,
				IncludeProperties: true,
				IncrementalBase:   snap1,
			},
		})
		require.NoError(t, err)

		const fullNewFs = testZPool + "/" + newFs
		ds, err = zfs.GetDataset(context.Background(), fullNewFs)
		require.NoError(t, err)

		snaps, err := ds.Snapshots(context.Background())
		require.NoError(t, err)
		require.Len(t, snaps, 2)
		require.Equal(t, fullNewFs+"@lala1", snaps[0].Name)
		require.Equal(t, fullNewFs+"@lala2", snaps[1].Name)
	})
}
