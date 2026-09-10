package http

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	zfs "github.com/vansante/go-zfsutils"

	"github.com/stretchr/testify/require"
)

func clientTest(t *testing.T, fn func(client *Client, host string)) {
	t.Helper()
	TestHTTPZPool(testZPool, testPrefix, testFilesystem, func(server *httptest.Server) {
		c := NewClient(nil, slog.Default())
		fn(c, server.URL+testPrefix)
	})
}

func TestClient_Send(t *testing.T) {
	clientTest(t, func(client *Client, host string) {
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

		results, err := client.Send(ctx, host, SnapshotSendOptions{
			DatasetName: newFs,
			Snapshot:    snap1,
			Properties: ReceiveProperties{
				zfs.PropertyCanMount: zfs.ValueOff,
			},
		})
		require.NoError(t, err)
		require.NotZero(t, results.BytesSent)
		require.NotZero(t, results.TimeTaken)

		results, err = client.Send(ctx, host, SnapshotSendOptions{
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

		require.Equal(t, testPropVal, ds.ExtraProps[testProp])

		snaps, err := ds.Snapshots(context.Background(), zfs.ListOptions{ExtraProperties: []string{testProp}})
		require.NoError(t, err)
		require.Len(t, snaps, 2)
		require.Equal(t, fullNewFs+"@lala1", snaps[0].Name)
		require.Equal(t, fullNewFs+"@lala2", snaps[1].Name)
	})
}

func TestClient_SetFilesystemProperties(t *testing.T) {
	clientTest(t, func(client *Client, host string) {
		const testProp = "nl.vansante:pipo"
		const testPropVal = "clown"

		err := client.SetFilesystemProperties(context.Background(), host, testFilesystemName, SetProperties{
			Set: map[string]string{testProp: testPropVal},
		})
		require.NoError(t, err)

		ds, err := zfs.GetDataset(context.Background(), testFilesystem, testProp)
		require.NoError(t, err)
		require.Equal(t, testPropVal, ds.ExtraProps[testProp])

		err = client.SetFilesystemProperties(context.Background(), host, testFilesystemName, SetProperties{
			Unset: []string{testProp},
		})
		require.NoError(t, err)

		ds, err = zfs.GetDataset(context.Background(), testFilesystem, testProp)
		require.NoError(t, err)
		require.Empty(t, ds.ExtraProps[testProp])

		err = client.SetFilesystemProperties(context.Background(), host, "doesnotexist", SetProperties{
			Set: map[string]string{testProp: testPropVal},
		})
		require.Error(t, err)
	})
}

func TestClient_SetClientSetHeader(t *testing.T) {
	var gotHeader, gotUserAgent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotHeader = req.Header.Get("X-Test")
		gotUserAgent = req.UserAgent()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("[]"))
	}))
	defer server.Close()

	c := NewClient(nil, slog.Default())
	c.SetClient(server.Client())
	c.SetHeader("X-Test", "hello")

	snaps, err := c.DatasetSnapshots(t.Context(), server.URL, "testfs1", nil)
	require.NoError(t, err)
	require.Empty(t, snaps)
	require.Equal(t, "hello", gotHeader)
	require.Contains(t, gotUserAgent, "go-zfsutils@")
}
