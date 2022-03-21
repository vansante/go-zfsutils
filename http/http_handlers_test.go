package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/vansante/go-zfs"

	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const (
	testZPool          = "go-test-zpool-http"
	testToken          = "blaat"
	testFilesystemName = "filesys1"
	testFilesystem     = testZPool + "/" + testFilesystemName
)

func httpTest(t *testing.T, fn func(server *httptest.Server)) {
	t.Helper()
	zfs.TestZPool(testZPool, func() {
		rtr := httprouter.New()
		h := HTTP{
			router: rtr,
			config: Config{
				ParentDataset:        testZPool,
				AllowDestroy:         true,
				AuthenticationTokens: []string{testToken},
			},
			logger: logrus.WithField("test", "test"),
			ctx:    context.Background(),
		}
		h.registerRoutes()

		ds, err := zfs.CreateFilesystem(testFilesystem, map[string]string{zfs.PropertyCanMount: zfs.PropertyOff}, nil)
		require.NoError(t, err)
		require.Equal(t, testFilesystem, ds.Name)

		server := httptest.NewServer(rtr)
		fn(server)
	})
}

func TestHTTP_handleListFilesystems(t *testing.T) {
	httpTest(t, func(server *httptest.Server) {
		resp, err := http.Get(fmt.Sprintf("%s/filesystems?%s=%s", server.URL, authenticationTokenGETParam, testToken))
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		var list []zfs.Dataset
		err = json.NewDecoder(resp.Body).Decode(&list)
		require.NoError(t, err)
		require.Len(t, list, 2)
		require.Equal(t, testZPool, list[0].Name)
		require.Equal(t, testFilesystem, list[1].Name)
	})
}

func TestHTTP_handleSetFilesystemProps(t *testing.T) {
	httpTest(t, func(server *httptest.Server) {
		props := SetProperties{
			Set: map[string]string{"nl.test:blaat": "disk"},
		}
		data, err := json.Marshal(&props)
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodPatch, fmt.Sprintf("%s/filesystems/%s?%s=%s&%s=%s",
			server.URL, testFilesystemName,
			authenticationTokenGETParam, testToken,
			GETParamExtraProperties, "nl.test:blaat,refquota",
		), bytes.NewBuffer(data))

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		var ds zfs.Dataset
		err = json.NewDecoder(resp.Body).Decode(&ds)
		require.NoError(t, err)
		require.Equal(t, testFilesystem, ds.Name)
		require.Len(t, ds.ExtraProps, 2)
		require.Equal(t, map[string]string{"nl.test:blaat": "disk", "refquota": "0"}, ds.ExtraProps)
	})
}

func TestHTTP_handleMakeSnapshot(t *testing.T) {
	httpTest(t, func(server *httptest.Server) {
		const snapName = "snappie"

		req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/filesystems/%s/snapshots/%s?%s=%s",
			server.URL, testFilesystemName,
			snapName,
			authenticationTokenGETParam, testToken,
		), nil)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusCreated, resp.StatusCode)

		var ds zfs.Dataset
		err = json.NewDecoder(resp.Body).Decode(&ds)
		require.NoError(t, err)
		name := fmt.Sprintf("%s/%s@%s", testZPool, testFilesystemName, snapName)
		require.Equal(t, name, ds.Name)

		snaps, err := zfs.Snapshots(fmt.Sprintf("%s/%s", testZPool, testFilesystemName), nil)
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		require.Equal(t, name, snaps[0].Name)
	})
}

func TestHTTP_handleGetSnapshot(t *testing.T) {
	httpTest(t, func(server *httptest.Server) {
		const snapName = "snappie"

		ds, err := zfs.GetDataset(testFilesystem, nil)
		require.NoError(t, err)
		_, err = ds.Snapshot(snapName, false)
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/filesystems/%s/snapshots/%s?%s=%s",
			server.URL, testFilesystemName,
			snapName,
			authenticationTokenGETParam, testToken,
		), nil)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		testName := fmt.Sprintf("%s/%s", testZPool, "receive")
		ds, err = zfs.ReceiveSnapshot(resp.Body, testName, false, map[string]string{zfs.PropertyCanMount: zfs.PropertyOff})
		require.NoError(t, err)
		require.Equal(t, testName, ds.Name)

		snaps, err := zfs.Snapshots(testName, nil)
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		require.Equal(t, fmt.Sprintf("%s/%s@%s", testZPool, "receive", snapName), snaps[0].Name)
	})
}

func TestHTTP_handleGetSnapshotIncremental(t *testing.T) {
	httpTest(t, func(server *httptest.Server) {
		const snapName1 = "snappie1"
		const snapName2 = "snappie2"

		ds, err := zfs.GetDataset(testFilesystem, nil)
		require.NoError(t, err)
		snap1, err := ds.Snapshot(snapName1, false)
		require.NoError(t, err)
		_, err = ds.Snapshot(snapName2, false)
		require.NoError(t, err)

		// setup the first snapshot without http
		const newFilesys = testZPool + "/inctest"
		pipeRdr, pipeWrtr := io.Pipe()
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			_, err = zfs.ReceiveSnapshot(pipeRdr, newFilesys, false, map[string]string{zfs.PropertyCanMount: zfs.PropertyOff})
			require.NoError(t, err)
			wg.Done()
		}()
		err = snap1.SendSnapshot(pipeWrtr, true)
		require.NoError(t, err)
		require.NoError(t, pipeWrtr.Close())
		wg.Wait()

		// Begin the actual test here.
		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/filesystems/%s/snapshots/%s/incremental/%s?%s=%s",
			server.URL, testFilesystemName,
			snapName1, snapName2,
			authenticationTokenGETParam, testToken,
		), nil)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		ds, err = zfs.ReceiveSnapshot(resp.Body, newFilesys, false, map[string]string{zfs.PropertyCanMount: zfs.PropertyOff})
		require.NoError(t, err)
		require.Equal(t, newFilesys, ds.Name)

		snaps, err := zfs.Snapshots(newFilesys, nil)
		require.NoError(t, err)
		require.Len(t, snaps, 2)
		require.Equal(t, fmt.Sprintf("%s@%s", newFilesys, snapName1), snaps[0].Name)
		require.Equal(t, fmt.Sprintf("%s@%s", newFilesys, snapName2), snaps[1].Name)
	})
}

func TestHTTP_handleResumeGetSnapshot(t *testing.T) {
	httpTest(t, func(server *httptest.Server) {
		const snapName = "snappie"

		ds, err := zfs.GetDataset(testFilesystem, nil)
		require.NoError(t, err)
		_, err = ds.Snapshot(snapName, false)
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/filesystems/%s/snapshots/%s?%s=%s",
			server.URL, testFilesystemName,
			snapName,
			authenticationTokenGETParam, testToken,
		), nil)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		testName := fmt.Sprintf("%s/%s", testZPool, "receive")
		ds, err = zfs.ReceiveSnapshot(io.LimitReader(resp.Body, 29_636), testName, true, map[string]string{zfs.PropertyCanMount: zfs.PropertyOff})
		require.Error(t, err)
		var recvErr *zfs.Error
		require.True(t, errors.As(err, &recvErr))
		require.True(t, recvErr.Resumable(), recvErr)

		fs, err := zfs.Filesystems(testName, []string{zfs.PropertyReceiveResumeToken})
		require.NoError(t, err)
		require.Len(t, fs, 1)
		require.Equal(t, testName, fs[0].Name)
		require.True(t, len(fs[0].ExtraProps[zfs.PropertyReceiveResumeToken]) > 32)

		// Now do a resumption on this stream
		req, err = http.NewRequest(http.MethodGet, fmt.Sprintf("%s/snapshot/resume/%s?%s=%s",
			server.URL, fs[0].ExtraProps[zfs.PropertyReceiveResumeToken],
			authenticationTokenGETParam, testToken,
		), nil)
		require.NoError(t, err)

		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		ds, err = zfs.ReceiveSnapshot(resp.Body, testName, true, map[string]string{zfs.PropertyCanMount: zfs.PropertyOff})
		require.NoError(t, err)

		snaps, err := zfs.Snapshots(testName, nil)
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		require.Equal(t, fmt.Sprintf("%s/%s@%s", testZPool, "receive", snapName), snaps[0].Name)
	})
}

func TestHTTP_handleReceiveSnapshot(t *testing.T) {
	httpTest(t, func(server *httptest.Server) {
		const snapName = "send"

		pipeRdr, pipeWrtr := io.Pipe()

		const newFilesystem = "bla"
		const newSnap = "recv"
		req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/filesystems/%s/snapshots/%s?%s=%s&%s=%s",
			server.URL, newFilesystem,
			newSnap,
			authenticationTokenGETParam, testToken,
			GETParamReceiveProperties, ReceiveProperties{zfs.PropertyCanMount: zfs.PropertyOff}.Encode(),
		), pipeRdr)
		require.NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.EqualValues(t, http.StatusCreated, resp.StatusCode)

			ds := zfs.Dataset{}
			err = json.NewDecoder(resp.Body).Decode(&ds)
			require.NoError(t, err)
			name := fmt.Sprintf("%s/%s@%s", testZPool, newFilesystem, newSnap)
			require.Equal(t, name, ds.Name)

			snaps, err := zfs.Snapshots(fmt.Sprintf("%s/%s", testZPool, newFilesystem), nil)
			require.NoError(t, err)
			require.Len(t, snaps, 1)
			require.Equal(t, name, snaps[0].Name)

			wg.Done()
		}()

		ds, err := zfs.GetDataset(testFilesystem, nil)
		require.NoError(t, err)
		ds, err = ds.Snapshot(snapName, false)
		require.NoError(t, err)
		err = ds.SendSnapshot(pipeWrtr, true)
		require.NoError(t, err)
		require.NoError(t, pipeWrtr.Close())

		wg.Wait()
	})
}

func TestHTTP_handleReceiveSnapshotResume(t *testing.T) {
	httpTest(t, func(server *httptest.Server) {
		const snapName = "send"

		ds, err := zfs.GetDataset(testFilesystem, nil)
		require.NoError(t, err)
		toBeSent, err := ds.Snapshot(snapName, false)
		require.NoError(t, err)

		const newFilesystem = "bla"
		const newSnap = "recv"

		newFullSnap := fmt.Sprintf("%s/%s@%s", testZPool, newFilesystem, newSnap)
		pipeRdr, pipeWrtr := io.Pipe()

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			_, err := zfs.ReceiveSnapshot(io.LimitReader(pipeRdr, 28_725),
				newFullSnap,
				true, map[string]string{
					zfs.PropertyCanMount: zfs.PropertyOff,
				},
			)
			require.Error(t, err)
			wg.Done()
		}()

		go func() {
			time.Sleep(time.Second / 5)
			require.NoError(t, pipeWrtr.Close())
			wg.Done()
		}()

		err = toBeSent.SendSnapshot(pipeWrtr, true)
		require.Error(t, err)
		wg.Wait()

		ds, err = zfs.GetDataset(fmt.Sprintf("%s/%s", testZPool, newFilesystem), []string{zfs.PropertyReceiveResumeToken})
		require.NoError(t, err)
		require.NotEmpty(t, ds.ExtraProps[zfs.PropertyReceiveResumeToken])
		require.True(t, len(ds.ExtraProps[zfs.PropertyReceiveResumeToken]) > 100)

		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/filesystems/%s/resume-token?%s=%s",
			server.URL, newFilesystem,
			authenticationTokenGETParam, testToken,
		), nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
		token := resp.Header.Get(HeaderResumeReceiveToken)
		require.True(t, len(token) > 100)

		t.Logf("Got resume token: %s", ds.ExtraProps[zfs.PropertyReceiveResumeToken])

		pipeRdr, pipeWrtr = io.Pipe()

		// Now do a resume HTTP receive request
		req, err = http.NewRequest(http.MethodPut, fmt.Sprintf("%s/filesystems/%s/snapshots/%s?%s=%s&%s=%s&%s=%s",
			server.URL, newFilesystem,
			newSnap,
			authenticationTokenGETParam, testToken,
			GETParamResumable, "true",
			GETParamReceiveProperties, ReceiveProperties{zfs.PropertyCanMount: zfs.PropertyOff}.Encode(),
		), pipeRdr)
		req.Header.Set(HeaderResumeReceiveToken, token)
		require.NoError(t, err)

		wg = sync.WaitGroup{}
		wg.Add(1)
		go func() {
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.EqualValues(t, http.StatusCreated, resp.StatusCode)

			ds := zfs.Dataset{}
			err = json.NewDecoder(resp.Body).Decode(&ds)
			require.NoError(t, err)
			name := fmt.Sprintf("%s/%s@%s", testZPool, newFilesystem, newSnap)
			require.Equal(t, name, ds.Name)

			snaps, err := zfs.Snapshots(fmt.Sprintf("%s/%s", testZPool, newFilesystem), nil)
			require.NoError(t, err)
			require.Len(t, snaps, 1)
			require.Equal(t, name, snaps[0].Name)

			wg.Done()
		}()

		err = zfs.ResumeSend(pipeWrtr, token)
		require.NoError(t, err)
		require.NoError(t, pipeWrtr.Close())

		wg.Wait()

		ds, err = zfs.GetDataset(newFullSnap, nil)
		require.NoError(t, err)
		require.Equal(t, ds.Name, newFullSnap)
	})
}
