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

	"github.com/stretchr/testify/require"
)

const (
	testZPool          = "go-test-zpool-http"
	testAuthToken      = "blaat"
	testFilesystemName = "filesys1"
	testFilesystem     = testZPool + "/" + testFilesystemName
)

func httpHandlerTest(t *testing.T, fn func(server *httptest.Server)) {
	t.Helper()
	TestHTTPZPool(testZPool, testAuthToken, testFilesystem, zfs.NewTestLogger(t), func(server *httptest.Server) {
		fn(server)
	})
}

func TestHTTP_handleListFilesystems(t *testing.T) {
	httpHandlerTest(t, func(server *httptest.Server) {
		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/filesystems", server.URL), nil)
		require.NoError(t, err)
		req.Header.Set(HeaderAuthenticationToken, testAuthToken)

		resp, err := http.DefaultClient.Do(req)
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
	httpHandlerTest(t, func(server *httptest.Server) {
		props := SetProperties{
			Set: map[string]string{"nl.test:blaat": "disk"},
		}
		data, err := json.Marshal(&props)
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodPatch, fmt.Sprintf("%s/filesystems/%s?%s=%s",
			server.URL, testFilesystemName,
			GETParamExtraProperties, "nl.test:blaat,refquota",
		), bytes.NewBuffer(data))
		require.NoError(t, err)
		req.Header.Set(HeaderAuthenticationToken, testAuthToken)

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
	httpHandlerTest(t, func(server *httptest.Server) {
		const snapName = "snappie"

		req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/filesystems/%s/snapshots/%s",
			server.URL, testFilesystemName,
			snapName,
		), nil)
		require.NoError(t, err)
		req.Header.Set(HeaderAuthenticationToken, testAuthToken)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusCreated, resp.StatusCode)

		var ds zfs.Dataset
		err = json.NewDecoder(resp.Body).Decode(&ds)
		require.NoError(t, err)
		name := fmt.Sprintf("%s/%s@%s", testZPool, testFilesystemName, snapName)
		require.Equal(t, name, ds.Name)

		snaps, err := zfs.Snapshots(context.Background(), fmt.Sprintf("%s/%s", testZPool, testFilesystemName), nil)
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		require.Equal(t, name, snaps[0].Name)
	})
}

func TestHTTP_handleGetSnapshot(t *testing.T) {
	httpHandlerTest(t, func(server *httptest.Server) {
		const snapName = "snappie"

		ds, err := zfs.GetDataset(context.Background(), testFilesystem, nil)
		require.NoError(t, err)
		_, err = ds.Snapshot(context.Background(), snapName, false)
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/filesystems/%s/snapshots/%s",
			server.URL, testFilesystemName,
			snapName,
		), nil)
		require.NoError(t, err)
		req.Header.Set(HeaderAuthenticationToken, testAuthToken)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		testName := fmt.Sprintf("%s/%s", testZPool, "receive")
		ds, err = zfs.ReceiveSnapshot(context.Background(), resp.Body, testName, zfs.ReceiveOptions{
			Resumable:  false,
			Properties: map[string]string{zfs.PropertyCanMount: zfs.PropertyOff},
		})
		require.NoError(t, err)
		require.Equal(t, testName, ds.Name)

		snaps, err := zfs.Snapshots(context.Background(), testName, nil)
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		require.Equal(t, fmt.Sprintf("%s/%s@%s", testZPool, "receive", snapName), snaps[0].Name)
	})
}

func TestHTTP_handleGetSnapshotIncremental(t *testing.T) {
	httpHandlerTest(t, func(server *httptest.Server) {
		const snapName1 = "snappie1"
		const snapName2 = "snappie2"

		ds, err := zfs.GetDataset(context.Background(), testFilesystem, nil)
		require.NoError(t, err)
		snap1, err := ds.Snapshot(context.Background(), snapName1, false)
		require.NoError(t, err)
		_, err = ds.Snapshot(context.Background(), snapName2, false)
		require.NoError(t, err)

		// setup the first snapshot without http
		const newFilesys = testZPool + "/inctest"
		pipeRdr, pipeWrtr := io.Pipe()
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			_, err = zfs.ReceiveSnapshot(context.Background(), pipeRdr, newFilesys, zfs.ReceiveOptions{
				Resumable:  false,
				Properties: map[string]string{zfs.PropertyCanMount: zfs.PropertyOff},
			})
			require.NoError(t, err)
			wg.Done()
		}()
		err = snap1.SendSnapshot(context.Background(), pipeWrtr, zfs.SendOptions{Raw: true})
		require.NoError(t, err)
		require.NoError(t, pipeWrtr.Close())
		wg.Wait()

		// Begin the actual test here.
		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/filesystems/%s/snapshots/%s/incremental/%s",
			server.URL, testFilesystemName,
			snapName2, snapName1,
		), nil)
		require.NoError(t, err)
		req.Header.Set(HeaderAuthenticationToken, testAuthToken)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		ds, err = zfs.ReceiveSnapshot(context.Background(), resp.Body, newFilesys, zfs.ReceiveOptions{
			Resumable:  false,
			Properties: map[string]string{zfs.PropertyCanMount: zfs.PropertyOff},
		})
		require.NoError(t, err)
		require.Equal(t, newFilesys, ds.Name)

		snaps, err := zfs.Snapshots(context.Background(), newFilesys, nil)
		require.NoError(t, err)
		require.Len(t, snaps, 2)
		require.Equal(t, fmt.Sprintf("%s@%s", newFilesys, snapName1), snaps[0].Name)
		require.Equal(t, fmt.Sprintf("%s@%s", newFilesys, snapName2), snaps[1].Name)
	})
}

func TestHTTP_handleResumeGetSnapshot(t *testing.T) {
	httpHandlerTest(t, func(server *httptest.Server) {
		const snapName = "snappie"

		ds, err := zfs.GetDataset(context.Background(), testFilesystem, nil)
		require.NoError(t, err)
		_, err = ds.Snapshot(context.Background(), snapName, false)
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/filesystems/%s/snapshots/%s",
			server.URL, testFilesystemName,
			snapName,
		), nil)
		require.NoError(t, err)
		req.Header.Set(HeaderAuthenticationToken, testAuthToken)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		testName := fmt.Sprintf("%s/%s", testZPool, "receive")
		ds, err = zfs.ReceiveSnapshot(context.Background(), io.LimitReader(resp.Body, 29_636), testName, zfs.ReceiveOptions{
			Resumable:  true,
			Properties: map[string]string{zfs.PropertyCanMount: zfs.PropertyOff},
		})
		require.Error(t, err)
		var recvErr *zfs.ResumableStreamError
		require.True(t, errors.As(err, &recvErr))

		fs, err := zfs.Filesystems(context.Background(), testName, []string{zfs.PropertyReceiveResumeToken})
		require.NoError(t, err)
		require.Len(t, fs, 1)
		require.Equal(t, testName, fs[0].Name)
		require.True(t, len(fs[0].ExtraProps[zfs.PropertyReceiveResumeToken]) > 32)
		require.Equal(t, recvErr.ResumeToken(), fs[0].ExtraProps[zfs.PropertyReceiveResumeToken])

		// Now do a resumption on this stream
		req, err = http.NewRequest(http.MethodGet, fmt.Sprintf("%s/snapshot/resume/%s",
			server.URL, fs[0].ExtraProps[zfs.PropertyReceiveResumeToken],
		), nil)
		require.NoError(t, err)
		req.Header.Set(HeaderAuthenticationToken, testAuthToken)

		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.EqualValues(t, http.StatusOK, resp.StatusCode)

		ds, err = zfs.ReceiveSnapshot(context.Background(), resp.Body, testName, zfs.ReceiveOptions{
			Resumable:  true,
			Properties: map[string]string{zfs.PropertyCanMount: zfs.PropertyOff},
		})
		require.NoError(t, err)

		snaps, err := zfs.Snapshots(context.Background(), testName, nil)
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		require.Equal(t, fmt.Sprintf("%s/%s@%s", testZPool, "receive", snapName), snaps[0].Name)
	})
}

func TestHTTP_handleReceiveSnapshot(t *testing.T) {
	httpHandlerTest(t, func(server *httptest.Server) {
		const snapName = "send"
		const testProp = "nl.test:dsk"
		const testPropVal = "1234"

		pipeRdr, pipeWrtr := io.Pipe()

		const newFilesystem = "bla"
		const newSnap = "recv"
		req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/filesystems/%s/snapshots/%s?%s=%s",
			server.URL, newFilesystem,
			newSnap,
			GETParamReceiveProperties, ReceiveProperties{zfs.PropertyCanMount: zfs.PropertyOff}.Encode(),
		), pipeRdr)
		require.NoError(t, err)
		req.Header.Set(HeaderAuthenticationToken, testAuthToken)

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

			newFs, err := zfs.GetDataset(context.Background(), fmt.Sprintf("%s/%s", testZPool, newFilesystem), []string{testProp})
			require.NoError(t, err)
			require.Equal(t, newFs.ExtraProps[testProp], testPropVal)

			snaps, err := newFs.Snapshots(context.Background(), nil)
			require.NoError(t, err)
			require.Len(t, snaps, 1)
			require.Equal(t, name, snaps[0].Name)

			wg.Done()
		}()

		ds, err := zfs.GetDataset(context.Background(), testFilesystem, nil)
		require.NoError(t, err)

		err = ds.SetProperty(context.Background(), testProp, testPropVal)
		require.NoError(t, err)

		ds, err = ds.Snapshot(context.Background(), snapName, false)
		require.NoError(t, err)
		err = ds.SendSnapshot(context.Background(), pipeWrtr, zfs.SendOptions{Raw: true, IncludeProperties: true})
		require.NoError(t, err)
		require.NoError(t, pipeWrtr.Close())

		wg.Wait()
	})
}

func TestHTTP_handleReceiveSnapshotNoExplicitName(t *testing.T) {
	httpHandlerTest(t, func(server *httptest.Server) {
		const snapName = "send"

		pipeRdr, pipeWrtr := io.Pipe()

		const newFilesystem = "bla"
		req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/filesystems/%s/snapshots?%s=%s",
			server.URL, newFilesystem,
			GETParamReceiveProperties, ReceiveProperties{zfs.PropertyCanMount: zfs.PropertyOff}.Encode(),
		), pipeRdr)
		require.NoError(t, err)
		req.Header.Set(HeaderAuthenticationToken, testAuthToken)

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
			require.Equal(t, fmt.Sprintf("%s/%s", testZPool, newFilesystem), ds.Name)

			snaps, err := zfs.Snapshots(context.Background(), fmt.Sprintf("%s/%s", testZPool, newFilesystem), nil)
			require.NoError(t, err)
			require.Len(t, snaps, 1)
			require.Equal(t, fmt.Sprintf("%s/%s@%s", testZPool, newFilesystem, snapName), snaps[0].Name)

			wg.Done()
		}()

		ds, err := zfs.GetDataset(context.Background(), testFilesystem, nil)
		require.NoError(t, err)
		ds, err = ds.Snapshot(context.Background(), snapName, false)
		require.NoError(t, err)
		err = ds.SendSnapshot(context.Background(), pipeWrtr, zfs.SendOptions{Raw: true, IncludeProperties: true})
		require.NoError(t, err)
		require.NoError(t, pipeWrtr.Close())

		wg.Wait()
	})
}

func TestHTTP_handleReceiveSnapshotResume(t *testing.T) {
	httpHandlerTest(t, func(server *httptest.Server) {
		const snapName = "send"

		ds, err := zfs.GetDataset(context.Background(), testFilesystem, nil)
		require.NoError(t, err)
		toBeSent, err := ds.Snapshot(context.Background(), snapName, false)
		require.NoError(t, err)

		const newFilesystem = "bla"
		const newSnap = "recv"

		newFullSnap := fmt.Sprintf("%s/%s@%s", testZPool, newFilesystem, newSnap)
		pipeRdr, pipeWrtr := io.Pipe()

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			_, err := zfs.ReceiveSnapshot(context.Background(), io.LimitReader(pipeRdr, 28_725), newFullSnap, zfs.ReceiveOptions{
				Resumable:  true,
				Properties: map[string]string{zfs.PropertyCanMount: zfs.PropertyOff},
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

		err = toBeSent.SendSnapshot(context.Background(), pipeWrtr, zfs.SendOptions{
			Raw:               true,
			IncludeProperties: true,
		})
		require.Error(t, err)
		wg.Wait()

		ds, err = zfs.GetDataset(context.Background(), fmt.Sprintf("%s/%s", testZPool, newFilesystem), []string{zfs.PropertyReceiveResumeToken})
		require.NoError(t, err)
		require.NotEmpty(t, ds.ExtraProps[zfs.PropertyReceiveResumeToken])
		require.True(t, len(ds.ExtraProps[zfs.PropertyReceiveResumeToken]) > 100)

		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/filesystems/%s/resume-token",
			server.URL, newFilesystem,
		), nil)
		require.NoError(t, err)
		req.Header.Set(HeaderAuthenticationToken, testAuthToken)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
		token := resp.Header.Get(HeaderResumeReceiveToken)
		require.True(t, len(token) > 100)

		t.Logf("Got resume token: %s", ds.ExtraProps[zfs.PropertyReceiveResumeToken])

		pipeRdr, pipeWrtr = io.Pipe()

		// Now do a resume HTTP receive request
		req, err = http.NewRequest(http.MethodPut, fmt.Sprintf("%s/filesystems/%s/snapshots/%s?%s=%s&%s=%s",
			server.URL, newFilesystem,
			newSnap,
			GETParamResumable, "true",
			GETParamReceiveProperties, ReceiveProperties{zfs.PropertyCanMount: zfs.PropertyOff}.Encode(),
		), pipeRdr)
		require.NoError(t, err)
		req.Header.Set(HeaderAuthenticationToken, testAuthToken)
		req.Header.Set(HeaderResumeReceiveToken, token)

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

			snaps, err := zfs.Snapshots(context.Background(), fmt.Sprintf("%s/%s", testZPool, newFilesystem), nil)
			require.NoError(t, err)
			require.Len(t, snaps, 1)
			require.Equal(t, name, snaps[0].Name)

			wg.Done()
		}()

		err = zfs.ResumeSend(context.Background(), pipeWrtr, token, zfs.ResumeSendOptions{})
		require.NoError(t, err)
		require.NoError(t, pipeWrtr.Close())

		wg.Wait()

		ds, err = zfs.GetDataset(context.Background(), newFullSnap, nil)
		require.NoError(t, err)
		require.Equal(t, ds.Name, newFullSnap)
	})
}
