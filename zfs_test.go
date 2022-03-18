package zfs

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testZPool = "go-test-zpool"

func sleep(delay int) {
	time.Sleep(time.Duration(delay) * time.Second)
}

func pow2(x int) int64 {
	return int64(math.Pow(2, float64(x)))
}

func zpoolTest(t *testing.T, fn func()) {
	t.Helper()

	args := []string{
		"create", testZPool,
	}

	for i := 0; i < 3; i++ {
		f, err := ioutil.TempFile(os.TempDir(), "zfs-zpool-")
		require.NoError(t, err)
		err = f.Truncate(pow2(29))
		require.NoError(t, f.Close())
		require.NoError(t, err)

		args = append(args, f.Name())

		defer os.Remove(f.Name()) // nolint:revive // its ok to defer to end of func
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "zpool", args...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, "zpool", "destroy", testZPool)
		_, err := cmd.Output()
		require.NoError(t, err)
	}()

	fn()
}

func TestDatasets(t *testing.T) {
	t.Helper()

	zpoolTest(t, func() {
		_, err := Datasets("", nil)
		require.NoError(t, err)

		ds, err := GetDataset(testZPool, nil)
		require.NoError(t, err)
		require.Equal(t, DatasetFilesystem, ds.Type)
		require.Equal(t, "", ds.Origin)
		require.Greater(t, ds.Logicalused, uint64(0), "Logicalused is not greater than 0")
	})
}

func TestDatasetsWithProps(t *testing.T) {
	t.Helper()

	zpoolTest(t, func() {
		ds, err := GetDataset(testZPool, []string{"name", "refquota"})
		require.NoError(t, err)

		t.Log(ds.ExtraProps)
		require.Len(t, ds.ExtraProps, 2)
		require.Equal(t, ds.ExtraProps["name"], testZPool)
		require.Equal(t, ds.ExtraProps["refquota"], "0")
	})
}

func TestDatasetGetProperty(t *testing.T) {
	zpoolTest(t, func() {
		ds, err := GetDataset(testZPool, nil)
		require.NoError(t, err)

		prop, err := ds.GetProperty("readonly")
		require.NoError(t, err)
		require.Equal(t, "off", prop)

		prop, err = ds.GetProperty("compression")
		require.NoError(t, err)
		require.Equal(t, "off", prop)
	})
}

func TestDatasetSetInheritProperty(t *testing.T) {
	zpoolTest(t, func() {
		ds, err := GetDataset(testZPool, nil)
		require.NoError(t, err)

		const testProp = "nl.bla:nope"
		require.NoError(t, ds.SetProperty(testProp, "hello"))

		prop, err := ds.GetProperty(testProp)
		require.NoError(t, err)
		require.Equal(t, "hello", prop)

		require.NoError(t, ds.InheritProperty(testProp))

		prop, err = ds.GetProperty(testProp)
		require.NoError(t, err)
		require.Equal(t, "-", prop)
	})
}

func TestSnapshots(t *testing.T) {
	zpoolTest(t, func() {
		snapshots, err := Snapshots("", nil)
		require.NoError(t, err)

		for _, snapshot := range snapshots {
			require.Equal(t, DatasetSnapshot, snapshot.Type)
		}
	})
}

func TestFilesystems(t *testing.T) {
	zpoolTest(t, func() {
		f, err := CreateFilesystem(testZPool+"/filesystem-test", nil, nil)
		require.NoError(t, err)

		filesystems, err := Filesystems("", nil)
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		require.NoError(t, f.Destroy(DestroyDefault))
	})
}

func TestCreateFilesystemWithProperties(t *testing.T) {
	zpoolTest(t, func() {
		props := map[string]string{
			"compression": "lz4",
		}

		f, err := CreateFilesystem(testZPool+"/filesystem-test", props, nil)
		require.NoError(t, err)
		require.Equal(t, "lz4", f.Compression)

		filesystems, err := Filesystems("", nil)
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		require.NoError(t, f.Destroy(DestroyDefault))
	})
}

func TestVolumes(t *testing.T) {
	zpoolTest(t, func() {
		v, err := CreateVolume(testZPool+"/volume-test", uint64(pow2(23)), nil, nil)
		require.NoError(t, err)

		// volumes are sometimes "busy" if you try to manipulate them right away
		sleep(1)

		require.Equal(t, DatasetVolume, v.Type)
		volumes, err := Volumes("", nil)
		require.NoError(t, err)

		for _, volume := range volumes {
			require.Equal(t, DatasetVolume, volume.Type)
		}

		require.NoError(t, v.Destroy(DestroyDefault))
	})
}

func TestSnapshot(t *testing.T) {
	zpoolTest(t, func() {
		f, err := CreateFilesystem(testZPool+"/snapshot-test", nil, nil)
		require.NoError(t, err)

		filesystems, err := Filesystems("", nil)
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		s, err := f.Snapshot("test", false)
		require.NoError(t, err)

		require.Equal(t, DatasetSnapshot, s.Type)
		require.Equal(t, testZPool+"/snapshot-test@test", s.Name)

		require.NoError(t, s.Destroy(DestroyDefault))
		require.NoError(t, f.Destroy(DestroyDefault))
	})
}

func TestClone(t *testing.T) {
	zpoolTest(t, func() {
		f, err := CreateFilesystem(testZPool+"/snapshot-test", nil, nil)
		require.NoError(t, err)

		filesystems, err := Filesystems("", nil)
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		s, err := f.Snapshot("test", false)
		require.NoError(t, err)

		require.Equal(t, DatasetSnapshot, s.Type)
		require.Equal(t, testZPool+"/snapshot-test@test", s.Name)

		c, err := s.Clone(testZPool+"/clone-test", nil)
		require.NoError(t, err)
		require.Equal(t, DatasetFilesystem, c.Type)
		require.NoError(t, c.Destroy(DestroyDefault))
		require.NoError(t, s.Destroy(DestroyDefault))
		require.NoError(t, f.Destroy(DestroyDefault))
	})
}

func TestSendSnapshot(t *testing.T) {
	zpoolTest(t, func() {
		f, err := CreateFilesystem(testZPool+"/snapshot-test", nil, nil)
		require.NoError(t, err)

		filesystems, err := Filesystems("", nil)
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		s, err := f.Snapshot("test", false)
		require.NoError(t, err)

		err = s.SendSnapshot(io.Discard, false)
		require.NoError(t, err)
		require.NoError(t, s.Destroy(DestroyDefault))
		require.NoError(t, f.Destroy(DestroyDefault))
	})
}

func TestSendSnapshotResume(t *testing.T) {
	zpoolTest(t, func() {
		f, err := CreateFilesystem(testZPool+"/snapshot-test", nil, nil)
		require.NoError(t, err)

		s, err := f.Snapshot("test", false)
		require.NoError(t, err)

		pipeRdr, pipeWrtr := io.Pipe()
		go func() {
			err := s.SendSnapshot(pipeWrtr, true)
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		_, err = ReceiveSnapshot(io.LimitReader(pipeRdr, 10*1024), testZPool+"/recv-test", true)
		require.Error(t, err)
		var zfsErr *Error
		require.True(t, errors.As(err, &zfsErr))
		require.True(t, zfsErr.Resumable())

		list, err := Filesystems(testZPool+"/recv-test", []string{PropertyReceiveResumeToken})
		require.NoError(t, err)
		require.Len(t, list, 1)
		require.Len(t, list[0].ExtraProps, 1)
		require.NotEmpty(t, list[0].ExtraProps[PropertyReceiveResumeToken])

		t.Logf("Found resume token: %s", list[0].ExtraProps[PropertyReceiveResumeToken])

		// Go again with resume :)

		pipeRdr, pipeWrtr = io.Pipe()
		go func() {
			err := ResumeSend(pipeWrtr, list[0].ExtraProps[PropertyReceiveResumeToken])
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		_, err = ReceiveSnapshot(pipeRdr, testZPool+"/recv-test", true)
		require.NoError(t, err)

		snaps, err := Snapshots(testZPool+"/recv-test", nil)
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		require.Equal(t, snaps[0].Name, testZPool+"/recv-test@test")
	})
}

func TestChildren(t *testing.T) {
	zpoolTest(t, func() {
		f, err := CreateFilesystem(testZPool+"/snapshot-test", nil, nil)
		require.NoError(t, err)

		s, err := f.Snapshot("test", false)
		require.NoError(t, err)

		require.Equal(t, DatasetSnapshot, s.Type)
		require.Equal(t, testZPool+"/snapshot-test@test", s.Name)

		children, err := f.Children(0)
		require.NoError(t, err)

		require.Equal(t, 1, len(children))
		require.Equal(t, testZPool+"/snapshot-test@test", children[0].Name)

		require.NoError(t, s.Destroy(DestroyDefault))
		require.NoError(t, f.Destroy(DestroyDefault))
	})
}

func TestRollback(t *testing.T) {
	zpoolTest(t, func() {
		f, err := CreateFilesystem(testZPool+"/snapshot-test", nil, nil)
		require.NoError(t, err)

		filesystems, err := Filesystems("", nil)
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		s1, err := f.Snapshot("test", false)
		require.NoError(t, err)

		_, err = f.Snapshot("test2", false)
		require.NoError(t, err)

		s3, err := f.Snapshot("test3", false)
		require.NoError(t, err)

		err = s3.Rollback(false)
		require.NoError(t, err)

		err = s1.Rollback(false)
		require.Error(t, err, "should error when rolling back beyond most recent without destroyMoreRecent = true")

		err = s1.Rollback(true)
		require.NoError(t, err)

		require.NoError(t, s1.Destroy(DestroyDefault))
		require.NoError(t, f.Destroy(DestroyDefault))
	})
}
