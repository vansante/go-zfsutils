package zfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/stretchr/testify/require"
)

const testZPool = "go-test-zpool"

var noMountProps = map[string]string{PropertyCanMount: PropertyOff}

func TestDatasets(t *testing.T) {
	t.Helper()

	TestZPool(testZPool, func() {
		_, err := ListDatasets(context.Background(), ListOptions{})
		require.NoError(t, err)

		ds, err := GetDataset(context.Background(), testZPool)
		require.NoError(t, err)
		require.Equal(t, DatasetFilesystem, ds.Type)
		require.Equal(t, "", ds.Origin)
		require.Greater(t, ds.Logicalused, uint64(0), "Logicalused is not greater than 0")
	})
}

func TestDatasetsWithProps(t *testing.T) {
	TestZPool(testZPool, func() {
		ds, err := GetDataset(context.Background(), testZPool)
		require.NoError(t, err)

		require.NoError(t, ds.SetProperty(context.Background(), "nl.test:hello", "world"))

		ds, err = GetDataset(context.Background(), testZPool, "nl.test:hello", "canmount")
		require.NoError(t, err)

		require.Len(t, ds.ExtraProps, 2, fmt.Sprintf("%#v", ds.ExtraProps))
		require.Equal(t, "world", ds.ExtraProps["nl.test:hello"])
		require.Equal(t, "on", ds.ExtraProps["canmount"])
	})
}

func TestGetNotExistingDataset(t *testing.T) {
	TestZPool(testZPool, func() {
		_, err := GetDataset(context.Background(), testZPool+"/doesnt-exist")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDatasetNotFound)
	})
}

func TestDatasetGetProperty(t *testing.T) {
	TestZPool(testZPool, func() {
		ds, err := GetDataset(context.Background(), testZPool)
		require.NoError(t, err)

		prop, err := ds.GetProperty(context.Background(), PropertyReadOnly)
		require.NoError(t, err)
		require.Equal(t, "off", prop)

		prop, err = ds.GetProperty(context.Background(), PropertyCompression)
		require.NoError(t, err)
		require.Equal(t, "off", prop)
	})
}

func TestDatasetSetInheritProperty(t *testing.T) {
	TestZPool(testZPool, func() {
		ds, err := GetDataset(context.Background(), testZPool)
		require.NoError(t, err)

		const testProp = "nl.bla:nope"
		require.NoError(t, ds.SetProperty(context.Background(), testProp, "hello"))

		prop, err := ds.GetProperty(context.Background(), testProp)
		require.NoError(t, err)
		require.Equal(t, "hello", prop)

		require.NoError(t, ds.InheritProperty(context.Background(), testProp))

		prop, err = ds.GetProperty(context.Background(), testProp)
		require.NoError(t, err)
		require.Equal(t, "-", prop)
	})
}

func TestSnapshots(t *testing.T) {
	TestZPool(testZPool, func() {
		snapshots, err := ListSnapshots(context.Background(), ListOptions{})
		require.NoError(t, err)

		for _, snapshot := range snapshots {
			require.Equal(t, DatasetSnapshot, snapshot.Type)
		}
	})
}

func TestFilesystems(t *testing.T) {
	TestZPool(testZPool, func() {
		f, err := CreateFilesystem(context.Background(), testZPool+"/filesystem-test", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)

		filesystems, err := ListFilesystems(context.Background(), ListOptions{})
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		require.NoError(t, f.Destroy(context.Background(), DestroyOptions{}))
	})
}

func TestCreateFilesystemWithProperties(t *testing.T) {
	TestZPool(testZPool, func() {
		f, err := CreateFilesystem(context.Background(), testZPool+"/filesystem-test", CreateFilesystemOptions{
			Properties: map[string]string{
				PropertyCompression: "lz4",
				PropertyCanMount:    PropertyOff,
			},
		})
		require.NoError(t, err)
		require.Equal(t, "lz4", f.Compression)

		filesystems, err := ListFilesystems(context.Background(), ListOptions{})
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		require.NoError(t, f.Destroy(context.Background(), DestroyOptions{}))
	})
}

func TestVolumes(t *testing.T) {
	TestZPool(testZPool, func() {
		v, err := CreateVolume(context.Background(), testZPool+"/volume-test", uint64(pow2(23)), CreateVolumeOptions{})
		require.NoError(t, err)

		// volumes are sometimes "busy" if you try to manipulate them right away
		time.Sleep(time.Second)

		require.Equal(t, DatasetVolume, v.Type)
		volumes, err := ListVolumes(context.Background(), ListOptions{})
		require.NoError(t, err)

		for _, volume := range volumes {
			require.Equal(t, DatasetVolume, volume.Type)
		}

		require.NoError(t, v.Destroy(context.Background(), DestroyOptions{}))
	})
}

func TestSnapshot(t *testing.T) {
	TestZPool(testZPool, func() {
		f, err := CreateFilesystem(context.Background(), testZPool+"/snapshot-test", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)

		filesystems, err := ListFilesystems(context.Background(), ListOptions{})
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		s, err := f.Snapshot(context.Background(), "test", SnapshotOptions{})
		require.NoError(t, err)

		require.Equal(t, DatasetSnapshot, s.Type)
		require.Equal(t, testZPool+"/snapshot-test@test", s.Name)

		require.NoError(t, s.Destroy(context.Background(), DestroyOptions{}))
		require.NoError(t, f.Destroy(context.Background(), DestroyOptions{}))
	})
}

func TestListingWithProperty(t *testing.T) {
	TestZPool(testZPool, func() {
		const prop1 = "nl.test:bla"
		const prop2 = "nl.test:hoi"

		root, err := GetDataset(context.Background(), testZPool)
		require.NoError(t, err)

		f1, err := CreateFilesystem(context.Background(), testZPool+"/list-test1", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)
		require.NoError(t, f1.SetProperty(context.Background(), prop1, "123"))
		require.NoError(t, f1.SetProperty(context.Background(), prop2, "wereld"))

		f2, err := CreateFilesystem(context.Background(), testZPool+"/list-test2", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)
		require.NoError(t, f2.SetProperty(context.Background(), prop1, "321"))

		ds, err := root.Children(context.Background(), ListOptions{
			DatasetType:     DatasetFilesystem,
			ExtraProperties: []string{prop1, prop2},
		})
		require.NoError(t, err)
		require.Len(t, ds, 2)

		require.Equal(t, f1.Name, ds[0].Name)
		require.Equal(t, "123", ds[0].ExtraProps[prop1])
		require.Equal(t, "wereld", ds[0].ExtraProps[prop2])

		require.Equal(t, f2.Name, ds[1].Name)
		require.Equal(t, "321", ds[1].ExtraProps[prop1])
	})
}

func TestListWithProperty(t *testing.T) {
	TestZPool(testZPool, func() {
		const prop = "nl.test:bla"

		f1, err := CreateFilesystem(context.Background(), testZPool+"/list-test", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)
		require.NoError(t, f1.SetProperty(context.Background(), prop, "123"))

		ls, err := ListWithProperty(context.Background(), DatasetFilesystem, testZPool+"/list-test", prop)
		require.NoError(t, err)
		require.Len(t, ls, 1)
		require.Equal(t, map[string]string{
			f1.Name: "123",
		}, ls)
	})
}

func TestClone(t *testing.T) {
	TestZPool(testZPool, func() {
		f, err := CreateFilesystem(context.Background(), testZPool+"/snapshot-test", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)

		filesystems, err := ListFilesystems(context.Background(), ListOptions{})
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		s, err := f.Snapshot(context.Background(), "test", SnapshotOptions{})
		require.NoError(t, err)

		require.Equal(t, DatasetSnapshot, s.Type)
		require.Equal(t, testZPool+"/snapshot-test@test", s.Name)

		c, err := s.Clone(context.Background(), testZPool+"/clone-test", CloneOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)
		require.Equal(t, DatasetFilesystem, c.Type)
		require.NoError(t, c.Destroy(context.Background(), DestroyOptions{}))
		require.NoError(t, s.Destroy(context.Background(), DestroyOptions{}))
		require.NoError(t, f.Destroy(context.Background(), DestroyOptions{}))
	})
}

func TestSendSnapshot(t *testing.T) {
	TestZPool(testZPool, func() {
		f, err := CreateFilesystem(context.Background(), testZPool+"/snapshot-test", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)

		filesystems, err := ListFilesystems(context.Background(), ListOptions{})
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		s, err := f.Snapshot(context.Background(), "test", SnapshotOptions{})
		require.NoError(t, err)

		err = s.SendSnapshot(context.Background(), io.Discard, SendOptions{})
		require.NoError(t, err)
		require.NoError(t, s.Destroy(context.Background(), DestroyOptions{}))
		require.NoError(t, f.Destroy(context.Background(), DestroyOptions{}))
	})
}

func TestSendSnapshotResume(t *testing.T) {
	TestZPool(testZPool, func() {
		f, err := CreateFilesystem(context.Background(), testZPool+"/snapshot-test", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)

		s, err := f.Snapshot(context.Background(), "test", SnapshotOptions{})
		require.NoError(t, err)

		pipeRdr, pipeWrtr := io.Pipe()
		go func() {
			err := s.SendSnapshot(context.Background(), pipeWrtr, SendOptions{})
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		_, err = ReceiveSnapshot(context.Background(), io.LimitReader(pipeRdr, 10*1024), testZPool+"/recv-test", ReceiveOptions{
			Resumable:  true,
			Properties: noMountProps,
		})
		require.Error(t, err)
		var zfsErr *ResumableStreamError
		require.True(t, errors.As(err, &zfsErr))
		require.NotEmpty(t, zfsErr.ResumeToken(), zfsErr)

		list, err := ListFilesystems(context.Background(), ListOptions{
			ParentDataset:   testZPool + "/recv-test",
			ExtraProperties: []string{PropertyReceiveResumeToken},
		})
		require.NoError(t, err)
		require.Len(t, list, 1)
		require.Len(t, list[0].ExtraProps, 1)
		require.NotEmpty(t, list[0].ExtraProps[PropertyReceiveResumeToken])
		require.Equal(t, zfsErr.ResumeToken(), list[0].ExtraProps[PropertyReceiveResumeToken])

		t.Logf("Found resume token: %s", list[0].ExtraProps[PropertyReceiveResumeToken])

		// Go again with resume :)

		pipeRdr, pipeWrtr = io.Pipe()
		go func() {
			err := ResumeSend(context.Background(), pipeWrtr, list[0].ExtraProps[PropertyReceiveResumeToken], ResumeSendOptions{})
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		_, err = ReceiveSnapshot(context.Background(), pipeRdr, testZPool+"/recv-test", ReceiveOptions{
			Resumable:  true,
			Properties: noMountProps,
		})
		require.NoError(t, err)

		snaps, err := ListSnapshots(context.Background(), ListOptions{
			ParentDataset: testZPool + "/recv-test",
			Recursive:     true,
		})
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		require.Equal(t, snaps[0].Name, testZPool+"/recv-test@test")
	})
}

func TestSendSnapshotSpeedLimit(t *testing.T) {
	TestZPool(testZPool, func() {
		f, err := CreateFilesystem(context.Background(), testZPool+"/snapshot-test", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)

		s, err := f.Snapshot(context.Background(), "test", SnapshotOptions{})
		require.NoError(t, err)

		pipeRdr, pipeWrtr := io.Pipe()
		go func() {
			err := s.SendSnapshot(context.Background(), pipeWrtr, SendOptions{BytesPerSecond: 10_000})
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		_, err = ReceiveSnapshot(context.Background(), pipeRdr, testZPool+"/recv-test", ReceiveOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)
	})
}

func TestSendSnapshotCompressed(t *testing.T) {
	TestZPool(testZPool, func() {
		f, err := CreateFilesystem(context.Background(), testZPool+"/snapshot-test", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)

		s, err := f.Snapshot(context.Background(), "test", SnapshotOptions{})
		require.NoError(t, err)

		pipeRdr, pipeWrtr := io.Pipe()
		go func() {
			err := s.SendSnapshot(context.Background(), pipeWrtr, SendOptions{CompressionLevel: zstd.SpeedDefault})
			require.NoError(t, err)
			require.NoError(t, pipeWrtr.Close())
		}()

		_, err = ReceiveSnapshot(context.Background(), pipeRdr, testZPool+"/recv-test", ReceiveOptions{
			EnableDecompression: true,
			Properties:          noMountProps,
		})
		require.NoError(t, err)
	})
}

func TestChildren(t *testing.T) {
	TestZPool(testZPool, func() {
		f, err := CreateFilesystem(context.Background(), testZPool+"/snapshot-test", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)

		s, err := f.Snapshot(context.Background(), "test", SnapshotOptions{})
		require.NoError(t, err)

		require.Equal(t, DatasetSnapshot, s.Type)
		require.Equal(t, testZPool+"/snapshot-test@test", s.Name)

		children, err := f.Children(context.Background(), ListOptions{
			DatasetType:     DatasetSnapshot,
			ExtraProperties: []string{PropertyMounted},
		})
		require.NoError(t, err)

		require.Equal(t, 1, len(children))
		require.Equal(t, testZPool+"/snapshot-test@test", children[0].Name)
		require.Len(t, children[0].ExtraProps, 1)
		require.Equal(t, children[0].ExtraProps, map[string]string{PropertyMounted: ""})

		require.NoError(t, s.Destroy(context.Background(), DestroyOptions{}))
		require.NoError(t, f.Destroy(context.Background(), DestroyOptions{}))
	})
}

func TestRollback(t *testing.T) {
	TestZPool(testZPool, func() {
		f, err := CreateFilesystem(context.Background(), testZPool+"/snapshot-test", CreateFilesystemOptions{
			Properties: noMountProps,
		})
		require.NoError(t, err)

		filesystems, err := ListDatasets(context.Background(), ListOptions{DatasetType: DatasetFilesystem})
		require.NoError(t, err)

		for _, filesystem := range filesystems {
			require.Equal(t, DatasetFilesystem, filesystem.Type)
		}

		s1, err := f.Snapshot(context.Background(), "test", SnapshotOptions{})
		require.NoError(t, err)

		_, err = f.Snapshot(context.Background(), "test2", SnapshotOptions{})
		require.NoError(t, err)

		s3, err := f.Snapshot(context.Background(), "test3", SnapshotOptions{})
		require.NoError(t, err)

		err = s3.Rollback(context.Background(), RollbackOptions{})
		require.NoError(t, err)

		err = s1.Rollback(context.Background(), RollbackOptions{})
		require.Error(t, err, "should error when rolling back beyond most recent without destroyMoreRecent = true")

		err = s1.Rollback(context.Background(), RollbackOptions{DestroyMoreRecent: true})
		require.NoError(t, err)

		require.NoError(t, s1.Destroy(context.Background(), DestroyOptions{}))
		require.NoError(t, f.Destroy(context.Background(), DestroyOptions{}))
	})
}

func TestDataset_Mount(t *testing.T) {
	TestZPool(testZPool, func() {
		f, err := CreateFilesystem(context.Background(), testZPool+"/mount-test", CreateFilesystemOptions{
			CreateParents: false,
		})
		require.NoError(t, err)

		_, err = f.Mount(context.Background(), MountOptions{})
		require.NoError(t, err)
	})
}
