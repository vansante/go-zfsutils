// Package zfs provides wrappers around the ZFS command line tools.
package zfs

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	Binary = "zfs"
)

// ZFS dataset types, which can indicate if a dataset is a filesystem, snapshot, or volume.
const (
	DatasetFilesystem = "filesystem"
	DatasetSnapshot   = "snapshot"
	DatasetVolume     = "volume"
)

// Dataset is a ZFS dataset.  A dataset could be a clone, filesystem, snapshot, or volume.
// The Type struct member can be used to determine a dataset's type.
//
// The field definitions can be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
type Dataset struct {
	Name          string
	Origin        string
	Used          uint64
	Avail         uint64
	Mountpoint    string
	Compression   string
	Type          string
	Written       uint64
	Volsize       uint64
	Logicalused   uint64
	Usedbydataset uint64
	Quota         uint64
	Referenced    uint64
	ExtraProps    map[string]string
}

// DestroyFlag is the options flag passed to Destroy.
type DestroyFlag int

// Valid destroy options.
const (
	DestroyDefault         DestroyFlag = 1 << iota
	DestroyRecursive                   = 1 << iota
	DestroyRecursiveClones             = 1 << iota
	DestroyDeferDeletion               = 1 << iota
	DestroyForceUmount                 = 1 << iota
)

// zfs is a helper function to wrap typical calls to zfs that ignores stdout.
func zfs(arg ...string) error {
	_, err := zfsOutput(arg...)
	return err
}

// zfs is a helper function to wrap typical calls to zfs.
func zfsOutput(arg ...string) ([][]string, error) {
	c := command{Command: Binary}
	return c.Run(arg...)
}

// Datasets returns a slice of ZFS datasets, regardless of type.
// A filter argument may be passed to select a dataset with the matching name, or empty string ("") may be used to select all datasets.
func Datasets(filter string, extraFields []string) ([]*Dataset, error) {
	return ListByType("all", filter, extraFields)
}

// Snapshots returns a slice of ZFS snapshots.
// A filter argument may be passed to select a snapshot with the matching name, or empty string ("") may be used to select all snapshots.
func Snapshots(filter string, extraFields []string) ([]*Dataset, error) {
	return ListByType(DatasetSnapshot, filter, extraFields)
}

// Filesystems returns a slice of ZFS filesystems.
// A filter argument may be passed to select a filesystem with the matching name, or empty string ("") may be used to select all filesystems.
func Filesystems(filter string, extraFields []string) ([]*Dataset, error) {
	return ListByType(DatasetFilesystem, filter, extraFields)
}

// Volumes returns a slice of ZFS volumes.
// A filter argument may be passed to select a volume with the matching name, or empty string ("") may be used to select all volumes.
func Volumes(filter string, extraFields []string) ([]*Dataset, error) {
	return ListByType(DatasetVolume, filter, extraFields)
}

// GetDataset retrieves a single ZFS dataset by name.
// This dataset could be any valid ZFS dataset type, such as a clone, filesystem, snapshot, or volume.
func GetDataset(name string, extraFields []string) (*Dataset, error) {
	fields := append(dsPropList, extraFields...) // nolint: gocritic
	out, err := zfsOutput("list", "-Hp", "-o", strings.Join(fields, ","), name)
	if err != nil {
		return nil, err
	}

	ds := &Dataset{Name: name}
	for _, line := range out {
		err := ds.parseLine(line, extraFields)
		if err != nil {
			return nil, err
		}
	}

	return ds, nil
}

// Clone clones a ZFS snapshot and returns a clone dataset.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) Clone(dest string, properties map[string]string) (*Dataset, error) {
	if d.Type != DatasetSnapshot {
		return nil, errors.New("can only clone snapshots")
	}
	args := make([]string, 2, 4)
	args[0] = "clone"
	args[1] = "-p"
	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}
	args = append(args, []string{d.Name, dest}...)

	err := zfs(args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(dest, nil)
}

// Unmount unmounts currently mounted ZFS file systems.
func (d *Dataset) Unmount(force bool) (*Dataset, error) {
	if d.Type == DatasetSnapshot {
		return nil, errors.New("cannot unmount snapshots")
	}
	args := make([]string, 1, 3)
	args[0] = "umount"
	if force {
		args = append(args, "-f")
	}
	args = append(args, d.Name)

	err := zfs(args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(d.Name, nil)
}

// LoadKey loads the encryption key for this and optionally children datasets.
// See: https://openzfs.github.io/openzfs-docs/man/8/zfs-load-key.8.html
func (d *Dataset) LoadKey(recursive bool, keyLocation string, stdin io.Reader) error {
	args := []string{"load-key"}
	if recursive {
		args = append(args, "-r")
	}
	if keyLocation != "" {
		args = append(args, "-L", keyLocation)
	}
	args = append(args, d.Name)
	cmd := command{Command: Binary, Stdin: stdin}
	_, err := cmd.Run(args...)
	return err
}

// UnloadKey unloads the encryption key for this dataset and optionally for child datasets as well.
// See: https://openzfs.github.io/openzfs-docs/man/8/zfs-unload-key.8.html
func (d *Dataset) UnloadKey(recursive bool) error {
	args := []string{"unload-key"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, d.Name)
	return zfs(args...)
}

// Mount mounts ZFS file systems.
func (d *Dataset) Mount(overlay bool, options []string) (*Dataset, error) {
	if d.Type == DatasetSnapshot {
		return nil, errors.New("cannot mount snapshots")
	}
	args := make([]string, 1, 5)
	args[0] = "mount"
	if overlay {
		args = append(args, "-O")
	}
	if options != nil {
		args = append(args, "-o")
		args = append(args, strings.Join(options, ","))
	}
	args = append(args, d.Name)

	err := zfs(args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(d.Name, nil)
}

// ReceiveSnapshot receives a ZFS stream from the input io.Reader.
// A new snapshot is created with the specified name, and streams the input data into the newly-created snapshot.
func ReceiveSnapshot(input io.Reader, name string, resumable bool, properties map[string]string) (*Dataset, error) {
	c := command{Command: Binary, Stdin: input}

	args := []string{"receive"}
	if resumable {
		args = append(args, "-s")
	}
	args = append(args, propsSlice(properties)...)
	args = append(args, name)

	_, err := c.Run(args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(name, nil)
}

func (d *Dataset) sendSnapshot(output io.Writer, options ...string) error {
	if d.Type != DatasetSnapshot {
		return errors.New("can only send snapshots")
	}

	c := command{Command: Binary, Stdout: output}
	args := append([]string{"send"}, options...)
	args = append(args, d.Name)
	_, err := c.Run(args...)
	return err
}

// SendSnapshot sends a ZFS stream of a snapshot to the input io.Writer.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) SendSnapshot(output io.Writer, raw bool) error {
	var args []string
	if raw {
		args = append(args, "-w")
	}
	return d.sendSnapshot(output, args...)
}

// IncrementalSend sends a ZFS stream of a snapshot to the input io.Writer using the baseSnapshot as the starting point.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) IncrementalSend(output io.Writer, baseSnapshot *Dataset, raw bool) error {
	if baseSnapshot.Type != DatasetSnapshot {
		return errors.New("can only send snapshots")
	}
	args := []string{"-i", baseSnapshot.Name}
	if raw {
		args = append(args, "-w")
	}
	return d.sendSnapshot(output, args...)
}

// ResumeSend resumes an interrupted ZFS stream of a snapshot to the input io.Writer using the receive_resume_token.
// An error will be returned if the input dataset is not of snapshot type.
func ResumeSend(output io.Writer, resumeToken string) error {
	c := command{Command: Binary, Stdout: output}
	args := append([]string{"send"}, "-t", resumeToken)
	_, err := c.Run(args...)
	return err
}

// CreateVolume creates a new ZFS volume with the specified name, size, and properties.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func CreateVolume(name string, size uint64, properties map[string]string, stdin io.Reader) (*Dataset, error) {
	args := make([]string, 4, 5)
	args[0] = "create"
	args[1] = "-p"
	args[2] = "-V"
	args[3] = strconv.FormatUint(size, 10)
	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}
	args = append(args, name)
	cmd := command{Command: Binary, Stdin: stdin}
	_, err := cmd.Run(args...)
	if err != nil {
		return nil, err
	}

	return GetDataset(name, nil)
}

// Destroy destroys a ZFS dataset.
// If the destroy bit flag is set, any descendents of the dataset will be recursively destroyed, including snapshots.
// If the deferred bit flag is set, the snapshot is marked for deferred deletion.
func (d *Dataset) Destroy(flags DestroyFlag) error {
	args := make([]string, 1, 3)
	args[0] = "destroy"
	if flags&DestroyRecursive != 0 {
		args = append(args, "-r")
	}

	if flags&DestroyRecursiveClones != 0 {
		args = append(args, "-R")
	}

	if flags&DestroyDeferDeletion != 0 {
		args = append(args, "-d")
	}

	if flags&DestroyForceUmount != 0 {
		args = append(args, "-f")
	}

	args = append(args, d.Name)
	err := zfs(args...)
	return err
}

// SetProperty sets a ZFS property on the receiving dataset.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func (d *Dataset) SetProperty(key, val string) error {
	prop := strings.Join([]string{key, val}, "=")
	err := zfs("set", prop, d.Name)
	return err
}

// GetProperty returns the current value of a ZFS property from the receiving dataset.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func (d *Dataset) GetProperty(key string) (string, error) {
	out, err := zfsOutput("get", "-Hp", "-o", "value", key, d.Name)
	if err != nil {
		return "", err
	}

	return out[0][0], nil
}

// InheritProperty clears a property from the receiving dataset, making it use its parent datasets value.
func (d *Dataset) InheritProperty(key string) error {
	return zfs("inherit", key, d.Name)
}

// Rename renames a dataset.
func (d *Dataset) Rename(name string, createParent, recursiveRenameSnapshots bool) (*Dataset, error) {
	args := make([]string, 3, 5)
	args[0] = "rename"
	args[1] = d.Name
	args[2] = name
	if createParent {
		args = append(args, "-p")
	}
	if recursiveRenameSnapshots {
		args = append(args, "-r")
	}

	err := zfs(args...)
	if err != nil {
		return d, err
	}

	return GetDataset(name, nil)
}

// Snapshots returns a slice of all ZFS snapshots of a given dataset.
func (d *Dataset) Snapshots(extraFields []string) ([]*Dataset, error) {
	return Snapshots(d.Name, extraFields)
}

// CreateFilesystem creates a new ZFS filesystem with the specified name and properties.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func CreateFilesystem(name string, properties map[string]string, stdin io.Reader) (*Dataset, error) {
	args := make([]string, 1, 4)
	args[0] = "create"

	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}

	args = append(args, name)
	cmd := command{Command: Binary, Stdin: stdin}
	_, err := cmd.Run(args...)
	if err != nil {
		return nil, err
	}

	return GetDataset(name, nil)
}

// Snapshot creates a new ZFS snapshot of the receiving dataset, using the specified name.
// Optionally, the snapshot can be taken recursively, creating snapshots of all descendent filesystems in a single, atomic operation.
func (d *Dataset) Snapshot(name string, recursive bool) (*Dataset, error) {
	args := make([]string, 1, 4)
	args[0] = "snapshot"
	if recursive {
		args = append(args, "-r")
	}
	snapName := fmt.Sprintf("%s@%s", d.Name, name)
	args = append(args, snapName)

	err := zfs(args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(snapName, nil)
}

// Rollback rolls back the receiving ZFS dataset to a previous snapshot.
// Optionally, intermediate snapshots can be destroyed.
// A ZFS snapshot rollback cannot be completed without this option, if more recent snapshots exist.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) Rollback(destroyMoreRecent bool) error {
	if d.Type != DatasetSnapshot {
		return errors.New("can only rollback snapshots")
	}

	args := make([]string, 1, 3)
	args[0] = "rollback"
	if destroyMoreRecent {
		args = append(args, "-r")
	}
	args = append(args, d.Name)

	err := zfs(args...)
	return err
}

// Children returns a slice of children of the receiving ZFS dataset.
// A recursion depth may be specified, or a depth of 0 allows unlimited recursion.
func (d *Dataset) Children(depth uint64) ([]*Dataset, error) {
	args := []string{"list"}
	if depth > 0 {
		args = append(args, "-d")
		args = append(args, strconv.FormatUint(depth, 10))
	} else {
		args = append(args, "-r")
	}
	args = append(args, "-t", "all", "-Hp", "-o", strings.Join(dsPropList, ","))
	args = append(args, d.Name)

	out, err := zfsOutput(args...)
	if err != nil {
		return nil, err
	}

	var datasets []*Dataset
	name := ""
	var ds *Dataset
	for _, line := range out {
		if name != line[0] {
			name = line[0]
			ds = &Dataset{Name: name}
			datasets = append(datasets, ds)
		}

		err := ds.parseLine(line, nil)
		if err != nil {
			return nil, err
		}
	}
	return datasets[1:], nil
}
