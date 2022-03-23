// Package zfs provides wrappers around the ZFS command line tools.
package zfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	Binary = "zfs"
)

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

// ListByType lists the datasets by type and allows you to fetch extra custom fields
func ListByType(ctx context.Context, t DatasetType, filter string, extraProps []string) ([]Dataset, error) {
	allFields := append(dsPropList, extraProps...) // nolint: gocritic

	dsPropListOptions := strings.Join(allFields, ",")
	args := []string{"list", "-rHp", "-t", string(t), "-o", dsPropListOptions}
	if filter != "" {
		args = append(args, filter)
	}

	out, err := zfsOutput(ctx, args...)
	if err != nil {
		return nil, err
	}

	datasets := make([]Dataset, 0, len(out))
	if len(out) == 0 {
		return datasets, nil
	}

	for _, fields := range out {
		ds, err := datasetFromFields(fields, extraProps)
		if err != nil {
			return datasets, err
		}
		datasets = append(datasets, *ds)
	}

	return datasets, nil
}

// Datasets returns a slice of ZFS datasets, regardless of type.
// A filter argument may be passed to select a dataset with the matching name, or empty string ("") may be used to select all datasets.
func Datasets(ctx context.Context, filter string, extraFields []string) ([]Dataset, error) {
	return ListByType(ctx, DatasetAll, filter, extraFields)
}

// Snapshots returns a slice of ZFS snapshots.
// A filter argument may be passed to select a snapshot with the matching name, or empty string ("") may be used to select all snapshots.
func Snapshots(ctx context.Context, filter string, extraFields []string) ([]Dataset, error) {
	return ListByType(ctx, DatasetSnapshot, filter, extraFields)
}

// Filesystems returns a slice of ZFS filesystems.
// A filter argument may be passed to select a filesystem with the matching name, or empty string ("") may be used to select all filesystems.
func Filesystems(ctx context.Context, filter string, extraFields []string) ([]Dataset, error) {
	return ListByType(ctx, DatasetFilesystem, filter, extraFields)
}

// Volumes returns a slice of ZFS volumes.
// A filter argument may be passed to select a volume with the matching name, or empty string ("") may be used to select all volumes.
func Volumes(ctx context.Context, filter string, extraFields []string) ([]Dataset, error) {
	return ListByType(ctx, DatasetVolume, filter, extraFields)
}

// ListWithProperty returns a map of dataset names mapped to the properties value for datasets which have the given ZFS property.
func ListWithProperty(ctx context.Context, t DatasetType, filter, prop string) (map[string]string, error) {
	c := command{
		cmd: Binary,
		ctx: ctx,
	}
	lines, err := c.Run("get", "-t", string(t), "-Hp", "-o", "name,value", "-r", "-s", "local", prop, filter)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(lines))
	for _, line := range lines {
		result[line[0]] = line[1]
	}
	return result, nil
}

// GetDataset retrieves a single ZFS dataset by name.
// This dataset could be any valid ZFS dataset type, such as a clone, filesystem, snapshot, or volume.
func GetDataset(ctx context.Context, name string, extraProps []string) (*Dataset, error) {
	fields := append(dsPropList, extraProps...) // nolint: gocritic
	out, err := zfsOutput(ctx, "list", "-Hp", "-o", strings.Join(fields, ","), name)
	if err != nil {
		return nil, err
	}

	if len(out) > 1 {
		return nil, fmt.Errorf("more output than expected: %v", out)
	}

	return datasetFromFields(out[0], extraProps)
}

// Clone clones a ZFS snapshot and returns a clone dataset.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) Clone(ctx context.Context, dest string, properties map[string]string) (*Dataset, error) {
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

	err := zfs(ctx, args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(ctx, dest, nil)
}

// Unmount unmounts currently mounted ZFS file systems.
func (d *Dataset) Unmount(ctx context.Context, force bool) (*Dataset, error) {
	if d.Type == DatasetSnapshot {
		return nil, errors.New("cannot unmount snapshots")
	}
	args := make([]string, 1, 3)
	args[0] = "umount"
	if force {
		args = append(args, "-f")
	}
	args = append(args, d.Name)

	err := zfs(ctx, args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(ctx, d.Name, nil)
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
	cmd := command{cmd: Binary, stdin: stdin}
	_, err := cmd.Run(args...)
	return err
}

// UnloadKey unloads the encryption key for this dataset and optionally for child datasets as well.
// See: https://openzfs.github.io/openzfs-docs/man/8/zfs-unload-key.8.html
func (d *Dataset) UnloadKey(ctx context.Context, recursive bool) error {
	args := []string{"unload-key"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, d.Name)
	return zfs(ctx, args...)
}

// Mount mounts ZFS file systems.
func (d *Dataset) Mount(ctx context.Context, overlay bool, options []string) (*Dataset, error) {
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

	err := zfs(ctx, args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(ctx, d.Name, nil)
}

// ReceiveSnapshot receives a ZFS stream from the input io.Reader.
// A new snapshot is created with the specified name, and streams the input data into the newly-created snapshot.
func ReceiveSnapshot(ctx context.Context, input io.Reader, name string, resumable bool, properties map[string]string) (*Dataset, error) {
	c := command{
		cmd:   Binary,
		ctx:   ctx,
		stdin: input,
	}

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
	return GetDataset(ctx, name, nil)
}

type SendOptions struct {
	// For encrypted datasets, send data exactly as it exists on disk. This allows backups to
	//           be taken even if encryption keys are not currently loaded. The backup may then be
	//           received on an untrusted machine since that machine will not have the encryption keys
	//           to read the protected data or alter it without being detected. Upon being received,
	//           the dataset will have the same encryption keys as it did on the send side, although
	//           the keylocation property will be defaulted to prompt if not otherwise provided. For
	//           unencrypted datasets, this flag will be equivalent to -Lec.  Note that if you do not
	//           use this flag for sending encrypted datasets, data will be sent unencrypted and may be
	//           re-encrypted with a different encryption key on the receiving system, which will
	//           disable the ability to do a raw send to that system for incrementals.
	Raw bool
	// Include the dataset's properties in the stream.  This flag is implicit when -R is
	//           specified.  The receiving system must also support this feature. Sends of encrypted
	//           datasets must use -w when using this flag.
	Props bool
	// Generate an incremental stream from the first snapshot (the incremental source) to the
	//           second snapshot (the incremental target).  The incremental source can be specified as
	//           the last component of the snapshot name (the @ character and following) and it is
	//           assumed to be from the same file system as the incremental target.
	//
	//           If the destination is a clone, the source may be the origin snapshot, which must be
	//           fully specified (for example, pool/fs@origin, not just @origin).
	IncrementalBase *Dataset
}

// SendSnapshot sends a ZFS stream of a snapshot to the input io.Writer.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) SendSnapshot(ctx context.Context, output io.Writer, sendOptions SendOptions) error {
	if d.Type != DatasetSnapshot {
		return errors.New("can only send snapshots")
	}

	args := make([]string, 0, 8)
	if sendOptions.Raw {
		args = append(args, "-w")
	}
	if sendOptions.Props {
		args = append(args, "-p")
	}
	if sendOptions.IncrementalBase != nil {
		if sendOptions.IncrementalBase.Type != DatasetSnapshot {
			return errors.New("base is not a snapshot")
		}
		args = append(args, "-i", sendOptions.IncrementalBase.Name)
	}

	c := command{
		cmd:    Binary,
		ctx:    ctx,
		stdout: output,
	}
	args = append([]string{"send"}, args...)
	args = append(args, d.Name)
	_, err := c.Run(args...)
	return err
}

// ResumeSend resumes an interrupted ZFS stream of a snapshot to the input io.Writer using the receive_resume_token.
// An error will be returned if the input dataset is not of snapshot type.
func ResumeSend(ctx context.Context, output io.Writer, resumeToken string) error {
	c := command{
		cmd:    Binary,
		ctx:    ctx,
		stdout: output,
	}
	args := append([]string{"send"}, "-t", resumeToken)
	_, err := c.Run(args...)
	return err
}

// CreateVolume creates a new ZFS volume with the specified name, size, and properties.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func CreateVolume(ctx context.Context, name string, size uint64, properties map[string]string, stdin io.Reader) (*Dataset, error) {
	args := make([]string, 4, 5)
	args[0] = "create"
	args[1] = "-p"
	args[2] = "-V"
	args[3] = strconv.FormatUint(size, 10)
	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}
	args = append(args, name)

	cmd := command{
		cmd:   Binary,
		ctx:   ctx,
		stdin: stdin,
	}
	_, err := cmd.Run(args...)
	if err != nil {
		return nil, err
	}

	return GetDataset(ctx, name, nil)
}

// Destroy destroys a ZFS dataset.
// If the destroy bit flag is set, any descendents of the dataset will be recursively destroyed, including snapshots.
// If the deferred bit flag is set, the snapshot is marked for deferred deletion.
func (d *Dataset) Destroy(ctx context.Context, flags DestroyFlag) error {
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

	return zfs(ctx, args...)
}

// SetProperty sets a ZFS property on the receiving dataset.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func (d *Dataset) SetProperty(ctx context.Context, key, val string) error {
	prop := strings.Join([]string{key, val}, "=")

	return zfs(ctx, "set", prop, d.Name)
}

// GetProperty returns the current value of a ZFS property from the receiving dataset.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func (d *Dataset) GetProperty(ctx context.Context, key string) (string, error) {
	out, err := zfsOutput(ctx, "get", "-Hp", "-o", "value", key, d.Name)
	if err != nil {
		return "", err
	}

	return out[0][0], nil
}

// InheritProperty clears a property from the receiving dataset, making it use its parent datasets value.
func (d *Dataset) InheritProperty(ctx context.Context, key string) error {
	return zfs(ctx, "inherit", key, d.Name)
}

// Rename renames a dataset.
func (d *Dataset) Rename(ctx context.Context, name string, createParent, recursiveRenameSnapshots bool) (*Dataset, error) {
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

	err := zfs(ctx, args...)
	if err != nil {
		return d, err
	}

	return GetDataset(ctx, name, nil)
}

// Snapshots returns a slice of all ZFS snapshots of a given dataset.
func (d *Dataset) Snapshots(ctx context.Context, extraFields []string) ([]Dataset, error) {
	return Snapshots(ctx, d.Name, extraFields)
}

// CreateFilesystem creates a new ZFS filesystem with the specified name and properties.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func CreateFilesystem(ctx context.Context, name string, properties map[string]string, stdin io.Reader) (*Dataset, error) {
	args := make([]string, 1, 4)
	args[0] = "create"

	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}
	args = append(args, name)

	cmd := command{
		cmd:   Binary,
		ctx:   ctx,
		stdin: stdin,
	}
	_, err := cmd.Run(args...)
	if err != nil {
		return nil, err
	}

	return GetDataset(ctx, name, nil)
}

// Snapshot creates a new ZFS snapshot of the receiving dataset, using the specified name.
// Optionally, the snapshot can be taken recursively, creating snapshots of all descendent filesystems in a single, atomic operation.
func (d *Dataset) Snapshot(ctx context.Context, name string, recursive bool) (*Dataset, error) {
	args := make([]string, 1, 4)
	args[0] = "snapshot"
	if recursive {
		args = append(args, "-r")
	}
	snapName := fmt.Sprintf("%s@%s", d.Name, name)
	args = append(args, snapName)

	err := zfs(ctx, args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(ctx, snapName, nil)
}

// Rollback rolls back the receiving ZFS dataset to a previous snapshot.
// Optionally, intermediate snapshots can be destroyed.
// A ZFS snapshot rollback cannot be completed without this option, if more recent snapshots exist.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) Rollback(ctx context.Context, destroyMoreRecent bool) error {
	if d.Type != DatasetSnapshot {
		return errors.New("can only rollback snapshots")
	}

	args := make([]string, 1, 3)
	args[0] = "rollback"
	if destroyMoreRecent {
		args = append(args, "-r")
	}
	args = append(args, d.Name)

	return zfs(ctx, args...)
}

// Children returns a slice of children of the receiving ZFS dataset.
// A recursion depth may be specified, or a depth of 0 allows unlimited recursion.
func (d *Dataset) Children(ctx context.Context, depth uint64, extraProps []string) ([]Dataset, error) {
	allFields := append(dsPropList, extraProps...) // nolint: gocritic

	args := []string{"list"}
	if depth > 0 {
		args = append(args, "-d")
		args = append(args, strconv.FormatUint(depth, 10))
	} else {
		args = append(args, "-r")
	}
	args = append(args, "-t", "all", "-Hp", "-o", strings.Join(allFields, ","))
	args = append(args, d.Name)

	out, err := zfsOutput(ctx, args...)
	if err != nil {
		return nil, err
	}

	datasets := make([]Dataset, 0, len(out)-1)
	for i, fields := range out {
		if i == 0 { // Skip the first parent entry, because we are looking for its children
			continue
		}

		ds, err := datasetFromFields(fields, extraProps)
		if err != nil {
			return nil, err
		}
		datasets = append(datasets, *ds)
	}
	return datasets, nil
}
