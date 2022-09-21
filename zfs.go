// Package zfs provides wrappers around the ZFS command line tools.
package zfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/juju/ratelimit"
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
func ListByType(ctx context.Context, t DatasetType, filter string, extraProps ...string) ([]Dataset, error) {
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
func Datasets(ctx context.Context, filter string, extraProperties ...string) ([]Dataset, error) {
	return ListByType(ctx, DatasetAll, filter, extraProperties...)
}

// Snapshots returns a slice of ZFS snapshots.
// A filter argument may be passed to select a snapshot with the matching name, or empty string ("") may be used to select all snapshots.
func Snapshots(ctx context.Context, filter string, extraProperties ...string) ([]Dataset, error) {
	return ListByType(ctx, DatasetSnapshot, filter, extraProperties...)
}

// Filesystems returns a slice of ZFS filesystems.
// A filter argument may be passed to select a filesystem with the matching name, or empty string ("") may be used to select all filesystems.
func Filesystems(ctx context.Context, filter string, extraProperties ...string) ([]Dataset, error) {
	return ListByType(ctx, DatasetFilesystem, filter, extraProperties...)
}

// Volumes returns a slice of ZFS volumes.
// A filter argument may be passed to select a volume with the matching name, or empty string ("") may be used to select all volumes.
func Volumes(ctx context.Context, filter string, extraProperties ...string) ([]Dataset, error) {
	return ListByType(ctx, DatasetVolume, filter, extraProperties...)
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
		switch len(line) {
		case 2:
			result[line[0]] = line[1]
		case 1:
			result[line[0]] = PropertyUnset
		}
	}
	return result, nil
}

// GetDataset retrieves a single ZFS dataset by name.
// This dataset could be any valid ZFS dataset type, such as a clone, filesystem, snapshot, or volume.
func GetDataset(ctx context.Context, name string, extraProperties ...string) (*Dataset, error) {
	fields := append(dsPropList, extraProperties...) // nolint: gocritic
	out, err := zfsOutput(ctx, "list", "-Hp", "-o", strings.Join(fields, ","), name)
	if err != nil {
		return nil, err
	}

	if len(out) > 1 {
		return nil, fmt.Errorf("more output than expected: %v", out)
	}

	return datasetFromFields(out[0], extraProperties)
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
	return GetDataset(ctx, dest)
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
	return GetDataset(ctx, d.Name)
}

// LoadKeyOptions are options you can specify to customize the ZFS key loading
type LoadKeyOptions struct {
	// Recursively loads the keys for the specified filesystem and all descendent encryption roots.
	Recursive bool
	// Do a dry-run (Qq No-op ) load-key. This will cause zfs to simply check that the provided key is correct.
	// This command may be run even if the key is already loaded.
	Noop bool
	// When the key is in a file, load it using this keylocation.
	// This is optional when the ZFS dataset already has this property set.
	KeyLocation string
	// Provide a reader to read the key from stdin
	KeyReader io.Reader
}

// LoadKey loads the encryption key for this and optionally children datasets.
// See: https://openzfs.github.io/openzfs-docs/man/8/zfs-load-key.8.html
func (d *Dataset) LoadKey(ctx context.Context, loadOptions LoadKeyOptions) error {
	args := make([]string, 1, 4)
	args[0] = "load-key"
	if loadOptions.Recursive {
		args = append(args, "-r")
	}
	if loadOptions.Noop {
		args = append(args, "-n")
	}
	if loadOptions.KeyLocation != "" {
		args = append(args, "-L", loadOptions.KeyLocation)
	}
	args = append(args, d.Name)
	cmd := command{
		cmd:   Binary,
		ctx:   ctx,
		stdin: loadOptions.KeyReader,
	}
	_, err := cmd.Run(args...)
	return err
}

// UnloadKey unloads the encryption key for this dataset and optionally for child datasets as well.
// See: https://openzfs.github.io/openzfs-docs/man/8/zfs-unload-key.8.html
func (d *Dataset) UnloadKey(ctx context.Context, recursive bool) error {
	args := make([]string, 1, 4)
	args[0] = "unload-key"
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, d.Name)
	return zfs(ctx, args...)
}

// Mount mounts ZFS file systems.
func (d *Dataset) Mount(ctx context.Context, overlay bool, options ...string) (*Dataset, error) {
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
	return GetDataset(ctx, d.Name)
}

// ReceiveOptions are options you can specify to customize the ZFS snapshot reception
type ReceiveOptions struct {
	// When set, uses a rate-limiter to limit the flow to this amount of bytes per second
	BytesPerSecond int64

	// Whether the received snapshot should be resumable on interrupions, or be thrown away
	Resumable bool

	// Properties to be applied to the dataset
	Properties map[string]string
}

func wrapReader(reader io.Reader, bytesPerSecond int64) io.Reader {
	if bytesPerSecond <= 0 {
		return reader
	}
	return ratelimit.Reader(reader, ratelimit.NewBucketWithRate(1, bytesPerSecond))
}

// ReceiveSnapshot receives a ZFS stream from the input io.Reader.
// A new snapshot is created with the specified name, and streams the input data into the newly-created snapshot.
func ReceiveSnapshot(ctx context.Context, input io.Reader, name string, recvOptions ReceiveOptions) (*Dataset, error) {
	c := command{
		cmd:   Binary,
		ctx:   ctx,
		stdin: wrapReader(input, recvOptions.BytesPerSecond),
	}

	args := make([]string, 1, 3)
	args[0] = "receive"
	if recvOptions.Resumable {
		args = append(args, "-s")
	}
	args = append(args, propsSlice(recvOptions.Properties)...)
	args = append(args, name)

	_, err := c.Run(args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(ctx, name)
}

// SendOptions are options you can specify to customize the ZFS send stream
type SendOptions struct {
	// When set, uses a rate-limiter to limit the flow to this amount of bytes per second
	BytesPerSecond int64

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
	IncludeProperties bool
	// Generate an incremental stream from the first snapshot (the incremental source) to the
	//           second snapshot (the incremental target).  The incremental source can be specified as
	//           the last component of the snapshot name (the @ character and following) and it is
	//           assumed to be from the same file system as the incremental target.
	//
	//           If the destination is a clone, the source may be the origin snapshot, which must be
	//           fully specified (for example, pool/fs@origin, not just @origin).
	IncrementalBase *Dataset
}

func wrapWriter(writer io.Writer, bytesPerSecond int64) io.Writer {
	if bytesPerSecond <= 0 {
		return writer
	}
	return ratelimit.Writer(writer, ratelimit.NewBucketWithRate(1, bytesPerSecond))
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
	if sendOptions.IncludeProperties {
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
		stdout: wrapWriter(output, sendOptions.BytesPerSecond),
	}
	args = append([]string{"send"}, args...)
	args = append(args, d.Name)
	_, err := c.Run(args...)
	return err
}

// ResumeSendOptions are options you can specify to customize the ZFS send resume stream
type ResumeSendOptions struct {
	// When set, uses a rate-limiter to limit the flow to this amount of bytes per second
	BytesPerSecond int64
}

// ResumeSend resumes an interrupted ZFS stream of a snapshot to the input io.Writer using the receive_resume_token.
// An error will be returned if the input dataset is not of snapshot type.
func ResumeSend(ctx context.Context, output io.Writer, resumeToken string, sendOptions ResumeSendOptions) error {
	c := command{
		cmd:    Binary,
		ctx:    ctx,
		stdout: wrapWriter(output, sendOptions.BytesPerSecond),
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

	return GetDataset(ctx, name)
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

	return GetDataset(ctx, name)
}

// Snapshots returns a slice of all ZFS snapshots of a given dataset.
func (d *Dataset) Snapshots(ctx context.Context, extraProperties ...string) ([]Dataset, error) {
	return Snapshots(ctx, d.Name, extraProperties...)
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

	return GetDataset(ctx, name)
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
	return GetDataset(ctx, snapName)
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
func (d *Dataset) Children(ctx context.Context, depth uint64, extraProperties ...string) ([]Dataset, error) {
	allFields := append(dsPropList, extraProperties...) // nolint: gocritic

	args := make([]string, 1, 16)
	args[0] = "list"
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

		ds, err := datasetFromFields(fields, extraProperties)
		if err != nil {
			return nil, err
		}
		datasets = append(datasets, *ds)
	}
	return datasets, nil
}
