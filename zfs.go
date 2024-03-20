// Package zfs provides wrappers around the ZFS command line tools.
package zfs

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/juju/ratelimit"
)

const (
	Binary = "zfs"
)

// PropertySource specifies the source of a property
type PropertySource string

const (
	PropertySourceLocal     PropertySource = "local"
	PropertySourceDefault                  = "default"
	PropertySourceInherited                = "inherited"
	PropertySourceTemporary                = "temporary"
	PropertySourceReceived                 = "received"
	PropertySourceNone                     = "none"
)

// ListOptions are options you can specify to customize the list command
type ListOptions struct {
	// ParentDataset filters by parent dataset, empty lists all
	ParentDataset string
	// DatasetType filters the results by type
	DatasetType DatasetType
	// ExtraProperties lists the properties to retrieve besides the ones in the Dataset struct (in the ExtraProps key)
	ExtraProperties []string
	// Recursive, if true will list all under the parent dataset
	Recursive bool
	// Depth specifies the depth to go below the parent dataset (or root if no parent)
	Depth int
	// PropertySources is a list of sources to display. Those properties coming from a source other than those in this
	// list are ignored
	PropertySources []PropertySource
}

func (lo ListOptions) propertySourceStrings() []string {
	sl := make([]string, len(lo.PropertySources))
	for i, ps := range lo.PropertySources {
		sl[i] = string(ps)
	}
	return sl
}

// ListDatasets lists the datasets by type and allows you to fetch extra custom fields
func ListDatasets(ctx context.Context, options ListOptions) ([]Dataset, error) {
	args := make([]string, 0, 16)
	args = append(args, "get", "-Hp", "-o", "name,property,value")
	if options.DatasetType != "" {
		args = append(args, "-t", string(options.DatasetType))
	}

	if options.Recursive {
		args = append(args, "-r")
	}

	if options.Depth > 0 {
		args = append(args, "-d", strconv.Itoa(options.Depth))
	}

	if len(options.PropertySources) > 0 {
		if !slices.Contains(options.PropertySources, PropertySourceNone) {
			options.PropertySources = append(options.PropertySources, PropertySourceNone)
		}
		args = append(args, "-s", strings.Join(options.propertySourceStrings(), ","))
	}

	allFields := append(dsPropList, options.ExtraProperties...) // nolint: gocritic
	dsPropListOptions := strings.Join(allFields, ",")
	args = append(args, dsPropListOptions)

	if options.ParentDataset != "" {
		args = append(args, options.ParentDataset)
	}

	out, err := zfsOutput(ctx, args...)
	if err != nil {
		return nil, err
	}

	return readDatasets(out, options.ExtraProperties)
}

// GetDataset retrieves a single ZFS dataset by name.
// This dataset could be any valid ZFS dataset type, such as a clone, filesystem, snapshot, or volume.
func GetDataset(ctx context.Context, name string, extraProperties ...string) (*Dataset, error) {
	ds, err := ListDatasets(ctx, ListOptions{
		ParentDataset:   name,
		Recursive:       false,
		ExtraProperties: extraProperties,
	})
	if err != nil {
		return nil, err
	}

	if len(ds) > 1 {
		return nil, fmt.Errorf("more datasets than expected: %d", len(ds))
	}
	return &ds[0], nil
}

// CloneOptions are options you can specify to customize the clone command
type CloneOptions struct {
	// Properties to be applied to the new dataset
	Properties map[string]string

	// Creates all the non-existing parent datasets. Datasets created in this manner are automatically mounted according
	// to the mountpoint property inherited from their parent. If the target filesystem or volume already exists,
	// the operation completes successfully.
	CreateParents bool
}

// Clone clones a ZFS snapshot and returns a clone dataset.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) Clone(ctx context.Context, dest string, options CloneOptions) (*Dataset, error) {
	if d.Type != DatasetSnapshot {
		return nil, ErrOnlySnapshotsSupported
	}
	args := make([]string, 1, 8)
	args[0] = "clone"
	if options.CreateParents {
		args = append(args, "-p")
	}
	if options.Properties != nil {
		args = append(args, propsSlice(options.Properties)...)
	}
	args = append(args, []string{d.Name, dest}...)

	err := zfs(ctx, args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(ctx, dest)
}

// UnmountOptions are options you can specify to customize the unmount command
type UnmountOptions struct {
	// Forcefully unmount the file system, even if it is currently in use.
	Force bool

	// Unload keys for any encryption roots unmounted by this command.
	UnloadKeys bool
}

// Unmount unmounts currently mounted ZFS file systems.
func (d *Dataset) Unmount(ctx context.Context, options UnmountOptions) (*Dataset, error) {
	if d.Type == DatasetSnapshot {
		return nil, ErrSnapshotsNotSupported
	}
	args := make([]string, 1, 5)
	args[0] = "umount"
	if options.Force {
		args = append(args, "-f")
	}
	if options.UnloadKeys {
		args = append(args, "-u")
	}
	args = append(args, d.Name)

	err := zfs(ctx, args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(ctx, d.Name)
}

// LoadKeyOptions are options you can specify to customize the load-key command
type LoadKeyOptions struct {
	// Recursively loads the keys for the specified filesystem and all descendent encryption roots.
	Recursive bool

	// Do a dry-run (no-op) load-key. This will cause zfs to simply check that the provided key is correct.
	// This command may be run even if the key is already loaded.
	DryRun bool

	// When the key is in a file, load it using this keylocation.
	// This is optional when the ZFS dataset already has this property set.
	KeyLocation string

	// Provide a reader to read the key from stdin
	KeyReader io.Reader
}

// LoadKey loads the encryption key for this and optionally children datasets.
// See: https://openzfs.github.io/openzfs-docs/man/8/zfs-load-key.8.html
func (d *Dataset) LoadKey(ctx context.Context, options LoadKeyOptions) error {
	args := make([]string, 1, 5)
	args[0] = "load-key"
	if options.Recursive {
		args = append(args, "-r")
	}
	if options.DryRun {
		args = append(args, "-n")
	}
	if options.KeyLocation != "" {
		args = append(args, "-L", options.KeyLocation)
	}
	args = append(args, d.Name)
	cmd := command{
		cmd:   Binary,
		ctx:   ctx,
		stdin: options.KeyReader,
	}
	_, err := cmd.Run(args...)
	return err
}

// UnloadKeyOptions are options you can specify to customize the unload-key command
type UnloadKeyOptions struct {
	// Recursively loads the keys for the specified filesystem and all descendent encryption roots.
	Recursive bool
}

// UnloadKey unloads the encryption key for this dataset and optionally for child datasets as well.
// See: https://openzfs.github.io/openzfs-docs/man/8/zfs-unload-key.8.html
func (d *Dataset) UnloadKey(ctx context.Context, options UnloadKeyOptions) error {
	args := make([]string, 1, 3)
	args[0] = "unload-key"
	if options.Recursive {
		args = append(args, "-r")
	}
	args = append(args, d.Name)
	return zfs(ctx, args...)
}

// MountOptions are options you can specify to customize the mount command
type MountOptions struct {
	// Perform an overlay mount. Allows mounting in non-empty mountpoint.
	OverlayMount bool

	// An optional, comma-separated list of mount options to use temporarily for the duration of the mount.
	Options []string

	// Load keys for encrypted filesystems as they are being mounted. This is equivalent to executing zfs load-key
	// on each encryption root before mounting it. Note that if a filesystem has keylocation=prompt, this will cause
	// the terminal to interactively block after asking for the key.
	LoadKeys bool
}

// Mount mounts ZFS file systems.
func (d *Dataset) Mount(ctx context.Context, options MountOptions) (*Dataset, error) {
	if d.Type == DatasetSnapshot {
		return nil, ErrSnapshotsNotSupported
	}
	args := make([]string, 1, 5)
	args[0] = "mount"
	if options.OverlayMount {
		args = append(args, "-O")
	}
	if options.LoadKeys {
		args = append(args, "-l")
	}
	if len(options.Options) > 0 {
		args = append(args, "-o")
		args = append(args, strings.Join(options.Options, ","))
	}
	args = append(args, d.Name)

	err := zfs(ctx, args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(ctx, d.Name)
}

// ReceiveOptions are options you can specify to customize the receive command
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
func ReceiveSnapshot(ctx context.Context, input io.Reader, name string, options ReceiveOptions) (*Dataset, error) {
	c := command{
		cmd:   Binary,
		ctx:   ctx,
		stdin: wrapReader(input, options.BytesPerSecond),
	}

	args := make([]string, 1, 3)
	args[0] = "receive"
	if options.Resumable {
		args = append(args, "-s")
	}
	args = append(args, propsSlice(options.Properties)...)
	args = append(args, name)

	_, err := c.Run(args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(ctx, name)
}

// SendOptions are options you can specify to customize the send command
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
func (d *Dataset) SendSnapshot(ctx context.Context, output io.Writer, options SendOptions) error {
	if d.Type != DatasetSnapshot {
		return ErrOnlySnapshotsSupported
	}

	args := make([]string, 1, 8)
	args[0] = "send"

	if options.Raw {
		args = append(args, "-w")
	}
	if options.IncludeProperties {
		args = append(args, "-p")
	}
	if options.IncrementalBase != nil {
		if options.IncrementalBase.Type != DatasetSnapshot {
			return fmt.Errorf("send base %s: %w", options.IncrementalBase.Name, ErrOnlySnapshotsSupported)
		}
		args = append(args, "-i", options.IncrementalBase.Name)
	}

	c := command{
		cmd:    Binary,
		ctx:    ctx,
		stdout: wrapWriter(output, options.BytesPerSecond),
	}
	args = append(args, d.Name)
	_, err := c.Run(args...)
	return err
}

// ResumeSendOptions are options you can specify to customize the send resume command
type ResumeSendOptions struct {
	// When set, uses a rate-limiter to limit the flow to this amount of bytes per second
	BytesPerSecond int64
}

// ResumeSend resumes an interrupted ZFS stream of a snapshot to the input io.Writer using the receive_resume_token.
// An error will be returned if the input dataset is not of snapshot type.
func ResumeSend(ctx context.Context, output io.Writer, resumeToken string, options ResumeSendOptions) error {
	c := command{
		cmd:    Binary,
		ctx:    ctx,
		stdout: wrapWriter(output, options.BytesPerSecond),
	}
	args := append([]string{"send"}, "-t", resumeToken)
	_, err := c.Run(args...)
	return err
}

// CreateVolumeOptions are options you can specify to customize the create volume command
type CreateVolumeOptions struct {
	// Sets the specified properties as if the command zfs set property=value was invoked at the same time the dataset was created.
	Properties map[string]string

	// Creates all the non-existing parent datasets. Datasets created in this manner are automatically mounted according
	// to the mountpoint property inherited from their parent. Any property specified on the command line using the -o option
	// is ignored. If the target filesystem already exists, the operation completes successfully.
	CreateParents bool

	// Creates a sparse volume with no reservation.
	Sparse bool

	// Do a dry-run creation. No datasets will be created. This is useful in conjunction with the -v or -P flags
	// to validate properties that are passed via -o options and those implied by other options. The actual dataset creation
	// can still fail due to insufficient privileges or available capacity.
	DryRun bool

	// Provide input to stdin, for instance for loading keys
	Stdin io.Reader
}

// CreateVolume creates a new ZFS volume with the specified name, size, and properties.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func CreateVolume(ctx context.Context, name string, size uint64, options CreateVolumeOptions) (*Dataset, error) {
	args := make([]string, 3, 10)
	args[0] = "create"
	args[1] = "-V"
	args[2] = strconv.FormatUint(size, 10)

	if options.Properties != nil {
		args = append(args, propsSlice(options.Properties)...)
	}
	if options.CreateParents {
		args = append(args, "-p")
	}
	if options.Sparse {
		args = append(args, "-s")
	}
	if options.DryRun {
		args = append(args, "-n")
	}

	args = append(args, name)

	cmd := command{
		cmd:   Binary,
		ctx:   ctx,
		stdin: options.Stdin,
	}
	_, err := cmd.Run(args...)
	if err != nil {
		return nil, err
	}

	return GetDataset(ctx, name)
}

// DestroyOptions are options you can specify to customize the destroy command
type DestroyOptions struct {
	// Recursively destroy all children.
	Recursive bool

	// Recursively destroy all dependents, including cloned file systems outside the target hierarchy.
	RecursiveClones bool

	// Forcibly unmount file systems. This option has no effect on non-file systems or unmounted file systems.
	Force bool

	// Do a dry-run (no-op) deletion. No data will be deleted.
	DryRun bool

	// Only for snapshots. Destroy immediately. If a snapshot cannot be destroyed now, mark it for deferred destruction.
	Defer bool
}

// Destroy destroys a ZFS dataset.
// If the destroy bit flag is set, any descendents of the dataset will be recursively destroyed, including snapshots.
// If the deferred bit flag is set, the snapshot is marked for deferred deletion.
func (d *Dataset) Destroy(ctx context.Context, options DestroyOptions) error {
	args := make([]string, 1, 6)
	args[0] = "destroy"
	if options.Recursive {
		args = append(args, "-r")
	}
	if options.RecursiveClones {
		args = append(args, "-R")
	}
	if options.Defer {
		args = append(args, "-d")
	}
	if options.Force {
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

// RenameOptions are options you can specify to customize the rename command
type RenameOptions struct {
	// Creates all the nonexistent parent datasets. Datasets created in this manner are automatically mounted
	// according to the mountpoint property inherited from their parent.
	CreateParent bool

	// Recursively rename the snapshots of all descendent datasets. Snapshots are the only dataset that can
	// be renamed recursively.
	Recursive bool

	// Do not remount file systems during rename. If a file system's mountpoint property is set to legacy or none,
	// the file system is not unmounted even if this option is not given.
	NoMount bool

	// Force unmount any file systems that need to be unmounted in the process. This flag has no effect if used together
	// with the no mount flag.
	Force bool
}

// Rename renames a dataset.
func (d *Dataset) Rename(ctx context.Context, name string, options RenameOptions) (*Dataset, error) {
	args := make([]string, 1, 6)
	args[0] = "rename"
	if options.CreateParent {
		args = append(args, "-p")
	}
	if options.Recursive {
		args = append(args, "-r")
	}
	if options.NoMount {
		args = append(args, "-u")
	}
	if options.Force {
		args = append(args, "-f")
	}

	args = append(args, d.Name)
	args = append(args, name)

	err := zfs(ctx, args...)
	if err != nil {
		return d, err
	}

	return GetDataset(ctx, name)
}

// Snapshots returns a slice of all ZFS snapshots of a given dataset.
func (d *Dataset) Snapshots(ctx context.Context, options ListOptions) ([]Dataset, error) {
	options.ParentDataset = d.Name
	options.DatasetType = DatasetSnapshot
	options.Recursive = true
	return ListDatasets(ctx, options)
}

// CreateFilesystemOptions are options you can specify to customize the create filesystem command
type CreateFilesystemOptions struct {
	// Sets the specified properties as if the command zfs set property=value was invoked at the same time the dataset was created.
	Properties map[string]string

	// Creates all the non-existing parent datasets. Datasets created in this manner are automatically mounted according
	// to the mountpoint property inherited from their parent. Any property specified on the command line using the -o option
	// is ignored. If the target filesystem already exists, the operation completes successfully.
	CreateParents bool

	// Do a dry-run creation. No datasets will be created. This is useful in conjunction with the -v or -P flags
	// to validate properties that are passed via -o options and those implied by other options. The actual dataset creation
	// can still fail due to insufficient privileges or available capacity.
	DryRun bool

	// Do not mount the newly created file system.
	NoMount bool

	// Provide input to stdin, for instance for loading keys
	Stdin io.Reader
}

// CreateFilesystem creates a new ZFS filesystem with the specified name and properties.
//
// A full list of available ZFS properties may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
func CreateFilesystem(ctx context.Context, name string, options CreateFilesystemOptions) (*Dataset, error) {
	args := make([]string, 1, 10)
	args[0] = "create"

	if options.Properties != nil {
		args = append(args, propsSlice(options.Properties)...)
	}

	if options.CreateParents {
		args = append(args, "-p")
	}
	if options.DryRun {
		args = append(args, "-n")
	}
	if options.NoMount {
		args = append(args, "-u")
	}

	args = append(args, name)

	cmd := command{
		cmd:   Binary,
		ctx:   ctx,
		stdin: options.Stdin,
	}
	_, err := cmd.Run(args...)
	if err != nil {
		return nil, err
	}

	return GetDataset(ctx, name)
}

// SnapshotOptions are options you can specify to customize the snapshot command
type SnapshotOptions struct {
	// Sets the specified properties on the snapshot.
	Properties map[string]string

	// Recursively create snapshots of all descendent datasets.
	Recursive bool
}

// Snapshot creates a new ZFS snapshot of the receiving dataset, using the specified name.
// Optionally, the snapshot can be taken recursively, creating snapshots of all descendent filesystems in a single, atomic operation.
func (d *Dataset) Snapshot(ctx context.Context, name string, options SnapshotOptions) (*Dataset, error) {
	args := make([]string, 1, 10)
	args[0] = "snapshot"
	if options.Recursive {
		args = append(args, "-r")
	}
	if options.Properties != nil {
		args = append(args, propsSlice(options.Properties)...)
	}

	snapName := fmt.Sprintf("%s@%s", d.Name, name)
	args = append(args, snapName)

	err := zfs(ctx, args...)
	if err != nil {
		return nil, err
	}
	return GetDataset(ctx, snapName)
}

// RollbackOptions are options you can specify to customize the rollback command
type RollbackOptions struct {
	// Destroy any snapshots and bookmarks more recent than the one specified.
	DestroyMoreRecent bool

	// Destroy any more recent snapshots and bookmarks, as well as any clones of those snapshots.
	DestroyMoreRecentClones bool

	// Used with the DestroyMoreRecentClones option to force an unmount of any clone file systems that are to be destroyed.
	Force bool
}

// Rollback rolls back the receiving ZFS dataset to a previous snapshot.
// Optionally, intermediate snapshots can be destroyed.
// A ZFS snapshot rollback cannot be completed without this option, if more recent snapshots exist.
// An error will be returned if the input dataset is not of snapshot type.
func (d *Dataset) Rollback(ctx context.Context, options RollbackOptions) error {
	if d.Type != DatasetSnapshot {
		return ErrOnlySnapshotsSupported
	}

	args := make([]string, 1, 5)
	args[0] = "rollback"
	if options.DestroyMoreRecent {
		args = append(args, "-r")
	}
	if options.DestroyMoreRecentClones {
		args = append(args, "-R")
	}
	if options.Force {
		args = append(args, "-f")
	}
	args = append(args, d.Name)

	return zfs(ctx, args...)
}

// Children returns a slice of children of the receiving ZFS dataset.
// A recursion depth may be specified, or a depth of 0 allows unlimited recursion.
func (d *Dataset) Children(ctx context.Context, options ListOptions) ([]Dataset, error) {
	options.ParentDataset = d.Name
	options.Recursive = true
	ds, err := ListDatasets(ctx, options)
	if err != nil {
		return nil, err
	}
	// Skip the first parent entry, because we are looking for its children
	return ds[1:], nil
}
