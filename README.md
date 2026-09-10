# go-zfsutils

Go wrappers around the ZFS command line tools, plus an HTTP server/client for replicating snapshots between
machines and a job runner that automates snapshot creation, sending and pruning.

The library shells out to the `zfs` binary and parses its output; there is no CGO or libzfs dependency.

```
go get github.com/vansante/go-zfsutils
```

## Packages

| Package | Import path | Purpose |
| --- | --- | --- |
| `zfs` | `github.com/vansante/go-zfsutils` | Dataset primitives: list, create, snapshot, clone, send/receive, properties, mount, destroy. |
| `http` | `github.com/vansante/go-zfsutils/http` | HTTP handlers exposing datasets and snapshots over the network, and a matching client. |
| `job` | `github.com/vansante/go-zfsutils/job` | Runner that periodically creates, replicates, marks and prunes snapshots based on ZFS properties. |

## The `zfs` package

Every call takes a `context.Context`, and every command has an `Options` struct mirroring the flags of the
underlying `zfs` subcommand, so unused options can be left at their zero value.

```go
ctx := context.Background()

fs, err := zfs.CreateFilesystem(ctx, "tank/data", zfs.CreateFilesystemOptions{
    Properties:    map[string]string{"compression": "lz4"},
    CreateParents: true,
})

snap, err := fs.Snapshot(ctx, "backup-1", zfs.SnapshotOptions{})

datasets, err := zfs.ListFilesystems(ctx, zfs.ListOptions{
    ParentDataset:   "tank",
    ExtraProperties: []string{"com.github.vansante:snapshot-created-at"},
})
```

`Dataset` holds the commonly used properties as typed fields (`Used`, `Available`, `Mountpoint`,
`Referenced`, …). Anything requested through `ListOptions.ExtraProperties` ends up as a string in
`Dataset.ExtraProps`, which is how the `job` package stores its own bookkeeping.

### Sending and receiving

`SendSnapshot` writes a stream to any `io.Writer` and `ReceiveSnapshot` reads one from any `io.Reader`.
Both can optionally apply zstd compression and a bytes-per-second rate limit, and sends can be incremental:

```go
err := snap.SendSnapshot(ctx, w, zfs.SendOptions{
    Raw:              true,
    IncrementalBase:  previousSnap,
    CompressionLevel: zstd.SpeedFastest,
    BytesPerSecond:   10 * 1024 * 1024,
})

received, err := zfs.ReceiveSnapshot(ctx, r, "tank/restored@backup-1", zfs.ReceiveOptions{
    Resumable:           true,
    EnableDecompression: true,
})
```

When a resumable send is interrupted, the returned error is a `*zfs.ResumableStreamError` carrying the
`receive_resume_token`; pass it to `zfs.ResumeSend` to continue where the transfer left off.

### Errors

Common ZFS failures are translated from stderr into sentinel errors that can be matched with `errors.Is`,
including `ErrDatasetNotFound`, `ErrDatasetExists`, `ErrPoolOrDatasetBusy`, `ErrPoolIOSuspended`,
`ErrSnapshotHasDependentClones`, `ErrKeyAlreadyLoaded` and `ErrFilesystemAlreadyMounted`. Anything else is
wrapped in a `*zfs.CommandError` that keeps the executed command and its stderr for debugging.

## The `http` package

`http.NewHTTP` returns an `http.Handler` that exposes filesystems and snapshots under a configurable path
prefix, with routes for listing, creating and destroying snapshots and for streaming them in either
direction (including resuming a partial transfer by token).

```go
conf := zfshttp.Config{}
conf.ApplyDefaults()
conf.ParentDataset = "tank/backups"
conf.Permissions.AllowDestroySnapshots = true

server := zfshttp.NewHTTP(ctx, conf, logger)
_ = http.ListenAndServe(":7654", server)
```

`Config.Permissions` gates the operations a remote caller may perform, and `MaximumConcurrentReceives`
and `SpeedBytesPerSecond` bound the resources a single peer can consume. `zfshttp.NewClient` speaks the
same API from the other side, returning `SendResult` statistics for each transfer.

## The `job` package

The runner drives replication from ZFS properties rather than from a static schedule, so datasets opt in
by setting properties in a configurable namespace (`com.github.vansante` by default): a snapshot interval,
a destination to send to, retention counts and durations, and a lock property to temporarily exclude a
dataset. Separate goroutines then create snapshots, send them to their configured target, mark expired
snapshots for deletion and prune them, and optionally prune whole filesystems.

```go
conf := job.Config{}
conf.ApplyDefaults()
conf.ParentDataset = "tank/data"

runner := job.NewRunner(ctx, conf, logger)
runner.AddListener(job.SentSnapshotEvent, func(args ...any) { /* ... */ })
runner.Run()
```

Progress is reported through an event emitter (`CreatedSnapshotEvent`, `StartSendingSnapshotEvent`,
`SnapshotSendingProgressEvent`, `SentSnapshotEvent`, `DeletedSnapshotEvent`, and others), and
`Runner.ListCurrentSends` reports the transfers currently in flight.

## Testing

Sudo permissions are required to run `zpool` commands unfortunately. The tests create a test zpool using
some files in `/tmp`.

The `TestZPool` helper creates the pool, grants the required delegations with `zfs allow everyone`, runs
the test body and destroys the pool afterwards. A machine with ZFS installed is therefore needed; the
included `Dockerfile` provides an Ubuntu image with `zfsutils-linux` and Go for that purpose.

```
go test ./...
```

## License

Apache License 2.0, see [LICENSE](LICENSE).
