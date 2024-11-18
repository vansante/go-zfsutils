package zfs

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

const (
	datasetNotFoundMessage       = "dataset does not exist"
	resumableErrorMessage        = "resuming stream can be generated on the sending system"
	datasetBusyMessage           = "pool or dataset is busy"
	datasetNoLongerExistsMessage = "no longer exists"
	snapshotHasDependentsMessage = "snapshot has dependent clones"
	keyAlreadyLoadedMessage      = "Key already loaded for"
	keyAlreadyUnloadedMessage    = "Key already unloaded for"
	datasetExistsMessage1        = "destination '"
	datasetExistsMessage2        = "' exists"
)

var (
	// ErrDatasetNotFound is returned when the dataset was not found
	ErrDatasetNotFound = errors.New("dataset not found")

	// ErrDatasetExists is returned when the dataset already exists and overwrite is off
	ErrDatasetExists = errors.New("dataset already exists")

	// ErrOnlySnapshotsSupported is returned when a snapshot only action is executed on another type of dataset
	ErrOnlySnapshotsSupported = errors.New("only snapshots are supported for this action")

	// ErrSnapshotsNotSupported is returned when an unsupported action is executed on a snapshot
	ErrSnapshotsNotSupported = errors.New("snapshots are not supported for this action")

	// ErrPoolOrDatasetBusy is returned when an action fails because ZFS is doing another action
	ErrPoolOrDatasetBusy = errors.New("pool or dataset busy")

	// ErrSnapshotHasDependentClones is returned when the snapshot has dependent clones
	ErrSnapshotHasDependentClones = errors.New("snapshot has dependent clones")

	// ErrKeyAlreadyLoaded is returned when loadnig a key that is already loaded
	ErrKeyAlreadyLoaded = errors.New("key already loaded")

	// ErrKeyAlreadyUnloaded is returned when unloading a key that is already unloaded
	ErrKeyAlreadyUnloaded = errors.New("key is already unloaded")
)

// CommandError is an error which is returned when the `zfs` or `zpool` shell
// commands return with a non-zero exit code.
type CommandError struct {
	Err    error
	Debug  string
	Stderr string
}

// ResumableStreamError is returned when a zfs send is interrupted and contains the token
// with which send can be resumed.
type ResumableStreamError struct {
	CommandError

	ReceiveResumeToken string
}

func createError(cmd *exec.Cmd, stderr string, err error) error {
	switch {
	case strings.Contains(stderr, datasetNotFoundMessage):
		return ErrDatasetNotFound
	case strings.Contains(stderr, datasetBusyMessage):
		idx := strings.LastIndex(stderr, ":")
		if idx > 0 {
			stderr = stderr[:idx]
		}
		return fmt.Errorf("%s: %w", stderr, ErrPoolOrDatasetBusy)
	case strings.Contains(stderr, datasetNoLongerExistsMessage):
		return fmt.Errorf("%s: %w", stderr, ErrDatasetNotFound)
	case strings.Contains(stderr, datasetExistsMessage1) && strings.Contains(stderr, datasetExistsMessage2):
		return fmt.Errorf("%s: %w", stderr, ErrDatasetExists)
	case strings.Contains(stderr, snapshotHasDependentsMessage):
		return fmt.Errorf("%s: %w", stderr, ErrSnapshotHasDependentClones)
	case strings.Contains(stderr, keyAlreadyLoadedMessage):
		return fmt.Errorf("%s: %w", stderr, ErrKeyAlreadyLoaded)
	case strings.Contains(stderr, keyAlreadyUnloadedMessage):
		return fmt.Errorf("%s: %w", stderr, ErrKeyAlreadyUnloaded)
	case strings.Contains(stderr, resumableErrorMessage):
		return &ResumableStreamError{
			CommandError: CommandError{
				Err:    err,
				Debug:  strings.Join(append([]string{cmd.Path}, cmd.Args...), " "),
				Stderr: stderr,
			},
			ReceiveResumeToken: extractStderrResumeToken(stderr),
		}
	}

	return &CommandError{
		Err:    err,
		Debug:  strings.Join(append([]string{cmd.Path}, cmd.Args...), " "),
		Stderr: stderr,
	}
}

// CommandError returns the string representation of an CommandError.
func (e CommandError) Error() string {
	return fmt.Sprintf("%s: %q => %s", e.Err, e.Debug, e.Stderr)
}

// ResumeToken returns the resume token for this send
func (e ResumableStreamError) ResumeToken() string {
	return e.ReceiveResumeToken
}

func extractStderrResumeToken(stderr string) string {
	const search = "zfs send -t"

	idx := strings.LastIndex(stderr, search)
	if idx < 0 {
		return ""
	}
	return strings.TrimSpace(stderr[idx+len(search):])
}
