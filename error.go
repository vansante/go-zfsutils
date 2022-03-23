package zfs

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

const (
	datasetNotFoundMessage = "dataset does not exist"
	resumableErrorMessage  = "resuming stream can be generated on the sending system"
)

// ErrDatasetNotFound is returned when the dataset was not found
var ErrDatasetNotFound = errors.New("dataset not found")

// CommandError is an error which is returned when the `zfs` or `zpool` shell
// commands return with a non-zero exit code.
type CommandError struct {
	Err    error
	Debug  string
	Stderr string
}

// ResumableStreamError is returned when a zfs send is interrupted and contains the token
// with which the send can be resumed.
type ResumableStreamError struct {
	CommandError

	ReceiveResumeToken string
}

func createError(cmd *exec.Cmd, stderr string, err error) error {
	switch {
	case strings.Contains(stderr, datasetNotFoundMessage):
		return ErrDatasetNotFound
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
