package zfs

import (
	"fmt"
	"strings"
)

const (
	resumableErrorMessage = "resuming stream can be generated on the sending system"
)

// Error is an error which is returned when the `zfs` or `zpool` shell
// commands return with a non-zero exit code.
type Error struct {
	Err    error
	Debug  string
	Stderr string
}

// Error returns the string representation of an Error.
func (e Error) Error() string {
	return fmt.Sprintf("%s: %q => %s", e.Err, e.Debug, e.Stderr)
}

// Resumable returns true if this is a transfer receive error that can be resumed using a token.
func (e Error) Resumable() bool {
	return strings.Contains(e.Stderr, resumableErrorMessage)
}
