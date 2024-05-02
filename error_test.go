package zfs

import (
	"errors"
	"fmt"
	"os/exec"
	"testing"
)

func TestError(t *testing.T) {
	tests := []struct {
		err    error
		debug  string
		stderr string
	}{
		// Empty error
		{nil, "", ""},
		// Typical error
		{errors.New("exit status foo"), "/sbin/foo bar qux", "command not found"},
		// Quoted error
		{errors.New("exit status quoted"), "\"/sbin/foo\" bar qux", "\"some\" 'random' `quotes`"},
	}

	for _, test := range tests {
		// Generate error from tests
		zErr := CommandError{
			Err:    test.err,
			Debug:  test.debug,
			Stderr: test.stderr,
		}

		// Verify output format is consistent, so that any changes to the
		// CommandError method must be reflected by the test
		if str := zErr.Error(); str != fmt.Sprintf("%s: %q => %s", test.err, test.debug, test.stderr) {
			t.Fatalf("unexpected CommandError string: %v", str)
		}
	}
}

func Test_createError(t *testing.T) {
	err := createError(
		&exec.Cmd{},
		`exit status 1: "/sbin/zfs zfs umount fe29/252799" => cannot unmount '/disks/252799': pool or dataset is busy`,
		errors.New("test"),
	)

	if !errors.Is(err, ErrPoolOrDatasetBusy) {
		t.Fatalf("unexpected error type: %v", err)
	}
}
