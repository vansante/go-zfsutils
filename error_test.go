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
		{ErrFilesystemAlreadyMounted, "does not matter", "cannot mount 'fh27/115125': filesystem already mounted"},
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

func Test_createError_branches(t *testing.T) {
	tests := []struct {
		name    string
		stderr  string
		wantErr error
	}{
		{
			"datasetNotFound",
			"cannot open 'testpool/nope': dataset does not exist",
			ErrDatasetNotFound,
		},
		{
			"datasetBusy",
			"cannot unmount '/disks/252799': pool or dataset is busy",
			ErrPoolOrDatasetBusy,
		},
		{
			"poolIOSuspended",
			"cannot open 'testpool': pool I/O is currently suspended",
			ErrPoolIOSuspended,
		},
		{
			"datasetNoLongerExists",
			"cannot iterate filesystems: dataset testpool/fs1 no longer exists",
			ErrDatasetNotFound,
		},
		{
			"datasetExists",
			"cannot create 'testpool/fs1': dataset already exists",
			ErrDatasetExists,
		},
		{
			"destinationExists",
			"cannot receive new filesystem stream: destination 'testpool/fs1' exists",
			ErrDatasetExists,
		},
		{
			"snapshotHasDependentClones",
			"cannot destroy 'testpool/fs1@snap': snapshot has dependent clones",
			ErrSnapshotHasDependentClones,
		},
		{
			"keyAlreadyLoaded",
			"Key load error: Key already loaded for 'testpool/fs1'.",
			ErrKeyAlreadyLoaded,
		},
		{
			"keyAlreadyUnloaded",
			"Key unload error: Key already unloaded for 'testpool/fs1'.",
			ErrKeyAlreadyUnloaded,
		},
		{
			"filesystemAlreadyMounted",
			"cannot mount 'testpool/fs1': filesystem already mounted",
			ErrFilesystemAlreadyMounted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := createError(&exec.Cmd{}, tt.stderr, errors.New("exit status 1"))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("createError() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func Test_createError_resumable(t *testing.T) {
	const token = "1-1162e8fb98-e0-789c636064000310a500c4ec50360710e72765a52697303030419460caa7a515a796806474e0f26c48f2499525a9c5"

	stderr := fmt.Sprintf(
		"warning: cannot send 'testpool/fs1@snap': signal received\n"+
			"a resuming stream can be generated on the sending system by running:\n    zfs send -t %s",
		token,
	)

	err := createError(&exec.Cmd{}, stderr, errors.New("exit status 1"))

	var resumeErr *ResumableStreamError
	if !errors.As(err, &resumeErr) {
		t.Fatalf("createError() = %T, want *ResumableStreamError", err)
	}
	if resumeErr.ResumeToken() != token {
		t.Fatalf("ResumeToken() = %v, want %v", resumeErr.ResumeToken(), token)
	}
}

func Test_createError_unmatched(t *testing.T) {
	err := createError(&exec.Cmd{}, "something else went wrong", errors.New("exit status 1"))

	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("createError() = %T, want *CommandError", err)
	}
	if cmdErr.Stderr != "something else went wrong" {
		t.Fatalf("unexpected stderr: %v", cmdErr.Stderr)
	}
}

func Test_extractStderrResumeToken(t *testing.T) {
	tests := []struct {
		name   string
		stderr string
		want   string
	}{
		{
			"token",
			"a resuming stream can be generated on the sending system by running:\n    zfs send -t 1-1162e8fb98-e0",
			"1-1162e8fb98-e0",
		},
		{
			"noToken",
			"warning: cannot send 'testpool/fs1@snap': signal received",
			"",
		},
		{
			"empty",
			"",
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractStderrResumeToken(tt.stderr); got != tt.want {
				t.Fatalf("extractStderrResumeToken() = %q, want %q", got, tt.want)
			}
		})
	}
}
