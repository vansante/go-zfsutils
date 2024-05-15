package zfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

// List of HTTPConfig properties to retrieve from zfs list command by default
var dsPropList = []string{
	PropertyName,
	PropertyType,
	PropertyOrigin,
	PropertyUsed,
	PropertyAvailable,
	PropertyMounted,
	PropertyMountPoint,
	PropertyCompression,
	PropertyVolSize,
	PropertyQuota,
	PropertyRefQuota,
	PropertyReferenced,
	PropertyWritten,
	PropertyLogicalUsed,
	PropertyUsedByDataset,
}

const (
	fieldSeparator = "\t"
)

// zfs is a helper function to wrap typical calls to zfs that ignores stdout.
func zfs(ctx context.Context, arg ...string) error {
	_, err := zfsOutput(ctx, arg...)
	return err
}

// zfs is a helper function to wrap typical calls to zfs.
func zfsOutput(ctx context.Context, arg ...string) ([][]string, error) {
	c := command{
		cmd: Binary,
		ctx: ctx,
	}
	return c.Run(arg...)
}

type command struct {
	ctx    context.Context
	cmd    string
	stdin  io.Reader
	stdout io.Writer
}

func (c *command) Run(arg ...string) ([][]string, error) {
	cmd := exec.CommandContext(c.ctx, c.cmd, arg...)
	cmd.SysProcAttr = procAttributes()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = c.stdout
	cmd.Stderr = &stderr
	if c.stdout == nil {
		cmd.Stdout = &stdout
	}
	if c.stdin != nil {
		cmd.Stdin = c.stdin
	}

	err := cmd.Run()
	if err != nil {
		return nil, createError(cmd, stderr.String(), err)
	}

	// assume if you passed in something for stdout, that you know what to do with it
	if c.stdout != nil {
		return nil, nil
	}

	return splitOutput(stdout.String()), nil
}

func splitOutput(out string) [][]string {
	lines := strings.Split(out, "\n")

	// last line is always blank
	lines = lines[0 : len(lines)-1]
	output := make([][]string, len(lines))
	for i, l := range lines {
		output[i] = strings.Split(l, fieldSeparator)
	}
	return output
}

func propsSlice(properties map[string]string) []string {
	args := make([]string, 0, len(properties)*2)
	for k, v := range properties {
		args = append(args, "-o")
		args = append(args, fmt.Sprintf("%s=%s", k, v))
	}
	return args
}
