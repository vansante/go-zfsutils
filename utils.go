package zfs

import (
	"bytes"
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
	PropertyMountPoint,
	PropertyCompression,
	PropertyVolSize,
	PropertyQuota,
	PropertyReferenced,
	PropertyWritten,
	PropertyLogicalUsed,
	PropertyUsedByDataset,
}

type command struct {
	Command string
	Stdin   io.Reader
	Stdout  io.Writer
}

func (c *command) Run(arg ...string) ([][]string, error) {
	cmd := exec.Command(c.Command, arg...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = c.Stdout
	cmd.Stderr = &stderr
	if c.Stdout == nil {
		cmd.Stdout = &stdout
	}
	if c.Stdin != nil {
		cmd.Stdin = c.Stdin
	}

	err := cmd.Run()
	if err != nil {
		return nil, createError(cmd, stderr.String(), err)
	}

	// assume if you passed in something for stdout, that you know what to do with it
	if c.Stdout != nil {
		return nil, nil
	}

	lines := strings.Split(stdout.String(), "\n")

	// last line is always blank
	lines = lines[0 : len(lines)-1]
	output := make([][]string, len(lines))

	for i, l := range lines {
		output[i] = strings.Fields(l)
	}

	return output, nil
}

func propsSlice(properties map[string]string) []string {
	args := make([]string, 0, len(properties)*3)
	for k, v := range properties {
		args = append(args, "-o")
		args = append(args, fmt.Sprintf("%s=%s", k, v))
	}
	return args
}
