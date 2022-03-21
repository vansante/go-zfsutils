package zfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
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

	joinedArgs := strings.Join(cmd.Args, " ")
	err := cmd.Run()
	if err != nil {
		return nil, &Error{
			Err:    err,
			Debug:  strings.Join([]string{cmd.Path, joinedArgs[1:]}, " "),
			Stderr: stderr.String(),
		}
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

func setString(field *string, values []string) []string {
	val, values := values[0], values[1:]
	if val == PropertyUnset {
		return values
	}
	*field = val
	return values
}

func setUint(field *uint64, values []string) ([]string, error) {
	var val string
	val, values = values[0], values[1:]
	if val == PropertyUnset {
		return values, nil
	}

	v, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return values, err
	}

	*field = v
	return values, nil
}

func (d *Dataset) parseLine(lines []string, extraFields []string) error {
	if len(lines) != len(dsPropList)+len(extraFields) {
		return errors.New("output does not match what is expected on this platform")
	}

	lines = setString(&d.Name, lines)

	d.Type = DatasetType(lines[0])
	lines = lines[1:]

	lines = setString(&d.Origin, lines)
	lines, err := setUint(&d.Used, lines)
	if err != nil {
		return err
	}
	lines, err = setUint(&d.Avail, lines)
	if err != nil {
		return err
	}
	lines = setString(&d.Mountpoint, lines)
	lines = setString(&d.Compression, lines)
	lines, err = setUint(&d.Volsize, lines)
	if err != nil {
		return err
	}
	lines, err = setUint(&d.Quota, lines)
	if err != nil {
		return err
	}
	lines, err = setUint(&d.Referenced, lines)
	if err != nil {
		return err
	}
	lines, err = setUint(&d.Written, lines)
	if err != nil {
		return err
	}
	lines, err = setUint(&d.Logicalused, lines)
	if err != nil {
		return err
	}
	lines, err = setUint(&d.Usedbydataset, lines)
	if err != nil {
		return err
	}

	d.ExtraProps = make(map[string]string, len(extraFields))
	for i, field := range extraFields {
		d.ExtraProps[field] = lines[i]
	}
	return nil
}

// ListByType lists the datasets by type and allows you to fetch extra custom fields
func ListByType(t DatasetType, filter string, extraFields []string) ([]*Dataset, error) {
	fields := append(dsPropList, extraFields...) // nolint: gocritic

	dsPropListOptions := strings.Join(fields, ",")
	args := []string{"list", "-rHp", "-t", string(t), "-o", dsPropListOptions}
	if filter != "" {
		args = append(args, filter)
	}

	out, err := zfsOutput(args...)
	if err != nil {
		return nil, err
	}

	name := ""
	var datasets []*Dataset
	var ds *Dataset
	for _, line := range out {
		if name != line[0] {
			name = line[0]
			ds = &Dataset{Name: name}
			datasets = append(datasets, ds)
		}

		err := ds.parseLine(line, extraFields)
		if err != nil {
			return nil, err
		}
	}

	return datasets, nil
}

func propsSlice(properties map[string]string) []string {
	args := make([]string, 0, len(properties)*3)
	for k, v := range properties {
		args = append(args, "-o")
		args = append(args, fmt.Sprintf("%s=%s", k, v))
	}
	return args
}
