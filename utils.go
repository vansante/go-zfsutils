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
	"name",
	"origin",
	"used",
	"available",
	"mountpoint",
	"compression",
	"type",
	"volsize",
	"quota",
	"referenced",
	"written",
	"logicalused",
	"usedbydataset",
}

type command struct {
	Command string
	Stdin   io.Reader
	Stdout  io.Writer
}

func (c *command) Run(arg ...string) ([][]string, error) {
	cmd := exec.Command(c.Command, arg...)

	var stdout, stderr bytes.Buffer
	if c.Stdout == nil {
		cmd.Stdout = &stdout
	} else {
		cmd.Stdout = c.Stdout
	}

	if c.Stdin != nil {
		cmd.Stdin = c.Stdin
	}
	cmd.Stderr = &stderr

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

func setString(field *string, value string) {
	v := ""
	if value != "-" {
		v = value
	}
	*field = v
}

func setUint(field *uint64, value string) error {
	var v uint64
	if value != "-" {
		var err error
		v, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
	}
	*field = v
	return nil
}

func (d *Dataset) parseLine(line []string, extraFields []string) error {
	var err error
	if len(line) != len(dsPropList)+len(extraFields) {
		return errors.New("output does not match what is expected on this platform")
	}

	setString(&d.Name, line[0])
	setString(&d.Origin, line[1])

	if err = setUint(&d.Used, line[2]); err != nil {
		return err
	}
	if err = setUint(&d.Avail, line[3]); err != nil {
		return err
	}

	setString(&d.Mountpoint, line[4])
	setString(&d.Compression, line[5])
	setString(&d.Type, line[6])

	if err = setUint(&d.Volsize, line[7]); err != nil {
		return err
	}
	if err = setUint(&d.Quota, line[8]); err != nil {
		return err
	}
	if err = setUint(&d.Referenced, line[9]); err != nil {
		return err
	}
	if err = setUint(&d.Written, line[10]); err != nil {
		return err
	}
	if err = setUint(&d.Logicalused, line[11]); err != nil {
		return err
	}
	if err = setUint(&d.Usedbydataset, line[12]); err != nil {
		return err
	}

	d.ExtraProps = make(map[string]string, len(extraFields))
	for i, field := range extraFields {
		d.ExtraProps[field] = line[i+13]
	}
	return nil
}

// ListByType lists the datasets by type and allows you to fetch extra custom fields
func ListByType(t, filter string, extraFields []string) ([]*Dataset, error) {
	fields := append(dsPropList, extraFields...) // nolint: gocritic

	dsPropListOptions := strings.Join(fields, ",")
	args := []string{"list", "-rHp", "-t", t, "-o", dsPropListOptions}
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
