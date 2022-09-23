package zfs

import (
	"fmt"
	"strconv"
)

// DatasetType is the zfs dataset type
type DatasetType string

// ZFS dataset types, which can indicate if a dataset is a filesystem, snapshot, or volume.
const (
	DatasetAll        DatasetType = "all"
	DatasetFilesystem DatasetType = "filesystem"
	DatasetSnapshot   DatasetType = "snapshot"
	DatasetVolume     DatasetType = "volume"
)

// Dataset is a ZFS dataset.  A dataset could be a clone, filesystem, snapshot, or volume.
// The Type struct member can be used to determine a dataset's type.
//
// The field definitions can be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
type Dataset struct {
	Name          string
	Type          DatasetType
	Origin        string
	Used          uint64
	Avail         uint64
	Mountpoint    string
	Compression   string
	Written       uint64
	Volsize       uint64
	Logicalused   uint64
	Usedbydataset uint64
	Quota         uint64
	Referenced    uint64
	ExtraProps    map[string]string
}

func datasetFromFields(fields, extraProps []string) (*Dataset, error) {
	if len(fields) != len(dsPropList)+len(extraProps) {
		return nil, fmt.Errorf("output invalid: %d fields where %d were expected", len(fields), len(dsPropList)+len(extraProps))
	}

	d := &Dataset{
		Name: fields[0],
		Type: DatasetType(fields[1]),
	}
	fields = setString(&d.Origin, fields[2:])

	fields, err := setUint(&d.Used, fields)
	if err != nil {
		return nil, err
	}
	fields, err = setUint(&d.Avail, fields)
	if err != nil {
		return nil, err
	}
	fields = setString(&d.Mountpoint, fields)
	fields = setString(&d.Compression, fields)
	fields, err = setUint(&d.Volsize, fields)
	if err != nil {
		return nil, err
	}
	fields, err = setUint(&d.Quota, fields)
	if err != nil {
		return nil, err
	}
	fields, err = setUint(&d.Referenced, fields)
	if err != nil {
		return nil, err
	}
	fields, err = setUint(&d.Written, fields)
	if err != nil {
		return nil, err
	}
	fields, err = setUint(&d.Logicalused, fields)
	if err != nil {
		return nil, err
	}
	fields, err = setUint(&d.Usedbydataset, fields)
	if err != nil {
		return nil, err
	}

	d.ExtraProps = make(map[string]string, len(extraProps))
	for i, field := range extraProps {
		d.ExtraProps[field] = fields[i]
	}

	return d, nil
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
