package zfs

import (
	"fmt"
	"strconv"
	"strings"
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
	Name          string            `json:"Name"`
	Type          DatasetType       `json:"Type"`
	Origin        string            `json:"Origin"`
	Used          uint64            `json:"Used"`
	Available     uint64            `json:"Available"`
	Mountpoint    string            `json:"Mountpoint"`
	Compression   string            `json:"Compression"`
	Written       uint64            `json:"Written"`
	Volsize       uint64            `json:"Volsize"`
	Logicalused   uint64            `json:"Logicalused"`
	Usedbydataset uint64            `json:"Usedbydataset"`
	Quota         uint64            `json:"Quota"`
	Refquota      uint64            `json:"Refquota"`
	Referenced    uint64            `json:"Referenced"`
	ExtraProps    map[string]string `json:"ExtraProps"`
}

const (
	nameField = iota
	propertyField
	valueField
)

func readDatasets(output [][]string, extraProps []string) ([]Dataset, error) {
	multiple := len(dsPropList) + len(extraProps)
	if len(output)%multiple != 0 {
		return nil, fmt.Errorf("output invalid: %d lines where a multiple of %d was expected", len(output), multiple)
	}

	count := len(output) / (len(dsPropList) + len(extraProps))
	curDataset := 0
	datasets := make([]Dataset, count)
	for i, fields := range output {
		if len(fields) != 3 {
			return nil, fmt.Errorf("output contains line with %d fields: %s", len(fields), strings.Join(fields, " "))
		}

		if i > 0 && fields[nameField] != datasets[curDataset].Name {
			curDataset++
		}

		ds := &datasets[curDataset]
		ds.ExtraProps = make(map[string]string, len(extraProps))
		ds.Name = fields[nameField]

		val := fields[valueField]

		var setError error
		switch fields[propertyField] {
		case PropertyName:
			ds.Name = val
		case PropertyType:
			ds.Type = DatasetType(val)
		case PropertyOrigin:
			setString(&ds.Origin, val)
		case PropertyUsed:
			setError = setUint(&ds.Used, val)
		case PropertyAvailable:
			setError = setUint(&ds.Available, val)
		case PropertyMountPoint:
			setString(&ds.Mountpoint, val)
		case PropertyCompression:
			setString(&ds.Compression, val)
		case PropertyWritten:
			setError = setUint(&ds.Written, val)
		case PropertyVolSize:
			setError = setUint(&ds.Volsize, val)
		case PropertyLogicalUsed:
			setError = setUint(&ds.Logicalused, val)
		case PropertyUsedByDataset:
			setError = setUint(&ds.Usedbydataset, val)
		case PropertyQuota:
			setError = setUint(&ds.Quota, val)
		case PropertyRefQuota:
			setError = setUint(&ds.Refquota, val)
		case PropertyReferenced:
			setError = setUint(&ds.Referenced, val)
		default:
			ds.ExtraProps[fields[propertyField]] = val
		}
		if setError != nil {
			return nil, fmt.Errorf("error in dataset %d field %s [%s]: %w", curDataset, fields[propertyField], fields[valueField], setError)
		}
	}

	return datasets, nil
}

func setString(field *string, val string) {
	if val == PropertyUnset {
		return
	}
	*field = val
}

func setUint(field *uint64, val string) error {
	if val == PropertyUnset {
		return nil
	}

	v, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return err
	}
	*field = v
	return nil
}
