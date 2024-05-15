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
	Mounted       bool              `json:"Mounted"`
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
		return nil, fmt.Errorf("output invalid: %d lines where a multiple of %d was expected: %s",
			len(output), multiple, strings.Join(output[0], " "),
		)
	}

	count := len(output) / multiple
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
		ds.Name = fields[nameField]
		// Init extra props if needed
		if ds.ExtraProps == nil {
			ds.ExtraProps = make(map[string]string, len(extraProps))
		}

		prop := fields[propertyField]
		val := fields[valueField]

		var setError error
		switch prop {
		case PropertyName:
			ds.Name = val
		case PropertyType:
			ds.Type = DatasetType(val)
		case PropertyOrigin:
			ds.Origin = setString(val)
		case PropertyUsed:
			ds.Used, setError = setUint(val)
		case PropertyAvailable:
			ds.Available, setError = setUint(val)
		case PropertyMounted:
			ds.Mounted, setError = setBool(val)
		case PropertyMountPoint:
			ds.Mountpoint = setString(val)
		case PropertyCompression:
			ds.Compression = setString(val)
		case PropertyWritten:
			ds.Written, setError = setUint(val)
		case PropertyVolSize:
			ds.Volsize, setError = setUint(val)
		case PropertyLogicalUsed:
			ds.Logicalused, setError = setUint(val)
		case PropertyUsedByDataset:
			ds.Usedbydataset, setError = setUint(val)
		case PropertyQuota:
			ds.Quota, setError = setUint(val)
		case PropertyRefQuota:
			ds.Refquota, setError = setUint(val)
		case PropertyReferenced:
			ds.Referenced, setError = setUint(val)
		default:
			if val == PropertyUnset {
				ds.ExtraProps[prop] = ""
				continue
			}
			ds.ExtraProps[prop] = val
		}
		if setError != nil {
			return nil, fmt.Errorf("error in dataset %d (%s) field %s [%s]: %w", curDataset, ds.Name, prop, val, setError)
		}
	}

	return datasets, nil
}

func setString(val string) string {
	if val == PropertyUnset {
		return ""
	}
	return val
}

func setUint(val string) (uint64, error) {
	if val == PropertyUnset {
		return 0, nil
	}

	v, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func setBool(val string) (bool, error) {
	if val == PropertyUnset {
		return false, nil
	}

	return val == PropertyYes || val == PropertyOn, nil
}
