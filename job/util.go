package job

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	zfs "github.com/vansante/go-zfsutils"
)

// propertyIsSet returns whether a property is set
func propertyIsSet(val string) bool {
	return val != "" && val != zfs.ValueUnset
}

func propertyIsBefore(val string, tm time.Time) bool {
	dsTime, err := time.Parse(dateTimeFormat, val)
	if err != nil {
		return false
	}
	return dsTime.Before(tm)
}

func isContextError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func parseDatasetTimeProperty(ds *zfs.Dataset, prop string) (time.Time, error) {
	return time.Parse(dateTimeFormat, ds.ExtraProps[prop])
}

func parseDatasetIntProperty(ds *zfs.Dataset, prop string) (int64, error) {
	return strconv.ParseInt(ds.ExtraProps[prop], 10, 64)
}

func datasetName(name string, stripSnap bool) string {
	idx := strings.LastIndex(name, "/")
	if idx < 0 {
		return name
	}
	name = name[idx+1:]
	if !stripSnap {
		return name
	}

	idx = strings.Index(name, "@")
	if idx < 0 {
		return name
	}
	return name[:idx]
}

func stripDatasetSnapshot(name string) string {
	idx := strings.Index(name, "@")
	if idx < 0 {
		return name
	}
	return name[:idx]
}

func snapshotName(name string) string {
	idx := strings.LastIndex(name, "@")
	if idx < 0 {
		return name
	}
	return name[idx+1:]
}

func filterSnapshotsWithoutProp(list []zfs.Dataset, prop string) []zfs.Dataset {
	nwList := make([]zfs.Dataset, 0, len(list))
	for _, snap := range list {
		if !propertyIsSet(snap.ExtraProps[prop]) {
			continue
		}
		nwList = append(nwList, snap)
	}
	return nwList
}

func snapshotsContain(list []zfs.Dataset, dataset, snapshot string) bool {
	for _, ds := range list {
		if datasetName(ds.Name, false) == fmt.Sprintf("%s@%s", dataset, snapshot) {
			return true
		}
	}
	return false
}

// randomizeDuration adds or removes up to 5% of the duration to randomize background routine wake up times
func randomizeDuration(d time.Duration) time.Duration { // nolint:unparam
	// Subtract 1/40th of duration d
	d -= (d / 40).Truncate(time.Second)
	// Generate a random duration of zero to 1/20th of duration d
	rnd := time.Duration(rand.Int63n(int64(d / 20))).Truncate(time.Second)
	// then add it to d, giving a duration of duration d plus or minus 1/20th
	return d + rnd
}
