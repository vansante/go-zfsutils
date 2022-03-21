package jobrunner

import (
	"github.com/vansante/go-zfs"
	"strconv"
	"time"
)

func parseDatasetTimeProperty(ds *zfs.Dataset, prop string) (time.Time, error) {
	return time.Parse(dateTimeFormat, ds.ExtraProps[prop])
}

func parseDatasetIntProperty(ds *zfs.Dataset, prop string) (int64, error) {
	return strconv.ParseInt(ds.ExtraProps[prop], 10, 64)
}
