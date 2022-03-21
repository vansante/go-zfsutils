package jobrunner

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vansante/go-zfs"
	zfshttp "github.com/vansante/go-zfs/http"
)

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

func snapshotName(name string) string {
	idx := strings.LastIndex(name, "@")
	if idx < 0 {
		return name
	}
	return name[idx+1:]
}

func requestToServer(ctx context.Context, server, token, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("%s/%s", server, url), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(zfshttp.AuthenticationTokenHeader, token)
	return req, nil
}

func filterSnapshotsWithProp(list []*zfs.Dataset, prop string) []*zfs.Dataset {
	nwList := make([]*zfs.Dataset, 0, len(list))
	for _, snap := range list {
		if snap.ExtraProps[prop] == zfs.PropertyUnset {
			continue
		}
		nwList = append(nwList, snap)
	}
	return nwList
}

func orderSnapshotsByCreated(set []*zfs.Dataset, prop string) ([]*zfs.Dataset, error) {
	var err error
	sort.Slice(set, func(i, j int) bool {
		createdI, parseErr := parseDatasetTimeProperty(set[i], prop)
		if parseErr != nil {
			err = parseErr
			return false
		}
		createdJ, parseErr := parseDatasetTimeProperty(set[j], prop)
		if parseErr != nil {
			err = parseErr
			return false
		}
		return createdI.Before(createdJ)
	})
	return set, err
}

func snapshotsContain(list []*zfs.Dataset, dataset, snapshot string) bool {
	for _, ds := range list {
		if datasetName(ds.Name, false) == fmt.Sprintf("%s@%s", dataset, snapshot) {
			return true
		}
	}
	return false
}
