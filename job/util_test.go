package job

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	zfs "github.com/vansante/go-zfsutils"
)

func Test_propertyIsSet(t *testing.T) {
	tests := []struct {
		val  string
		want bool
	}{
		{"", false},
		{zfs.ValueUnset, false},
		{"0", true},
		{zfs.ValueOff, true},
		{"2021-01-01T00:00:00Z", true},
	}

	for _, tt := range tests {
		t.Run(tt.val, func(t *testing.T) {
			require.Equal(t, tt.want, propertyIsSet(tt.val))
		})
	}
}

func Test_propertyIsBefore(t *testing.T) {
	now := time.Date(2021, 6, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		val  string
		want bool
	}{
		{"before", now.Add(-time.Hour).Format(dateTimeFormat), true},
		{"after", now.Add(time.Hour).Format(dateTimeFormat), false},
		{"equal", now.Format(dateTimeFormat), false},
		{"unparseable", "not a time", false},
		{"empty", "", false},
		{"unset", zfs.ValueUnset, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, propertyIsBefore(tt.val, now))
		})
	}
}

func Test_isContextError(t *testing.T) {
	require.True(t, isContextError(context.Canceled))
	require.True(t, isContextError(context.DeadlineExceeded))
	require.True(t, isContextError(fmt.Errorf("wrapped: %w", context.Canceled)))
	require.False(t, isContextError(nil))
	require.False(t, isContextError(errors.New("some other error")))
	require.False(t, isContextError(zfs.ErrDatasetNotFound))
}

func Test_stripDatasetSnapshot(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"testpool/testfs1@backup_1", "testpool/testfs1"},
		{"testpool/testfs1", "testpool/testfs1"},
		{"testfs1@backup_1", "testfs1"},
		{"@backup_1", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, stripDatasetSnapshot(tt.name))
		})
	}
}

func Test_parseDatasetTimeProperty(t *testing.T) {
	const prop = "nl.test:created-at"
	tm := time.Date(2021, 6, 15, 12, 0, 0, 0, time.UTC)

	ds := &zfs.Dataset{ExtraProps: map[string]string{prop: tm.Format(dateTimeFormat)}}
	got, err := parseDatasetTimeProperty(ds, prop)
	require.NoError(t, err)
	require.True(t, tm.Equal(got))

	_, err = parseDatasetTimeProperty(ds, "nl.test:missing")
	require.Error(t, err)

	ds.ExtraProps[prop] = zfs.ValueUnset
	_, err = parseDatasetTimeProperty(ds, prop)
	require.Error(t, err)
}

func Test_parseDatasetIntProperty(t *testing.T) {
	const prop = "nl.test:retention-count"

	ds := &zfs.Dataset{ExtraProps: map[string]string{prop: "42"}}
	got, err := parseDatasetIntProperty(ds, prop)
	require.NoError(t, err)
	require.EqualValues(t, 42, got)

	_, err = parseDatasetIntProperty(ds, "nl.test:missing")
	require.Error(t, err)

	ds.ExtraProps[prop] = "notanumber"
	_, err = parseDatasetIntProperty(ds, prop)
	require.Error(t, err)
}

func Test_filterSnapshotsWithProp(t *testing.T) {
	const prop = "nl.test:sent-at"

	list := []zfs.Dataset{
		{Name: "testfs1@snap1", ExtraProps: map[string]string{prop: "2021-06-15T12:00:00Z"}},
		{Name: "testfs1@snap2", ExtraProps: map[string]string{prop: ""}},
		{Name: "testfs1@snap3", ExtraProps: map[string]string{prop: zfs.ValueUnset}},
		{Name: "testfs1@snap4", ExtraProps: map[string]string{}},
	}

	got := filterSnapshotsWithProp(list, prop)
	require.Len(t, got, 3)
	require.Equal(t, "testfs1@snap2", got[0].Name)
	require.Equal(t, "testfs1@snap3", got[1].Name)
	require.Equal(t, "testfs1@snap4", got[2].Name)

	require.Empty(t, filterSnapshotsWithProp(nil, prop))
}

func Test_snapshotsContain(t *testing.T) {
	list := []zfs.Dataset{
		{Name: "testpool/testfs1@snap1"},
		{Name: "testpool/testfs2@snap2"},
	}

	require.True(t, snapshotsContain(list, "testfs1", "snap1"))
	require.True(t, snapshotsContain(list, "testfs2", "snap2"))
	require.False(t, snapshotsContain(list, "testfs1", "snap2"))
	require.False(t, snapshotsContain(list, "testfs3", "snap1"))
	require.False(t, snapshotsContain(nil, "testfs1", "snap1"))
}

func Test_datasetName(t *testing.T) {
	tests := []struct {
		name  string
		strip bool
		want  string
	}{
		{
			"test/tester", true, "tester",
		},
		{
			"bla/die/bla", true, "bla",
		},
		{
			"none", true, "none",
		},
		{
			"none@ew", true, "none",
		},
		{
			"none@ew", false, "none@ew",
		},
		{
			"parent/fs@now", true, "fs",
		},
		{
			"parent/fs@now", false, "fs@now",
		},
		{
			"/fs@now", false, "fs@now",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := datasetName(tt.name, tt.strip); got != tt.want {
				t.Errorf("fullDatasetName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_snapshotName(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{
			"bla/die/bla", "bla/die/bla",
		},
		{
			"none@snap", "snap",
		},
		{
			"parent/fs@now", "now",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := snapshotName(tt.name); got != tt.want {
				t.Errorf("snapshotName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_randomizeDuration(t *testing.T) {
	for range 100 {
		dur := randomizeDuration(5 * time.Minute)
		if dur < time.Second*280 {
			t.Errorf("randomizeDuration() = %v < %v", dur, time.Second*57)
		}
		if dur > time.Second*320 {
			t.Errorf("randomizeDuration() = %v > %v", dur, time.Second*63)
		}
		// t.Logf("randomizeDuration() = %v", dur)
	}
}
