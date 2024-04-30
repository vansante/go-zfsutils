package job

import (
	"testing"
	"time"
)

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
			"parent/fs@now", true, "fs",
		},
		{
			"parent/fs@now", false, "fs@now",
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
	for i := 0; i < 100; i++ {
		dur := randomizeDuration(5 * time.Minute)
		if dur < time.Second*280 {
			t.Errorf("randomizeDuration() = %v < %v", dur, time.Second*57)
		}
		if dur > time.Second*320 {
			t.Errorf("randomizeDuration() = %v > %v", dur, time.Second*63)
		}
		//t.Logf("randomizeDuration() = %v", dur)
	}
}
