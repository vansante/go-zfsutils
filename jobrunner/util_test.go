package jobrunner

import "testing"

func Test_datasetName(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{
			"test/tester", "tester",
		},
		{
			"bla/die/bla", "bla",
		},
		{
			"none", "none",
		},
		{
			"parent/fs@now", "fs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := datasetName(tt.name); got != tt.want {
				t.Errorf("datasetName() = %v, want %v", got, tt.want)
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
