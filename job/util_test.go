package job

import "testing"

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
