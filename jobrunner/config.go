package jobrunner

import "github.com/vansante/go-zfs"

const (
	defaultDatasetType          = zfs.DatasetAll
	defaultSnapshotNameTemplate = "backup_%UNIXTIME%"

	defaultSnapshotIntervalMinutesProperty = "com.github.vansante:snapshot-interval-minutes"
	defaultSnapshotCreatedAtProperty       = "com.github.vansante:snapshot-created-at"
	defaultSendSnapshotToProperty          = "com.github.vansante:send-snapshot-to"
	defaultSnapshotSentAtProperty          = "com.github.vansante:snapshot-sent-at"
	defaultSnapshotRetentionCountProperty  = "com.github.vansante:snapshot-retention-count"
	defaultDeleteAtProperty                = "com.github.vansante:delete-at"
)

type Config struct {
	ParentDataset                         string          `json:"ParentDataset" yaml:"ParentDataset"`
	DatasetType                           zfs.DatasetType `json:"DatasetTypes" yaml:"DatasetTypes"`
	SnapshotNameTemplate                  string          `json:"SnapshotNameTemplate" yaml:"SnapshotNameTemplate"`
	SnapshotRecursive                     bool            `json:"SnapshotRecursive" yaml:"SnapshotRecursive"`
	IgnoreSnapshotsWithoutCreatedProperty bool            `json:"IgnoreSnapshotsWithoutCreatedProperty" yaml:"IgnoreSnapshotsWithoutCreatedProperty"`
	DeleteFilesystems                     bool            `json:"DeleteFilesystems" yaml:"DeleteFilesystems"`

	Properties Properties `json:"Properties" yaml:"Properties"`
}

type Properties struct {
	SnapshotIntervalMinutes string `json:"SnapshotIntervalMinutes" yaml:"SnapshotIntervalMinutes"`
	SnapshotCreatedAt       string `json:"SnapshotCreatedAt" yaml:"SnapshotCreatedAt"`
	SendSnapshotTo          string `json:"SendSnapshotTo" yaml:"SendSnapshotTo"`
	SnapshotSentAt          string `json:"SnapshotSentAt" yaml:"SnapshotSentAt"`
	SnapshotRetentionCount  string `json:"SnapshotRetentionCount" yaml:"SnapshotRetentionCount"`
	DeleteAt                string `json:"DeleteAt" yaml:"DeleteAt"`
}

// ApplyDefaults applies all the default values to the configuration
func (c *Config) ApplyDefaults() {
	c.DatasetType = defaultDatasetType
	c.SnapshotNameTemplate = defaultSnapshotNameTemplate

	c.Properties.SnapshotIntervalMinutes = defaultSnapshotIntervalMinutesProperty
	c.Properties.SnapshotCreatedAt = defaultSnapshotCreatedAtProperty
	c.Properties.SendSnapshotTo = defaultSendSnapshotToProperty
	c.Properties.SnapshotSentAt = defaultSnapshotSentAtProperty
	c.Properties.SnapshotRetentionCount = defaultSnapshotRetentionCountProperty
	c.Properties.DeleteAt = defaultDeleteAtProperty
}
