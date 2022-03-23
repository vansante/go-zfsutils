package jobrunner

import "github.com/vansante/go-zfs"

const (
	defaultDatasetType            = zfs.DatasetFilesystem
	defaultSnapshotNameTemplate   = "backup_%UNIXTIME%"
	defaultMaximumSendTimeMinutes = 12 * 60

	defaultSnapshotIntervalMinutesProperty     = "com.github.vansante:snapshot-interval-minutes"
	defaultSnapshotCreatedAtProperty           = "com.github.vansante:snapshot-created-at"
	defaultSnapshotSendToProperty              = "com.github.vansante:snapshot-send-to"
	defaultSnapshotSentAtProperty              = "com.github.vansante:snapshot-sent-at"
	defaultSnapshotRetentionCountProperty      = "com.github.vansante:snapshot-retention-count"
	defaultSnapshotMaxRetentionMinutesProperty = "com.github.vansante:snapshot-max-retention-minutes"
	defaultDeleteAtProperty                    = "com.github.vansante:delete-at"
)

type Config struct {
	ParentDataset                         string          `json:"ParentDataset" yaml:"ParentDataset"`
	DatasetType                           zfs.DatasetType `json:"DatasetTypes" yaml:"DatasetTypes"`
	AuthorisationToken                    string          `json:"AuthorisationToken" yaml:"AuthorisationToken"`
	SnapshotNameTemplate                  string          `json:"SnapshotNameTemplate" yaml:"SnapshotNameTemplate"`
	IgnoreSnapshotsWithoutCreatedProperty bool            `json:"IgnoreSnapshotsWithoutCreatedProperty" yaml:"IgnoreSnapshotsWithoutCreatedProperty"`
	DeleteFilesystems                     bool            `json:"DeleteFilesystems" yaml:"DeleteFilesystems"`
	MaximumSendTimeMinutes                int64           `json:"MaximumSendTimeMinutes" yaml:"MaximumSendTimeMinutes"`

	Properties Properties `json:"Properties" yaml:"Properties"`
}

type Properties struct {
	SnapshotIntervalMinutes     string `json:"SnapshotIntervalMinutes" yaml:"SnapshotIntervalMinutes"`
	SnapshotCreatedAt           string `json:"SnapshotCreatedAt" yaml:"SnapshotCreatedAt"`
	SnapshotSendTo              string `json:"SnapshotSendTo" yaml:"SnapshotSendTo"`
	SnapshotSentAt              string `json:"SnapshotSentAt" yaml:"SnapshotSentAt"`
	SnapshotRetentionCount      string `json:"SnapshotRetentionCount" yaml:"SnapshotRetentionCount"`
	SnapshotMaxRetentionMinutes string `json:"SnapshotMaxRetentionMinutes" yaml:"SnapshotMaxRetentionMinutes"`
	DeleteAt                    string `json:"DeleteAt" yaml:"DeleteAt"`
}

// ApplyDefaults applies all the default values to the configuration
func (c *Config) ApplyDefaults() {
	c.DatasetType = defaultDatasetType
	c.SnapshotNameTemplate = defaultSnapshotNameTemplate
	c.MaximumSendTimeMinutes = defaultMaximumSendTimeMinutes

	c.Properties.SnapshotIntervalMinutes = defaultSnapshotIntervalMinutesProperty
	c.Properties.SnapshotCreatedAt = defaultSnapshotCreatedAtProperty
	c.Properties.SnapshotSendTo = defaultSnapshotSendToProperty
	c.Properties.SnapshotSentAt = defaultSnapshotSentAtProperty
	c.Properties.SnapshotRetentionCount = defaultSnapshotRetentionCountProperty
	c.Properties.DeleteAt = defaultDeleteAtProperty
}
