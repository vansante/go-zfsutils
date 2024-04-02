package job

import (
	"fmt"
	"time"

	"github.com/klauspost/compress/zstd"

	zfs "github.com/vansante/go-zfsutils"
)

const (
	defaultDatasetType               = zfs.DatasetFilesystem
	defaultSnapshotNameTemplate      = "backup_%UNIXTIME%"
	defaultMaximumSendTimeMinutes    = 12 * 60
	defaultSendRoutines              = 3
	defaultSendProgressEventInterval = 5 * time.Minute
)

// Config configures the runner
type Config struct {
	ParentDataset        string            `json:"ParentDataset" yaml:"ParentDataset"`
	DatasetType          zfs.DatasetType   `json:"DatasetTypes" yaml:"DatasetTypes"`
	HTTPHeaders          map[string]string `json:"HTTPHeaders" yaml:"HTTPHeaders"`
	SnapshotNameTemplate string            `json:"SnapshotNameTemplate" yaml:"SnapshotNameTemplate"`

	EnableSnapshotCreate  bool `json:"EnableSnapshotCreate" yaml:"EnableSnapshotCreate"`
	EnableSnapshotSend    bool `json:"EnableSnapshotSend" yaml:"EnableSnapshotSend"`
	EnableSnapshotMark    bool `json:"EnableSnapshotMark" yaml:"EnableSnapshotMark"`
	EnableSnapshotPrune   bool `json:"EnableSnapshotPrune" yaml:"EnableSnapshotPrune"`
	EnableFilesystemPrune bool `json:"EnableFilesystemPrune" yaml:"EnableFilesystemPrune"`

	SendRoutines          int               `json:"SendRoutines" yaml:"SendRoutines"`
	SendRaw               bool              `json:"SendRaw" yaml:"SendRaw"`
	SendIncludeProperties bool              `json:"SendIncludeProperties" yaml:"SendIncludeProperties"`
	SendCopyProperties    []string          `json:"SendCopyProperties" yaml:"SendCopyProperties"`
	SendSetProperties     map[string]string `json:"SendSetProperties" yaml:"SendSetProperties"`

	IgnoreSnapshotsWithoutCreatedProperty bool `json:"IgnoreSnapshotsWithoutCreatedProperty" yaml:"IgnoreSnapshotsWithoutCreatedProperty"`

	SendCompressionLevel      zstd.EncoderLevel `json:"SendCompressionLevel" yaml:"SendCompressionLevel"`
	SendSpeedBytesPerSecond   int64             `json:"SendSpeedBytesPerSecond" yaml:"SendSpeedBytesPerSecond"`
	SendProgressEventInterval time.Duration     `json:"SendProgressEventInterval" yaml:"SendProgressEventInterval"`
	MaximumSendTimeMinutes    int64             `json:"MaximumSendTimeMinutes" yaml:"MaximumSendTimeMinutes"`

	Properties Properties `json:"Properties" yaml:"Properties"`
}

// ApplyDefaults applies all the default values to the configuration
func (c *Config) ApplyDefaults() {
	c.DatasetType = defaultDatasetType
	c.SnapshotNameTemplate = defaultSnapshotNameTemplate
	c.MaximumSendTimeMinutes = defaultMaximumSendTimeMinutes
	c.SendProgressEventInterval = defaultSendProgressEventInterval

	c.EnableSnapshotCreate = true
	c.EnableSnapshotSend = true
	c.EnableSnapshotMark = true
	c.EnableSnapshotPrune = true
	c.EnableFilesystemPrune = false

	c.IgnoreSnapshotsWithoutCreatedProperty = true

	c.SendRoutines = defaultSendRoutines
	c.SendRaw = true
	c.SendIncludeProperties = false

	c.Properties.ApplyDefaults()

	c.SendCopyProperties = []string{
		c.Properties.snapshotCreatedAt(),
	}
}

// Properties sets the names of the custom ZFS properties to use
type Properties struct {
	Namespace string `json:"Namespace" yaml:"Namespace"`

	SnapshotIntervalMinutes  string `json:"SnapshotIntervalMinutes" yaml:"SnapshotIntervalMinutes"`
	SnapshotCreatedAt        string `json:"SnapshotCreatedAt" yaml:"SnapshotCreatedAt"`
	SnapshotSendTo           string `json:"SnapshotSendTo" yaml:"SnapshotSendTo"`
	SnapshotSentAt           string `json:"SnapshotSentAt" yaml:"SnapshotSentAt"`
	SnapshotRetentionCount   string `json:"SnapshotRetentionCount" yaml:"SnapshotRetentionCount"`
	SnapshotRetentionMinutes string `json:"SnapshotRetentionMinutes" yaml:"SnapshotRetentionMinutes"`
	DeleteAt                 string `json:"DeleteAt" yaml:"DeleteAt"`
}

const (
	defaultNamespace                        = "com.github.vansante"
	defaultSnapshotIntervalMinutesProperty  = "snapshot-interval-minutes"
	defaultSnapshotCreatedAtProperty        = "snapshot-created-at"
	defaultSnapshotSendToProperty           = "snapshot-send-to"
	defaultSnapshotSentAtProperty           = "snapshot-sent-at"
	defaultSnapshotRetentionCountProperty   = "snapshot-retention-count"
	defaultSnapshotRetentionMinutesProperty = "snapshot-retention-minutes"
	defaultDeleteAtProperty                 = "delete-at"
)

// ApplyDefaults applies all the default values to the Properties
func (p *Properties) ApplyDefaults() {
	p.Namespace = defaultNamespace

	p.SnapshotIntervalMinutes = defaultSnapshotIntervalMinutesProperty
	p.SnapshotCreatedAt = defaultSnapshotCreatedAtProperty
	p.SnapshotSendTo = defaultSnapshotSendToProperty
	p.SnapshotSentAt = defaultSnapshotSentAtProperty
	p.SnapshotRetentionCount = defaultSnapshotRetentionCountProperty
	p.SnapshotRetentionMinutes = defaultSnapshotRetentionMinutesProperty
	p.DeleteAt = defaultDeleteAtProperty
}

func (p *Properties) snapshotIntervalMinutes() string {
	return fmt.Sprintf("%s:%s", p.Namespace, p.SnapshotIntervalMinutes)
}

func (p *Properties) snapshotCreatedAt() string {
	return fmt.Sprintf("%s:%s", p.Namespace, p.SnapshotCreatedAt)
}

func (p *Properties) snapshotSendTo() string {
	return fmt.Sprintf("%s:%s", p.Namespace, p.SnapshotSendTo)
}

func (p *Properties) snapshotSentAt() string {
	return fmt.Sprintf("%s:%s", p.Namespace, p.SnapshotSentAt)
}

func (p *Properties) snapshotRetentionCount() string {
	return fmt.Sprintf("%s:%s", p.Namespace, p.SnapshotRetentionCount)
}

func (p *Properties) snapshotRetentionMinutes() string {
	return fmt.Sprintf("%s:%s", p.Namespace, p.SnapshotRetentionMinutes)
}

func (p *Properties) deleteAt() string {
	return fmt.Sprintf("%s:%s", p.Namespace, p.DeleteAt)
}
