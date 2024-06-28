package job

import (
	"fmt"
	"time"

	"github.com/klauspost/compress/zstd"

	zfs "github.com/vansante/go-zfsutils"
)

const (
	defaultDatasetType                          = zfs.DatasetFilesystem
	defaultSnapshotNameTemplate                 = "backup_%UNIXTIME%"
	defaultMaximumSendTimeSeconds               = 12 * 60 * 60 // 12 hours
	defaultSendRoutines                         = 3
	defaultSendProgressEventIntervalSeconds     = 5 * 60  // 5 minutes
	defaultMaximumRemoteSnapshotCacheAgeSeconds = 30 * 60 // 30 minutes
)

// Config configures the runner
type Config struct {
	ParentDataset        string            `json:"ParentDataset" yaml:"ParentDataset"`
	DatasetType          zfs.DatasetType   `json:"DatasetType" yaml:"DatasetType"`
	HTTPHeaders          map[string]string `json:"HTTPHeaders" yaml:"HTTPHeaders"`
	SnapshotNameTemplate string            `json:"SnapshotNameTemplate" yaml:"SnapshotNameTemplate"`

	EnableSnapshotCreate     bool `json:"EnableSnapshotCreate" yaml:"EnableSnapshotCreate"`
	EnableSnapshotSend       bool `json:"EnableSnapshotSend" yaml:"EnableSnapshotSend"`
	EnableSnapshotMark       bool `json:"EnableSnapshotMark" yaml:"EnableSnapshotMark"`
	EnableSnapshotMarkRemote bool `json:"EnableSnapshotMarkRemote" yaml:"EnableSnapshotMarkRemote"`
	EnableSnapshotPrune      bool `json:"EnableSnapshotPrune" yaml:"EnableSnapshotPrune"`
	EnableFilesystemPrune    bool `json:"EnableFilesystemPrune" yaml:"EnableFilesystemPrune"`

	SendRoutines          int  `json:"SendRoutines" yaml:"SendRoutines"`
	SendResumable         bool `json:"SendResumable" yaml:"SendResumable"`
	SendRaw               bool `json:"SendRaw" yaml:"SendRaw"`
	SendIncludeProperties bool `json:"SendIncludeProperties" yaml:"SendIncludeProperties"`

	SendCopyProperties []string          `json:"SendCopyProperties" yaml:"SendCopyProperties"`
	SendSetProperties  map[string]string `json:"SendSetProperties" yaml:"SendSetProperties"`

	SendCopySnapshotProperties []string          `json:"SendCopySnapshotProperties" yaml:"SendCopySnapshotProperties"`
	SendSetSnapshotProperties  map[string]string `json:"SendSetSnapshotProperties" yaml:"SendSetSnapshotProperties"`

	CreateSnapshotsIgnoreWithoutCreatedProperty bool `json:"CreateIgnoreSnapshotsWithoutCreatedProperty" yaml:"CreateIgnoreSnapshotsWithoutCreatedProperty"`
	SendSnapshotsIgnoreWithoutCreatedProperty   bool `json:"SendSnapshotsIgnoreWithoutCreatedProperty" yaml:"SendSnapshotsIgnoreWithoutCreatedProperty"`
	PruneSnapshotsIgnoreWithoutCreatedProperty  bool `json:"PruneSnapshotsIgnoreWithoutCreatedProperty" yaml:"PruneSnapshotsIgnoreWithoutCreatedProperty"`

	SendCompressionLevel                 zstd.EncoderLevel `json:"SendCompressionLevel" yaml:"SendCompressionLevel"`
	SendSpeedBytesPerSecond              int64             `json:"SendSpeedBytesPerSecond" yaml:"SendSpeedBytesPerSecond"`
	SendProgressEventIntervalSeconds     int64             `json:"SendProgressEventIntervalSeconds" yaml:"SendProgressEventIntervalSeconds"`
	SendReceiveForceRollback             bool              `json:"SendReceiveForceRollback" yaml:"SendReceiveForceRollback"`
	MaximumSendTimeSeconds               int64             `json:"MaximumSendTimeSeconds" yaml:"MaximumSendTimeSeconds"`
	MaximumRemoteSnapshotCacheAgeSeconds int64             `json:"MaximumRemoteSnapshotCacheAgeSeconds" yaml:"MaximumRemoteSnapshotCacheAgeSeconds"`

	Properties Properties `json:"Properties" yaml:"Properties"`
}

// ApplyDefaults applies all the default values to the configuration
func (c *Config) ApplyDefaults() {
	c.DatasetType = defaultDatasetType
	c.SnapshotNameTemplate = defaultSnapshotNameTemplate
	c.MaximumSendTimeSeconds = defaultMaximumSendTimeSeconds
	c.SendProgressEventIntervalSeconds = defaultSendProgressEventIntervalSeconds
	c.MaximumRemoteSnapshotCacheAgeSeconds = defaultMaximumRemoteSnapshotCacheAgeSeconds

	c.EnableSnapshotCreate = true
	c.EnableSnapshotSend = true
	c.EnableSnapshotMark = true
	c.EnableSnapshotPrune = true
	c.EnableFilesystemPrune = false

	c.CreateSnapshotsIgnoreWithoutCreatedProperty = true
	c.SendSnapshotsIgnoreWithoutCreatedProperty = true
	c.PruneSnapshotsIgnoreWithoutCreatedProperty = true

	c.SendRoutines = defaultSendRoutines
	c.SendRaw = true
	c.SendIncludeProperties = false

	c.Properties.ApplyDefaults()

	c.SendCopySnapshotProperties = []string{
		c.Properties.snapshotCreatedAt(),
	}
}

func (c *Config) maximumSendTime() time.Duration {
	return time.Duration(c.MaximumSendTimeSeconds) * time.Second
}

func (c *Config) sendProgressInterval() time.Duration {
	return time.Duration(c.SendProgressEventIntervalSeconds) * time.Second
}

func (c *Config) maximumRemoteSnapshotCacheAge() time.Duration {
	return time.Duration(c.MaximumRemoteSnapshotCacheAgeSeconds) * time.Second
}

func (c *Config) sendSetProperties() map[string]string {
	props := make(map[string]string, len(c.SendSetProperties)+len(c.SendCopyProperties))
	for k, v := range c.SendSetProperties {
		props[k] = v
	}
	return props
}

func (c *Config) sendSetSnapshotProperties() map[string]string {
	props := make(map[string]string, len(c.SendSetSnapshotProperties)+len(c.SendCopySnapshotProperties))
	for k, v := range c.SendSetSnapshotProperties {
		props[k] = v
	}
	return props
}

// Properties sets the names of the custom ZFS properties to use
type Properties struct {
	Namespace string `json:"Namespace" yaml:"Namespace"`

	DatasetLocked            string `json:"DatasetLocked" yaml:"DatasetLocked"`
	SnapshotIntervalMinutes  string `json:"SnapshotIntervalMinutes" yaml:"SnapshotIntervalMinutes"`
	SnapshotCreatedAt        string `json:"SnapshotCreatedAt" yaml:"SnapshotCreatedAt"`
	SnapshotSendTo           string `json:"SnapshotSendTo" yaml:"SnapshotSendTo"`
	SnapshotSending          string `json:"SnapshotSending" yaml:"SnapshotSending"`
	SnapshotSentAt           string `json:"SnapshotSentAt" yaml:"SnapshotSentAt"`
	SnapshotRetentionCount   string `json:"SnapshotRetentionCount" yaml:"SnapshotRetentionCount"`
	SnapshotRetentionMinutes string `json:"SnapshotRetentionMinutes" yaml:"SnapshotRetentionMinutes"`
	DeleteAt                 string `json:"DeleteAt" yaml:"DeleteAt"`
	DeleteWithoutSnapshots   string `json:"DeleteWithoutSnapshots" yaml:"DeleteWithoutSnapshots"`
}

const (
	defaultNamespace = "com.github.vansante"

	defaultDatasetLockedProperty            = "dataset-locked"
	defaultSnapshotIntervalMinutesProperty  = "snapshot-interval-minutes"
	defaultSnapshotCreatedAtProperty        = "snapshot-created-at"
	defaultSnapshotSendToProperty           = "snapshot-send-to"
	defaultSnapshotSendingProperty          = "snapshot-sending"
	defaultSnapshotSentAtProperty           = "snapshot-sent-at"
	defaultSnapshotRetentionCountProperty   = "snapshot-retention-count"
	defaultSnapshotRetentionMinutesProperty = "snapshot-retention-minutes"
	defaultDeleteAtProperty                 = "delete-at"
	defaultDeleteWithoutSnapshotsProperty   = "delete-without-snapshots"
)

// ApplyDefaults applies all the default values to the Properties
func (p *Properties) ApplyDefaults() {
	p.Namespace = defaultNamespace

	p.DatasetLocked = defaultDatasetLockedProperty
	p.SnapshotIntervalMinutes = defaultSnapshotIntervalMinutesProperty
	p.SnapshotCreatedAt = defaultSnapshotCreatedAtProperty
	p.SnapshotSendTo = defaultSnapshotSendToProperty
	p.SnapshotSending = defaultSnapshotSendingProperty
	p.SnapshotSentAt = defaultSnapshotSentAtProperty
	p.SnapshotRetentionCount = defaultSnapshotRetentionCountProperty
	p.SnapshotRetentionMinutes = defaultSnapshotRetentionMinutesProperty
	p.DeleteAt = defaultDeleteAtProperty
	p.DeleteWithoutSnapshots = defaultDeleteWithoutSnapshotsProperty
}

func (p *Properties) datasetLocked() string {
	return fmt.Sprintf("%s:%s", p.Namespace, p.DatasetLocked)
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

func (p *Properties) snapshotSending() string {
	return fmt.Sprintf("%s:%s", p.Namespace, p.SnapshotSending)
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

func (p *Properties) deleteWithoutSnapshots() string {
	return fmt.Sprintf("%s:%s", p.Namespace, p.DeleteWithoutSnapshots)
}
