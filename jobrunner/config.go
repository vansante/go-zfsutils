package jobrunner

const (
	defaultSnapshotIntervalProperty      = "com.github.vansante:snapshot-interval"
	defaultLastSnapshotAtProperty        = "com.github.vansante:last-snapshot-at"
	defaultSendSnapshotToProperty        = "com.github.vansante:send-snapshot-to"
	defaultSnapshotRetentionTimeProperty = "com.github.vansante:snapshot-retention-time"
	defaultDeleteAtProperty              = "com.github.vansante:delete-at"
)

type Config struct {
	ParentDataset string `json:"ParentDataset" yaml:"ParentDataset"`

	SnapshotIntervalProperty      string `json:"SnapshotIntervalProperty" yaml:"SnapshotIntervalProperty"`
	LastSnapshotAtProperty        string `json:"LastSnapshotAtProperty" yaml:"LastSnapshotAtProperty"`
	SendSnapshotToProperty        string `json:"SendSnapshotToProperty" yaml:"SendSnapshotToProperty"`
	SnapshotRetentionTimeProperty string `json:"SnapshotRetentionTimeProperty" yaml:"SnapshotRetentionTimeProperty"`
	DeleteAtProperty              string `json:"DeleteAtProperty" yaml:"DeleteAtProperty"`
}
