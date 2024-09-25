package http

const (
	defaultBytesPerSecond            = 100 * 1024 * 1024
	defaultMaximumConcurrentReceives = 3
)

// Config specifies the configuration for the zfs http server
type Config struct {
	HTTPPathPrefix      string `json:"HTTPPathPrefix" yaml:"HTTPPathPrefix"`
	ParentDataset       string `json:"ParentDataset" yaml:"ParentDataset"`
	SpeedBytesPerSecond int64  `json:"SpeedBytesPerSecond" yaml:"SpeedBytesPerSecond"`

	// MaximumConcurrentReceives limits the concurrent amount of ZFS receives, set to zero to disable limits
	MaximumConcurrentReceives int `json:"MaximumConcurrentReceives" yaml:"MaximumConcurrentReceives"`

	Permissions Permissions `json:"Permissions" yaml:"Permissions"`
}

// Permissions specifies permissions for requests over zfs http
type Permissions struct {
	AllowSpeedOverride      bool `json:"AllowSpeedOverride" yaml:"AllowSpeedOverride"`
	AllowNonRaw             bool `json:"AllowNonRaw" yaml:"AllowNonRaw"`
	AllowIncludeProperties  bool `json:"AllowIncludeProperties" yaml:"AllowIncludeProperties"`
	AllowDestroyFilesystems bool `json:"AllowDestroyFilesystems" yaml:"AllowDestroyFilesystems"`
	AllowDestroySnapshots   bool `json:"AllowDestroySnapshots" yaml:"AllowDestroySnapshots"`
}

// ApplyDefaults sets all config values to their defaults (if they have one)
func (c *Config) ApplyDefaults() {
	c.SpeedBytesPerSecond = defaultBytesPerSecond
	c.MaximumConcurrentReceives = defaultMaximumConcurrentReceives
}
