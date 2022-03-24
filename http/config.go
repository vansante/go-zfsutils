package http

const (
	defaultHTTPPort       = 7654
	defaultBytesPerSecond = 50 * 1024 * 1024
)

// Config specifies the configuration for the zfs http server
type Config struct {
	ParentDataset string `json:"ParentDataset" yaml:"ParentDataset"`

	Port int    `json:"Port" yaml:"Port"`
	Host string `json:"Host" yaml:"Host"`

	AuthenticationTokens []string `json:"AuthenticationTokens" yaml:"AuthenticationTokens"`
	SpeedBytesPerSecond  int64    `json:"SpeedBytesPerSecond" yaml:"SpeedBytesPerSecond"`

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
	c.Port = defaultHTTPPort
}
