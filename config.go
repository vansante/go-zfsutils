package zfs

const (
	defaultHTTPPort       = 7654
	defaultBytesPerSecond = 50 * 1024 * 1024
)

type HTTPConfig struct {
	ParentDataset string `json:"ParentDataset" yaml:"ParentDataset"`

	Port                 int      `json:"Port" yaml:"Port"`
	Host                 string   `json:"Host" yaml:"Host"`
	AuthenticationTokens []string `json:"AuthenticationTokens" yaml:"AuthenticationTokens"`
	SpeedBytesPerSecond  int64    `json:"BytesPerSecond" yaml:"BytesPerSecond"`
	AllowSpeedOverride   bool     `json:"AllowSpeedOverride" yaml:"AllowSpeedOverride"`
	AllowDestroy         bool     `json:"AllowDestroy" yaml:"AllowDestroy"`
}

// ApplyDefaults sets all config values to their defaults (if they have one)
func (c *HTTPConfig) ApplyDefaults() {
	c.SpeedBytesPerSecond = defaultBytesPerSecond
	c.Port = defaultHTTPPort
}
