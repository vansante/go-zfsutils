package zfs

const (
	defaultHTTPPort       = 3456
	defaultBytesPerSecond = 50 * 1024 * 1024
)

type HTTPConfig struct {
	ParentDataset string `json:"ParentDataset" yaml:"ParentDataset"`

	AllowDestroy         bool     `json:"AllowDestroy" yaml:"AllowDestroy"`
	Port                 int      `json:"Port" yaml:"Port"`
	Host                 string   `json:"Host" yaml:"Host"`
	AuthenticationTokens []string `json:"AuthenticationTokens" yaml:"AuthenticationTokens"`
	SpeedBytesPerSecond  int64    `json:"BytesPerSecond" yaml:"BytesPerSecond"`
	AllowSpeedOverride   bool     `json:"AllowSpeedOverride" yaml:"AllowSpeedOverride"`
}

// ApplyDefaults sets all config values to their defaults (if they have one)
func (c *HTTPConfig) ApplyDefaults() {
	c.SpeedBytesPerSecond = defaultBytesPerSecond
	c.Port = defaultHTTPPort
}
