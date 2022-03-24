package http

const (
	defaultHTTPPort       = 7654
	defaultBytesPerSecond = 50 * 1024 * 1024
)

type Config struct {
	ParentDataset string `json:"ParentDataset" yaml:"ParentDataset"`

	Port                   int      `json:"Port" yaml:"Port"`
	Host                   string   `json:"Host" yaml:"Host"`
	AuthenticationTokens   []string `json:"AuthenticationTokens" yaml:"AuthenticationTokens"`
	SpeedBytesPerSecond    int64    `json:"BytesPerSecond" yaml:"BytesPerSecond"`
	AllowNonRaw            bool     `json:"AllowNonRaw" yaml:"AllowNonRaw"`
	AllowIncludeProperties bool     `json:"AllowIncludeProperties" yaml:"AllowIncludeProperties"`
	AllowSpeedOverride     bool     `json:"AllowSpeedOverride" yaml:"AllowSpeedOverride"`
	AllowDestroy           bool     `json:"AllowDestroy" yaml:"AllowDestroy"`
}

// ApplyDefaults sets all config values to their defaults (if they have one)
func (c *Config) ApplyDefaults() {
	c.SpeedBytesPerSecond = defaultBytesPerSecond
	c.Port = defaultHTTPPort
}
