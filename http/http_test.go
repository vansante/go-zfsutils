package http

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// testHTTP creates an HTTP server object with the given permissions, without a zpool
func testHTTP(conf Config) *HTTP {
	return &HTTP{config: conf}
}

// testRequest creates a GET request with the given query parameter set
func testRequest(param, value string) *http.Request {
	url := "/filesystems"
	if param != "" {
		url = fmt.Sprintf("%s?%s=%s", url, param, value)
	}
	return httptest.NewRequest(http.MethodGet, url, nil)
}

func TestHTTP_getSpeed(t *testing.T) {
	const configSpeed = 1024

	tests := []struct {
		name          string
		allowOverride bool
		value         string
		want          int64
	}{
		{"denied", false, "2048", configSpeed},
		{"deniedNoParam", false, "", configSpeed},
		{"allowedNoParam", true, "", configSpeed},
		{"allowedOverride", true, "2048", 2048},
		{"allowedZero", true, "0", 0},
		{"allowedNegative", true, "-1", -1},
		{"allowedMalformed", true, "notanumber", configSpeed},
		{"allowedOverflow", true, "99999999999999999999", configSpeed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := testHTTP(Config{
				SpeedBytesPerSecond: configSpeed,
				Permissions:         Permissions{AllowSpeedOverride: tt.allowOverride},
			})
			require.Equal(t, tt.want, h.getSpeed(testRequest(GETParamBytesPerSecond, tt.value)))
		})
	}
}

func TestHTTP_getRaw(t *testing.T) {
	tests := []struct {
		name        string
		allowNonRaw bool
		value       string
		want        bool
	}{
		{"deniedForcesRaw", false, "false", true},
		{"deniedNoParam", false, "", true},
		{"allowedNoParam", true, "", false},
		{"allowedTrue", true, "true", true},
		{"allowedOne", true, "1", true},
		{"allowedFalse", true, "false", false},
		{"allowedMalformed", true, "notabool", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := testHTTP(Config{Permissions: Permissions{AllowNonRaw: tt.allowNonRaw}})
			require.Equal(t, tt.want, h.getRaw(testRequest(GETParamRaw, tt.value)))
		})
	}
}

func TestHTTP_getIncludeProperties(t *testing.T) {
	tests := []struct {
		name         string
		allowInclude bool
		value        string
		want         bool
	}{
		{"denied", false, "true", false},
		{"deniedNoParam", false, "", false},
		{"allowedNoParam", true, "", false},
		{"allowedTrue", true, "true", true},
		{"allowedFalse", true, "false", false},
		{"allowedMalformed", true, "notabool", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := testHTTP(Config{Permissions: Permissions{AllowIncludeProperties: tt.allowInclude}})
			require.Equal(t, tt.want, h.getIncludeProperties(testRequest(GETParamIncludeProperties, tt.value)))
		})
	}
}

func TestHTTP_getBoolParams(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{"absent", "", false},
		{"true", "true", true},
		{"one", "1", true},
		{"false", "false", false},
		{"malformed", "notabool", false},
	}

	h := testHTTP(Config{})
	for _, tt := range tests {
		t.Run(fmt.Sprintf("forceRollback/%s", tt.name), func(t *testing.T) {
			require.Equal(t, tt.want, h.getReceiveForceRollback(testRequest(GETParamForceRollback, tt.value)))
		})
		t.Run(fmt.Sprintf("enableDecompression/%s", tt.name), func(t *testing.T) {
			require.Equal(t, tt.want, h.getEnableDecompression(testRequest(GETParamEnableDecompression, tt.value)))
		})
	}
}

func TestHTTP_getCompressionLevel(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  zstd.EncoderLevel
	}{
		{"absent", "", zstd.EncoderLevel(0)},
		{"fastest", "fastest", zstd.SpeedFastest},
		{"default", "default", zstd.SpeedDefault},
		{"better", "better", zstd.SpeedBetterCompression},
		{"best", "best", zstd.SpeedBestCompression},
		{"malformed", "notalevel", zstd.SpeedDefault},
	}

	h := testHTTP(Config{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, h.getCompressionLevel(testRequest(GETParamCompressionLevel, tt.value)))
		})
	}
}

func TestConfig_ApplyDefaults(t *testing.T) {
	conf := Config{}
	conf.ApplyDefaults()

	require.EqualValues(t, defaultBytesPerSecond, conf.SpeedBytesPerSecond)
	require.Equal(t, defaultMaximumConcurrentReceives, conf.MaximumConcurrentReceives)
}
