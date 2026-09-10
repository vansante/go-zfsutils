package zfs

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func Test_rateLimitWriter(t *testing.T) {
	buf := &bytes.Buffer{}

	require.Same(t, buf, rateLimitWriter(buf, 0))
	require.Same(t, buf, rateLimitWriter(buf, -1))

	limited := rateLimitWriter(buf, 1024*1024)
	require.NotSame(t, buf, limited)

	n, err := limited.Write([]byte("hello world"))
	require.NoError(t, err)
	require.Equal(t, 11, n)
	require.Equal(t, "hello world", buf.String())
}

func Test_zstdWriter_passthrough(t *testing.T) {
	buf := &bytes.Buffer{}

	writer, closeFn, err := zstdWriter(buf, 0)
	require.NoError(t, err)
	require.NotNil(t, closeFn)
	require.Same(t, buf, writer)

	closeFn()
}

func Test_zstdWriter_compresses(t *testing.T) {
	buf := &bytes.Buffer{}

	writer, closeFn, err := zstdWriter(buf, zstd.SpeedFastest)
	require.NoError(t, err)
	require.NotSame(t, buf, writer)

	const payload = "hello compressed world"
	_, err = writer.Write([]byte(payload))
	require.NoError(t, err)
	closeFn()

	decoder, err := zstd.NewReader(buf)
	require.NoError(t, err)
	defer decoder.Close()

	out, err := io.ReadAll(decoder)
	require.NoError(t, err)
	require.Equal(t, payload, string(out))
}

func TestCountReader_Count(t *testing.T) {
	reader := NewCountReader(strings.NewReader("hello world"))
	require.Zero(t, reader.Count())

	out, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(out))
	require.EqualValues(t, 11, reader.Count())
}

func TestCountReader_noProgressCallback(t *testing.T) {
	reader := NewCountReader(strings.NewReader("hello world"))

	// Without a callback the progress logic must not panic
	_, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.EqualValues(t, 11, reader.Count())
}

func TestCountReader_SetProgressCallback_zeroInterval(t *testing.T) {
	var calls int
	reader := NewCountReader(strings.NewReader("hello world"))
	reader.SetProgressCallback(0, func(bytes int64) {
		calls++
	})

	_, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Zero(t, calls, "callback should not fire without an interval")
}

func TestCountReader_SetProgressCallback_throttled(t *testing.T) {
	const interval = 50 * time.Millisecond

	var counts []int64
	reader := NewCountReader(strings.NewReader("hello world"))
	reader.SetProgressCallback(interval, func(bytes int64) {
		counts = append(counts, bytes)
	})

	buf := make([]byte, 1)

	// The first read is within the interval, so no callback yet
	_, err := reader.Read(buf)
	require.NoError(t, err)
	require.Empty(t, counts)

	time.Sleep(interval * 2)

	_, err = reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, counts)

	// Immediately reading again is throttled
	_, err = reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, counts)
}
