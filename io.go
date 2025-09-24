package zfs

import (
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/juju/ratelimit"
	"github.com/klauspost/compress/zstd"
)

func rateLimitWriter(writer io.Writer, bytesPerSecond int64) io.Writer {
	if bytesPerSecond <= 0 {
		return writer
	}
	return ratelimit.Writer(writer, ratelimit.NewBucketWithRate(float64(bytesPerSecond), bytesPerSecond))
}

func zstdWriter(writer io.Writer, level zstd.EncoderLevel) (io.Writer, func(), error) {
	if level == 0 {
		return writer, func() {}, nil
	}

	encoder, err := zstd.NewWriter(writer, zstd.WithEncoderLevel(level))
	if err != nil {
		return writer, func() {}, fmt.Errorf("error creating zstd encoder: %w", err)
	}
	return encoder, func() {
		err := encoder.Close()
		if err != nil {
			slog.Error("zstdWriter: Error closing encoder", "error", err)
		}
	}, nil
}

// ProgressCallback is a callback function that lets you monitor progress
type ProgressCallback func(bytes int64)

// NewCountReader creates a new CountReader
func NewCountReader(reader io.Reader) *CountReader {
	return &CountReader{
		Reader: reader,
	}
}

// CountReader counts the bytes it has read
type CountReader struct {
	io.Reader
	n int64

	every      time.Duration
	progressFn ProgressCallback
	last       time.Time
}

// SetProgressCallback sets a new progress handler every duration
func (r *CountReader) SetProgressCallback(every time.Duration, progressFn ProgressCallback) {
	r.progressFn = progressFn
	r.every = every
	r.last = time.Now()
}

func (r *CountReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	atomic.AddInt64(&r.n, int64(n))
	r.progress()
	return n, err
}

func (r *CountReader) progress() {
	if r.progressFn == nil || r.every <= 0 {
		return
	}
	if time.Since(r.last) < r.every {
		return
	}

	r.progressFn(atomic.LoadInt64(&r.n))
	r.last = time.Now()
}

func (r *CountReader) Count() int64 {
	return atomic.LoadInt64(&r.n)
}
