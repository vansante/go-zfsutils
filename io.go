package zfs

import (
	"fmt"
	"io"
	"log/slog"

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
}

func (r *CountReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.n += int64(n)
	return n, err
}

func (r *CountReader) Count() int64 {
	return r.n
}

// NewCountWriter creates a new CountWriter
func NewCountWriter(writer io.Writer) *CountWriter {
	return &CountWriter{
		Writer: writer,
	}
}

// CountWriter counts the bytes it has written
type CountWriter struct {
	io.Writer
	n int64
}

func (w *CountWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.n += int64(n)
	return n, err
}

func (w *CountWriter) Count() int64 {
	return w.n
}
