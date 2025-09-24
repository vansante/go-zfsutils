package http

import (
	"context"
	"log/slog"
	"net/http/httptest"

	zfs "github.com/vansante/go-zfsutils"
)

func TestHTTPZPool(testZPool, prefix, testFs string, fn func(server *httptest.Server)) {
	zfs.TestZPool(testZPool, func() {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		h := NewHTTP(context.Background(), Config{
			ParentDataset:  testZPool,
			HTTPPathPrefix: prefix,

			MaximumConcurrentReceives: 2,

			Permissions: Permissions{
				AllowSpeedOverride:      true,
				AllowNonRaw:             true,
				AllowIncludeProperties:  true,
				AllowDestroyFilesystems: true,
				AllowDestroySnapshots:   true,
			},
		}, slog.Default())

		if testFs != "" {
			_, err := zfs.CreateFilesystem(context.Background(), testFs, zfs.CreateFilesystemOptions{
				Properties: map[string]string{zfs.PropertyCanMount: zfs.ValueOff},
			})
			if err != nil {
				panic(err)
			}
		}

		server := httptest.NewServer(h)
		fn(server)
	})
}
