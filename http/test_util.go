package http

import (
	"context"
	"log/slog"
	"net/http/httptest"

	zfs "github.com/vansante/go-zfsutils"
)

func TestHTTPZPool(testZPool, prefix, testFs string, fn func(server *httptest.Server)) {
	zfs.TestZPool(testZPool, func() {
		h := NewHTTP(context.Background(), Config{
			ParentDataset: testZPool,

			Permissions: Permissions{
				AllowSpeedOverride:      true,
				AllowNonRaw:             true,
				AllowIncludeProperties:  true,
				AllowDestroyFilesystems: true,
				AllowDestroySnapshots:   true,
			},
		}, slog.Default())
		h.registerRoutes(prefix)

		if testFs != "" {
			_, err := zfs.CreateFilesystem(context.Background(), testFs, zfs.CreateFilesystemOptions{
				Properties: map[string]string{zfs.PropertyCanMount: zfs.PropertyOff},
			})
			if err != nil {
				panic(err)
			}
		}

		server := httptest.NewServer(h.HTTPHandler())
		fn(server)
	})
}
