package http

import (
	"context"
	"net/http/httptest"

	"github.com/vansante/go-zfs"

	"github.com/julienschmidt/httprouter"
)

func TestHTTPZPool(testZPool, testAuthToken, testFs string, logger zfs.Logger, fn func(server *httptest.Server)) {
	zfs.TestZPool(testZPool, func() {
		rtr := httprouter.New()
		h := HTTP{
			router: rtr,
			config: Config{
				ParentDataset:        testZPool,
				AuthenticationTokens: []string{testAuthToken},

				Permissions: Permissions{
					AllowSpeedOverride:      true,
					AllowNonRaw:             true,
					AllowIncludeProperties:  true,
					AllowDestroyFilesystems: true,
					AllowDestroySnapshots:   true,
				},
			},
			logger: logger,
			ctx:    context.Background(),
		}
		h.registerRoutes()

		if testFs != "" {
			_, err := zfs.CreateFilesystem(context.Background(), testFs, zfs.CreateFilesystemOptions{
				Properties: map[string]string{zfs.PropertyCanMount: zfs.PropertyOff},
			})
			if err != nil {
				panic(err)
			}
		}

		server := httptest.NewServer(rtr)
		fn(server)
	})
}
