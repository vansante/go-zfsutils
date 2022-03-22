package http

import (
	"context"
	"net/http/httptest"

	"github.com/vansante/go-zfs"

	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"
)

func TestHTTPZPool(testZPool, testAuthToken, testFs string, fn func(server *httptest.Server)) {
	zfs.TestZPool(testZPool, func() {
		rtr := httprouter.New()
		h := HTTP{
			router: rtr,
			config: Config{
				ParentDataset:        testZPool,
				AllowDestroy:         true,
				AuthenticationTokens: []string{testAuthToken},
			},
			logger: logrus.WithField("test", "test"),
			ctx:    context.Background(),
		}
		h.registerRoutes()

		if testFs != "" {
			_, err := zfs.CreateFilesystem(testFs, map[string]string{zfs.PropertyCanMount: zfs.PropertyOff}, nil)
			if err != nil {
				panic(err)
			}
		}

		server := httptest.NewServer(rtr)
		fn(server)
	})
}
