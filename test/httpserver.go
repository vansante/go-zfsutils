package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	zfshttp "github.com/vansante/go-zfsutils/http"
)

func main() {
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	logger := slog.New(h)
	slog.SetDefault(logger)

	server := zfshttp.NewHTTP(context.Background(), zfshttp.Config{
		HTTPPathPrefix:      "",
		ParentDataset:       "testpool",
		SpeedBytesPerSecond: 10 * 1024 * 1024,
		Permissions: zfshttp.Permissions{
			AllowSpeedOverride:      true,
			AllowNonRaw:             true,
			AllowIncludeProperties:  true,
			AllowDestroyFilesystems: true,
			AllowDestroySnapshots:   true,
		},
	}, logger)

	err := http.ListenAndServe(":1337", &httpHandler{
		h: server,
		l: logger,
	})
	panic(err)
}

type httpHandler struct {
	h *zfshttp.HTTP
	l *slog.Logger
}

func (h *httpHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	h.l.Info("handling", "method", request.Method, "url", request.URL)
	h.h.HTTPHandler().ServeHTTP(writer, request)
}
