package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/klauspost/compress/zstd"
)

// HTTP is the main object for serving the ZFS HTTP server
type HTTP struct {
	router *http.ServeMux
	config Config
	logger *slog.Logger
	ctx    context.Context
}

type handle func(http.ResponseWriter, *http.Request, *slog.Logger)

// NewHTTP creates a new HTTP server for ZFS interactions
func NewHTTP(ctx context.Context, conf Config, logger *slog.Logger) *HTTP {
	h := &HTTP{
		router: http.NewServeMux(),
		config: conf,
		logger: logger,
		ctx:    ctx,
	}

	h.registerRoutes()
	return h
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Server", "go-zfsutils")

	h.router.ServeHTTP(w, req)
}

// nolint: goconst
func (h *HTTP) registerRoutes() {
	h.registerRoute(http.MethodGet, "/filesystems", h.handleListFilesystems)
	h.registerRoute(http.MethodPatch, "/filesystems/{filesystem}", h.handleSetFilesystemProps)
	h.registerRoute(http.MethodDelete, "/filesystems/{filesystem}", h.handleDestroyFilesystem)

	h.registerRoute(http.MethodGet, "/filesystems/{filesystem}/snapshots", h.handleListSnapshots)
	h.registerRoute(http.MethodGet, "/filesystems/{filesystem}/resume-token", h.handleGetResumeToken)

	h.registerRoute(http.MethodGet, "/filesystems/{filesystem}/snapshots/{snapshot}", h.handleGetSnapshot)
	h.registerRoute(http.MethodGet, "/filesystems/{filesystem}/snapshots/{snapshot}/incremental/{basesnapshot}", h.handleGetSnapshotIncremental)
	h.registerRoute(http.MethodGet, "/snapshot/resume/{token}", h.handleResumeGetSnapshot)

	h.registerRoute(http.MethodPost, "/filesystems/{filesystem}/snapshots/{snapshot}", h.handleMakeSnapshot)
	h.registerRoute(http.MethodPut, "/filesystems/{filesystem}/snapshots", h.handleReceiveSnapshot)
	h.registerRoute(http.MethodPut, "/filesystems/{filesystem}/snapshots/{snapshot}", h.handleReceiveSnapshot)
	h.registerRoute(http.MethodPatch, "/filesystems/{filesystem}/snapshots/{snapshot}", h.handleSetSnapshotProps)
	h.registerRoute(http.MethodDelete, "/filesystems/{filesystem}/snapshots/{snapshot}", h.handleDestroySnapshot)
}

func (h *HTTP) registerRoute(method, url string, handler handle) {
	h.router.HandleFunc(fmt.Sprintf("%s %s%s", method, h.config.HTTPPathPrefix, url), h.middleware(handler))
}

// middleware is an HTTP handler wrapper
func (h *HTTP) middleware(handle handle) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		logger := h.logger.With(slog.Group("req",
			"URL", req.URL.String(),
			"method", req.Method),
		)
		logger.Info("zfs.http.middleware: Handling")

		handle(w, req, logger)
	}
}

func (h *HTTP) getSpeed(req *http.Request) int64 {
	speed := h.config.SpeedBytesPerSecond
	if !h.config.Permissions.AllowSpeedOverride {
		return speed
	}
	speedStr := req.URL.Query().Get(GETParamBytesPerSecond)
	if speedStr == "" {
		return speed
	}
	customSpeed, err := strconv.ParseInt(speedStr, 10, 64)
	if err == nil {
		return customSpeed
	}
	return speed
}

func (h *HTTP) getReceiveForceRollback(req *http.Request) bool {
	rollbackStr := req.URL.Query().Get(GETParamForceRollback)
	if rollbackStr == "" {
		return false
	}
	rollback, _ := strconv.ParseBool(rollbackStr)
	return rollback
}

func (h *HTTP) getEnableDecompression(req *http.Request) bool {
	enableStr := req.URL.Query().Get(GETParamEnableDecompression)
	if enableStr == "" {
		return false
	}
	enable, _ := strconv.ParseBool(enableStr)
	return enable
}

func (h *HTTP) getCompressionLevel(req *http.Request) zstd.EncoderLevel {
	level := zstd.EncoderLevel(0)
	levelStr := req.URL.Query().Get(GETParamCompressionLevel)
	if levelStr == "" {
		return level
	}
	_, level = zstd.EncoderLevelFromString(levelStr)
	return level
}

func (h *HTTP) getRaw(req *http.Request) bool {
	if !h.config.Permissions.AllowNonRaw {
		return true
	}
	raw, _ := strconv.ParseBool(req.URL.Query().Get(GETParamRaw))
	return raw
}

func (h *HTTP) getIncludeProperties(req *http.Request) bool {
	if !h.config.Permissions.AllowIncludeProperties {
		return false
	}
	incl, _ := strconv.ParseBool(req.URL.Query().Get(GETParamIncludeProperties))
	return incl
}
