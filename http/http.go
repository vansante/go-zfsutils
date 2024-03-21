package http

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/klauspost/compress/zstd"

	"github.com/julienschmidt/httprouter"
)

// HTTP is the main object for serving the ZFS HTTP server
type HTTP struct {
	router *httprouter.Router
	config Config
	logger *slog.Logger
	ctx    context.Context
}

type handle func(http.ResponseWriter, *http.Request, httprouter.Params, *slog.Logger)

// NewHTTP creates a new HTTP server for ZFS interactions
func NewHTTP(ctx context.Context, conf Config, logger *slog.Logger) *HTTP {
	h := &HTTP{
		router: httprouter.New(),
		config: conf,
		logger: logger,
		ctx:    ctx,
	}

	h.registerRoutes(conf.HTTPPathPrefix)
	return h
}

// HTTPHandler returns the handler to handle ZFS HTTP requests
func (h *HTTP) HTTPHandler() http.Handler {
	return h.router
}

// nolint: goconst
func (h *HTTP) registerRoutes(prefix string) {
	h.router.GET(prefix+"/filesystems", h.middleware(h.handleListFilesystems))
	h.router.PATCH(prefix+"/filesystems/:filesystem", h.middleware(h.handleSetFilesystemProps))
	h.router.DELETE(prefix+"/filesystems/:filesystem", h.middleware(h.handleDestroyFilesystem))

	h.router.GET(prefix+"/filesystems/:filesystem/snapshots", h.middleware(h.handleListSnapshots))
	h.router.GET(prefix+"/filesystems/:filesystem/resume-token", h.middleware(h.handleGetResumeToken))

	h.router.GET(prefix+"/filesystems/:filesystem/snapshots/:snapshot", h.middleware(h.handleGetSnapshot))
	h.router.GET(prefix+"/filesystems/:filesystem/snapshots/:snapshot/incremental/:basesnapshot", h.middleware(h.handleGetSnapshotIncremental))
	h.router.GET(prefix+"/snapshot/resume/:token", h.middleware(h.handleResumeGetSnapshot))

	h.router.POST(prefix+"/filesystems/:filesystem/snapshots/:snapshot", h.middleware(h.handleMakeSnapshot))
	h.router.PUT(prefix+"/filesystems/:filesystem/snapshots", h.middleware(h.handleReceiveSnapshot))
	h.router.PUT(prefix+"/filesystems/:filesystem/snapshots/:snapshot", h.middleware(h.handleReceiveSnapshot))
	h.router.PATCH(prefix+"/filesystems/:filesystem/snapshots/:snapshot", h.middleware(h.handleSetSnapshotProps))
	h.router.DELETE(prefix+"/filesystems/:filesystem/snapshots/:snapshot", h.middleware(h.handleDestroySnapshot))
}

// middleware is an HTTP handler wrapper that ensures a valid authentication is used for the request
func (h *HTTP) middleware(handle handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		logger := h.logger.With(slog.Group("req",
			"URL", req.URL.String(),
			"method", req.Method),
		)
		logger.Info("zfs.http.middleware: Handling")

		handle(w, req, ps, logger)
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
