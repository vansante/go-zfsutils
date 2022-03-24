package http

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"github.com/vansante/go-zfs"

	"github.com/julienschmidt/httprouter"
)

const (
	AuthenticationTokenHeader   = "X-ZFS-Auth-Token"
	authenticationTokenGETParam = "authToken"
)

// HTTP is the main object for serving the ZFS HTTP server
type HTTP struct {
	router     *httprouter.Router
	config     Config
	httpSocket net.Listener
	httpServer *http.Server
	logger     zfs.Logger
	ctx        context.Context
}

type handle func(http.ResponseWriter, *http.Request, httprouter.Params, zfs.Logger)

// NewHTTP creates a new HTTP server for ZFS interactions
func NewHTTP(ctx context.Context, conf Config, logger zfs.Logger) (*HTTP, error) {
	h := &HTTP{
		router: httprouter.New(),
		config: conf,
		logger: logger,
		ctx:    ctx,
	}

	return h, h.init()
}

func (h *HTTP) init() error {
	h.registerRoutes()

	h.logger.Infof("zfs.http.init: Opening socket on port %d", h.config.Port)
	var err error
	h.httpSocket, err = net.Listen("tcp", fmt.Sprintf("%s:%d", h.config.Host, h.config.Port))
	if err != nil {
		h.logger.WithError(err).Errorf("zfs.http.init: Failed to open socket on port %d", h.config.Port)
		return err
	}
	h.logger.Infof("zfs.http.init: Serving on %s:%d", h.config.Host, h.config.Port)
	h.httpServer = &http.Server{
		Handler: h.router,
		BaseContext: func(listener net.Listener) context.Context {
			return h.ctx
		},
	}
	return nil
}

func (h *HTTP) registerRoutes() {
	h.router.GET("/filesystems", h.authenticated(h.handleListFilesystems))
	h.router.PATCH("/filesystems/:filesystem", h.authenticated(h.handleSetFilesystemProps))
	h.router.DELETE("/filesystems/:filesystem", h.authenticated(h.handleDestroyFilesystem))

	h.router.GET("/filesystems/:filesystem/snapshots", h.authenticated(h.handleListSnapshots))
	h.router.GET("/filesystems/:filesystem/resume-token", h.authenticated(h.handleGetResumeToken))

	h.router.GET("/filesystems/:filesystem/snapshots/:snapshot", h.authenticated(h.handleGetSnapshot))
	h.router.GET("/filesystems/:filesystem/snapshots/:snapshot/incremental/:basesnapshot", h.authenticated(h.handleGetSnapshotIncremental))
	h.router.GET("/snapshot/resume/:token", h.authenticated(h.handleResumeGetSnapshot))

	h.router.POST("/filesystems/:filesystem/snapshots/:snapshot", h.authenticated(h.handleMakeSnapshot))
	h.router.PUT("/filesystems/:filesystem/snapshots", h.authenticated(h.handleReceiveSnapshot))
	h.router.PUT("/filesystems/:filesystem/snapshots/:snapshot", h.authenticated(h.handleReceiveSnapshot))
	h.router.PATCH("/filesystems/:filesystem/snapshots/:snapshot", h.authenticated(h.handleSetSnapshotProps))
	h.router.DELETE("/filesystems/:filesystem/snapshots/:snapshot", h.authenticated(h.handleDestroySnapshot))
}

// Serve starts the main HTTP server
func (h *HTTP) Serve() {
	err := h.httpServer.Serve(h.httpSocket)
	if !errors.Is(err, http.ErrServerClosed) && h.ctx.Err() == nil {
		h.logger.WithError(err).Error("zfs.http.Serve: HTTP server error")
	} else {
		h.logger.Info("zfs.http.Serve: HTTP server closed")
	}
}

// authenticated is an HTTP handler wrapper that ensures a valid authentication is used for the request
func (h *HTTP) authenticated(handle handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		logger := h.logger.WithFields(map[string]interface{}{
			"URL":    req.URL.String(),
			"method": req.Method,
		})

		authToken := req.Header.Get(AuthenticationTokenHeader)
		if authToken == "" {
			authToken = req.URL.Query().Get(authenticationTokenGETParam)
		}

		found := false
		for _, tkn := range h.config.AuthenticationTokens {
			found = tkn == authToken
			if found {
				break
			}
		}
		if !found {
			logger.Info("zfs.http.authenticated: Invalid authentication")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		logger.Info("zfs.http.authenticated: Handling")

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
