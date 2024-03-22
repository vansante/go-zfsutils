package http

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	zfs "github.com/vansante/go-zfsutils"
)

const (
	GETParamExtraProperties     = "extraProps"
	GETParamResumable           = "resumable"
	GETParamIncludeProperties   = "includeProps"
	GETParamRaw                 = "raw"
	GETParamReceiveProperties   = "receiveProps"
	GETParamBytesPerSecond      = "bytesPerSecond"
	GETParamEnableDecompression = "enableDecompression"
	GETParamCompressionLevel    = "compressionLevel"
)

const HeaderResumeReceiveToken = "X-Receive-Resume-Token"

type ReceiveProperties map[string]string

// DecodeReceiveProperties decodes receive properties from an URL GET parameter
func DecodeReceiveProperties(in string) (ReceiveProperties, error) {
	data, err := base64.URLEncoding.DecodeString(in)
	if err != nil {
		return ReceiveProperties{}, err
	}
	var props ReceiveProperties
	err = json.Unmarshal(data, &props)
	return props, err
}

// Encode encodes a set of ReceiveProperties
func (r ReceiveProperties) Encode() string {
	data, _ := json.Marshal(&r) // nolint: errchkjson
	return base64.URLEncoding.EncodeToString(data)
}

// SetProperties is used by the http api to set and unset zfs properties remotely
type SetProperties struct {
	Set   map[string]string `json:"set,omitempty"`
	Unset []string          `json:"unset,omitempty"`
}

var (
	validIdentifierRegexp  = regexp.MustCompile(`^[a-zA-Z0-9_]{2,100}$`)
	validResumeTokenRegexp = regexp.MustCompile(`^[a-zA-Z0-9_-]{100,500}$`)
)

func validIdentifier(name string) bool {
	return validIdentifierRegexp.MatchString(name)
}

func zfsExtraProperties(req *http.Request) []string {
	fieldsStr := req.URL.Query().Get(GETParamExtraProperties)
	if fieldsStr == "" {
		return nil
	}

	fields := strings.Split(fieldsStr, ",")
	filtered := make([]string, 0, len(fields))
	for _, field := range fields {
		if field == "" {
			continue
		}
		filtered = append(filtered, field)
	}
	return filtered
}

func (h *HTTP) handleListFilesystems(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	list, err := zfs.ListFilesystems(req.Context(), zfs.ListOptions{
		ParentDataset:   h.config.ParentDataset,
		ExtraProperties: zfsExtraProperties(req),
		Recursive:       true,
	})
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.Info("zfs.http.handleListFilesystems: Parent dataset not found", "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.Error("zfs.http.handleListFilesystems: Error getting filesystems", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(list)
	if err != nil {
		logger.Error("zfs.http.handleListFilesystems: Error encoding json", "error", err)
		return
	}
}

func (h *HTTP) handleSetFilesystemProps(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	filesystem := req.PathValue("filesystem")
	if !validIdentifier(filesystem) {
		logger.Info("zfs.http.handleSetFilesystemProps: Invalid identifier", "filesystem", filesystem)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.Info("zfs.http.handleSetFilesystemProps: Filesystem not found", "error", err, "filesystem", filesystem)
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.Error("zfs.http.handleSetFilesystemProps: Error getting filesystem", "error", err, "filesystem", filesystem)
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetFilesystem:
		logger.Info("zfs.http.handleSetFilesystemProps: Invalid type", "type", ds.Type, "filesystem", filesystem)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	h.setProperties(w, req, ds, logger)
}

func (h *HTTP) setProperties(w http.ResponseWriter, req *http.Request, ds *zfs.Dataset, logger *slog.Logger) {
	props := &SetProperties{}
	err := json.NewDecoder(req.Body).Decode(props)
	if err != nil {
		logger.Error("zfs.http.setProperties: Error decoding properties", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	for prop, val := range props.Set {
		err = ds.SetProperty(req.Context(), prop, val)
		if err != nil {
			logger.Error("zfs.http.setProperties: Error setting property",
				"error", err,
				"property", prop,
				"value", val,
			)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	for _, prop := range props.Unset {
		err = ds.InheritProperty(req.Context(), prop)
		if err != nil {
			logger.Error("zfs.http.setProperties: Error inheriting property", "error", err, "property", prop)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	ds, err = zfs.GetDataset(req.Context(), ds.Name, zfsExtraProperties(req)...)
	if err != nil {
		logger.Error("zfs.http.setProperties: Error fetching dataset", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(ds)
	if err != nil {
		logger.Error("zfs.http.setProperties: Error encoding json", "error", err)
		return
	}
}

func (h *HTTP) handleListSnapshots(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	filesystem := req.PathValue("filesystem")
	if !validIdentifier(filesystem) {
		logger.Info("zfs.http.handleListSnapshots: Invalid identifier", "filesystem", filesystem)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	list, err := zfs.ListSnapshots(req.Context(), zfs.ListOptions{
		ParentDataset:   fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem),
		ExtraProperties: zfsExtraProperties(req),
	})
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.Info("zfs.http.handleListSnapshots: Filesystem not found", "error", err, "filesystem", filesystem)
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.Error("zfs.http.handleListSnapshots: Error getting filesystem", "error", err, "filesystem", filesystem)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(list)
	if err != nil {
		logger.Error("zfs.http.handleListSnapshots: Error encoding json", "error", err, "filesystem", filesystem)
		return
	}
}

func (h *HTTP) handleGetResumeToken(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	filesystem := req.PathValue("filesystem")
	if !validIdentifier(filesystem) {
		logger.Info("zfs.http.handleGetResumeToken: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem), zfs.PropertyReceiveResumeToken)
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.Info("zfs.http.handleGetResumeToken: Filesystem not found", "error", err, "filesystem", filesystem)
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.Error("zfs.http.handleGetResumeToken: Error getting filesystem", "error", err, "filesystem", filesystem)
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetFilesystem:
		logger.Info("zfs.http.handleGetResumeToken: Invalid type", "filesystem", filesystem, "type", ds.Type)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(ds.ExtraProps[zfs.PropertyReceiveResumeToken]) < 10 {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	w.Header().Set(HeaderResumeReceiveToken, ds.ExtraProps[zfs.PropertyReceiveResumeToken])
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTP) handleReceiveSnapshot(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	filesystem := req.PathValue("filesystem")
	snapshot := req.PathValue("snapshot")
	logger = logger.With(
		"filesystem", filesystem,
		"snapshot", snapshot,
	)

	if !validIdentifier(filesystem) || (snapshot != "" && !validIdentifier(snapshot)) {
		logger.Info("zfs.http.handleReceiveSnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	givenResumeToken := req.Header.Get(HeaderResumeReceiveToken)
	datasetResumeToken := ""
	ds, dsErr := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem), zfs.PropertyReceiveResumeToken)
	if dsErr == nil {
		datasetResumeToken = ds.ExtraProps[zfs.PropertyReceiveResumeToken]
	}

	if datasetResumeToken != "" {
		// Set the resume token if set.
		w.Header().Set(HeaderResumeReceiveToken, datasetResumeToken)
	}

	if datasetResumeToken == "" && givenResumeToken != "" {
		logger.Info("zfs.http.handleReceiveSnapshot: Got resume token but found none on dataset", "resumeToken", givenResumeToken)
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	if givenResumeToken != "" && datasetResumeToken != givenResumeToken {
		logger.Info("zfs.http.handleReceiveSnapshot: Got invalid resume token compared with dataset",
			"givenResumeToken", givenResumeToken,
			"actualResumeToken", datasetResumeToken,
		)
		w.WriteHeader(http.StatusConflict)
		return
	}

	resumable, _ := strconv.ParseBool(req.URL.Query().Get(GETParamResumable))
	props, _ := DecodeReceiveProperties(req.URL.Query().Get(GETParamReceiveProperties))

	receiveDataset := fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot)
	if snapshot == "" {
		receiveDataset = fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem)
	}

	ds, err := zfs.ReceiveSnapshot(req.Context(), req.Body, receiveDataset, zfs.ReceiveOptions{
		BytesPerSecond:      h.getSpeed(req),
		EnableDecompression: h.getEnableDecompression(req),
		Resumable:           resumable,
		Properties:          props,
	})
	if err != nil {
		logger.Error("zfs.http.handleReceiveSnapshot: Error storing", "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(ds)
	if err != nil {
		logger.Error("zfs.http.handleReceiveSnapshot: Error encoding json", "error", err)
		return
	}
}

func (h *HTTP) handleSetSnapshotProps(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	filesystem := req.PathValue("filesystem")
	snapshot := req.PathValue("snapshot")
	logger = logger.With(
		"filesystem", filesystem,
		"snapshot", snapshot,
	)

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleSetSnapshotProps: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.Info("zfs.http.handleSetSnapshotProps: Snapshot not found", "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.Error("zfs.http.handleSetSnapshotProps: Error getting snapshot", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetSnapshot:
		logger.Info("zfs.http.handleSetSnapshotProps: Invalid type", "type", ds.Type)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	h.setProperties(w, req, ds, logger)
}

func (h *HTTP) handleGetSnapshot(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	filesystem := req.PathValue("filesystem")
	snapshot := req.PathValue("snapshot")
	logger = logger.With(
		"filesystem", filesystem,
		"snapshot", snapshot,
	)

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleGetSnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.Info("zfs.http.handleGetSnapshot: Snapshot not found", "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.Error("zfs.http.handleGetSnapshot: Error getting snapshot", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetSnapshot:
		logger.Info("zfs.http.handleGetSnapshot: Invalid type", "type", ds.Type)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = ds.SendSnapshot(req.Context(), w, zfs.SendOptions{
		BytesPerSecond:    h.getSpeed(req),
		IncludeProperties: h.getIncludeProperties(req),
		Raw:               h.getRaw(req),
		CompressionLevel:  h.getCompressionLevel(req),
	})
	if err != nil {
		logger.Error("zfs.http.handleGetSnapshot: Error sending snapshot", "error", err)
		return // Cannot send status code here.
	}
}

func (h *HTTP) handleGetSnapshotIncremental(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	filesystem := req.PathValue("filesystem")
	snapshot := req.PathValue("snapshot")
	basesnapshot := req.PathValue("basesnapshot")
	logger = logger.With(
		"filesystem", filesystem,
		"snapshot", snapshot,
		"basesnapshot", basesnapshot,
	)

	if !validIdentifier(filesystem) || !validIdentifier(basesnapshot) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleGetSnapshotIncremental: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	snap, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.Info("zfs.http.handleGetSnapshotIncremental: Snapshot not found", "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.Error("zfs.http.handleGetSnapshotIncremental: Error getting snapshot", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	case snap.Type != zfs.DatasetSnapshot:
		logger.Info("zfs.http.handleGetSnapshotIncremental: Invalid base type", "type", snap.Type)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	base, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, basesnapshot))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.Info("zfs.http.handleGetSnapshotIncremental: Base snapshot not found", "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.Error("zfs.http.handleGetSnapshotIncremental: Error getting base snapshot", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	case base.Type != zfs.DatasetSnapshot:
		logger.Info("zfs.http.handleGetSnapshotIncremental: Invalid base type", "type", base.Type)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = snap.SendSnapshot(req.Context(), w, zfs.SendOptions{
		BytesPerSecond:    h.getSpeed(req),
		IncludeProperties: h.getIncludeProperties(req),
		Raw:               h.getRaw(req),
		IncrementalBase:   base,
		CompressionLevel:  h.getCompressionLevel(req),
	})
	if err != nil {
		logger.Error("zfs.http.handleGetSnapshotIncremental: Error sending incremental snapshot", "error", err)
		return // Cannot send status code here.
	}
}

func (h *HTTP) handleResumeGetSnapshot(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	token := req.PathValue("token")
	if !validResumeTokenRegexp.MatchString(token) {
		logger.Info("zfs.http.handleResumeGetSnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err := zfs.ResumeSend(req.Context(), w, token, zfs.ResumeSendOptions{
		BytesPerSecond:   h.getSpeed(req),
		CompressionLevel: h.getCompressionLevel(req),
	})
	if err != nil {
		logger.Error("zfs.http.handleResumeGetSnapshot: Error sending snapshot", "error", err, "token", token)
		return // Cannot send status code here.
	}
}

func (h *HTTP) handleMakeSnapshot(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	filesystem := req.PathValue("filesystem")
	snapshot := req.PathValue("snapshot")
	logger = logger.With(
		"filesystem", filesystem,
		"snapshot", snapshot,
	)

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleMakeSnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.Info("zfs.http.handleMakeSnapshot: Filesystem not found", "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.Error("zfs.http.handleMakeSnapshot: Error getting filesystem", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetFilesystem:
		logger.Info("zfs.http.handleMakeSnapshot: Invalid type", "type", ds.Type)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err = ds.Snapshot(req.Context(), snapshot, zfs.SnapshotOptions{})
	if err != nil {
		logger.Error("zfs.http.handleMakeSnapshot: Error making snapshot", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(ds)
	if err != nil {
		logger.Error("zfs.http.handleMakeSnapshot: Error encoding json", "error", err)
		return
	}
}

func (h *HTTP) handleDestroyFilesystem(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	if !h.config.Permissions.AllowDestroyFilesystems {
		logger.Info("zfs.http.handleDestroyFilesystem: Destroy forbidden")
		w.WriteHeader(http.StatusForbidden)
		return
	}

	filesystem := req.PathValue("filesystem")
	if !validIdentifier(filesystem) {
		logger.Info("zfs.http.handleDestroyFilesystem: Invalid identifier", "filesystem", filesystem)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.Info("zfs.http.handleDestroyFilesystem: Filesystem not found", "error", err, "filesystem", filesystem)
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.Error("zfs.http.handleDestroyFilesystem: Error getting filesystem", "error", err, "filesystem", filesystem)
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetFilesystem:
		logger.Info("zfs.http.handleDestroyFilesystem: Invalid type", "type", ds.Type, "filesystem", filesystem)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// TODO: FIXME: Allow recursive deletes?
	err = ds.Destroy(req.Context(), zfs.DestroyOptions{})
	if err != nil {
		logger.Error("zfs.http.handleDestroyFilesystem: Error destroying", "error", err, "filesystem", filesystem)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTP) handleDestroySnapshot(w http.ResponseWriter, req *http.Request, logger *slog.Logger) {
	if !h.config.Permissions.AllowDestroySnapshots {
		logger.Info("zfs.http.handleDestroySnapshot: Destroy forbidden")
		w.WriteHeader(http.StatusForbidden)
		return
	}

	filesystem := req.PathValue("filesystem")
	snapshot := req.PathValue("snapshot")
	logger = logger.With(
		"filesystem", filesystem,
		"snapshot", snapshot,
	)

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleDestroySnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.Info("zfs.http.handleDestroySnapshot: Snapshot not found", "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.Error("zfs.http.handleDestroySnapshot: Error getting snapshot", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetSnapshot:
		logger.Info("zfs.http.handleDestroySnapshot: Invalid type", "type", ds.Type)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = ds.Destroy(req.Context(), zfs.DestroyOptions{})
	if err != nil {
		logger.Error("zfs.http.handleDestroySnapshot: Error destroying", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
