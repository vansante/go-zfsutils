package http

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/vansante/go-zfs"

	"github.com/julienschmidt/httprouter"
)

const (
	GETParamExtraProperties   = "extraProps"
	GETParamResumable         = "resumable"
	GETParamIncludeProperties = "includeProps"
	GETParamRaw               = "raw"
	GETParamReceiveProperties = "receiveProps"
	GETParamBytesPerSecond    = "bytesPerSecond"
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

func (h *HTTP) handleListFilesystems(w http.ResponseWriter, req *http.Request, _ httprouter.Params, logger zfs.Logger) {
	list, err := zfs.Filesystems(req.Context(), h.config.ParentDataset, zfsExtraProperties(req)...)
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.WithError(err).Info("zfs.http.handleListFilesystems: Parent dataset not found")
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.WithError(err).Error("zfs.http.handleListFilesystems: Error getting filesystems")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(list)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleListFilesystems: Error encoding json")
		return
	}
}

func (h *HTTP) handleSetFilesystemProps(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger zfs.Logger) {
	filesystem := ps.ByName("filesystem")
	logger = logger.WithFields(map[string]interface{}{
		"filesystem": filesystem,
	})

	if !validIdentifier(filesystem) {
		logger.Info("zfs.http.handleSetFilesystemProps: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.WithError(err).Info("zfs.http.handleSetFilesystemProps: Filesystem not found")
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.WithError(err).Error("zfs.http.handleSetFilesystemProps: Error getting filesystem")
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetFilesystem:
		logger.WithField("type", ds.Type).Info("zfs.http.handleSetFilesystemProps: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	h.setProperties(w, req, ds, logger)
}

func (h *HTTP) setProperties(w http.ResponseWriter, req *http.Request, ds *zfs.Dataset, logger zfs.Logger) {
	props := &SetProperties{}
	err := json.NewDecoder(req.Body).Decode(props)
	if err != nil {
		logger.WithError(err).Error("zfs.http.setProperties: Error decoding properties")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	for prop, val := range props.Set {
		err = ds.SetProperty(req.Context(), prop, val)
		if err != nil {
			logger.WithError(err).WithFields(map[string]interface{}{
				"property": prop,
				"value":    val,
			}).Error("zfs.http.setProperties: Error setting property")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	for _, prop := range props.Unset {
		err = ds.InheritProperty(req.Context(), prop)
		if err != nil {
			logger.WithError(err).WithField("property", prop).Error("zfs.http.setProperties: Error inheriting property")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	ds, err = zfs.GetDataset(req.Context(), ds.Name, zfsExtraProperties(req)...)
	if err != nil {
		logger.WithError(err).Error("zfs.http.setProperties: Error fetching dataset")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(ds)
	if err != nil {
		logger.WithError(err).Error("zfs.http.setProperties: Error encoding json")
		return
	}
}

func (h *HTTP) handleListSnapshots(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger zfs.Logger) {
	filesystem := ps.ByName("filesystem")
	logger = logger.WithFields(map[string]interface{}{
		"filesystem": filesystem,
	})

	if !validIdentifier(filesystem) {
		logger.Info("zfs.http.handleListSnapshots: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	list, err := zfs.Snapshots(req.Context(), fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem), zfsExtraProperties(req)...)
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.WithError(err).Info("zfs.http.handleListSnapshots: Filesystem not found")
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.WithError(err).Error("zfs.http.handleListSnapshots: Error getting filesystem")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(list)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleListSnapshots: Error encoding json")
		return
	}
}

func (h *HTTP) handleGetResumeToken(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger zfs.Logger) {
	filesystem := ps.ByName("filesystem")
	logger = logger.WithFields(map[string]interface{}{
		"filesystem": filesystem,
	})

	if !validIdentifier(filesystem) {
		logger.Info("zfs.http.handleGetResumeToken: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem), zfs.PropertyReceiveResumeToken)
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.WithError(err).Info("zfs.http.handleGetResumeToken: Filesystem not found")
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.WithError(err).Error("zfs.http.handleGetResumeToken: Error getting filesystem")
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetFilesystem:
		logger.WithField("type", ds.Type).Info("zfs.http.handleGetResumeToken: Invalid type")
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

func (h *HTTP) handleReceiveSnapshot(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger zfs.Logger) {
	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	logger = logger.WithFields(map[string]interface{}{
		"filesystem": filesystem,
		"snapshot":   snapshot,
	})

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
		logger.WithField("resumeToken", givenResumeToken).Info("zfs.http.handleReceiveSnapshot: Got resume token but found none on dataset")
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	if givenResumeToken != "" && datasetResumeToken != givenResumeToken {
		logger.WithFields(map[string]interface{}{
			"givenResumeToken":  givenResumeToken,
			"actualResumeToken": datasetResumeToken,
		}).Info("zfs.http.handleReceiveSnapshot: Got invalid resume token compared with dataset")
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
		BytesPerSecond: h.getSpeed(req),
		Resumable:      resumable,
		Properties:     props,
	})
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleReceiveSnapshot: Error storing")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(ds)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleReceiveSnapshot: Error encoding json")
		return
	}
}

func (h *HTTP) handleSetSnapshotProps(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger zfs.Logger) {
	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	logger = logger.WithFields(map[string]interface{}{
		"filesystem": filesystem,
		"snapshot":   snapshot,
	})

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleSetSnapshotProps: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.WithError(err).Info("zfs.http.handleSetSnapshotProps: Snapshot not found")
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.WithError(err).Error("zfs.http.handleSetSnapshotProps: Error getting snapshot")
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetSnapshot:
		logger.WithField("type", ds.Type).Info("zfs.http.handleSetSnapshotProps: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	h.setProperties(w, req, ds, logger)
}

func (h *HTTP) handleGetSnapshot(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger zfs.Logger) {
	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	logger = logger.WithFields(map[string]interface{}{
		"filesystem": filesystem,
		"snapshot":   snapshot,
	})

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleGetSnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.WithError(err).Info("zfs.http.handleGetSnapshot: Snapshot not found")
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.WithError(err).Error("zfs.http.handleGetSnapshot: Error getting snapshot")
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetSnapshot:
		logger.WithField("type", ds.Type).Info("zfs.http.handleGetSnapshot: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = ds.SendSnapshot(req.Context(), w, zfs.SendOptions{
		BytesPerSecond:    h.getSpeed(req),
		IncludeProperties: h.getIncludeProperties(req),
		Raw:               h.getRaw(req),
	})
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleGetSnapshot: Error sending snapshot")
		return // Cannot send status code here.
	}
}

func (h *HTTP) handleGetSnapshotIncremental(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger zfs.Logger) {
	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	basesnapshot := ps.ByName("basesnapshot")
	logger = logger.WithFields(map[string]interface{}{
		"filesystem":   filesystem,
		"snapshot":     snapshot,
		"basesnapshot": basesnapshot,
	})

	if !validIdentifier(filesystem) || !validIdentifier(basesnapshot) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleGetSnapshotIncremental: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	snap, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.WithError(err).Info("zfs.http.handleGetSnapshotIncremental: Snapshot not found")
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.WithError(err).Error("zfs.http.handleGetSnapshotIncremental: Error getting snapshot")
		w.WriteHeader(http.StatusInternalServerError)
		return
	case snap.Type != zfs.DatasetSnapshot:
		logger.WithField("type", snap.Type).Info("zfs.http.handleGetSnapshotIncremental: Invalid base type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	base, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, basesnapshot))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.WithError(err).Info("zfs.http.handleGetSnapshotIncremental: Base snapshot not found")
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.WithError(err).Error("zfs.http.handleGetSnapshotIncremental: Error getting base snapshot")
		w.WriteHeader(http.StatusInternalServerError)
		return
	case base.Type != zfs.DatasetSnapshot:
		logger.WithField("type", base.Type).Info("zfs.http.handleGetSnapshotIncremental: Invalid base type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = snap.SendSnapshot(req.Context(), w, zfs.SendOptions{
		BytesPerSecond:    h.getSpeed(req),
		IncludeProperties: h.getIncludeProperties(req),
		Raw:               h.getRaw(req),
		IncrementalBase:   base,
	})
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleGetSnapshotIncremental: Error sending incremental snapshot")
		return // Cannot send status code here.
	}
}

func (h *HTTP) handleResumeGetSnapshot(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger zfs.Logger) {
	token := ps.ByName("token")
	if !validResumeTokenRegexp.MatchString(token) {
		logger.Info("zfs.http.handleResumeGetSnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	logger = logger.WithFields(map[string]interface{}{
		"token": token,
	})

	err := zfs.ResumeSend(req.Context(), w, token, zfs.ResumeSendOptions{
		BytesPerSecond: h.getSpeed(req),
	})
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleResumeGetSnapshot: Error sending snapshot")
		return // Cannot send status code here.
	}
}

func (h *HTTP) handleMakeSnapshot(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger zfs.Logger) {
	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	logger = logger.WithFields(map[string]interface{}{
		"filesystem": filesystem,
		"snapshot":   snapshot,
	})

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleMakeSnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.WithError(err).Info("zfs.http.handleMakeSnapshot: Filesystem not found")
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.WithError(err).Error("zfs.http.handleMakeSnapshot: Error getting filesystem")
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetFilesystem:
		logger.WithField("type", ds.Type).Info("zfs.http.handleMakeSnapshot: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err = ds.Snapshot(req.Context(), snapshot, false)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleMakeSnapshot: Error making snapshot")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(ds)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleMakeSnapshot: Error encoding json")
		return
	}
}

func (h *HTTP) handleDestroyFilesystem(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger zfs.Logger) {
	if !h.config.Permissions.AllowDestroyFilesystems {
		logger.Info("zfs.http.handleDestroyFilesystem: Destroy forbidden")
		w.WriteHeader(http.StatusForbidden)
		return
	}

	filesystem := ps.ByName("filesystem")
	logger = logger.WithFields(map[string]interface{}{
		"filesystem": filesystem,
	})

	if !validIdentifier(filesystem) {
		logger.Info("zfs.http.handleDestroyFilesystem: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.WithError(err).Info("zfs.http.handleDestroyFilesystem: Filesystem not found")
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.WithError(err).Error("zfs.http.handleDestroyFilesystem: Error getting filesystem")
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetFilesystem:
		logger.WithField("type", ds.Type).Info("zfs.http.handleDestroyFilesystem: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	flag := zfs.DestroyDefault
	// TODO: FIXME: Allow recursive deletes?
	err = ds.Destroy(req.Context(), flag)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleDestroyFilesystem: Error destroying")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTP) handleDestroySnapshot(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger zfs.Logger) {
	if !h.config.Permissions.AllowDestroySnapshots {
		logger.Info("zfs.http.handleDestroySnapshot: Destroy forbidden")
		w.WriteHeader(http.StatusForbidden)
		return
	}

	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	logger = logger.WithFields(map[string]interface{}{
		"filesystem": filesystem,
		"snapshot":   snapshot,
	})

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleDestroySnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := zfs.GetDataset(req.Context(), fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot))
	switch {
	case errors.Is(err, zfs.ErrDatasetNotFound):
		logger.WithError(err).Info("zfs.http.handleDestroySnapshot: Snapshot not found")
		w.WriteHeader(http.StatusNotFound)
		return
	case err != nil:
		logger.WithError(err).Error("zfs.http.handleDestroySnapshot: Error getting snapshot")
		w.WriteHeader(http.StatusInternalServerError)
		return
	case ds.Type != zfs.DatasetSnapshot:
		logger.WithField("type", ds.Type).Info("zfs.http.handleDestroySnapshot: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = ds.Destroy(req.Context(), zfs.DestroyDefault)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleDestroySnapshot: Error destroying")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
