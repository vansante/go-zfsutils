package zfs

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/sirupsen/logrus"
)

const (
	GETParamExtraProperties   = "extraProps"
	GETParamResumable         = "resumable"
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

func (h *HTTP) handleListFilesystems(w http.ResponseWriter, req *http.Request, _ httprouter.Params, logger *logrus.Entry) {
	list, err := Filesystems(h.config.ParentDataset, zfsExtraProperties(req))
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleListFilesystems: Error listing")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(list)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleListFilesystems: Error encoding json")
		return
	}
}

func (h *HTTP) handleSetFilesystemProps(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger *logrus.Entry) {
	filesystem := ps.ByName("filesystem")
	logger = logger.WithFields(logrus.Fields{
		"filesystem": filesystem,
	})

	if !validIdentifier(filesystem) {
		logger.Info("zfs.http.handleSetFilesystemProps: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := GetDataset(fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem), nil)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleSetFilesystemProps: Filesystem not found")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if ds.Type != DatasetFilesystem {
		logger.WithField("dataset", ds).Error("zfs.http.handleSetFilesystemProps: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	h.setProperties(w, req, ds, logger)
}

func (h *HTTP) setProperties(w http.ResponseWriter, req *http.Request, ds *Dataset, logger *logrus.Entry) {
	props := &SetProperties{}
	err := json.NewDecoder(req.Body).Decode(props)
	if err != nil {
		logger.WithError(err).Error("zfs.http.setProperties: Error decoding properties")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	for prop, val := range props.Set {
		err = ds.SetProperty(prop, val)
		if err != nil {
			logger.WithError(err).WithFields(logrus.Fields{
				"property": prop,
				"value":    val,
			}).Error("zfs.http.setProperties: Error setting property")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	for _, prop := range props.Unset {
		err = ds.InheritProperty(prop)
		if err != nil {
			logger.WithError(err).WithField("property", prop).Error("zfs.http.setProperties: Error inheriting property")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	ds, err = GetDataset(ds.Name, zfsExtraProperties(req))
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

func (h *HTTP) handleListSnapshots(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger *logrus.Entry) {
	filesystem := ps.ByName("filesystem")
	logger = logger.WithFields(logrus.Fields{
		"filesystem": filesystem,
	})

	if !validIdentifier(filesystem) {
		logger.Info("zfs.http.handleListSnapshots: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	list, err := Snapshots(fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem), zfsExtraProperties(req))
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleListSnapshots: Error listing")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(list)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleListSnapshots: Error encoding json")
		return
	}
}

func (h *HTTP) handleReceiveSnapshot(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger *logrus.Entry) {
	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	logger = logger.WithFields(logrus.Fields{
		"filesystem": filesystem,
		"snapshot":   snapshot,
	})

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleReceiveSnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, dsErr := GetDataset(fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem), []string{PropertyReceiveResumeToken})
	if dsErr == nil {
		if ds.ExtraProps[PropertyReceiveResumeToken] != "" {
			w.Header().Set(HeaderResumeReceiveToken, ds.ExtraProps[PropertyReceiveResumeToken])
		}
		w.WriteHeader(http.StatusConflict)
		return
	}

	resumable, _ := strconv.ParseBool(req.URL.Query().Get(GETParamResumable))
	props, _ := DecodeReceiveProperties(req.URL.Query().Get(GETParamReceiveProperties))

	ds, err := ReceiveSnapshot(h.getReader(req),
		fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot),
		resumable, props,
	)
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

func (h *HTTP) handleSetSnapshotProps(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger *logrus.Entry) {
	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	logger = logger.WithFields(logrus.Fields{
		"filesystem": filesystem,
		"snapshot":   snapshot,
	})

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleSetSnapshotProps: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := GetDataset(fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot), nil)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleSetSnapshotProps: Snapshot not found")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if ds.Type != DatasetSnapshot {
		logger.WithField("dataset", ds).Error("zfs.http.handleSetSnapshotProps: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	h.setProperties(w, req, ds, logger)
}

func (h *HTTP) handleGetSnapshot(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger *logrus.Entry) {
	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	logger = logger.WithFields(logrus.Fields{
		"filesystem": filesystem,
		"snapshot":   snapshot,
	})

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleGetSnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := GetDataset(fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot), nil)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleGetSnapshot: Error retrieving")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if ds.Type != DatasetSnapshot {
		logger.WithField("dataset", ds).Error("zfs.http.handleGetSnapshot: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = ds.SendSnapshot(h.getWriter(w, req), true)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleGetSnapshot: Error sending snapshot")
		return // Cannot send status code here.
	}
}

func (h *HTTP) handleGetSnapshotIncremental(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger *logrus.Entry) {
	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	basesnapshot := ps.ByName("basesnapshot")
	logger = logger.WithFields(logrus.Fields{
		"filesystem":   filesystem,
		"snapshot":     snapshot,
		"basesnapshot": basesnapshot,
	})

	if !validIdentifier(filesystem) || !validIdentifier(basesnapshot) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleGetSnapshotIncremental: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	snap, err := GetDataset(fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot), nil)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleGetSnapshotIncremental: Error retrieving")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if snap.Type != DatasetSnapshot {
		logger.WithField("dataset", snap).Error("zfs.http.handleGetSnapshotIncremental: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	base, err := GetDataset(fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, basesnapshot), nil)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleGetSnapshotIncremental: Error retrieving base")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if base.Type != DatasetSnapshot {
		logger.WithField("dataset", base).Error("zfs.http.handleGetSnapshotIncremental: Invalid base type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = base.IncrementalSend(h.getWriter(w, req), snap, true)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleGetSnapshotIncremental: Error sending incremental snapshot")
		return // Cannot send status code here.
	}
}

func (h *HTTP) handleResumeGetSnapshot(w http.ResponseWriter, req *http.Request, ps httprouter.Params, logger *logrus.Entry) {
	token := ps.ByName("token")
	if !validResumeTokenRegexp.MatchString(token) {
		logger.Info("zfs.http.handleResumeGetSnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	logger = logger.WithFields(logrus.Fields{
		"token": token,
	})

	err := ResumeSend(h.getWriter(w, req), token)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleResumeGetSnapshot: Error sending snapshot")
		return // Cannot send status code here.
	}
}

func (h *HTTP) handleMakeSnapshot(w http.ResponseWriter, _ *http.Request, ps httprouter.Params, logger *logrus.Entry) {
	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	logger = logger.WithFields(logrus.Fields{
		"filesystem": filesystem,
		"snapshot":   snapshot,
	})

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleMakeSnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := GetDataset(fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem), nil)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleMakeSnapshot: Filesystem not found")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if ds.Type != DatasetFilesystem {
		logger.WithField("dataset", ds).Error("zfs.http.handleMakeSnapshot: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err = ds.Snapshot(snapshot, false)
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

func (h *HTTP) handleDestroyFilesystem(w http.ResponseWriter, _ *http.Request, ps httprouter.Params, logger *logrus.Entry) {
	if !h.config.AllowDestroy {
		logger.Info("zfs.http.handleDestroyFilesystem: Destroy forbidden")
		w.WriteHeader(http.StatusForbidden)
		return
	}

	filesystem := ps.ByName("filesystem")
	logger = logger.WithFields(logrus.Fields{
		"filesystem": filesystem,
	})

	if !validIdentifier(filesystem) {
		logger.Info("zfs.http.handleDestroyFilesystem: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := GetDataset(fmt.Sprintf("%s/%s", h.config.ParentDataset, filesystem), nil)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleDestroyFilesystem: Filesystem not found")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if ds.Type != DatasetFilesystem {
		logger.WithField("dataset", ds).Error("zfs.http.handleDestroyFilesystem: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	flag := DestroyDefault
	// TODO: FIXME: Allow recursive deletes?
	err = ds.Destroy(flag)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleDestroyFilesystem: Error destroying")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *HTTP) handleDestroySnapshot(w http.ResponseWriter, _ *http.Request, ps httprouter.Params, logger *logrus.Entry) {
	if !h.config.AllowDestroy {
		logger.Info("zfs.http.handleDestroySnapshot: Destroy forbidden")
		w.WriteHeader(http.StatusForbidden)
		return
	}

	filesystem := ps.ByName("filesystem")
	snapshot := ps.ByName("snapshot")
	logger = logger.WithFields(logrus.Fields{
		"filesystem": filesystem,
		"snapshot":   snapshot,
	})

	if !validIdentifier(filesystem) || !validIdentifier(snapshot) {
		logger.Info("zfs.http.handleDestroySnapshot: Invalid identifier")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds, err := GetDataset(fmt.Sprintf("%s/%s@%s", h.config.ParentDataset, filesystem, snapshot), nil)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleDestroySnapshot: Snapshot not found")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if ds.Type != DatasetSnapshot {
		logger.WithField("dataset", ds).Error("zfs.http.handleDestroySnapshot: Invalid type")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = ds.Destroy(DestroyDefault)
	if err != nil {
		logger.WithError(err).Error("zfs.http.handleDestroySnapshot: Error destroying")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
