package job

import (
	"context"
	"time"
)

// ZFSSend represents a ZFS send process
type ZFSSend interface {
	// Dataset returns the full Dataset(=Snapshot) that is sent
	Dataset() string
	// DatasetName returns the dataset name
	DatasetName() string
	// SnapshotName returns the snapshot name
	SnapshotName() string
	// Host returns the host/URL where the snapshot is being sent to
	Host() string
	// BytesSent returns how many bytes have been sent
	BytesSent() int64
	// UpdatedAt returns when this was last updated
	UpdatedAt() time.Time
	// StartedAt returns when send was started
	StartedAt() time.Time
	// CancelSend cancels the send process
	CancelSend()
}

type zfsSend struct {
	dataset   string
	host      string
	bytesSent int64
	started   time.Time
	updated   time.Time
	cancel    context.CancelFunc
}

func (z zfsSend) Dataset() string {
	return z.dataset
}

func (z zfsSend) DatasetName() string {
	return datasetName(z.dataset, true)
}

func (z zfsSend) SnapshotName() string {
	return snapshotName(z.dataset)
}

func (z zfsSend) Host() string {
	return z.host
}

func (z zfsSend) BytesSent() int64 {
	return z.bytesSent
}

func (z zfsSend) UpdatedAt() time.Time {
	return z.updated
}

func (z zfsSend) StartedAt() time.Time {
	return z.started
}

func (z zfsSend) CancelSend() {
	z.cancel()
}
