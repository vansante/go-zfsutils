package job

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_zfsSend(t *testing.T) {
	started := time.Now().Add(-time.Minute)
	updated := time.Now()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	send := zfsSend{
		dataset:   "testpool/testfs1@backup_1",
		host:      "http://host1",
		bytesSent: 1234,
		started:   started,
		updated:   updated,
		cancel:    cancel,
	}

	var iface ZFSSend = send
	require.Equal(t, "testpool/testfs1@backup_1", iface.Dataset())
	require.Equal(t, "testfs1", iface.DatasetName())
	require.Equal(t, "backup_1", iface.SnapshotName())
	require.Equal(t, "http://host1", iface.Host())
	require.EqualValues(t, 1234, iface.BytesSent())
	require.Equal(t, started, iface.StartedAt())
	require.Equal(t, updated, iface.UpdatedAt())

	require.NoError(t, ctx.Err())
	iface.CancelSend()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
