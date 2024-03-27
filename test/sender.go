package main

import (
	"context"
	"fmt"
	zfs "github.com/vansante/go-zfsutils"
	zfshttp "github.com/vansante/go-zfsutils/http"
	"log/slog"
	"os"
	"time"
)

func main() {
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	logger := slog.New(h)
	slog.SetDefault(logger)

	client := zfshttp.NewClient("http://localhost:1337", logger)

	snaps, err := zfs.ListSnapshots(context.Background(), zfs.ListOptions{
		ParentDataset: "testpool/testfs",
		DatasetType:   zfs.DatasetFilesystem,
	})
	if err != nil {
		panic(err)
	}
	if len(snaps) != 1 {
		panic(snaps)
	}

	logger.Info("dataset", "ds", snaps[0])

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	results, err := client.Send(ctx, zfshttp.SnapshotSend{
		SendOptions: zfs.SendOptions{
			BytesPerSecond:    10 * 1024 * 1024,
			Raw:               true,
			IncludeProperties: true,
			IncrementalBase:   nil,
			CompressionLevel:  0,
		},
		DatasetName:  "testpool/newdataset",
		SnapshotName: "newsnap",
		Snapshot:     &snaps[0],
		Properties: map[string]string{
			"nl.test.bla": "hoihoi",
		},
	})

	fmt.Printf("Bytes sent: %d\n", results.BytesSent)
	fmt.Printf("Time taken: %s\n", results.TimeTaken.String())

	panic(err)
}
