package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	zfs "github.com/vansante/go-zfsutils"
)

var (
	ErrDatasetNotFound    = errors.New("dataset not found")
	ErrInvalidResumeToken = errors.New("invalid resume token given")
	ErrResumeNotPossible  = errors.New("resume not possible")
)

// Client is the struct used to send requests to a zfs http server
type Client struct {
	server  string
	headers map[string]string
	logger  *slog.Logger
	client  *http.Client
}

// NewClient creates a new client for a zfs http server
func NewClient(server string, logger *slog.Logger) *Client {
	return &Client{
		server:  server,
		headers: make(map[string]string, 8),
		logger:  logger,
		client:  http.DefaultClient,
	}
}

// SetClient configures a custom http client for doing requests
func (c *Client) SetClient(client *http.Client) {
	c.client = client
}

// SetHeader configures a header to be sent with all requests
func (c *Client) SetHeader(name, value string) {
	c.headers[name] = value
}

// Server returns the server
func (c *Client) Server() string {
	return c.server
}

func (c *Client) request(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("%s/%s", c.server, url), body)
	if err != nil {
		return nil, err
	}
	for hdr := range c.headers {
		req.Header.Set(hdr, c.headers[hdr])
	}
	return req, nil
}

// DatasetSnapshots requests the snapshots for a remote dataset
func (c *Client) DatasetSnapshots(ctx context.Context, dataset string, extraProps []string) ([]zfs.Dataset, error) {
	req, err := c.request(ctx, http.MethodGet, fmt.Sprintf("filesystems/%s/snapshots?%s=%s",
		dataset,
		GETParamExtraProperties, strings.Join(extraProps, ","),
	), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error requesting remote snapshots: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// Continue
	case http.StatusNotFound:
		return nil, ErrDatasetNotFound
	default:
		return nil, fmt.Errorf("unexpected status %d requesting remote snapshots", resp.StatusCode)
	}

	var datasets []zfs.Dataset
	err = json.NewDecoder(resp.Body).Decode(&datasets)
	return datasets, err
}

// ResumableSendToken requests the resume token for a remote dataset, if there is one
func (c *Client) ResumableSendToken(ctx context.Context, dataset string) (token string, curBytes uint64, err error) {
	req, err := c.request(ctx, http.MethodGet, fmt.Sprintf("filesystems/%s/resume-token",
		dataset,
	), nil)
	if err != nil {
		return "", 0, fmt.Errorf("error creating token request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("error requesting resume token: %w", err)
	}
	_ = resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		curBytes, _ = strconv.ParseUint(resp.Header.Get(HeaderResumeReceivedBytes), 10, 64)
		return resp.Header.Get(HeaderResumeReceiveToken), curBytes, nil
	case http.StatusNotFound:
		return "", 0, ErrDatasetNotFound
	case http.StatusPreconditionFailed:
		return "", 0, nil // Nothing to resume
	default:
		return "", 0, fmt.Errorf("unexpected status %d requesting resume token", resp.StatusCode)
	}
}

// ResumeSendOptions is a struct for a resume of a send job to a remote server using a Client
type ResumeSendOptions struct {
	zfs.ResumeSendOptions

	// ProgressFn: Set a callback function to receive updates about progress
	ProgressFn zfs.ProgressCallback
	// ProgressEvery determines progress update interval
	ProgressEvery time.Duration
}

// ResumeSend resumes a send for a dataset given the resume token
func (c *Client) ResumeSend(ctx context.Context, dataset, resumeToken string, options ResumeSendOptions) (SendResult, error) {
	pipeRdr, pipeWrtr := io.Pipe()

	sendCtx, cancelSend := context.WithCancel(ctx)
	go func() {
		err := zfs.ResumeSend(sendCtx, pipeWrtr, resumeToken, options.ResumeSendOptions)
		if err != nil {
			c.logger.Error("zfs.http.Client.ResumeSend: Error sending resume stream",
				"error", err,
				"server", c.server,
				"dataset", dataset,
				"resumeToken", resumeToken,
			)
		}
		err = pipeWrtr.Close()
		if err != nil {
			c.logger.Error("zfs.http.Client.sendWithBase: Error closing snapshot pipe",
				"error", err,
				"server", c.server,
				"dataset", dataset,
				"resumeToken", resumeToken,
			)
		}
	}()

	startTime := time.Now()
	countReader := zfs.NewCountReader(pipeRdr)
	countReader.SetProgressCallback(options.ProgressEvery, options.ProgressFn)
	req, err := c.request(ctx, http.MethodPut, fmt.Sprintf("filesystems/%s/snapshots?%s=%s&%s=%s",
		dataset,
		GETParamResumable, "true",
		GETParamEnableDecompression, strconv.FormatBool(options.CompressionLevel > 0),
	), countReader)
	if err != nil {
		cancelSend()
		return SendResult{
			BytesSent: countReader.Count(),
			TimeTaken: time.Since(startTime),
		}, fmt.Errorf("error creating resume request: %w", err)
	}

	err = c.doSendStream(req, pipeWrtr, cancelSend)
	return SendResult{
		BytesSent: countReader.Count(),
		TimeTaken: time.Since(startTime),
	}, err
}

// SnapshotSendOptions is a struct for a send job to a remote server using a Client
type SnapshotSendOptions struct {
	zfs.SendOptions

	// Which dataset to send to
	DatasetName string
	// Which snapshot to send to (optional)
	SnapshotName string
	// The snapshot to send
	Snapshot *zfs.Dataset
	// Resumable determines whether the stream can be resumed
	Resumable bool
	// ReceiveForceRollback sets whether the receiving dataset is rolled back to the received snapshot
	ReceiveForceRollback bool

	// Properties are set on the receiving dataset (filesystem usually)
	Properties ReceiveProperties

	// ProgressFn: Set a callback function to receive updates about progress
	ProgressFn zfs.ProgressCallback
	// ProgressEvery determines progress update interval
	ProgressEvery time.Duration
}

// SendResult contains some statistics from the sending of a snapshot
type SendResult struct {
	BytesSent int64
	TimeTaken time.Duration
}

// Send sends the snapshot job to the remote server
func (c *Client) Send(ctx context.Context, send SnapshotSendOptions) (SendResult, error) {
	pipeRdr, pipeWrtr := io.Pipe()

	sendCtx, cancelSend := context.WithCancel(ctx)
	go func() {
		err := send.Snapshot.SendSnapshot(sendCtx, pipeWrtr, send.SendOptions)
		if err != nil {
			c.logger.Error("zfs.http.Client.sendWithBase: Error sending incremental snapshot stream",
				"error", err,
				"server", c.client,
				"snapshot", send.Snapshot.Name,
				"baseSnapshot", send.IncrementalBase,
			)
		}
		err = pipeWrtr.Close()
		if err != nil {
			c.logger.Error("zfs.http.Client.sendWithBase: Error closing snapshot pipe",
				"error", err,
				"server", c.client,
				"snapshot", send.Snapshot.Name,
				"baseSnapshot", send.IncrementalBase,
			)
		}
	}()

	url := fmt.Sprintf("filesystems/%s/snapshots", send.DatasetName)
	if send.SnapshotName != "" {
		url = fmt.Sprintf("filesystems/%s/snapshots/%s", send.DatasetName, send.SnapshotName)
	}

	startTime := time.Now()
	countReader := zfs.NewCountReader(pipeRdr)
	countReader.SetProgressCallback(send.ProgressEvery, send.ProgressFn)
	req, err := c.request(ctx, http.MethodPut, url, countReader)
	if err != nil {
		cancelSend()
		return SendResult{}, fmt.Errorf("error creating incremental send request: %w", err)
	}
	q := req.URL.Query()
	q.Set(GETParamResumable, strconv.FormatBool(send.Resumable))
	q.Set(GETParamEnableDecompression, strconv.FormatBool(send.CompressionLevel > 0))
	q.Set(GETParamForceRollback, strconv.FormatBool(send.ReceiveForceRollback))
	if len(send.Properties) > 0 {
		q.Set(GETParamReceiveProperties, send.Properties.Encode())
	}
	req.URL.RawQuery = q.Encode() // Add new GET params
	err = c.doSendStream(req, pipeWrtr, cancelSend)
	result := SendResult{
		BytesSent: countReader.Count(),
		TimeTaken: time.Since(startTime),
	}
	return result, err
}

func (c *Client) doSendStream(req *http.Request, pipeWrtr *io.PipeWriter, cancelSend context.CancelFunc) error {
	resp, err := c.client.Do(req)
	if err != nil {
		_ = pipeWrtr.Close()
		return fmt.Errorf("error sending stream: %w", err)
	}
	defer resp.Body.Close()

	cancelSend()
	err = pipeWrtr.Close()
	if err != nil {
		return fmt.Errorf("error closing transfer pipe: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusCreated:
		return nil
	case http.StatusConflict:
		return ErrInvalidResumeToken
	case http.StatusPreconditionFailed:
		return ErrResumeNotPossible
	case http.StatusNotFound:
		return ErrDatasetNotFound
	default:
		return fmt.Errorf("unexpected status %d sending stream", resp.StatusCode)
	}
}

// SetFilesystemProperties sets and/or unsets properties on the remote zfs filesystem
func (c *Client) SetFilesystemProperties(ctx context.Context, filesystem string, props SetProperties) error {
	payload, err := json.Marshal(&props)
	if err != nil {
		return fmt.Errorf("error encoding payload json: %w", err)
	}

	req, err := c.request(ctx, http.MethodPatch, fmt.Sprintf("filesystems/%s",
		filesystem,
	), bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("error creating property request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	return nil
}

// SetSnapshotProperties sets and/or unsets properties on the remote zfs snapshot
func (c *Client) SetSnapshotProperties(ctx context.Context, filesystem, snapshot string, props SetProperties) error {
	payload, err := json.Marshal(&props)
	if err != nil {
		return fmt.Errorf("error encoding payload json: %w", err)
	}

	req, err := c.request(ctx, http.MethodPatch, fmt.Sprintf("filesystems/%s/snapshots/%s",
		filesystem, snapshot,
	), bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("error creating property request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	return nil
}
