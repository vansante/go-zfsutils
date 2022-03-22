package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/vansante/go-zfs"
)

var (
	ErrDatasetNotFound    = errors.New("dataset not found")
	ErrInvalidResumeToken = errors.New("invalid resume token given")
	ErrResumeNotPossible  = errors.New("resume not possible")
)

// Client is the struct used to send requests to a zfs http server
type Client struct {
	server    string
	authToken string
	client    *http.Client
}

// NewClient creates a new client for a zfs http server
func NewClient(server, authToken string) *Client {
	return &Client{
		server:    server,
		authToken: authToken,
		client:    http.DefaultClient,
	}
}

func (c *Client) request(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("%s/%s", c.server, url), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(AuthenticationTokenHeader, c.authToken)
	return req, nil
}

// DatasetSnapshots requests the snapshots for a remote dataset
func (c *Client) DatasetSnapshots(ctx context.Context, dataset string, extraProps []string) ([]*zfs.Dataset, error) {
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
		return nil, fmt.Errorf("unexpected status %d requesting remote snapshots: %w", resp.StatusCode, err)
	}

	var datasets []*zfs.Dataset
	err = json.NewDecoder(resp.Body).Decode(&datasets)
	return datasets, err
}

// ResumableSendToken requests the resume token for a remote dataset, if there is one
func (c *Client) ResumableSendToken(ctx context.Context, dataset string) (string, error) {
	req, err := c.request(ctx, http.MethodGet, fmt.Sprintf("filesystems/%s/resume-token",
		dataset,
	), nil)
	if err != nil {
		return "", fmt.Errorf("error creating token request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error requesting resume token: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		return resp.Header.Get(HeaderResumeReceiveToken), nil
	case http.StatusNotFound:
		return "", ErrDatasetNotFound
	case http.StatusPreconditionFailed:
		return "", nil // Nothing to resume
	default:
		return "", fmt.Errorf("unexpected status %d requesting resume token: %w", resp.StatusCode, err)
	}
}

// ResumeSend resumes a send for a dataset given the resume token
func (c *Client) ResumeSend(ctx context.Context, dataset string, resumeToken string) error {
	pipeRdr, pipeWrtr := io.Pipe()

	go func() {
		logger := logrus.WithFields(logrus.Fields{
			"server":      c.server,
			"dataset":     dataset,
			"resumeToken": resumeToken,
		})
		err := zfs.ResumeSend(pipeWrtr, resumeToken)
		if err != nil {
			logger.WithError(err).Error("zfs.http.Client.ResumeSend: Error sending resume stream")
		}
		err = pipeWrtr.Close()
		if err != nil {
			logger.WithError(err).Error("zfs.http.Client.sendWithBase: Error closing snapshot pipe")
		}
	}()

	req, err := c.request(ctx, http.MethodPut, fmt.Sprintf("filesystems/%s/snapshots?%s=%s",
		dataset, GETParamResumable, "true",
	), pipeRdr)
	if err != nil {
		_ = pipeWrtr.Close()
		return fmt.Errorf("error creating resume request: %w", err)
	}

	return c.doSendStream(req, pipeWrtr)
}

// SnapshotSend is a struct for a send job to a remote server using a Client
type SnapshotSend struct {
	zfs.SendOptions

	// Which dataset to send to
	DatasetName string
	// Which snapshot to send to (optional)
	SnapshotName string
	// The snapshot to send
	Snapshot *zfs.Dataset

	Properties ReceiveProperties
}

// Send sends the snapshot job to the remote server
func (c *Client) Send(ctx context.Context, send SnapshotSend) error {
	pipeRdr, pipeWrtr := io.Pipe()

	go func() {
		logger := logrus.WithFields(logrus.Fields{
			"server":       c.client,
			"snapshot":     send.Snapshot.Name,
			"baseSnapshot": send.IncrementalBase,
		})
		err := send.Snapshot.SendSnapshot(pipeWrtr, send.SendOptions)
		if err != nil {
			logger.Error("zfs.http.Client.sendWithBase: Error sending incremental snapshot stream")
		}
		err = pipeWrtr.Close()
		if err != nil {
			logger.WithError(err).Error("zfs.http.Client.sendWithBase: Error closing snapshot pipe")
		}
	}()

	url := fmt.Sprintf("filesystems/%s/snapshots", send.DatasetName)
	if send.SnapshotName != "" {
		url = fmt.Sprintf("filesystems/%s/snapshots/%s", send.DatasetName, send.SnapshotName)
	}

	req, err := c.request(ctx, http.MethodPut, url, pipeRdr)
	if err != nil {
		_ = pipeWrtr.Close()
		return fmt.Errorf("error creating incremental send request: %w", err)
	}
	q := req.URL.Query()
	q.Set(GETParamResumable, "true")
	if send.Properties != nil {
		q.Set(GETParamReceiveProperties, send.Properties.Encode())
	}
	req.URL.RawQuery = q.Encode() // Add new GET params
	return c.doSendStream(req, pipeWrtr)
}

func (c *Client) doSendStream(req *http.Request, pipeWrtr *io.PipeWriter) error {
	resp, err := c.client.Do(req)
	if err != nil {
		_ = pipeWrtr.Close()
		return fmt.Errorf("error sending stream: %w", err)
	}
	defer resp.Body.Close()

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
		return fmt.Errorf("unexpected status %d sending stream: %w", resp.StatusCode, err)
	}
}