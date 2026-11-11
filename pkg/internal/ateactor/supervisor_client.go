package ateactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

// StepState describes the execution lifecycle state of a step inside Step Supervisor.
type StepState string

const (
	StepStateRunning   StepState = "RUNNING"
	StepStateCompleted StepState = "COMPLETED"
	StepStateFailed    StepState = "FAILED"
)

// IsTerminal reports whether st is COMPLETED or FAILED.
func (st StepState) IsTerminal() bool {
	return st == StepStateCompleted || st == StepStateFailed
}

// EnvEntry represents a single environment variable assignment on a step.
type EnvEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// RunStepRequest is the JSON request body for POST /v1/steps on Step Supervisor.
type RunStepRequest struct {
	StepID      string     `json:"stepId,omitempty"`
	Name        string     `json:"name,omitempty"`
	TargetAddr  string     `json:"targetAddr,omitempty"`
	Command     []string   `json:"command,omitempty"`
	Args        []string   `json:"args,omitempty"`
	Script      string     `json:"script,omitempty"`
	WorkingDir  string     `json:"workingDir,omitempty"`
	Env         []EnvEntry `json:"env,omitempty"`
	ResultNames []string   `json:"resultNames,omitempty"`
}

// StepStatus describes the current or terminal state of a step returned by Step Supervisor.
type StepStatus struct {
	StepID     string            `json:"stepId"`
	Name       string            `json:"name,omitempty"`
	State      StepState         `json:"state"`
	ExitCode   *int              `json:"exitCode,omitempty"`
	StartedAt  time.Time         `json:"startedAt"`
	FinishedAt *time.Time        `json:"finishedAt,omitempty"`
	Results    map[string]string `json:"results,omitempty"`
	LogSize    int64             `json:"logSize"`
	Error      string            `json:"error,omitempty"`
}

// UploadFilesRequest is the JSON request body for POST /v1/files on Step Supervisor.
type UploadFilesRequest struct {
	SubDir string            `json:"subDir,omitempty"`
	Files  map[string][]byte `json:"files"`
	Mode   uint32            `json:"mode,omitempty"`
}

// UploadFilesResponse is the JSON response from POST /v1/files.
type UploadFilesResponse struct {
	WrittenFiles []string `json:"writtenFiles"`
}

// RescuePatchRequest is the JSON request body for POST /v1/rescue/patch.
type RescuePatchRequest struct {
	Files       map[string]string `json:"files,omitempty"`
	Diff        string            `json:"diff,omitempty"`
	DeleteFiles []string          `json:"deleteFiles,omitempty"`
}

// RescuePatchResponse is the JSON response from POST /v1/rescue/patch.
type RescuePatchResponse struct {
	PatchedFiles []string `json:"patchedFiles"`
}

type readyResponse struct {
	OK bool `json:"ok"`
}

type supervisorErrorResponse struct {
	Error string `json:"error"`
}

// SupervisorClient talks HTTP/1.1 to a Step Supervisor coordinator (:8080).
type SupervisorClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewSupervisorClient constructs a SupervisorClient targeting baseURL over httpClient.
func NewSupervisorClient(baseURL string, httpClient *http.Client) *SupervisorClient {
	trimmed := strings.TrimSpace(baseURL)
	if !strings.HasPrefix(trimmed, "http://") && !strings.HasPrefix(trimmed, "https://") {
		trimmed = "http://" + trimmed
	}
	trimmed = strings.TrimRight(trimmed, "/")
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &SupervisorClient{
		baseURL:    trimmed,
		httpClient: httpClient,
	}
}

// Ready checks GET /readyz on the target Step Supervisor.
func (c *SupervisorClient) Ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/readyz", nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return readSupervisorHTTPError("readyz", resp)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read readyz response: %w", err)
	}
	var out readyResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Errorf("decode readyz response: %w", err)
	}
	if !out.OK {
		return fmt.Errorf("supervisor reported ok=false")
	}
	return nil
}

// RunStep starts a step asynchronously via POST /v1/steps.
func (c *SupervisorClient) RunStep(ctx context.Context, stepReq RunStepRequest) (*StepStatus, error) {
	var out StepStatus
	if err := c.postJSON(ctx, "/v1/steps", stepReq, &out, http.StatusAccepted, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetStep retrieves the current non-blocking status of stepID via GET /v1/steps/{id}.
func (c *SupervisorClient) GetStep(ctx context.Context, stepID string) (*StepStatus, error) {
	return c.getStepWithWait(ctx, stepID, false)
}

// IsStepNotFound reports whether err came from a 404 Not Found on GetStep.
func IsStepNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "(HTTP 404)")
}

// IsStepConflict reports whether err came from a 409 Conflict on RunStep.
func IsStepConflict(err error) bool {
	return err != nil && strings.Contains(err.Error(), "(HTTP 409)")
}

// WaitStep waits up to maxWait for stepID to reach a terminal state. If maxWait
// elapses while the step is still RUNNING, WaitStep falls back to a non-blocking
// GetStep and returns the latest RUNNING StepStatus without error so reconcilers
// can requeue cleanly without blocking worker threads.
func (c *SupervisorClient) WaitStep(ctx context.Context, stepID string, maxWait time.Duration) (*StepStatus, error) {
	waitCtx := ctx
	var cancel context.CancelFunc
	if maxWait > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, maxWait)
		defer cancel()
	}
	st, err := c.getStepWithWait(waitCtx, stepID, true)
	if err == nil {
		return st, nil
	}
	if maxWait > 0 && ctx.Err() == nil && waitCtx.Err() != nil {
		return c.GetStep(ctx, stepID)
	}
	return nil, err
}

func (c *SupervisorClient) getStepWithWait(ctx context.Context, stepID string, wait bool) (*StepStatus, error) {
	u := c.baseURL + "/v1/steps/" + url.PathEscape(stepID)
	if wait {
		u += "?wait=true"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, readSupervisorHTTPError("get step", resp)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read step status: %w", err)
	}
	var out StepStatus
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode step status: %w", err)
	}
	return &out, nil
}

// ReadLogs reads available step log bytes starting at offset without blocking.
func (c *SupervisorClient) ReadLogs(ctx context.Context, stepID string, offset int64, dst io.Writer) (int64, error) {
	q := url.Values{}
	q.Set("offset", strconv.FormatInt(offset, 10))
	q.Set("follow", "false")
	u := c.baseURL + "/v1/steps/" + url.PathEscape(stepID) + "/logs?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return offset, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return offset, err
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return offset, readSupervisorHTTPError("read logs", resp)
	}
	var written int64
	if dst != nil {
		written, err = io.Copy(dst, resp.Body)
	} else {
		written, err = io.Copy(io.Discard, resp.Body)
	}
	if err != nil {
		return offset + written, err
	}
	if hdr := resp.Header.Get("X-Log-Offset"); hdr != "" {
		if parsed, parseErr := strconv.ParseInt(hdr, 10, 64); parseErr == nil {
			return parsed, nil
		}
	}
	return offset + written, nil
}

// UploadFiles seeds ConfigMap or Secret files into --workspace-dir via POST /v1/files.
func (c *SupervisorClient) UploadFiles(ctx context.Context, req UploadFilesRequest) (*UploadFilesResponse, error) {
	var out UploadFilesResponse
	if err := c.postJSON(ctx, "/v1/files", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// RescuePatch applies file additions, unified diffs, or deletions inside --workspace-dir.
func (c *SupervisorClient) RescuePatch(ctx context.Context, req RescuePatchRequest) (*RescuePatchResponse, error) {
	var out RescuePatchResponse
	if err := c.postJSON(ctx, "/v1/rescue/patch", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// EnsureWorkspaceDirs ensures each relative subdirectory under /workspace exists
// without requiring shell utilities inside distroless/scratch containers and
// without leaving marker files behind that would block `git clone`.
func (c *SupervisorClient) EnsureWorkspaceDirs(ctx context.Context, relDirs ...string) error {
	const sentinel = ".tekton-ws-dir-init"
	files := make(map[string][]byte)
	var deletes []string
	for _, raw := range relDirs {
		cleaned := strings.Trim(path.Clean(strings.TrimSpace(raw)), "/")
		if cleaned == "" || cleaned == "." || strings.HasPrefix(cleaned, "..") {
			continue
		}
		p := cleaned + "/" + sentinel
		if _, dup := files[p]; dup {
			continue
		}
		files[p] = []byte{}
		deletes = append(deletes, p)
	}
	if len(files) == 0 {
		return nil
	}
	if _, err := c.UploadFiles(ctx, UploadFilesRequest{Files: files}); err != nil {
		return fmt.Errorf("create workspace subdirectories: %w", err)
	}
	if _, err := c.RescuePatch(ctx, RescuePatchRequest{DeleteFiles: deletes}); err != nil {
		return fmt.Errorf("clean workspace subdirectory sentinels: %w", err)
	}
	return nil
}

func (c *SupervisorClient) postJSON(ctx context.Context, path string, reqBody any, respBody any, expectedStatuses ...int) error {
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("encode request JSON: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer drainAndClose(resp.Body)

	matched := false
	for _, st := range expectedStatuses {
		if resp.StatusCode == st {
			matched = true
			break
		}
	}
	if !matched {
		return readSupervisorHTTPError(path, resp)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s response: %w", path, err)
	}
	if respBody != nil {
		if err := json.Unmarshal(raw, respBody); err != nil {
			return fmt.Errorf("decode %s response: %w", path, err)
		}
	}
	return nil
}

func readSupervisorHTTPError(op string, resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	var errResp supervisorErrorResponse
	if json.Unmarshal(raw, &errResp) == nil && errResp.Error != "" {
		return fmt.Errorf("%s failed (HTTP %d): %s", op, resp.StatusCode, errResp.Error)
	}
	return fmt.Errorf("%s failed (HTTP %d): %s", op, resp.StatusCode, strings.TrimSpace(string(raw)))
}

func drainAndClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, rc)
	_ = rc.Close()
}
