package stewardclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/superset-studio/majordomo-steward/internal/config"
)

// Job represents a pending job received from Butler.
type Job struct {
	ID      uuid.UUID `json:"id"`
	OrgID   uuid.UUID `json:"org_id"`
	JobType string    `json:"job_type"` // "replay" or "eval"
	RunID   uuid.UUID `json:"run_id"`
}

// JobResult is the payload sent back to Butler after executing a job.
type JobResult struct {
	Status  string  `json:"status"`  // "done" or "failed"
	Error   *string `json:"error,omitempty"`
}

// JobExecutor knows how to execute a replay or eval job given its run ID.
type JobExecutor interface {
	ExecuteReplay(ctx context.Context, orgID, runID uuid.UUID) error
	ExecuteEval(ctx context.Context, orgID, runID uuid.UUID) error
}

// JobPoller polls Butler for pending jobs, claims each one, and executes it.
type JobPoller struct {
	cfg      config.StewardConfig
	executor JobExecutor
	client   *http.Client
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewJobPoller creates a JobPoller. Call Start() to begin background polling.
func NewJobPoller(cfg config.StewardConfig, executor JobExecutor) *JobPoller {
	return &JobPoller{
		cfg:      cfg,
		executor: executor,
		client:   &http.Client{Timeout: 15 * time.Second},
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start launches the background job-polling goroutine. It must be called once.
func (jp *JobPoller) Start() {
	go jp.run()
}

// Stop signals the poller to exit.
func (jp *JobPoller) Stop() {
	close(jp.stopCh)
	<-jp.doneCh
}

func (jp *JobPoller) run() {
	defer close(jp.doneCh)

	// Spread concurrent org pollers across the interval to avoid thundering herd.
	jitter := time.Duration(rand.Int63n(int64(jp.cfg.JobPollInterval)))
	select {
	case <-time.After(jitter):
	case <-jp.stopCh:
		return
	}

	ticker := time.NewTicker(jp.cfg.JobPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			jp.poll()
		case <-jp.stopCh:
			return
		}
	}
}

func (jp *JobPoller) poll() {
	jobs, err := jp.fetchJobs()
	if err != nil {
		slog.Warn("job poll failed", "error", err)
		return
	}

	for _, job := range jobs {
		if err := jp.claim(job.ID); err != nil {
			slog.Warn("job claim failed", "job_id", job.ID, "error", err)
			continue
		}
		go jp.execute(job)
	}
}

func (jp *JobPoller) fetchJobs() ([]Job, error) {
	url := fmt.Sprintf("%s/api/v1/steward/jobs?limit=%d",
		jp.cfg.ButlerBaseURL, jp.cfg.JobPollLimit)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+jp.cfg.StewardToken)

	resp, err := jp.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get jobs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("butler returned %d", resp.StatusCode)
	}

	var jobs []Job
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		return nil, fmt.Errorf("decode jobs: %w", err)
	}

	return jobs, nil
}

func (jp *JobPoller) claim(jobID uuid.UUID) error {
	url := fmt.Sprintf("%s/api/v1/steward/jobs/%s/claim", jp.cfg.ButlerBaseURL, jobID)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("build claim request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+jp.cfg.StewardToken)

	resp, err := jp.client.Do(req)
	if err != nil {
		return fmt.Errorf("claim job: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("butler returned %d on claim", resp.StatusCode)
	}

	return nil
}

func (jp *JobPoller) execute(job Job) {
	ctx := context.Background()
	var execErr error

	switch job.JobType {
	case "replay":
		execErr = jp.executor.ExecuteReplay(ctx, job.OrgID, job.RunID)
	case "eval":
		execErr = jp.executor.ExecuteEval(ctx, job.OrgID, job.RunID)
	default:
		slog.Warn("unknown job type", "job_id", job.ID, "job_type", job.JobType)
		return
	}

	result := JobResult{Status: "done"}
	if execErr != nil {
		slog.Warn("job execution failed", "job_id", job.ID, "error", execErr)
		errStr := execErr.Error()
		result.Status = "failed"
		result.Error = &errStr
	}

	if err := jp.postResult(job.ID, result); err != nil {
		slog.Warn("job result post failed", "job_id", job.ID, "error", err)
	}
}

func (jp *JobPoller) postResult(jobID uuid.UUID, result JobResult) error {
	body, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/steward/jobs/%s/result", jp.cfg.ButlerBaseURL, jobID)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build result request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+jp.cfg.StewardToken)

	resp, err := jp.client.Do(req)
	if err != nil {
		return fmt.Errorf("post result: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("butler returned %d on result", resp.StatusCode)
	}

	return nil
}
