package demo

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"tenantpulse/internal/stress"
)

// StressJob runs the direct-vs-aggregation stress test from the dashboard
// button (DEMO_MODE only). One run at a time; settings are capped so a click
// can never overload the machine for long.
type StressJob struct {
	AppDSN    string // store workload role
	DirectURL string // api-direct (queries the primary DB)
	APIURL    string // aggregation-layer api
	IngestDSN string // ClickHouse, to publish the result for the panels

	mu       sync.Mutex
	running  bool
	progress *stress.Progress
	started  time.Time
	finished time.Time
	lastErr  string
	runID    string
	scenario string
}

type stressReq struct {
	Scenario  string `json:"scenario"` // reads (default) | writes
	VUs       int    `json:"vus"`
	DurationS int    `json:"duration_s"`
	Rate      int    `json:"rate"`    // writes: store transactions per second
	Viewers   int    `json:"viewers"` // writes: dashboard viewers
}

func (s *Service) authAny(c *gin.Context) {
	if _, err := s.Signer.Verify(c.GetHeader("Authorization")); err != nil {
		c.AbortWithStatusJSON(401, gin.H{"error": "unauthenticated"})
	}
}

func (s *Service) startStress(c *gin.Context) {
	var req stressReq
	_ = c.ShouldBindJSON(&req)
	req.VUs = min(max(req.VUs, 1), 50)
	if req.VUs == 1 && req.DurationS == 0 {
		req.VUs = 20
	}
	req.DurationS = min(max(req.DurationS, 10), 60)
	j := s.Stress
	j.mu.Lock()
	if j.running {
		j.mu.Unlock()
		c.JSON(409, gin.H{"error": "already_running", "detail": "a stress test is already running; its progress is shown"})
		return
	}
	j.running, j.progress, j.lastErr, j.runID, j.started, j.scenario = true, nil, "", "", time.Now(), "reads"
	j.mu.Unlock()

	if req.Scenario == "writes" {
		s.startWrites(c, j, req)
		return
	}

	cfg := stress.Config{AppDSN: j.AppDSN, Tokens: map[string]string{}, Operator: s.OperatorToken,
		DirectURL: j.DirectURL, APIURL: j.APIURL, VUs: req.VUs, Duration: time.Duration(req.DurationS) * time.Second,
		Warmup: 3 * time.Second, OLTPRate: 150, OLTPWorkers: 8, Source: "dashboard"}
	for _, t := range s.Reg.Current().Ordered {
		if t.Status == "active" {
			cfg.Tenants = append(cfg.Tenants, stress.Tenant{Slug: t.Slug, Database: t.SourceDatabase, Currency: t.DefaultCurrency})
			cfg.Tokens[t.Slug] = s.Signer.Issue("tenant:" + t.Slug)
		}
	}
	go j.run(cfg)
	c.JSON(202, gin.H{"status": "started", "vus": req.VUs, "duration_s": req.DurationS,
		"estimated_s": 4*req.DurationS + 3*3 + 5, "tenants": len(cfg.Tenants)})
}

func (j *StressJob) run(cfg stress.Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	res, err := stress.Run(ctx, cfg, func(p stress.Progress) {
		j.mu.Lock()
		j.progress = &p
		j.mu.Unlock()
	})
	runID := ""
	if err == nil {
		b, _ := json.Marshal(res)
		runID, err = stress.Publish(ctx, j.IngestDSN, "stress", res.At, b)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.running, j.finished, j.runID = false, time.Now(), runID
	if err != nil {
		j.lastErr = err.Error()
		slog.Error("stress test failed", "err", err)
	} else {
		slog.Info("stress test finished", "run", runID)
	}
}

// startWrites launches the write storm (every store writing hard while
// dashboard viewers watch). Capped so a click cannot overload the machine
// for long; the CLI adds a pipeline-off phase, which needs control-plane writes.
func (s *Service) startWrites(c *gin.Context, j *StressJob, req stressReq) {
	if req.Rate == 0 {
		req.Rate = 1000
	}
	if req.Viewers == 0 {
		req.Viewers = 100
	}
	req.Rate, req.Viewers = min(max(req.Rate, 50), 3000), min(max(req.Viewers, 1), 300)
	j.mu.Lock()
	j.scenario = "writes"
	j.mu.Unlock()
	cfg := stress.WriteConfig{AppDSN: j.AppDSN, Tokens: map[string]string{}, Operator: s.OperatorToken, APIURL: j.APIURL,
		DirectURL: j.DirectURL, Rate: req.Rate, Workers: 48, Viewers: req.Viewers,
		Duration: time.Duration(req.DurationS) * time.Second, Warmup: 5 * time.Second, Source: "dashboard"}
	for _, t := range s.Reg.Current().Ordered {
		if t.Status == "active" {
			cfg.Tenants = append(cfg.Tenants, stress.Tenant{Slug: t.Slug, Database: t.SourceDatabase, Currency: t.DefaultCurrency})
			cfg.Tokens[t.Slug] = s.Signer.Issue("tenant:" + t.Slug)
		}
	}
	go j.runWrites(cfg)
	c.JSON(202, gin.H{"status": "started", "scenario": "writes", "rate": req.Rate, "viewers": req.Viewers, "duration_s": req.DurationS,
		"estimated_s": 3*(req.DurationS+5) + 5, "tenants": len(cfg.Tenants)})
}

func (j *StressJob) runWrites(cfg stress.WriteConfig) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	res, err := stress.RunWrites(ctx, cfg, func(p stress.Progress) {
		j.mu.Lock()
		j.progress = &p
		j.mu.Unlock()
	})
	runID := ""
	if err == nil {
		b, _ := json.Marshal(res)
		runID, err = stress.Publish(ctx, j.IngestDSN, "writestorm", res.At, b)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.running, j.finished, j.runID = false, time.Now(), runID
	if err != nil {
		j.lastErr = err.Error()
		slog.Error("write storm failed", "err", err)
	} else {
		slog.Info("write storm finished", "run", runID)
	}
}

func (s *Service) stressStatus(c *gin.Context) {
	j := s.Stress
	j.mu.Lock()
	defer j.mu.Unlock()
	out := gin.H{"running": j.running, "progress": j.progress, "error": j.lastErr, "run_id": j.runID, "scenario": j.scenario}
	if !j.started.IsZero() {
		out["started_at"] = j.started.UTC()
	}
	if !j.finished.IsZero() && !j.running {
		out["finished_at"] = j.finished.UTC()
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(200, out)
}
