// Package agenthttp exposes the node-local SporeVM agent over a narrow HTTP API.
package agenthttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/sporevm/k8s/internal/agent"
	"github.com/sporevm/k8s/internal/fleet"
)

// Server serves fleet agent status, bundle inspection, and shard execution.
type Server struct {
	Runner                  *agent.Runner
	Client                  agent.SporeClient
	AgentID                 string
	CellID                  string
	Region                  string
	Backend                 string
	BundleCacheRoot         string
	RootFSCacheRoot         string
	Pressure                fleet.Pressure
	AllowMetadataOnlyRootFS bool
	Now                     func() time.Time
}

// Handler returns the HTTP handler for the agent API.
func (s *Server) Handler() (http.Handler, error) {
	if s.Runner == nil || s.Client == nil {
		return nil, errors.New("agent HTTP server requires runner and spore client")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("POST /inspect-run-bundle", s.handleInspectRunBundle)
	mux.HandleFunc("POST /prepare-bundle", s.handlePrepareBundle)
	mux.HandleFunc("POST /run-shard", s.handleRunShard)
	return mux, nil
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	info, err := s.Client.HostInfo(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if _, err := info.FleetHostClass(s.backend()); err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	w.Header().Set("content-type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ready\n"))
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	cache, err := statusForCacheRoots(s.BundleCacheRoot, s.RootFSCacheRoot)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	status, err := s.Runner.Status(r.Context(), agent.StatusRequest{
		AgentID:    s.AgentID,
		CellID:     s.CellID,
		Backend:    s.backend(),
		Cache:      cache,
		Pressure:   s.pressure(),
		ObservedAt: s.now(),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleInspectRunBundle(w http.ResponseWriter, r *http.Request) {
	var run fleet.Run
	if !decodeJSON(w, r, &run) {
		return
	}
	inspection, err := (agent.RunBundleInspector{Client: s.Client}).InspectRunBundle(r.Context(), run)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, inspection)
}

func (s *Server) handlePrepareBundle(w http.ResponseWriter, r *http.Request) {
	var run fleet.GenericRun
	if !decodeJSON(w, r, &run) {
		return
	}
	start := time.Now()
	log.Printf("prepare-bundle start run_id=%s children=%d agent_id=%s", run.RunID, run.Children.Count, s.AgentID)
	prepared, err := s.Runner.PrepareBundle(r.Context(), agent.PrepareBundleRequest{
		Run:     run,
		Backend: s.backend(),
	})
	if err != nil {
		log.Printf("prepare-bundle failed run_id=%s duration=%s: %v", run.RunID, time.Since(start), err)
		writeError(w, http.StatusBadRequest, err)
		return
	}
	log.Printf("prepare-bundle complete run_id=%s digest=%s children=%d duration=%s", run.RunID, prepared.Bundle.Digest, prepared.ChildCount, time.Since(start))
	writeJSON(w, http.StatusOK, prepared)
}

func (s *Server) handleRunShard(w http.ResponseWriter, r *http.Request) {
	var req fleet.ShardExecutionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Lease.AgentID != s.AgentID {
		writeError(w, http.StatusConflict, fmt.Errorf("lease agentID %q does not match this agent %q", req.Lease.AgentID, s.AgentID))
		return
	}
	start := time.Now()
	log.Printf("run-shard start run_id=%s shard_id=%s child_start=%d child_count=%d attempt=%d agent_id=%s", req.Run.RunID, req.Lease.ShardID, req.Lease.ChildStart, req.Lease.ChildCount, req.Attempt, s.AgentID)
	results, err := (agent.RunnerShardExecutor{
		Runner:                  s.Runner,
		Pressure:                s.pressure(),
		Region:                  s.Region,
		AllowMetadataOnlyRootFS: s.AllowMetadataOnlyRootFS,
		Backend:                 s.backend(),
	}).RunShard(r.Context(), req)
	resultCount := shardResultCount(results)
	if err != nil {
		log.Printf("run-shard failed run_id=%s shard_id=%s results=%d duration=%s: %v", req.Run.RunID, req.Lease.ShardID, resultCount, time.Since(start), err)
		if resultCount > 0 {
			writeJSON(w, http.StatusOK, results)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	log.Printf("run-shard complete run_id=%s shard_id=%s results=%d duration=%s", req.Run.RunID, req.Lease.ShardID, resultCount, time.Since(start))
	writeJSON(w, http.StatusOK, results)
}

func shardResultCount(results []fleet.AttemptResult) int {
	count := 0
	for _, result := range results {
		if result.AttemptID != "" {
			count++
		}
	}
	return count
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now().UTC()
}

func (s *Server) backend() string {
	if s.Backend == "" {
		return "kvm"
	}
	return s.Backend
}

func (s *Server) pressure() fleet.Pressure {
	if s.Pressure.Disk == "" && s.Pressure.Memory == "" {
		return fleet.Pressure{Disk: fleet.PressureNormal, Memory: fleet.PressureNormal}
	}
	return s.Pressure
}

func statusForCacheRoots(bundleRoot, rootFSRoot string) (fleet.CacheStatus, error) {
	bundleBytes, bundleEntries, err := dirStats(bundleRoot)
	if err != nil {
		return fleet.CacheStatus{}, fmt.Errorf("bundle cache status: %w", err)
	}
	rootFSBytes, rootFSEntries, err := dirStats(rootFSRoot)
	if err != nil {
		return fleet.CacheStatus{}, fmt.Errorf("rootfs cache status: %w", err)
	}
	return fleet.CacheStatus{
		BundleCacheBytes:   bundleBytes,
		RootFSCacheBytes:   rootFSBytes,
		BundleCacheEntries: bundleEntries,
		RootFSCacheEntries: rootFSEntries,
	}, nil
}

func dirStats(root string) (int64, int, error) {
	if root == "" {
		return 0, 0, nil
	}
	var bytes int64
	var entries int
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		bytes += info.Size()
		entries++
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	return bytes, entries, err
}

func decodeJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return false
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err == nil {
		writeError(w, http.StatusBadRequest, errors.New("request must contain exactly one JSON document"))
		return false
	} else if !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, err)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	if status < 400 {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
