package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sporevm/k8s/internal/agent"
	"github.com/sporevm/k8s/internal/agenthttp"
	"github.com/sporevm/k8s/internal/fleet"
)

const (
	defaultRunPath       = "/etc/sporevm/run/run.json"
	defaultBundleRunPath = "/etc/sporevm/run/bundle-run.json"
)

type agentURLsFlag []string

func (f *agentURLsFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *agentURLsFlag) Set(value string) error {
	for _, raw := range strings.Split(value, ",") {
		trimmed := strings.TrimSpace(raw)
		if trimmed != "" {
			*f = append(*f, trimmed)
		}
	}
	return nil
}

type coordinatorConfig struct {
	RunPath         string
	BundleRunPath   string
	Listen          string
	ResultStoreRoot string
	Timeout         time.Duration
	AgentURLs       agentURLsFlag
	HTTPClient      *http.Client
}

type agentEndpoint struct {
	URL    string
	Client agenthttp.Client
	Status fleet.AgentStatus
}

func main() {
	cfg := coordinatorConfig{}

	flag.StringVar(&cfg.RunPath, "run", envString("SPORE_RUN_PATH", ""), "fleet run JSON path")
	flag.StringVar(&cfg.BundleRunPath, "bundle-run", envString("SPORE_BUNDLE_RUN_PATH", ""), "prebuilt bundle run JSON path")
	flag.StringVar(&cfg.Listen, "listen", envString("SPORE_COORDINATOR_LISTEN", ""), "HTTP listen address for the resident coordinator API")
	flag.Var(&cfg.AgentURLs, "agent-url", "agent base URL; may be repeated or comma-separated")
	flag.StringVar(&cfg.ResultStoreRoot, "result-store-root", envString("SPORE_COORDINATOR_RESULT_STORE_ROOT", "/var/lib/sporevm/coordinator-results"), "local root for coordinator terminal prechecks")
	flag.DurationVar(&cfg.Timeout, "timeout", envDuration("SPORE_COORDINATOR_TIMEOUT", 30*time.Minute), "coordinator run timeout")
	flag.Parse()

	if envURLs := os.Getenv("SPORE_AGENT_URLS"); envURLs != "" {
		_ = cfg.AgentURLs.Set(envURLs)
	}
	if cfg.Listen != "" {
		if err := serveCoordinator(context.Background(), cfg); err != nil {
			log.Fatal(err)
		}
		return
	}
	if cfg.RunPath == "" && cfg.BundleRunPath == "" {
		cfg.RunPath = defaultRunPath
	}
	if err := runCoordinator(context.Background(), cfg, os.Stdout); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			log.Fatalf("coordinator timed out after %s: %v", cfg.Timeout, err)
		}
		_, _ = fmt.Fprintf(os.Stderr, "coordinator failed: %v\n", err)
		os.Exit(1)
	}
}

func serveCoordinator(ctx context.Context, cfg coordinatorConfig) error {
	if cfg.Listen == "" {
		return errors.New("listen address is required")
	}
	handler, err := coordinatorHandler(cfg)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              cfg.Listen,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	log.Printf("spore-coordinator API listening on %s", cfg.Listen)
	return server.ListenAndServe()
}

func coordinatorHandler(cfg coordinatorConfig) (http.Handler, error) {
	if len(cfg.AgentURLs) == 0 {
		return nil, errors.New("at least one --agent-url or SPORE_AGENT_URLS entry is required")
	}
	if cfg.ResultStoreRoot == "" {
		return nil, errors.New("result store root is required")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("timeout must be positive")
	}
	api := coordinatorAPI{cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", api.handleHealthz)
	mux.HandleFunc("GET /readyz", api.handleReadyz)
	mux.HandleFunc("POST /runs", api.handleRun)
	mux.HandleFunc("POST /sandboxes", api.handleCreateSandbox)
	mux.HandleFunc("POST /sandboxes/{name}/exec", api.handleExecSandbox)
	mux.HandleFunc("DELETE /sandboxes/{name}", api.handleDeleteSandbox)
	return mux, nil
}

type coordinatorAPI struct {
	cfg coordinatorConfig
}

func (a coordinatorAPI) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok\n"))
}

func (a coordinatorAPI) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if _, err := discoverAgentEndpoints(ctx, a.cfg.AgentURLs, a.cfg.HTTPClient); err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, err)
		return
	}
	w.Header().Set("content-type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ready\n"))
}

func (a coordinatorAPI) handleRun(w http.ResponseWriter, r *http.Request) {
	var source fleet.Run
	if !decodeRequestJSON(w, r, &source) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.Timeout)
	defer cancel()

	store, err := agent.NewLocalResultStore(a.cfg.ResultStoreRoot)
	if err != nil {
		writeHTTPError(w, http.StatusInternalServerError, fmt.Errorf("create coordinator result store: %w", err))
		return
	}
	endpoints, err := discoverAgentEndpoints(ctx, a.cfg.AgentURLs, a.cfg.HTTPClient)
	if err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, err)
		return
	}
	report, runErr := runSource(ctx, source, store, endpoints)
	if !hasRuntimeReport(report) {
		if runErr != nil {
			writeHTTPError(w, http.StatusBadRequest, runErr)
			return
		}
		writeHTTPError(w, http.StatusInternalServerError, errors.New("coordinator produced no runtime report"))
		return
	}
	writeResponseJSON(w, http.StatusOK, report)
}

func (a coordinatorAPI) handleCreateSandbox(w http.ResponseWriter, r *http.Request) {
	var create agent.CreateVMRequest
	if !decodeRequestJSON(w, r, &create) {
		return
	}
	endpoint, ok := a.sandboxEndpoint(w, r, create.Name)
	if !ok {
		return
	}
	if err := endpoint.Client.CreateSandbox(r.Context(), create); err != nil {
		writeHTTPError(w, http.StatusBadRequest, err)
		return
	}
	writeResponseJSON(w, http.StatusOK, map[string]string{"name": create.Name})
}

func (a coordinatorAPI) handleExecSandbox(w http.ResponseWriter, r *http.Request) {
	var execReq agent.ExecRequest
	if !decodeRequestJSON(w, r, &execReq) {
		return
	}
	endpoint, ok := a.sandboxEndpoint(w, r, r.PathValue("name"))
	if !ok {
		return
	}
	events, err := endpoint.Client.ExecSandbox(r.Context(), r.PathValue("name"), execReq.Command)
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, err)
		return
	}
	writeResponseJSON(w, http.StatusOK, events)
}

func (a coordinatorAPI) handleDeleteSandbox(w http.ResponseWriter, r *http.Request) {
	endpoint, ok := a.sandboxEndpoint(w, r, r.PathValue("name"))
	if !ok {
		return
	}
	if err := endpoint.Client.RemoveSandbox(r.Context(), r.PathValue("name")); err != nil {
		writeHTTPError(w, http.StatusBadRequest, err)
		return
	}
	writeResponseJSON(w, http.StatusOK, map[string]string{"name": r.PathValue("name")})
}

func (a coordinatorAPI) sandboxEndpoint(w http.ResponseWriter, r *http.Request, name string) (agentEndpoint, bool) {
	if name == "" {
		writeHTTPError(w, http.StatusBadRequest, errors.New("sandbox name is required"))
		return agentEndpoint{}, false
	}
	endpoints, err := discoverAgentEndpoints(r.Context(), a.cfg.AgentURLs, a.cfg.HTTPClient)
	if err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, err)
		return agentEndpoint{}, false
	}
	endpoint, err := selectSandboxEndpoint(endpoints)
	if err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, err)
		return agentEndpoint{}, false
	}
	return endpoint, true
}

func selectSandboxEndpoint(endpoints []agentEndpoint) (agentEndpoint, error) {
	var selected agentEndpoint
	compatible := 0
	for _, endpoint := range endpoints {
		if endpoint.Status.Healthy && !endpoint.Status.Pressure.Critical() {
			selected = endpoint
			compatible++
		}
	}
	if compatible == 0 {
		return agentEndpoint{}, fleet.ErrNoCompatibleAgents
	}
	if compatible > 1 {
		return agentEndpoint{}, errors.New("sandbox API requires exactly one compatible agent")
	}
	return selected, nil
}

func runCoordinator(ctx context.Context, cfg coordinatorConfig, stdout io.Writer) error {
	if len(cfg.AgentURLs) == 0 {
		return errors.New("at least one --agent-url or SPORE_AGENT_URLS entry is required")
	}
	if cfg.RunPath != "" && cfg.BundleRunPath != "" {
		return errors.New("only one of --run or --bundle-run may be set")
	}
	if cfg.RunPath == "" && cfg.BundleRunPath == "" {
		cfg.RunPath = defaultRunPath
	}
	if cfg.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}

	runCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	var run fleet.BundleRun
	var sourceRun fleet.Run
	var err error
	if cfg.RunPath != "" {
		sourceRun, err = readRun(cfg.RunPath)
	} else {
		run, err = readBundleRun(cfg.BundleRunPath)
	}
	if err != nil {
		return err
	}

	store, err := agent.NewLocalResultStore(cfg.ResultStoreRoot)
	if err != nil {
		return fmt.Errorf("create coordinator result store: %w", err)
	}
	endpoints, err := discoverAgentEndpoints(runCtx, cfg.AgentURLs, cfg.HTTPClient)
	if err != nil {
		return err
	}

	var report fleet.RuntimeReport
	var runErr error
	if cfg.RunPath != "" {
		report, runErr = runSource(runCtx, sourceRun, store, endpoints)
	} else {
		report, runErr = runBundle(runCtx, run, store, endpoints)
	}
	if !hasRuntimeReport(report) {
		if runErr != nil {
			return runErr
		}
		return errors.New("coordinator produced no runtime report")
	}

	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	if runErr != nil {
		return runErr
	}
	return runtimeReportError(report)
}

func hasRuntimeReport(report fleet.RuntimeReport) bool {
	return report.Plan.Summary.RunID != "" || report.Summary.RunID != ""
}

func runtimeReportError(report fleet.RuntimeReport) error {
	if report.Summary.RunID == "" || report.Summary.State == "" || report.Summary.State == "succeeded" {
		return nil
	}
	return fmt.Errorf(
		"runtime report failed: run_id=%s state=%s failed_children=%d platform_mismatches=%d lease_errors=%d",
		report.Summary.RunID,
		report.Summary.State,
		report.Summary.FailedChildren,
		report.Summary.PlatformMismatches,
		report.Summary.LeaseErrors,
	)
}

func decodeRequestJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		writeHTTPError(w, http.StatusBadRequest, err)
		return false
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err == nil {
		writeHTTPError(w, http.StatusBadRequest, errors.New("request must contain exactly one JSON document"))
		return false
	} else if !errors.Is(err, io.EOF) {
		writeHTTPError(w, http.StatusBadRequest, err)
		return false
	}
	return true
}

func writeResponseJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}

func writeHTTPError(w http.ResponseWriter, status int, err error) {
	if status < 400 {
		status = http.StatusInternalServerError
	}
	writeResponseJSON(w, status, map[string]string{"error": err.Error()})
}

func readBundleRun(path string) (fleet.BundleRun, error) {
	file, err := os.Open(path)
	if err != nil {
		return fleet.BundleRun{}, fmt.Errorf("open bundle run: %w", err)
	}
	defer file.Close()
	run, err := fleet.DecodeBundleRun(file)
	if err != nil {
		return fleet.BundleRun{}, err
	}
	return run, nil
}

func readRun(path string) (fleet.Run, error) {
	file, err := os.Open(path)
	if err != nil {
		return fleet.Run{}, fmt.Errorf("open run: %w", err)
	}
	defer file.Close()
	run, err := fleet.DecodeRun(file)
	if err != nil {
		return fleet.Run{}, err
	}
	return run, nil
}

func discoverAgentEndpoints(ctx context.Context, urls []string, httpClient *http.Client) ([]agentEndpoint, error) {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	endpoints := make([]agentEndpoint, 0, len(urls))
	for _, rawURL := range urls {
		client := agenthttp.Client{BaseURL: rawURL, HTTPClient: httpClient}
		statusCtx, cancelStatus := context.WithTimeout(ctx, 10*time.Second)
		status, err := client.Status(statusCtx)
		cancelStatus()
		if err != nil {
			return nil, fmt.Errorf("fetch status from %s: %w", rawURL, err)
		}
		endpoints = append(endpoints, agentEndpoint{
			URL:    rawURL,
			Client: client,
			Status: status,
		})
	}
	return endpoints, nil
}

func runBundle(ctx context.Context, run fleet.BundleRun, store fleet.TerminalResultReader, endpoints []agentEndpoint) (fleet.RuntimeReport, error) {
	return runBundleWithOptions(ctx, run, store, endpoints, fleet.CoordinatorOptions{})
}

func runBundleWithOptions(ctx context.Context, run fleet.BundleRun, store fleet.TerminalResultReader, endpoints []agentEndpoint, opts fleet.CoordinatorOptions) (fleet.RuntimeReport, error) {
	return runBundleWithExecutorOverrides(ctx, run, store, endpoints, opts, nil)
}

func runBundleWithExecutorOverrides(ctx context.Context, run fleet.BundleRun, store fleet.TerminalResultReader, endpoints []agentEndpoint, opts fleet.CoordinatorOptions, overrides map[string]fleet.ShardExecutor) (fleet.RuntimeReport, error) {
	if len(endpoints) == 0 {
		return fleet.RuntimeReport{}, fleet.ErrCoordinatorNotConfigured
	}
	return runBundleWithInspectorAndExecutorOverrides(ctx, run, store, endpoints, opts, endpoints[0].Client, overrides)
}

func runBundleWithInspectorAndExecutorOverrides(ctx context.Context, run fleet.BundleRun, store fleet.TerminalResultReader, endpoints []agentEndpoint, opts fleet.CoordinatorOptions, inspector fleet.BundleInspector, overrides map[string]fleet.ShardExecutor) (fleet.RuntimeReport, error) {
	if len(endpoints) == 0 {
		return fleet.RuntimeReport{}, fleet.ErrCoordinatorNotConfigured
	}
	if inspector == nil {
		return fleet.RuntimeReport{}, fleet.ErrCoordinatorNotConfigured
	}
	agents := make([]fleet.AgentStatus, 0, len(endpoints))
	executors := make(map[string]fleet.ShardExecutor, len(endpoints))
	for _, endpoint := range endpoints {
		agents = append(agents, endpoint.Status)
		if override, ok := overrides[endpoint.Status.AgentID]; ok {
			executors[endpoint.Status.AgentID] = override
		} else {
			executors[endpoint.Status.AgentID] = endpoint.Client
		}
	}
	coordinator, err := fleet.NewCoordinator(
		agents,
		inspector,
		store,
		executors,
		opts,
	)
	if err != nil {
		return fleet.RuntimeReport{}, fmt.Errorf("create coordinator: %w", err)
	}
	return coordinator.Run(ctx, run)
}

func runSource(ctx context.Context, source fleet.Run, store fleet.TerminalResultReader, endpoints []agentEndpoint) (fleet.RuntimeReport, error) {
	endpoint, err := selectPrepareEndpoint(source, endpoints)
	if err != nil {
		return fleet.RuntimeReport{}, err
	}
	localPrepare := source.RetryPolicy.MaxAttemptsPerChild == 1
	prepareMode := "local"
	if !localPrepare {
		prepareMode = "bundle"
	}
	log.Printf("run prepare selected run_id=%s agent_id=%s url=%s mode=%s", source.RunID, endpoint.Status.AgentID, endpoint.URL, prepareMode)

	var prepared fleet.PreparedBundle
	if localPrepare {
		prepared, err = endpoint.Client.PrepareLocal(ctx, source)
	} else {
		prepared, err = endpoint.Client.PrepareBundle(ctx, source)
	}
	if err != nil {
		return fleet.RuntimeReport{}, err
	}
	run, err := source.Compile(prepared)
	if err != nil {
		return fleet.RuntimeReport{}, err
	}
	endpoint.Status.HostClass = prepared.HostClass
	executors := map[string]fleet.ShardExecutor{}
	if prepared.Local != nil && prepared.Local.ChildrenDir != "" {
		executors[endpoint.Status.AgentID] = preparedLocalShardExecutor{
			Client: endpoint.Client,
			Local:  *prepared.Local,
		}
	}
	inspector := fleet.BundleInspector(endpoint.Client)
	if localPrepare {
		inspector = preparedLocalInspector{Prepared: prepared}
	}
	report, err := runBundleWithInspectorAndExecutorOverrides(ctx, run, store, []agentEndpoint{endpoint}, fleet.CoordinatorOptions{
		PlanBuilder: fleet.BuildSingleAgentSequentialPlan,
	}, inspector, executors)
	if report.Summary.RunID != "" {
		report.Prepare = &fleet.RuntimePrepareSummary{
			AgentID:      endpoint.Status.AgentID,
			BundleDigest: prepared.Bundle.Digest,
			ChildCount:   prepared.ChildCount,
			TimingsMS:    prepared.TimingsMS,
		}
	}
	return report, err
}

type preparedLocalInspector struct {
	Prepared fleet.PreparedBundle
}

func (i preparedLocalInspector) InspectRunBundle(_ context.Context, run fleet.BundleRun) (fleet.BundleInspection, error) {
	return fleet.BundleInspection{
		BundleDigest: i.Prepared.Bundle.Digest,
		ChildCount:   i.Prepared.ChildCount,
		HostClass:    i.Prepared.HostClass,
	}, nil
}

type preparedLocalShardExecutor struct {
	Client agenthttp.Client
	Local  fleet.PreparedLocalBundle
}

func (e preparedLocalShardExecutor) RunShard(ctx context.Context, req fleet.ShardExecutionRequest) ([]fleet.AttemptResult, error) {
	req.Local = &e.Local
	return e.Client.RunShard(ctx, req)
}

func selectPrepareEndpoint(source fleet.Run, endpoints []agentEndpoint) (agentEndpoint, error) {
	if err := source.Validate(); err != nil {
		return agentEndpoint{}, err
	}
	requiredCapacity := fleet.RequiredInFlightSlots(source.Children.Count, source.Execution)
	bestCapacity := 0
	healthy := false
	for _, endpoint := range endpoints {
		status := endpoint.Status
		if !status.Healthy || status.Pressure.Critical() {
			continue
		}
		healthy = true
		capacity := min(status.ExecutionSlots.Available, source.Execution.MaxInFlightPerAgent)
		if capacity > bestCapacity {
			bestCapacity = capacity
		}
		if capacity >= requiredCapacity {
			return endpoint, nil
		}
	}
	if !healthy {
		return agentEndpoint{}, fleet.ErrNoCompatibleAgents
	}
	return agentEndpoint{}, fmt.Errorf("%w: run needs %d in-flight slots on one preparing agent while bundle URI is local, have %d", fleet.ErrInsufficientCapacity, requiredCapacity, bestCapacity)
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
