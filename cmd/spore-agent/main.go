package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sporevm/k8s/internal/agent"
	"github.com/sporevm/k8s/internal/agenthttp"
)

const (
	defaultChildMemoryBytes   = int64(512 * 1024 * 1024)
	defaultMemoryReserveBytes = int64(256 * 1024 * 1024)
	defaultTemplateRetain     = 16
	defaultTemplateReconcile  = 15 * time.Minute
	templateReconcileTimeout  = 2 * time.Minute
	sporeRuntimeDirEnv        = "SPOREVM_RUNTIME_DIR"
	sporeRuntimeAuthorityFile = "local-ram-backing.key"
)

var cgroupMemoryLimitFiles = []string{
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

func main() {
	var listen string
	var agentID string
	var cellID string
	var slots int
	var childMemoryBytes int64
	var memoryReserveBytes int64
	var sporePath string
	var resultStoreRoot string
	var resultStoreBackend string
	var resultStoreRegion string
	var resultStoreEndpoint string
	var resultStorePathStyle bool
	var workRoot string
	var bundleCacheRoot string
	var rootFSCacheRoot string
	var region string
	var backend string
	var childTimeout time.Duration
	var allowMetadataOnlyRootFS bool
	var templateRetain int
	var templateReconcileInterval time.Duration
	var resetRuntimeOnStart bool

	flag.StringVar(&listen, "listen", envString("SPORE_AGENT_LISTEN", ":8080"), "HTTP listen address")
	flag.StringVar(&agentID, "agent-id", envString("SPORE_AGENT_ID", envString("HOSTNAME", "spore-agent-local")), "stable fleet agent id")
	flag.StringVar(&cellID, "cell-id", envString("SPORE_CELL_ID", "default"), "fleet cell id")
	flag.IntVar(&slots, "slots", envInt("SPORE_AGENT_SLOTS", 1), "local child execution slots")
	flag.Int64Var(&childMemoryBytes, "child-memory-bytes", envInt64("SPORE_AGENT_CHILD_MEMORY_BYTES", defaultChildMemoryBytes), "memory budget per child used to clamp slots; 0 disables the cgroup clamp")
	flag.Int64Var(&memoryReserveBytes, "memory-reserve-bytes", envInt64("SPORE_AGENT_MEMORY_RESERVE_BYTES", defaultMemoryReserveBytes), "memory held back from slot calculation for the agent and kernel")
	flag.StringVar(&sporePath, "spore-path", envString("SPORE_PATH", "spore"), "path to the spore CLI")
	flag.StringVar(&resultStoreRoot, "result-store-root", envString("SPORE_RESULT_STORE_ROOT", "/var/lib/sporevm/results"), "local root for S3-shaped result documents")
	flag.StringVar(&resultStoreBackend, "result-store-backend", envString("SPORE_RESULT_STORE_BACKEND", "local"), "result store backend: local or s3")
	flag.StringVar(&resultStoreRegion, "result-store-region", envString("SPORE_RESULT_STORE_REGION", envString("AWS_REGION", "")), "AWS region for the S3 result store")
	flag.StringVar(&resultStoreEndpoint, "result-store-endpoint", envString("SPORE_RESULT_STORE_ENDPOINT", ""), "optional S3-compatible result store endpoint")
	flag.BoolVar(&resultStorePathStyle, "result-store-path-style", envBool("SPORE_RESULT_STORE_PATH_STYLE", false), "use path-style S3 result store URLs")
	flag.StringVar(&workRoot, "work-root", envString("SPORE_WORK_ROOT", "/var/lib/sporevm/work"), "local root for materialized child spores")
	flag.StringVar(&bundleCacheRoot, "bundle-cache-root", envString("SPORE_BUNDLE_CACHE_ROOT", "/var/lib/sporevm/bundle-cache"), "bundle cache root used for status")
	flag.StringVar(&rootFSCacheRoot, "rootfs-cache-root", envString("SPORE_ROOTFS_CACHE_ROOT", "/var/lib/sporevm/rootfs-cache"), "rootfs cache root used for status")
	flag.StringVar(&region, "region", envString("AWS_REGION", ""), "AWS region passed to spore pull")
	flag.StringVar(&backend, "backend", envString("SPORE_BACKEND", "kvm"), "SporeVM backend used for resume")
	flag.DurationVar(&childTimeout, "child-timeout", envDuration("SPORE_AGENT_CHILD_TIMEOUT", 0), "optional per-child resume timeout; 0 disables the agent-side timeout")
	flag.BoolVar(&allowMetadataOnlyRootFS, "allow-metadata-only-rootfs", envBool("SPORE_ALLOW_METADATA_ONLY_ROOTFS", false), "allow metadata-only rootfs pulls")
	flag.IntVar(&templateRetain, "template-retain", envInt("SPORE_AGENT_TEMPLATE_RETAIN", defaultTemplateRetain), "newest completed boot templates retained on this agent")
	flag.DurationVar(&templateReconcileInterval, "template-reconcile-interval", envDuration("SPORE_AGENT_TEMPLATE_RECONCILE_INTERVAL", defaultTemplateReconcile), "interval between boot-template reconciliation passes; 0 disables periodic reconciliation")
	flag.BoolVar(&resetRuntimeOnStart, "reset-runtime-on-start", envBool("SPORE_AGENT_RESET_RUNTIME_ON_START", false), "remove process-local SporeVM runtime state before serving while retaining local backing authority")
	flag.Parse()

	for _, root := range []string{resultStoreRoot, workRoot, bundleCacheRoot, rootFSCacheRoot} {
		if err := os.MkdirAll(root, 0o755); err != nil {
			log.Fatalf("create %s: %v", root, err)
		}
	}
	if resetRuntimeOnStart {
		runtimeRoot := os.Getenv(sporeRuntimeDirEnv)
		if runtimeRoot == "" {
			log.Fatalf("--reset-runtime-on-start requires %s", sporeRuntimeDirEnv)
		}
		if err := resetSporeRuntimeDir(workRoot, runtimeRoot); err != nil {
			log.Fatalf("reset SporeVM runtime directory: %v", err)
		}
	}

	if slots < 1 {
		log.Fatalf("slots must be >= 1: slots=%d", slots)
	}
	if templateRetain < 0 {
		log.Fatalf("template-retain must be >= 0: template_retain=%d", templateRetain)
	}
	if templateReconcileInterval < 0 {
		log.Fatalf("template-reconcile-interval must be >= 0: template_reconcile_interval=%s", templateReconcileInterval)
	}
	memoryLimitFiles := cgroupMemoryLimitFileCandidates("/proc/self/cgroup", "/sys/fs/cgroup", cgroupMemoryLimitFiles)
	effectiveSlots, memoryLimited, err := effectiveSlotsForCgroup(slots, childMemoryBytes, memoryReserveBytes, memoryLimitFiles)
	if err != nil {
		log.Fatalf("detect memory limit: %v", err)
	}
	if effectiveSlots < 1 {
		log.Fatalf("memory limit cannot fit one child: requested_slots=%d child_memory_bytes=%d memory_reserve_bytes=%d", slots, childMemoryBytes, memoryReserveBytes)
	}
	if memoryLimited && effectiveSlots < slots {
		log.Printf("clamped execution slots by cgroup memory requested_slots=%d effective_slots=%d child_memory_bytes=%d memory_reserve_bytes=%d", slots, effectiveSlots, childMemoryBytes, memoryReserveBytes)
	}

	store, err := agent.NewResultStore(context.Background(), agent.ResultStoreConfig{
		Backend:      resultStoreBackend,
		LocalRoot:    resultStoreRoot,
		Region:       resultStoreRegion,
		Endpoint:     resultStoreEndpoint,
		UsePathStyle: resultStorePathStyle,
	})
	if err != nil {
		log.Fatalf("create result store: %v", err)
	}
	spore := agent.CommandClient{Path: sporePath}
	runner, err := agent.NewRunner(
		effectiveSlots,
		agent.WithSporeClient(spore),
		agent.WithResultStore(store),
		agent.WithWorkRoot(workRoot),
		agent.WithBackend(backend),
		agent.WithChildTimeout(childTimeout),
	)
	if err != nil {
		log.Fatalf("create runner: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	reconcileTemplates := func() {
		reconcileCtx, cancel := context.WithTimeout(ctx, templateReconcileTimeout)
		defer cancel()
		result, err := runner.ReconcileBootTemplates(reconcileCtx, templateRetain)
		if err != nil {
			log.Printf("reconcile boot templates: %v", err)
		}
		if result.RemovedBuilds > 0 || result.RemovedTemplates > 0 {
			log.Printf("reconciled boot templates removed_builds=%d removed_templates=%d retained_templates=%d", result.RemovedBuilds, result.RemovedTemplates, result.RetainedTemplates)
		}
	}
	reconcileTemplates()
	if templateReconcileInterval > 0 {
		go func() {
			ticker := time.NewTicker(templateReconcileInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					reconcileTemplates()
				}
			}
		}()
	}
	handler, err := (&agenthttp.Server{
		Runner:                  runner,
		Client:                  spore,
		AgentID:                 agentID,
		CellID:                  cellID,
		Region:                  region,
		Backend:                 backend,
		BundleCacheRoot:         bundleCacheRoot,
		RootFSCacheRoot:         rootFSCacheRoot,
		AllowMetadataOnlyRootFS: allowMetadataOnlyRootFS,
	}).Handler()
	if err != nil {
		log.Fatalf("create handler: %v", err)
	}

	server := &http.Server{
		Addr:              listen,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown HTTP server: %v", err)
		}
	}()

	log.Printf("spore-agent listening on %s agent_id=%s cell_id=%s slots=%d requested_slots=%d backend=%s", listen, agentID, cellID, effectiveSlots, slots, backend)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve HTTP: %v", err)
	}
}

func resetSporeRuntimeDir(workRoot, root string) error {
	absWorkRoot, err := filepath.Abs(workRoot)
	if err != nil {
		return err
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(absWorkRoot, absRoot)
	if err != nil {
		return err
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("runtime root %s must be beneath work root %s", root, workRoot)
	}
	root = absRoot

	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("runtime root %s is not a directory", root)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return err
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == sporeRuntimeAuthorityFile {
			authorityPath := filepath.Join(root, entry.Name())
			entryInfo, err := os.Lstat(authorityPath)
			if err != nil {
				return err
			}
			if !entryInfo.Mode().IsRegular() {
				return fmt.Errorf("runtime authority %s is not a regular file", authorityPath)
			}
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func effectiveSlotsForCgroup(requested int, childMemoryBytes int64, reserveBytes int64, files []string) (int, bool, error) {
	if childMemoryBytes <= 0 {
		return requested, false, nil
	}
	limitBytes, ok, err := cgroupMemoryLimitBytes(files)
	if err != nil || !ok {
		return requested, false, err
	}
	if reserveBytes < 0 {
		reserveBytes = 0
	}
	usableBytes := limitBytes - reserveBytes
	if usableBytes < childMemoryBytes {
		return 0, true, nil
	}
	slotsByMemory := usableBytes / childMemoryBytes
	if slotsByMemory < int64(requested) {
		return int(slotsByMemory), true, nil
	}
	return requested, true, nil
}

func cgroupMemoryLimitFileCandidates(procCgroupPath string, mountRoot string, fallback []string) []string {
	files := make([]string, 0, len(fallback)+1)
	if current, ok := currentCgroupV2MemoryLimitFile(procCgroupPath, mountRoot); ok {
		files = append(files, current)
	}
	for _, path := range fallback {
		if !containsString(files, path) {
			files = append(files, path)
		}
	}
	return files
}

func currentCgroupV2MemoryLimitFile(procCgroupPath string, mountRoot string) (string, bool) {
	data, err := os.ReadFile(procCgroupPath)
	if err != nil {
		return "", false
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 || parts[0] != "0" || parts[1] != "" {
			continue
		}
		return filepath.Join(mountRoot, strings.TrimPrefix(parts[2], "/"), "memory.max"), true
	}
	return "", false
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func cgroupMemoryLimitBytes(files []string) (int64, bool, error) {
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return 0, false, err
		}
		value := strings.TrimSpace(string(data))
		if value == "" || value == "max" {
			return 0, false, nil
		}
		limit, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("parse %s: %w", path, err)
		}
		if limit <= 0 {
			return 0, false, fmt.Errorf("parse %s: memory limit must be positive", path)
		}
		return limit, true, nil
	}
	return 0, false, nil
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envInt64(name string, fallback int64) int64 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(name string, fallback bool) bool {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
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
