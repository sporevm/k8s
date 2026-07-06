package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sporevm/k8s/internal/benchmark"
	"github.com/sporevm/k8s/internal/fleet"
)

func main() {
	var bundleRunPath string
	var agentCount int
	var slotsPerAgent int
	var targetConcurrency int
	var cachePosture string
	var admissionDelay time.Duration
	flag.StringVar(&bundleRunPath, "bundle-run", "examples/fleet/bundle-run-1000.json", "fleet bundle run contract JSON")
	flag.IntVar(&agentCount, "agents", 10, "synthetic agent count")
	flag.IntVar(&slotsPerAgent, "slots-per-agent", 100, "synthetic child slots per agent")
	flag.IntVar(&targetConcurrency, "target-concurrency", 0, "target ready child count; defaults to run child count")
	flag.StringVar(&cachePosture, "cache-posture", string(benchmark.CachePostureWarmBundleColdMaterialization), "cache posture")
	flag.DurationVar(&admissionDelay, "admission-delay", 50*time.Millisecond, "synthetic admission delay")
	flag.Parse()

	run, err := readBundleRun(bundleRunPath)
	if err != nil {
		fatal(err)
	}
	result, err := benchmark.RunSynthetic(context.Background(), run, benchmark.SyntheticOptions{
		AgentCount:        agentCount,
		SlotsPerAgent:     slotsPerAgent,
		TargetConcurrency: targetConcurrency,
		CachePosture:      benchmark.CachePosture(cachePosture),
		AdmissionDelay:    admissionDelay,
	})
	if err != nil {
		fatal(err)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result.Summary); err != nil {
		fatal(err)
	}
}

func readBundleRun(path string) (fleet.BundleRun, error) {
	file, err := os.Open(path)
	if err != nil {
		return fleet.BundleRun{}, err
	}
	defer file.Close()
	return fleet.DecodeBundleRun(file)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
