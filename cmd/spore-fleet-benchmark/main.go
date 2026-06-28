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
	var runPath string
	var agentCount int
	var slotsPerAgent int
	var targetConcurrency int
	var cachePosture string
	var admissionDelay time.Duration
	flag.StringVar(&runPath, "run", "examples/fleet/run-1000.json", "fleet run contract JSON")
	flag.IntVar(&agentCount, "agents", 10, "synthetic agent count")
	flag.IntVar(&slotsPerAgent, "slots-per-agent", 100, "synthetic child slots per agent")
	flag.IntVar(&targetConcurrency, "target-concurrency", 0, "target ready child count; defaults to run child count")
	flag.StringVar(&cachePosture, "cache-posture", string(benchmark.CachePostureWarmBundleColdMaterialization), "cache posture")
	flag.DurationVar(&admissionDelay, "admission-delay", 50*time.Millisecond, "synthetic admission delay")
	flag.Parse()

	run, err := readRun(runPath)
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

func readRun(path string) (fleet.Run, error) {
	file, err := os.Open(path)
	if err != nil {
		return fleet.Run{}, err
	}
	defer file.Close()
	return fleet.DecodeRun(file)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
