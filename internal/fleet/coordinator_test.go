package fleet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDecodeBundleRunRejectsUnknownFields(t *testing.T) {
	var raw map[string]any
	decodeExample(t, "bundle-run-1000.json", &raw)
	raw["unexpected"] = true

	payload, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal run: %v", err)
	}

	_, err = DecodeBundleRun(bytes.NewReader(payload))
	if err == nil {
		t.Fatal("DecodeBundleRun succeeded with unknown field")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("DecodeBundleRun error = %v, want ErrInvalidContract", err)
	}
}

func TestDecodeBundleRunRejectsTrailingJSON(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("..", "..", "examples", "fleet", "bundle-run-1000.json"))
	if err != nil {
		t.Fatalf("read run example: %v", err)
	}
	payload = append(payload, []byte("\n{}")...)

	_, err = DecodeBundleRun(bytes.NewReader(payload))
	if err == nil {
		t.Fatal("DecodeBundleRun succeeded with trailing JSON")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("DecodeBundleRun error = %v, want ErrInvalidContract", err)
	}
}

func TestBuildDryRunPlanAssigns1000Children(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 10, 100, run.HostClass)
	now := time.Date(2026, 6, 20, 4, 0, 0, 0, time.UTC)

	plan, err := BuildDryRunPlan(run, agents, DryRunOptions{
		Now:      now,
		LeaseTTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("BuildDryRunPlan: %v", err)
	}

	if plan.Summary.ChildCount != 1000 {
		t.Fatalf("child count = %d, want 1000", plan.Summary.ChildCount)
	}
	if plan.Summary.ShardCount != 10 || len(plan.Leases) != 10 {
		t.Fatalf("shards = summary %d leases %d, want 10", plan.Summary.ShardCount, len(plan.Leases))
	}
	if plan.Summary.AssignedChildren != 1000 {
		t.Fatalf("assigned children = %d, want 1000", plan.Summary.AssignedChildren)
	}
	if err := ValidateCompleteCoverage(run, plan.Leases); err != nil {
		t.Fatalf("ValidateCompleteCoverage: %v", err)
	}

	for i, lease := range plan.Leases {
		if lease.ShardID != fmt.Sprintf("%s-shard-%04d", run.RunID, i) {
			t.Fatalf("lease %d shard id = %q", i, lease.ShardID)
		}
		if !lease.LeaseDeadline.Equal(now.Add(10 * time.Minute)) {
			t.Fatalf("lease %d deadline = %s", i, lease.LeaseDeadline)
		}
		if lease.ChildStart != i*100 || lease.ChildCount != 100 {
			t.Fatalf("lease %d range = [%d, %d)", i, lease.ChildStart, lease.ChildStart+lease.ChildCount)
		}
	}
}

func TestBuildSingleAgentSequentialPlanAssigns1000Children(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 1, 100, run.HostClass)
	now := time.Date(2026, 6, 20, 4, 0, 0, 0, time.UTC)

	plan, err := BuildSingleAgentSequentialPlan(run, agents, DryRunOptions{
		Now:      now,
		LeaseTTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("BuildSingleAgentSequentialPlan: %v", err)
	}

	if plan.Summary.ShardCount != 1 || len(plan.Leases) != 1 {
		t.Fatalf("shards = summary %d leases %d, want 1", plan.Summary.ShardCount, len(plan.Leases))
	}
	if plan.Summary.AvailableChildSlots != 100 {
		t.Fatalf("available slots = %d, want 100", plan.Summary.AvailableChildSlots)
	}
	if plan.Summary.AssignedChildren != 1000 {
		t.Fatalf("assigned children = %d, want 1000", plan.Summary.AssignedChildren)
	}
	if err := ValidateCompleteCoverage(run, plan.Leases); err != nil {
		t.Fatalf("ValidateCompleteCoverage: %v", err)
	}

	lease := plan.Leases[0]
	if lease.AgentID != agents[0].AgentID {
		t.Fatalf("lease agent = %q, want %q", lease.AgentID, agents[0].AgentID)
	}
	if lease.ChildStart != 0 || lease.ChildCount != 1000 {
		t.Fatalf("lease range = [%d, %d), want [0, 1000)", lease.ChildStart, lease.ChildStart+lease.ChildCount)
	}
	if !lease.LeaseDeadline.Equal(now.Add(10 * time.Minute)) {
		t.Fatalf("lease deadline = %s", lease.LeaseDeadline)
	}
}

func TestBuildSingleAgentSequentialPlanRejectsInsufficientInFlightCapacity(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 1, 99, run.HostClass)

	plan, err := BuildSingleAgentSequentialPlan(run, agents, DryRunOptions{})
	if !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("BuildSingleAgentSequentialPlan error = %v, want ErrInsufficientCapacity", err)
	}
	if plan.Summary.State != "refused" {
		t.Fatalf("summary state = %q, want refused", plan.Summary.State)
	}
}

func TestBuildDryRunPlanRejectsInsufficientCapacity(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 9, 100, run.HostClass)

	plan, err := BuildDryRunPlan(run, agents, DryRunOptions{})
	if !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("BuildDryRunPlan error = %v, want ErrInsufficientCapacity", err)
	}
	if plan.Summary.State != "refused" {
		t.Fatalf("summary state = %q, want refused", plan.Summary.State)
	}
}

func TestBuildDryRunPlanRejectsHostClassMismatch(t *testing.T) {
	run := loadExampleRun(t)
	hostClass := run.HostClass
	hostClass.CPUProfile = "graviton2"
	agents := makeAgents(t, 10, 100, hostClass)

	plan, err := BuildDryRunPlan(run, agents, DryRunOptions{})
	if !errors.Is(err, ErrNoCompatibleAgents) {
		t.Fatalf("BuildDryRunPlan error = %v, want ErrNoCompatibleAgents", err)
	}
	if plan.Summary.State != "refused" {
		t.Fatalf("summary state = %q, want refused", plan.Summary.State)
	}
}

func TestBuildDryRunPlanRejectsFragmentedCapacity(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 20, 50, run.HostClass)

	_, err := BuildDryRunPlan(run, agents, DryRunOptions{})
	if !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("BuildDryRunPlan error = %v, want ErrInsufficientCapacity", err)
	}
}

func TestValidateCompleteCoverageRejectsGaps(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 10, 100, run.HostClass)

	plan, err := BuildDryRunPlan(run, agents, DryRunOptions{})
	if err != nil {
		t.Fatalf("BuildDryRunPlan: %v", err)
	}
	plan.Leases[5].ChildStart += 1

	err = ValidateCompleteCoverage(run, plan.Leases)
	if err == nil {
		t.Fatal("ValidateCompleteCoverage succeeded with a gap")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("ValidateCompleteCoverage error = %v, want ErrInvalidContract", err)
	}
}

func loadExampleRun(t *testing.T) BundleRun {
	t.Helper()

	path := filepath.Join("..", "..", "examples", "fleet", "bundle-run-1000.json")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open run example: %v", err)
	}
	defer f.Close()

	run, err := DecodeBundleRun(f)
	if err != nil {
		t.Fatalf("DecodeBundleRun: %v", err)
	}
	return run
}

func loadExampleAgent(t *testing.T) AgentStatus {
	t.Helper()

	var agent AgentStatus
	decodeExample(t, "agent-status.json", &agent)
	if err := agent.Validate(); err != nil {
		t.Fatalf("agent Validate: %v", err)
	}
	return agent
}

func makeAgents(t *testing.T, count int, available int, hostClass HostClass) []AgentStatus {
	t.Helper()

	base := loadExampleAgent(t)
	base.HostClass = hostClass
	base.ExecutionSlots.Total = available
	base.ExecutionSlots.Available = available

	agents := make([]AgentStatus, 0, count)
	for i := 0; i < count; i++ {
		agent := base
		agent.AgentID = fmt.Sprintf("spore-agent-us-east-1a-%04d", i+1)
		agents = append(agents, agent)
	}
	return agents
}

func decodeExample(t *testing.T, name string, out any) {
	t.Helper()

	path := filepath.Join("..", "..", "examples", "fleet", name)
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if err := json.Unmarshal(payload, out); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
}
