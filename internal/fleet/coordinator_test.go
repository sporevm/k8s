package fleet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDecodeBundleRunRejectsUnknownFields(t *testing.T) {
	var raw map[string]any
	decodeExample(t, "checkpoint-run-1000.json", &raw)
	raw["unexpected"] = true

	payload, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal run: %v", err)
	}

	_, err = DecodeCheckpointRun(bytes.NewReader(payload))
	if err == nil {
		t.Fatal("DecodeBundleRun succeeded with unknown field")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("DecodeBundleRun error = %v, want ErrInvalidContract", err)
	}
}

func TestDecodeBundleRunRejectsTrailingJSON(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("..", "..", "examples", "fleet", "checkpoint-run-1000.json"))
	if err != nil {
		t.Fatalf("read run example: %v", err)
	}
	payload = append(payload, []byte("\n{}")...)

	_, err = DecodeCheckpointRun(bytes.NewReader(payload))
	if err == nil {
		t.Fatal("DecodeBundleRun succeeded with trailing JSON")
	}
	if !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("DecodeBundleRun error = %v, want ErrInvalidContract", err)
	}
}

func TestBuildPlanAssigns1000Children(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 1, 100, run.HostClass)

	plan, err := BuildPlan(run, agents)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}

	if plan.Summary.ShardCount != 1 {
		t.Fatalf("shards = %d, want 1", plan.Summary.ShardCount)
	}
	if plan.Summary.AvailableChildSlots != 100 {
		t.Fatalf("available slots = %d, want 100", plan.Summary.AvailableChildSlots)
	}
	if plan.Summary.AssignedChildren != 1000 {
		t.Fatalf("assigned children = %d, want 1000", plan.Summary.AssignedChildren)
	}

	lease := plan.Leases[0]
	if lease.AgentID != agents[0].AgentID {
		t.Fatalf("lease agent = %q, want %q", lease.AgentID, agents[0].AgentID)
	}
	if lease.ChildStart != 0 || lease.ChildCount != 1000 {
		t.Fatalf("lease range = [%d, %d), want [0, 1000)", lease.ChildStart, lease.ChildStart+lease.ChildCount)
	}
}

func TestBuildPlanRejectsNoAvailableSlots(t *testing.T) {
	run := loadExampleRun(t)
	agents := makeAgents(t, 1, 99, run.HostClass)
	agents[0].ExecutionSlots.Available = 0

	plan, err := BuildPlan(run, agents)
	if !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("BuildPlan error = %v, want ErrInsufficientCapacity", err)
	}
	if plan.Summary.State != "refused" {
		t.Fatalf("summary state = %q, want refused", plan.Summary.State)
	}
}

func TestBuildPlanRejectsHostClassMismatch(t *testing.T) {
	run := loadExampleRun(t)
	hostClass := run.HostClass
	hostClass.CPUProfile = "graviton2"
	agents := makeAgents(t, 1, 100, hostClass)

	plan, err := BuildPlan(run, agents)
	if !errors.Is(err, ErrNoCompatibleAgents) {
		t.Fatalf("BuildPlan error = %v, want ErrNoCompatibleAgents", err)
	}
	if plan.Summary.State != "refused" {
		t.Fatalf("summary state = %q, want refused", plan.Summary.State)
	}
}

func TestBuildPlanCoversGlobalRangeWithinAgentCapacity(t *testing.T) {
	run := loadExampleRun(t)
	run.Children = ChildRange{Start: 11, Count: 7}
	agents := makeAgents(t, 3, 3, run.HostClass)
	agents[0].ExecutionSlots.Available = 1
	agents[2].Pressure.Memory = PressureCritical
	agents[2].Healthy = false
	plan, err := BuildPlan(run, []AgentStatus{agents[2], agents[1], agents[0]})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Leases) != 2 || plan.Summary.AssignedChildren != 7 || plan.Summary.AvailableChildSlots != 4 {
		t.Fatalf("plan = %+v", plan)
	}
	for i, want := range []ChildRange{{Start: 11, Count: 1}, {Start: 12, Count: 6}} {
		lease := plan.Leases[i]
		if lease.ChildRange() != want || lease.AgentID != agents[i].AgentID || lease.CheckpointDigest != run.Checkpoint.Digest {
			t.Fatalf("lease %d = %+v, want range %+v on %s", i, lease, want, agents[i].AgentID)
		}
		if err := lease.Validate(run); err != nil {
			t.Fatal(err)
		}
		if got := ShardSlotDemand(run, lease); got != []int{1, 3}[i] {
			t.Fatalf("lease %d slot demand = %d", i, got)
		}
	}
	if _, err := BuildPlan(run, []AgentStatus{agents[0], agents[0]}); !errors.Is(err, ErrInvalidContract) {
		t.Fatalf("duplicate agent error = %v", err)
	}
}

func loadExampleRun(t *testing.T) CheckpointRun {
	t.Helper()

	path := filepath.Join("..", "..", "examples", "fleet", "checkpoint-run-1000.json")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open run example: %v", err)
	}
	defer f.Close()

	run, err := DecodeCheckpointRun(f)
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
