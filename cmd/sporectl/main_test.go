package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sporevm/k8s/internal/fleet"
)

func TestBuildSubmitResourcesUsesPerRunObjects(t *testing.T) {
	run := testRun("ruby.counter_20260620")
	runBytes := []byte(`{"runID":"ruby.counter_20260620"}`)
	resources, names, err := buildSubmitResources(run, runBytes, submitOptions{
		Namespace:       "sporevm-system",
		Image:           "example.com/sporevm-k8s-runtime:dev",
		ImagePullPolicy: "Always",
		AgentURLs:       stringsFlag{"http://spore-agent.sporevm-system.svc.cluster.local:8080"},
		ResultStoreRoot: "/var/lib/sporevm/coordinator-results",
		Timeout:         30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("build resources: %v", err)
	}
	if names.ConfigMap == "spore-run" || names.Job == "spore-coordinator" {
		t.Fatalf("expected per-run names, got configmap=%s job=%s", names.ConfigMap, names.Job)
	}
	configMap := resources.Items[0].(configMap)
	if value := configMap.Metadata.Labels["sporevm.io/run"]; strings.ContainsAny(value, "_.") {
		t.Fatalf("run label value is not DNS-safe: %s", value)
	}
	payload, err := json.Marshal(resources)
	if err != nil {
		t.Fatalf("marshal resources: %v", err)
	}
	body := string(payload)
	for _, want := range []string{
		`"kind":"ConfigMap"`,
		`"kind":"Job"`,
		`"name":"` + names.ConfigMap + `"`,
		`"name":"` + names.Job + `"`,
		`"--run=/etc/sporevm/run/run.json"`,
		`"--agent-url=http://spore-agent.sporevm-system.svc.cluster.local:8080"`,
		`"emptyDir":{}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("resource JSON missing %s:\n%s", want, body)
		}
	}
}

func TestBuildGenericSubmitResourcesUsesGenericRunArg(t *testing.T) {
	run := testGenericRun("rails-rspec-20260624")
	runBytes := []byte(`{"runID":"rails-rspec-20260624"}`)
	resources, names, err := buildGenericSubmitResources(run, runBytes, submitOptions{
		Namespace:       "sporevm-system",
		Image:           "example.com/sporevm-k8s-runtime:dev",
		ImagePullPolicy: "Always",
		AgentURLs:       stringsFlag{"http://spore-agent.sporevm-system.svc.cluster.local:8080"},
		ResultStoreRoot: "/var/lib/sporevm/coordinator-results",
		Timeout:         30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("build generic resources: %v", err)
	}
	if names.ConfigMap == "spore-run" || names.Job == "spore-coordinator" {
		t.Fatalf("expected per-run names, got configmap=%s job=%s", names.ConfigMap, names.Job)
	}
	configMap := resources.Items[0].(configMap)
	if got := configMap.Data["generic-run.json"]; got != string(runBytes) {
		t.Fatalf("generic run configmap data = %q", got)
	}
	payload, err := json.Marshal(resources)
	if err != nil {
		t.Fatalf("marshal resources: %v", err)
	}
	body := string(payload)
	for _, want := range []string{
		`"kind":"ConfigMap"`,
		`"kind":"Job"`,
		`"name":"` + names.ConfigMap + `"`,
		`"name":"` + names.Job + `"`,
		`"generic-run.json":"{\"runID\":\"rails-rspec-20260624\"}"`,
		`"--generic-run=/etc/sporevm/run/generic-run.json"`,
		`"--agent-url=http://spore-agent.sporevm-system.svc.cluster.local:8080"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("resource JSON missing %s:\n%s", want, body)
		}
	}
	if strings.Contains(body, `--run=/etc/sporevm/run/run.json`) {
		t.Fatalf("generic resource JSON included bundle run arg:\n%s", body)
	}
}

func TestKubernetesNameIsDNSLabelSized(t *testing.T) {
	name := kubernetesName("spore-coordinator", "UPPER.long_name.with.dots.and.enough.characters.to.force.truncation.20260620")
	if len(name) > 63 {
		t.Fatalf("name too long: %d %s", len(name), name)
	}
	if strings.ContainsAny(name, "_.ABCDEFGHIJKLMNOPQRSTUVWXYZ") {
		t.Fatalf("name is not sanitized: %s", name)
	}
}

func TestJobTerminalState(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		complete bool
		failed   bool
	}{
		{
			name:     "complete",
			body:     `{"status":{"conditions":[{"type":"Complete","status":"True"}]}}`,
			complete: true,
		},
		{
			name:   "failed",
			body:   `{"status":{"conditions":[{"type":"Failed","status":"True","message":"backoff limit reached"}]}}`,
			failed: true,
		},
		{
			name: "still running",
			body: `{"status":{"conditions":[]}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, err := jobTerminalState([]byte(tc.body))
			if err != nil {
				t.Fatalf("jobTerminalState: %v", err)
			}
			if state.Complete != tc.complete || state.Failed != tc.failed {
				t.Fatalf("state = %+v, want complete=%v failed=%v", state, tc.complete, tc.failed)
			}
		})
	}
}

func TestSubmitHelpExitsZero(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{"submit", "--help"}, &stdout, &stderr); err != nil {
		t.Fatalf("help returned error: %v", err)
	}
	if !strings.Contains(stderr.String(), "Usage of submit:") {
		t.Fatalf("help did not print flag usage: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestSubmitRequiresExactlyOneRunInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing",
			args: []string{"submit", "--dry-run"},
			want: "one of --run or --generic-run is required",
		},
		{
			name: "both",
			args: []string{"submit", "--run", "run.json", "--generic-run", "generic-run.json", "--dry-run"},
			want: "only one of --run or --generic-run may be set",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(tc.args, &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func testRun(id string) fleet.Run {
	return fleet.Run{
		RunID: id,
		Bundle: fleet.Bundle{
			URI:    "s3://example-sporevm-artifacts/runs/ruby-counter.bundle",
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
		Children: fleet.ChildRange{Start: 0, Count: 1000},
		HostClass: fleet.HostClass{
			ID:                   "linux-aarch64-kvm-graviton1-v0",
			SporePlatformVersion: "v0",
			Architecture:         "aarch64",
			Backend:              "kvm",
			CPUProfile:           "graviton1",
			DeviceModel:          "sporevm-aarch64-v0",
		},
		Execution: fleet.Execution{
			ChildrenPerShard:    100,
			MaxInFlightPerAgent: 100,
		},
		RetryPolicy: fleet.RetryPolicy{
			MaxAttemptsPerChild:    2,
			RerunCommittedChildren: false,
		},
		SideEffects: fleet.SideEffects{IdempotencyRequired: true},
		ResultStore: "s3://example-sporevm-results/ruby-counter-20260620/",
	}
}

func testGenericRun(id string) fleet.GenericRun {
	return fleet.GenericRun{
		RunID: id,
		Source: fleet.GenericSource{
			Image:    "example.com/sporevm/rails-rspec:sha-1111111",
			Platform: "linux/arm64",
		},
		Prepare: fleet.PrepareSpec{
			Command:       []string{"/bin/bash", "/usr/local/bin/sporevm-rails-coordinator", "--capture-delay", "2"},
			CaptureSignal: "USR1",
			ReadyMarker:   "SPOREVM_RAILS_READY",
		},
		Fork: fleet.ForkSpec{Count: 1000},
		Children: fleet.GenericChildren{
			Start:   0,
			Count:   1000,
			Command: []string{"/usr/local/bin/sporevm-rspec-shard"},
		},
		Execution: fleet.Execution{
			ChildrenPerShard:    100,
			MaxInFlightPerAgent: 100,
		},
		RetryPolicy: fleet.RetryPolicy{
			MaxAttemptsPerChild:    2,
			RerunCommittedChildren: false,
		},
		SideEffects: fleet.SideEffects{IdempotencyRequired: true},
		ResultStore: "s3://example-sporevm-results/rails-rspec-20260624/",
	}
}
