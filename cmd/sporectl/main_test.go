package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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

func TestBuildBundleSubmitResourcesUsesBundleRunArg(t *testing.T) {
	run := testBundleRun("ruby-counter-20260620")
	runBytes := []byte(`{"runID":"ruby-counter-20260620"}`)
	resources, names, err := buildBundleSubmitResources(run, runBytes, submitOptions{
		Namespace:       "sporevm-system",
		Image:           "example.com/sporevm-k8s-runtime:dev",
		ImagePullPolicy: "Always",
		AgentURLs:       stringsFlag{"http://spore-agent.sporevm-system.svc.cluster.local:8080"},
		ResultStoreRoot: "/var/lib/sporevm/coordinator-results",
		Timeout:         30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("build bundle resources: %v", err)
	}
	if names.ConfigMap == "spore-run" || names.Job == "spore-coordinator" {
		t.Fatalf("expected per-run names, got configmap=%s job=%s", names.ConfigMap, names.Job)
	}
	configMap := resources.Items[0].(configMap)
	if got := configMap.Data["bundle-run.json"]; got != string(runBytes) {
		t.Fatalf("bundle run configmap data = %q", got)
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
		`"bundle-run.json":"{\"runID\":\"ruby-counter-20260620\"}"`,
		`"--bundle-run=/etc/sporevm/run/bundle-run.json"`,
		`"--agent-url=http://spore-agent.sporevm-system.svc.cluster.local:8080"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("resource JSON missing %s:\n%s", want, body)
		}
	}
}

func TestBuildSubmitResourcesUsesRunArg(t *testing.T) {
	run := testRun("rails-rspec-20260624")
	runBytes := []byte(`{"runID":"rails-rspec-20260624"}`)
	resources, names, err := buildSubmitResources(run, runBytes, submitOptions{
		Namespace:       "sporevm-system",
		Image:           "example.com/sporevm-k8s-runtime:dev",
		ImagePullPolicy: "Always",
		AgentURLs:       stringsFlag{"http://spore-agent.sporevm-system.svc.cluster.local:8080"},
		ResultStoreRoot: "/var/lib/sporevm/coordinator-results",
		Timeout:         30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("build source resources: %v", err)
	}
	if names.ConfigMap == "spore-run" || names.Job == "spore-coordinator" {
		t.Fatalf("expected per-run names, got configmap=%s job=%s", names.ConfigMap, names.Job)
	}
	configMap := resources.Items[0].(configMap)
	if got := configMap.Data["run.json"]; got != string(runBytes) {
		t.Fatalf("run configmap data = %q", got)
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
		`"run.json":"{\"runID\":\"rails-rspec-20260624\"}"`,
		`"--run=/etc/sporevm/run/run.json"`,
		`"--agent-url=http://spore-agent.sporevm-system.svc.cluster.local:8080"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("resource JSON missing %s:\n%s", want, body)
		}
	}
	if strings.Contains(body, `--bundle-run=/etc/sporevm/run/bundle-run.json`) {
		t.Fatalf("run resource JSON included bundle run arg:\n%s", body)
	}
}

func TestBuildSubmitResourcesConfiguresS3ResultStore(t *testing.T) {
	run := testRun("rails-rspec-s3")
	resources, _, err := buildSubmitResources(run, mustJSON(t, run), submitOptions{
		Namespace:            "sporevm-system",
		Image:                "example.com/sporevm-k8s-runtime:dev",
		ImagePullPolicy:      "Always",
		AgentURLs:            stringsFlag{"http://spore-agent.sporevm-system.svc.cluster.local:8080"},
		ResultStoreRoot:      "/unused",
		ResultStoreBackend:   "s3",
		ResultStoreRegion:    "example-region-1",
		ResultStoreEndpoint:  "https://objects.example.test",
		ResultStorePathStyle: true,
		ServiceAccountName:   "spore-coordinator",
		Timeout:              30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("build resources: %v", err)
	}
	payload, err := json.Marshal(resources)
	if err != nil {
		t.Fatalf("marshal resources: %v", err)
	}
	body := string(payload)
	for _, want := range []string{
		`--result-store-backend=s3`,
		`--result-store-region=example-region-1`,
		`--result-store-endpoint=https://objects.example.test`,
		`--result-store-path-style=true`,
		`"serviceAccountName":"spore-coordinator"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("resource JSON missing %q:\n%s", want, body)
		}
	}
}

func TestBuildSubmitResourcesFromOptionsInfersBundleRun(t *testing.T) {
	runBytes := mustJSON(t, testBundleRun("ruby-counter-20260620"))
	resources, _, details, err := buildSubmitResourcesFromOptions(testSubmitOptions(t, runBytes))
	if err != nil {
		t.Fatalf("build submit resources: %v", err)
	}
	if details.RunID != "ruby-counter-20260620" {
		t.Fatalf("runID = %q", details.RunID)
	}

	configMap := resources.Items[0].(configMap)
	if got := configMap.Data["bundle-run.json"]; got != string(runBytes) {
		t.Fatalf("bundle run configmap data = %q", got)
	}
	payload, err := json.Marshal(resources)
	if err != nil {
		t.Fatalf("marshal resources: %v", err)
	}
	body := string(payload)
	if !strings.Contains(body, `--bundle-run=/etc/sporevm/run/bundle-run.json`) {
		t.Fatalf("bundle resource JSON missing coordinator bundle-run arg:\n%s", body)
	}
}

func TestBuildSubmitResourcesFromOptionsInfersRun(t *testing.T) {
	runBytes := mustJSON(t, testRun("rails-rspec-20260624"))
	resources, _, details, err := buildSubmitResourcesFromOptions(testSubmitOptions(t, runBytes))
	if err != nil {
		t.Fatalf("build submit resources: %v", err)
	}
	if details.RunID != "rails-rspec-20260624" {
		t.Fatalf("runID = %q", details.RunID)
	}

	configMap := resources.Items[0].(configMap)
	if got := configMap.Data["run.json"]; got != string(runBytes) {
		t.Fatalf("run configmap data = %q", got)
	}
	payload, err := json.Marshal(resources)
	if err != nil {
		t.Fatalf("marshal resources: %v", err)
	}
	body := string(payload)
	if !strings.Contains(body, `--run=/etc/sporevm/run/run.json`) {
		t.Fatalf("run resource JSON missing coordinator run arg:\n%s", body)
	}
	if strings.Contains(body, `--bundle-run=/etc/sporevm/run/bundle-run.json`) {
		t.Fatalf("run resource JSON included bundle run arg:\n%s", body)
	}
}

func TestDetectSubmitRunKindRejectsAmbiguousOrUnknownRunShape(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "mixed",
			body: `{"runID":"mixed","bundle":{},"source":{}}`,
			want: "mixes bundle run fields with run fields",
		},
		{
			name: "unknown",
			body: `{"runID":"unknown"}`,
			want: "must include either run fields",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := detectSubmitRunKind([]byte(tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSubmitDryRunReadsPositionalRunFile(t *testing.T) {
	path := writeRunFile(t, mustJSON(t, testRun("rails-rspec-20260624")))
	var stdout, stderr bytes.Buffer
	if err := run([]string{"submit", "--dry-run", path}, &stdout, &stderr); err != nil {
		t.Fatalf("dry-run submit: %v\nstderr=%s", err, stderr.String())
	}

	body := stdout.String()
	for _, want := range []string{
		`"kind": "List"`,
		`"run.json"`,
		`"--run=/etc/sporevm/run/run.json"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("dry-run output missing %s:\n%s", want, body)
		}
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

func TestBuildkiteRunAnnotationUsesAggregateReportAndResultLink(t *testing.T) {
	logs := []byte("coordinator log line\n" + `{
  "summary": {
    "runID": "rails-rspec-20260624",
    "state": "failed",
    "childCount": 100,
    "succeededChildren": 98,
    "failedChildren": 2,
    "attemptCount": 102
  }
}` + "\n")
	report, ok := runtimeReportFromLogs(logs)
	if !ok {
		t.Fatal("runtime report was not found in coordinator logs")
	}
	annotation := buildkiteRunAnnotation(
		submitDetails{RunID: "rails-rspec-20260624", ResultStore: "s3://example-results/run/"},
		report,
		errors.New("coordinator job failed"),
		"https://results.example/run/",
	)
	for _, want := range []string{
		"SporeVM run `rails-rspec-20260624`: failed",
		"| 100 | 98 | 2 | 102 |",
		"[Child result objects](https://results.example/run/)",
		"Coordinator: `coordinator job failed`",
	} {
		if !strings.Contains(annotation, want) {
			t.Fatalf("annotation missing %q:\n%s", want, annotation)
		}
	}
}

func TestSubmitHelpExitsZero(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{"submit", "--help"}, &stdout, &stderr); err != nil {
		t.Fatalf("help returned error: %v", err)
	}
	if !strings.Contains(stderr.String(), "usage: sporectl submit [flags] RUN.json") {
		t.Fatalf("help did not print flag usage: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestSubmitRequiresOneRunInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing",
			args: []string{"submit", "--dry-run"},
			want: "run JSON path is required",
		},
		{
			name: "extra",
			args: []string{"submit", "--dry-run", "run.json", "other.json"},
			want: `unexpected argument "other.json"`,
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

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()

	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return data
}

func testSubmitOptions(t *testing.T, runBytes []byte) submitOptions {
	t.Helper()

	return submitOptions{
		InputPath:       writeRunFile(t, runBytes),
		Namespace:       "sporevm-system",
		Image:           "example.com/sporevm-k8s-runtime:dev",
		ImagePullPolicy: "Always",
		AgentURLs:       stringsFlag{"http://spore-agent.sporevm-system.svc.cluster.local:8080"},
		ResultStoreRoot: "/var/lib/sporevm/coordinator-results",
		Timeout:         30 * time.Minute,
	}
}

func writeRunFile(t *testing.T, runBytes []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "run.json")
	if err := os.WriteFile(path, runBytes, 0o600); err != nil {
		t.Fatalf("write run JSON: %v", err)
	}
	return path
}

func testBundleRun(id string) fleet.BundleRun {
	return fleet.BundleRun{
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

func testRun(id string) fleet.Run {
	return fleet.Run{
		RunID: id,
		Source: fleet.RunSource{
			Image:    "example.com/sporevm/rails-rspec:sha-1111111",
			Platform: "linux/arm64",
		},
		Prepare: fleet.PrepareSpec{
			Command:       []string{"/bin/bash", "/usr/local/bin/sporevm-rails-coordinator", "--capture-delay", "2"},
			CaptureSignal: "USR1",
			ReadyMarker:   "SPOREVM_RAILS_READY",
		},
		Fork: fleet.ForkSpec{Count: 1000},
		Children: fleet.RunChildren{
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
