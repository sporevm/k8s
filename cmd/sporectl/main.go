package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/sporevm/k8s/internal/fleet"
)

const (
	defaultNamespace          = "sporevm-system"
	defaultImage              = "ghcr.io/sporevm/k8s-runtime:0.1.13"
	defaultResultRoot         = "/var/lib/sporevm/coordinator-results"
	defaultRunMountPath       = "/etc/sporevm/run/run.json"
	defaultBundleRunMountPath = "/etc/sporevm/run/bundle-run.json"
	defaultRuntimePolicy      = "Always"
	submitUsage               = "usage: sporectl submit [flags] RUN.json"
)

type submitOptions struct {
	InputPath       string
	Namespace       string
	Image           string
	ImagePullPolicy string
	AgentURLs       stringsFlag
	ResultStoreRoot string
	Timeout         time.Duration
	Kubectl         string
	Wait            bool
	Logs            bool
	DryRun          bool
	Replace         bool
}

type stringsFlag []string

func (f *stringsFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringsFlag) Set(value string) error {
	for _, raw := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(raw); trimmed != "" {
			*f = append(*f, trimmed)
		}
	}
	return nil
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New(submitUsage)
	}
	switch args[0] {
	case "submit":
		return runSubmit(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		fmt.Fprintln(stdout, submitUsage)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runSubmit(args []string, stdout, stderr io.Writer) error {
	opts := submitOptions{
		Namespace:       defaultNamespace,
		Image:           envString("SPORE_RUNTIME_IMAGE", defaultImage),
		ImagePullPolicy: defaultRuntimePolicy,
		ResultStoreRoot: defaultResultRoot,
		Timeout:         30 * time.Minute,
		Kubectl:         envString("KUBECTL", "kubectl"),
		Wait:            true,
		Logs:            true,
	}

	fs := flag.NewFlagSet("submit", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), submitUsage)
		fs.PrintDefaults()
	}
	fs.StringVar(&opts.Namespace, "namespace", opts.Namespace, "Kubernetes namespace")
	fs.StringVar(&opts.Image, "image", opts.Image, "coordinator image")
	fs.StringVar(&opts.ImagePullPolicy, "image-pull-policy", opts.ImagePullPolicy, "coordinator image pull policy")
	fs.Var(&opts.AgentURLs, "agent-url", "agent base URL; may be repeated or comma-separated")
	fs.StringVar(&opts.ResultStoreRoot, "result-store-root", opts.ResultStoreRoot, "coordinator local result-store root")
	fs.DurationVar(&opts.Timeout, "timeout", opts.Timeout, "coordinator and kubectl wait timeout")
	fs.StringVar(&opts.Kubectl, "kubectl", opts.Kubectl, "kubectl binary")
	fs.BoolVar(&opts.Wait, "wait", opts.Wait, "wait for the coordinator Job to complete")
	fs.BoolVar(&opts.Logs, "logs", opts.Logs, "print coordinator logs after waiting")
	fs.BoolVar(&opts.DryRun, "dry-run", opts.DryRun, "print Kubernetes JSON and do not apply it")
	fs.BoolVar(&opts.Replace, "replace", opts.Replace, "delete an existing coordinator Job for the run before apply")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	switch fs.NArg() {
	case 0:
		return errors.New("run JSON path is required")
	case 1:
		opts.InputPath = fs.Arg(0)
	default:
		return fmt.Errorf("unexpected argument %q", fs.Arg(1))
	}
	if envURLs := os.Getenv("SPORE_AGENT_URLS"); envURLs != "" {
		if err := opts.AgentURLs.Set(envURLs); err != nil {
			return err
		}
	}
	if len(opts.AgentURLs) == 0 {
		_ = opts.AgentURLs.Set(defaultAgentURL(opts.Namespace))
	}

	resources, names, runID, err := buildSubmitResourcesFromOptions(opts)
	if err != nil {
		return err
	}
	payload, err := json.MarshalIndent(resources, "", "  ")
	if err != nil {
		return fmt.Errorf("encode Kubernetes resources: %w", err)
	}
	payload = append(payload, '\n')

	if opts.DryRun {
		_, err := stdout.Write(payload)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout+30*time.Second)
	defer cancel()
	kubectl := kubectlRunner{Path: opts.Kubectl, Stdout: stdout, Stderr: stderr}
	if opts.Replace {
		if err := kubectl.Run(ctx, nil, "delete", "job", "-n", opts.Namespace, names.Job, "--ignore-not-found"); err != nil {
			return err
		}
	}
	if err := kubectl.Run(ctx, payload, "apply", "-f", "-"); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "submitted run %s as job/%s\n", runID, names.Job)
	if !opts.Wait {
		return nil
	}
	if err := waitForJob(ctx, kubectl, opts.Namespace, names.Job, opts.Timeout); err != nil {
		if opts.Logs {
			_ = kubectl.Run(ctx, nil, "logs", "-n", opts.Namespace, "job/"+names.Job, "--all-containers=true")
		}
		return err
	}
	if opts.Logs {
		return kubectl.Run(ctx, nil, "logs", "-n", opts.Namespace, "job/"+names.Job, "--all-containers=true")
	}
	return nil
}

func buildSubmitResourcesFromOptions(opts submitOptions) (resourceList, resourceNames, string, error) {
	if opts.InputPath == "" {
		return resourceList{}, resourceNames{}, "", errors.New("run JSON path is required")
	}
	runBytes, err := os.ReadFile(opts.InputPath)
	if err != nil {
		return resourceList{}, resourceNames{}, "", fmt.Errorf("read run: %w", err)
	}
	kind, err := detectSubmitRunKind(runBytes)
	if err != nil {
		return resourceList{}, resourceNames{}, "", err
	}

	switch kind {
	case submitRunKindBundle:
		run, err := fleet.DecodeBundleRun(bytes.NewReader(runBytes))
		if err != nil {
			return resourceList{}, resourceNames{}, "", err
		}
		resources, names, err := buildBundleSubmitResources(run, runBytes, opts)
		return resources, names, run.RunID, err
	case submitRunKindRun:
		run, err := fleet.DecodeRun(bytes.NewReader(runBytes))
		if err != nil {
			return resourceList{}, resourceNames{}, "", err
		}
		resources, names, err := buildSubmitResources(run, runBytes, opts)
		return resources, names, run.RunID, err
	default:
		return resourceList{}, resourceNames{}, "", fmt.Errorf("unsupported run document kind %q", kind)
	}
}

type submitRunKind string

const (
	submitRunKindBundle submitRunKind = "bundle"
	submitRunKindRun    submitRunKind = "run"
)

func detectSubmitRunKind(data []byte) (submitRunKind, error) {
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&fields); err != nil {
		return "", fmt.Errorf("%w: decode run input: %v", fleet.ErrInvalidContract, err)
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return "", fmt.Errorf("%w: run input must contain exactly one JSON document", fleet.ErrInvalidContract)
		}
		return "", fmt.Errorf("%w: decode trailing run input data: %v", fleet.ErrInvalidContract, err)
	}

	_, hasBundle := fields["bundle"]
	_, hasHostClass := fields["hostClass"]
	_, hasSource := fields["source"]
	_, hasPrepare := fields["prepare"]
	_, hasFork := fields["fork"]

	hasBundleRunFields := hasBundle || hasHostClass
	hasRunFields := hasSource || hasPrepare || hasFork
	if hasBundleRunFields && hasRunFields {
		return "", fmt.Errorf("%w: run input mixes bundle run fields with run fields", fleet.ErrInvalidContract)
	}
	if hasRunFields {
		return submitRunKindRun, nil
	}
	if hasBundleRunFields {
		return submitRunKindBundle, nil
	}
	return "", fmt.Errorf("%w: run input must include either run fields (source, prepare, fork) or bundle run fields (bundle, hostClass)", fleet.ErrInvalidContract)
}

type resourceNames struct {
	ConfigMap string
	Job       string
}

type submitPayload struct {
	RunID           string
	ConfigMapKey    string
	CoordinatorFlag string
	MountPath       string
	Bytes           []byte
}

func buildBundleSubmitResources(run fleet.BundleRun, runBytes []byte, opts submitOptions) (resourceList, resourceNames, error) {
	if err := run.Validate(); err != nil {
		return resourceList{}, resourceNames{}, err
	}
	return buildSubmitResourcesForPayload(submitPayload{
		RunID:           run.RunID,
		ConfigMapKey:    "bundle-run.json",
		CoordinatorFlag: "bundle-run",
		MountPath:       defaultBundleRunMountPath,
		Bytes:           runBytes,
	}, opts)
}

func buildSubmitResources(run fleet.Run, runBytes []byte, opts submitOptions) (resourceList, resourceNames, error) {
	if err := run.Validate(); err != nil {
		return resourceList{}, resourceNames{}, err
	}
	return buildSubmitResourcesForPayload(submitPayload{
		RunID:           run.RunID,
		ConfigMapKey:    "run.json",
		CoordinatorFlag: "run",
		MountPath:       defaultRunMountPath,
		Bytes:           runBytes,
	}, opts)
}

func buildSubmitResourcesForPayload(payload submitPayload, opts submitOptions) (resourceList, resourceNames, error) {
	if payload.RunID == "" {
		return resourceList{}, resourceNames{}, errors.New("run id must not be empty")
	}
	if payload.ConfigMapKey == "" || payload.CoordinatorFlag == "" || payload.MountPath == "" {
		return resourceList{}, resourceNames{}, errors.New("submit payload is incomplete")
	}
	if opts.Namespace == "" {
		return resourceList{}, resourceNames{}, errors.New("namespace must not be empty")
	}
	if opts.Image == "" {
		return resourceList{}, resourceNames{}, errors.New("image must not be empty")
	}
	if opts.ImagePullPolicy == "" {
		return resourceList{}, resourceNames{}, errors.New("image pull policy must not be empty")
	}
	if len(opts.AgentURLs) == 0 {
		return resourceList{}, resourceNames{}, errors.New("at least one agent URL is required")
	}
	if opts.Timeout <= 0 {
		return resourceList{}, resourceNames{}, errors.New("timeout must be positive")
	}

	names := resourceNames{
		ConfigMap: kubernetesName("spore-run", payload.RunID),
		Job:       kubernetesName("spore-coordinator", payload.RunID),
	}
	labels := map[string]string{
		"app.kubernetes.io/name":    "spore-coordinator",
		"app.kubernetes.io/part-of": "sporevm-k8s",
		"sporevm.io/run":            kubernetesName("run", payload.RunID),
	}
	list := resourceList{
		APIVersion: "v1",
		Kind:       "List",
		Items: []any{
			configMap{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Metadata: metadata{
					Name:      names.ConfigMap,
					Namespace: opts.Namespace,
					Labels:    labels,
				},
				Data: map[string]string{payload.ConfigMapKey: string(payload.Bytes)},
			},
			coordinatorJob(names, labels, opts, payload),
		},
	}
	return list, names, nil
}

func coordinatorJob(names resourceNames, labels map[string]string, opts submitOptions, payload submitPayload) job {
	args := []string{
		"--" + payload.CoordinatorFlag + "=" + payload.MountPath,
		"--result-store-root=" + opts.ResultStoreRoot,
		"--timeout=" + opts.Timeout.String(),
	}
	for _, url := range opts.AgentURLs {
		args = append(args, "--agent-url="+url)
	}
	return job{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Metadata: metadata{
			Name:      names.Job,
			Namespace: opts.Namespace,
			Labels:    labels,
		},
		Spec: jobSpec{
			BackoffLimit:            intPtr(0),
			TTLSecondsAfterFinished: intPtr(3600),
			Template: podTemplate{
				Metadata: metadata{Labels: labels},
				Spec: podSpec{
					RestartPolicy:                "Never",
					AutomountServiceAccountToken: boolPtr(false),
					NodeSelector: map[string]string{
						"kubernetes.io/arch": "arm64",
						"sporevm.io/agent":   "true",
						"sporevm.io/arch":    "aarch64",
						"sporevm.io/backend": "kvm",
					},
					Tolerations: []toleration{{
						Key:      "sporevm.io/agent",
						Operator: "Equal",
						Value:    "true",
						Effect:   "NoSchedule",
					}},
					Containers: []container{{
						Name:            "coordinator",
						Image:           opts.Image,
						ImagePullPolicy: opts.ImagePullPolicy,
						Command:         []string{"/usr/local/bin/spore-coordinator"},
						Args:            args,
						Resources: resourceRequirements{
							Requests: map[string]string{"cpu": "25m", "memory": "64Mi"},
							Limits:   map[string]string{"cpu": "500m", "memory": "512Mi"},
						},
						VolumeMounts: []volumeMount{
							{Name: "run", MountPath: "/etc/sporevm/run", ReadOnly: true},
							{Name: "coordinator-results", MountPath: "/var/lib/sporevm/coordinator-results"},
						},
					}},
					Volumes: []volume{
						{Name: "run", ConfigMap: &configMapVolume{Name: names.ConfigMap}},
						{Name: "coordinator-results", EmptyDir: &emptyDirVolume{}},
					},
				},
			},
		},
	}
}

func kubernetesName(prefix, raw string) string {
	const maxName = 63
	clean := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return '-'
	}, raw)
	clean = strings.Trim(clean, "-")
	if clean == "" {
		clean = "run"
	}
	sum := sha256.Sum256([]byte(raw))
	suffix := hex.EncodeToString(sum[:4])
	base := prefix + "-" + clean
	if len(base)+1+len(suffix) > maxName {
		base = base[:maxName-1-len(suffix)]
		base = strings.TrimRight(base, "-")
	}
	return base + "-" + suffix
}

type kubectlRunner struct {
	Path   string
	Stdout io.Writer
	Stderr io.Writer
}

func (r kubectlRunner) Run(ctx context.Context, stdin []byte, args ...string) error {
	cmd := exec.CommandContext(ctx, r.Path, args...)
	cmd.Stdout = r.Stdout
	cmd.Stderr = r.Stderr
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", r.Path, strings.Join(args, " "), err)
	}
	return nil
}

func (r kubectlRunner) Output(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, r.Path, args...)
	cmd.Stderr = r.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", r.Path, strings.Join(args, " "), err)
	}
	return out, nil
}

func waitForJob(ctx context.Context, kubectl kubectlRunner, namespace, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		out, err := kubectl.Output(ctx, "get", "job", "-n", namespace, name, "-o", "json")
		if err != nil {
			return err
		}
		state, err := jobTerminalState(out)
		if err != nil {
			return err
		}
		switch {
		case state.Complete:
			return nil
		case state.Failed:
			return fmt.Errorf("coordinator job failed: %s", state.Message)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for job/%s after %s", name, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

type terminalState struct {
	Complete bool
	Failed   bool
	Message  string
}

func jobTerminalState(data []byte) (terminalState, error) {
	var job struct {
		Status struct {
			Conditions []struct {
				Type    string `json:"type"`
				Status  string `json:"status"`
				Reason  string `json:"reason"`
				Message string `json:"message"`
			} `json:"conditions"`
		} `json:"status"`
	}
	if err := json.Unmarshal(data, &job); err != nil {
		return terminalState{}, fmt.Errorf("decode job status: %w", err)
	}
	for _, condition := range job.Status.Conditions {
		if condition.Status != "True" {
			continue
		}
		if condition.Type == "Complete" {
			return terminalState{Complete: true}, nil
		}
		if condition.Type == "Failed" {
			message := condition.Message
			if message == "" {
				message = condition.Reason
			}
			if message == "" {
				message = "failed condition is true"
			}
			return terminalState{Failed: true, Message: message}, nil
		}
	}
	return terminalState{}, nil
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func defaultAgentURL(namespace string) string {
	return "http://spore-agent." + namespace + ".svc.cluster.local:8080"
}

func intPtr(v int) *int {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

type resourceList struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Items      []any  `json:"items"`
}

type metadata struct {
	Name      string            `json:"name,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type configMap struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   metadata          `json:"metadata"`
	Data       map[string]string `json:"data"`
}

type job struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   metadata `json:"metadata"`
	Spec       jobSpec  `json:"spec"`
}

type jobSpec struct {
	BackoffLimit            *int        `json:"backoffLimit,omitempty"`
	TTLSecondsAfterFinished *int        `json:"ttlSecondsAfterFinished,omitempty"`
	Template                podTemplate `json:"template"`
}

type podTemplate struct {
	Metadata metadata `json:"metadata,omitempty"`
	Spec     podSpec  `json:"spec"`
}

type podSpec struct {
	RestartPolicy                string            `json:"restartPolicy"`
	AutomountServiceAccountToken *bool             `json:"automountServiceAccountToken,omitempty"`
	NodeSelector                 map[string]string `json:"nodeSelector,omitempty"`
	Tolerations                  []toleration      `json:"tolerations,omitempty"`
	Containers                   []container       `json:"containers"`
	Volumes                      []volume          `json:"volumes,omitempty"`
}

type toleration struct {
	Key      string `json:"key"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
	Effect   string `json:"effect"`
}

type container struct {
	Name            string               `json:"name"`
	Image           string               `json:"image"`
	ImagePullPolicy string               `json:"imagePullPolicy,omitempty"`
	Command         []string             `json:"command,omitempty"`
	Args            []string             `json:"args,omitempty"`
	Resources       resourceRequirements `json:"resources,omitempty"`
	VolumeMounts    []volumeMount        `json:"volumeMounts,omitempty"`
}

type resourceRequirements struct {
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
}

type volumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

type volume struct {
	Name      string           `json:"name"`
	ConfigMap *configMapVolume `json:"configMap,omitempty"`
	EmptyDir  *emptyDirVolume  `json:"emptyDir,omitempty"`
}

type configMapVolume struct {
	Name string `json:"name"`
}

type emptyDirVolume struct{}
