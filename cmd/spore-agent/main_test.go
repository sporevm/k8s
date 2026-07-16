package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResetSporeRuntimeDirRetainsAuthorityOnly(t *testing.T) {
	workRoot := t.TempDir()
	runtimeRoot := filepath.Join(workRoot, "runtime")
	if err := os.MkdirAll(filepath.Join(runtimeRoot, "vms", "stale-sandbox"), 0o700); err != nil {
		t.Fatalf("create stale VM state: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(runtimeRoot, "forks", "stale-batch"), 0o700); err != nil {
		t.Fatalf("create stale fork state: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(runtimeRoot, "leases"), 0o700); err != nil {
		t.Fatalf("create stale lease state: %v", err)
	}
	keyPath := filepath.Join(runtimeRoot, sporeRuntimeAuthorityFile)
	key := []byte("retained authority")
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		t.Fatalf("write authority: %v", err)
	}

	if err := resetSporeRuntimeDir(workRoot, runtimeRoot); err != nil {
		t.Fatalf("resetSporeRuntimeDir: %v", err)
	}
	entries, err := os.ReadDir(runtimeRoot)
	if err != nil {
		t.Fatalf("read runtime root: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != sporeRuntimeAuthorityFile {
		t.Fatalf("runtime entries = %v, want only %s", entries, sporeRuntimeAuthorityFile)
	}
	got, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read retained authority: %v", err)
	}
	if string(got) != string(key) {
		t.Fatalf("retained authority = %q, want %q", got, key)
	}
}

func TestResetSporeRuntimeDirRejectsNonRegularAuthority(t *testing.T) {
	workRoot := t.TempDir()
	runtimeRoot := filepath.Join(workRoot, "runtime")
	invalidAuthority := filepath.Join(runtimeRoot, sporeRuntimeAuthorityFile)
	if err := os.MkdirAll(invalidAuthority, 0o700); err != nil {
		t.Fatalf("create invalid authority: %v", err)
	}

	if err := resetSporeRuntimeDir(workRoot, runtimeRoot); err == nil {
		t.Fatal("resetSporeRuntimeDir succeeded with directory authority")
	}

	if err := os.RemoveAll(invalidAuthority); err != nil {
		t.Fatalf("remove invalid authority: %v", err)
	}
	target := filepath.Join(runtimeRoot, "authority-target")
	if err := os.WriteFile(target, []byte("authority"), 0o600); err != nil {
		t.Fatalf("write authority target: %v", err)
	}
	if err := os.Symlink(target, invalidAuthority); err != nil {
		t.Fatalf("symlink authority: %v", err)
	}
	if err := resetSporeRuntimeDir(workRoot, runtimeRoot); err == nil {
		t.Fatal("resetSporeRuntimeDir succeeded with symlink authority")
	}
}

func TestResetSporeRuntimeDirRejectsWorkRootAndOutsidePath(t *testing.T) {
	root := t.TempDir()
	workRoot := filepath.Join(root, "work")
	outside := filepath.Join(root, "outside")
	if err := os.MkdirAll(workRoot, 0o755); err != nil {
		t.Fatalf("create work root: %v", err)
	}
	if err := os.MkdirAll(outside, 0o700); err != nil {
		t.Fatalf("create outside root: %v", err)
	}
	sentinel := filepath.Join(outside, "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}

	if err := resetSporeRuntimeDir(workRoot, workRoot); err == nil {
		t.Fatal("resetSporeRuntimeDir accepted work root")
	}
	if err := resetSporeRuntimeDir(workRoot, outside); err == nil {
		t.Fatal("resetSporeRuntimeDir accepted outside root")
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("outside sentinel was removed: %v", err)
	}
}

func TestEffectiveSlotsForCgroupClampsByMemoryLimit(t *testing.T) {
	limit := filepath.Join(t.TempDir(), "memory.max")
	if err := os.WriteFile(limit, []byte("2147483648\n"), 0o644); err != nil {
		t.Fatalf("write cgroup limit: %v", err)
	}

	slots, limited, err := effectiveSlotsForCgroup(10, defaultChildMemoryBytes, defaultMemoryReserveBytes, []string{limit})
	if err != nil {
		t.Fatalf("effectiveSlotsForCgroup: %v", err)
	}
	if slots != 3 || !limited {
		t.Fatalf("slots=%d limited=%v, want slots=3 limited=true", slots, limited)
	}
}

func TestEffectiveSlotsForCgroupFailsWhenOneChildCannotFit(t *testing.T) {
	limit := filepath.Join(t.TempDir(), "memory.max")
	if err := os.WriteFile(limit, []byte("536870912\n"), 0o644); err != nil {
		t.Fatalf("write cgroup limit: %v", err)
	}

	slots, limited, err := effectiveSlotsForCgroup(10, defaultChildMemoryBytes, defaultMemoryReserveBytes, []string{limit})
	if err != nil {
		t.Fatalf("effectiveSlotsForCgroup: %v", err)
	}
	if slots != 0 || !limited {
		t.Fatalf("slots=%d limited=%v, want slots=0 limited=true", slots, limited)
	}
}

func TestEffectiveSlotsForCgroupIgnoresUnlimitedMemory(t *testing.T) {
	limit := filepath.Join(t.TempDir(), "memory.max")
	if err := os.WriteFile(limit, []byte("max\n"), 0o644); err != nil {
		t.Fatalf("write cgroup limit: %v", err)
	}

	slots, limited, err := effectiveSlotsForCgroup(10, defaultChildMemoryBytes, defaultMemoryReserveBytes, []string{limit})
	if err != nil {
		t.Fatalf("effectiveSlotsForCgroup: %v", err)
	}
	if slots != 10 || limited {
		t.Fatalf("slots=%d limited=%v, want slots=10 limited=false", slots, limited)
	}
}

func TestEffectiveSlotsForCgroupUsesCurrentCgroupBeforeRoot(t *testing.T) {
	dir := t.TempDir()
	mountRoot := filepath.Join(dir, "sys", "fs", "cgroup")
	currentRoot := filepath.Join(mountRoot, "kubepods.slice", "pod.slice", "cri.scope")
	if err := os.MkdirAll(currentRoot, 0o755); err != nil {
		t.Fatalf("create cgroup: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mountRoot, "memory.max"), []byte("max\n"), 0o644); err != nil {
		t.Fatalf("write root cgroup limit: %v", err)
	}
	if err := os.WriteFile(filepath.Join(currentRoot, "memory.max"), []byte("2147483648\n"), 0o644); err != nil {
		t.Fatalf("write current cgroup limit: %v", err)
	}
	procCgroup := filepath.Join(dir, "self-cgroup")
	if err := os.WriteFile(procCgroup, []byte("0::/kubepods.slice/pod.slice/cri.scope\n"), 0o644); err != nil {
		t.Fatalf("write proc cgroup: %v", err)
	}

	files := cgroupMemoryLimitFileCandidates(procCgroup, mountRoot, []string{filepath.Join(mountRoot, "memory.max")})
	slots, limited, err := effectiveSlotsForCgroup(10, defaultChildMemoryBytes, defaultMemoryReserveBytes, files)
	if err != nil {
		t.Fatalf("effectiveSlotsForCgroup: %v", err)
	}
	if slots != 3 || !limited {
		t.Fatalf("slots=%d limited=%v, want slots=3 limited=true", slots, limited)
	}
}

func TestCgroupMemoryLimitBytesFallsBackToNextFile(t *testing.T) {
	dir := t.TempDir()
	limit := filepath.Join(dir, "memory.limit_in_bytes")
	if err := os.WriteFile(limit, []byte("1073741824\n"), 0o644); err != nil {
		t.Fatalf("write cgroup limit: %v", err)
	}

	got, ok, err := cgroupMemoryLimitBytes([]string{filepath.Join(dir, "missing"), limit})
	if err != nil {
		t.Fatalf("cgroupMemoryLimitBytes: %v", err)
	}
	if got != 1073741824 || !ok {
		t.Fatalf("limit=%d ok=%v, want limit=1073741824 ok=true", got, ok)
	}
}
