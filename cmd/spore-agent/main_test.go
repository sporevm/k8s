package main

import (
	"os"
	"path/filepath"
	"testing"
)

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
