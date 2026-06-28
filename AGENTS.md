# AGENTS

- The plan of record is `docs/plans/design.md`.
- Keep SporeVM's core contract intact: Kubernetes adapts a cell; SporeVM
  verifies, materializes, and resumes a selected child.
- Do not model one Kubernetes object per child. Use aggregate run state, compact
  status, object-storage results, and metrics.
- Treat host compatibility as an admission-time invariant, not a best-effort
  retry behavior.
- Prefer conventional commit messages.
