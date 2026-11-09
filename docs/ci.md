# CI submission

CI uses the normal batch command; there is no separate CI execution API:

```bash
sporectl submit \
  --buildkite \
  --result-url-prefix "$SPOREVM_RESULT_BROWSER_URL" \
  sporevm-run.json
```

`submit` creates one ConfigMap and one coordinator Job for the logical run,
waits for aggregate completion, prints the coordinator logs, and exits with the
aggregate result. `--buildkite` parses the final runtime report and posts a
stable annotation with child, attempt, success, and failure counts. When
`--result-url-prefix` is present, the annotation links to the external result
browser; otherwise it shows the run's result-store URI.

The CI environment must already have Kubernetes credentials and the
`buildkite-agent` command. Keep cluster credentials, result locations, and
environment-specific run documents in the private ops repository.

For a warm multi-service test environment, start the application, database and
cache in one guest and checkpoint only after readiness and fixtures are ready.
`source` submissions capture that parent once; `checkpoint` submissions reuse
a native parent already published to a store. Each child receives generation
JSON with `parallel_index` and `parallel_count` for disjoint suite selection.
Omit `children.command` to resume the saved warm process, as in the Rails
example; supply it only to start a replacement command in the restored guest.
Children restore the same parent without sharing subsequent guest disk or RAM
writes. Committed children are skipped on retry.

`sporectl submit` detects either source or native-checkpoint input. Old `bundle`
inputs are rejected. The current execution boundary is one agent/node, even
when Kubernetes has several agent pods. Separate agent handoff and cross-node
acceptance are required before claiming distributed test sharding. External
service state is not forked; put it in the guest or define a separate isolation
contract. Named live VM forks do not gain networking from this migration.
