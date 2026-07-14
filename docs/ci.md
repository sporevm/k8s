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
