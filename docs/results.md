# Result storage

Batch runs store attempt and terminal documents under the `s3://` prefix in the
run contract:

```text
children/<child-id>/attempts/<attempt-id>.json
children/<child-id>/terminal.json
```

The local backend maps those URIs into a directory and remains useful for
tests and single-node smokes. Production cells should configure the agent and
coordinator with the S3 backend. The AWS SDK uses its default credential chain,
so environment-specific identity belongs on the Kubernetes service accounts in
the private ops repository.

```bash
sporectl submit \
  --result-store-backend=s3 \
  --result-store-region=example-region-1 \
  --service-account=spore-coordinator \
  sporevm-run.json
```

The agent chart has matching `agent.resultStore.backend` and
`agent.resultStore.region` values. An S3-compatible endpoint can use
`endpoint` and `pathStyle`; ordinary AWS S3 should leave both unset.

Attempt and terminal writes send `If-None-Match: *`. The first writer creates
the object, while an existing key returns a precondition failure and is never
overwritten. Agents need `GetObject` and `PutObject` only for the run prefixes
they execute; no list or delete permission is required by the result path.
