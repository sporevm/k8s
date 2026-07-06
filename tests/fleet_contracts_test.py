from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from fleet_contracts import (  # noqa: E402
    ContractError,
    attempt_key,
    derive_shard_ranges,
    load_json,
    reject_overlapping_ranges,
    terminal_result_key,
    validate_agent_status,
    validate_attempt_result,
    validate_benchmark_summary,
    validate_bundle_run,
    validate_complete_coverage,
    validate_examples,
    validate_run,
    validate_shard_lease,
)


class FleetContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.examples = ROOT / "examples" / "fleet"
        self.run = load_json(self.examples / "run-rails-rspec.json")
        self.bundle_run = load_json(self.examples / "bundle-run-1000.json")
        self.lease = load_json(self.examples / "shard-lease.json")

    def test_examples_validate(self) -> None:
        validate_examples(ROOT)

    def test_run_example_validates(self) -> None:
        validate_run(self.run)

    def test_run_requires_source_image(self) -> None:
        run = copy.deepcopy(self.run)
        run["source"]["image"] = ""

        with self.assertRaisesRegex(ContractError, "source.image"):
            validate_run(run)

    def test_run_children_must_fit_fork_count(self) -> None:
        run = copy.deepcopy(self.run)
        run["children"]["start"] = 1

        with self.assertRaisesRegex(ContractError, "fork.count"):
            validate_run(run)

    def test_run_requires_complete_capture_trigger(self) -> None:
        run = copy.deepcopy(self.run)
        del run["prepare"]["readyMarker"]

        with self.assertRaisesRegex(ContractError, "readyMarker"):
            validate_run(run)

    def test_run_rejects_unsupported_capture_signal(self) -> None:
        run = copy.deepcopy(self.run)
        run["prepare"]["captureSignal"] = "TERM"

        with self.assertRaisesRegex(ContractError, "captureSignal"):
            validate_run(run)

    def test_1000_child_run_derives_non_overlapping_shards(self) -> None:
        ranges = derive_shard_ranges(self.bundle_run)

        self.assertEqual(10, len(ranges))
        self.assertEqual((0, 100), ranges[0])
        self.assertEqual((900, 1000), ranges[-1])
        validate_complete_coverage(ranges, 0, 1000)

    def test_rejects_overlapping_shard_ranges(self) -> None:
        with self.assertRaisesRegex(ContractError, "must not overlap"):
            reject_overlapping_ranges([(0, 100), (99, 200)])

    def test_attempt_and_terminal_keys_include_global_child_identity(self) -> None:
        key = attempt_key(
            self.bundle_run["runID"],
            self.bundle_run["bundle"]["digest"],
            742,
            2,
        )

        self.assertEqual(
            "ruby-counter-20260620/"
            "sha256:1111111111111111111111111111111111111111111111111111111111111111/"
            "children/742/attempts/2",
            key,
        )
        self.assertEqual(
            "ruby-counter-20260620/children/742/terminal.json",
            terminal_result_key(self.bundle_run["runID"], 742),
        )

    def test_missing_bundle_digest_is_rejected(self) -> None:
        run = copy.deepcopy(self.bundle_run)
        del run["bundle"]["digest"]

        with self.assertRaisesRegex(ContractError, "bundle.*digest"):
            validate_bundle_run(run)

    def test_missing_child_range_is_rejected(self) -> None:
        run = copy.deepcopy(self.bundle_run)
        del run["children"]["count"]

        with self.assertRaisesRegex(ContractError, "children.*count"):
            validate_bundle_run(run)

    def test_ambiguous_host_class_is_rejected(self) -> None:
        run = copy.deepcopy(self.bundle_run)
        del run["hostClass"]["cpuProfile"]

        with self.assertRaisesRegex(ContractError, "hostClass.*cpuProfile"):
            validate_bundle_run(run)

    def test_unsafe_retry_settings_are_rejected(self) -> None:
        run = copy.deepcopy(self.bundle_run)
        run["retryPolicy"]["rerunCommittedChildren"] = True

        with self.assertRaisesRegex(ContractError, "rerunCommittedChildren"):
            validate_bundle_run(run)

    def test_result_store_requires_bucket_and_prefix(self) -> None:
        run = copy.deepcopy(self.bundle_run)
        run["resultStore"] = "s3:///missing-bucket/"

        with self.assertRaisesRegex(ContractError, "resultStore"):
            validate_bundle_run(run)

    def test_shard_lease_must_fit_run_range(self) -> None:
        lease = copy.deepcopy(self.lease)
        lease["childStart"] = 950
        lease["childCount"] = 100

        with self.assertRaisesRegex(ContractError, "outside the run"):
            validate_shard_lease(lease, self.bundle_run)

    def test_shard_lease_attempt_budget_cannot_exceed_run_policy(self) -> None:
        lease = copy.deepcopy(self.lease)
        lease["attemptBudget"] = 3

        with self.assertRaisesRegex(ContractError, "retry budget"):
            validate_shard_lease(lease, self.bundle_run)

    def test_attempt_result_child_must_fit_shard_range(self) -> None:
        result = load_json(self.examples / "attempt-result.json")
        result["childID"] = 800

        with self.assertRaisesRegex(ContractError, "outside the shard lease"):
            validate_attempt_result(result, self.lease)

    def test_skipped_terminal_result_requires_terminal_uri(self) -> None:
        result = load_json(self.examples / "attempt-result.json")
        result["status"] = "skipped-terminal-exists"
        result["terminal"] = False
        del result["resultURI"]

        with self.assertRaisesRegex(ContractError, "terminal"):
            validate_attempt_result(result, self.lease)

    def test_attempt_result_rejects_non_s3_terminal_uri(self) -> None:
        result = load_json(self.examples / "attempt-result.json")
        result["resultURI"] = "https://example.com/result.json"

        with self.assertRaisesRegex(ContractError, "s3 JSON object URI"):
            validate_attempt_result(result, self.lease)

    def test_attempt_result_rejects_empty_bucket_terminal_uri(self) -> None:
        result = load_json(self.examples / "attempt-result.json")
        result["resultURI"] = "s3:///terminal.json"

        with self.assertRaisesRegex(ContractError, "s3 JSON object URI"):
            validate_attempt_result(result, self.lease)

    def test_agent_cannot_be_healthy_under_critical_pressure(self) -> None:
        status = load_json(self.examples / "agent-status.json")
        status["pressure"]["disk"] = "critical"

        with self.assertRaisesRegex(ContractError, "critical pressure"):
            validate_agent_status(status)

    def test_benchmark_summary_must_match_run_child_count(self) -> None:
        summary = load_json(self.examples / "benchmark-summary-1000.json")
        summary["childCount"] = 999

        with self.assertRaisesRegex(ContractError, "childCount"):
            validate_benchmark_summary(summary, self.bundle_run)

    def test_benchmark_percentiles_must_be_ordered(self) -> None:
        summary = load_json(self.examples / "benchmark-summary-1000.json")
        summary["stagePercentilesMs"]["resume"]["p95"] = 1

        with self.assertRaisesRegex(ContractError, "percentiles"):
            validate_benchmark_summary(summary, self.bundle_run)


if __name__ == "__main__":
    unittest.main()
