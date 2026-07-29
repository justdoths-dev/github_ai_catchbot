# Cline Worker Harness Adapter

This file is a thin Cline execution adapter. The Task Packet, not this rule,
owns the concrete provider, model, reasoning effort, task scope, allowed files,
command authority, and validation scope.

## Required Cline Execution

1. Read the root and any scoped `AGENTS.md` files before acting, then read the
   full Task Packet.
2. Verify the Task Packet SHA-256 when the Task Packet supplies one. Stop when a
   supplied checksum cannot be verified or the required task authority is absent.
3. Record executor, provider, provider_route, `exact_model_id` or
   `model_id_unavailable`, display model name, reasoning effort, client surface,
   Plan/Act mode, auto_approve_state, and Cline version when applicable.
4. Verify the required branch, HEAD, local-origin relation, and initial
   worktree/index/untracked state before editing.
5. Enforce the Task Packet allowed-file list and command/path authority. Run only
   the authorized tests and validations.
6. For repository-modification tasks, default to `auto-approve=off`. A permitted
   exception must be task-specific and explicitly operator-approved; it cannot
   expand files, commands, network, secrets, runtime, DB, systemd, Git
   publication, or product authority.
7. Generate the external Review Bundle through the existing lifecycle and return
   exactly one worker outcome:

   ```text
   IMPLEMENTED_FOR_REVIEW
   BLOCKED
   INTERRUPTED
   TESTS_FAILED
   ```

8. Never stage, commit, or push.

Unsupported or unobservable Cline capability assumptions are stop conditions.
Do not substitute them with provider/model fallback, automatic dirty-state
handoff, or an altered Task Packet.
