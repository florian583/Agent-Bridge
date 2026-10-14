# Shared worker daemon

This fork adds an opt-in, POSIX per-user runtime. Existing standalone MCP operation remains available.

## Guarantees and boundaries

- One daemon owns adapters and worker subprocesses. MCP clients are disposable connections. Closing a Claude/Codex client does not cancel accepted work.
- SQLite WAL is authoritative. Task ownership and the conversation-scoped request key commit before worker execution. Disk failure refuses new work.
- A first dispatch requires a UUID `request_id`. Repeat the same key and arguments after a lost response. Deduplication lasts while the retained task/request exists, including daemon restart; not forever.
- Real conversation UUIDs isolate tasks even when callers share a worktree. Task/session reads and mutations are checked at the daemon boundary. This prevents accidental cross-talk, not hostile programs running as the same OS user.
- Nested worker clients cannot dispatch, cancel, end sessions, or alter preferences.
- SIGTERM/SIGINT starts draining: reject new dispatches, finish accepted work, then close. Do not force-kill the daemon for upgrades.
- Daemon crash/reboot is different from client restart. In-flight tasks become failed/interrupted with uncertain side effects. No automatic prompt replay or claim that live ACP pipes survive daemon death.

## Launch and connect

Use a separate installation and runtime home from legacy bridges. Copy your own `agents.toml` and optional `worker-instructions.md` into that private home; do not copy `state.json` or `pids.json` from running legacy instances.

```sh
agent-bridge-daemon serve --home "$HOME/.agent-bridge-daemon"
agent-bridge-daemon status --home "$HOME/.agent-bridge-daemon"
```

The socket is `daemon.sock` in that home (directory 0700, socket 0600). Long Unix-socket paths are rejected. Only one daemon can acquire that home's lock.

Configure each MCP client to run this installation's `agent-bridge`, with:

```json
{
  "AGENT_BRIDGE_DAEMON_SOCKET": "/absolute/private/runtime/daemon.sock",
  "AGENT_BRIDGE_SKIP_SKILL": "1"
}
```

For Claude, add a user-level `SessionStart` command invoking this installation's `agent-bridge-bind`, with a two-second timeout. It records the host-provided UUID against the nearest actual Claude process's PID **and creation time**. The MCP client resolves that binding on every request, so clear/new conversations in one process change scope and resuming preserves the original scope. Worktree paths and tmux names are not identities. Hook failure does not block Claude; dispatch fails clearly if binding is unavailable.

For hosts that expose a stable ID, `AGENT_BRIDGE_CONVERSATION_ID` may explicitly carry it. `CODEX_THREAD_ID` is also recognized when present. Do not invent a shared constant for every client. These are integration mechanisms, not proof every host supplies that environment variable.

## Storage, logs, and notifications

- `daemon.sqlite3`: authoritative tasks, sessions, request keys and scoped transition sequence. WAL/full synchronization; DB connections always close.
- `state.json`: compatibility projection, never a second writable authority.
- `conversations/<UUID>/index.json` and `<task-id>.json`: watchable metadata created when dispatch is persisted. A lost channel wake-up can be recovered by one file monitor.
- `transcripts/`: provider events, flushed periodically every two seconds, including quiet streams.
- `logs/daemon.log`: rotating operational log, 2 MB plus three backups. Lifecycle logs do not contain full prompts.

`integrations/bridge-live` contains the Studio compatibility integration. Its shared service reads both legacy and daemon ledgers; daemon conversation ownership registers tasks without a PostToolUse hook. Old clients drain unchanged. Terminal notification policy stays DONE/FAILED, at-most-once attempt, batched; no ACK required, no automatic delivery retries. Restart must not replay historical claimed events. Thin per-conversation relays remain necessary for independent host channels.

## Usage and performance

`inspect_task(task_id, cursor?, limit=12)` is the on-demand live view. First call returns the most recent visible tool calls, commands, statuses, output excerpts and errors. Reuse `next_cursor` to obtain only newer events. It includes elapsed/silent time and available usage, without waiting for completion. `quiet` means no recent output, not proof of a stuck process. Prompt/thinking streams are omitted. Provider omissions remain omissions; redaction is best effort. Reads are capped at 128 KiB and visible event output at 16 KiB, with truncation/reset indicators. No Git scan, provider probe, separate watcher or notification retry is triggered. Poll deliberately (normally no faster than 15-30 seconds). Existing `get_transcript` remains available for deeper paged historical investigation.

- Kimi observer understands current and legacy terminal schemas. Current-task native failure changes daemon task status to failed, not completed-with-warning.
- ACP context `used/size` is retained as telemetry, not counted as consumed tokens. Actual response counters take precedence; cancellation preserves available partial counters.
- Kimi `usage.record` counters are read incrementally, with bounded reads, partial-line buffering and event replay deduplication across rotation. Only main-agent records after task start count. Input totals include uncached + cache-read + cache-write; cache is not added again. Native message copies are not summed a second time.
- Usage becomes visible when the provider emits its record, not necessarily token by token. Missing/unsupported usage stays unknown. Large/unparseable tails are marked partial. Current native wire has no unique ID on `usage.record`; byte-equivalent events deduplicate conservatively.
- One shared single-flight worker-probe cache (30 s); status reads never trigger provider probes. Legacy Bridge Live snapshot caches remain intact.
- This release does not replace workspace before/after scans or add worker-runner crash survival. Those remain separate follow-up work, not implied guarantees.

## Rollout and rollback

1. Test an isolated runtime with fake workers and client-process termination.
2. Start the production daemon with copied provider configuration; verify health and binding.
3. Switch **new** MCP clients only. Existing clients and workers keep their installed runtime until they finish.
4. Enable the dual-ledger observer before new daemon dispatches.
5. To roll back, restore the previous MCP command for new clients. Let daemon work finish; drain before changing its installed files.

Do not replace a running release in place. Store a versioned release alongside the old install. Launchd's exit timeout must permit drain, but logout/reboot may still interrupt work.

Validation includes same-directory scope isolation, storage failure before launch, lost response/retry, durable restart keys, killed stdio client, PID-reuse-safe binding, 30 clients/50 tasks, native log replay/rotation, and no-replay Bridge Live delivery.
