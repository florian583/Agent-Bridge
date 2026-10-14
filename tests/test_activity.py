import json

from agent_bridge.activity import READ_LIMIT, task_activity
from agent_bridge.models import Task
from agent_bridge.paths import transcript_path
from agent_bridge.transcript import append_event, flush_session


def task():
    return Task(task_id="task-test", session_id="session-test", agent="kimi", message="private prompt", cwd="/tmp",
                created_at="2026-01-02T00:00:00+00:00", started_at="2026-01-02T00:00:00+00:00")


def test_pending_live_calls_cursor_flush_and_redaction(tmp_path):
    t = task()
    append_event(t.session_id, "tool_call", {"title": "Run tests", "input": 'API_KEY="dontshow" pnpm test'}, tmp_path)
    append_event(t.session_id, "thought_chunk", {"text": "private reasoning"}, tmp_path)
    append_event(t.session_id, "prompt_sent", {"text": "private prompt"}, tmp_path)
    first = task_activity(t, tmp_path)
    assert len(first["events"]) == 1
    assert "dontshow" not in json.dumps(first)
    assert "private" not in json.dumps(first)
    flush_session(t.session_id, tmp_path)
    append_event(t.session_id, "tool_call_update", {"title": "tests", "status": "completed"}, tmp_path)
    second = task_activity(t, tmp_path, first["next_cursor"])
    assert len(second["events"]) == 1
    assert second["events"][0]["status"] == "completed"
    assert not task_activity(t, tmp_path, second["next_cursor"])["events"]


def test_paging_limits_and_task_time_boundaries(tmp_path):
    t = task()
    path = transcript_path(t.session_id, tmp_path)
    path.parent.mkdir(parents=True)
    rows = [{"ts": "2026-01-01T00:00:00+00:00", "type": "tool_call", "data": {"title": "OLD"}}]
    rows += [{"ts": "2026-01-02T00:00:00+00:00", "type": "tool_call", "data": {"title": str(i)}} for i in range(4)]
    rows += [{"ts": "2026-01-03T00:00:00+00:00", "type": "tool_call", "data": {"title": "NEXT TASK"}}]
    path.write_text("\n".join(map(json.dumps, rows))+"\n")
    t.finished_at = "2026-01-02T01:00:00+00:00"
    recent = task_activity(t, tmp_path, limit=2)
    assert [r["title"] for r in recent["events"]] == ["2", "3"]
    first = task_activity(t, tmp_path, cursor=f"{t.task_id}:0:0", limit=2)
    assert [r["title"] for r in first["events"]] == ["0", "1"]
    assert first["has_more"]
    second = task_activity(t, tmp_path, first["next_cursor"], limit=2)
    assert [r["title"] for r in second["events"]] == ["2", "3"]
    assert not second["has_more"]


def test_bounded_tail_partial_record_and_rotation(tmp_path):
    t = task()
    path = transcript_path(t.session_id, tmp_path)
    path.parent.mkdir(parents=True)
    valid = json.dumps({"ts": "2026-01-02T00:00:00+00:00", "type": "tool_call", "data": {"title": "visible"}})+"\n"
    path.write_text("x"*(READ_LIMIT*2)+"\n"+valid+valid[:10])
    first = task_activity(t, tmp_path)
    assert first["bytes_read"] <= READ_LIMIT
    assert first["history_truncated"] and first["skipped_records"]
    assert len(first["events"]) == 1
    with path.open("a") as stream:
        stream.write(valid[10:])
    second = task_activity(t, tmp_path, first["next_cursor"])
    assert len(second["events"]) == 1
    path.rename(path.with_suffix(".old"))
    path.write_text(valid)
    assert task_activity(t, tmp_path, second["next_cursor"])["cursor_reset"]


def test_codex_commands_visible_reasoning_hidden(tmp_path):
    t = task()
    for item in [{"type": "command_execution", "command": "pnpm test", "status": "in_progress"},
                 {"type": "reasoning", "text": "private"}]:
        append_event(t.session_id, "raw", {"payload": {"type": "item.started", "item": item}}, tmp_path)
    page = task_activity(t, tmp_path)
    assert len(page["events"]) == 1
    assert page["events"][0]["command"] == "pnpm test"


def test_utf8_output_stays_bounded_and_advances(tmp_path):
    t = task()
    append_event(t.session_id, "tool_call_update", {k: "𐀀"*2000 for k in
        ("title", "input", "output", "locations", "text", "error", "message", "decision", "status")}, tmp_path)
    page = task_activity(t, tmp_path)
    assert len(page["events"]) == 1
    assert not page["has_more"]
    assert len(json.dumps(page["events"], ensure_ascii=False).encode()) < 16000
