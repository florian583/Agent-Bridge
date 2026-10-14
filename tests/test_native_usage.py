import json

from agent_bridge.native_usage import KimiUsageReader


def sample(time=1000):
    return {"type": "usage.record", "agentId": "main", "time": time,
            "usageScope": "turn", "usage": {"inputOther": 10, "inputCacheRead": 20, "inputCacheCreation": 30, "output": 40}}


def test_incremental_partial_replay_rotation_and_task_boundary(tmp_path):
    file = tmp_path/"wire.jsonl"
    old, current = sample(1), sample(1000)
    encoded = json.dumps(current)+"\n"
    file.write_text(json.dumps(old)+"\n"+encoded[:10])
    reader = KimiUsageReader(file, 500)
    assert reader.read() == {}
    with file.open("a") as f:
        f.write(encoded[10:])
    usage = reader.read()
    assert usage["total_tokens"] == 100
    assert usage["input_tokens"] == 60
    assert reader.read() == usage
    with file.open("a") as f:
        f.write(encoded)
        f.write(json.dumps({"type": "agent.message.appended", "message": {"meta": {"usage": current["usage"]}}})+"\n")
    assert reader.read()["total_tokens"] == 100
    file.rename(tmp_path/"rotated.jsonl")
    file.write_text(encoded+json.dumps(sample(2000))+"\n")
    assert reader.read()["total_tokens"] == 200
    assert reader.read()["samples"] == 2


def test_context_unknown_counters_and_child_agents_not_counted(tmp_path):
    file = tmp_path/"wire.jsonl"
    rows = [{"type": "UsageUpdate", "used": 999999, "size": 1000000},
            {**sample(), "agentId": "child"}, {**sample(), "usage": {"inputOther": 10}}]
    file.write_text("\n".join(map(json.dumps, rows))+"\n")
    assert KimiUsageReader(file, 0).read() == {}


def test_failure_correlates_current_task_time(tmp_path):
    file = tmp_path/"wire.jsonl"
    file.write_text(json.dumps({"type": "agent.turn.ended", "time": 1, "outcome": "failed"})+"\n")
    reader = KimiUsageReader(file, 500)
    reader.read()
    assert reader.failure is None
    with file.open("a") as f:
        f.write(json.dumps({"type": "agent.turn.ended", "time": 1000, "outcome": "failed", "errorMessage": "quota"})+"\n")
    reader.read()
    assert reader.failure == "quota"


def test_invalid_timestamps_do_not_count(tmp_path):
    file = tmp_path/"wire.jsonl"
    file.write_text("\n".join(json.dumps(sample(t)) for t in [True, float("nan"), float("inf"), "1000"])+"\n")
    assert KimiUsageReader(file, 0).read() == {}
