import json

from agent_bridge.binding import binding_path, current_conversation, record_session


class Process:
    def __init__(self, pid, name="claude", created=123.0):
        self.pid, self._name, self.created = pid, name, created

    def name(self):
        return self._name

    def exe(self):
        return "/bin/"+self._name

    def create_time(self):
        return self.created


def test_binding_follows_host_uuid_not_worktree(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_BRIDGE_BINDINGS", str(tmp_path))
    monkeypatch.delenv("AGENT_BRIDGE_CONVERSATION_ID", raising=False)
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    a, b = Process(10), Process(11)
    monkeypatch.setattr("agent_bridge.binding.ancestors", lambda: [Process(9, "bash"), a])
    assert record_session({"session_id": "well-a", "cwd": "/same/path"})
    assert current_conversation() == "well-a"
    monkeypatch.setattr("agent_bridge.binding.ancestors", lambda: [b])
    assert current_conversation() is None
    assert record_session({"session_id": "well-b", "cwd": "/same/path"})
    assert current_conversation() == "well-b"
    assert record_session({"session_id": "well-new"})
    assert current_conversation() == "well-new"
    assert not record_session({"session_id": "nested", "agent_id": "worker"})
    assert current_conversation() == "well-new"
    monkeypatch.setattr("agent_bridge.binding.ancestors", lambda: [Process(11, created=999)])
    assert current_conversation() is None
    assert json.loads(binding_path(a).read_text())["session_id"] == "well-a"
