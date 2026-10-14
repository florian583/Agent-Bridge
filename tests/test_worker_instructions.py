import pytest

from agent_bridge.worker_instructions import worker_message


def test_worker_instructions_optional_and_bounded(tmp_path):
    assert worker_message(tmp_path, "task") == "task"
    source = tmp_path/"worker-instructions.md"
    source.write_text("Commit progressively.")
    result = worker_message(tmp_path, "task")
    assert "Commit progressively." in result and "task" in result
    source.write_bytes(b"x"*16385)
    with pytest.raises(ValueError):
        worker_message(tmp_path, "task")
