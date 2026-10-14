from pathlib import Path


def worker_message(home: Path, message: str) -> str:
    """Apply bounded operator instructions without changing request identity."""
    try:
        with (home / "worker-instructions.md").open("rb") as stream:
            raw = stream.read(16385)
    except FileNotFoundError:
        return message
    if len(raw) > 16384:
        raise ValueError("worker-instructions.md exceeds 16 KiB")
    try:
        instructions = raw.decode("utf-8").strip()
    except UnicodeDecodeError:
        raise ValueError("worker-instructions.md must be UTF-8") from None
    if not instructions:
        return message
    return (f"<bridge-standing-instructions>\n{instructions}\n</bridge-standing-instructions>\n\n"
            f"<coordinator-task>\n{message}\n</coordinator-task>")
