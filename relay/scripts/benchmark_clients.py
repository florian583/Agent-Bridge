"""Compare idle initialized relays using an isolated fake daemon; no provider calls."""

import ctypes
import importlib.util
import json
import os
import statistics
import sys
import time
from pathlib import Path

import psutil

spec = importlib.util.spec_from_file_location(
    "relay_test_support", Path(__file__).resolve().parents[1] / "tests/test_protocol.py"
)
support = importlib.util.module_from_spec(spec)
spec.loader.exec_module(support)
Client, FakeDaemon, content = support.Client, support.FakeDaemon, support.content

FIELDS = [
    "user_time",
    "system_time",
    "pkg_idle_wkups",
    "interrupt_wkups",
    "pageins",
    "wired_size",
    "resident_size",
    "phys_footprint",
    "proc_start_abstime",
    "proc_exit_abstime",
    "child_user_time",
    "child_system_time",
    "child_pkg_idle_wkups",
    "child_interrupt_wkups",
    "child_pageins",
    "child_elapsed_abstime",
    "diskio_bytesread",
    "diskio_byteswritten",
]


class Usage(ctypes.Structure):
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [(name, ctypes.c_uint64) for name in FIELDS]


def memory(pid):
    process = psutil.Process(pid)
    row = {"rssMiB": process.memory_info().rss / 1048576}
    if sys.platform == "darwin":
        usage = Usage()
        if ctypes.CDLL("/usr/lib/libproc.dylib").proc_pid_rusage(pid, 2, ctypes.byref(usage)) == 0:
            row["footprintMiB"] = usage.phys_footprint / 1048576
    return row


def main():
    if not os.environ.get("BRIDGE_PYTHON"):
        raise SystemExit("BRIDGE_PYTHON is required")
    daemon = FakeDaemon()
    clients = []
    rows = []
    try:
        for reference in (False, True):
            for _ in range(3):
                started = time.monotonic()
                client = Client(daemon, reference=reference)
                clients.append(client)
                startup = (time.monotonic() - started) * 1000
                assert content(client.call("list_sessions", {}))["ok"]
                rows.append(
                    {
                        "implementation": "python" if reference else "rust",
                        "initializeMS": startup,
                        **memory(client.process.pid),
                    }
                )
        before = [sum(psutil.Process(c.process.pid).cpu_times()[:2]) for c in clients]
        started = time.monotonic()
        time.sleep(5)
        elapsed = time.monotonic() - started
        for client, row, cpu in zip(clients, rows, before, strict=True):
            row["idleCpuPct"] = (sum(psutil.Process(client.process.pid).cpu_times()[:2]) - cpu) / elapsed * 100
        summary = {
            kind: {
                key: round(statistics.median(row[key] for row in rows if row["implementation"] == kind), 3)
                for key in rows[0]
                if key != "implementation"
            }
            for kind in ("rust", "python")
        }
        print(
            json.dumps(
                {
                    "samplesPerImplementation": 3,
                    "idleSeconds": round(elapsed, 3),
                    "summaryMedian": summary,
                    "samples": rows,
                },
                indent=2,
            )
        )
    finally:
        for client in clients:
            client.close()
        daemon.close()


if __name__ == "__main__":
    main()
