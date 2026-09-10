"""Isolated process runner; logs are files so renderer children cannot hold a pipe open."""
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def run_driver(command, tmp_path, timeout=180):
    env = os.environ.copy()
    env.pop("ELECTRON_RUN_AS_NODE", None)
    env["VS_TEST_WORK_DIR"] = str(tmp_path)
    env["VS_TEST_ARTIFACTS"] = str(tmp_path / "screenshots")
    env["TEMP"] = str(tmp_path)
    env["TMP"] = str(tmp_path)
    options = (
        {"creationflags": subprocess.CREATE_NO_WINDOW}
        if os.name == "nt"
        else {"start_new_session": True}
    )
    log_path = tmp_path / "driver.log"
    expired = False
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            command, cwd=str(ROOT), env=env, stdout=log,
            stderr=subprocess.STDOUT, **options,
        )
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            expired = True
            if os.name == "nt":
                # Only terminate the process tree launched for this isolated test.
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW,
                )
            else:
                import signal
                os.killpg(process.pid, signal.SIGKILL)
            process.kill()
            process.wait()
    raw = log_path.read_bytes()
    try:
        output = raw.decode("utf-8")
    except UnicodeDecodeError:
        output = raw.decode("gb18030", errors="replace")
    print(output)
    assert not expired, f"Driver exceeded {timeout}s. Log: {log_path}\n{output}"
    assert process.returncode == 0, output
    return output
