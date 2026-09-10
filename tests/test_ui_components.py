"""Shared controls and accessible date/time picker regression tests."""
import os
import shutil

import pytest

from support.runner import ROOT, run_driver


@pytest.mark.parametrize("suite", ["core", "ui"])
def test_controls(suite, tmp_path):
    if suite == "ui":
        executable = ROOT / "desktop/node_modules/electron/dist" / ("electron.exe" if os.name == "nt" else "electron")
        assert executable.exists()
        command = [str(executable)]
    else:
        node = shutil.which("node")
        assert node
        command = [node]
    output = run_driver(command + [str(ROOT / "tests/controls_driver.cjs"), suite], tmp_path)
    assert "PASS controls-" + suite + ":" in output
