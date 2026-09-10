"""Regression tests for optional same-window night stays, using synthetic data only."""
import shutil

import pytest

from support.runner import ROOT, run_driver


@pytest.mark.parametrize("suite", ["core", "api"])
def test_night_window(suite, tmp_path):
    node = shutil.which("node")
    assert node, "Node.js 18+ is required."
    output = run_driver(
        [node, str(ROOT / "tests" / "night_window_driver.cjs"), suite],
        tmp_path,
    )
    assert "PASS night-window-" + suite + ":" in output
