"""Desktop workflow regression entrypoints. No real traffic data is used."""
import os
import shutil

import pytest


from support.runner import ROOT, run_driver

DRIVER = ROOT / "tests" / "workflow_driver.cjs"


@pytest.mark.parametrize("suite", ["core", "api", "ui"])
def test_workflow(suite, tmp_path):
    if suite == "ui":
        executable = ROOT / "desktop" / "node_modules" / "electron" / "dist" / "electron.exe"
        if os.name != "nt":
            executable = executable.with_name("electron")
        assert executable.exists(), "Run npm install in desktop/ to install Electron."
        command = [str(executable), str(DRIVER), suite]
    else:
        node = shutil.which("node")
        assert node, "Node.js 18+ is required for the API test driver."
        command = [node, str(DRIVER), suite]
    output = run_driver(command, tmp_path)
    assert "PASS " + suite + ":" in output


@pytest.mark.parametrize("script", ["night-stay-test.js", "night-stay-e2e.js"])
def test_existing_night_stay_regression(script, tmp_path):
    node = shutil.which("node")
    assert node, "Node.js is required."
    run_driver([node, str(ROOT / "desktop" / "scripts" / script)], tmp_path)
