"""Desktop-only checkout and generated build-input regressions; no user files are used."""
import json
import shutil
import struct

import pytest

from support.runner import ROOT, run_driver


@pytest.mark.parametrize("entrypoint", ["start", "dist", "dist:x64", "dist:ia32"])
def test_icon_preparation_hooks(entrypoint):
    package = json.loads((ROOT / "desktop/package.json").read_text(encoding="utf-8"))
    scripts = package["scripts"]
    assert scripts.get("build:icon") == "node scripts/make-icon.js"
    assert scripts.get("pre" + entrypoint) == "npm run build:icon"


def test_icons_generated_from_source_only(tmp_path):
    node = shutil.which("node")
    assert node, "Node.js is required for icon generation."
    scripts = tmp_path / "desktop/scripts"
    scripts.mkdir(parents=True)
    script = scripts / "make-icon.js"
    shutil.copyfile(ROOT / "desktop/scripts/make-icon.js", script)
    output = tmp_path / "desktop/build"
    assert not output.exists(), "The test must start without pre-generated build assets."

    run_driver([node, str(script)], tmp_path)
    png_signature = b"\x89PNG\r\n\x1a\n"
    png = (output / "icon.png").read_bytes()
    assert png.startswith(png_signature)
    assert struct.unpack_from(">II", png, 16) == (512, 512)

    ico = (output / "icon.ico").read_bytes()
    sizes = [16, 24, 32, 48, 64, 128, 256]
    assert struct.unpack_from("<HHH", ico) == (0, 1, len(sizes))
    for index, expected in enumerate(sizes):
        offset = 6 + index * 16
        assert (ico[offset] or 256, ico[offset + 1] or 256) == (expected, expected)
        length, start = struct.unpack_from("<II", ico, offset + 8)
        assert start >= 6 + len(sizes) * 16
        assert start + length <= len(ico)
        image = ico[start:start + length]
        assert image.startswith(png_signature)
        assert struct.unpack_from(">II", image, 16) == (expected, expected)


def test_desktop_checkout_and_packaging_inputs():
    for relative in [
        "desktop/main/main.js",
        "desktop/main/preload.js",
        "desktop/server/index.js",
        "desktop/package-lock.json",
        "static/frontend/index.html",
        "static/frontend/app.js",
        "static/frontend/app.mjs",
        "static/frontend/styles.css",
    ]:
        assert (ROOT / relative).is_file(), f"Missing desktop source: {relative}"
    for relative in ["app.py", "templates", "build_exe.ps1", "build_win7_dual_installers.ps1"]:
        assert not (ROOT / relative).exists(), f"Retired web/build source reintroduced: {relative}"

    config = (ROOT / "desktop/electron-builder.yml").read_text(encoding="utf-8")
    files = config.split("files:\n", 1)[1].split("\nextraResources:", 1)[0]
    assert "  - main/**/*\n" in files
    assert "  - server/**/*\n" in files
    assert "  - build/icon.ico\n" in files, "BrowserWindow's generated icon must be packaged."
    assert "from: ../static/frontend" in config, "The renderer is part of the desktop app."
