"""Check the desktop transition without reading private data or altering the working tree."""
import ast
import hashlib
import json
from pathlib import Path
import re

ROOT = next(parent for parent in Path(__file__).resolve().parents
            if (parent / ".trellis/spec").is_dir())
RESEARCH = Path(__file__).resolve().parent
baseline = json.loads((RESEARCH / "source-baseline.json").read_text(encoding="utf-8"))
comments = json.loads((RESEARCH / "comment-only-changes.json").read_text(encoding="utf-8"))
for relative, expected in baseline.items():
    source = ROOT / relative
    assert source.is_file(), f"Retained source missing: {relative}"
    if relative in comments:
        text = source.read_text(encoding="utf-8")
        statements = "\n".join(line for line in text.splitlines()
                               if line.strip() and not line.lstrip().startswith("//"))
        assert hashlib.sha256(statements.encode()).hexdigest() == comments[relative], relative
    else:
        assert hashlib.sha256(source.read_bytes()).hexdigest() == expected, relative

manifest = json.loads((RESEARCH / "cleanup-manifest.json").read_text(encoding="utf-8"))
for relative in manifest["removed"]:
    assert not (ROOT / relative).exists(), f"Retired source remains: {relative}"
assert not (ROOT / "templates").exists()

package = json.loads((ROOT / "desktop/package.json").read_text(encoding="utf-8"))
lock = json.loads((ROOT / "desktop/package-lock.json").read_text(encoding="utf-8"))
for field in ("name", "version", "dependencies", "devDependencies"):
    assert package[field] == lock["packages"][""][field], f"Lockfile mismatch: {field}"

links = 0
specs = sorted((ROOT / ".trellis/spec").rglob("*.md"))
documents = specs + list((ROOT / "docs").rglob("*.md")) + [ROOT / "README.md"]
pattern = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
for document in documents:
    text = document.read_text(encoding="utf-8")
    if document in specs:
        assert not re.search(r"To be filled|\bTo fill\b|TODO:\s*fill|\bplaceholder\b", text, re.I), document
    for destination in pattern.findall(text):
        if re.match(r"[a-z][a-z0-9+.-]*:", destination, re.I) or destination.startswith("#"):
            continue
        target = (document.parent / destination.split("#", 1)[0]).resolve()
        assert target.is_relative_to(ROOT) and target.exists(), f"Broken link: {document} -> {destination}"
        links += 1
for layer in ("backend", "frontend", "guides"):
    index = ROOT / ".trellis/spec" / layer / "index.md"
    indexed = {(index.parent / dest.split("#", 1)[0]).resolve()
               for dest in pattern.findall(index.read_text(encoding="utf-8"))}
    for document in index.parent.glob("*.md"):
        assert document == index or document.resolve() in indexed, f"Unindexed spec: {document}"

python_files = []
for directory in ("tests", ".trellis/scripts", ".codex/hooks"):
    for source in (ROOT / directory).rglob("*.py"):
        ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        python_files.append(source)
print(f"PASS: {len(baseline)} retained source files preserved ({len(comments)} comment/blank-line-only edits); "
      f"{len(manifest['removed'])} retired files absent; lockfile consistent; "
      f"{len(specs)} specs, {links} local links and {len(python_files)} Python source files verified.")
