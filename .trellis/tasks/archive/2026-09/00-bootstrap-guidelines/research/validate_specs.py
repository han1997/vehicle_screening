"""Validate this bootstrap's specs and protect pre-existing product files (stdlib only)."""
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = next(parent for parent in Path(__file__).resolve().parents
            if (parent / ".trellis/spec").is_dir())
SPEC = ROOT / ".trellis/spec"
RESEARCH = Path(__file__).resolve().parent
errors = []
links_checked = 0
source_paths_checked = 0
json_examples_checked = 0
files = sorted(SPEC.rglob("*.md"))
scaffold = re.compile(
    r"\bplaceholder\b|To be filled|\bTo fill\b|TODO:\s*fill|\(To be filled by the team\)|"
    r"Fill in each file|Document your project's|Questions to answer:",
    re.IGNORECASE,
)
link_pattern = re.compile(r"\[[^\]]+\]\(([^)]+)\)")


def require(condition, message):
    if not condition:
        errors.append(message)


for path in files:
    label = path.relative_to(ROOT).as_posix()
    text = path.read_text(encoding="utf-8")
    require(text.startswith("# "), f"Missing document title: {label}")
    require(not scaffold.search(text), f"Scaffolding remains: {label}")
    require(text.endswith("\n"), f"Missing final newline: {label}")
    require(not any(ord(ch) < 32 and ch not in "\n\r\t" for ch in text),
            f"Unexpected control character: {label}")
    require(text.count("```") % 2 == 0, f"Unbalanced code fences: {label}")
    for destination in link_pattern.findall(text):
        if re.match(r"[a-z]+://", destination) or destination.startswith("#"):
            continue
        local = destination.split("#", 1)[0]
        target = (path.parent / local).resolve()
        links_checked += 1
        require(target.is_relative_to(ROOT) and target.exists(),
                f"Broken or out-of-repo link: {label} -> {destination}")
    for token in re.findall(r"`([^`\n]+)`", text):
        if not token.startswith(("desktop/", "static/frontend/", "tests/", "docs/", ".trellis/", "templates/")):
            continue
        if any(ch in token for ch in '*?<> {}()=;,"'):
            continue
        source_paths_checked += 1
        require((ROOT / token).exists(), f"Missing source path: {label} -> {token}")
    for example in re.findall(r"```json\s*\n(.*?)\n```", text, re.DOTALL):
        try:
            json.loads(example)
            json_examples_checked += 1
        except json.JSONDecodeError as exc:
            errors.append(f"Invalid JSON example: {label}: {exc}")
    if path.parent.name in {"backend", "frontend"} and path.name != "index.md":
        require("../../../" in text, f"Missing concrete source references: {label}")

for layer in ("backend", "frontend", "guides"):
    index = SPEC / layer / "index.md"
    text = index.read_text(encoding="utf-8")
    indexed = {(index.parent / dest.split("#", 1)[0]).resolve()
               for dest in link_pattern.findall(text) if not re.match(r"[a-z]+://", dest)}
    for path in (SPEC / layer).glob("*.md"):
        require(path == index or path.resolve() in indexed,
                f"Unindexed document: {path.relative_to(ROOT)}")
    if layer != "guides":
        for heading in ("Pre-Development Checklist", "Quality Check"):
            require(f"## {heading}" in text, f"Missing {heading}: {layer}/index.md")

contracts = (SPEC / "backend/screening-contracts.md").read_text(encoding="utf-8")
for number in range(1, 8):
    require(f"## {number}. " in contracts, f"Missing contract section {number}")
require(not (SPEC / "frontend/hook-guidelines.md").exists(), "Obsolete hook scaffold remains")

baseline = json.loads((RESEARCH / "baseline.json").read_text(encoding="utf-8"))["files"]
for relative, expected in baseline.items():
    path = ROOT / relative
    require(path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == expected,
            f"Protected pre-existing file changed: {relative}")

if errors:
    print("Spec verification failed:")
    print("\n".join(f"- {message}" for message in errors))
    sys.exit(1)
print(f"PASS: {len(files)} specs; {links_checked} local links; "
      f"{source_paths_checked} source paths; {json_examples_checked} JSON examples; "
      f"{len(baseline)} protected files unchanged.")
