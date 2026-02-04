#!/usr/bin/env python3
import argparse
import re
from pathlib import Path

def die(msg: str):
    raise SystemExit(f"[ssot_patch_evidence] ERROR: {msg}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project_state", required=True)
    ap.add_argument("--evidence_id", required=True)
    ap.add_argument("--status", required=True)
    ap.add_argument("--type", required=True)
    ap.add_argument("--commit", required=True)
    ap.add_argument("--patch_id", required=True)
    ap.add_argument("--time", required=True)
    ap.add_argument("--env", required=True)
    ap.add_argument("--command", required=True)
    ap.add_argument("--artifact", required=True)
    args = ap.parse_args()

    p = Path(args.project_state)
    if not p.exists():
        die(f"project_state not found: {p}")

    s = p.read_text(encoding="utf-8")

    begin = "<!-- NAVIGATOR:BEGIN SSOT_V2 -->"
    end = "<!-- NAVIGATOR:END SSOT_V2 -->"
    if begin not in s or end not in s:
        die("SSOT_V2 managed block not found")

    # Extract SSOT block
    pre, rest = s.split(begin, 1)
    block, post = rest.split(end, 1)

    # We patch only within block, by locating evidence item "- id: <EID>"
    # and rewriting a fixed set of keys beneath it (indent-aware).
    eid = re.escape(args.evidence_id)

    # Find the evidence item start line
    m = re.search(rf"(?m)^[ \t]*- id:\s*{eid}\s*$", block)
    if not m:
        die(f"evidence item not found in SSOT_V2: {args.evidence_id}")

    start = m.start()

    # Determine slice for this item: from its "- id:" line to before the next "- id:" at same indent, or end of block.
    # We assume evidence list items have consistent indentation.
    # Compute the indentation prefix of "- id:" line.
    line_start = block.rfind("\n", 0, start) + 1
    line = block[line_start:block.find("\n", line_start)]
    indent = re.match(r"^([ \t]*)- id:", line).group(1)

    # Next item boundary
    next_m = re.search(rf"(?m)^{re.escape(indent)}- id:\s*(?!{eid}).*$", block[m.end():])
    if next_m:
        item_end = m.end() + next_m.start()
    else:
        item_end = len(block)

    item = block[line_start:item_end]

    def set_key(item_text: str, key: str, value: str) -> str:
        # key lines look like: "    status: xxx"
        key_re = rf"(?m)^{re.escape(indent)}  {re.escape(key)}:\s*.*$"
        if re.search(key_re, item_text):
            return re.sub(key_re, f"{indent}  {key}: {value}", item_text)
        # Insert after id line if key missing
        id_line_re = rf"(?m)^{re.escape(indent)}- id:\s*{eid}\s*$"
        return re.sub(id_line_re, f"{indent}- id: {args.evidence_id}\n{indent}  {key}: {value}", item_text, count=1)

    # Patch fields
    item2 = item
    item2 = set_key(item2, "status", args.status)
    item2 = set_key(item2, "type", args.type)
    item2 = set_key(item2, "commit", f"\"{args.commit}\"")
    item2 = set_key(item2, "patch_id", f"\"{args.patch_id}\"")
    item2 = set_key(item2, "time", f"\"{args.time}\"")
    item2 = set_key(item2, "env", f"\"{args.env}\"")
    item2 = set_key(item2, "command", f"\"{args.command}\"")
    item2 = set_key(item2, "artifact", f"\"{args.artifact}\"")

    if item2 == item:
        die("no changes applied (unexpected)")

    block2 = block[:line_start] + item2 + block[item_end:]
    out = pre + begin + block2 + end + post
    p.write_text(out, encoding="utf-8")
    print(f"[ssot_patch_evidence] OK: patched {args.evidence_id} -> {args.status}")

if __name__ == "__main__":
    main()

