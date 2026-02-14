#!/usr/bin/env python3

import argparse
import datetime as _dt
import re
import sys
from typing import List, Optional, Tuple


SSOT_BEGIN = "<!-- NAVIGATOR:BEGIN SSOT_V2 -->"
SSOT_END = "<!-- NAVIGATOR:END SSOT_V2 -->"


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _write_text(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _iso_utc_now() -> str:
    return _dt.datetime.now(tz=_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_findings_ids(findings_yaml_path: str) -> Tuple[List[str], List[str], List[str]]:
    text = _read_text(findings_yaml_path)
    open_ids: List[str] = []
    fixed_ids: List[str] = []
    partial_ids: List[str] = []

    cur_id: Optional[str] = None
    for raw in text.splitlines():
        line = raw.rstrip("\n")
        m = re.match(r"^\s*-\s+id:\s*([A-Za-z0-9_\-]+)\s*$", line)
        if m:
            cur_id = m.group(1)
            continue
        m = re.match(r"^\s*status:\s*([A-Za-z0-9_\-]+)\s*$", line)
        if m and cur_id:
            st = m.group(1).lower()
            if st == "open":
                open_ids.append(cur_id)
            elif st in ("fixed", "closed"):
                fixed_ids.append(cur_id)
            elif st in ("partially_fixed", "partial"):
                partial_ids.append(cur_id)
            cur_id = None

    def uniq(xs: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for x in xs:
            if x in seen:
                continue
            seen.add(x)
            out.append(x)
        return out

    return uniq(open_ids), uniq(fixed_ids), uniq(partial_ids)


def _fmt_bool_yaml(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    t = str(v).strip().lower()
    if t in ("1", "true", "yes", "y", "on"):
        return "true"
    if t in ("0", "false", "no", "n", "off"):
        return "false"
    return None


def _make_review_block(
    *,
    baseline_commit: str,
    run_time: str,
    run_range: str,
    artifacts_dir: str,
    review_md: str,
    findings_yaml: str,
    next_actions: str,
    open_ids: List[str],
    fixed_ids: List[str],
    partial_ids: List[str],
    review_run_id: Optional[str],
    review_commit: Optional[str],
    gatecheck_pass: Optional[str],
    gatecheck_artifact: Optional[str],
    open_must_fix_count: Optional[int],
) -> str:
    def fmt_list(ids: List[str]) -> str:
        return "[" + ", ".join(ids) + "]"

    lines: List[str] = []
    lines.append("review:")
    lines.append(f'  baseline_commit: "{baseline_commit}"')
    lines.append(f"  open: {fmt_list(open_ids)}")
    lines.append(f"  fixed: {fmt_list(fixed_ids)}")
    lines.append(f"  partially_fixed: {fmt_list(partial_ids)}")
    lines.append("  last_run:")
    lines.append(f'    time: "{run_time}"')
    lines.append(f'    range: "{run_range}"')
    lines.append(f'    artifacts_dir: "{artifacts_dir}"')
    lines.append(f'    review_md: "{review_md}"')
    lines.append(f'    findings_yaml: "{findings_yaml}"')
    lines.append(f'    next_actions: "{next_actions}"')

    if review_run_id:
        lines.append(f'    run_id: "{review_run_id}"')
    if review_commit:
        lines.append(f'    commit: "{review_commit}"')

    gcp = _fmt_bool_yaml(gatecheck_pass)
    if gcp is not None:
        lines.append(f"    gatecheck_pass: {gcp}")
    if gatecheck_artifact:
        lines.append(f'    gatecheck_artifact: "{gatecheck_artifact}"')
    if open_must_fix_count is not None:
        lines.append(f"    open_must_fix_count: {open_must_fix_count}")

    lines.append("")
    return "\n".join(lines)


def _patch_ssot_block(ssot_text: str, review_block: str, op: str) -> str:
    lines = ssot_text.splitlines(keepends=True)

    try:
        begin_i = next(i for i, l in enumerate(lines) if SSOT_BEGIN in l)
        end_i = next(i for i, l in enumerate(lines) if SSOT_END in l)
    except StopIteration:
        raise RuntimeError("SSOT_V2 managed block not found")

    ssot_lines = lines[begin_i + 1 : end_i]
    before = lines[: begin_i + 1]
    after = lines[end_i:]

    review_start = None
    for i, l in enumerate(ssot_lines):
        if re.match(r"^review:\s*$", l.rstrip("\n")):
            review_start = i
            break

    if review_start is None:
        if op == "update":
            raise RuntimeError("review block not found (op=update)")
        insert_at = len(ssot_lines)
        for i, l in enumerate(ssot_lines):
            if re.match(r"^pr_ready:\s*$", l.rstrip("\n")):
                insert_at = i
                break
        new_ssot = ssot_lines[:insert_at] + [review_block] + ssot_lines[insert_at:]
        return "".join(before + new_ssot + after)

    if op == "ensure":
        return ssot_text

    review_end = len(ssot_lines)
    for j in range(review_start + 1, len(ssot_lines)):
        if re.match(r"^[A-Za-z0-9_]+:\s*$", ssot_lines[j].rstrip("\n")):
            review_end = j
            break

    new_ssot = ssot_lines[:review_start] + [review_block] + ssot_lines[review_end:]
    return "".join(before + new_ssot + after)


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Patch SSOT_V2 with review status + last_run pointers")
    ap.add_argument("--ssot", required=True, help="Path to PROJECT_STATE.md")
    ap.add_argument("--op", required=True, choices=["ensure", "update"], help="ensure inserts review if missing; update overwrites existing")
    ap.add_argument("--base", required=True)
    ap.add_argument("--head", required=True)
    ap.add_argument("--run-range", required=True, help="<base>..<head>")
    ap.add_argument("--findings-yaml", required=True)
    ap.add_argument("--review-md", required=True)
    ap.add_argument("--next-actions", required=True)
    ap.add_argument("--artifacts-dir", required=True)
    ap.add_argument("--baseline-commit", default=None, help="If unset, baseline_commit=head")

    ap.add_argument("--review-run-id", default=None)
    ap.add_argument("--review-commit", default=None)
    ap.add_argument("--gatecheck-pass", default=None)
    ap.add_argument("--gatecheck-artifact", default=None)
    ap.add_argument("--open-must-fix-count", default=None, type=int)

    args = ap.parse_args(argv)

    open_ids, fixed_ids, partial_ids = _parse_findings_ids(args.findings_yaml)
    baseline_commit = args.baseline_commit or args.head
    run_time = _iso_utc_now()

    review_block = _make_review_block(
        baseline_commit=baseline_commit,
        run_time=run_time,
        run_range=args.run_range,
        artifacts_dir=args.artifacts_dir,
        review_md=args.review_md,
        findings_yaml=args.findings_yaml,
        next_actions=args.next_actions,
        open_ids=open_ids,
        fixed_ids=fixed_ids,
        partial_ids=partial_ids,
        review_run_id=args.review_run_id,
        review_commit=args.review_commit,
        gatecheck_pass=args.gatecheck_pass,
        gatecheck_artifact=args.gatecheck_artifact,
        open_must_fix_count=args.open_must_fix_count,
    )

    doc = _read_text(args.ssot)
    patched = _patch_ssot_block(doc, review_block, args.op)

    if patched == doc:
        if args.op == "ensure":
            print(f"review block already exists; ensure is a no-op: {args.ssot}")
            return 0
        print("no changes to SSOT (unexpected); refusing to overwrite", file=sys.stderr)
        return 4

    _write_text(args.ssot, patched)
    print(f"patched SSOT review section ({args.op}): {args.ssot}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
