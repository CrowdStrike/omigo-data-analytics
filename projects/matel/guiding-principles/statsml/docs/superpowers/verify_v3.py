#!/usr/bin/env python3
"""Folder-level mechanical verification for Pass B v3 (clean-room) pages.

Per NN-topic.v3.html in the folder — same bar as v2:
  1. sibling .txt.md exists; .viz.md exists when the page has canvases
  2. fences pair (TEXT / VIZ / LIB open+close)
  3. every <canvas id> has its id in viz.md and a VIZ fence; no duplicate ids
  4. node --check passes on inline <script> blocks
  5. visible text matches the ORIGINAL NN-topic.html (normalized; index-number
     removal tolerated; ratio >= 0.985 -> warn, below -> FAIL)

Usage: verify_v3.py <folder> [--prose-only]
Exit 0 = all pass. Prints PASS/FAIL per page with reasons.
"""
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from verify_folder import visible_text, check_fences, inline_scripts, node_check


def verify_v3_page(v3: Path, prose_only: bool):
    errs, warns = [], []
    base = v3.name[:-len('.v3.html')]
    folder = v3.parent
    orig = folder / f'{base}.html'
    txt = folder / f'{base}.txt.md'
    viz = folder / f'{base}.viz.md'

    src = v3.read_text(encoding='utf-8', errors='replace')
    if not txt.exists():
        errs.append('missing .txt.md')
    canvases = re.findall(r'<canvas[^>]*\bid="([^"]+)"', src)
    if canvases and not viz.exists() and not prose_only:
        errs.append(f'{len(canvases)} canvases but no .viz.md')
    if len(canvases) != len(set(canvases)):
        dupes = sorted({c for c in canvases if canvases.count(c) > 1})
        errs.append(f'duplicate canvas ids: {dupes}')

    fe, _ = check_fences(src)
    errs += fe

    if canvases:
        if viz.exists():
            vsrc = viz.read_text(encoding='utf-8', errors='replace')
            missing = [c for c in set(canvases) if not re.search(r'\b' + re.escape(c) + r'\b', vsrc)]
            if missing:
                errs.append(f'canvas ids missing from viz.md: {sorted(missing)}')
            # clean-room completeness: every briefed canvas must exist in v3
            briefed = re.findall(r'^##\s*\[sec-\d+\]\s+(\S+)', vsrc, re.M)
            absent = [c for c in briefed if c not in set(canvases)]
            if absent:
                errs.append(f'viz.md briefs with no canvas in v3: {sorted(absent)}')
        for c in set(canvases):
            if not re.search(r'VIZ\s+' + re.escape(c) + r'\b', src):
                errs.append(f'canvas {c} has no VIZ fence')

    for js in inline_scripts(src):
        ok, msg = node_check(js)
        if not ok:
            errs.append(f'node --check failed: {"; ".join(msg)}')

    if orig.exists():
        a = visible_text(orig.read_text(encoding='utf-8', errors='replace'))
        b = visible_text(src)
        if a != b:
            sm = SequenceMatcher(None, a, b)
            r = sm.ratio()
            diffs = [f'{tag} orig[{a[i1:i2][:60]!r}] v3[{b[j1:j2][:60]!r}]'
                     for tag, i1, i2, j1, j2 in sm.get_opcodes() if tag != 'equal'][:4]
            if r < 0.985:
                errs.append(f'text mismatch ratio={r:.4f}: ' + ' | '.join(diffs))
            else:
                warns.append(f'text near-match ratio={r:.4f}: ' + ' | '.join(diffs))
    else:
        warns.append('no original html to compare')
    return errs, warns


def main():
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    prose_only = '--prose-only' in sys.argv
    folder = Path(args[0])
    pages = sorted(folder.glob('*.v3.html'))
    if not pages:
        print(f'{folder}: no v3 pages found')
        sys.exit(2)
    nfail = 0
    for pg in pages:
        errs, warns = verify_v3_page(pg, prose_only)
        status = 'FAIL' if errs else 'PASS'
        if errs:
            nfail += 1
        line = f'{status} {pg.name}'
        for e in errs:
            line += f'\n      ERR  {e}'
        for w in warns:
            line += f'\n      warn {w}'
        print(line)
    print(f'== {folder}: {len(pages) - nfail}/{len(pages)} pass ==')
    sys.exit(1 if nfail else 0)


if __name__ == '__main__':
    main()
