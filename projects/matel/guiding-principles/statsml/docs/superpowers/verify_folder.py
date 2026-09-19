#!/usr/bin/env python3
"""Folder-level mechanical verification for the text/viz migration.

Per NN-topic.v2.html in the folder:
  1. sibling .txt.md exists; .viz.md exists when the page has canvases
  2. fences pair (TEXT / VIZ / LIB open+close, matching labels)
  3. every <canvas id> has its id mentioned in viz.md and a VIZ fence
  4. node --check passes on the inline <script> block
  5. visible text of v2 matches the original NN-topic.html (normalized;
     title/h1 index-number removal tolerated)

Usage: verify_folder.py <folder> [--prose-only] [--v2-only] [--md-only]
  --v2-only: skip txt.md/viz.md existence and brief checks (v2-first pass)
  --md-only: Pass-A mode — iterate ORIGINAL htmls; check txt.md text coverage
             and viz.md briefs per canvas; no v2.html required
Exit 0 = all pass. Prints PASS/FAIL per page with reasons.
"""
import html as htmlmod
import re
import subprocess
import sys
import tempfile
from difflib import SequenceMatcher
from pathlib import Path


def visible_text(html_src: str) -> str:
    s = re.sub(r'<script\b.*?</script>', ' ', html_src, flags=re.S | re.I)
    s = re.sub(r'<style\b.*?</style>', ' ', s, flags=re.S | re.I)
    s = re.sub(r'<!--.*?-->', ' ', s, flags=re.S)
    s = re.sub(r'<[^>]+>', ' ', s)
    s = htmlmod.unescape(s)
    # tolerate index-number strips: "12. Title" -> "Title"
    s = re.sub(r'\b\d{1,3}\.\s+(?=[A-Z])', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip().lower()
    return s


def check_fences(src: str):
    errs = []
    marks = re.findall(r'(?<!=)====(?!=)\s*(/?)((?:(?!====).)+?)\s*(?<!=)====(?!=)', src)
    stack = {}
    order = []
    for close, label in marks:
        label = label.split('(')[0]  # strip annotations: LIB (page-local ...), incl. nested parens
        label = re.sub(r'\s+', ' ', label.strip())
        if not close:
            stack[label] = stack.get(label, 0) + 1
            order.append(label)
        else:
            if stack.get(label, 0) <= 0:
                errs.append(f'close without open: {label}')
            else:
                stack[label] -= 1
    for label, n in stack.items():
        if n > 0:
            errs.append(f'unclosed fence: {label}')
    return errs, order


def inline_scripts(src: str):
    out = []
    for m in re.finditer(r'<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>', src, re.S | re.I):
        body = m.group(1).strip()
        if body:
            out.append(body)
    return out


def node_check(js: str):
    with tempfile.NamedTemporaryFile('w', suffix='.js', delete=False) as f:
        f.write(js)
        p = f.name
    r = subprocess.run(['node', '--check', p], capture_output=True, text=True)
    Path(p).unlink(missing_ok=True)
    return r.returncode == 0, r.stderr.strip().splitlines()[:3]


def verify_md_page(orig: Path, prose_only: bool):
    """Pass-A check: txt.md covers the original's text; viz.md briefs every canvas."""
    errs, warns = [], []
    base = orig.name[:-len('.html')]
    folder = orig.parent
    txt = folder / f'{base}.txt.md'
    viz = folder / f'{base}.viz.md'

    osrc = orig.read_text(encoding='utf-8', errors='replace')
    canvases = re.findall(r'<canvas[^>]*\bid="([^"]+)"', osrc)

    if not txt.exists():
        return ['missing .txt.md'], []
    tsrc = txt.read_text(encoding='utf-8', errors='replace')

    # text coverage: original visible text vs txt.md (both as alphanumeric tokens)
    a = visible_text(osrc)
    b = re.sub(r'\bsec-\d+\b', ' ', tsrc.lower())
    a_tokens = re.findall(r'[a-z0-9]+', a)
    b_set = set(re.findall(r'[a-z0-9]+', b))
    missing_tokens = [t for t in a_tokens if t not in b_set]
    cov = 1 - len(missing_tokens) / max(1, len(a_tokens))
    if cov < 0.97:
        errs.append(f'txt.md covers only {cov:.1%} of original tokens; first missing: {missing_tokens[:12]}')
    elif cov < 1.0:
        warns.append(f'txt.md token coverage {cov:.2%}; missing sample: {missing_tokens[:8]}')

    if canvases:
        if not viz.exists():
            if not prose_only:
                errs.append(f'{len(canvases)} canvases but no .viz.md')
        else:
            vsrc = viz.read_text(encoding='utf-8', errors='replace')
            for c in set(canvases):
                if not re.search(r'^##.*\b' + re.escape(c) + r'\b', vsrc, re.M):
                    errs.append(f'canvas {c} has no "## [sec-N] {c}" brief in viz.md')
            for field in ('Type', 'Data', 'Colors', 'Shows'):
                n = len(re.findall(r'\*\*' + field + r':?\*\*', vsrc))
                if n < len(set(canvases)):
                    warns.append(f'viz.md has {n} "{field}" bullets for {len(set(canvases))} canvases')
    return errs, warns


def verify_page(v2: Path, prose_only: bool, v2_only: bool = False):
    errs, warns = [], []
    base = v2.name[:-len('.v2.html')]
    folder = v2.parent
    orig = folder / f'{base}.html'
    txt = folder / f'{base}.txt.md'
    viz = folder / f'{base}.viz.md'

    src = v2.read_text(encoding='utf-8', errors='replace')
    if not txt.exists() and not v2_only:
        errs.append('missing .txt.md')
    canvases = re.findall(r'<canvas[^>]*\bid="([^"]+)"', src)
    if canvases and not viz.exists() and not prose_only and not v2_only:
        errs.append(f'{len(canvases)} canvases but no .viz.md')
    if len(canvases) != len(set(canvases)):
        dupes = sorted({c for c in canvases if canvases.count(c) > 1})
        errs.append(f'duplicate canvas ids: {dupes}')

    fe, _ = check_fences(src)
    errs += fe

    if canvases:
        if viz.exists() and not v2_only:
            vsrc = viz.read_text(encoding='utf-8', errors='replace')
            missing = [c for c in set(canvases) if not re.search(r'\b' + re.escape(c) + r'\b', vsrc)]
            if missing:
                errs.append(f'canvas ids missing from viz.md: {sorted(missing)}')
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
            r = SequenceMatcher(None, a, b).ratio()
            sm = SequenceMatcher(None, a, b)
            diffs = [f'{tag} orig[{a[i1:i2][:60]!r}] v2[{b[j1:j2][:60]!r}]'
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
    v2_only = '--v2-only' in sys.argv
    md_only = '--md-only' in sys.argv
    folder = Path(args[0])
    if md_only:
        pages = sorted(p for p in folder.glob('*.html') if not p.name.endswith('.v2.html'))
    else:
        pages = sorted(folder.glob('*.v2.html'))
    if not pages:
        print(f'{folder}: no pages found')
        sys.exit(2)
    nfail = 0
    for pg in pages:
        if md_only:
            errs, warns = verify_md_page(pg, prose_only)
        else:
            errs, warns = verify_page(pg, prose_only, v2_only)
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
