# Tutorials Review & Fix Report

Full-corpus review and fix pass over `23-tutorials/` (topic pages + category grids + sibling `.md` specs), run 2026-08-26/27.

## Scope and method

- **1,169 HTML pages reviewed** (1,111 topic pages in 58 folders + 58 category grids), including files added or renamed while the review was running (two delta passes caught them).
- Two layers:
  1. **Automated sweep** over every file: inline-script syntax (`node --check`), grid-link integrity, card-number ↔ filename match, `Math.random()` ban, devicePixelRatio scaling, forbidden nav/cross-links, md-sibling presence.
  2. **Agent deep review** in 126 chunks (~10-12 pages each): recompute every worked example by hand, verify text numbers match hardcoded chart arrays, trace canvas drawing code for clipping/overlap/off-scale geometry, check factual claims, example quality, and page conventions per `23-tutorials/CLAUDE.md`.
- Every finding was then **re-verified and fixed** by a second agent pass (minimal edits, sibling `.md` kept in sync, `node --check` after every edit).

## Results

| Severity | Found | Meaning |
|----------|-------|---------|
| HIGH | 84 | factually wrong content, arithmetic that doesn't recompute, chart contradicting text, clipped/illegible key annotations |
| MED | 479 | overgeneralized claims needing caveats, text↔chart number drift, misleading labels, missing "illustrative" tags |
| LOW | 623 | label collisions, minor clipping, wording/convention nits |

- All findings were fixed except ~15 deliberate skips (see "Deliberately not changed").
- Automated sweep is **clean both before and after** the fixes: every script parses, every grid link resolves, numbering is consistent, no conventions were broken by the edits.
- Fix agents overrode reviewer suggestions where the suggestion itself didn't verify (e.g. instrumental-variables numbers, several annotation coordinates) — each such case was recomputed independently.

## Flavor of the HIGH findings (all fixed)

- **Stats core:** standard-error page called a t≈1.77 / p≈0.08 difference "very real"; quorums page used ⌈N/2⌉+1 for majority; reading-results page stated a CI/p-value pair impossible for its n; sampling-distribution SE inconsistent with its own histogram.
- **Arithmetic in worked examples:** derivative page (80.4 vs correct 80.802), backprop/forward-pass pages (0.91 vs correct 0.88), chain-of-thought total, kappa (0.22 vs 0.25), tomcat thread math (600 vs 400), climbing-stairs call count, edit distance (6 vs 5), two-egg ratios, hot-shard totals.
- **Chart vs text:** gradient-descent landscape drawn upside down; GMM bell heights ~2.5× true densities; Dijkstra example missing its 7th route; dropbox conflict chart swapping laptops; learning-to-rank verdict contradicting plotted scores.
- **Factual:** Flutter "renders real native widgets"; pandas "sum fails on NaN"; C++ virtual inheritance resolving sibling overrides; cmd.exe "has no pipes"; macOS `sort -h`/`readlink -f` "fail"; Arrow Spark→pandas "zero-copy"; QR level-H check share 30% (is ~65%).

## Corpus changed while the job ran

Another session was actively editing the repo. All of the following were caught and folded in:
- `cryptography-privacy/` split into `cryptography/` + `privacy/` (grid renamed to `48-cryptography.html`, new `58-privacy.html`).
- 13 brand-new topic pages appeared mid-job (quantum-computing, homomorphic-encryption, remote-administration-tools, remote-access-trojans, concurrent-hash-maps, t-digest, bitmaps-bitsets, self-balancing-trees, fenwick-trees, suffix-arrays, …). All reviewed and fixed via two delta passes; missing grid cards were added for concurrency/11, malware/15, data-structures/16-19, algorithms/24, computation-compilers/23.

## One structural change to be aware of

**tech-infra was renumbered.** To fix the Terraform card sitting at position 18 while displayed after card 2, a fix agent renamed `18-terraform` → `03-terraform` and shifted old `03`–`17` to `04`–`18` (32 files: html + md), updating the grid links and h3 numbers. The result is fully consistent (grid shows 1..23, all links resolve), but it's a bigger change than the minimal-edit policy elsewhere — easy to revert wholesale if you prefer the old numbering.

## Deliberately not changed (flagged, not fixed)

- **Responsive breakpoints:** all 58 grids use `1400/1100/900/600px`; `23-tutorials/CLAUDE.md` documents `1200/900/600`. The corpus is self-consistent, so the doc likely lags reality — update CLAUDE.md or the grids, one or the other.
- **Red subcategory labels** on ~6 grids and **red key-point borders** on statistical-inference 06–19: series-wide style choices, left as-is.
- **Numbering gaps/order across sections** in a few grids (40, 44, 46; data-structures has no files 20+…19 now contiguous): numbers ascend within each subcategory; full renumbering was out of scope.
- **Pre-existing `.md` spec drift:** a handful of `.md` regeneration specs (e.g. tech-data 12, 17, 25, 27, 31, airflow) describe older versions of their html and didn't contain the text being fixed — they predate this job and may deserve a regeneration pass.
- Single-card subcategory sections (47-malware Defense, 56-payments): structural judgment calls.

## Artifacts

- Per-chunk detailed findings: `~/.claude/jobs/034e3e94/tmp/review/*.md` (126 files, per-file findings with line refs and fixes suggested)
- All HIGH findings in one file: `~/.claude/jobs/034e3e94/tmp/ALL-HIGHS.md`
- Automated sweep script: `~/.claude/jobs/034e3e94/tmp/sweep.py`
- Review/fix rubrics: `~/.claude/jobs/034e3e94/tmp/review-rubric.md`, `fix-rubric.md`

(These live in the background-job directory and disappear if the job is deleted — copy anything you want to keep.)
