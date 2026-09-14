# The Pull Request Lifecycle

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Pull Request Lifecycle

**Subtitle:** A pull request is the unit of change in modern development — open it, let machines gate it, let humans review it, then pick one of three merge buttons that write very different histories

## One Bug Fix, Four Gates

**Tags:** `core idea` (blue), `open → CI → review → merge` (green), `automated gates` (orange)

- **The fix** — PR #482 changes one line so an orders total rounds once, not twice, plus a test
- **Open** — a branch off main and a description: what changed, why, how it was verified
- **The description** — it is documentation; future archaeologists read it long after the author left
- **CI first** — tests, lint, and build run automatically; required checks block merging until green
- **Machines veto early** — a red build stops the PR before any human spends review time on it
- **Review rounds** — comments arrive, new commits answer them, CI reruns, then approval unlocks merge

*Example (italic):* #482 opens at 9:00, CI goes green at 9:12, a reviewer asks for a test rename at 10:30, the fix lands at 11:00, approval comes at 13:45, merge at 13:50.

**Key point:** The lifecycle is a funnel of cheap gates before expensive ones — automated checks veto first so human reviewers only ever look at code that already builds and passes tests.

### Visualization (canvas `c1`, 720×300)

Horizontal stage-flow diagram of PR #482's lifecycle: four stage boxes on a main track with a review loop-back arc, and the day's timestamps beneath each stage.

- **Title (bold 15px, `#1a5276`, top center):** "PR #482: Open → CI → Review → Merge, With One Review Round".
- **Main track:** 2px `#999` horizontal line at y=160 from x=40 to x=690, 3px arrowheads between boxes.
- **Stage boxes (rounded 8px, 40px tall, centered on y=160, 12px `#2c3e50` bold labels):** "Open" blue `rgba(42,120,214,0.15)` border `#2a78d6` at x=55 width 105; "CI checks" orange `rgba(217,89,38,0.15)` border `#d95926` at x=205 width 115; "Review" violet `rgba(74,58,167,0.12)` border `#4a3aa7` at x=365 width 115; "Merge" green `rgba(0,131,0,0.12)` border `#008300` at x=530 width 105.
- **Loop-back arc:** dashed `#6b7280` (dash 4/3) curve from the Review box top (x≈420, y=140) arcing up to y=75 and back down to the CI box top (x≈260, y=140), arrowhead at the CI end; bold 12px `#6b7280` label "new commits → CI reruns" centered above the arc at y=62.
- **Gate marker:** bold 13px red `#e74c3c` "✗ red build blocks here" at (x=205, y=225) with a short red tick on the track after the CI box.
- **Timestamps (12px `#444`, under each box at y=250):** "9:00" under Open, "9:12 green / 11:12 green" under CI, "10:30 comment, 13:45 approve" under Review, "13:50" under Merge.
- **Caption (12px `#444`, bottom right):** "timestamps illustrative".

## Three Merge Buttons, Three Histories

**Tags:** `worked example` (blue), `merge / squash / rebase` (green)

- **The branch** — #482 carries 3 commits: `a1` fix rounding, `a2` add test, `a3` fix typo; main has `M1`, `M2`
- **Merge commit** — keeps all 3 branch commits plus a join node: 6 nodes, truthful but noisy history
- **Squash** — the whole PR becomes ONE commit on main: 3 nodes total, clean and linear, steps lost
- **Rebase-merge** — replays `a1`,`a2`,`a3` one by one onto main: 5 nodes, linear AND granular, new hashes
- **How to choose** — ask one question: will anyone need the intermediate commits later (bisect, revert)?
- **Company default** — squash is the common default because most intra-branch commits are "fix typo" noise

*Example (italic):* After squash, main shows one commit "Fix double rounding in order totals (#482)"; after rebase-merge it shows three commits with the same diffs but brand-new hashes.

**Key point:** All three buttons apply the same code change — they differ only in what history main remembers: everything plus a join (merge), one summary commit (squash), or each step replayed linearly (rebase).

### Visualization (canvas `c2`, 720×300)

Three commit-graph rows showing what main's history looks like after each merge strategy, starting from main = `M1, M2` and a branch with `a1, a2, a3`.

- **Title (bold 15px, `#1a5276`, top center):** "Same PR, Three Histories: Merge Commit vs Squash vs Rebase".
- **Rows at y = 90, 165, 240; each row:** left-aligned bold 12px `#1a5276` strategy label at x=20 ("merge commit", "squash", "rebase-merge"), then a 2px `#999` main line from x=170 to x=690 with commit dots (9px radius, 11px labels below each dot).
- **Row 1 (merge commit):** blue `#2a78d6` dots `M1` at x=210, `M2` at x=290; branch arc rising to y=55 with green `#008300` dots `a1` x=380, `a2` x=460, `a3` x=540; violet `#4a3aa7` join dot `J` on the main line at x=620 with both lines converging into it; 11px `#6b7280` note "6 nodes, full truth + noise" at x=640 above the row.
- **Row 2 (squash):** blue dots `M1` x=210, `M2` x=290; single green dot `S` at x=430 labeled "S (#482)"; 11px `#6b7280` note "3 nodes, steps lost" at x=560.
- **Row 3 (rebase-merge):** blue dots `M1` x=210, `M2` x=290; green dots `a1'` x=400, `a2'` x=490, `a3'` x=580; 11px `#6b7280` note "5 nodes, new hashes" at x=640 above the row.
- **Annotation (bold 12px orange `#d95926`, near x=400, y=282):** "primes mean rewritten hashes — same diff, new identity".
- **Caption (12px `#444`, bottom right):** "commit graphs schematic".

## Why Small PRs Win

**Tags:** `where it's used` (blue), `PR hygiene` (green), `rule of thumb` (orange)

- **Review collapses with size** — at 100 changed lines reviewers catch ~9 issues per 100 lines; at 1,600 lines, ~1
- **The rubber stamp** — past roughly 400 lines, "LGTM" replaces reading; big PRs get worse review, not more
- **Draft PRs** — open early as a draft to get direction feedback before polishing the wrong design
- **Link the issue** — a PR tied to its issue tells archaeologists what problem the diff was solving
- **Stay current** — a long-lived branch drifts from main; merge main in (noisy, safe) or rebase (clean, rewrites)
- **Merge queues** — busy repos queue approved PRs and re-test each against the latest main before landing

*Example (italic):* A 1,600-line PR skimmed at 1 issue per 100 lines yields ~16 catches; split into four 400-line PRs read at 4 per 100, the same code yields ~64.

**Key point:** PR hygiene is mostly one rule — keep the scope small — because every downstream step (CI signal, review depth, revert safety, merge conflicts) degrades with size.

### Visualization (canvas `c3`, 720×300)

Bar chart of review effectiveness versus PR size: issues found per 100 changed lines drops as the PR grows.

- **Title (bold 15px, `#1a5276`, top center):** "Review Depth Collapses as the PR Grows".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = issues found per 100 lines, 0 to 10, gridlines `#e5e9ef` at 2.5/5/7.5, 12px `#444` y-tick labels 0/5/10; x = PR size in changed lines, 5 bars with 12px `#444` labels beneath.
- **Bars (70px wide, centered at x = 130, 245, 360, 475, 590), heights scaled 18px per issue:** "100 lines" green `#008300` value 9; "200 lines" green value 7; "400 lines" orange `#d95926` value 4; "800 lines" red `#e74c3c` value 2; "1,600 lines" red value 1. Bold 12px value labels ("9", "7", "4", "2", "1") above each bar in the bar's color.
- **Threshold marker:** vertical dashed `#6b7280` (dash 4/3) line at x=418 (between the 400 and 800 bars) from y=65 to y=245, bold 12px `#6b7280` label "~400-line rubber-stamp line" at its top.
- **Annotation (bold 13px red `#e74c3c`, near x=520, y=110):** "big PRs get LGTM, not review".
- **Caption (12px `#444`, bottom right):** "issues per 100 lines illustrative".

## The Merge Button Is Not Cosmetic

**Tags:** `common mistake` (red), `history` (orange)

- **The mistake** — treating merge / squash / rebase as a style choice instead of a data-retention choice
- **Squash regret** — squash a 940-line PR and `git bisect` can only tell you "the bug is somewhere in here"
- **Granular payoff** — with rebase-merged commits, bisect lands on one 38-line commit you can read in minutes
- **Rebase regret** — rewritten hashes orphan any comment, tag, or CI record pinned to the old commits
- **The test** — before picking a repo default, ask which failure you would rather debug six months from now

*Example (italic):* A production bug bisects to squashed commit "#517 checkout revamp" — 940 lines, 14 files — and the team spends a day re-reviewing a diff that was once a dozen readable commits.

**Common mistake:** Assuming the merge strategy only affects how the graph looks. It decides what `git bisect`, `git revert`, and future readers can recover — history you squash away is gone for good.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same production bug hunted by `git bisect` on a squashed history versus a rebase-merged (granular) history.

- **Title (bold 15px, `#1a5276`, top center):** "Six Months Later: Bisect on Squashed vs Granular History".
- **Row 1 (y=100), label 12px `#444` at x=20:** "squashed"; blue `rgba(42,120,214,0.15)` rounded box at x=140 width 170 labeled "bug found — run bisect" (12px), 3px arrow to a red `rgba(231,76,60,0.12)` box at x=400 width 250 labeled "lands on 1 commit: 940 lines, 14 files", bold 12px red `#e74c3c` "✗ a day of re-reading" beneath the red box at y=145.
- **Row 2 (y=215), label:** "rebase-merged"; identical blue box "bug found — run bisect" at x=140 width 170, 3px arrow to a green `rgba(0,131,0,0.12)` box at x=400 width 250 labeled "lands on 1 commit: 38 lines, 1 file", bold 12px green `#008300` "✓ read in minutes, revert cleanly" beneath at y=260.
- **Box style:** 44px tall, 8px radius, 12px `#2c3e50` text, borders in the fill's solid color (`#2a78d6` / `#e74c3c` / `#008300`).
- **Annotation (bold 13px orange `#d95926`, centered near y=285):** "squash trades tomorrow's forensics for today's clean log".
- **Caption (12px `#444`, top right under title):** "line counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); timestamps, review-effectiveness bars (9/7/4/2/1 issues per 100 lines at 100/200/400/800/1,600 lines), and bisect line counts (940 vs 38) are invented and labeled illustrative; the commit-graph node counts (6 / 3 / 5) follow exactly from a 3-commit branch onto a 2-commit main.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
