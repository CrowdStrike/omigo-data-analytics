# Mixed-Effects Models

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Mixed-Effects Models

**Subtitle:** When data comes in groups — branches, schools, patients — a mixed-effects model lets small groups borrow strength from the rest, pulling noisy averages toward the shared mean

## Six Branches Share One Chain

**Tags:** `core idea` (blue), `borrowing strength` (green), `shrinkage` (orange)

- **The chain** — a coffee chain has six branches, A–F, rated by customers; the chain average is 4.0 stars
- **Uneven data** — branch A has 4 reviews averaging 4.8 stars; branch F has 300 reviews averaging 3.9
- **The doubt** — 4 glowing reviews could easily be luck; 300 reviews at 3.9 is hard to argue with
- **Borrowing strength** — the model pulls each branch toward 4.0, and pulls harder when n is small
- **After pooling** — A's 4.8 shrinks to 4.13 while F's 3.9 barely moves, landing at 3.91
- **The name** — "mixed" = one shared chain-level effect (fixed) plus a per-branch wiggle (random)

*Example (italic):* Branch A's 4.8 from 4 reviews gets read as "probably a good branch, plus some luck" — its estimate lands at 4.13, not 4.8.

**Key point:** A mixed-effects model treats groups as siblings, not strangers: each estimate blends the group's own data with the family average, weighted by how much data the group has.

### Visualization (canvas `c1`, 720×300)

Dot-and-arrow shrinkage plot: for each of the six branches, a blue dot at the raw average and a green dot at the mixed-model estimate, joined by a gray arrow pointing toward the chain-average line.

- **Title (bold 15px, `#1a5276`, top center):** "Raw Branch Averages vs Mixed-Model Estimates (illustrative)".
- **Data:** branches A–F; review counts `[4, 8, 20, 50, 120, 300]`; raw averages `[4.8, 3.2, 4.4, 3.8, 4.1, 3.9]`; shrunk estimates `[4.13, 3.77, 4.20, 3.86, 4.09, 3.91]`.
- **Axes:** y range 3.0–5.0 mapped from baseline y=245 up to y=60; y-axis at x=70 with ticks 3.0, 3.5, 4.0, 4.5, 5.0 (12px `#444`); branch x positions `[130, 225, 320, 415, 510, 605]`; labels below baseline 12px `#444`: "A (n=4)", "B (n=8)", "C (n=20)", "D (n=50)", "E (n=120)", "F (n=300)".
- **Chain-average line:** dashed `#6b7280` (dash 4/3) horizontal line at the y of 4.0, labeled "chain average 4.0" bold 12px `#6b7280` at the right end.
- **Marks:** per branch, blue `#2a78d6` 6px dot at raw value, green `#008300` 6px dot at shrunk value, 2px `#9aa4af` arrow (line + small arrowhead; arrowhead omitted when the shift is under ~16px) from raw dot to shrunk dot.
- **Legend (top left, from x=85, y=50):** blue dot + "raw average" and green dot + "mixed-model estimate", 12px `#444`.
- **Annotations:** magenta `#d55181` bold 12px near branch A: "4 reviews: pulled hard"; green `#008300` bold 12px near branch F: "300 reviews: barely moves".

## The Shrinkage Recipe by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The recipe** — estimate = w × branch average + (1 − w) × 4.0, with weight w = n / (n + 20)
- **Phantom reviews** — the 20 acts like 20 imaginary reviews at the chain average added to every branch
- **Branch A** — w = 4 / 24 ≈ 0.17, so estimate = 0.17 × 4.8 + 0.83 × 4.0 = 4.13
- **Branch F** — w = 300 / 320 ≈ 0.94, so estimate = 0.94 × 3.9 + 0.06 × 4.0 = 3.91
- **Fitted, not chosen** — the model estimates the 20 from how much branches genuinely differ
- **Sliding scale** — at exactly n = 20 the branch and the chain get equal say, w = 0.5

*Example (italic):* Branch C has exactly 20 reviews at 4.4, so its estimate splits the difference: 0.5 × 4.4 + 0.5 × 4.0 = 4.2.

**Key point:** Shrinkage is just a weighted average with weight n / (n + k): lots of data means trust the branch, little data means lean on the chain — and k is learned from the data, not hand-picked.

### Visualization (canvas `c2`, 720×300)

Curve of the shrinkage weight w = n / (n + 20) against review count n, with the six branches marked as dots on the curve.

- **Title (bold 15px, `#1a5276`, top center):** "How Much a Branch Trusts Its Own Data: w = n / (n + 20)".
- **Axes:** origin x=70, plot width 560 (to x=630), baseline y=240, plot height 175 (top y=65); x is n from 0 to 320 with ticks 0, 50, 100, 150, 200, 250, 300 (12px `#444`); y is w from 0 to 1 with ticks 0, 0.25, 0.5, 0.75, 1.0; light `#e5e9ef` horizontal gridlines at each y tick.
- **Curve:** blue `#2a78d6` 3px line, computed deterministically as w = n / (n + 20) at every integer n from 0 to 320 (no randomness).
- **Branch dots:** green `#008300` 6px dots at (n, w) pairs `(4, 0.17)`, `(8, 0.29)`, `(20, 0.50)`, `(50, 0.71)`, `(120, 0.86)`, `(300, 0.94)`, each with its branch letter A–F bold 12px `#008300` beside it.
- **Guides:** dashed `#6b7280` (dash 4/3) vertical line at n=20 and horizontal line at w=0.5, meeting at branch C's dot; orange `#d95926` bold 12px annotation at the crossing: "n = 20: equal say".
- **Annotations:** magenta `#d55181` bold 12px near A: "A: w = 0.17, mostly the chain"; green bold 12px near F: "F: w = 0.94, mostly itself".
- **Caption (12px `#444`, bottom right):** "w is the weight on the branch's own average".

## Three Ways to Estimate the Same Branches

**Tags:** `where it's used` (blue), `partial pooling` (green), `hierarchy` (orange)

- **No pooling** — fit each branch alone: A stays at 4.8 even though 4 reviews is nearly all noise
- **Complete pooling** — one number, 4.0, for everyone: real branch differences are erased entirely
- **Partial pooling** — the mixed model lands in between: 4.13, 3.77, 4.20, 3.86, 4.09, 3.91
- **Same pattern** — students within schools, patients within hospitals, sessions within users
- **Random intercepts** — each group gets its own baseline, drawn from one shared distribution

*Example (italic):* A hospital study with 30 clinics uses the same trick — a clinic with 5 patients borrows strength from the other 29 clinics.

**Key point:** Partial pooling is the middle road: it keeps genuine group differences that complete pooling erases, while taming the small-sample noise that no pooling swallows whole.

### Visualization (canvas `c3`, 720×300)

Three-column shrinkage plot: the six branch estimates drawn as dots in a "no pooling" column, a "partial pooling" column, and a "complete pooling" column, with thin lines tracing each branch across the three strategies.

- **Title (bold 15px, `#1a5276`, top center):** "Three Strategies for the Same Six Branches (illustrative)".
- **Data:** no pooling `[4.8, 3.2, 4.4, 3.8, 4.1, 3.9]`; partial pooling `[4.13, 3.77, 4.20, 3.86, 4.09, 3.91]`; complete pooling: 4.0 for all six branches.
- **Axes:** y range 3.0–5.0 mapped from baseline y=245 up to y=60; y-axis at x=70 with ticks 3.0, 3.5, 4.0, 4.5, 5.0 (12px `#444`); column x positions 165 (no pooling), 360 (partial), 555 (complete); column headings bold 12px `#1a5276` at y=50 above each column.
- **Connecting lines:** for each branch, a 1.5px `#d0d7de` polyline through its three dots, drawn before the dots.
- **Dots:** no-pooling column blue `#2a78d6` 5px; partial column green `#008300` 5px; complete column a single orange `#d95926` 6px dot at 4.0 labeled "4.0 for everyone" bold 12px orange to its right.
- **Branch labels:** letters A–F 12px `#444` just left of each no-pooling dot.
- **Annotation (magenta `#d55181` bold 13px, bottom center at y=285):** "partial pooling keeps real differences but tames the noise".

## The Leaderboard Trap

**Tags:** `common mistake` (red), `ranking` (orange)

- **Raw ranking** — sorted by raw average, branch A (4 reviews) sits at #1 and looks like the flagship
- **Shrunk ranking** — after pooling, C (4.20 from 20 reviews) takes #1 and A drops to #2 at 4.13
- **Extremes = small n** — the top and bottom of raw leaderboards are usually the smallest samples
- **Real stakes** — bonuses, closures, and "best school" lists all misfire when ranked by raw means
- **Not cheating** — shrinkage never edits the data; it just admits 4 reviews carry less evidence

*Example (italic):* Head office almost crowned branch A "branch of the year" on 4 reviews; the shrunk board crowns C instead.

**Common mistake:** Ranking groups by raw averages when sample sizes differ — the leaderboard rewards small noisy groups, not genuinely good ones. Shrink first, rank second.

### Visualization (canvas `c4`, 720×300)

Dual-panel horizontal-bar leaderboards: the six branches ranked by raw average (left) vs ranked by shrunk estimate (right), with branch A highlighted to show its drop from #1 to #2, split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Raw Leaderboard vs Shrunk Leaderboard (illustrative)".
- **Left panel (raw order):** heading bold 12px `#444` "ranked by raw average" at y=55; rows top to bottom `A 4.8 (n=4)`, `C 4.4 (n=20)`, `E 4.1 (n=120)`, `F 3.9 (n=300)`, `D 3.8 (n=50)`, `B 3.2 (n=8)`; row y centers 85, 115, 145, 175, 205, 235; bars 16px tall starting at x=115, length = (value − 3.0) / 2.0 × 200 px; fill `rgba(42,120,214,0.45)` except branch A in `rgba(213,81,129,0.5)`; row label 12px `#444` left of each bar, value bold 12px `#2c3e50` at bar end.
- **Right panel (shrunk order):** heading "ranked by shrunk estimate"; rows top to bottom `C 4.20`, `A 4.13`, `E 4.09`, `F 3.91`, `D 3.86`, `B 3.77`; same row y centers; bars start at x=465, same length scale; fill `rgba(0,131,0,0.4)` except branch A in `rgba(213,81,129,0.5)`.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=42 to h-12.
- **Annotations:** orange `#d95926` bold 12px near A's right-panel bar: "A: #1 → #2"; green `#008300` bold 12px near C's right-panel bar, on two lines: "C wins with" / "20 reviews".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all arrays hardcoded exactly as specified above; the c2 curve is the deterministic formula w = n / (n + 20) — no `Math.random()` anywhere.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
