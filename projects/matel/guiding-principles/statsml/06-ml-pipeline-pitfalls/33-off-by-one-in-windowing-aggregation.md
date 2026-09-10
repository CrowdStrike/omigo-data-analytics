# Pitfall: Off-By-One in Windowing / Aggregation

**Page type:** detail page (three `.card-section` blocks, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Off-By-One in Windowing / Aggregation

**Subtitle:** When inclusive vs exclusive time boundaries cause features to leak future data or miss critical information.

## The Problem

**Tags:** `the trap` (red pill), `windowing` (blue pill)

- **Ambiguous boundaries** — "last 7 days" could mean [t-7, t), [t-7, t], (t-7, t), or (t-7, t]
- **One bracket** — the difference either leaks the target event or drops the freshest data
- **Leakage form** — [label_date - 7d, label_date] includes the day of the target event itself
- **Miss form** — (label_date - 7d, label_date) excludes both endpoints and loses fresh signal
- **Tool ambiguity** — SQL BETWEEN is inclusive on both ends; pandas iloc slicing is half-open

*Example:* Predicting churn on day T, purchases_last_7d computed with BETWEEN T-7 AND T counts day-T purchases, leaking the label.

**Impact:** Boundary leakage can inflate offline AUC by 10-20 points, while excluding both endpoints can cost around 15 points of real signal.

### Visualization (canvas `c1`, 720×300)

Timeline diagram contrasting the wrong inclusive window with the correct half-open window.

- **Title (bold 14px, `#1a5276`, top center):** "Off-By-One: Inclusive vs Exclusive Boundaries".
- **Timeline:** gray (`#999`, 2px) horizontal line from x=80 to x=640 at y=100; tick marks every 80px labeled T-7, T-6, … T-0 (drawn as 'T-' + (7-i) for i=0..7) in 11px `#444`.
- **WRONG band:** translucent red band (`#e74c3c` at alpha 0.25, 20px tall) spanning the full timeline above it; bold red 11px label "WRONG: [T-7, T] (inclusive both)"; filled red 6px-radius dots at both endpoints; red 10px annotations near the right endpoint: "Includes day T!" / "LEAKAGE".
- **CORRECT band (at y=180):** translucent green band (`#27ae60` at alpha 0.25) spanning the timeline minus the last day; bold green 11px label "CORRECT: [T-7, T) (half-open)"; filled green dot at the left endpoint, open (white-filled, green-stroked) dot at the right endpoint (x=560); green 10px annotations "Excludes day T" / "(no leakage)".
- **Bottom notes (11px `#444`, left-aligned at x=80):** "SQL: WHERE date >= T-7 AND date < T" (y=250); "Pandas: .loc[T-7:T, ...]  ← INCLUSIVE (wrong!)" (y=268); "Pandas: .iloc[start:end]  ← EXCLUSIVE end (correct)" (y=286).

## Why It Happens

**Tags:** `root cause` (orange pill), `conventions` (blue pill)

- **No forced choice** — nothing in code makes engineers state which boundary convention they mean
- **Plausible output** — a window off by one day still yields feature values that look correct
- **SQL BETWEEN** — inclusive on both ends, which surprises developers coming from Python
- **pandas split brain** — .loc is inclusive on the end while .iloc is exclusive, so copies drift
- **No team convention** — each engineer picks an interpretation; reviews miss boundary semantics

**Root Cause:** The difference between [a, b] and [a, b) is a single bracket that is invisible in code review and passes silently without tests.

### Visualization (canvas `c2`, 720×300)

Side-by-side comparison of SQL BETWEEN (inclusive) and pandas iloc (half-open) boundary semantics.

- **Title (bold 14px, `#1a5276`, top center):** "Same Intent, Different Boundaries!".
- **Left panel:** 300×110 box at (30,50) with 2px `#e74c3c` stroke; bold blue 12px header "SQL BETWEEN T-7 AND T"; large red bracket glyphs "[" and "]" (bold 18px) framing a red timeline (x=75–290 at y=100) with filled red 5px dots at both ends, labeled "T-7" and "T"; bold red 11px "Inclusive BOTH ends" and 10px "Includes target day → LEAKS!".
- **Right panel:** 310×110 box at (380,50) with 2px `#27ae60` stroke; bold blue 12px header "pandas iloc[start:end]"; green brackets "[" and ")" framing a green timeline (x=425–645) with a filled green dot at the start and an open (white-filled, green-stroked) dot at the end, labeled "start" and "end"; bold green 11px "Exclusive upper bound" and 10px "Excludes end → safe".
- **Center annotations (centered):** bold red 13px 'Same intent: "last 7 days of features"' at y=200; 11px `#444` "But SQL includes day T (leaks label), while iloc excludes it (correct)" at y=220.
- **Bottom warnings (centered):** bold red 11px "Without tests, the 1-day difference is invisible in code review" at y=260; 10px `#555` "Bonus trap: pandas .loc[] IS inclusive on both ends — opposite of .iloc[]!" at y=280.

## The Correct Approach

**Tags:** `the fix` (green pill), `windowing` (blue pill)

- **One convention** — standardize on the half-open window [start, end) across every pipeline
- **Machine-checked** — enforce the rule with helper functions and assertions, not memory
- **Boundary tests** — assert max(feature_timestamp) < prediction_timestamp for every row
- **Document it** — comment the bracket notation used in every feature engineering window
- **Safe API** — a helper like get_window(start, end_exclusive) makes wrong windows hard to write

**Fix:** Every feature window needs an automated test proving max(feature_ts) < label_ts — if it fails, you have leakage.

### Visualization (canvas `c3`, 720×300)

Three-step validated pipeline flow plus an assertion box and helper-function note.

- **Title (bold 14px, `#1a5276`, top center):** "Correct Pipeline: Enforce [start, end) + Validate".
- **Step boxes** (70px tall at y=55, light fills, bold 12px blue titles, connected by green (`#27ae60`) arrows with filled arrowheads):
  - "Feature Window" (170px wide at x=40, 2px `#1a5276` stroke, fill `rgba(26,82,118,0.08)`): bold green 13px "[T-7, T)", 10px `#444` "exclusive upper bound".
  - "Validation" (180px wide at x=275, 2px `#27ae60` stroke, fill `rgba(39,174,96,0.08)`): green 11px "max_ts < T ?", bold 16px green ✓.
  - "Model Input" (160px wide at x=520, 3px `#27ae60` stroke, fill `rgba(39,174,96,0.08)`): green bold title, 11px "No leakage", bold 16px ✓.
- **Assertion box:** 520×45 at (100,160), fill `rgba(39,174,96,0.08)`, 2px `#27ae60` stroke; bold 12px monospace green "assert features.timestamp.max() < label_date"; 10px `#444` "This test runs on every pipeline execution — catches leakage before training".
- **Bottom summary (centered):** bold blue 11px "Helper function enforces convention:" at y=240; green 12px monospace "def get_features(start_inclusive, end_exclusive):" at y=260; 10px `#555` "API naming makes it impossible to forget which end is which" at y=280.

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (full width, border-collapse) with one row: `.text-col` `<td>` (45%) and `.viz-col` `<td>` (55%). Text cell holds a `.tags` div of pill spans, a `<ul>` of `<li><b>Label</b> — sentence</li>` bullets, an optional italic `.example` paragraph (only in section 1 on this page), and a `.key-point` callout (`#f8f9fa` background, 3px `#e74c3c` left border, `<strong>` lead word). The "Boundary tests" bullet uses `&lt;` for the less-than sign.
- **Tag pills:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width: 100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each canvas declares `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#444`/`#555`, box fills `rgba(26,82,118,0.08)` and `rgba(39,174,96,0.08)`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
