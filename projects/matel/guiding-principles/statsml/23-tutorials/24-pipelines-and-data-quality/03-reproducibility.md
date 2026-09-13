# Reproducibility

**Page type:** detail page (tutorial layout: h1 + subtitle, then one `.card-section` per concept, each an h2 + two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Reproducibility

**Subtitle:** Rerun it and get the same numbers — pinned inputs, versioned code, no hidden randomness

## Why Does the March Report Show Different Numbers Today?

**Tags:** `core idea` (blue), `running example` (green)

- **April 2** — the March revenue report runs and shows $1,240,000; leadership signs off
- **August 12** — an auditor reruns the same report and gets $1,192,000
- **Nothing "broke"** — no error, no crash; the same query returned different numbers
- **Two quiet changes** — the source table changed, and the report code changed
- **The question** — which number is "the March revenue"? Nobody can say

*Example:* Same notebook, same query, four months apart: $1,240,000 then $1,192,000 — and both printouts look equally official.

**Key point:** a result is reproducible when rerunning it gives the same numbers. When it doesn't, you can't tell a bug from a data change from a bad memory.

### Visualization (canvas `c1`, 720×300)

Side-by-side report cards: the same report run twice with two different answers.

- **Title (bold 16px, `#1a5276`, top center):** "One Report, Two Runs, Two Answers".
- **Report cards (230×158, `#f8f9fa` fill, 2px colored stroke, tinted header strip at 15% alpha of the card color):**
  - Left at (50,58), blue `#2a78d6`: header bold 13px "run: April 2, 2026"; 12px `#6b7280` "the number leadership approved"; bold 12px "March revenue"; value bold 22px blue "$1,240,000"; 12px `#6b7280` "same query, same notebook".
  - Right at (440,58), orange `#d95926`: header "run: August 12, 2026"; "the auditor's rerun"; "March revenue"; value bold 22px orange "$1,192,000"; "same query, same notebook".
- **Arrow:** gray `#6b7280` arrow between the cards labeled 12px "4 months later".
- **Callouts (centered):** bold 14px magenta `#d55181` at y=250: "−$48,000 (−3.9%) with no error and no explanation"; 12px `#6b7280` at y=274: "which one goes in the audit file? (illustrative numbers)".

## Finding the Missing $48,000: Three Usual Suspects

**Tags:** `worked example` (green), `core idea` (blue)

- **Input drift** — in June, $35,000 of late refunds were backfilled into March's source rows
- **Code drift** — in July, a new test-order filter shipped, cutting another $13,000
- **Nondeterminism** — none here, but random seeds and unordered "top 1000" do the same
- **The math** — $1,240,000 − 35,000 − 13,000 = $1,192,000: fully explained
- **The cost** — four hours of archaeology for one number, because nothing was pinned

*Example:* The refunds were real and the filter was right — yet the report changed with no record of either happening.

**Key point:** every irreproducible number decomposes into input changes + code changes + randomness. Pin all three and the number cannot move.

### Visualization (canvas `c2`, 720×300)

Waterfall chart: decomposing the $48,000 gap into two drops.

- **Title (bold 15px, `#1a5276`, top center):** "Decomposing the Gap: $1,240,000 − 35,000 − 13,000 = $1,192,000".
- **Axes:** padding top 56, bottom 66, left 88, right 24; y scale $1,150,000 to $1,260,000 with tick labels "$1150k", "$1200k", "$1250k" (12px `#6b7280`); gridlines `#e5e9ef`; axis lines `#999`.
- **Columns (120px wide; fill at 35% alpha of the stroke color, 2px stroke; value bold 12px in the column color above; label 12px `#6b7280` below):**
  - "April run" — full bar from axis floor to 1,240,000, blue `#2a78d6`, value "$1,240,000"
  - "June: refunds" — floating segment 1,240,000→1,205,000, orange `#d95926`, value "−$35,000"
  - "July: new filter" — floating segment 1,205,000→1,192,000, magenta `#d55181`, value "−$13,000"
  - "August run" — full bar to 1,192,000, green `#008300`, value "$1,192,000"
- **Connectors:** thin dashed (4/3) gray lines between consecutive column tops.
- **Callout (bold 13px orange `#d95926`, centered at bottom):** "input drift + code drift — four hours to reconstruct, zero seconds if it had been stamped".

## Pin the Inputs, Version the Code, Fix the Seeds

**Tags:** `rule of thumb` (green), `where it's used` (blue)

- **Pinned inputs** — read a frozen snapshot (orders_2026_03_v1), not the live table
- **Versioned code** — the report records which code version (git commit) produced it
- **Deterministic logic** — fixed random seeds, explicit sort orders, no "first 1000 rows"
- **Stamp the output** — every report carries: snapshot name, code version, run date
- **Then reruns match** — the August rerun of the April recipe returns $1,240,000 exactly

*Example:* Footer of the fixed report: "snapshot orders_2026_03_v1 · code a3f92c1 · run 2026-04-02" — rerun those three and the cents match.

**Rule of thumb:** reproducibility is not a tool you buy — it is three habits: freeze what goes in, record what ran, and remove luck from the middle.

### Visualization (canvas `c3`, 720×300)

Flow diagram: three pinned inputs converging on one report job, fanning out to three identical outputs.

- **Title (bold 15px, `#1a5276`, top center):** "Freeze the Three Moving Parts and the Number Stops Moving".
- **Pin boxes (left column, 190×52 at x=40, `#f8f9fa` fill, 2px colored stroke, bold 13px label + 12px `#6b7280` sub):**
  - "pinned input" / "orders_2026_03_v1", blue `#2a78d6` (y≈70)
  - "versioned code" / "commit a3f92c1", violet `#4a3aa7` (y≈140)
  - "deterministic logic" / "fixed seed, sorted", aqua `#199e70` (y≈210)
- **Center box:** (312,108,140×64) filled `rgba(0,131,0,0.07)`, stroked green `#008300`, labeled bold 13px green "March report" / 12px `#6b7280` "run anytime"; gray arrows converge into it from the pin boxes.
- **Output boxes (right column, 168×46 at x=532, `#f8f9fa` fill, green 2px stroke, bold 13px green "$1,240,000" over 12px `#6b7280` sub):** "run in April" (y≈70), "run in August" (y≈140), "run next year" (y≈210); green arrows fan out from the center box.
- **Callout (bold 13px green `#008300`, centered at y=278):** "same three pins in, same number out — every single time".

## If You Can't Rerun It, You Can't Debug It

**Tags:** `why it matters` (orange), `common mistake` (red)

- **Debugging** — a result you cannot reproduce is a bug you cannot isolate or prove fixed
- **Model training** — retraining on a moved table gives a different model, unexplained
- **Handoffs** — a teammate rerunning your notebook should get your numbers, not new ones
- **Audits** — "show how you got $1.24M" must still be answerable a year later
- **The trap** — code in git but data read live: reruns still drift with the table

*Example:* "The model got worse after retraining" — was it the code change or 60 days of new data? With nothing pinned, no one knows.

**Common mistake:** half-reproducibility — versioning the code while reading live tables. The number still moves, and now it moves with false confidence.

### Visualization (canvas `c4`, 720×300)

Two-series line chart: the "March revenue" number rerun month after month — live table drifts down, pinned snapshot stays flat.

- **Title (bold 15px, `#1a5276`, top center):** "\"March Revenue\" Rerun Month After Month".
- **Data:** months Apr–Nov; live-table reruns `[1240000, 1240000, 1205000, 1194000, 1192000, 1190500, 1188000, 1187000]`; pinned-snapshot reruns constant `1240000` for all eight months.
- **Axes:** padding top 56, bottom 56, left 78, right 185; y scale $1,170,000 to $1,255,000 with tick labels "$1180k", "$1210k", "$1240k" (12px `#6b7280`); gridlines `#e5e9ef`; axis lines `#999`; x labels Apr…Nov (12px) with axis caption "month the report was rerun" in `#6b7280`.
- **Series:** pinned line solid green `#008300`, width 3, 4px dots; live line dashed (7/4) magenta `#d55181`, width 3, 4px dots.
- **In-chart annotation (bold 13px magenta near the June point):** "live table: a history" / "that keeps rewriting itself".
- **Legend (top right, 12px):** green swatch "pinned snapshot"; magenta swatch "live table".

## Regeneration instructions

- **Template:** tutorials topic-page layout. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` line, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` line, one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Canvas:** intrinsic 720×300 attributes; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; shared `arrow()` drawing helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette:** shared `P` object — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; site palette `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
