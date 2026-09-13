# Backfills

**Page type:** detail page (tutorial layout: h1 + subtitle, then one `.card-section` per concept, each an h2 + two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Backfills

**Subtitle:** Running the pipeline for the past — recomputing old days after a fix, one date at a time

## A Bug Fixed Today, 90 Days of Wrong History

**Tags:** `core idea` (blue), `running example` (green)

- **The bug** — since May 26, the pipeline counted discounts twice, understating revenue ~6%
- **Fixed today** — Aug 24's code is right, but the fix only helps days that run from now on
- **90 dirty days** — May 26 to Aug 23 sit in the warehouse, computed by the buggy code
- **The backfill** — rerun the fixed pipeline once per old day, overwriting each day's rows
- **Nothing exotic** — a backfill is the normal nightly job, just pointed at past dates

*Example:* for each day from May 26 to Aug 23: run the pipeline with date = d — that loop is the entire backfill.

**Key point:** a backfill is running the pipeline for the past. The code fix repairs tomorrow; only the backfill repairs history.

### Visualization (canvas `c1`, 720×300)

Partition strip diagram: a row of daily-partition cells with the buggy region highlighted and a backfill loop arrow.

- **Title (bold 16px, `#1a5276`, top center):** "The Warehouse as Daily Partitions: 90 of Them Are Wrong".
- **Strip:** 30 cells (20×40, 1px spacing) starting at x=50, y=100 — cells 1–9 "clean" filled `rgba(42,120,214,0.30)` stroked blue `#2a78d6`; cells 10–29 "buggy" filled `rgba(213,81,129,0.35)` stroked magenta `#d55181`; last cell "today" filled `rgba(0,131,0,0.35)` stroked green `#008300`.
- **Region labels (bold 12px over 12px):** blue "correct" / "before May 26" over the clean region; magenta "90 days computed by the buggy code" / "May 26 — Aug 23" over the buggy region; green "today" / "fixed code" below the last cell.
- **Backfill loop:** green 2.5px path from the today cell, down and left under the strip, ending in a green arrow back up into the start of the buggy region; bold 13px green caption centered below: "backfill: run the fixed pipeline for each of the 90 old dates, overwriting each day".
- **Caption (12px `#6b7280`, centered at y=280):** "each cell = one daily partition of ~12,000 rows (strip compressed for display)".

## The 90-Day Loop, by the Numbers

**Tags:** `worked example` (green), `core idea` (blue)

- **Scope** — 90 daily partitions of roughly 12,000 rows each: about 1.08M rows to redo
- **One day at a time** — the job overwrites June 3's partition, then June 4's, independently
- **Spot check** — June 3 revenue: $41,300 → $44,050 corrected (+$2,750)
- **Progress is visible** — after 30 days done, 60 partitions still show the old numbers
- **Restart-safe** — if the loop dies at day 41, rerun it; days just get overwritten again

*Example:* The whole backfill ran as 90 small jobs over one weekend; Monday's dashboard quietly showed the corrected quarter.

**Key point:** because each day is recomputed independently, a backfill is 90 small checkable jobs — not one giant terrifying one.

### Visualization (canvas `c2`, 720×300)

Two-series line chart with shaded gap: buggy vs backfilled daily revenue over the 90-day window.

- **Title (bold 15px, `#1a5276`, top center):** "Daily Revenue: Buggy Numbers vs the Backfilled Correction".
- **Data (7 weekly points labeled May 26, Jun 9, Jun 23, Jul 7, Jul 21, Aug 4, Aug 18):**
  - buggy: `[40800, 41300, 42100, 40200, 43000, 41700, 42600]`
  - corrected: `[43400, 44050, 44800, 42800, 45700, 44400, 45300]`
- **Axes:** padding top 56, bottom 56, left 70, right 175; y scale $38,000 to $47,000 with tick labels "$38k", "$42k", "$46k" (12px `#6b7280`); gridlines `#e5e9ef`; axis lines `#999`; x labels 12px.
- **Gap fill:** area between the two lines shaded `rgba(0,131,0,0.12)`.
- **Series:** buggy line dashed (7/4) magenta `#d55181`, width 3; corrected line solid green `#008300`, width 3.
- **Annotations:** bold 12px aqua `#199e70` top-left: "Jun 3: $41,300 → $44,050"; bold 13px green centered below x labels: "~6% recovered on every one of the 90 days"; 12px `#6b7280` right-aligned on the same line: "illustrative series".
- **Legend (top right, 12px):** magenta swatch "buggy (before)"; green swatch "backfilled (after)".

## Idempotency Is What Makes Backfills Boring

**Tags:** `rule of thumb` (green), `design` (orange)

- **Overwrite pattern** — each run replaces one day's partition, so old rows can't linger
- **Mutation pattern** — jobs that UPDATE rows in place or append deltas can't just rerun
- **Terrifying version** — appending 90 days onto existing history doubles the quarter
- **Order doesn't matter** — independent days can backfill in parallel or in any order
- **Everyday uses** — new metric for history, late-arriving data, recovered outage days

*Example:* Team A backfilled with one command; team B's UPDATE-in-place pipeline needed a week of hand-written repair SQL.

**Rule of thumb:** the property that makes reruns safe — same input, same output, any number of runs — is exactly what turns "recompute 90 days" into a for-loop.

### Visualization (canvas `c3`, 720×300)

Two-panel bar comparison: rows per daily partition after a double-run backfill — overwrite stays flat, append doubles.

- **Title (bold 15px, `#1a5276`, top center):** "Backfilling 90 Days: Overwrite Stays 12,000/day, Append Doubles".
- **Divider:** vertical dashed (4/3) line `#bdc3c7` at x=360.
- **Left panel (green `#008300`), title bold 13px "idempotent: overwrite each partition":** bars for Jun 1–Jun 5 all 12,000 (labels "12k"); dashed gray reference line at 12,000 labeled 11px "true 12,000"; note bold 12px green: "rerun it twice by accident — still 12,000".
- **Right panel (magenta `#d55181`), title "non-idempotent: append onto history":** bars `[24000, 24000, 24000, 12000, 12000]` (labels "24k"/"12k"); same dashed reference; note bold 12px magenta: "backfill re-ran days 1-3: those days doubled".
- **Bar style:** 42px wide, fill at 35% alpha of the panel color, 2px stroke; day labels 11px `#6b7280`.
- **Caption (12px `#6b7280`, centered at y=284):** "rows per daily partition after the backfill touches the first three days twice".

## The Confusion: Today's Code, Yesterday's World

**Tags:** `common confusion` (red), `leakage risk` (orange)

- **Time travel risk** — the backfill runs today's code and today's lookup tables on old dates
- **Changed lookups** — a customer who moved in July gets the new city stamped on May orders
- **Feature leakage** — backfilled ML features can quietly use data unavailable at the time
- **As-of joins** — careful backfills join to reference data as it was on that date
- **Label it** — record which days were backfilled and when, so drift has an explanation

*Example:* A churn model's backfilled "days since last order" used today's order table — it saw the future and looked brilliant until launch.

**Common mistake:** a backfill rewrites the past with the present. Ask of every input: "is this what we knew then, or what we know now?"

### Visualization (canvas `c4`, 720×300)

Join diagram: one May order joined against two versions of the customer lookup table — as-of vs naive.

- **Title (bold 15px, `#1a5276`, top center):** "Backfilling May 30 with Today's Lookup Table Rewrites History".
- **Order box:** (40,100,180×84) `#f8f9fa` fill, blue `#2a78d6` 2px stroke, labeled bold 13px blue "order 71204" / 12px "placed May 30" / "customer_id 5521".
- **Lookup boxes (200×74 at x=320):**
  - Upper (y=62), filled `rgba(0,131,0,0.07)` stroked green `#008300`: bold 12px green "customers as of May 30"; 12px "5521 → city: Austin"; bold 12px green "what we knew then".
  - Lower (y=158), filled `rgba(213,81,129,0.08)` stroked magenta `#d55181`: bold 12px magenta "customers today (Aug 24)"; 12px "5521 → city: Denver"; bold 12px magenta "moved in July".
- **Arrows:** green arrow from the order box to the as-of lookup; magenta arrow to the today lookup.
- **Outcomes (bold 13px, left-aligned at x=545):** green "as-of join: May order → Austin ✓"; magenta "naive join: May order" / "→ Denver ✗".
- **Callouts (centered):** bold 13px magenta at y=262: "same trap in ML features: a backfilled feature that reads today's tables sees the future"; 12px `#6b7280` at y=284: "ask of every backfill input: what we knew then, or what we know now?".

## Regeneration instructions

- **Template:** tutorials topic-page layout. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` line, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` line, one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Canvas:** intrinsic 720×300 attributes; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; shared `arrow()` drawing helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette:** shared `P` object — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; site palette `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
