# Idempotency

**Page type:** detail page (tutorial layout: h1 + subtitle, then one `.card-section` per concept, each an h2 + two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Idempotency

**Subtitle:** Same input, same output, no matter how many times the job runs — so reruns after a crash are harmless

## The Tuesday Job That Ran Twice

**Tags:** `core idea` (blue), `running example` (green)

- **The job** — every night, load that day's orders into the warehouse revenue table
- **Tuesday 2:17am** — it crashes halfway, after writing 1,120 of Tuesday's 1,930 rows
- **The rerun** — on-call restarts it at 7:00am; it appends all 1,930 rows again
- **The damage** — Tuesday now holds 3,050 rows; revenue reads $76,150, not $48,200
- **The fix idea** — a rerun should leave the table exactly as if the job ran once

*Example:* Wednesday's standup: "why did Tuesday revenue jump 58%?" — nothing sold better; the loader just ran twice.

**Key point:** idempotent = running the job 1, 2, or 10 times produces the identical table. Crashes are guaranteed; idempotency is what makes them boring.

### Visualization (canvas `c1`, 720×300)

Timeline plus stacked partition bar: the crash and the naive rerun stacking duplicate rows.

- **Title (bold 16px, `#1a5276`, top center):** "Crash at 2:17am + Naive Rerun = 1,120 Duplicate Rows".
- **Timeline:** horizontal axis line `#999` at y=78 from x=50 to x=690, with 6px event dots and bold 12px time labels over 12px sub-labels:
  - x=90, blue `#2a78d6`: "2:00am" / "run 1 starts"
  - x=260, magenta `#d55181`: "2:17am" / "crash: 1,120 of 1,930 written"
  - x=470, orange `#d95926`: "7:00am" / "run 2 appends all 1,930"
  - x=640, magenta `#d55181`: "7:09am" / "3,050 rows total"
- **Partition bar:** at x=140, y=130, 440×78, split proportionally by rows out of 3,050 — left segment 1,120 rows filled `rgba(42,120,214,0.35)` stroked blue, labeled bold 12px blue "run 1 (partial)" / "1,120 rows"; right segment 1,930 rows filled `rgba(217,89,38,0.30)` stroked orange, labeled bold 12px orange "run 2 (full append)" / "1,930 rows"; bold 13px "Tuesday" / "partition" label to the right of the bar.
- **Duplicate bracket:** magenta bracket under the first 1,120-row-wide span of the run-2 segment, with bold 13px magenta label "1,120 rows now exist twice".
- **Caption (12px `#6b7280`, centered at y=278):** "revenue in the table: $27,950 + $48,200 = $76,150 — the true day was $48,200".

## Append vs Overwrite: One Rerun, Two Endings

**Tags:** `worked example` (green), `core idea` (blue)

- **Truth** — Tuesday really had 1,930 orders worth $48,200
- **Append, run 1** — writes 1,120 rows ($27,950), then crashes mid-job
- **Append, run 2** — adds the full 1,930 on top: 3,050 rows, $76,150
- **Overwrite, run 2** — first deletes everything tagged Tuesday, then writes 1,930 rows
- **Overwrite, runs 3, 4, 5…** — always the same end state: 1,930 rows, $48,200

*Example:* Delete-where-date='Tue' then insert: the rerun wipes its own half-finished mess before writing anything.

**Key point:** "overwrite the partition" — replace all of one day's rows — is the classic idempotent pattern: the run's first act is to erase any earlier attempt at that day.

### Visualization (canvas `c2`, 720×300)

Branching flow diagram: one post-crash state forking into an append path (wrong) and an overwrite path (correct).

- **Title (bold 15px, `#1a5276`, top center):** "After the Crash, the Rerun Takes One of Two Paths".
- **Start box:** (x=30, y=118, 150×64) `#f8f9fa` fill, blue `#2a78d6` 2px stroke, bold 13px blue "after the crash" over 12px "1,120 rows / $27,950".
- **Path A (append, upper):** orange arrow to box (272,56,190×64) filled `#fdf2e9`, stroked orange `#d95926`, labeled bold 13px orange "rerun: append" / 12px "INSERT all 1,930 again"; orange arrow to result box (544,52,158×72) filled `rgba(213,81,129,0.10)`, stroked magenta `#d55181`, labeled bold 13px magenta "3,050 rows" / "$76,150" / bold 12px "WRONG, forever".
- **Path B (overwrite, lower):** green arrow to box (272,176,190×80) filled `rgba(0,131,0,0.07)`, stroked green `#008300`, labeled bold 13px green "rerun: overwrite" / 12px "1. delete Tuesday's rows" / "2. insert all 1,930"; green arrow to result box (544,178,158×72) same green style, labeled bold 13px green "1,930 rows" / "$48,200" / bold 12px "correct, every rerun".
- **Callout (bold 13px green `#008300`, centered at y=284):** "the overwrite path ends the same whether it runs 2 or 10 times".

## Why Doubled Rows Are So Hard to Catch

**Tags:** `where it's used` (blue), `failure mode` (orange)

- **Failures are routine** — networks blip, machines restart; big pipelines rerun jobs daily
- **Silent corruption** — doubled rows look like good news (revenue up!), so nobody digs
- **Models eat it too** — a model trained on the doubled Tuesday learns from ghost orders
- **Auto-retries** — schedulers retry failed jobs by default; only safe if jobs are idempotent
- **Cheap to test** — run the job twice on purpose; if the table changes, it isn't idempotent

*Example:* The anomaly detector flagged ordinary Tuesdays for months — it had learned one doubled Tuesday as "normal growth".

**Key point:** if a job is idempotent, "just rerun it" is always a safe answer. If it isn't, every failure becomes a careful manual cleanup at 7am.

### Visualization (canvas `c3`, 720×300)

Weekly bar chart: daily revenue with the corrupted Tuesday standing out and a dashed true-value marker.

- **Title (bold 15px, `#1a5276`, top center):** "The Doubled Tuesday Just Looks Like a Great Tuesday".
- **Data:** days Mon–Sun, revenue `[46300, 76150, 47800, 45900, 52300, 58100, 55400]` (Tuesday corrupted); true Tuesday value 48,200.
- **Axes:** padding top 56, bottom 56, left 70, right 24; y scale 0 to $80,000 with tick labels "$0k", "$40k", "$80k" (12px `#6b7280`); gridlines `#e5e9ef`; axis lines `#999`.
- **Bars:** 58px wide; normal days filled `rgba(42,120,214,0.35)` stroked blue `#2a78d6`; Tuesday filled `rgba(213,81,129,0.40)` stroked magenta `#d55181` with bold 12px magenta value label "$76,150" above; day labels 12px below.
- **True-value marker:** dashed (6/4) green `#008300` line at $48,200 across the Tuesday bar, labeled bold 12px green "true Tuesday: $48,200".
- **Callout (bold 13px magenta `#d55181`, centered below x labels):** "+58% ghost revenue from one rerun — no alert fires on good news".
- **Caption (12px `#6b7280`, right-aligned at x=708 on the same line):** "illustrative week".

## The Confusion: Idempotent Does Not Mean "Never Fails"

**Tags:** `common confusion` (red), `design trap` (orange)

- **Not crash-proof** — the job still crashes; idempotency only makes the rerun safe
- **Append is the trap** — plain INSERT is the natural first version and is never rerun-safe
- **Half-done runs count** — the design must survive a crash at any point, not just the end
- **Scope it** — idempotent per day (per partition) is usually enough; no full-table wipes
- **One test question** — "what does the table look like if this runs twice?"

*Example:* A job that failed cleanly 100 times is still dangerous if run 101 appends on top of run 100.

**Common mistake:** calling a job "reliable" because it rarely fails. Reliability is about how often it breaks; idempotency is about what a rerun does. You need both.

### Visualization (canvas `c4`, 720×300)

Two-series line chart: rows in the Tuesday partition vs number of runs — append grows linearly, overwrite stays flat.

- **Title (bold 15px, `#1a5276`, top center):** "The Two-Run Test: What Does Run #2 Do to the Table?".
- **Data:** runs 1–5; append rows `[1930, 3860, 5790, 7720, 9650]`; overwrite rows `[1930, 1930, 1930, 1930, 1930]`.
- **Axes:** padding top 56, bottom 56, left 70, right 190; y scale 0 to 10,000 with tick labels 0 / 5,000 / 10,000 (12px `#6b7280`); gridlines `#e5e9ef`; axis lines `#999`; x labels "run 1"…"run 5" (12px) with axis caption "times the same Tuesday job ran" in `#6b7280`.
- **Series:** append line magenta `#d55181`, width 3, 4px dots; overwrite line green `#008300`, width 3, 4px dots.
- **In-chart annotations:** bold 13px magenta near run 4: "append: +1,930 rows" / "every rerun"; bold 13px green above the flat line: "overwrite: flat at 1,930".
- **Legend (top right, 12px):** magenta swatch "naive append"; green swatch "overwrite partition".

## Regeneration instructions

- **Template:** tutorials topic-page layout. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` line, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` line, one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Canvas:** intrinsic 720×300 attributes; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; shared `arrow()` drawing helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette:** shared `P` object — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; site palette `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
