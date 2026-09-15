# Event Notifications

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Event Notifications

**Subtitle:** A file landing in a bucket can wake a queue or a function on its own, but a message may arrive twice and two messages about one file may arrive in the wrong order

## An Object Lands, Something Wakes Up

**Tags:** `core idea` (blue), `fan-out` (green), `folder + extension filter` (orange)

- **The setup** — one file is uploaded every hour, and each one needs a job run on it
- **The wiring** — you tell the bucket which changes matter and where to send them
- **What counts as a change** — an object being written, overwritten, or deleted from it
- **The filter** — a rule narrows it to one folder and one extension, so noise stops
- **Where it goes** — a queue, a function, a fan-out topic, or a general event bus
- **No polling** — no job has to keep listing the folder on a timer to find the work
- **Faster reaction** — work starts within seconds of the file landing, not on the hour
- **Only a hint** — the message says something changed, and nothing more than that

*Example (illustrative):* A CSV lands in the incoming folder, a message appears on a queue, and one worker picks it up and runs the job.

**Key point:** Writing the rule and picking the destination is the easy half. The hard half is that the delivery itself promises very little.

### Visualization (canvas `c1`, 720×300)

Fan-out diagram: the bucket on the left, a filter gate in the middle, four kinds of destination on the right.

- **Title (bold 15.5px, `#1a5276`, centered at y=24):** "One Object Lands, One Filter Gate, Four Kinds of Destination".
- **Source box (x=30, y=110, w=130, h=64, r=8):** fill `rgba(42,120,214,0.14)`, 2px `#2a78d6`; bold 13px `#1a5276` "bucket" centered at y=134, 12px `#2c3e50` "object written" at y=154.
- **Key label (11.5px monospace `#6b7280`, centered at x=95, y=192):** `incoming/store-7.csv` — below the box, so nothing overlaps.
- **Arrow 1 (2px `#6b7280`):** line from (162, 142) to (212, 142), arrowhead tip at (218, 142).
- **Filter gate (rounded box x=220, y=98, w=150, h=88, r=8):** fill `rgba(201,133,0,0.13)`, 2.5px `#c98500`; bold 12.5px `#c98500` "filter gate" centered at y=120; 12px `#2c3e50` "folder: incoming/" at y=142 and "name ends: .csv" at y=162.
- **Destination rows:** four boxes x=470, w=220, h=36, r=6, top `y = 56 + i × 44` (56, 100, 144, 188), centre line `cy = y + 18`. Derived in JS from the index, not hardcoded per row.
- **Elbow connectors (2px `#6b7280`):** (372, 142) → (430, 142) → (430, cy) → (465, cy), arrowhead tip at (470, cy) so the head lands exactly on the box edge, never past it.
- **Destination labels (bold 12.5px in the border colour, left-aligned at x=482, baseline cy+4)** — each ≤29 characters so the text stays inside the 220px box:
  - "a queue — buffered, retried" fill `rgba(0,131,0,0.12)` border `#008300`
  - "a topic — copies to many" fill `rgba(213,81,129,0.12)` border `#d55181`
  - "a function — runs at once" fill `rgba(74,58,167,0.12)` border `#4a3aa7`
  - "an event bus — more targets" fill `rgba(25,158,112,0.12)` border `#199e70`
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=252):** "the gate decides which files notify; the destination decides who wakes up".
- **Caption (12px `#444`, right-aligned at y=294):** "labels illustrative; filtering by folder and extension is documented".

## At Least Once, and in No Fixed Order

**Tags:** `common mistake` (red), `at-least-once` (green), `no ordering` (orange)

- **At least once** — one change can arrive as two messages; exactly once is not offered
- **No order** — two messages about one file can arrive newest first, oldest second
- **The message is old news** — it describes a past change, not the file right now
- **The right pattern** — treat it as "go look at this file", then read the file itself
- **Version guard** — compare the version in the message with the one stored right now
- **Timing is best effort** — usually a few seconds, sometimes a minute or longer
- **Messages can be lost** — rare, but a pipeline should not assume every one arrives
- **So sweep as well** — a nightly pass over the folder fills in whatever was missed

*Example (illustrative):* The overwrite message arrives first and the original write message second, so the job publishes the older row count as the current one.

**Common mistake:** Trusting the message as the file's current state, and assuming one message per change. Both hold most of the time, which is why the failures show up as wrong numbers instead of as errors.

### Visualization (canvas `c2`, 720×300)

Two lanes on one time axis: a duplicate delivery lane and an out-of-order lane, with the wrong downstream result called out in red.

- **Title (bold 15.5px, `#1a5276`, centered at y=22):** "Duplicate and Reordered Delivery on One Time Axis".
- **Time axis:** x0=70 at t=0 s, x1=640 at t=12 s, so `tx(t) = 70 + (t / 12) × 570` (47.5 px per second). Axis line 1.5px `#e5e9ef` at y=250 with ticks down to y=256 at 0, 3, 6, 9, 12 s; tick labels 12px `#6b7280` centered at y=270.
- **Axis title (12px `#6b7280`, left-aligned at (70, 290)):** "seconds after the file lands" — left-aligned deliberately, so it cannot collide with the right-aligned caption.
- **Lane labels (bold 12.5px `#1a5276`, right-aligned at x=64):** "duplicate" at y=94, "reordered" at y=204.
- **Lane 1 (guide line 2px `#e5e9ef` at y=90, markers filled circles r=7, labels 12px centered at y=72):**
  - t=0 blue `#2a78d6` "written (v1)"
  - t=3 green `#008300` "message arrives"
  - t=9 red `#e74c3c` "same message again"
- **Lane 1 outcome (bold 12.5px `#e74c3c`, left-aligned at (70, 124)):** "the job ran twice → the file is counted twice".
- **Fix strip (between the lanes):** rounded box x=70, y=140, w=570, h=28, r=6, fill `rgba(25,158,112,0.12)`, 2px `#199e70`, bold 12.5px `#199e70` centered at (355, 159): "fix: use the message as a trigger, then read the file’s current version".
- **Lane 2 (guide line at y=200, markers r=7):**
  - t=0 blue `#2a78d6` "written (v1)", label above at y=186
  - t=2 violet `#4a3aa7` "overwritten (v2)", label above at y=186
  - t=5 yellow `#c98500` "v2 message arrives", label below at y=222
  - t=9 red `#e74c3c` "v1 message arrives", label below at y=222
- **Lane 2 outcome (bold 12.5px `#e74c3c`, left-aligned at (70, 241)):** "last one wins downstream → the record says v1, the bucket holds v2".
- **Label spacing check:** at 47.5 px/s the lane-1 labels centre at x=70, 212.5 and 497.5; lane 2's write labels at x=70 and 165 and its arrival labels at x=307.5 and 497.5. At 12px no two labels on a row overlap — the tightest pair is lane 2's "written (v1)" ending near x=106 and "overwritten (v2)" starting near x=113.
- **Caption (12px `#444`, right-aligned at y=294):** "timings illustrative; at-least-once and unordered delivery are documented".

## 40,000 Files a Day, Counted as 40,600

**Tags:** `worked example` (blue), `run it twice safely` (green), `dedupe key` (orange)

- **The inputs** — 40,000 files land in a day and 1.5% of them notify twice (illustrative)
- **The duplicates** — 40,000 × 0.015 = 600 files send a second, identical message
- **The invocations** — the job therefore runs 40,000 + 600 = 40,600 times that day
- **The naive count** — a counter that adds one per run reports 40,600, not 40,000
- **The error** — 600 files too many: 600 ÷ 40,000 = 1.50%, and nothing looks wrong
- **Over a month** — 30 days of that is 1,218,000 reported against 1,200,000 real
- **The fix** — record each file name and version handled, then ignore any repeat
- **The corrected count** — 40,600 arrive, 600 are rejected as repeats, 40,000 counted

*Example (illustrative):* The store already holds `store-7.csv` at version v1, so the second message for it is rejected and the counter does not move.

**Key point:** Make the job safe to run twice, keyed on the file's version. An ordered queue does not fix the ordering either, because the reordering happens before the queue.

### Visualization (canvas `c3`, 720×300)

Three bars on a deliberately zoomed y axis: the true count, the naive count, and the corrected count.

- **Title (bold 15.5px, `#1a5276`, centered at y=24):** "Files Counted per Day: Truth, Naive Job, Repeat-Safe Job".
- **Plot geometry:** bars sit on y=234, plot spans x=90 to x=660; the y axis runs 39,400 (bottom, y=234) to 40,800 (top, y=66), so `yv(v) = 234 − ((v − 39400) / 1400) × 168`. Every bar top and label position is computed from `yv`, never hardcoded: `yv(40000) = 162`, `yv(40600) = 90`.
- **Gridlines (1px `#e5e9ef`) with y labels (12px `#6b7280`, right-aligned at x=82, baseline y+4):** 39,400 / 39,800 / 40,200 / 40,600 / 40,800.
- **Bars (width 110, centres at x=180, 350, 520):**
  - "true files" 40,000 → fill `rgba(42,120,214,0.55)`, 2px `#2a78d6`
  - "naive count" 40,600 → fill `rgba(231,76,60,0.45)`, 2.5px `#e74c3c`
  - "repeat-safe count" 40,000 → fill `rgba(0,131,0,0.45)`, 2px `#008300`
- **Bar value labels (bold 13px in the border colour, centered 8px above the bar top):** "40,000", "40,600", "40,000" — formatted from the plotted value at render time.
- **X labels (12.5px `#2c3e50`, centered under each bar at y=254):** "true files", "naive count", "repeat-safe count".
- **Truth reference line (1.5px dashed 5/4 `#2a78d6`)** across the plot at `yv(40000)`, labelled bold 12px `#2a78d6` "truth = 40,000" right-aligned at (656, yv(40000) − 8).
- **Delta bracket (2px `#e74c3c`)** on the naive bar: vertical segment at x=418 from `yv(40000)` to `yv(40600)` with 8px caps at both ends; bold 13px `#e74c3c` label "+600 messages (+1.50%)" left-aligned at (432, midpoint + 4) — the midpoint is y=130, which is above every bar top except the naive bar's, so nothing is written over a bar.
- **Green tick (bold 14px `#008300`) at (484, yv(40000) − 8):** "✓" placed immediately left of the third bar's value label, on the same baseline. Do not put it at y ≈ 134 — that row is occupied by the delta-bracket label.
- **Zoom warning (12px `#6b7280`, left-aligned at (90, 276)):** "y axis starts at 39,400 — a 1.50% gap is invisible from zero".
- **Caption (12px `#444`, right-aligned at y=294):** "counts illustrative; arithmetic exact — 40,000 × 0.015 = 600".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section two's callout uses the label "Common mistake:"; sections one and three use "Key point:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms). Helpers `roundRect`, `arrowHead` and `rgba` exactly as in `11-cross-region-replication.html`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine failure: the duplicate marker and both wrong-result lines in `c2`, and the inflated bar plus its bracket in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, so each holds one line at the 50/50 split without wrapping while still carrying a full clause. An earlier pass clipped them to ~55–70 characters and was rejected for compromising quality; do **not** clip them again, and equally do **not** pad them back to the folder default of ~90–100. Eight bullets per section, three sections.
- **Register and vocabulary.** Plain technical English for a first-time reader. No analogies, no invented scenes, no named actors. Far less API surface than the earlier draft: say "you tell the bucket which changes matter", not a notification configuration; say "an object being written, overwritten, or deleted", never the literal event-type strings; say "a queue", "a function", "a fan-out topic", "a general event bus", never the service names; say "the version in the message", not a JSON field name; say "folder and extension", not prefix and suffix. Exact event-type and configuration names live on the reference pages, not here.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No multipart-event section.** A section taught that a rule matching only the single-request upload event silently skips every large file uploaded in parts, with a `c2` showing the dropped lane. It was cut because it is a catalogue of exact event-type strings — the one thing this page's register bans — and because the page only needs three sections. That material belongs with the multipart-upload and event-type reference pages.
  - **No dead-letter queue, no ordered-queue configuration, no client-side multipart threshold bullets.** They travelled with the cut section or with the product-name register; the ordered-queue point survives only as one clause in the third callout.
  - **Three sections, one canvas each — `c1`, `c2`, `c3`.** Do not restore a fourth canvas.
- **Data:** all values are hardcoded literals; no `Math.random()` and no PRNG is needed, since nothing is scattered.
  - **Documented behaviour the page stands on:** a bucket can be configured to emit a message when an object is written, overwritten or deleted; rules may be narrowed by key prefix and suffix (written here as folder and extension); the message can be delivered to a queue, a function, a fan-out topic or a general event bus; delivery is at least once, not exactly once; ordering between messages is not guaranteed; delivery is best effort, typically seconds, occasionally longer, and a message can be missed.
  - **Illustrative and labelled as such:** every timing in `c2`, the 40,000 files per day, and the 1.5% duplicate rate.
  - **Arithmetic, verified exactly:** 40,000 × 0.015 = 600 duplicated messages; 40,000 + 600 = 40,600 invocations; 600 ÷ 40,000 = 0.015 = 1.50% inflation; 40,600 × 30 = 1,218,000 reported versus 40,000 × 30 = 1,200,000 real, a gap of 18,000; the corrected count is 40,600 − 600 = 40,000. The text, the example and all three bars carry the same five numbers.
  - **Layout hazards fixed in this pass — keep them fixed.** The destination labels in `c1` are short enough to stay inside their 220px boxes (the earlier product-name labels overflowed the box and ran off the canvas). Arrowheads land on their line's endpoint, not past it. In `c2` the two write labels are two seconds apart so they no longer overlap (they were one second apart and collided), and the axis title is left-aligned so it clears the right-aligned caption (both were previously drawn across the same band). In `c3` the green tick sits on the value-label baseline instead of in the delta-bracket label's row.
  - No account ids, no ARNs, no credential-like strings. Versions are written symbolically as v1 and v2.
