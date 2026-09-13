# Snapshots vs Deltas

**Page type:** detail page (tutorial card-sections: h2 per section, two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Snapshots vs Deltas

**Subtitle:** Save a full copy of everything each time, or save only what changed since last time — a storage vs reconstruction trade-off

## Backing Up a Store's Inventory Every Night

**Tags:** `core idea` (blue), `running example` (green)

- **The table** — a grocery store's inventory: 500 items, one row each with today's stock
- **Snapshot** — every night, save all 500 rows as a dated full copy ("point-in-time")
- **Delta** — every night, save only the rows whose stock changed since last night
- **This week** — only 12, 9, 15, and 7 items changed on Tue, Wed, Thu, Fri
- **The math** — snapshots store 5 × 500 = 2,500 rows; one snapshot + deltas store 500 + 43 = 543

*Example:* Four nightly snapshots re-saved the row "rice: 82 in stock" four times — rice never sold once.

**Key point:** A snapshot is complete but repeats everything; a delta is tiny but only meaningful on top of what came before.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: rows stored per night, snapshot plan vs delta plan.

- **Title (bold 15px, `#1a5276`, top center):** "Rows Saved per Night: Full Snapshot vs Delta (500-item inventory)"
- **Data:** days Mon–Fri; snapshot plan = `[500, 500, 500, 500, 500]`; delta plan = `[500, 12, 9, 15, 7]` (Mon is the starting snapshot in both plans).
- **Scale:** y max 550, plot area padded top 62 / bottom 62 / left 65 / right 30; gray L-shaped axes `#999`. Delta bars get a minimum 3px height.
- **Bars:** 42px wide, paired per day. Snapshot bars: fill `rgba(213,81,129,0.4)`, stroke magenta `#d55181`. Delta bars: fill `rgba(0,131,0,0.4)`, stroke green `#008300`. Value labels bold 12px above each bar in matching color; day labels `#222` below axis.
- **Legend (top left):** magenta swatch "snapshot plan (full copy)", green swatch "delta plan (changes only)".
- **Annotation (bold 13px orange `#d95926`, bottom center):** "week total: 2,500 rows vs 543 rows — deltas store ~22%"

## Rebuilding Wednesday's Stock by Hand

**Tags:** `worked example` (green), `core idea` (blue)

- **Question** — how many apples and bananas were in stock at close on Wednesday?
- **Start** — Monday snapshot says: apples 40, bananas 60
- **Apply Tue delta** — "apples → 35" (5 sold); no banana row, so bananas stay 60
- **Apply Wed delta** — "apples → 28" (7 sold); again no banana row
- **Answer** — Wednesday: apples 28, bananas 60 — snapshot plus two deltas, in order

*Example:* "No delta row" is itself information: it means "same as yesterday" — that is where the saving comes from.

**Key point:** State on day N = last snapshot + every delta up to day N, applied in order. Skip or reorder one and the answer is wrong.

### Visualization (canvas `c2`, 720×300)

Flow diagram: snapshot box + two delta boxes + reconstructed-state box connected by arrows.

- **Title (bold 15px `#1a5276`, top center):** "Snapshot + Deltas in Order = Wednesday's Stock"
- **Boxes (left to right, arrows in ink `#1a5276` between them):**
  1. "Mon SNAPSHOT" (magenta `#d55181` stroke, fill `rgba(213,81,129,0.08)`, 150×96 at x=30): lines "apples: 40", "bananas: 60", "(+ 498 more rows)".
  2. "Tue DELTA" (green `#008300` stroke, fill `rgba(0,131,0,0.07)`, 130×80): "apples → 35", muted `#6b7280` "(no banana row)".
  3. "Wed DELTA" (same green style): "apples → 28", muted "(no banana row)".
  4. "Wed state" (blue `#2a78d6` stroke, fill `rgba(42,120,214,0.08)`, 108×96): bold "apples: 28", "bananas: 60".
- **Running math under the chain:** bold ink "apples:   40  →  35  →  28"; muted "bananas: 60  →  60  →  60  (never in a delta = never changed)".
- **Annotation (bold 13px orange `#d95926`, bottom center):** "replay left to right — order matters, gaps are fatal"

## Where the Trade-Off Bites a Data Scientist

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **Time-travel questions** — "what did the customer table look like when we trained the model?"
- **Snapshot answer** — open one file; done, whatever the day
- **Delta answer** — replay every delta since the last snapshot; day 30 needs 30 files
- **Storage answer** — deltas win big when little changes per day (here ~2% of rows)
- **Real systems mix both** — a fresh snapshot every week, deltas in between

*Example:* Reconstructing "inventory as of last March" from a year of daily deltas took hours; from a snapshot, seconds.

**Key point:** Deltas trade cheap storage for expensive reconstruction — periodic snapshots cap how long any replay can get.

### Visualization (canvas `c3`, 720×300)

Line chart: number of files to read to rebuild state on day N, for three storage plans.

- **Title (bold 15px `#1a5276`, top center):** 'Files to Read to Rebuild "Stock on Day N"'
- **Axes:** x = day N from 1 to 14 (labels on odd days plus 14; muted x-axis caption "day N being reconstructed"); y = 0 to 16 with gridlines `#e5e9ef` and labels at 0, 4, 8, 12, 16; padding top 58 / bottom 58 / left 65 / right 185; gray axes `#999`.
- **Series (all 3px wide):**
  - Daily snapshots: horizontal magenta `#d55181` line at y=1 across days 1–14.
  - Deltas only: green `#008300` line y=N (1 through 14).
  - Weekly snap + deltas (hybrid): blue `#2a78d6` dashed (7/5) line through `[1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7]` (weekly snapshot resets at day 8).
- **Legend (right side):** line swatches with labels "daily snapshots: 1" (magenta), "deltas only: N" (green), "weekly snap + deltas" (blue dashed).
- **Annotations:** bold 13px blue "hybrid caps replay at 7" under the legend; bold 13px orange `#d95926` "delta cost keeps growing — snapshots reset it" near the top of the plot (~45% width).

## The Confusion: One Lost Delta Poisons Every Later Day

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The chain** — each delta only makes sense on top of ALL the previous ones
- **Lose Wednesday's file** — Tuesday still reconstructs fine; Wednesday onward does not
- **Silently wrong** — replaying Thu and Fri deltas still "works", it just gives wrong stock counts
- **Snapshots don't chain** — a lost snapshot loses one day, not every day after it
- **Rule of thumb** — never run deltas without periodic snapshots and a row-count check against them

*Example:* Apples showed 35 instead of 28 on Wednesday — the missing delta ("apples → 28") failed nothing loudly.

**Key point:** A delta chain fails silently: everything after the gap is wrong but looks fine. Snapshots are the firebreaks.

### Visualization (canvas `c4`, 720×300)

Broken-chain diagram plus a true-vs-replayed value table.

- **Title (bold 15px `#1a5276`, top center):** "Wed Delta File Lost: Every Later Day Reconstructs Wrong"
- **Chain of 5 boxes (108×66 at y=70, x = 30/168/306/444/582), arrows between them:**
  - "Mon SNAP" / "apples 40" — magenta `#d55181` stroke, fill `rgba(213,81,129,0.08)`.
  - "Tue delta" / "→ 35" — green `#008300` stroke, fill `rgba(0,131,0,0.07)`.
  - "Wed delta" / "LOST" — red `#e74c3c` dashed stroke, fill `rgba(231,76,60,0.12)`, sub-label bold red.
  - "Thu delta" / "→ 26" — green.
  - "Fri delta" / "→ 33" — green.
  - Arrows are ink `#1a5276` except arrows touching the lost box, which are red `#e74c3c`.
- **Value rows (bold 13px, left labels in ink):** "apples, true stock:" `40, 35, 28, 26, 33`; "apples, replayed:" `40, 35, 35, 26, 33` — the replayed Wednesday value 35 rendered in red `#e74c3c`, others `#444`.
- **Annotations:** bold 12px red, centered: "Wed stuck at 35 — but Thu/Fri deltas overwrite apples, hiding the damage"; muted 12px: "items whose LAST change was Wed stay wrong forever — no error is ever raised"; bold 13px orange `#d95926` at bottom: "the fix: periodic snapshots as firebreaks + count checks against them"

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`, social-graph reference skeleton). Body: `<h1>` (no index number), `.subtitle`, then four `.card-section` blocks, each `<h2>` + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Left column structure:** `.tags` pill row first, then a `<ul>` of bullets each opening with `<b>bold term</b>` (bold terms render `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Tag pill styles:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each canvas declared 720×300; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers `boxAt` (filled/stroked, optionally dashed 6/4 rectangle) and `arrowTo` (line with filled arrowhead) draw the diagrams. All data arrays are hardcoded literals — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (`P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; alarm red `#e74c3c`. Project palette anchors: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
