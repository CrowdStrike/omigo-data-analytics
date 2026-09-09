# Entity Key Granularity: Which Key Combination Defines a Unit?

**Page type:** detail page (backlog kusto-style 2-col layout: text left 45%, canvas right 55%, one `.card-section` per numbered section)
**HTML title tag:** Entity Key Granularity

**Subtitle:** Choosing too coarse a key to group and correlate events is one of the most common analysis mistakes — the "obvious" identifier mixes distinct contexts, inflating noise and creating aggregations that were never real.

**Status badge:** TO DISCUSS

## 1. Why user_id Alone Is Too Coarse

A `user_id` names a container, not a unit of behavior. A single one can carry:

- Multiple **sessions** active simultaneously (laptop + phone + tablet)
- Multiple **devices** with different usage contexts (work desktop vs personal phone)
- Multiple **people** on the same account (shared family login, team accounts)
- Multiple **intents** in parallel (browsing in one tab, checkout in another)

Correlating events by `user_id` alone mixes all of these — the resulting "behavior" is a superposition of distinct activities.

**Key point (red-accent callout):** **Inflated session length:** overlapping sessions collapse into one long session that no device ever had. Duration, dwell time, and engagement metrics all inherit the fabrication.

**Open question (orange-accent callout):** The correct unit of analysis might be `(user_id, session_id)`, or `(user_id, device_id, session_id)`, or even `(user_id, session_id, tab_id)` — depending on the question being asked.

### Visualization (canvas `c1`, 720×380)

Gantt-style session timelines for three devices under one user_id, plus the collapsed merged "session".

- **Title (bold 14px, `#1a5276`, centered, y=25):** "One user_id, Three Overlapping Session Timelines".
- **Time axis:** horizontal line from x=115 over 570px, tick labels every 30 minutes: 09:00, 09:30, 10:00, 10:30, 11:00, 11:30, 12:00 (9px `#999`); left annotation "time of day" (10px `#888`). Time range 0–180 minutes mapped linearly.
- **Overlap band:** light red rectangle `rgba(231,76,60,0.10)` spanning t=35 to t=50, y=70–192, labeled "concurrent" (9px `#e74c3c`) below.
- **Lanes (bars 22px tall, one lane per 40px from y=80, duration labels above each bar):**
  | Lane | Color / fill | Spans (start, end, label) |
  |---|---|---|
  | Laptop | `#1a5276` / `rgba(26,82,118,0.35)` | (10, 50, "40m"), (140, 170, "30m") |
  | Phone | `#e67e22` / `rgba(230,126,34,0.35)` | (35, 60, "25m") |
  | Tablet | `#27ae60` / `rgba(39,174,96,0.35)` | (100, 115, "15m") |
- **Divider:** dashed `#ccc` line at y=214.
- **Collapsed view:** header "Collapsed by user_id" (bold 11px `#e74c3c`). Single red bar from t=10 to t=170 (y=250, 30px tall, fill `rgba(231,76,60,0.35)`, stroke `#e74c3c` width 2) labeled inside: "one "session" of 2h 40m" (bold 11px red).
- **Annotations (centered):** "True session durations: 40m, 25m, 15m, 30m — none of them 2h 40m" (10px `#555`, y=308); "Gaps between sessions become "engagement"; the overlap becomes invisible" (10px `#888`, y=332).
- **Caption (11px `#888`, bottom center):** "Collapsing to user_id fabricates a session no device ever had".

## 2. False Sequences and Mixed Intent

A coarse key does more than add noise — it manufactures structure that never existed:

- **False sequences:** event A on the phone followed by event B on the laptop looks like an A→B journey, but the two are independent.
- **Mixed intent:** "user searched for X then bought Y" — except X was a different person on a shared account, or a different context entirely.

Ordering is only meaningful *within* a genuine unit. Sort events by timestamp inside a container and the sort itself invents a causal story.

**Key point (red-accent callout):** **Why it is dangerous:** fabricated sequences are indistinguishable from real ones downstream. Sequence models, next-action predictors, and path analyses will happily learn the artifact.

*Example: phone searches "running shoes" at 10:02, laptop checks out a laptop bag at 10:05 — joined by `user_id`, this becomes a shoes→bag journey nobody took.*

### Visualization (canvas `c2`, 720×300)

Flow diagram: two independent device events joined by user_id into a fabricated A→B sequence.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "A Journey That Never Happened".
- **Event boxes (210×56 at x=24):**
  - "A  Phone  ·  10:02" / "search "running shoes"" — orange `#e67e22`, fill `rgba(230,126,34,0.10)`, y=58.
  - "B  Laptop  ·  10:05" / "checkout "laptop bag"" — blue `#1a5276`, fill `rgba(26,82,118,0.10)`, y=150.
- **Join node:** box 132×50 at (278,106), fill `rgba(26,82,118,0.10)`, stroke `#1a5276`, bold text "GROUP BY" / "user_id"; gray `#999` lines feed both event boxes into it, gray arrow out to the result box.
- **Fabricated sequence box:** 236×62 at (456,100), fill `rgba(231,76,60,0.10)`, stroke `#e74c3c` width 2. Contents: bold 13px red "A  →  B"; 10px `#2c3e50` ""searched shoes, then bought a bag""; below in red 10px: "two contexts, possibly two people" and "the arrow is an artifact of the key".
- **Truth line (10px `#27ae60`, left, y=240):** "truth: A and B are independent — no ordering exists between them".
- **Caption (11px `#888`, bottom center):** "A coarse key does not only add noise — it invents order".

## 3. Broken Funnels and Ambiguous Attribution

Two downstream metrics break in specific, measurable ways:

- **Broken funnels:** a conversion funnel assumes linear progression within a key. Parallel sessions break that assumption — stage-completion rates inflate and the apparent drop-off shifts downstream.
- **Wrong attribution:** one conversion, several devices and sessions all touched it. Which one gets credit? A user-level key cannot break the tie because it erased the candidates.

**Key point (red-accent callout):** **Direction of the bias:** merging parallel sessions makes the middle of the funnel look healthier than it is, so optimisation effort gets pointed at the wrong stage.

*Example: per-session cart→checkout looks like a major leak; merged to per-user it looks fine, because a second session's checkout is credited to the first session's cart.*

### Visualization (canvas `c3`, 720×380)

Top: paired horizontal funnel bars (per-session vs merged per-user). Bottom: attribution ambiguity diagram.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Funnel Shape: Per Session vs Merged Per User".
- **Funnel data (bars from x=150, max width 420 at 1000, rows 40px apart from y=62; green bar = per session, fill `rgba(39,174,96,0.6)`; red bar = merged per user, fill `rgba(231,76,60,0.5)`; value labels at bar ends in `#27ae60` / `#e74c3c`):**
  | Stage | Per session (true) | Merged per user |
  |---|---|---|
  | View | 1000 | 1000 |
  | Add to cart | 400 | 520 |
  | Checkout | 220 | 380 |
  | Purchase | 150 | 150 |
- **Legend:** "per session (true)" (green swatch), "merged per user" (red swatch).
- **Annotation (10px `#e67e22`, below funnel):** "middle stages inflate — the real leak is hidden".
- **Divider:** dashed `#ccc` line at y=246.
- **Attribution panel:** header "Attribution: who gets credit for the one purchase?" (bold 11px `#1a5276`). Three session boxes 130×24 at x=24 (28px apart): "Phone session" (`#e67e22`), "Laptop session" (`#1a5276`), "Tablet session" (`#27ae60`), each with a gray connector line toward the conversion box and a bold red "?" on the line. Conversion box 160×36 at (504, dy+22), fill `rgba(231,76,60,0.10)`, stroke `#e74c3c` width 2, bold red text "1 purchase".
- **Caption (11px `#888`, bottom center):** "Merging shifts the apparent drop-off downstream and erases the credit candidates".

## 4. Choosing the Right Granularity

Every analysis question implicitly defines a **unit of observation**. The key combination must uniquely identify that unit — no more, no less.

- **Too coarse:** groups heterogeneous things → Simpson's paradox, inflated variance, impossible sequences.
- **Too fine:** every row is unique → no repetition to learn from, sparse features, overfitting.
- **Just right:** groups things that are genuinely the same context, same actor, same intent.

**Key point (red-accent callout):** **The test:** state the question first, then ask what one row must mean for the question to be answerable. The key is whatever makes that row unique — it is not a property of the data, it is a property of the question.

*Example: "how long is a visit?" needs session granularity; "how many devices does a person use?" needs device granularity; "which tab converted?" needs tab granularity.*

### Visualization (canvas `c4`, 720×380)

Granularity ladder: four key levels with can/cannot-answer annotations.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Granularity Ladder: What Each Key Can and Cannot Answer".
- **Rotated left-axis label (10px `#888`):** "finer granularity  →".
- **Ladder rungs (boxes 208×58 at x=30, one per 74px from y=48, background `#f8f9fa`, colored border width 2, key bold 10px in the rung color, tag 9px `#888` below; dashed `#ccc` rails connect consecutive rungs; green ✓ line and red ✗ line to the right of each rung, 11px):**
  | Key | Color | Tag | ✓ Can answer | ✗ Cannot answer |
  |---|---|---|---|---|
  | user_id | `#e74c3c` | too coarse | Account totals, lifetime counts | Sequence, session length, device, actor |
  | (user_id, session_id) | `#27ae60` | usually the unit | Ordered journeys, session length, funnels | Which device; parallel tabs inside a session |
  | (user_id, device_id, session_id) | `#1a5276` | for device questions | Cross-device behaviour, attribution candidates | Intent split inside a single session |
  | (user_id, session_id, tab_id) | `#e67e22` | often too fine | Parallel intents inside one session | Almost nothing repeats — sparse, overfits |
- **Caption (11px `#888`, bottom center):** "The right rung is set by the question, not by which id is easiest to join on".

## 5. Where Else This Bites

The same mistake recurs across domains, always with a convenient id standing in for the real unit:

- **Network security:** `source_ip` alone — NAT means a thousand users share one IP. Needs `(ip, port, timestamp)` or `(ip, user_agent)` at minimum.
- **E-commerce:** `order_id` without `line_item_id` — aggregating to order level loses product-level signal.
- **Healthcare:** `patient_id` without `encounter_id` — mixes an outpatient visit with an ICU stay.
- **Ad tech:** `campaign_id` without `creative_id` + `placement_id` — can't tell if it was the ad or the context.
- **IoT / manufacturing:** `machine_id` without `shift_id` or `operator_id` — different operators produce different defect rates.
- **Finance:** `account_id` without distinguishing joint holders or authorized signers.

**Key point (red-accent callout):** **The common shape:** the id that is easiest to join on names the container (IP, order, patient, campaign, machine, account), while the unit of behavior lives one level below it.

### Visualization (canvas `c5`, 720×300)

Table-style chart: domain, the naive container key (red chip), and the missing dimension (orange text).

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Container Id vs the Missing Dimension".
- **Column headers (9px `#999`):** "key used" (centered at x=172), "missing dimension" (left at x=228).
- **Rows (one per 34px from y=48; domain right-aligned 10px `#2c3e50` at x=118; naive key in a 92×20 chip, fill `rgba(231,76,60,0.10)`, stroke/text `#e74c3c`; missing-dimension text 10px `#e67e22` at x=228):**
  | Domain | Key used | Missing dimension |
  |---|---|---|
  | Network security | source_ip | + port, timestamp, user_agent  —  NAT hides many users behind one IP |
  | E-commerce | order_id | + line_item_id  —  order-level totals lose product signal |
  | Healthcare | patient_id | + encounter_id  —  outpatient visit and ICU stay are not one unit |
  | Ad tech | campaign_id | + creative_id, placement_id  —  was it the ad or the context? |
  | IoT / manufacturing | machine_id | + shift_id, operator_id  —  operators differ in defect rate |
  | Finance | account_id | + holder / signer  —  joint accounts blend two actors |
- **Caption (11px `#888`, bottom center):** "In every domain the convenient id names the container, not the unit of behavior".

## 6. Detecting a Too-Coarse Key

Open question: can the pipeline notice that a chosen key produces suspiciously mixed behavior, and say so?

- **Statistical signals:** bimodal or multimodal distributions within a single key value, impossible event transitions, concurrent conflicting states, variance inflation relative to a finer key.
- **Suggested refinements:** should the pipeline propose a key extension — "your `user_id` groups split into distinct behavior clusters, consider adding `session_id`"?
- **Entity resolution:** sometimes the right key is not in the data and must be constructed (session stitching, device graph).
- **Temporal dimension:** a key can be correct at one time granularity and broken at another — `user_id` is fine monthly, meaningless per-minute.
- **Hierarchy link (#24):** keys usually form a hierarchy (user → session → event), so this overlaps the hierarchical-relationships item.

**Key point (red-accent callout):** **Cheapest diagnostic:** compute a metric at the candidate key and at one level finer. If the distribution shape changes character — not just scale — the coarser key is mixing contexts.

### Visualization (canvas `c6`, 720×300)

Two-panel histogram comparison: bimodal at user_id level vs unimodal at (user_id, session_id).

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Detection Signal: Shape Changes When You Go One Level Finer".
- **Panels (each 300px wide, 12 bins, plot top y=78, bottom y=234, scale max 16):**
  - Left (x=40): title "grouped by user_id" (red `#e74c3c`), bars fill `rgba(231,76,60,0.35)` stroke red; bins `[3, 7, 12, 9, 4, 2, 3, 8, 13, 10, 5, 2]` (two modes); note above plot: "two modes = mixed contexts".
  - Right (x=380): title "grouped by (user_id, session_id)" (green `#27ae60`), bars fill `rgba(39,174,96,0.35)` stroke green; bins `[2, 5, 10, 16, 13, 7, 3, 2, 1, 1, 0, 0]` (single mode); note: "single mode = coherent unit".
- **Axis labels:** "session duration →" beneath each panel (10px `#666`); shared rotated y-axis label "count".
- **Annotation (10px `#e67e22`, centered, y=268):** "variance also inflates on the left — but shape change is the clearer flag".
- **Caption (11px `#888`, bottom center):** "Bimodality inside a single key value says the key is mixing contexts".

## Regeneration instructions

- **Layout:** backlog detail page. Body → h1 → `.subtitle` → `.status` badge ("TO DISCUSS") → one `.card-section` per numbered section, each an `<h2>` plus a `table.layout` with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`/`.questions`/`.example`, right `td.viz-col` (55%) for the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`. `.subtitle` `#666` 0.95rem, margin-bottom 12px. `.status` inline-block pill: background `#e8f0f8`, color `#1a5276`, padding 3px 10px, radius 12px, 0.85em bold. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`. `.questions` background `#f8f9fa`, `border-left: 3px solid #e67e22`. `.example` italic `#555` 0.9rem. `code` background `#e8f0f8`, color `#1a5276`. (Note: this page's CSS does not restyle `strong`.) Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links, no index number in h1.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic width/height attributes per chart (c1, c3, c4 are 720×380; c2, c5, c6 are 720×300); a `setup(id)` helper (with inline equivalents for 380-tall canvases) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- Regenerated HTML has no card links (detail page); any links elsewhere use `.html` extensions.
