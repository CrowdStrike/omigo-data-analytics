# The Knowledge Cutoff

**Page type:** detail page (tutorial layout: h1 + subtitle, then one `.card-section` per concept, each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** The Knowledge Cutoff

**Subtitle:** The model's picture of the world froze on the day its training data ended — ask about anything after that date and it answers from an old snapshot

## Asking About Last Month's Release

**Tags:** `core idea` (blue), `failure mode` (red)

- **The question** — "What's new in DataKit 3.0?", a tool released just last month
- **The answer** — a confident feature list... describing the old 2.4, or invented
- **The reason** — training data ended at a fixed date; the model's world froze there
- **The name** — that date is the knowledge cutoff; after it, nothing exists for the model
- **No sense of now** — it does not even know today's date unless you tell it

*Example (italic):* It answered in fluent present tense about a product state that is twenty months old.

**Key point:** The model is a snapshot — it answers from the world as of its cutoff, not the world of your question.

### Visualization (canvas `c1`, 720×300)

Timeline diagram: training window, cutoff line, and the invisible zone.

- **Title (bold 15px, `#1a5276`, top center):** "The World Froze in December 2024"
- **Timeline axis:** horizontal 2px `#999` line at y=150 from x=60 to x=w−40, with a filled arrowhead at the right end; x spans 2022 to Aug 2026
- **Zone shading (84px tall band centered on the axis):** training window left of cutoff filled `rgba(42,120,214,0.12)`; after-cutoff zone filled `rgba(231,76,60,0.08)`
- **Cutoff marker:** 3px vertical `#1a5276` line at 62% of the axis, labeled above in bold 13px `#1a5276`: "cutoff: Dec 2024"
- **Zone labels (bold 13px):** blue `#2a78d6` "training data: the model saw all this" centered over the left zone; red `#e74c3c` "invisible to the model" over the right zone
- **Event dots (6px radius, two-line 12px labels below in the dot color):**
  - 12%: "2022 World Cup" / "(known)" — blue `#2a78d6`
  - 45%: "DataKit 2.4 ships" / "(known)" — blue `#2a78d6`
  - 72%: "CEO changes" / "Mar 2025" — orange `#d95926`
  - 95%: "DataKit 3.0 ships" / "Jul 2026" — red `#e74c3c`
  - 99%: "you ask" / "Aug 2026" — violet `#4a3aa7`, labels above the axis
- **Year ticks (12px `#666`):** "2022" at 5%, "2023" at 28%, "2024" at 52%, "2025" at 74%, "2026" at 93%
- **Takeaway (bold 13px red `#e74c3c`, bottom center):** "20 months of events the model answers about anyway"

## Five Questions Against One Cutoff

**Tags:** `worked example` (green), `common mistake` (red)

- **Timeless** — "capital of France?" — never changes: fine
- **Before cutoff** — "who won the 2022 World Cup?" — in the data: fine
- **Changed since** — "who is Acme's CEO?" — true in 2024, changed in 2025: stale
- **After cutoff** — "what's new in DataKit 3.0?" — invents features or admits
- **Unknowable** — "what's today's date?" — it can only guess

*Example (italic):* The dangerous row is the CEO — the answer was true once, so it sounds exactly like a right answer.

**Key point:** Wrong-because-stale answers read identically to right ones — the model attaches no freshness warning.

### Visualization (canvas `c2`, 720×300)

Row table with mini-timelines and verdict chips: five questions judged against one cutoff.

- **Title (bold 15px, `#1a5276`, top center):** "Same Model, Five Questions — Where the Fact Lives Decides"
- **Column headers (bold 12px `#666`):** "question" (x=20), "where its fact sits in time" (x=300), "answer" (x=600)
- **Rows (5 rows, 42px tall, starting y=52; question text 12px `#222`; each row has a light `#ccc` mini timeline from x=300, width 250):**
  - '"Capital of France?"' — timeless: a 3px green bar drawn across the whole mini-timeline; verdict chip "correct", green `#008300`
  - '"Who won the 2022 World Cup?"' — dot at 20% (before cutoff); verdict "correct", green `#008300`
  - '"Who is Acme's CEO?"' — dot at 72%; verdict "stale", orange `#d95926`
  - '"What's new in DataKit 3.0?"' — dot at 88%; verdict "invented", red `#e74c3c`
  - '"What's today's date?"' — dot at 98%; verdict "a guess", orange `#d95926`
- **Cutoff line:** 2px vertical `#1a5276` line at 62% of the mini-timeline width, crossing all five rows, labeled "cutoff" (bold 12px `#1a5276`) below
- **Verdict chips:** 96×24 rounded-rect-style boxes at x=600 — fill in verdict color at 0.15 alpha, 1.5px border and bold 12px centered text in the verdict color
- **Takeaway (bold 13px orange `#d95926`, bottom center):** '"stale" is the trap — it was true once, so it sounds right'

## Why It Matters — and the One-Line Workaround

**Tags:** `where it's used` (blue), `best practice` (green)

- **Silent staleness** — no error marks an outdated answer; it arrives confident
- **Fast-moving facts** — prices, versions, laws, rosters, APIs rot in months
- **Slow facts survive** — geography and arithmetic barely age between snapshots
- **The workaround** — retrieval: paste or fetch current documents; it reads them fine
- **Rule of thumb** — before trusting: "could this have changed since the cutoff?"

*Example (italic):* Pasting the release notes into the prompt turned the invented feature list into a correct one.

**Key point:** The cutoff limits what the model remembers, not what it can read — supply fresh text and it uses it.

### Visualization (canvas `c3`, 720×300)

Two-line decay chart: how answers age — fast-moving vs timeless facts.

- **Title (bold 15px, `#1a5276`, top center):** "Answers Still Correct vs Months Since Cutoff"
- **Axes:** L-shaped `#999` axis; padding top 52 / bottom 52 / left 62 / right 175; x = months since cutoff 0–24 with 12px `#222` tick labels at 0, 6, 12, 18, 24; y scale 0–100
- **Axis captions (12px `#444`):** "months since the cutoff" centered below; rotated "answers still correct, %" on the left
- **Series (3px lines with 4px dots at each point):**
  - timeless facts, green `#008300`: `[99, 99, 98, 98, 98]` at months `[0, 6, 12, 18, 24]`
  - fast-moving facts, orange `#d95926`: `[98, 88, 74, 62, 52]`
- **Legend (right side, x = w−165):** 12×12 swatches — green "timeless facts", orange "fast-moving facts"; below in muted `#6b7280` 12px: "illustrative curves"
- **Annotations:** orange bold 13px, three lines under the legend: "staleness grows silently —" / "the confident tone" / "never changes"; green bold 12px centered in the open area below the fast line: "pasting fresh documents restores the fast-moving line"

## The Confusion: It Doesn't Learn From Your Chats

**Tags:** `common mistake` (red), `mechanism` (blue)

- **Frozen weights** — chatting does not update the model; it recalls nothing tomorrow
- **Within one chat** — it "knows" what you pasted only inside that conversation
- **Confident ≠ current** — it states 2024 facts in 2026 with unchanged confidence
- **New version ≠ live feed** — retraining makes a new snapshot with a new cutoff

*Example (italic):* Telling it the new CEO's name fixes this chat only — a fresh chat is back to the frozen snapshot.

**Key point:** There are two clocks: the model's frozen one and your live one — every stale answer is that gap showing.

### Visualization (canvas `c4`, 720×300)

Two-panel chat-bubble diagram: the snapshot never updates between chats. Vertical dashed divider (`#bdc3c7`, dash 4/3) at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "What You Teach It Lives in the Chat, Not in the Model"
- **Bubble style:** 30px-tall rectangles, fill in the bubble color at 0.12 alpha, 1.5px border, 12px text in the bubble color
- **Left panel — header bold 13px `#1a5276` centered at x=190:** "Chat 1 — today"
  - 'you: "The CEO is now Priya Rao."' — blue `#2a78d6`
  - 'model: "Got it."' — muted `#6b7280`
  - 'you: "So who runs Acme?"' — blue `#2a78d6`
  - 'model: "Priya Rao."  ✓' — green `#008300`
- **Right panel — header bold 13px `#1a5276` centered at x=540:** "Chat 2 — tomorrow, fresh chat"
  - 'you: "Who runs Acme?"' — blue `#2a78d6`
  - 'model: "Daniel Ortiz."  ✗' — red `#e74c3c`
  - Below (12px red `#e74c3c`): "(the 2024 name — back to the snapshot)"
- **Takeaway (bold 13px orange `#d95926`, centered):** "the correction lived in chat 1's window and died with it"
- **Note (muted `#6b7280`, 12px, bottom center):** "weights frozen at the cutoff — conversations read text, they do not retrain it"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (social-graph reference style). Structure: `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks; each has an `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border. Section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem; `li b` colored `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all four canvases 720×300 logical; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and clears. All data arrays hardcoded (no randomness).
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
