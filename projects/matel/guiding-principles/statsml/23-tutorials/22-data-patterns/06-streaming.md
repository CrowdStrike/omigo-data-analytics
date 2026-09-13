# Streaming

**Page type:** detail page (tutorial topic page: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%; one section adds a monospace payload block under its canvas)
**HTML title tag:** Streaming

**Subtitle:** Process each event the moment it arrives, one at a time — a conveyor belt instead of a nightly truck

## 200 Milliseconds to Say Yes or No

**Tags:** `core idea` (blue), `running example` (green)

- **The moment** — a card is swiped; the bank must approve or decline before the terminal beeps
- **The budget** — the whole fraud check must finish within 200 milliseconds of the swipe
- **No pile** — you cannot wait for tonight's batch; the answer is needed NOW, per payment
- **Conveyor belt** — each payment rides the belt alone and gets scored as it passes
- **Always on** — the belt never stops; the system runs 24/7, not 75 minutes a night

*Example:* Swipe at 14:32:07.000 — the decline decision lands at 14:32:07.180, inside the 200ms budget.

**Key point:** Streaming means the DATA decides when work happens — one event arrives, one event gets processed, immediately.

### Visualization (canvas `c1`, 720×300)

Millisecond timeline of one fraud check against a 200ms deadline.

- **Title (bold 15px, `#1a5276`, top center):** "One Swipe, One Deadline: the 200ms Budget".
- **Axis:** horizontal 2px `#999` line at y=170 spanning 0–220 ms (left pad 70, right pad 40); ticks with 12px labels "0ms", "50ms", "100ms", "150ms", "200ms"; caption in `#444`: "time since the swipe at 14:32:07.000".
- **Budget zone:** shaded rectangle `rgba(0,131,0,0.07)` from 0 to 200 ms, y=60 down to the axis.
- **Deadline:** dashed red `#e74c3c` vertical line (dash 6/4, width 2) at 200 ms, labeled above in red bold 13px "deadline: 200ms".
- **Events** (8px colored dot on the axis, vertical stem up to a staggered label — bold 13px "label @ Nms" plus 12px `#444` sub-line; even-index labels lower, odd-index 26px higher):
  - 0 ms — "swipe", sub "$840 arrives", blue `#2a78d6`
  - 45 ms — "enrich", sub "history fetched", aqua `#199e70`
  - 120 ms — "score", sub "model says 0.91", violet `#4a3aa7`
  - 180 ms — "decide", sub "DECLINE sent", orange `#d95926`
- **Annotations (bottom center):** green `#008300` bold 13px "done at 180ms — 20ms to spare"; muted 12px "the next payment gets the same treatment, thousands of times a second".

## One Payment Rides the Belt

**Tags:** `worked example` (green), `core idea` (blue)

- **Event in** — payment of $840 on card ****4417 at an electronics store, 14:32:07.000
- **Enrich** — look up the card's recent history: 3 payments in the last 10 minutes, average past purchase $62
- **Score** — the model sees $840 vs a $62 habit plus a burst of 3 — fraud score 0.91
- **Decide** — 0.91 is above the 0.80 threshold, so the payment is declined
- **Done at 180ms** — the decision is out the door 20ms under budget

*Example:* By hand: $840 is 13x this card's $62 average, and it is the 4th charge in 10 minutes — decline.

**Key point:** A streaming job is a small function applied to every single event: enrich it, score it, act on it — then forget it and take the next one.

### Visualization (canvas `c2`, 720×300)

Four-stage pipeline diagram with the actual numbers, boxes connected by muted arrows.

- **Title (bold 15px, `#1a5276`, top center):** "The $840 Payment, Step by Step".
- **Stage boxes (all at y=60, height 130, 2px colored stroke, bold 13px colored heading, 12px `#444` detail lines):**
  - "1. event" — x=24, 150 wide, fill `rgba(42,120,214,0.08)`, stroke blue `#2a78d6`; lines "$840", "card ****4417", "electronics store", "14:32:07.000"
  - "2. enrich (+45ms)" — x=208, 160 wide, fill `rgba(25,158,112,0.08)`, stroke aqua `#199e70`; lines "3 payments in", "last 10 minutes", "avg purchase: $62"
  - "3. score (+120ms)" — x=402, 150 wide, fill `rgba(74,58,167,0.08)`, stroke violet `#4a3aa7`; lines "$840 vs $62 habit", "+ burst of 3", then violet bold 16px "score 0.91"
  - "4. decide" — x=586, 116 wide, fill `rgba(217,89,38,0.10)`, stroke orange `#d95926`; lines "0.91 > 0.80", "threshold", then red `#e74c3c` bold 15px "DECLINE"
- **Arrows:** muted `#6b7280` horizontal arrows between consecutive boxes at mid-height.
- **Annotations (bottom center):** orange bold 13px "one small function, applied to every event as it passes — no pile, no waiting"; muted 12px "then the event is done, and the belt delivers the next one".

### Payload block (under canvas `c2`)

Italic `.payload-note`: "The event as it enters the pipeline — illustrative record."

Monospace `.payload` pre block (verbatim):

```
{ "event": "payment_attempt",
  "ts": "2026-08-24T14:32:07.000Z",
  "card": "****4417",
  "amount": 840.00,
  "merchant": "electronics-store-291",

  // added by the enrichment step, 45ms later
  "history": { "payments_last_10min": 3, "avg_amount_90d": 62.00 },

  // added by the model step, 120ms after the swipe
  "fraud_score": 0.91, "decision": "DECLINE" }
```

## Windows: How a Belt Computes "In the Last 10 Minutes"

**Tags:** `core idea` (blue), `rule of thumb` (blue)

- **The problem** — "3 payments in the last 10 minutes" is a sum, but the belt sees one event at a time
- **The trick** — keep a running window: a 10-minute frame that slides along with the clock
- **On arrival** — add the new payment to the window, drop everything older than 10 minutes
- **Tiny memory** — the system remembers only the window's contents, never the whole history
- **Same idea** — counts per minute, sums per hour: every streaming aggregate lives in a window

*Example:* At 14:32 the window [14:22–14:32] holds $35, $120, and $310 — so "3 recent payments" before the $840 one.

**Key point:** Windows are how streams do aggregates — a moving frame over recent events replaces batch's frozen pile.

### Visualization (canvas `c3`, 720×300)

Timeline with a shaded sliding window and payment dots.

- **Title (bold 15px, `#1a5276`, top center):** "A 10-Minute Window Sliding Along the Stream".
- **Axis:** horizontal 2px `#999` line at y=190 spanning 14:18–14:34 (left pad 60, right pad 30); ticks with 12px labels at every 2 minutes: "14:18", "14:20", "14:22", "14:24", "14:26", "14:28", "14:30", "14:32", "14:34".
- **Window:** shaded rectangle `rgba(42,120,214,0.10)` with dashed blue `#2a78d6` border (dash 6/4, width 2) from 14:22 to 14:32, y=70 down to the axis; blue bold 13px label above: "window: last 10 minutes [14:22 – 14:32]".
- **Payment dots (7px radius at y=axis−45, bold 12px amount label above each):**
  - 14:19.5 — "$12", muted `#6b7280` (outside window)
  - 14:22.8 — "$35", blue `#2a78d6` (in window)
  - 14:24.9 — "$120", blue (in window)
  - 14:28.4 — "$310", blue (in window)
  - 14:32 — "$840", orange `#d95926`, larger 9px dot, bold 13px (current event)
- **Dot annotations:** muted 12px "too old —" / "dropped" above the $12 dot; orange bold 12px "new event" above the $840 dot.
- **Captions (bottom center):** blue bold 13px "the window holds exactly 3 prior payments: $35 + $120 + $310"; 12px `#444` "each new event: add it, evict anything older than 10 minutes, recount — memory stays tiny".

## The Price Tag: You Pay in Complexity for Freshness

**Tags:** `trade-off` (orange), `common mistake` (red)

- **Freshness win** — a nightly batch answer can be 26 hours old; the stream answers in 0.2 seconds
- **Always-on cost** — a cluster running 24/7 instead of one machine for 75 minutes
- **Ordering pain** — events arrive late or out of order; batch never has this problem
- **Retry pain** — a crashed stream must resume mid-flow without double-counting; batch just reruns
- **The mistake** — building streaming for a report nobody reads before 9am

*Example:* A team streamed their exec dashboard, then noticed it is opened once a day — a 2am batch was enough.

**Key point:** Choose streaming only when someone acts on the answer within minutes — otherwise you bought the complexity and shelved the freshness.

### Visualization (canvas `c4`, 720×300)

Two-panel comparison: freshness gain (log-scale bars) vs the machinery taken on, split by a dashed `#bdc3c7` vertical divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "What You Gain vs What You Sign Up For".
- **Left panel:** heading ink bold 13px "age of the answer (log scale)". Two horizontal bars starting at x=84, height 30, length log10(seconds) mapped from 0.1 to 200,000 over 240px:
  - "nightly batch" (label right-aligned 12px `#222`) — 93,600 s, fill `rgba(42,120,214,0.5)`, stroke blue `#2a78d6`; blue bold 12px note above the bar "up to 26 hours"
  - "streaming" — 0.2 s, fill `rgba(217,89,38,0.55)`, stroke orange `#d95926`; orange bold 12px note "0.2 seconds"
  - Below: green `#008300` bold 13px "~500,000x fresher"; muted 12px "that is the entire benefit".
- **Right panel:** heading ink bold 13px "the machinery you take on"; four magenta `#d55181` bullet dots at x=400, each with a bold 12px `#222` line and a muted 12px sub-line:
  - "cluster running 24/7" / "vs one machine, 75 min/night"
  - "late & out-of-order events" / "batch inputs are frozen"
  - "resume without double-counting" / "batch just reruns"
  - "windows & state to manage" / "batch state is the whole pile"
- **Caption (magenta bold 13px, bottom of right panel):** "worth it only if someone acts within minutes".

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (one `<tr>`; left `td.text-col` 50% width, right `td.viz-col` 50% width, cells padded 12px, no cell borders). Section 2's viz cell holds the canvas, then the `.payload-note` and `.payload` pre block.
- **Text column structure:** `.tags` row of colored pill spans (`.tag` — 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each starting with `<b>` in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) opening with `<strong>Key point:</strong>`.
- **Payload styles:** `.payload` — background `#f8f9fa`, left border 3px solid `#1a5276`, padding 10px, ui-monospace/Menlo 0.78em, `white-space: pre`, `overflow-x: auto`, line-height 1.45, left-aligned. `.payload-note` — 0.82em, `#666`, italic, left-aligned, margin 12px 0 -6px 0.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box; }`; h1 2rem `#1a5276`; subtitle `#666` 0.95rem. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
- **Canvas:** each canvas declared `width="720" height="300"`, CSS `width:100%`, border `1px solid #e0e0e0`, radius 4px; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
