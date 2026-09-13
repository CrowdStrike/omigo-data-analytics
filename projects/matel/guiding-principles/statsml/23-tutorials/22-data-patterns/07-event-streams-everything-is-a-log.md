# Event Streams: Everything Is a Log

**Page type:** detail page (tutorial card-section layout: one h2 per section, two-column `table.layout` with 50% text / 50% viz)
**HTML title tag:** Event Streams: Everything Is a Log

**Subtitle:** Record what HAPPENED as an append-only list of events — every table is just a replay of that list

## One Shopper, Four Events, Never an Edit

**Tags:** `core idea` (blue), `running example` (green)

- **10:01** — a shopper views a $64 coffee grinder: event "product_viewed" is appended
- **10:04** — she adds it to the cart: "added_to_cart" is appended after the first event
- **10:09** — she buys it: "order_placed" for $64 joins the end of the list
- **Tuesday** — she returns it: "refund_issued" is appended too — nothing is erased
- **Append-only** — the log only ever grows at the end; old events are never edited

**Example (italic):** Even the refund did not delete the purchase — it became a fourth line in her story.

**Key point:** An event records a fact about the past — "this happened at this time" — and facts don't get updated, only followed by newer facts.

### Visualization (canvas `c1`, 720×300)

Horizontal timeline diagram of the shopper's four events on a time axis.

- **Title (bold 15px, ink `#1a5276`, top center):** "One Shopper's Log — Events Only Ever Get Appended"
- **Axis:** horizontal line at y=160 from x=50 to x=680, color `#999`, width 2, with a filled right-pointing arrowhead at x=690.
- **Events (8px-radius filled dots on the axis), alternating labels above/below (even index up, odd index down):**
  | x | timestamp | label | sub-label | color |
  |---|-----------|-------|-----------|-------|
  | 100 | Mon 10:01 | product_viewed | grinder, $64 | `#2a78d6` (blue) |
  | 250 | Mon 10:04 | added_to_cart | grinder | `#199e70` (aqua) |
  | 400 | Mon 10:09 | order_placed | order 7712, $64 | `#008300` (green) |
  | 580 | Tue 09:14 | refund_issued | order 7712, $64 | `#d95926` (orange) |
- Each dot has a short connector line in its color to a stacked label: "N. event_name" (bold 13px in event color), sub-label (12px `#444`), timestamp (bold 12px mute `#6b7280`).
- **Day break:** a double diagonal squiggle across the axis around x=483–501 (gray `#999` strokes with a white gap stroke), labeled "overnight" (11px mute) below the axis.
- **Append cursor:** violet `#4a3aa7` bold 13px two-line text near the arrow end (x=655, above axis): "next event" / "appends HERE".
- **Caption (bold 13px orange `#d95926`, bottom center, y=278):** "the refund did not erase the purchase — it was appended after it"

## Replaying the Log Builds the Current State

**Tags:** `worked example` (green), `core idea` (blue)

- **The question** — "what is order 7712's status and our net revenue from it right now?"
- **The method** — start from empty, read the events in order, apply each one
- **After event 3** — status: paid, net revenue: $64
- **After event 4** — status: refunded, net revenue: $0
- **State is derived** — the table row is not stored truth; it is the log's running total

**Example (italic):** Replay by hand: {} → viewed → in cart → paid $64 → refunded $0 — four steps, done.

**Key point:** Events are the facts; state is what you get by replaying them — the same log always replays to the same state.

### Visualization (canvas `c2`, 720×300)

Four state boxes left-to-right connected by arrows, showing the state after each event.

- **Title (bold 15px ink, top center):** "Replay: Start Empty, Apply Each Event in Order"
- **Boxes:** 148px wide × 150px tall, starting at x=32, y=62, 24px gaps; fill `rgba(42,120,214,0.05)`, 2px stroke in the state color; title (bold 13px, state color) centered at top, then left-aligned field names ("cart:", "status:", "net rev:") with right-aligned bold values:
  | title | cart | status | net rev | border color |
  |-------|------|--------|---------|--------------|
  | after 1. viewed | (empty) | — | $0 | `#2a78d6` (blue) |
  | after 2. carted | grinder | — | $0 | `#199e70` (aqua) |
  | after 3. purchased | (empty) | paid | $64 | `#008300` (green) |
  | after 4. refunded | (empty) | refunded | $0 | `#d95926` (orange) |
- **Arrows:** mute gray `#6b7280` horizontal arrows between consecutive boxes at mid-height.
- **Captions (bottom center):** bold 13px orange: "current state = whatever the full replay ends on: refunded, $0 net" (y=248); 12px mute: "replay the same 4 events tomorrow, next year, on a new machine — identical state every time" (y=272).

**Payload note (italic, below canvas):** The log itself — illustrative events, one line appended per happening.

**Payload block (monospace, `#f8f9fa` background, left border 3px solid `#1a5276`):**

```
{ "seq": 1, "ts": "Mon 10:01:12", "event": "product_viewed",  "user": "u42", "item": "grinder" }
{ "seq": 2, "ts": "Mon 10:04:55", "event": "added_to_cart",   "user": "u42", "item": "grinder" }
{ "seq": 3, "ts": "Mon 10:09:31", "event": "order_placed",    "user": "u42", "order": 7712, "amount": 64.00 }
{ "seq": 4, "ts": "Tue 09:14:02", "event": "refund_issued",   "user": "u42", "order": 7712, "amount": 64.00 }
```

## Why Keep the Log? You Can Rebuild Any Table From It

**Tags:** `where it's used` (blue), `best practice` (green)

- **Many tables, one log** — orders, revenue, and funnel tables are all replays of the same events
- **New questions later** — "how many carts were abandoned?" — replay the old log with a new counter
- **Bug recovery** — a broken revenue table is dropped and rebuilt from the log, not patched
- **Time travel** — replay only events up to March 31 to see exactly what March looked like
- **Audit trail** — every state a row was ever in is still in the log, with a timestamp

**Example (italic):** A year in, the team added an "abandoned carts" table — computed back to day one from the existing log.

**Key point:** Tables answer the questions you thought of; the log answers the ones you haven't thought of yet.

### Visualization (canvas `c3`, 720×300)

Fan-out diagram: one event-log box on the left feeding four derived-table boxes on the right via labeled arrows.

- **Title (bold 15px ink, top center):** "One Log, Many Tables — Including Ones Invented Later"
- **Log box:** at (40, 90), 180×120, fill `rgba(26,82,118,0.06)`, ink `#1a5276` border; contains bold 13px ink "the event log", monospace 11px `#444` two lines "viewed / carted /" and "purchased / refunded", italic 11px mute "append-only, kept forever".
- **Derived-table boxes:** 210×48 at x=470, fill `rgba(0,131,0,0.04)`, 2px border in each color (dashed for the last one); bold 13px label + 12px `#444` sub-label, with an arrow from the log box (220,150) to each box (dashed for the last):
  | y-center | label | sub | color | dashed |
  |----------|-------|-----|-------|--------|
  | 48 | orders table | status per order | `#008300` (green) | no |
  | 118 | daily revenue | net $ per day | `#199e70` (aqua) | no |
  | 188 | view → buy funnel | conversion steps | `#4a3aa7` (violet) | no |
  | 252 | abandoned carts | added a year later | `#d95926` (orange) | yes |
- **Annotations:** bold 12px `#444` "replay" at (340, 88); bold 12px orange two lines "new table, old answers:" / "replayed back to day one" at (330, 238/254).

## The Confusion: Updating State vs Appending Events

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The overwrite** — a table that sets status from "paid" to "refunded" destroys the "paid" fact
- **Lost questions** — that table can no longer answer "how long between purchase and refund?"
- **The log keeps both** — order_placed Mon 10:09 and refund_issued Tue 09:14: gap = 23 hours
- **Fixing mistakes** — a wrong event is corrected by appending a correction, never by editing
- **Rule of thumb** — if deleting a row would lose history someone might ask about, log events instead

**Example (italic):** Your bank statement is a log — the bank appends a reversal; it never edits last week's line.

**Key point:** An UPDATE is a fact-shredder — every overwrite quietly deletes a piece of history the log would have kept for free.

### Visualization (canvas `c4`, 720×300)

Split-panel comparison: table-with-UPDATE on the left vs log-with-APPEND on the right, divided by a vertical dashed line at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px ink, top center):** "Overwriting Destroys a Fact; Appending Keeps Both"
- **Left panel (header bold 13px magenta `#d55181`, centered at x=185):** "table with UPDATE"
  - Old row box (50,76, 270×34), gray fill `rgba(107,114,128,0.10)`, mute border; monospace 13px mute text "order 7712 | status: paid" with a red `#e74c3c` strikethrough line across it.
  - Bold 12px red annotation: "UPDATE overwrites the cell ↓"
  - New row box (50,146, 270×34), fill `rgba(213,81,129,0.08)`, magenta border; monospace 13px `#222` "order 7712 | status: refunded".
  - Bold 13px red two lines: '"was it ever paid? when?"' / "— the table no longer knows".
- **Right panel (header bold 13px green `#008300`, centered at x=540):** "log with APPEND"
  - Two log-line boxes (395, y=76 and y=122, 290×34), fill `rgba(0,131,0,0.05)`; monospace 13px `#222` text; borders green then orange:
    - "Mon 10:09  order_placed   $64" (green `#008300`)
    - "Tue 09:14  refund_issued  $64" (orange `#d95926`)
  - Violet `#4a3aa7` bracket at the right edge spanning both rows, with rotated bold 11px violet label "23h".
  - Bold 13px green two lines: "both facts kept: paid Mon 10:09," / "refunded 23 hours later".
- **Caption (bold 13px orange, bottom center, y=278):** "same business story — only one side can still tell all of it"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (reference `../most-powerful-signals/07-social-graph-connections.html` style). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray line, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `td.text-col` (50%) holding tags/bullets/example/key-point and `td.viz-col` (50%) holding the canvas (plus `.payload-note` and `.payload` pre-block in section 2).
- **Text column structure:** `.tags` row of colored pill spans (0.72rem bold, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); 5 one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius. `.payload` monospace 0.78em, `#f8f9fa` background, left border 3px solid `#1a5276`, pre-wrap off. No nav bar, no back/home links.
- **Canvases:** intrinsic 720×300; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded (no Math.random). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card/grid links use `.html` extensions (this page has none — no cross-page links).
