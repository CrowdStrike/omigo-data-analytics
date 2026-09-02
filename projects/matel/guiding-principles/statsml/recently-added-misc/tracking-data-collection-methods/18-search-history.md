# Tracking Data: Search History

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: Search History

**Subtitle:** A query log stores intent in the user's own words rather than in a category someone assigned. That is what makes it valuable, and it is also why it is so easy to misread — a query records what was asked, not what was true.

## Section 1: What is it?

**Lede:** A server-side log of queries, keyed to whatever identifier the session carried.

- **Identifier:** an account if signed in, otherwise a cookie or device identifier
- **Signed out still logs** the row — just keyed differently
- **Retention is a setting** — major providers offer auto-delete windows and a history view

**Callout:** **Unusually direct data:** most behavioural datasets record a choice among options the operator presented. A query is free text the user composed, so no taxonomy constrains it.

**Callout:** **Which is why it needs care:** the words are the user's, but the meaning attached to them afterwards is the analyst's.

### Visualization (canvas `c1`, 720×240)

Search bar with a cascading list of example queries. Hue carries the subject area of each string; marker shape carries how many intents the string admits (filled dot = one likely intent, open ring = many possible), measured against a vertical axis on the left. Note: this canvas declares its size via inline style `width:720px;height:240px` (no width/height attributes), so `setupCanvas` falls back to its 720×240 defaults.

- **Search bar:** rounded rect at (120,16) 480×32 r16, `#f8f9fa` fill, ink `#1a5276` stroke width 2; magnifying-glass icon (circle radius 7 + handle) in ink at the left end; placeholder text "Search..." in mute `#6b7280` 16px.
- **Query rows (start y=68, spacing 22, text 15px in the row's hue, marker dot radius 4.5 at w/2−160):**
  1. "best pizza near me" — area food, blue `#2a78d6`, filled (one intent)
  2. "how to fix leaky faucet" — area home repair, green `#008300`, filled
  3. "average salary software engineer" — area work, violet `#4a3aa7`, filled
  4. "why am I always tired" — area health, orange `#d95926`, ring (many intents)
  5. "chest pain left side" — area health, orange `#d95926`, ring
  6. "is it normal to feel alone" — area wellbeing, magenta `#d55181`, ring
  7. "divorce lawyer free consultation" — area legal, aqua `#199e70`, ring
- **Intent axis (left of the markers, x = w/2−182):** dashed vertical line (dash 4/3, width 1.5) in ink at alpha 0.45 with a downward arrowhead; right-aligned 13px end labels: "one likely intent" (text color, top) and "many possible intents" (mute, bottom).
- **Right-hand key (x≈626, from y=72, 18px row spacing):** 9×9 swatches with 12px labels for the distinct subject areas in order: food (blue), home repair (green), work (violet), health (orange), wellbeing (magenta), legal (aqua).
- **Bottom line (12px mute, left-aligned at (20,232)):** "The lower rows are not more revealing — they are more ambiguous."

## Section 2: What does it collect?

- **Query text** with timestamp, locale and device class
- **Clicked result** — which one, at which rank, and dwell before returning
- **Autocomplete requests** — the suggest endpoint is called as characters are typed, so partial strings reach the server whether or not the search is submitted
- **Voice queries** as a transcript; whether the audio is retained depends on the account setting
- **Reformulations** — the next query in the same session
- **Coarse location** associated with the request — usually derived from the IP address, typically to city or region level

**Callout:** **The null click is still a row:** searches where nothing was opened are recorded too, so the dataset covers what people looked for *and did not find*.

**Callout:** **`abandoned` is derived, not measured:** a user who read the answer in the result snippet and needed no click is coded the same as one who found nothing.

### Visualization (canvas `c2`, 720×240)

24-hour timeline with five search events for one identifier. Hue carries the topic thread: the recurring health thread is one colour (orange); unrelated one-off queries get their own hues. Sized via inline style `width:720px;height:240px` (setupCanvas defaults).

- **Title (bold 15px ink, left-aligned at (60,22)):** "One identifier, one day — schematic session timeline".
- **Thread key (12px, swatches at y=34):** "health thread — recurs three times" (orange `#d95926`, x=60), "work" (violet `#4a3aa7`, x=320), "food" (blue `#2a78d6`, x=400).
- **Timeline:** horizontal ink line width 2 at y=118 from x=60 to x=660; hour tick marks and 14px mute labels: 12am, 3am, 6am, 9am, 12pm, 3pm, 6pm, 9pm, 11pm (evenly spaced).
- **Events (dot radius 5 on the line, x positioned by time/23 across the axis; 13px label in the event's hue, above or below, connected by a dotted 2/2 connector; label x clamped inside the canvas):**
  1. time 2.23 — "02:14  chest pain left side" — health, orange, above
  2. time 2.32 — "02:19  reformulated, narrower" — health, orange, below
  3. time 9.05 — "09:03  jobs hiring near me" — work, violet, above
  4. time 12.5 — "12:30  lunch nearby" — food, blue, below
  5. time 23.75 — "23:45  same topic, third session" — health, orange, above
- **Thread tie-line:** dashed orange line (dash 5/4, width 1.2, alpha 0.4) at y=104 connecting the three health events, so three sessions read as one thread.
- **Bottom lines (12px mute, left-aligned at x=60, y=218 and 231):** "The rows show recurrence and reformulation. They do not show who typed them, or why —" / "a shared device merges several people into this one line."

**Payload note (right column, below canvas):** Sample payload — illustrative structure, not real captured data.

```
// No public API exposes a search engine's internal
// query log. Field names below are reconstruction.
{
  // ── inferred / plausible ──
  "query_id":      "q_7f3a…",
  "session_id":    "s_91be…",
  "query_text":    "chest pain left side worse lying down",
  "ts":            "2026-08-22T02:47:11Z",
  "locale":        "en-US",
  "device":        "mobile",
  "geo_coarse":    "region-level",
  "results_shown": ["r1","r2","r3","…"],
  "clicked_rank":  null,          // no result opened
  "dwell_ms":      null,          // no click, so no dwell
  "reformulated_to": "q_7f3b…",  // next query, 9s later
  "abandoned":     true           // derived from clicked_rank
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Ranking is learned from the log** — click position, dwell, and whether a session ended or was reworded are the relevance signals
- **Also drives** spelling correction and autocomplete; a ranker with no log degrades as the world changes

**Label (effect pill):** Additional consequence

- **Topic-interest segments** for ad targeting, finer than a survey would produce because the text is unconstrained
- **Retained rows are discoverable** — available to legal process in a way an unrecorded thought is not. A property of retention, not of anyone's intent

**Callout:** **The training data is generated by the model being trained:** people mostly click what was shown near the top, so the log measures interest interacting with placement, not interest alone. A model fitted to it reinforces its own ranking unless position bias is corrected for.

### Visualization (canvas `c3`, 720×240)

Position-bias chart: horizontal bars of click share by result rank (rank 1 highlighted blue against a neutral mute field), plus a right-hand panel describing the training loop with one hue per stage. Sized via inline style `width:720px;height:240px` (setupCanvas defaults).

- **Title (bold 15px ink, left-aligned at (20,22)):** "Clicks by result rank — schematic, illustrative shape". Subtitle (12px mute, y=37): "The ranker chose the order. The log then records clicks against that order."
- **Bars:** click shares by rank 1–10: `[0.94, 0.62, 0.41, 0.27, 0.18, 0.12, 0.08, 0.05, 0.035, 0.02]`; bar height 13, gap 5, left edge x=90, max width 330, top y=52. Rank 1 bar solid blue `#2a78d6` with bold blue "rank 1" label; ranks 2–10 filled mute `#6b7280` at alpha 0.35 with plain mute "rank N" labels (right-aligned at x=82).
- **Bracket:** dashed orange `#d95926` vertical line (dash 4/3, width 2) alongside ranks 2–10, with rotated 11px orange label "rarely tested".
- **Training-loop panel:** rect (450,52) 240×148, ink fill alpha 0.06, ink stroke 1.5; bold 12px ink heading "The training loop"; five numbered 12px lines, each with a 3×10 color chip and number in the series hue (blue, green, violet, orange, aqua in order):
  1. Ranker orders the results.
  2. User mostly clicks near the top.
  3. Log records those clicks.
  4. Ranker trains on the log.
  5. Ranker keeps the same order.
  Below, bold 12px orange: "A good result placed low" / "produces no clicks, so it looks bad."
- **Bottom line (bold 12px text color, left-aligned at (20,228)):** "The training data is generated by the model being trained — position bias has to be corrected for explicitly."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede` paragraph, bullets with bolded lead terms (`li b` in `#1a5276`), `.lbl` purpose/effect pills, and `.key-point` callouts; right `<td>` (55%, `text-align: center`) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption plus `.payload` `<pre>` block (both left-aligned).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em. `.key-point` and `.payload`: background `#f8f9fa`, left border `3px solid #1a5276`; `.payload` ui-monospace 0.78em pre, `.payload-note` 0.82em italic `#666`. `.lbl` pills: uppercase 0.7em bold, `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`. No nav bar, no back/home links.
- **Canvas:** on this page the three canvases carry inline styles `width:720px;height:240px` and no width/height attributes, so the shared `setupCanvas(id)` helper uses its 720×240 fallback; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Charts share a rounded-rect path helper and a `tint(hex, alpha)` helper producing rgba fills from palette tokens.
- **Palette:** this page's charts use the tracking categorical palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Site-wide accents remain #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
