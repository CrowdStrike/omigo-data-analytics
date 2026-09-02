# Tracking Data: Analytics Platforms

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows)
**HTML title tag:** Tracking Data: Analytics Platforms

**Subtitle:** Hosted services — Google Analytics, Mixpanel, Amplitude — that receive named events with attached properties and assemble them into sessions, funnels, and per-user profiles.

## What is it?

A store for named events, with the stitching done on the read side.

- **Input:** a named event — `page_view`, `add_to_cart`, `purchase` — with properties and a sender identifier
- **Platform's contribution:** stitching events into sessions and funnels, and keeping a rolling profile per identifier
- **Device identifier** generated on first visit, stored in a cookie
- **Account identifier** attached later, if and when the person signs in
- **Sessions cut by inactivity timeout**, typically 30 minutes

**Key point callout:** **Sessions and funnels are constructions:** nothing in the incoming data marks where one visit ends. That boundary is a configurable timeout, and changing it moves every session-based metric with no change in behaviour.

### Visualization (canvas `c1`, 720×320)

Funnel chart: nested trapezoids narrowing down the page, with per-stage counts and drop-off labels.

- **Title (bold 16px `#2a78d6`, top center):** "User Funnel — where people drop off".
- **Stages (label, pct of max width 500, user count):** "Visit Homepage" 100% "10,000"; "View Product" 60% "6,000"; "Add to Cart" 25% "2,500"; "Start Checkout" 12% "1,200"; "Complete Purchase" 4% "400".
- **Shapes:** centered trapezoids starting y=40, step height 36; each stage's top width = pct×500, bottom width = next stage's pct×500 (last: 70% of own width); fill `rgba(42,120,214, 0.2 + i*0.15)` so lower stages darken.
- **Labels:** stage name in white 15px centered inside; right of each bar in `#2a78d6` 14px: "10,000 (100%)" etc.; left of each bar (except last) magenta `#d55181` 13px drop-off "-40% left", "-35% left", "-13% left", "-8% left".
- **Caption (bottom center, 14px `#6b7280`):** "Analytics platforms track exactly where each person gives up".

## What does it collect?

- **Named events** in order, with a timestamp on each
- **Engagement duration** sent per event
- **Event properties** — product, price, currency, screen
- **Funnel stop point** — where the sequence ended
- **Device type**, browser, screen size, referring source
- **Coarse location** — usually derived at the platform from the IP address, typically to city level; the site never sends it
- **Device identifier**, plus an account identifier once signed in

**Key point callout:** **Two epistemic statuses, one storage:** `value: 128.40` was reported by the site. `segment: "high_value"` and `churn_risk: 0.31` are model output with an error rate nobody downstream sees.

**Key point callout:** **Model output cited as observation:** once both sit in the same profile row, exports, audiences and dashboards read them identically.

### Visualization (canvas `c2`, 720×320)

Dashboard metrics grid: 4×2 rounded stat tiles ("What the dashboard shows about YOU").

- **Title (bold 16px `#2a78d6`, top center):** "What the dashboard shows about YOU".
- **Tiles (label / value / sub):** Sessions / 47 / this month; Avg Time / 3m 22s / per visit; Pages/Visit / 6.4 / average; Bounce Rate / 12% / leave immediately; Device / iPhone / Safari 17; Location / Seattle / WA, USA; Source / Google / organic search; Segment / High Value / likely to buy.
- **Layout:** 4 columns × 2 rows, cell 160×85, 10px gaps, starting (40,40); rounded rect radius 6, fill `#f8f9fa`, stroke `#2a78d6` width 1. Label 14px `#6b7280` at cell top, value bold 16px `#2a78d6` in middle, sub 13px `#6b7280` below; all centered.

**Payload note (below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, verbatim):**

```
POST /mp/collect?measurement_id=…&api_secret=…
{
  // ── documented in the GA4 Measurement
  //    Protocol reference ──
  "client_id": "1842756193.1755820000",
  "user_id": "u_44817",              // optional
  "timestamp_micros": 1787840587000000,
  "consent": { "ad_user_data": "DENIED" },
  "events": [{
    "name": "purchase",
    "params": {
      "session_id": "1787839900",
      "engagement_time_msec": 41200,
      "transaction_id": "T-90214",
      "currency": "USD",
      "value": 128.40,
      "items": [ { "item_id": "SKU-771", … } ]
    }
  }]
}

// The same person's stored profile, server side:
{
  // ── inferred / plausible ──
  "segment": "high_value",       // model output
  "churn_risk": 0.31,            // model output
  "ltv_pred_usd": 940,           // model output
  "cohort": "2026-03-signup"     // derived
}
```

## Why is it collected?

**Label (purpose pill):** Stated purpose

- **Product measurement** — which features get used, where signup breaks
- **Release checks** — whether a change helped or hurt

**Label (effect pill):** Additional consequence

- The profile store doubles as an **audience builder**, and segments computed for reporting can be **exported to ad platforms**
- Can be used to **vary what different people are shown** — a different activity from measurement, on one dataset

**Key point callout:** **A funnel number needs its definition:** the steps are chosen after the fact by the analyst. Reordering or dropping a step changes the conversion rate, and a dashboard rarely displays which steps were used.

### Visualization (canvas `c3`, 720×320)

Horizontal bar chart: one set of events, three funnel step definitions, three conversion rates (counts match the funnel in `c1`).

- **Title (bold 14px `#1a5276`, center):** "Same events, same 400 purchases, three conversion rates"; subtitle (12px `#6b7280`): "only the first step of the funnel changes".
- **Data (steps label, starting count, rate = 400/from, color):** "homepage → purchase" from 10,000 → 4.0% `#2a78d6`; "product view → purchase" from 6,000 → 6.7% `#199e70`; "start checkout → purchase" from 1,200 → 33.3% `#4a3aa7`.
- **Layout:** bars start x=268, max width 340 = 40% scale, first row y=70, row height 56, bar height 26; fill is the row color at 0.38 alpha, stroke solid width 1. Steps label right-aligned 13px `#2c3e50` left of the bar; rate bold 14px in the row color right of the bar; under each bar 12px `#6b7280` "400 of 10,000" / "400 of 6,000" / "400 of 1,200".
- **Scale:** thin `#e5e9ef` axis line under the bars with 11px `#6b7280` tick labels 0%, 10%, 20%, 30%, 40%.
- **Annotation (12px `#2c3e50`, left-aligned, two lines at y=268/286):** "All three are correct statements about the same week. Which one reaches a slide" / "depends on the step list, and the dashboard prints the rate, not the list."
- **Caption (bottom center, italic 11px `#6b7280`):** "Illustrative counts, matching the funnel above."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` + bullets/`.key-point` callouts, right `<td>` (55%, `text-align:center`) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre below the canvas, left-aligned).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; li 0.93em with `li b` in `#1a5276` weight 600; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase — `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em `#666` italic. No nav bar, no back/home links.
- **Palette:** charts declare a tokens object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, violet:#4a3aa7, orange:#d95926, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; red is deliberately excluded from the series rotation, reserved for alarm states. Site palette anchors: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setupCanvas(id)` helper reads the element's own attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helper `rr()` draws rounded-rect paths.
- No `Math.random()` in charts — all data arrays are hardcoded literals; invented numbers are labeled "illustrative".
