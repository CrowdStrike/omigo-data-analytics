# Tracking Data: JavaScript Tags

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows)
**HTML title tag:** Tracking Data: JavaScript Tags

**Subtitle:** Scripts loaded with the page that subscribe to browser events — clicks, scrolls, focus changes — and post them back to a collector in batches.

## What is it?

A script that listens to events the browser already fires.

- **Delivery:** a small piece of JavaScript shipped with the page, usually via a tag manager
- **No new capability:** clicks, scrolls and focus changes already fire so the page can respond
- **The tag keeps a copy** by registering its own handlers on the same events
- **Sent in batches** to a collector — on a timer, or on unload via `sendBeacon`

**Key point callout:** **Inherits the page's failure modes:** a script that fails to load, is blocked, throws, or whose buffer never flushes records nothing. Absence in the data is not absence of the behaviour.

### Visualization (canvas `c1`, 720×320)

Schematic illustration: a browser window wireframe with JS script injection points marked as magenta dots, plus a legend on the right.

- **Browser frame:** rounded rect (radius 8) at (50,20) size 400×200, stroke `#2a78d6` width 2. Title bar fill `#f0f4f8` at (52,22) 396×25 with three traffic-light circles (radius 5) at x=68/84/100, y=34, colored `#d55181`, `#d95926`, `#008300`. URL bar: white rect (115,27) 320×16 with `#e5e9ef` border, gray (`#6b7280`) 13px text "www.shopping-site.com/shoes".
- **Page content area:** white rect (52,48) 396×170 with gray `#e5e9ef` placeholder blocks at (70,60) 180×20, (70,90) 360×80, (70,180) 100×25.
- **Injection points:** five magenta (`#d55181`) filled circles radius 8 with white bold 13px "JS" labels, at (70,60) "scroll listener", (250,60) "click tracker", (430,90) "mouse recorder", (170,180) "form spy", (350,170) "exit detector".
- **Legend (right):** heading "Third-party scripts:" in bold 16px `#2a78d6` at (490,50); below it five magenta dots (radius 5) at x=500 starting y=72 step 22, each followed by the point's label in 14px `#2c3e50`.
- **Caption (bottom center, 14px `#6b7280`):** "Page looks normal — scripts are invisible" at (250,228).

## What does it collect?

- **Page URL and referrer** with each pageview — often including query-string parameters
- **Clicks and taps**, with the CSS selector of the element hit
- **Scroll depth**, usually as percentage milestones
- **Hover duration** on a target element
- **Focus and blur** on form fields — that a field was touched
- **Rage clicks** — repeated clicks in a small area, short window
- **Viewport size and page timing** from the Performance API
- **Precision:** timestamps are typically millisecond-scale — browsers deliberately coarsen finer timers — and mouse moves are usually sampled, not continuous

**Key point callout:** **Touched, not read:** `"filled": true` instead of the postcode. Capturing values requires opting the field in, which is why replay tools ship field-masking as a setting.

**Key point callout:** **Testimony, not measurement:** every field is asserted by a client under the visitor's control. The batch can be dropped, replayed or edited. Reconcile against a server-side record before treating a count as real.

### Visualization (canvas `c2`, 720×320)

Timeline chart: events captured over 10 seconds of browsing, one dot per second along a horizontal axis.

- **Title (bold 16px `#2a78d6`, top center):** "Events captured in just 10 seconds of browsing".
- **Timeline:** horizontal line y=130 from x=40 to x=680, stroke `#2a78d6` width 2.
- **Events (dot radius 5, tick line to label, alternating above/below; time label 13px `#6b7280` on the opposite side):**
  - 0s "Page load" `#2a78d6` x=50
  - 1s "Mouse move" `#008300` x=115
  - 2s "Scroll down" `#008300` x=180
  - 3s "Hover button" `#d95926` x=245
  - 4s "Click link" `#d55181` x=310
  - 5s "Type in box" `#d55181` x=375
  - 6s "Delete text" `#d95926` x=440
  - 7s "Scroll up" `#008300` x=505
  - 8s "Rage click" `#e74c3c` x=570
  - 9s "Move to exit" `#d55181` x=635
- **Footer (bold 16px `#d55181`, bottom center):** "Also captured: pointer coordinates, scroll offsets, and event timings".

**Payload note (below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, verbatim):**

```
POST /collect  (batched, flushed every ~5s or on unload)
{
  // ── inferred / plausible: tag vendors differ,
  //    but this is the common shape ──
  "session_id": "s_9f2c…",
  "page": "/checkout/shipping",
  "viewport": { "w": 1440, "h": 780 },
  "timing":   { "ttfb_ms": 210, "dom_ready_ms": 940 },
  "events": [
    { "t": 0,     "type": "pageview" },
    { "t": 1830,  "type": "scroll",  "depth_pct": 25 },
    { "t": 4102,  "type": "click",
                  "sel": "button.promo-apply" },
    { "t": 5340,  "type": "field_focus",
                  "sel": "input#postcode" },
    { "t": 7908,  "type": "field_blur",
                  "sel": "input#postcode",
                  "filled": true },          // not the value
    { "t": 9120,  "type": "rage_click",
                  "sel": "button.continue",
                  "count": 4, "window_ms": 1100 }
  ]
}
```

## Why is it collected?

**Label (purpose pill):** Stated purpose

- **Finding where an interface fails** — a rage-click cluster on a disabled button
- **Locating drop-off** concentrated on one form field, hard to see any other way

**Label (effect pill):** Additional consequence

- The same stream supports **session replay** and behavioural segmentation
- Drives **exit-intent triggers**
- **First-party or third-party collector** decides who else holds the record

**Key point callout:** **Configuration sets the sensitivity, not the technology:** the same tag can be scoped to five named conversion events or left recording every DOM interaction. That is a settings screen, not a different mechanism.

### Visualization (canvas `c3`, 720×320)

Horizontal bar chart on a log scale: one tag, one visit, four scope settings — event count rises by orders of magnitude with no script change.

- **Title (bold 14px `#1a5276`, center):** "One tag, one visit, four settings"; subtitle (12px `#6b7280`): "events recorded from the same three-minute visit — log scale".
- **Data (count, label, color):** 5 "five named conversion events" `#008300`; 34 "plus clicks and scroll milestones" `#2a78d6`; 210 "plus form focus, hover, rage clicks" `#4a3aa7`; 1480 "every DOM interaction, for replay" `#d95926`.
- **Layout:** bars start at x=268, right pad 84, first row at y=68, row height 30, gap 16. Bar width is log10-scaled from log10(4) to log10(2000) across the plot width (minimum 4px). Fill is the row color at 0.32 alpha, stroke solid at width 1.5. Row label right-aligned in bold 12px of the bar color; count (locale-formatted, e.g. "1,480") bold 13px left of the bar end +9px.
- **Left annotation:** vertical dashed gray (`#6b7280`, dash 4/3, width 1.5) line at x=36 (padL−232) spanning the rows, with rotated (−90°) bold 12px gray label "the same script, unchanged" centered beside it.
- **Captions (bottom center):** italic 12px `#2c3e50` "The distance between the top row and the bottom one is a settings screen."; italic 11px `#6b7280` "Illustrative counts — the orders of magnitude are the point, not the figures."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` + bullets/`.key-point` callouts, right `<td>` (55%, `text-align:center`) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre below the canvas, left-aligned).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; li 0.93em with `li b` in `#1a5276` weight 600; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase — `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em `#666` italic. No nav bar, no back/home links.
- **Palette:** charts declare a tokens object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, violet:#4a3aa7, orange:#d95926, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; red `#e74c3c` reserved for genuine alarm states (used only for the "Rage click" event). Site palette anchors: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setupCanvas(id)` helper reads the element's own attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex,a)` translucent fill and `rr()` rounded-rect path.
- No `Math.random()` in charts — all data arrays are hardcoded literals; invented numbers are labeled "illustrative".
