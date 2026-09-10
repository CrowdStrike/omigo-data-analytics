# Tracking Data: Mouse & Keyboard Activity Detection

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows)
**HTML title tag:** Tracking Data: Mouse & Keyboard Activity Detection

**Subtitle:** Browsers dispatch mousemove, keydown, and scroll events to any script on the page, and expose tab visibility and window focus through standard APIs. Counting those events over an interval yields a presence signal.

## What is it?

Counting DOM input events over an interval to infer presence.

- **No permission prompt:** `mousemove`, `keydown`, `scroll` and `click` are how interactive pages work
- **Usually summarised, not stored raw** — counts in a fixed five- or ten-second interval
- **Visibility:** `document.visibilityState` — is the tab foregrounded
- **Focus:** `document.hasFocus()` — does the window hold focus
- **Idle duration** from a time-since-last-event counter
- **Common uses:** starting a video, deferring an ad request, extending or expiring a session. Vendor rules are rarely documented, so treat any product's rule as unverified

**Key point callout:** **Visibility and focus are different questions:** a visible tab can sit behind another window, and a focused window can hold a background tab. Logic built on one of them misclassifies a predictable slice of sessions.

### Visualization (canvas `c1`, 720×320)

Schematic illustration: a simplified webpage wireframe with a dotted cursor trail, click points, and highlighted hover zones.

- **Background:** full-canvas `#f0f4f8`; header bar `#dce6f0` at (20,15) width−40 ×30 with 13px `#2a78d6` text "Navigation Bar"; content blocks in `#e8eef4` at (20,55) 200×80, (240,55) 240×80, (500,55) 200×80, (20,145) 320×75, (360,145) 340×75.
- **Hover zones:** orange highlights — (240,55) 240×80 fill `rgba(230,126,34,0.25)` and (360,145) 340×75 fill `rgba(230,126,34,0.2)`, both stroked `#d95926` width 2; orange 12px labels "hover 2.1s" at (245,140) and "hover 4.8s" at (365,225).
- **Cursor trail:** 13 points from (80,30) descending diagonally to (560,190) — `(80,30),(150,45),(220,60),(280,75),(340,90),(370,100),(390,115),(420,130),(460,150),(500,165),(530,175),(550,185),(560,190)` — connected by a dashed (4/4) line in `rgba(26,82,118,0.5)` with 2px-radius dots in `rgba(26,82,118,0.4)`.
- **Click points:** magenta `#d55181` dots radius 6 with white 2.5px centers at (280,75), (460,150), (550,185); magenta 12px "click" labels beside each.
- **Cursor icon:** blue `#2a78d6` filled arrow-pointer polygon at (560,190).

## What does it collect?

- **Event counts per interval** — mousemove, keydown, scroll, click
- **Cursor coordinates**, where the raw stream is retained
- **Not collected: gaze** — cursor position is a weak stand-in for where the eyes point; measuring that directly is eye tracking
- **Tab visibility** state and window focus
- **Time since the last input event**
- **Inter-keystroke intervals**, if timestamps are kept
- **Precision:** `mousemove` usually arrives about once per display frame, with in-between points coalesced, so scripts see a sampled trail — and browsers deliberately coarsen event timestamps, which caps keystroke-timing resolution

**Key point callout:** **One constant under a family of metrics:** `is_active` is a threshold on `idle_seconds`, and `viewable` and `attention_ms` are functions of `is_active`.

**Key point callout:** **The threshold should be reported with the metric:** moving idle from 30 seconds to 60 moves engaged-session counts, with no change in behaviour to explain it.

### Visualization (canvas `c2`, 720×320)

Event-stream timeline: labeled micro-events with boxed monospace labels alternating above/below the axis.

- **Background:** full-canvas `#f8fafb`. Title (bold 13px `#2a78d6`): "Event Stream (one second of browsing)".
- **Axis:** horizontal `#e5e9ef` line at y=120 from x=30 to width−20, arrowhead at right, 12px `#7f8c8d` label "time".
- **Events (x, label, color, above/below):** 60 "move(342,198)" `#2a78d6` above; 140 "move(355,210)" `#2a78d6` below; 220 "click" `#d55181` above; 300 "hover 3.2s" `#d95926` below; 390 "scroll +140px" `#008300` above; 480 "keydown 45ms" `#2a78d6` below; 560 "idle 8s" `#d55181` above; 640 "focus lost" `#d95926` below.
- **Marks:** each event gets a colored tick (±8px) and dot (radius 4) on the axis, a dashed (2/2) connector to ±40px, and its label in 13px monospace inside a white box stroked in the event color.

**Payload note (below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, verbatim):**

```
// Presence heartbeat, one per 10s interval.
{
  "session_id":  "s_9f13…",
  "interval_ms": 10000,

  // ── documented / standard ── DOM event counts
  "mousemove_count": 0,
  "keydown_count":   0,
  "scroll_count":    1,
  "click_count":     0,

  // Page Visibility API / document.hasFocus()
  "visibility_state": "visible",
  "has_focus":        true,

  // ── inferred / plausible ──
  "idle_seconds":  46,     // since last input event
  "is_active":     false,  // threshold on the above
  "attention_ms":  0,      // credited to this interval
  "viewable":      false   // gates ad measurement
}
```

## Why is it collected?

**Label (purpose pill):** Stated purpose

- **Battery and bandwidth** — pausing polling when nobody is there, and expiring an idle login
- **Ad slot in view** — holding a request until the slot is on screen, and not billing an impression rendered in a hidden tab

**Label (effect pill):** Additional consequence

- **Weak bot signal** — automated clients often produce no input events, or implausibly regular ones
- **Behavioural biometric** — pointer speed and gaps between keystrokes have been studied as one; retained per session, another attribute available for linking

**Key point callout:** **The threshold was set for a cheap decision, then reused as a definition:** the idle cutoff was chosen so a wrong call costs one deferred request, which the next event corrects. Carried into a reported metric, that same cutoff becomes the working definition of attention, and it was never calibrated against reading.

### Visualization (canvas `c3`, 720×320)

Histogram with two cutoff lines: the same 100 sessions counted twice — the idle cutoff decides the total.

- **Title (bold 14px `#1a5276`, left-aligned):** "100 sessions, binned by their longest pause between events"; subtitle (12px `#6b7280`): "Nothing about the browsing changes across this chart."
- **Data:** 9 bins of 15 seconds each (last bin "120s +"), counts `[12, 19, 16, 15, 9, 8, 6, 5, 10]` summing to 100.
- **Bars:** plot from x=62 (padL) to width−70, baseline y=208, max height 118 scaled to max count 19; fill `rgba(42,120,214,0.32)`, stroke `#2a78d6` width 1, bars 68% of slot width.
- **Cutoff regions:** area left of 30s shaded `rgba(25,158,112,0.10)` (aqua) and between 30s and 60s shaded `rgba(217,89,38,0.09)` (orange).
- **Cutoff lines:** vertical dashed (6/4, width 2) lines at 30s in `#199e70` labeled "cutoff 30s" and at 60s in `#d95926` labeled "cutoff 60s" (bold 12px above the line).
- **Axes:** ink `#1a5276` baseline; x ticks at 0s, 30s, 60s, 90s, "120s +" in 12px `#6b7280`; x-axis title "longest pause in the session"; y ticks 0, 10, 20 with label "sessions".
- **Counts (bold 13px, left-aligned at x=420):** aqua "cutoff 30s → 31 of 100 called engaged" (y=84); orange "cutoff 60s → 62 of 100 called engaged" (y=106).
- **Captions (bottom center):** italic 12px `#2c3e50` "One edit to a setting doubles the reported figure, with no change in behaviour to explain it."; italic 11px `#6b7280` "Illustrative distribution — the shape carries the point, not the counts."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` + bullets/`.key-point` callouts, right `<td>` (55%, `text-align:center`) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre below the canvas, left-aligned).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; li 0.93em with `li b` in `#1a5276` weight 600; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase — `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em `#666` italic. No nav bar, no back/home links.
- **Palette:** charts declare a tokens object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, violet:#4a3aa7, orange:#d95926, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; red is deliberately excluded from the series rotation, reserved for alarm states. Site palette anchors: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setupCanvas(id)` helper reads the element's own attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helper `rr()` draws rounded-rect paths.
- No `Math.random()` in charts — all data arrays are hardcoded literals; invented numbers are labeled "illustrative".
