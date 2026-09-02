# Tracking Data: Workplace Input Activity Monitoring

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows)
**HTML title tag:** Tracking Data: Workplace Input Activity Monitoring

**Subtitle:** A monitoring agent on a work computer counts keyboard and mouse events across every application and rolls them into an active-minutes figure per employee.

## What is it?

An endpoint agent counting input events across the whole machine, attributed to a named employee.

- **Not a website script:** browser activity detection sees one page; this agent sees every application on the machine
- **Output is "active minutes":** a running idle clock, reset by any input event and cut at a threshold

**Key point callout:** **The collector is the employer:** the same event counts that elsewhere gate an ad request become, here, a record attached to a named person, read by their management chain.

### Visualization (canvas `c1`, 720×320)

One working day, two readings — a horizontal timeline strip drawn twice: the top strip labels what the person was doing, the bottom strip shows what the agent recorded (active / idle).

- **Title (bold 16px blue, centered):** "One working day, read twice"
- **Timeline:** 9:00 to 16:00, six segments, drawn as two aligned strips (top y=64, bottom y=170, each 40px tall, x from 50 to 690, width proportional to duration):
  1. Writing code — 1.5h — recorded ACTIVE
  2. Reading a design doc — 0.75h — recorded IDLE
  3. Meeting, laptop closed — 1h — recorded IDLE
  4. Whiteboard debugging — 1h — recorded IDLE
  5. Code and email — 1.75h — recorded ACTIVE
  6. Thinking through a design — 1h — recorded IDLE
- **Top strip ("the work"):** all segments one neutral fill `rgba(42,120,214,0.18)` stroked `#2a78d6`; segment labels in 11px alternating above the strip (y=40/54 stagger); row label "the work" left of strip.
- **Bottom strip ("the record"):** ACTIVE segments filled `rgba(25,158,112,0.45)` stroked `#199e70`, IDLE segments filled `rgba(217,89,38,0.30)` stroked `#d95926`; centered 11px bold labels "active"/"idle" inside segments where they fit; row label "the record".
- **Connectors:** dashed vertical grid lines at segment boundaries linking the strips, `#e5e9ef`.
- **Hour ticks:** 9:00 … 16:00 under the bottom strip, 11px `#6b7280`.
- **Caption (centered):** 14px `#2c3e50` "Seven hours of work, 3¼ recorded active — and the idle blocks held the hardest work."; below it italic 11px `#6b7280` "Schematic day — the pattern is the point, not these durations."

## What does it collect?

- **Event counts per interval** — keystrokes and mouse events as counts, often with the foreground application name
- **Active vs idle minutes** — a per-day total, sometimes broken down by application category
- **Not collected: output** — no field records whether anything was produced, decided, or fixed

**Key point callout:** **Idle is a threshold, not an observation:** the agent observes seconds since the last event; "idle" is a cutoff applied to that number. Move the cutoff and the same day produces a different score.

### Visualization (canvas `c2`, 720×320)

Horizontal bar chart: one hour of six different tasks, shown as the active minutes the agent records for each.

- **Title (bold 16px blue, centered):** "An hour of each task, as the agent records it"
- **Bars (label, active minutes out of 60):** Writing code 51; Email and chat 47; Spreadsheet edits 44; Reading a spec 8; Meeting, away from keyboard 0; Whiteboard design 0.
- **Layout:** labels left-aligned at x=30 (13px `#2c3e50`), bars start x=250, full scale (60 min) ends x=650; bar height 24, vertical pitch 38, first row y=56.
- **Colors:** rows with ≥40 min filled `rgba(42,120,214,0.35)` stroked `#2a78d6`; rows ≤8 min filled `rgba(217,89,38,0.35)` stroked `#d95926`; value labels (bold 13px, matching stroke color) at bar end, zero bars get "0" at the axis.
- **Axis:** baseline at x=250 in `#6b7280`; ticks 0/30/60 min along a bottom rule at the last row + 26px.
- **Annotation (right side, 12px `#d95926`, two lines):** "the low rows are not the low-value hours" beside the orange group.
- **Caption (centered, italic 11px `#6b7280`):** "Illustrative minutes — the ordering is by input events produced, not by what the hour was worth."

**Payload note (below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, verbatim):**

```
// Activity heartbeat, one per 60s interval, per employee device.
{
  "device_id":  "wks-4471",
  "user":       "emp_2183",
  "interval_s": 60,

  // ── documented in monitoring-product docs ──
  "keystroke_count":   0,
  "mouse_event_count": 4,
  "foreground_app":    "PDF Reader",
  "idle_seconds":      41,

  // ── inferred / plausible — derived, not measured ──
  "is_active":            false,  // threshold on idle_seconds
  "active_minutes_today": 197,
  "activity_score":       0.62,   // active ÷ scheduled; the column that gets ranked
  "output":               null    // no sensor above measures this
}
```

## Why is it collected?

**Label (purpose pill):** Stated purpose

- **Attendance and billing** — verifying presence for remote hourly or contract work
- **Security housekeeping** — idle screen lock, unused software seats

**Label (effect pill):** Additional consequence

- **Activity becomes productivity** — the score is attributable per person and one export away from a ranking
- **The metric is cheap to feed** — input devices are easy to actuate, and hardware that moves a cursor is an ordinary consumer product; once the count is watched, it measures compliance with the count

**Key point callout:** **The same fallacy in every column:** commit counts, ticket cycle time, hours in front of the screen, active minutes — each is a log of tool activity reused as a measure of output. Activity is what the tool can see; output is what the work was for. The correlation between them is assumed, not measured — and it is weakest for the work that needs the most thought.

### Visualization (canvas `c3`, 720×320)

Two-line chart over ten weeks: the activity score rises after the metric is announced; output does not move.

- **Title (bold 16px blue, centered):** "The metric is announced in week 5"; subtitle 12px `#6b7280` "team average, indexed"
- **Plot area:** x from 70 to 660, y baseline 240, top 60; weeks 1–10 evenly spaced; y-scale 0–100.
- **Series 1 — activity score (%, `#2a78d6`, 2.5px line with 3.5px dots):** [58, 57, 59, 58, 60, 71, 78, 83, 86, 88]
- **Series 2 — output: work items completed, indexed to 100 = week-1 rate (`#199e70`, 2.5px line with 3.5px dots), plotted on the same 0–100 axis:** [48, 52, 46, 50, 48, 50, 46, 48, 52, 48]
- **Announcement marker:** vertical dashed (6/4) `#d95926` line at week 5, bold 12px label "activity score added to reviews" beside it.
- **Legend (top left, 13px):** blue "activity score" / green "output, indexed".
- **Grid:** horizontal lines at y for 0/25/50/75/100 in `#e5e9ef`, tick labels 11px `#6b7280`.
- **Caption (centered):** 14px `#2c3e50` "The score responds to being watched; the output does not."; below it italic 11px `#6b7280` "Illustrative series — the divergence is the point, not these values."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` + bullets/`.key-point` callouts, right `<td>` (55%, `text-align:center`) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre below the canvas, left-aligned).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; li 0.93em with `li b` in `#1a5276` weight 600; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase — `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em `#666` italic. No nav bar, no back/home links.
- **Palette:** charts declare a tokens object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, violet:#4a3aa7, orange:#d95926, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; red is deliberately excluded from the series rotation, reserved for alarm states. Site palette anchors: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setupCanvas(id)` helper reads the element's own attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helper `rr()` draws rounded-rect paths.
- No `Math.random()` in charts — all data arrays are hardcoded literals; invented numbers are labeled "illustrative" or "schematic".
