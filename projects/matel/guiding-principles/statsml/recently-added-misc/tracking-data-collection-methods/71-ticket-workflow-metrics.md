# Tracking Data: Ticket Workflow Metrics

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Ticket Workflow Metrics

**Subtitle:** A ticket system records when someone moved a card. Every duration derived from it is the gap between two clicks, not the span of the work.

## Section 1: What is it?

**Lede:** A state machine over records, and a log of who moved what.

- **Statuses:** a ticket sits in To Do, In Progress, In Review or Done
- **Each change appends a row** — old status, new status, timestamp, who did it
- **Every board** built on this idea works the same way, Jira-style projects included
- **All the metrics are arithmetic on that list** — cycle time, lead time, time-in-status, throughput, velocity, cumulative flow. Nothing else is available

**Key point — The tracker observes clicks about work, not work:** cycle time is the interval between two manual state changes, so its accuracy depends on whether people update tickets promptly.

**Key point — The update error is not random:** tickets are commonly moved to In Progress late and to Done in a batch before a sprint close or status meeting. Team ritual sets the bias direction, not the work.

**Key point — The measurement rewards sloppiness:** a diligent team records longer durations than a careless one doing identical work, because careless updating compresses the recorded interval.

### Visualization (canvas `c1`, 720×320)

Two-track timeline: actual work sessions vs the recorded status transitions.

- **Title (bold 16px blue `#2a78d6`, centered at y=22):** "What happened, and what the tracker stored".
- **Timeline span:** x from 130 to 700; positions given as fractions of that span.
- **Track A ("Work done", 14px aqua `#199e70` label right-aligned at left, baseline gridline `#e5e9ef` at y=64):** five work-session blocks 24px tall at y=52, aqua fill at 0.35 alpha with aqua outline, spanning fractions `[0.02–0.09], [0.16–0.21], [0.30–0.37], [0.49–0.58], [0.74–0.90]`.
- **Track B ("Recorded", 14px violet `#4a3aa7` label, gridline at y=130):** four transition tick marks (2px vertical line ±10px plus 4px-radius dot) — one yellow `#c98500` at fraction 0.24 (the late In Progress click), three magenta `#d55181` at 0.925, 0.945, 0.965 (the sprint-close batch).
- **Mark labels:** 13px yellow centered above the first mark: "In Progress set late"; right-aligned magenta at x=700: "In Review / Testing / Done"; muted below it: "all within seconds of each other".
- **Bracket:** violet bracket (width 2) under track B from fraction 0.24 to 0.965 at y=164, with bold 15px violet centered label: "recorded cycle time = gap between two clicks".
- **Caption (13px muted, centered at bottom):** "Illustrative — the tracker stores the marks, never the blocks above."

## Section 2: What does it collect?

- **Every status transition** — from, to, timestamp, and the account that made it
- **Story point estimate**, and its edit history
- **Sprint membership**, and whether the ticket was carried over
- **Assignee**, and each reassignment
- **Comment, attachment and field-edit** timestamps
- **Links between tickets** — blocks, duplicates, parent

**Key point — Story points are ordinal, not cardinal:** a 5 is not five 1s and not necessarily 2.5 twos, so summing them into velocity is a category error.

**Key point — Velocity works approximately for the wrong reason:** averaged over many tickets the sum is dominated by count — a ticket counter wearing an estimate's clothes.

**Key point — Points are not comparable across teams:** each team calibrates its own scale privately against its own past work, so there is no shared unit. Cross-team velocity comparison has no defined meaning even when both numbers come from the same tool.

### Visualization (canvas `c2`, 720×320)

Horizontal range chart: five point labels, each with an overlapping range of actual effort in hours.

- **Title (bold 16px blue, centered at y=22):** "Same point label, overlapping actual effort".
- **Scale:** x from 120 to 690 mapping 0–100 hours.
- **Rows (20px-tall bars, 30px row pitch starting y=44; each row uses the next series hue — blue `#2a78d6`, green `#008300`, violet `#4a3aa7`, orange `#d95926`, aqua `#199e70`; fill at 0.32 alpha with solid outline; bold 15px right-aligned label "N pt" in the row color):**
  - 1 pt: 1–9 h
  - 2 pt: 3–16 h
  - 3 pt: 5–30 h
  - 5 pt: 9–58 h
  - 8 pt: 18–94 h
- **Axis:** muted horizontal line below the rows with 5px ticks and 13px labels at 0h, 25h, 50h, 75h, 100h.
- **Takeaway (bold 15px blue, centered):** "a 5 is bigger than a 1 — it is not five of them". Caption (13px muted, bottom): "Schematic".

**Payload note (italic gray, below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, `#f8f9fa` background, left border 3px solid `#1a5276`):**

```
{
  // ── recorded in the tracker ──
  "key": "PLAT-4127",
  "assignee": "u_8831",
  "story_points": 5,          // ordinal label, not hours
  "sprint": "Sprint 34",
  "changelog": [
    { "to": "In Progress", "at": "2026-08-11T14:02:10Z", "by": "u_8831" },
    { "to": "In Review",   "at": "2026-08-20T16:58:41Z", "by": "u_8831" },
    { "to": "Testing",     "at": "2026-08-20T16:58:47Z", "by": "u_8831" },
    { "to": "Done",        "at": "2026-08-20T16:58:52Z", "by": "u_8831" }
  ],
  // the last three transitions land seconds apart — a batch update at
  // sprint close, so time_in_status for In Review and Testing is near
  // zero and says nothing about review or testing.

  // ── inferred / plausible ──
  "cycle_time_hours": 218.9,  // In Progress → Done
  "lead_time_hours":  402.4,  // created → Done
  "time_in_status": { "In Progress": 218.9, "In Review": 0.002 },
  "blocked_hours_est": 0,     // no Blocked transition was ever logged
  "throughput_contribution": 1
}
```

## Section 3: Why is it collected?

**Label (STATED PURPOSE, blue pill):**

- **Coordination** — who is on what, making work visible, finding where tickets queue
- **Team-level forecasting**, which the transition rows genuinely support

**Label (ADDITIONAL CONSEQUENCE, orange pill):**

- Every transition carries an actor, so the record **repivots into per-person throughput or cycle time** with one group-by
- That is where validity fails: the record was built for coordination, and the **estimate came from the person being measured**

**Key point — The instrument is held by the people it measures:** once velocity is a target, the same work gets pointed higher and recorded velocity rises with no change in output. That is a feedback loop, not noise — a pressure sensor does not re-calibrate itself to look good; an estimate does.

### Visualization (canvas `c3`, 720×320)

Dual-axis line chart: recorded velocity rises across eight sprints while tickets shipped stays flat.

- **Title (bold 14px ink `#1a5276`, centered at y=24):** "Eight sprints, five tickets shipped in each one". Subtitle (12px muted at y=42): "two axes, because points and tickets are different units — only the shapes compare".
- **Plot:** x from 80 to 600, baseline y=226, top y=76; L-shaped ink axes.
- **Left scale (points, labels in magenta `#d55181`):** 0, 10, 20, 30, 40 with light gridlines `#e5e9ef`; max 40. **Right scale (tickets, labels in aqua `#199e70`):** 0, 5, 10; max 10.
- **Data (8 sprints):** recorded velocity (magenta line, width 2.5, 3.5px dots): `[16, 17, 16, 18, 22, 27, 31, 36]`; tickets delivered (aqua): `[5, 5, 5, 5, 5, 5, 5, 5]`.
- **Target marker:** dashed (5/4) orange `#d95926` vertical line at sprint index 4 (S5), labeled bold 12px orange above: "velocity becomes a target".
- **Series end labels (bold 12px, left of right margin at x=618):** magenta "points recorded", aqua "tickets shipped".
- **X labels:** S1–S8 in 12px muted under each point; "sprints  →" centered below.
- **Captions (italic, centered at bottom):** 12px text color: "The same work is pointed higher, so the measure rises while output holds flat."; 11px muted: "Illustrative — the numbers show the divergence, not a measured team."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above it.
- **Canvas:** intrinsic 720×320 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts use a shared `tint(hex, alpha)` rgba helper and a rounded-rect path helper.
- **Palette:** charts use the validated categorical token palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Page chrome uses primary blue `#1a5276` (site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange).
- No nav bar, no back/home links. In regenerated HTML any card links use `.html` extensions.
