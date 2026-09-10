# Tracking Data: Robotics & Warehouse Tracking

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Robotics &amp; Warehouse Tracking

**Subtitle:** Handheld scanners and automated storage systems timestamp every task in a fulfilment centre. The measured quantity is the gap between scans — and a gap is not the same thing as idleness.

## Section 1: What is it?

**Lede:** A timestamp per barcode scan, and arithmetic on the gaps.

- **Mechanism:** the warehouse system assigns tasks and stamps each scan of an item, bin, tote or station
- **Machine scans too:** goods-to-person systems add their own timestamps as a robot delivers or retrieves a shelf
- **Everything else is derived:** rate metrics, "time off task" and threshold flags are differences between timestamps

**Key point — One primitive:** the interval between two scans. It is precise, cheap and continuous, which is exactly why it ends up standing in for effort, productivity and compliance — all inferred from it rather than observed.

### Visualization (canvas `c1`, 720×320)

Schematic diagram: warehouse layout with a robot and a worker, showing that the record is scans, and the gap is derived.

- **Color roles:** machine-generated events aqua `#199e70` (BOT), worker scan events blue `#2a78d6` (SCAN), the derived interval orange `#d95926` (DERIVED). Ink `#1a5276`, muted text `#6b7280`. Translucent fills via a `tint(hex, alpha)` helper producing rgba from the hex.
- **Shelves backdrop:** 3 rows × 6 columns of rectangles 80×20 at x = 60 + col×105, y = 30 + row×65, filled with ink at 0.15 alpha (structural backdrop, not a data category).
- **Robot:** aqua square 40×30 at (150,110) labeled "BOT" in bold 12px white centered at (170,129); a sensor cone triangle from (170,110) to (140,60) to (200,60) filled aqua at 0.2 alpha; label "machine timestamps" in 10px aqua centered at (170,156).
- **Worker:** stick figure in blue, line width 2.5 — head circle radius 10 at (400,95), body (400,105)–(400,135), arms (400,115)–(385,128) and (400,115)–(415,128), legs (400,135)–(388,155) and (400,135)–(412,155).
- **Scan link lines:** two dashed (3/3) lines in blue at 0.5 alpha from (190,120) to (390,100) and from (190,130) to (390,135). Two recorded scan events as blue dots radius 4 at (240,114) and (340,104), each labeled "scan" in 10px centered above (at (240,106) and (340,96)).
- **Labels (left-aligned from x=250):** bold 14px blue "The record is a sequence of scans at known stations." at y=195; 12px muted "Everything else — rate, "time off task", flags — is arithmetic on the gaps between them." at y=212.
- **Gap indicator:** bold 13px orange "gap since last scan" at (385,170); 11px muted "cause not recorded" at (385,183).

## Section 2: What does it collect?

- **Task timestamps** — assignment, first scan, completion
- **Interval between scans** — the "time off task" primitive
- **Station and zone ids**, giving position at scan time
- **Units per interval**, from which a rate is computed
- **Mis-picks and exceptions** against the task
- **Rate trajectory** across a shift and across shifts
- **Threshold flags** — derived fields, not sensor readings

**Key point — Only the timestamps are measured:** `flagged_idle` is arithmetic on a threshold management chose, so the same 412-second gap is a violation under one setting and normal under another. The metric encodes a policy, not an observation.

**Key point — The gap only means "not scanning":** restocking a tote, helping a colleague, waiting on a jammed conveyor or a bathroom break all look identical, because `reason_code` is empty. A model trained on `flagged_idle` learns the policy, not productivity.

### Visualization (canvas `c2`, 720×320)

Schematic: one gap between scans mapped from many indistinguishable causes.

- **Color roles:** measured scan events blue `#2a78d6` (SCAN), inferred interval orange `#d95926` (GAP), candidate causes not in the record violet `#4a3aa7` (CAUSE), stored policy setting aqua `#199e70` (POLICY).
- **Title (bold 14px ink, left at (20,18)):** "ONE GAP BETWEEN SCANS — SCHEMATIC".
- **Timeline:** ink line width 2 at y=56 from x=40 to x=680. Two blue dots radius 6 at x=170 and x=520 on the line, labeled in bold 12px centered above: "scan" and "next scan".
- **Gap band:** rectangle from x=170 to x=520, 12px tall centered on the timeline, filled orange at 0.18 alpha, dashed (4/3) orange stroke width 1.5. Below it, bold 12px orange centered at midpoint, y=80: "the measured quantity: elapsed time".
- **Candidate causes:** seven violet-tinted boxes (300×19, fill violet 0.10 alpha, stroke violet 0.45 alpha) in two columns (first four at x=60, remaining three at x=390), rows at y = 104 + row×22. Text in 12px violet: "jammed conveyor", "tote needed restocking", "helping a colleague", "mislabelled item to resolve", "scanner fault", "safety stop", "a break". Each box has "→ same row" in 11px orange at its right edge.
- **Column footnote:** 11px violet at (60,186): "candidate causes — none of these is a field in the record".
- **Derived-flag box:** rectangle 600×48 at (60,196), fill aqua at 0.07 alpha, aqua stroke width 1.5. Inside: bold 12px aqua "The stored fields are: gap length, and a threshold chosen by management." at (70,211); then 12px text color `#2c3e50`, two lines: "The reason field is empty, so the flag is computed on a quantity that is genuinely" / "ambiguous. Same gap, different setting, different verdict." at (70,225) and (70,239).

**Payload note (italic gray, below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, `#f8f9fa` background, left border 3px solid `#1a5276`):**

```
// Warehouse management systems do not publish their
// worker-activity schema. Field names are reconstruction.
{
  // ── inferred / plausible ──
  "worker_id":       "wk_4821",
  "shift_id":        "2026-08-22-A",
  "station":         "pick-mod-7",
  "task_id":         "t_9f10…",
  "assigned_ts":     "2026-08-22T09:14:02Z",
  "first_scan_ts":   "2026-08-22T09:14:19Z",
  "completed_ts":    "2026-08-22T09:14:47Z",
  "units":           "seconds",
  "gap_since_prev_scan_s": 412,
  "idle_threshold_s":      300,   // policy setting
  "flagged_idle":    true,        // derived from the two above
  "reason_code":     null         // nothing recorded
}
```

## Section 3: Why is it collected?

**Label (STATED PURPOSE, blue pill):**

- **The timestamps are the control system** — where inventory is, which tasks are outstanding, where a queue is building
- **Routing and capacity planning** — pick paths, shift sizing, same-day promises all follow from measured throughput

**Label (ADDITIONAL CONSEQUENCE, orange pill):**

- The record is already **attributed to an individual**, so it supports per-worker performance management
- **Ranking follows for free** — comparable across people, which is what makes it attractive as an evaluation input and what makes it a poor one

**Key point — The unit is a task, not a person:** a scan is written where the operation needed a task boundary marked, so the record samples checkpoints rather than working time. Read as a productivity score it answers a question it was never built to answer. Whether any employer acts on such a score is not established here.

### Visualization (canvas `c3`, 720×320)

Bar chart schematic: the same gap sequence flagged differently under two threshold settings.

- **Color roles:** over both thresholds orange `#d95926` (BOTH), over only the low setting violet `#4a3aa7` (ONLY_LOW), under both blue `#2a78d6` at 0.35 alpha (NEITHER). Each threshold line takes the hue of the band it creates.
- **Title (bold 14px ink at (20,18)):** "SAME SHIFT, TWO THRESHOLD SETTINGS — SCHEMATIC". Subtitle 11px muted at (20,32): "Illustrative gap lengths. Nothing about the work changes between the two panels."
- **Axes:** ink lines width 2, vertical from (70,44) to (70,178), horizontal to (660,178). Rotated y-label (12px ink, centered, at translate(28,112)): "gap between scans". X-label 12px ink centered at (365,198): "consecutive tasks across one shift →".
- **Data (24 bars, illustrative gap lengths as fractions of plot height 0..1):** `[0.18, 0.22, 0.14, 0.61, 0.19, 0.25, 0.17, 0.48, 0.21, 0.16, 0.72, 0.20, 0.24, 0.15, 0.44, 0.19, 0.28, 0.55, 0.17, 0.23, 0.13, 0.66, 0.21, 0.18]`. Plot spans y 44–178; bar width 17, gap 7, starting at x=80; bars rise from the baseline y=178.
- **Thresholds:** strict at 0.40 and loose at 0.58 of plot height. Bar color: ≥0.58 orange; ≥0.40 violet; else tinted blue.
- **Threshold lines:** dashed (6/4), width 2, drawn from x=70 to x=660 at each threshold's y, labeled in bold 11px in the matching color just above the line: "threshold set high — fewer flags" (orange, loose) and "threshold set low — more flags" (violet, strict).
- **Legend (three swatches 11×10 at y=208, x = 70 + i×210, muted 0.8px outline, 11px labels in text color):** "flagged under either setting" (orange), "flagged only under the low setting" (violet), "not flagged" (tinted blue).
- **Takeaway (bold 12px violet at (70,234)):** "The middle band is the whole problem: the same worker, the same gaps, a different verdict."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above it.
- **Canvas:** intrinsic 720×320 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts use a shared `tint(hex, alpha)` rgba helper and a rounded-rect path helper.
- **Palette:** charts use the validated categorical token palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Page chrome uses primary blue `#1a5276` (site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange).
- No nav bar, no back/home links. In regenerated HTML any card links use `.html` extensions.
