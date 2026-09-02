# Tracking Data: Badge Access Control

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section; each row carries its own accent hue)
**HTML title tag:** Tracking Data: Badge Access Control

**Subtitle:** A badge reader decides whether to unlock one door. Each decision is logged, and the log of many doors is a movement trail nobody set out to build.

## Section 1: What is it?

**Lede:** A reader answers one question per door, and writes the answer down.

- **The decision:** a controller checks the credential against that door's permission list, and the lock releases or does not
- **The row:** which credential, which reader, what time, granted or denied
- **Reader placement** is a security choice that sets data resolution as a side effect — a turnstile sees everyone once, a lab door sees a subset

**Key point — The trail is a by-product of sorting:** one at a time the rows are door decisions; sorted by credential and timestamp they are a path through the building. No extra collection is involved.

### Visualization (canvas `c1`, 720×320)

Two-lane schematic: five independent door decisions as boxes above, the same five rows sorted by time into a path below.

- **Title (bold 16px ink `#1a5276`, centered at y=22):** "Five door decisions, and the same five rows sorted by time".
- **Data (5 events):** doors `['Garage', 'Lobby', 'Floor 6', 'Lab 2', 'Floor 6']`; times `['08:41', '08:44', '08:47', '11:12', '16:10']`; results `['grant', 'grant', 'grant', 'deny', 'grant']`; zone hues (one per security zone; Floor 6 repeats its hue so the return trip reads): aqua `#199e70`, violet `#4a3aa7`, blue `#2a78d6`, yellow `#c98500`, blue `#2a78d6`.
- **Top lane:** five boxes 108×44 at y=44, 18px gaps, centered as a group; fill = zone hue at 0.22 alpha, 1px zone-hue outline; door name in bold 14px zone hue, result below in 13px — green `#008300` for grant, orange `#d95926` for deny. Caption below (13px muted `#6b7280`, centered): "each row answers one question: open this door now?"
- **Bottom lane:** time axis from x=70 to x=660 at y=178 (gridline `#e5e9ef`); events at fractions `[0.02, 0.09, 0.16, 0.46, 0.96]` of the axis; connecting segments (width 2, arrowheads) each tinted 0.55 alpha in the hue of the zone it arrives at; 4.5px-radius event dots colored by zone (orange for the deny). Labels alternate above/below the axis (staggered to avoid collisions): time in 12px text color, door name in 12px zone hue, with a short tinted connector line.
- **Takeaway (bold 14px magenta `#d55181`, centered at y=240):** "sorted, the same rows are a route through the building". Caption (13px muted, bottom): "Schematic — one credential, one day".

## Section 2: What does it collect?

- **Credential identifier** — the number on the card or in the phone, not a person
- **Reader identifier**, and through it the door and its position
- **Timestamp**, typically to the second
- **Outcome** — granted, or denied with a reason code

**Key point — The raw events are unlock decisions:** `hours_present` is arithmetic laid on top — first event to last, a garage gate to an interior door, with the walk out producing no row at all. It is also the field a report will quote.

### Visualization (canvas `c2`, 720×320)

Paired-bar chart: people through each door (dashed outline) vs rows in the log (filled), with the shortfall labeled per door.

- **Title (bold 16px ink, centered at y=22):** "People through the door, and rows in the log".
- **Data (5 doors, illustrative counts; one hue per door since the shortfall differs by door):**
  - "Lobby turnstile" (violet `#4a3aa7`): people 60, swipes 58, shortfall -2
  - "Floor door (morning)" (blue `#2a78d6`): people 46, swipes 33, shortfall -13
  - "Floor door (midday)" (aqua `#199e70`): people 22, swipes 19, shortfall -3
  - "Garage stairwell" (yellow `#c98500`): people 34, swipes 19, shortfall -15
  - "Loading door" (magenta `#d55181`): people 18, swipes 7, shortfall -11
- **Bars:** plot x 96–690, baseline y=176, scale max 64 over 112px height; per door a pair of 34px-wide bars — arrivals as dashed (3/3) outline-only rect in the door hue, recorded swipes as filled rect (hue at 0.4 alpha with solid outline). Shortfall "-n" in bold 13px orange `#d95926` above the swipe bar. Two-line door labels in 12px door hue below the baseline.
- **Legend (top left, neutral ink since it encodes mark style):** dashed ink box = "people"; filled tinted ink box = "rows"; bold orange "-n" = "shortfall".
- **Takeaway (bold 14px orange, centered at y=222):** "the shortfall is not constant across doors — so it cannot be corrected with one factor". Caption (13px muted, bottom): "Illustrative counts, schematic — shape only".

**Payload note (italic gray, below canvas):** Sample payload — illustrative structure, not real captured data. The last block is what a downstream attendance report adds; the reader never produced it.

**Payload block (monospace, `#f8f9fa` background, left border 3px solid `#1a5276`):**

```
{
  // ── standard access-control event fields ──
  "credential_id": "C-4091…",
  "events": [
    { "reader": "GARAGE-GATE-IN", "at": "08:41:07", "result": "grant" },
    { "reader": "LOBBY-TURNSTILE", "at": "08:44:52", "result": "grant" },
    { "reader": "FL6-NORTH",      "at": "08:47:31", "result": "grant" },
    { "reader": "LAB-2",          "at": "11:12:04", "result": "deny",
      "reason": "not_in_access_group" },
    { "reader": "FL6-NORTH",      "at": "16:10:22", "result": "grant" }
  ],
  // no exit event exists — the lobby door is push-to-open outbound

  // ── inferred / plausible downstream enrichment ──
  "employee_id": "E-8831",     // credential→person mapping, may be stale
  "first_seen": "08:41:07",
  "last_seen":  "16:10:22",
  "hours_present": 7.49,       // last_seen − first_seen
  "in_office_day": true,
  "deny_count": 1              // cause not partitioned
}
```

## Section 3: Why is it collected?

**Label (STATED PURPOSE, blue pill):**

- **Physical security** — a door locked to the people who should not go through it
- **Revocation** of a lost card, **alarms** on a forced door, and some idea who is inside during a fire

**Label (ADDITIONAL CONSEQUENCE, orange pill):**

- The same rows, **grouped by credential and day, look like a timesheet**
- Deriving hours **costs one query** — no new hardware, no notice

**Key point — Reporting the metric changes it:** while the log is only a security record nobody swipes strategically. Once presence reaches a manager, the swipe measures compliance with the metric — an extra tap, an interior door touched late. And readers were sited to protect areas, not to mark a workday, so with free exit the second swipe a duration needs often does not exist.

### Visualization (canvas `c3`, 720×320)

Gantt-style comparison: six people's actual hours in the building (gray band) vs the span the swipe log can bracket (blue bar), with individual swipes as tick marks.

- **Title (bold 13px ink, centered at y=24):** "Hours in the building, and the hours the swipe log can bracket". Subtitle (12px muted at y=42): "one day, six people".
- **Time scale:** 7.5 to 19.5 hours mapped across the plot (left pad 178, right pad 116); hour gridlines `#e5e9ef` with 11px muted labels at 8:00, 10:00, 12:00, 14:00, 16:00, 18:00.
- **Rows (27px pitch from y=62; row label 12px text color right-aligned; gray presence band `rgba(107,114,128,0.22)` from t0 to t1; log-bracket bar from first to last swipe in `rgba(42,120,214,0.55)`, or `rgba(217,89,38,0.55)` when only one swipe exists; each swipe a 1.5px ink vertical tick; right-side annotation bold 12px, blue "X.X of Y.Y h" or orange "— of Y.Y h" when no duration):**
  - "entered, swiped out": present 8.6–17.4, swipes at 8.6, 12.1, 13.0, 17.4 → 8.8 of 8.8 h
  - "left through a free door": present 8.2–17.9, swipes at 8.2, 11.5 → 3.3 of 9.7 h
  - "tailgated in behind": present 9.1–16.8, one swipe at 13.4 → — of 7.7 h
  - "stayed at one desk": present 9.4–18.2, one swipe at 9.4 → — of 8.8 h
  - "moved between floors": present 8.0–16.2, swipes at 8.0, 10.3, 14.7, 16.0 → 8.0 of 8.2 h
  - "in for a short meeting": present 13.2–14.6, swipes at 13.2, 14.5 → 1.3 of 1.4 h
- **Legend (12px, below the grid):** gray swatch "in the building"; blue swatch "first swipe to last swipe"; orange swatch "one swipe only — no duration".
- **Captions (italic, centered at bottom):** 12px text color: "Readers were sited to protect areas, so the pair a duration needs is often not there."; 11px muted: "Illustrative hours — the gaps are the point, not these times."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` block below the canvas, left-aligned).
- **Per-row accent hues (this page's variation):** the grid is neutral (`1px solid #e5e9ef` cell borders) and each `<tr>` sets `--accent` — row 1 `#2a78d6`, row 2 `#199e70`, row 3 `#4a3aa7`, (row 4 `#d95926` defined). The accent drives: left `<td>` `border-left: 4px solid var(--accent)`, `.obj-title` color, `.key-point` left border (4px) and leading `<strong>` color, and `li b` color.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; li 0.93em.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `4px solid var(--accent)`, padding 10px 14px, 0.93em.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above it.
- **Canvas:** intrinsic 720×320 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts use a shared `tint(hex, alpha)` rgba helper and a rounded-rect path helper.
- **Palette:** charts use the validated categorical token palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Page chrome uses primary blue `#1a5276` (site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange).
- No nav bar, no back/home links. In regenerated HTML any card links use `.html` extensions.
