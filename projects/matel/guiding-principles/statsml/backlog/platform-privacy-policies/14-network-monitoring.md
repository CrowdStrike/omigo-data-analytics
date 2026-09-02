# Network Monitoring

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Network Monitoring

**Subtitle:** Who talks to whom, when, and how much — full traffic metadata for everyone on the network, including guests and personal phones on the office wifi.

**Disclaimer (orange callout):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared (the security purpose):** connection records — source, destination, port, time, bytes — for every device; alerts on known-bad destinations.
- **Incidental (swept up alongside):** every DNS query, which names every service anyone uses; personal phones and guest devices on the corporate wifi, monitored identically; timing and volume patterns that reveal work hours, breaks, and habits.
- **Inferred:** a behavioral baseline per user/device pair, a who-talks-to-whom communication graph, anomaly scores for "unusual" traffic.

**Key point (blue-left-border box):** Most surprising to the employee: joining the office wifi with a **personal phone** puts it in the same monitoring stream as corporate servers — every app it contacts becomes a logged, timestamped record.

### Visualization (canvas `c1`, 720×440)

Grouped horizontal bar chart: assumed vs realistic extent of collection, per data category.

- **Title (bold 13px `#1a5276`, top center):** "What employees assume is collected vs realistic extent (illustrative)".
- **Legend (top, at x=180 and x=280):** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent". Labels in `#2c3e50` 11px.
- **Rows (label, assumed a, realistic b — values on 0–100 scale):**
  - Blocked threats: a=85, b=95
  - Every connection, logged: a=30, b=95
  - Every DNS query: a=12, b=95
  - Timing / volume per user: a=20, b=90
  - Personal devices on wifi: a=10, b=85
  - Guest-network devices: a=8, b=75
  - Months of retro history: a=10, b=85
  - Behavioral anomaly profile: a=5, b=70
- **Geometry:** right-aligned labels at x=190 in `#2c3e50` 12px, bars start at x=202, max bar width 480px, bar height 13px, inner gap 3px, group gap 13px, start y=54. Assumed bar on top (`rgba(26,82,118,0.35)`), realistic bar below (`rgba(231,76,60,0.55)`). Numeric value in 10px just past each bar end: assumed value in `#1a5276`, realistic value in `#e74c3c`.
- **Caption (bottom center, `#999` 11px):** "Numbers are illustrative, not measured — the point is the gap, not the values."

## How it gets used

- **Threat detection:** flagging connections to known-bad or never-seen destinations.
- **Incident investigation:** long-lived flow logs let analysts retroactively reconstruct months of one person's network activity.
- **Behavioral baselining / insider risk:** anomaly models profile each user/device pair and score deviations.
- **Vendor cloud analysis and model training:** traffic metadata feeds the vendor's cross-customer detection models.
- **HR / legal escalation:** the same flow records can back misconduct and data-exfiltration cases.

### Visualization (canvas `c2`, 720×340)

Hub-and-spoke flow diagram: four data-category boxes on the left feed one central hub, which fans out to five use boxes on the right.

- **Title (bold 13px `#1a5276`, top center):** "From traffic metadata to five uses".
- **Left boxes (160×32 at x=25, all `#1a5276` — 0.12 alpha fill, 1.5px stroke, bold 11px `#1a5276` label):** Connection records (y=60), DNS queries (y=125), Volume / timing (y=190), Device inventory (y=255).
- **Central hub (130×52 box at x=290, centered on y=158, `#1a5276` at 0.18 alpha fill, 2px stroke):** two-line bold 12px label "Flow log" / "per user / device".
- **Right boxes (210×30 at x=487, each with 0.12 alpha fill, 1.5px stroke, bold 11px label in its color):**
  - Threat detection (y=50, `#27ae60`)
  - Retroactive reconstruction (y=108, `#2980b9`)
  - Insider-risk scoring (y=166, `#e67e22`)
  - Vendor cloud + model training (y=224, `#8e44ad`)
  - HR / legal escalation (y=282, `#e74c3c`)
- **Arrows:** straight `#bbb` 1.5px lines from each left box into the hub, and from the hub out to each right box with a small filled arrowhead.

## How long it's kept

- **Hot storage:** raw flow records searchable for roughly 30–90 days.
- **Archive:** compressed flow and DNS logs commonly kept a year or longer — metadata is cheap to store.
- **Incident data:** captures and logs attached to a case are kept indefinitely.
- **Vendor-side copies:** retention at the vendor runs on its own schedule, separate from the employer's.
- **Legal hold:** overrides all deletion schedules.
- **Retention outlives employment** — and outlives a guest's single visit.
- **Identifiable vs de-identified:** flow data the vendor de-identifies for threat intelligence and model training outlives the employer's retention window. The catch: user and device identifiers are typically only pseudonymized, not removed — the traffic pattern stays linkable to the person.

### Visualization (canvas `c3`, 720×330)

Horizontal retention-timeline bars over a gray track, with an "employee leaves company" dashed marker.

- **Title (bold 13px `#1a5276`, top center):** "How long each copy lives (illustrative)".
- **Geometry:** right-aligned labels at x=200 in `#2c3e50` 12px; bars start at x0=212, track width 460px filled `#f0f0f0`; colored fill at 0.55 alpha covering `frac` of the track; bar height 20px, gap 22px, start y=52.
- **Rows (label, fraction of track, color):**
  - Raw flow records (hot): 0.12, `#27ae60`
  - DNS + flow archive: 0.60, `#2980b9`
  - Vendor cloud copies: 0.68, `#8e44ad`
  - Incident captures / logs: 1.0, `#e74c3c`, with filled arrowhead past track end and bold 10px "indefinite" label
  - Anything under legal hold: 1.0, `#e67e22`, with filled arrowhead past track end and bold 10px "indefinite" label
- **Marker:** vertical dashed red line (`#e74c3c`, dash 5/4, width 2) at 40% of the track, labeled below in bold red 11px: "employee leaves company".
- **Axis labels (`#666` 11px):** "day 0" at track start, "years →" at track end.

## What you get back

- The people being monitored — employees, contractors, guests — have **no export path at all**; many never know the monitoring exists.
- Access requests route through the employer, which owns the data relationship with the vendor.
- Even the employer typically receives **dashboards and alerts**, not the raw flow archive the vendor holds.

**Key point (blue-left-border box):** The asymmetry vs consumer platforms: a consumer app must offer "download your data"; a network monitor observing the same person's phone on office wifi offers them **nothing** — they are the subject, never the customer.

### Visualization (canvas `c4`, 720×340)

Two side-by-side panels: a nearly empty green panel vs a red panel with a long bulleted list.

- **Title (bold 13px `#1a5276`, top center):** "What the employee can retrieve vs what exists about them".
- **Left panel (310×270 at x=30, y=40; green `#27ae60` at 0.08 alpha fill, 2px stroke):** bold 13px green title "What the employee can retrieve"; centered italic 12px `#2c3e50` "(nearly nothing)"; then 11px lines "No account. No portal. No export." and "Guests may never know it exists."
- **Right panel (310×270 at x=380, y=40; red `#e74c3c` at 0.08 alpha fill, 2px stroke):** bold 13px red title "What exists about them"; bulleted items (small red square bullets, `#2c3e50` 11px, 24px spacing):
  - Every connection, timestamped
  - Every DNS query — every service used
  - Traffic volume and timing patterns
  - Personal phone activity on office wifi
  - A who-talks-to-whom graph
  - Months of replayable flow history
  - A per-device behavioral baseline
  - Anomaly scores for "unusual" days
  - Copies in the vendor's cloud

## Regeneration instructions

- **Layout:** detail page with `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` box, right `<td>` (55%, `text-align: center`) holds the canvas. Table cell borders `1px solid #e0e0e0`, padding 16px. Above the table: h1, `.subtitle`, `.disclaimer`.
- **Page CSS:** body system sans-serif (-apple-system stack), `line-height 1.6`, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em (margin-bottom 12px); `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, red bar fill `rgba(231,76,60,0.55)`, gray text `#666`/`#999`.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper; `canvas { display: block; margin: 0 auto; }`.
- Any links in regenerated HTML use `.html` extensions.
