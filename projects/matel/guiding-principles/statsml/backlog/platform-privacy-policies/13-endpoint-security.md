# Endpoint Security

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Endpoint Security

**Subtitle:** The agent on every work machine sees process lists, files, browsing, and USB activity — everything on the machine, personal use included, for an employee who never read the policy.

**Disclaimer (orange callout):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared (the security purpose):** every process launched, files created / opened / deleted, network connections from the machine, logins and privilege use.
- **Incidental (swept up alongside):** browsing on the work laptop; personal sessions — banking, health portals, personal email — in the same event stream; every USB device plugged in; clipboard contents and periodic screenshots in some configurations.
- **Inferred:** a per-user behavioral baseline of "normal," anomaly flags on off-hours or unusual activity, an insider-risk score.

**Key point (blue-left-border box):** Most surprising to the employee: suspicious-looking files can be **uploaded from the laptop to the vendor's cloud** for analysis — including personal documents that merely look unusual. The lunch-break banking session lives in the same stream as the malware alerts.

### Visualization (canvas `c1`, 720×460)

Grouped horizontal bar chart: assumed vs realistic extent of collection, per data category.

- **Title (bold 13px `#1a5276`, top center):** "What employees assume is collected vs realistic extent (illustrative)".
- **Legend (top, at x=180 and x=280):** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent". Labels in `#2c3e50` 11px.
- **Rows (label, assumed a, realistic b — values on 0–100 scale):**
  - Malware scan results: a=90, b=95
  - Process launches: a=40, b=95
  - Files opened / created: a=30, b=90
  - Browsing activity: a=20, b=85
  - USB devices plugged in: a=25, b=90
  - Personal-use sessions: a=5, b=80
  - Files sent to vendor cloud: a=3, b=60
  - Clipboard / screenshots: a=2, b=40
  - Behavioral risk score: a=5, b=75
- **Geometry:** right-aligned labels at x=190 in `#2c3e50` 12px, bars start at x=202, max bar width 480px, bar height 13px, inner gap 3px, group gap 13px, start y=54. Assumed bar on top (`rgba(26,82,118,0.35)`), realistic bar below (`rgba(231,76,60,0.55)`). Numeric value in 10px just past each bar end: assumed value in `#1a5276`, realistic value in `#e74c3c`.
- **Caption (bottom center, `#999` 11px):** "Numbers are illustrative, not measured — the point is the gap, not the values."

## How it gets used

- **Threat detection:** matching events against known-bad patterns in near real time.
- **Incident investigation:** an analyst can replay days of one person's machine activity, event by event.
- **Behavioral baselining / insider risk:** models score how far today's behavior sits from that user's normal.
- **Vendor cloud analysis and model training:** telemetry from all customers feeds the vendor's detection models.
- **HR / legal escalation:** the same records support misconduct cases and litigation, not just security.

### Visualization (canvas `c2`, 720×340)

Hub-and-spoke flow diagram: four data-category boxes on the left feed one central hub, which fans out to five use boxes on the right.

- **Title (bold 13px `#1a5276`, top center):** "From one event stream to five uses".
- **Left boxes (160×32 at x=25, all `#1a5276` — 0.12 alpha fill, 1.5px stroke, bold 11px `#1a5276` label):** Process events (y=60), File events (y=125), Browsing / network (y=190), USB / clipboard (y=255).
- **Central hub (130×52 box at x=290, centered on y=158, `#1a5276` at 0.18 alpha fill, 2px stroke):** two-line bold 12px label "One combined" / "event stream".
- **Right boxes (210×30 at x=487, each with 0.12 alpha fill, 1.5px stroke, bold 11px label in its color):**
  - Threat detection (y=50, `#27ae60`)
  - Incident replay of your days (y=108, `#2980b9`)
  - Insider-risk scoring (y=166, `#e67e22`)
  - Vendor cloud + model training (y=224, `#8e44ad`)
  - HR / legal escalation (y=282, `#e74c3c`)
- **Arrows:** straight `#bbb` 1.5px lines from each left box into the hub, and from the hub out to each right box with a small filled arrowhead.

## How long it's kept

- **Hot storage:** raw telemetry searchable for roughly 30–90 days.
- **Archive:** compressed events kept for a year or more for audit and compliance.
- **Incident data:** anything attached to a case is typically kept indefinitely.
- **Vendor-side copies:** the vendor's retention clock is separate from the employer's — deleting one does not delete the other.
- **Legal hold:** overrides every deletion schedule.
- **Retention outlives employment:** leaving the company does not delete the record of your years on its machines.
- **Identifiable vs de-identified:** the de-identified telemetry the vendor keeps for threat intelligence and model training outlives the employer's retention window entirely. The catch: user and device identifiers are usually only pseudonymized, not removed, so the records remain linkable to the person.

### Visualization (canvas `c3`, 720×330)

Horizontal retention-timeline bars over a gray track, with an "employee leaves company" dashed marker.

- **Title (bold 13px `#1a5276`, top center):** "How long each copy lives (illustrative)".
- **Geometry:** right-aligned labels at x=200 in `#2c3e50` 12px; bars start at x0=212, track width 460px filled `#f0f0f0`; colored fill at 0.55 alpha covering `frac` of the track; bar height 20px, gap 22px, start y=52.
- **Rows (label, fraction of track, color):**
  - Raw telemetry (hot): 0.12, `#27ae60`
  - Archived events: 0.55, `#2980b9`
  - Vendor cloud copies: 0.65, `#8e44ad`
  - Incident case data: 1.0, `#e74c3c`, with filled arrowhead past track end and bold 10px "indefinite" label
  - Anything under legal hold: 1.0, `#e67e22`, with filled arrowhead past track end and bold 10px "indefinite" label
- **Marker:** vertical dashed red line (`#e74c3c`, dash 5/4, width 2) at 40% of the track, labeled below in bold red 11px: "employee leaves company".
- **Axis labels (`#666` 11px):** "day 0" at track start, "years →" at track end.

## What you get back

- The employee is the data subject but **not the customer** — there is no account, no portal, no export button.
- Any access request routes through the employer, who decides whether and what to show.
- Even the employer usually gets **dashboards and alerts, not raw telemetry** — the vendor may not return the full event stream to anyone.

**Key point (blue-left-border box):** This is the sharpest asymmetry in the privacy landscape: consumer platforms are legally pushed toward "download your data" tools; workplace security tools observe far more per person and give the observed person **no retrieval path at all**.

### Visualization (canvas `c4`, 720×340)

Two side-by-side panels: a nearly empty green panel vs a red panel with a long bulleted list.

- **Title (bold 13px `#1a5276`, top center):** "What the employee can retrieve vs what exists about them".
- **Left panel (310×270 at x=30, y=40; green `#27ae60` at 0.08 alpha fill, 2px stroke):** bold 13px green title "What the employee can retrieve"; centered italic 12px `#2c3e50` "(nearly nothing)"; then 11px lines "No account. No portal. No export." and "Requests route through the employer."
- **Right panel (310×270 at x=380, y=40; red `#e74c3c` at 0.08 alpha fill, 2px stroke):** bold 13px red title "What exists about them"; bulleted items (small red square bullets, `#2c3e50` 11px, 24px spacing):
  - Every process launched, timestamped
  - Every file created, opened, deleted
  - Browsing history, work and personal
  - Personal sessions on the work laptop
  - Every USB device ever plugged in
  - Clipboard / screenshots (some configs)
  - Files copied to the vendor cloud
  - A behavioral baseline of "you"
  - An insider-risk score, updated daily

## Regeneration instructions

- **Layout:** detail page with `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` box, right `<td>` (55%, `text-align: center`) holds the canvas. Table cell borders `1px solid #e0e0e0`, padding 16px. Above the table: h1, `.subtitle`, `.disclaimer`.
- **Page CSS:** body system sans-serif (-apple-system stack), `line-height 1.6`, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em (margin-bottom 12px); `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, red bar fill `rgba(231,76,60,0.55)`, gray text `#666`/`#999`.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper; `canvas { display: block; margin: 0 auto; }`.
- Any links in regenerated HTML use `.html` extensions.
