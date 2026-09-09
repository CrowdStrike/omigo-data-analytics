# Firewall / Gateway

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Firewall / Gateway

**Subtitle:** Every site visited by every user, categorized and logged — often with TLS inspection reading inside sessions users believe are encrypted.

**Disclaimer callout (orange left-border box):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared (the security purpose):** allow/block decisions per site, malware and phishing blocks, policy violations.
- **Incidental (swept up alongside):** every URL by every user, tagged with a category — the filtering taxonomy includes labels like health, religion, and job search; content *inside* HTTPS sessions where TLS inspection is on; blocked attempts, which are still logs of intent; home traffic for remote / VPN users.
- **Inferred:** a per-user browsing profile, category summaries per person, risk and "unusual browsing" flags.

**Key-point callout:** Most surprising to the employee: the padlock in the browser does not mean private here. With TLS inspection, the gateway **decrypts and reads inside the session** — and every visit is filed under a category label, next to your username.

### Visualization (canvas `c1`, 720×420)

Grouped horizontal bar chart: assumed vs realistic extent of collection.

- **Title (bold 13px, `#1a5276`, top center):** "What employees assume is collected vs realistic extent (illustrative)".
- **Legend (top, at x≈180 and x≈280):** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent".
- **Rows (label, assumed, realistic):** Malicious sites blocked 90/95; Every URL, with username 25/95; Category labels per visit 5/85; Inside HTTPS (TLS inspect) 3/70; Blocked attempts (intent) 15/90; Per-user manager reports 5/75; Home / VPN traffic 8/80.
- **Layout:** right-aligned labels at x=190, bars start x=202, max bar width 480px scaled to 0–100, bar height 13px, assumed bar above actual bar (3px inner gap, 14px group gap), starting y=54.
- **Bar colors:** assumed `rgba(26,82,118,0.35)`, realistic `rgba(231,76,60,0.55)`; numeric value labels after each bar end in `#1a5276` (assumed) and `#e74c3c` (actual), 10px.
- **Footnote (bottom center, `#999`, 11px):** "Numbers are illustrative, not measured — the point is the gap, not the values."

## How it gets used

- **Threat blocking:** stopping malicious and phishing sites in real time.
- **Incident investigation:** reconstructing exactly what a person browsed before and after an event.
- **Acceptable-use enforcement:** per-user browsing reports — by category, by hour — can be generated for managers.
- **Vendor cloud analysis and model training:** URLs are sent to the vendor's cloud for categorization and feed its models.
- **HR / legal escalation:** browsing logs are routine evidence in misconduct cases.

### Visualization (canvas `c2`, 720×340)

Flow diagram: data categories on the left, a central hub, five use boxes on the right.

- **Title (bold 13px, `#1a5276`, top center):** "From browsing logs to five uses".
- **Left boxes** (160×32px at x=25, `#1a5276` outline, 12% alpha fill, bold 11px centered label): "URL + username" (y 60), "Category label" (y 125), "Decrypted content" (y 190), "Blocked attempts" (y 255).
- **Central hub** (130×52px at x=290, centered y=158, `#1a5276`, 18% alpha fill, 2px stroke, bold 12px two-line label): "Per-user" / "browsing log".
- **Right boxes** (210×30px at x=487, 12% alpha fill, 1.5px colored stroke, bold 11px centered label): "Threat blocking" `#27ae60` (y 50), "Incident investigation" `#2980b9` (y 108), "Per-user manager reports" `#e67e22` (y 166), "Vendor cloud + model training" `#8e44ad` (y 224), "HR / legal escalation" `#e74c3c` (y 282).
- **Connectors:** thin gray `#bbb` lines from each left box to the hub, and from the hub to each right box, ending in small gray arrowheads before the right boxes.

## How long it's kept

- **Hot storage:** web logs searchable for roughly 30–90 days.
- **Archive:** per-user browsing history commonly retained a year or more for compliance.
- **Incident data:** logs attached to a case are kept indefinitely.
- **Vendor-side copies:** URLs sent for categorization live on the vendor's retention schedule, not the employer's.
- **Legal hold:** overrides every deletion schedule.
- **Retention outlives employment:** years of categorized browsing remain after you leave.
- **Identifiable vs de-identified:** browsing telemetry the vendor de-identifies for threat intelligence and model training outlives the employer's retention window. The catch: user and device identifiers are usually only pseudonymized, not removed, so the categorized history remains linkable to the person.

### Visualization (canvas `c3`, 720×330)

Horizontal retention timeline bars per data category, with an "employee leaves company" marker.

- **Title (bold 13px, `#1a5276`, top center):** "How long each copy lives (illustrative)".
- **Rows** (right-aligned labels at x=200, bars start x=212, track width 460px `#f0f0f0`, filled fraction at 55% alpha, bar height 20px, gap 22px, starting y=52): "Web logs (hot)" fraction 0.12 `#27ae60`; "Browsing-history archive" 0.58 `#2980b9`; "Vendor cloud copies" 0.66 `#8e44ad`; "Incident case logs" 1.0 `#e74c3c` with "indefinite" arrowhead; "Anything under legal hold" 1.0 `#e67e22` with "indefinite" arrowhead.
- **Indefinite marker:** bars at fraction 1.0 get a right-pointing solid triangle past the track end and a bold 10px "indefinite" label inside the bar end.
- **Employee-leaves marker:** vertical dashed red line (`#e74c3c`, dash 5/4, width 2) at 40% of the track width, spanning all rows, with bold 11px centered red label "employee leaves company" below.
- **Axis labels (11px `#666`):** "day 0" at track start (left-aligned), "years →" at track end (right-aligned), at the bottom.

## What you get back

- The employee has **no export path** — no account with the vendor, no portal, no download button for years of categorized browsing history.
- Access requests route through the employer, which may decline or redact.
- Even the employer often gets **reports, not raw logs** — the vendor's cloud copies stay with the vendor.

**Key-point callout:** The asymmetry: a consumer browser offers a "clear history" button and a data export; the workplace gateway that logged those *same visits* under health / religion / job-search labels offers the person **neither**.

### Visualization (canvas `c4`, 720×340)

Two side-by-side panels comparing what is retrievable vs what exists.

- **Title (bold 13px, `#1a5276`, top center):** "What the employee can retrieve vs what exists about them".
- **Left panel** (310×270px at x=30 y=40, `#27ae60` 2px stroke, 8% alpha fill): heading bold 13px green "What the employee can retrieve"; centered italic 12px "(nearly nothing)"; 11px lines "No account. No portal. No export." and "The browser's \"clear history\" clears nothing here."
- **Right panel** (310×270px at x=380 y=40, `#e74c3c` 2px stroke, 8% alpha fill): heading bold 13px red "What exists about them"; bulleted list (small red square bullets, 11px `#2c3e50` text, 24px line spacing): "Every URL visited, with username", "Category label per visit", "Health / religion / job-search tags", "Content read via TLS inspection", "Every blocked attempt (intent log)", "Browsing done from home on VPN", "Per-user reports viewable by managers", "Years of archived history", "Copies in the vendor's cloud".

## Regeneration instructions

- **Layout:** detail page using `.obj-table` — full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` callout, right `<td>` (55%, `text-align: center`) holds one canvas. Cell borders `1px solid #e0e0e0`, padding 16px. Page order: h1, `.subtitle`, `.disclaimer`, table.
- **Page CSS:** body system sans-serif stack, line-height 1.6, color `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, text `#7d5a29`, 0.9em; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em; canvas `display: block; margin: 0 auto`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fills `rgba(26,82,118,0.35)` and `rgba(231,76,60,0.55)`, gray text `#666`/`#999`/`#2c3e50`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
