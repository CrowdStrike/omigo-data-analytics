# Cloud Security

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Cloud Security

**Subtitle:** Tools that scan an organization's cloud drives, mail, and chat for threats and data leaks — which means they read the contents, including the personal files people quietly keep in work accounts.

**Disclaimer callout (orange left-border box):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared (the expected security purpose):** malware verdicts, phishing links, leak alerts, policy violations.
- **Incidental (swept up alongside):** to find threats, scanners read *file contents* across drives, mailboxes, and chat — personal photos, tax documents, resumes stored in the work account included.
- **Incidental:** sharing metadata — who shared what with whom, internally and externally.
- **Incidental:** per-user admin and audit actions — every open, download, permission change.
- **Inferred:** content classifications per document (financial, health, source code) and a collaboration map of the organization.
- **Inferred:** a risk score per user, built from behavior over time.

**Key-point callout:** The surprise: "we scan for threats" means "software reads every document." The medical form saved to a work drive gets content-classified as health data — attached to a name.

### Visualization (canvas `c1`, 720×420)

Grouped horizontal bar chart: assumed vs realistic extent of collection.

- **Title (bold 13px, `#1a5276`, top center):** "What employees assume is collected vs realistic extent (illustrative)".
- **Rows (label, assumed, actual):** Malware / phishing verdicts 75/95; File contents (drives) 15/90; Mail & chat contents 15/85; Personal files in work account 5/85; Sharing patterns (who ↔ whom) 20/90; Content categories per document 10/80; Every open / download / change 25/90; Risk score per user 5/80.
- **Layout:** right-aligned labels at x=220, bars start x=232, max bar width 370px scaled to 0–100, bar height 13px, assumed bar above actual bar (3px gap, 16px group gap), starting y=42; numeric value labels (10px `#2c3e50`) after each bar end.
- **Bar colors:** assumed `rgba(26,82,118,0.35)`, actual `rgba(231,76,60,0.55)`.
- **Legend (below the last row):** blue swatch "what employees assume"; red swatch "realistic extent".
- **Footnote (bottom center, `#999`, 11px):** "Numbers are illustrative, not measured."

## How it gets used

- **Threat detection:** malware, phishing, and account-takeover signals across the cloud footprint.
- **DLP enforcement:** documents flagged by content category; sharing blocked or reported by rule.
- **Incident investigation:** file, mail, and chat activity replayed per user to reconstruct events.
- **Behavioral baselining / risk scoring:** unusual downloads, mass sharing, or off-hours access raise a user's score.
- **Vendor cloud analysis and model training:** detections and content signals feed models trained across customers.
- **HR / legal escalation:** DLP hits and audit trails become evidence in disputes and terminations.

### Visualization (canvas `c2`, 720×340)

Flow diagram: data categories → central analysis box → uses.

- **Title (bold 13px, `#1a5276`, top center):** "From scanned content to uses".
- **Left boxes** (190×34px at x=30; 12% alpha fill, 1.5px colored stroke, 11px centered `#2c3e50` label): "File / mail / chat contents" `#1a5276` (y 40), "Sharing metadata" `#2980b9` (y 95), "Per-user audit actions" `#8e44ad` (y 150), "Content classifications" `#e67e22` (y 205).
- **Middle box** (160×60px at x=285 y=110, `#1a5276`): two lines "Content scanning +" / "user risk scoring".
- **Right boxes** (210×34px at x=490): "Threat detection & DLP blocking" `#27ae60` (y 34), "Incident investigation replay" `#2980b9` (y 90), "Vendor cloud models (all customers)" `#8e44ad` (y 146), "HR / legal escalation" `#e74c3c` (y 202).
- **Connectors:** gray `#bbb` 1.5px lines with solid arrowheads from each left box to the middle box, and from the middle box to each right box.
- **Footnote (bottom center, `#999`, 11px):** "A pipeline built to catch leaks also maps who collaborates with whom and what each person stores."

## How long it's kept

- **Hot storage:** recent scan results and alerts kept live — typically months.
- **Archive:** audit logs and DLP hits retained for years under compliance schedules.
- **Incident data:** anything tied to an investigation is kept effectively indefinitely.
- **Quarantine:** copies of "suspicious" files — including personal ones — can persist in vendor storage long after the original is deleted.
- **Vendor-side retention:** the vendor's logs, verdicts, and model data run on a separate clock from the employer's settings.
- **Legal hold overrides** every deletion schedule; retention outlives employment.
- **Identifiable vs de-identified:** scan telemetry the vendor de-identifies for threat intelligence and model training outlives the employer's retention window. The catch: user and device identifiers are usually only pseudonymized, not removed, so verdicts and DLP hits remain linkable to the person.

### Visualization (canvas `c3`, 720×360)

Horizontal retention timeline bars on a 0–8 year axis (8 = indefinite zone), with an "employee leaves company" marker.

- **Title (bold 13px, `#1a5276`, top center):** "Typical retention by data category (illustrative)".
- **Rows (label, years, color, note):** "Live scan results (hot)" 0.5 `#27ae60` "~months"; "Audit logs & DLP hits" 5 `#2980b9` "years"; "Vendor-side verdicts & models" 6 `#8e44ad` "vendor clock"; "Quarantined file copies" 6 `#e67e22` "can persist"; "Incident-linked records" 8 `#e74c3c` "indefinite" with arrowhead; "Anything under legal hold" 8 `#e74c3c` "indefinite" with arrowhead.
- **Layout:** right-aligned labels at x=215, axis starts x=227, axis width 430px, bar height 18px, gap 16px, starting y=50; bars filled at 45% alpha with 1px solid outline of the same color; notes in 10px `#666` after each bar; rows at 8 years get a solid right-pointing triangle arrowhead.
- **Axis (y=300):** gray line with tick labels "0y", "2y", "4y", "6y" and "indefinite →" at the 8-year position (11px `#666`).
- **Employee-leaves marker:** vertical dashed orange line (`#e67e22`, dash 5/4, width 2) at the 2-year position with bold 11px centered label "employee leaves company" below the axis.
- **Footnote (bottom center, `#999`, 11px):** "Deleting the file does not delete its scan record — or its quarantined copy. Durations are illustrative."

## What you get back

- The employee typically has **no export path** — no button to see what was scanned, flagged, classified, or quarantined about them.
- Access requests route through the employer, who may decline or redact.
- Even the employer often gets alerts and dashboards, not the vendor's raw scan data, model features, or quarantined copies.
- Your per-user risk score and content classifications are generally never shown to you.

**Key-point callout:** The asymmetry vs consumer platforms: a consumer service must offer the account holder an export. Here the account holder is the company — the person whose files were read has no standing with the vendor at all.

### Visualization (canvas `c4`, 720×320)

Two side-by-side outlined panels comparing what is retrievable vs what exists.

- **Title (bold 13px, `#1a5276`, top center):** "What the employee can retrieve vs what exists about them".
- **Left panel** (300×240px at x=40 y=40, `#27ae60` 2px stroke, no fill): heading bold 12px green "Retrievable by the employee"; 11px `#2c3e50` line "· their own files (while employed)"; italic 11px `#999` two lines "(no view of scans, flags, scores," / "or quarantined copies)".
- **Right panel** (300×240px at x=380 y=40, `#e74c3c` 2px stroke): heading bold 12px red "Exists about the employee"; left-aligned 11px `#2c3e50` list (21px spacing): "· scan verdicts on every file & message", "· content categories per document", "· personal files classified & indexed", "· sharing map: who ↔ whom, when", "· every open, download, permission change", "· DLP flags with content snippets", "· per-user risk score over time", "· quarantined copies in vendor storage", "· years of audit archive".
- **Footnote (bottom center, `#999`, 11px):** "The vendor answers to the company, not to the person whose documents were read."

## Regeneration instructions

- **Layout:** detail page using `.obj-table` — full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` callout, right `<td>` (55%, `text-align: center`) holds one canvas. Cell borders `1px solid #e0e0e0`, padding 16px. Page order: h1, `.subtitle`, `.disclaimer`, table.
- **Page CSS:** body system sans-serif stack, line-height 1.6, color `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, text `#7d5a29`, 0.9em; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em; canvas `display: block; margin: 0 auto`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fills `rgba(26,82,118,0.35)` and `rgba(231,76,60,0.55)`, gray text `#666`/`#999`/`#2c3e50`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
