# Identity Provider

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Identity Provider

**Subtitle:** The single sign-on layer sits in front of every work app — which makes it a complete access diary for every person in the organization, written one login at a time.

**Disclaimer callout (orange left-border box):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared (the expected security purpose):** username, login success/failure, MFA challenges and outcomes, password-reset events.
- **Incidental (swept up alongside):** timestamp, IP address and derived location, device model and OS, browser fingerprint, network name — for *every* login to *every* connected app.
- **Incidental:** MFA push metadata — the approval comes from your phone, so its location and status ride along.
- **Inferred:** per-person behavioral baseline — usual hours, usual places, usual devices; each login scored against it (impossible-travel, anomaly, risk level).
- **Inferred:** app-usage profile — which tools you open, when, how often.

**Key-point callout:** The surprise: the login log is a diary. Sign-in times reveal your work schedule, locations reveal your movements, and the MFA push quietly reports where your personal phone is at the moment you tap "approve."

### Visualization (canvas `c1`, 720×420)

Grouped horizontal bar chart: assumed vs realistic extent of collection.

- **Title (bold 13px, `#1a5276`, top center):** "What employees assume is collected vs realistic extent (illustrative)".
- **Rows (label, assumed, actual):** Login success / failure 80/95; Timestamp of every login 45/95; Location per login 20/90; Device & browser fingerprint 15/85; Network / IP address 20/90; MFA push phone metadata 5/70; Which apps, when, how often 10/85; Behavioral risk score per login 5/80.
- **Layout:** right-aligned labels at x=210, bars start x=222, max bar width 380px scaled to 0–100, bar height 13px, assumed bar above actual bar (3px gap, 16px group gap), starting y=42; numeric value labels (10px `#2c3e50`) after each bar end.
- **Bar colors:** assumed `rgba(26,82,118,0.35)`, actual `rgba(231,76,60,0.55)`.
- **Legend (below the last row):** blue swatch "what employees assume"; red swatch "realistic extent".
- **Footnote (bottom center, `#999`, 11px):** "Numbers are illustrative, not measured."

## How it gets used

- **Threat detection:** each login scored in real time — block, allow, or demand extra proof.
- **Incident investigation:** the access diary is replayed to reconstruct who touched what, when, from where.
- **Behavioral baselining:** models learn each person's normal hours, places, and devices; deviations raise the risk score.
- **Vendor cloud analysis:** login signals feed the vendor's shared detection models, trained across all customers.
- **HR / legal escalation:** the same logs answer non-security questions — "when did they log in?", "were they working that day?".

**Key-point callout:** A dataset collected to catch attackers doubles, without any change, as an attendance and productivity record.

### Visualization (canvas `c2`, 720×340)

Flow diagram: data categories → central analysis box → uses.

- **Title (bold 13px, `#1a5276`, top center):** "From login records to uses".
- **Left boxes** (190×34px at x=30; 12% alpha fill, 1.5px colored stroke, 11px centered `#2c3e50` label): "Login events (who, when)" `#1a5276` (y 40), "Location / device / network" `#2980b9` (y 95), "MFA push metadata" `#8e44ad` (y 150), "App-usage patterns" `#e67e22` (y 205).
- **Middle box** (160×60px at x=285 y=110, `#1a5276`): two lines "Behavioral baseline" / "+ risk scoring".
- **Right boxes** (210×34px at x=490): "Threat detection (block / allow)" `#27ae60` (y 34), "Incident investigation replay" `#2980b9` (y 90), "Vendor cloud models (all customers)" `#8e44ad` (y 146), "HR / legal escalation" `#e74c3c` (y 202).
- **Connectors:** gray `#bbb` 1.5px lines with solid arrowheads from each left box to the middle box, and from the middle box to each right box.
- **Footnote (bottom center, `#999`, 11px):** "The same pipeline that catches attackers also answers \"was this employee at their desk on Tuesday?\""

## How long it's kept

- **Hot storage:** recent sign-in logs kept live for search — typically months.
- **Archive:** audit trails exported to long-term storage — retention measured in *years*, often driven by compliance rules.
- **Incident data:** anything attached to an investigation is kept effectively indefinitely.
- **Vendor-side copies:** the identity vendor keeps its own logs and model data on a separate clock from the employer's settings.
- **Legal hold:** overrides every deletion schedule.
- **Leaving doesn't erase:** deactivating the account stops new logins — the years of history stay for the full audit window.
- **Identifiable vs de-identified:** sign-in telemetry the vendor de-identifies for threat intelligence and model training outlives the employer's retention window. The catch: user and device identifiers are typically only pseudonymized, not removed — the behavioral baseline stays linkable to the person.

### Visualization (canvas `c3`, 720×360)

Horizontal retention timeline bars on a 0–8 year axis (8 = indefinite zone), with an "employee leaves company" marker.

- **Title (bold 13px, `#1a5276`, top center):** "Typical retention by data category (illustrative)".
- **Rows (label, years, color, note):** "Live sign-in logs (hot)" 0.5 `#27ae60` "~months"; "Audit archive (employer)" 5 `#2980b9` "years"; "Vendor-side logs & models" 6 `#8e44ad` "vendor clock"; "Behavioral baseline / scores" 5 `#e67e22` "years"; "Incident-linked records" 8 `#e74c3c` "indefinite" with arrowhead; "Anything under legal hold" 8 `#e74c3c` "indefinite" with arrowhead.
- **Layout:** right-aligned labels at x=215, axis starts x=227, axis width 430px, bar height 18px, gap 16px, starting y=50; bars filled at 45% alpha with 1px solid outline of the same color; notes in 10px `#666` after each bar; rows at 8 years get a solid right-pointing triangle arrowhead.
- **Axis (y=300):** gray line with tick labels "0y", "2y", "4y", "6y" and "indefinite →" at the 8-year position (11px `#666`).
- **Employee-leaves marker:** vertical dashed orange line (`#e67e22`, dash 5/4, width 2) at the 2-year position with bold 11px centered label "employee leaves company" below the axis.
- **Footnote (bottom center, `#999`, 11px):** "Most of the access diary outlives the job. Durations are illustrative."

## What you get back

- The employee is the **data subject but not the customer** — there is typically no self-service export of your own login history at all.
- Access or deletion requests route *through the employer*, who decides whether and how to respond.
- Even the employer may only get dashboards and summaries — raw vendor-side logs and risk-model internals usually don't come back.
- Risk scores and behavioral baselines about you are generally never disclosed to you.

**Key-point callout:** The asymmetry vs consumer platforms: a consumer app must usually offer you an export button. Workplace identity systems owe that duty to your employer — the person the data describes gets nothing by default.

### Visualization (canvas `c4`, 720×320)

Two side-by-side outlined panels comparing what is retrievable vs what exists.

- **Title (bold 13px, `#1a5276`, top center):** "What the employee can retrieve vs what exists about them".
- **Left panel** (300×240px at x=40 y=40, `#27ae60` 2px stroke, no fill): heading bold 12px green "Retrievable by the employee"; 11px `#2c3e50` line "· maybe a \"recent devices\" screen"; italic 11px `#999` two lines "(no export path — requests go" / "through the employer)".
- **Right panel** (300×240px at x=380 y=40, `#e74c3c` 2px stroke): heading bold 12px red "Exists about the employee"; left-aligned 11px `#2c3e50` list (21px spacing): "· every login to every connected app", "· timestamp, IP, location per login", "· device & browser fingerprints", "· MFA challenges & push metadata", "· app-usage patterns over years", "· failed attempts & lockouts", "· behavioral baseline & risk scores", "· years of audit archive", "· vendor-side copies & model data".
- **Footnote (bottom center, `#999`, 11px):** "The data subject and the customer are different people — and only the customer holds the keys."

## Regeneration instructions

- **Layout:** detail page using `.obj-table` — full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` callout, right `<td>` (55%, `text-align: center`) holds one canvas. Cell borders `1px solid #e0e0e0`, padding 16px. Page order: h1, `.subtitle`, `.disclaimer`, table.
- **Page CSS:** body system sans-serif stack, line-height 1.6, color `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, text `#7d5a29`, 0.9em; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em; canvas `display: block; margin: 0 auto`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fills `rgba(26,82,118,0.35)` and `rgba(231,76,60,0.55)`, gray text `#666`/`#999`/`#2c3e50`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
