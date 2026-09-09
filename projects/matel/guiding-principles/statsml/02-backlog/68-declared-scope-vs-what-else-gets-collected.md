# Declared Scope vs What Else Gets Collected

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Declared Scope vs What Else Gets Collected

**Subtitle:** Every collection system has the fields it is about — and the fields the implementation needed. The second set is tracking data by any definition, and nobody designed its sampling.

**Intro callout (blue-left-border box):** A collection system stores the fields the product is about plus the fields the implementation needed to function. The second set — the residue — is tracking data with an undesigned sampling frame, and analyses quietly drift onto it because it is denser.

## 1. What is it?

A system built to collect data has a **declared scope**: the fields the product is about. A survey collects answers. A payment form collects card details. A fitness app collects workouts.

But the implementation picks up whatever it needs to function: device model and OS version (rendering), IP address (routing), timestamps and retry counts (reliability), session identifiers (auth), latency measurements (performance). None of these were requested from the user — they are **residue** of making the request work.

**Key point (red-left-border box):** The residue is tracking data by any definition. A survey platform that stores answer text also stores when you answered, from what device, on what network, how many times the submission retried, and how long you took. The declared scope is one column; the envelope is a dozen.

### Visualization (canvas `c1`, 720×340)

Two-box diagram: the visible form vs the actual stored payload, connected by a "submit" arrow.

- **Title (bold 14px `#1a5276`, top center):** "What the product asks vs. what the request carries".
- **Left box** at (50,45), 250×250: fill `rgba(39,174,96,0.08)`, stroke `#27ae60` width 2. Header (bold 13px `#27ae60`, centered): "DECLARED SCOPE (the form)". Inside: three white field boxes 210×40 (stroke `#27ae60` width 1) at y=90/150/210, left-aligned 12px `#2c3e50` labels: "Q1: satisfaction (1–5)", "Q2: would recommend?", "Q3: free-text comment".
- **Arrow:** gray `#888` horizontal line from (310,170) to (360,170) with filled triangle head; label "submit" in 11px `#666` above at (335,158).
- **Right box** at (370,45), 310×250: fill `rgba(231,76,60,0.06)`, stroke `#e74c3c` width 2. Header (bold 13px `#e74c3c`, centered): "WHAT ACTUALLY GETS STORED". Inside: 8 rows in 12px Menlo monospace starting y=92, 25px apart:
  - "● answers: {q1, q2, q3}" — declared, colored `#27ae60`
  - "○ timestamp, timezone, clock skew" — `#c0392b`
  - "○ device model, OS, app version" — `#c0392b`
  - "○ IP address, network type" — `#c0392b`
  - "○ session id, auth token id" — `#c0392b`
  - "○ retry count, request id" — `#c0392b`
  - "○ time-to-complete, edit count" — `#c0392b`
  - "○ latency trace, battery level" — `#c0392b`
- **Caption (bottom center, 12px `#999`):** "One declared field group — seven incidental ones. The envelope is the larger dataset."

## 2. Where the residue comes from

Each engineering layer contributes fields for its own reasons — no one decided to "track users", each layer just solved its local problem:

- **Transport:** IP address, user agent, TLS fingerprint — needed to deliver the request at all.
- **Authentication:** session tokens, device identifiers, login timestamps — needed to know who is asking.
- **Reliability:** retry counts, request IDs, client clocks, delivery receipts — needed to dedupe and recover from failures.
- **Performance:** latency traces, network type, battery state, app version — needed to debug slowness.
- **UX instrumentation:** screen views, scroll depth, taps, dwell time — added to "understand usage".

**Key point:** The residue accumulates monotonically. Fields get added when a layer needs them and are almost never removed, because removal risks breaking something and storage is cheap. The envelope only grows.

### Visualization (canvas `c2`, 720×340)

Stacked layer diagram: five engineering layers each feeding one merged event log.

- **Title (bold 14px `#1a5276`, top center):** "Each engineering layer adds fields for its own reasons".
- **Layer bars** at x=90, width 380, height 44, 8px gap, starting y=48, top to bottom (fill = layer color at 0.16 alpha, stroke = layer color width 1.5; layer name bold 13px in layer color, "why:" line 11px `#666`, fields right-aligned 12px Menlo `#2c3e50`):
  | Layer | why | fields | color |
  |---|---|---|---|
  | UX instrumentation | "understand usage" | screens, taps, scroll, dwell | #8e44ad |
  | Performance | debug slowness | latency, network, battery, version | #e67e22 |
  | Reliability | dedupe & recover | retries, request ids, clocks | #e74c3c |
  | Authentication | know who asks | session, device id, login times | #2980b9 |
  | Transport | deliver the request | IP, user agent, TLS fingerprint | #1a5276 |
- **Connectors:** thin `#aaa` elbow lines from each bar's right edge to x=545, converging at y=175 into the store box.
- **Store box** at (560,105), 130×140: fill `rgba(26,82,118,0.12)`, stroke `#1a5276` width 2; centered bold 13px `#1a5276` three-line label: "One event log," / "all layers" / "merged".
- **Caption (bottom center, 12px `#999`):** "Fields are added when a layer needs them and almost never removed — the envelope only grows"

## 3. Why it matters statistically

Declared-scope fields have designed sampling: someone decided who gets asked, when, and what the field means. Residue fields have **accidental sampling**:

- **Coverage varies by infrastructure:** a telemetry field added in app v5.2 exists only for users who upgraded — and upgrade speed correlates with device wealth and engagement.
- **Missingness is not random:** latency traces drop under poor connectivity, so the worst network experiences are the least measured.
- **Semantics drift silently:** a "session" redefinition in one release changes every downstream metric with no schema change to warn anyone.
- **Meaning was never specified:** the field exists because a debugger needed it, so no one documented what population or condition it covers.

**Key point:** The trap: analyses quietly migrate from declared fields to residue fields because residue is denser — "engagement" from session pings, "user quality" from device model. The analysis then rests on data whose sampling nobody designed or documented.

### Visualization (canvas `c3`, 720×340)

Grouped bar chart: coverage % of a declared field vs a residue field across five user cohorts.

- **Title (bold 14px `#1a5276`, top center):** "Coverage of a residue field vs. a declared field (illustrative)".
- **Data (coverage %):**
  | Cohort | declared | residue |
  |---|---|---|
  | new devices, fast upgraders | 96 | 98 |
  | mainstream | 95 | 84 |
  | older devices | 94 | 55 |
  | poor connectivity | 91 | 31 |
  | rarely updates app | 93 | 18 |
- **Axes:** plot origin x=90, baseline y=265, width 560, height 200; y gridlines `#eee` at 0/25/50/75/100% with right-aligned 11px `#666` "%" labels; axes stroke `#999` width 1.5. Cohort labels 11px `#2c3e50`, centered, two lines where wrapped ("new devices,\nfast upgraders", "poor\nconnectivity", "rarely\nupdates app").
- **Bars:** 34px wide pairs per cohort — declared: fill `rgba(39,174,96,0.55)` stroke `#27ae60`; residue: fill `rgba(231,76,60,0.55)` stroke `#e74c3c`.
- **Legend (top-left inside plot):** green swatch "declared field (survey answer)"; red swatch "residue field (telemetry added in v5.2)".
- **Caption (bottom center, 12px `#999`):** "The residue field's coverage follows upgrade speed — which correlates with the outcomes being studied"

## 4. What to do about it

**Inventory and classify.** For each stored field, record which class it belongs to:

- **Declared** — the product asked for it; sampling was designed; safe to analyze with known caveats.
- **Operational** — needed to run the service; sampling follows infrastructure, not population; analyze only with a coverage audit.
- **Residue** — collected incidentally; sampling undesigned and undocumented; treat any analysis resting on it as provisional until coverage is verified.

**For every analysis, ask:** which class of field does each conclusion actually rest on? A dashboard "based on survey data" whose key segmentation comes from device model is resting on residue, not on the survey.

**Key point:** This is a lineage question, not a privacy question (though it is that too). The classification tells you which conclusions inherit a designed sampling frame and which inherit whatever the infrastructure happened to record.

### Visualization (canvas `c4`, 720×340)

Three-column triage table: field classes with example fields, sampling nature, and usage rule.

- **Title (bold 14px `#1a5276`, top center):** "Field inventory: classify before you analyze".
- **Columns** 200px wide, 22px gap, starting x=55, y=48, height 220; each has a colored header band (32px tall, 0.85 alpha fill, white bold 13px text), white body, colored border width 1.5:
  | Header | color | example fields (12px Menlo) | sampling | use |
  |---|---|---|---|---|
  | DECLARED | #27ae60 | survey answers / order line items / profile fields | designed | analyze with known caveats |
  | OPERATIONAL | #e67e22 | timestamps / session ids / app version | follows infrastructure | coverage audit first |
  | RESIDUE | #e74c3c | device model / latency traces / retry counts | undesigned | provisional until verified |
- Inside each column: light `#eee` divider line, then "sampling: <value>" in bold 11px `#666` and the use line in 11px column color.
- **Bottom question (bold 13px `#1a5276`, centered):** "For every conclusion: which column is it actually resting on?"
- **Caption (bottom center, 12px `#999`):** "A "survey-based" dashboard segmented by device model is resting on the third column, not the first"

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** all four canvases declare intrinsic `width="720" height="340"`; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
