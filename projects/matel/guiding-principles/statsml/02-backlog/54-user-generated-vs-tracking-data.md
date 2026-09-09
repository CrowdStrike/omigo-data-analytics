# User Generated vs Tracking Data

**Page type:** detail page (kusto-style 2-col text/viz layout: one `.card-section` per topic, text left 45%, canvas right 55%)
**HTML title tag:** User Generated vs Tracking Data

**Subtitle:** Two fundamentally different data origins — one created by intent, one collected by observation — with different compliance regimes and analytical implications.

## 1. The Distinction

**User-Generated Data** — content the user actively creates and knowingly publishes or sends:

- Social media posts, comments, reviews
- Emails, text messages, chat
- Uploaded photos, videos, documents
- Form submissions, survey responses

**Tracking Data** — passively collected observations about user activity:

- Location (GPS, cell tower, IP geolocation)
- Timestamps, session duration, visit frequency
- Browser fingerprint, device info, OS
- Click paths, scroll depth, hover maps
- Referrer chains, ad impressions, attribution

**Key point:** **The asymmetry:** The user *chose* to write a post. They did not choose to have their scroll depth recorded. This asymmetry of intent is the foundation of most privacy regulation.

### Visualization (canvas `c1`, 720×300)

Two-column categorization diagram.

- **Title (bold 14px, `#1a5276`, top center):** "Data Origin: Intent vs Observation".
- **Left box** (x = 4% of width, top y=44, width 40% of canvas, extends to 30px above bottom): fill `rgba(39,174,96,0.08)`, 2px stroke green `#27ae60`. Header bold 12px green centered: "USER GENERATED"; sub-caption 10px `#555`: '"I created this"'. Bulleted items (11px, `#2c3e50`, left-aligned, 22px spacing): "Posts & comments", "Emails & messages", "Photos & videos", "Reviews & ratings", "Documents & files".
- **Right box** (x = 54% of width, same size): fill `rgba(231,76,60,0.08)`, 2px stroke red `#e74c3c`. Header bold 12px red centered: "TRACKING"; sub-caption 10px `#555`: '"Collected about me"'. Bulleted items: "Location & GPS", "Session timestamps", "Browser fingerprint", "Click & scroll paths", "Referrer & attribution".
- **Bottom caption (11px, `#888`, centered):** "← Intentional                                      Passive →".

## 2. Compliance Boundaries

The data's origin determines what you're legally allowed to do with it:

- **Consent** — User content: implicit via ToS. Tracking: requires explicit opt-in (GDPR, CCPA).
- **Deletion** — Both must be deletable, but tracking data is harder to locate across distributed systems.
- **Portability** — User content must be exportable. Tracking/derived data is a grey area.
- **Retention** — User controls their content. Tracking must have defined expiry windows.
- **Third-party sharing** — Public user content may be shareable. Tracking is heavily restricted.

**Key point:** **Impact:** Mixing both types in a single table without tagging origin is a compliance debt that compounds silently.

*Example: A feature store column "posts_per_session" combines user-generated count + tracking timestamp — which retention policy applies?*

### Visualization (canvas `c2`, 720×300)

Grouped horizontal bar chart comparing compliance burden.

- **Title (bold 14px, `#1a5276`, top center):** "Compliance Burden by Data Type".
- **Legend (10px):** green swatch `#27ae60` "User-generated"; red swatch `#e74c3c` "Tracking" — at x=160 and x=280, y≈38–47.
- **Rows** (start y=52, row height 46, labels right-aligned 11px `#2c3e50` at x=148; bar origin x=160, max bar width = (720−200)/2). Each row: user-generated bar (fill `rgba(39,174,96,0.5)`, 14px tall) above tracking bar (fill `rgba(231,76,60,0.5)`, 14px tall, 18px offset), with percentage value labels (9px) in green/red at bar ends:

| Dimension | User-generated | Tracking |
|---|---|---|
| Consent required | 20% | 90% |
| Deletion complexity | 30% | 75% |
| Retention mandates | 15% | 85% |
| Sharing restrictions | 35% | 90% |
| Audit requirements | 40% | 80% |

## 3. The Grey Zones

Some data doesn't fit cleanly into either category:

- **Search queries** — typed intentionally, but not "created" as lasting content.
- **Likes / reactions** — intentional action, but also a behavioral signal mined for recommendations.
- **Shopping cart** — user action, but treated as tracking for recommendation engines.
- **Voice assistant recordings** — user spoke intentionally, didn't consent to permanent storage.

**Key point:** **The test:** Did the user produce this with the expectation that it would exist as a record? If yes → user-generated. If no → tracking. If ambiguous → treat as tracking.

*Example: A "like" on a post — the user tapped intentionally, but did they expect it to feed a behavioral model sold to advertisers?*

### Visualization (canvas `c3`, 720×300)

Horizontal gradient spectrum bar with plotted example items.

- **Title (bold 14px, `#1a5276`, top center):** "Intent Spectrum: Clear to Ambiguous".
- **Gradient bar:** x=40 to x=680 (width−80), y=90, height 20; linear gradient green `#27ae60` (0) → amber `#f39c12` (0.35–0.65) → red `#e74c3c` (1).
- **Zone labels above bar (10px):** "USER GENERATED" left-aligned in green; "GREY ZONE" centered in amber `#f39c12`; "TRACKING" right-aligned in red.
- **Items** plotted as 4px dots on the bar (green for ug, amber `#f39c12` for grey, red for tr), each with a 1px `#ccc` leader line down to a 10px `#2c3e50` label; labels alternate between two rows (24px and 68px below the bar):

| Item | Position (0–1) | Class |
|---|---|---|
| Blog post | 0.05 | user-generated |
| Email | 0.15 | user-generated |
| Review | 0.25 | user-generated |
| Search query | 0.38 | grey |
| Like | 0.48 | grey |
| Cart items | 0.57 | grey |
| Click path | 0.70 | tracking |
| Scroll depth | 0.80 | tracking |
| Location | 0.92 | tracking |

- **Bottom caption (11px, `#888`, centered):** "When in doubt, treat as tracking for compliance".

## 4. Pipeline Implications

The distinction determines what you can do at each pipeline stage:

- **Store** — user content has broader retention; tracking has mandated expiry.
- **Join** — cross-referencing tracking data across contexts may violate consent scope.
- **Model** — ML trained on tracking data inherits consent limitations on outputs.
- **Audit** — tracking requires lineage/provenance; user content requires access controls.

**Key point:** **Impact:** Every column should be tagged with its data-origin class at ingestion time. Downstream joins, features, and model training inherit the most restrictive policy of their inputs.

*Example: Embedding user-generated text for search — does vectorization change the compliance category? The vector encodes identity signal.*

### Visualization (canvas `c4`, 720×300)

Horizontal pipeline flow diagram with per-stage compliance concerns.

- **Title (bold 14px, `#1a5276`, top center):** "Pipeline Stage → Compliance Concern".
- **Stage boxes:** five equal-width boxes (total width−100, start x=50, y=80, height 60), bold 12px `#1a5276` centered labels, joined by short gray `#999` arrows. Risk stages: fill `rgba(231,76,60,0.08)`, stroke red `#e74c3c`; non-risk: fill `rgba(39,174,96,0.08)`, stroke green `#27ae60`. Below each box, a dashed (2/2) drop line to a 10px concern label in the stage color:

| Stage | Concern | Risk? |
|---|---|---|
| Ingest | Consent scope | yes (red) |
| Store | Retention limits | yes (red) |
| Join | Cross-context | yes (red) |
| Model | Consent inheritance | yes (red) |
| Serve | Output lineage | no (green) |

- **Bottom caption (11px, `#888`, centered):** "Each stage inherits the most restrictive policy from its inputs".

## 5. Anonymization & Obfuscation → Extended Retention

If data can no longer identify an individual, retention limits relax or disappear entirely (GDPR Recital 26 exempts truly anonymized data).

- **Generalization** — replace precise values with ranges (age 34 → 30-39, ZIP 10013 → 100xx). Reduces re-identification while preserving distribution shape.
- **k-Anonymity** — ensure every record is indistinguishable from at least k−1 others on quasi-identifiers. Enables indefinite retention of the generalized dataset.
- **Differential privacy** — inject calibrated noise so no single record materially affects query output. Aggregates become retainable; raw rows are still deletable.
- **Tokenization / Pseudonymization** — replace identifiers with opaque tokens. Caution: still "personal data" under GDPR if re-linkable. Extends retention only if the mapping key is destroyed.
- **Suppression** — drop rare combinations entirely. High privacy, but loses tail-distribution signal.

**Key point:** **The tradeoff:** Stronger anonymization extends retention but degrades analytical utility. The decision is: how much signal loss can you tolerate in exchange for keeping data longer?

*Example: Raw clickstream expires at 90 days. Aggregate session-duration histograms (k≥50, no user-ID) can be retained indefinitely — same trend signal, no compliance clock.*

### Visualization (canvas `c5`, 720×380)

Grouped vertical bar chart: privacy protection vs analytical utility per technique.

- **Title (bold 14px, `#1a5276`, top center):** "Anonymization: Privacy Gain vs Analytical Utility".
- **Plot area:** x=80, width = canvas−140, y=55, height 240. L-shaped axes `#ccc`; rotated y-axis label "Score (%)" (11px `#666`); horizontal gridlines `#f0f0f0` at 0/25/50/75/100 with 9px `#999` tick labels.
- **Bars:** per technique, a green privacy bar (`rgba(39,174,96,0.6)`) and blue utility bar (`rgba(26,82,118,0.5)`) side by side (each 30% of the group width), 9px `#2c3e50` two-line x labels:

| Technique (label line-break) | Privacy | Utility |
|---|---|---|
| Raw (none) | 5 | 98 |
| Pseudo- nymization | 35 | 90 |
| Generali- zation | 55 | 72 |
| k-Anonymity (k=50) | 75 | 55 |
| Differential Privacy | 92 | 40 |
| Full Suppression | 99 | 5 |

- **Legend (top right of plot, 10px):** green swatch "Privacy protection"; blue swatch "Analytical utility".
- **Retention arrow:** orange `#e67e22` 2px horizontal arrow near the bottom (y = height−18) with filled arrowhead, and centered orange 11px caption: "← Short retention                         Longer permissible retention →".

## 6. Practical Retention Strategy

A layered approach: keep raw data short, progressively anonymize for longer horizons.

- **Hot tier (0–30 days)** — full-fidelity raw data. Needed for debugging, real-time personalization, support tickets.
- **Warm tier (30–90 days)** — pseudonymized. User IDs tokenized, timestamps rounded to day. Still joinable if legally required.
- **Cold tier (90 days–2 years)** — k-anonymous aggregates. Group-level patterns, no individual traces. Mapping keys destroyed.
- **Archive (2+ years)** — differentially private summaries. Statistical properties preserved, re-identification mathematically bounded.

**Key point:** **Key insight:** You don't lose the data — you lose the ability to link it back to individuals. The statistical signal survives; the compliance liability doesn't.

*Example: "Average session duration increased 12% in Q3" is derivable from archive-tier DP summaries — no raw tracking data needed.*

### Visualization (canvas `c6`, 720×380)

Tier flow boxes with a signal-retention bar chart below.

- **Title (bold 14px, `#1a5276`, top center):** "Tiered Retention: Progressive Anonymization Over Time".
- **Tier boxes:** four equal-width boxes (total width−80, start x=40, y=55, height 110), each with fill = tier color at low alpha (`color + '15'`), 2px stroke in tier color, bold 13px tier label, 11px `#555` time range, 10px `#2c3e50` method (wrapped to two lines if >2 words); gray `#999` arrows with filled heads between boxes:

| Tier | Time | Method | Color | Signal |
|---|---|---|---|---|
| Hot | 0–30d | Raw (full fidelity) | `#e74c3c` | 100% |
| Warm | 30–90d | Pseudonymized | `#e67e22` | 88% |
| Cold | 90d–2yr | k-Anonymous aggregates | `#2980b9` | 62% |
| Archive | 2yr+ | DP summaries | `#1a5276` | 40% |

- **Bar chart** (40px below tier boxes, 80px tall): caption above bars (11px `#666`, centered): "Statistical signal retained (%)". One bar per tier (width 50% of tier slot), fill = tier color at ~33% alpha (`color + '55'`), 1px stroke in tier color, bold 11px percentage label above each bar in the tier color.
- **Bottom caption (11px, `#888`, centered):** "Signal degrades gracefully — compliance liability drops to zero at archive tier".

## Regeneration instructions

- **Template/layout:** backlog/kusto-style detail page. Body: h1, `.subtitle`, then six `.card-section` divs each with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with one `<tr>`: left `td.text-col` (45%) with paragraphs, `<ul>` bullets, `.key-point` callout and optional `.example` line; right `td.viz-col` (55%) with one canvas.
- **Page CSS:** body system-ui/-apple-system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. `.key-point`: background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem. `.example`: italic `#555` 0.9rem. `ul` 0.92rem. Canvases: 1px `#e0e0e0` border, 4px radius, `width: 100%`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, amber `#f39c12`, bar fills `rgba(39,174,96,0.5)` / `rgba(231,76,60,0.5)` / `rgba(26,82,118,0.5)`.
- **Canvas:** a shared `setup(id)` helper sizes the backing store at 720×300 × `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates; canvases c5 and c6 use inline sizing code with 720×380.
- No nav bar, no back/home links. This page has no outbound card links; any regenerated links elsewhere use `.html` extensions.
