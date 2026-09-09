# Statistical De-anonymization

**Page type:** detail page (kusto-style 2-col text/viz `.card-section` layout plus one full-width cases table; BACKLOG status badge next to h1)
**HTML title tag:** Statistical De-anonymization

**Status badge (in h1):** BACKLOG

**Subtitle:** How statistical methods break anonymization — and the lawsuits that proved "anonymous" data isn't.

## 1. Core Statistical Methods

De-anonymization exploits the mathematical reality that high-dimensional data is inherently sparse — most people occupy unique positions in feature space.

- **Linkage attacks** — Join anonymized data with auxiliary datasets on shared quasi-identifiers (ZIP + DOB + gender). Exploits set intersection statistics.
- **Nearest-neighbor matching** — Find the record in the anonymous set closest (L2, cosine) to a known individual's public profile. Works even with noise.
- **Singular value decomposition (SVD)** — Decompose a sparse ratings matrix, project known ratings into latent space, match by proximity. (Netflix attack.)
- **Maximum likelihood estimation** — Given partial observations about a target, compute P(record = target | observations) across all records. Pick the MAP estimate.
- **Temporal fingerprinting** — Model transition probabilities (location A → B at time T). Markov chains on mobility traces uniquely identify individuals with 4 spatio-temporal points (de Montjoye, 2013).

**Key point:** **The math:** In a dataset with d dimensions, uniqueness grows exponentially. With ~30 binary attributes, 2³⁰ > 1 billion possible combinations — far exceeding any population. Each person is likely unique.

### Visualization (canvas `c1`, 720×380)

Line chart: percent uniquely identifiable vs number of quasi-identifier dimensions (exponential-saturation curve).

- **Title (bold 14px, `#1a5276`, top center):** "Uniqueness Grows Exponentially with Dimensions".
- **Plot area:** x=80, width = canvas−130, y=55, height 260. L-shaped axes `#ccc`; x-axis label "Number of quasi-identifier dimensions" (11px `#666`, centered below); y-axis label "% uniquely identifiable" rotated −90°. Horizontal gridlines `#f0f0f0` at 0/25/50/75/100% with 9px `#999` tick labels; x ticks at 2, 4, 6, 8, 10, 12, 14 (9px `#999`); x scale 0–16.
- **Data (dims → % unique):** 1→2, 2→8, 3→25, 4→52, 5→74, 6→87, 8→95, 10→99, 15→99.9.
- **Series:** connected line, red `#e74c3c`, 2.5px, with 4px red dots at each point; area under the curve filled `rgba(231,76,60,0.1)`.
- **Annotations:**
  - At (3 dims, 25%): dashed (3/3) orange `#e67e22` vertical line up to near the top, with orange 10px label "Sweeney: 3 fields → 87%".
  - At (8 dims, 95%): dashed blue `#2980b9` leader line down-right with blue 10px label "Netflix: 8 ratings → 99%".
- **Bottom caption (11px, `#888`, centered):** "More dimensions = more unique = easier to re-identify".

## 2. Attack Taxonomy

Different data types fall to different statistical strategies:

- **Tabular data** — Quasi-identifier linkage. Sweeney showed 87% of Americans identifiable by {ZIP, DOB, gender}. Simple join operations.
- **Ratings / preferences** — SVD-based embedding + nearest-neighbor. Even 8 movie ratings with ±14-day noise → 99% re-identification (Narayanan & Shmatikov, 2008).
- **Location traces** — Spatio-temporal uniqueness. 4 time-stamped points identify 95% of people in a 1.5M-person mobility dataset.
- **Graph / social network** — Structural fingerprinting. Node degree sequence + local topology matches across anonymized and public graphs.
- **Genomic data** — Surname inference via Y-chromosome markers + genealogy databases. Then standard linkage.
- **Purchase / transaction** — Temporal patterns + amounts form unique sequences. 4 transactions identify 90% of people (de Montjoye, 2015).

**Philosophy callout:** **Common thread:** All attacks exploit the same principle — human behavior is high-dimensional and individually distinctive. Removing names doesn't reduce dimensionality.

### Visualization (canvas `c2`, 720×380)

Horizontal bar chart: re-identification success rate by data type.

- **Title (bold 14px, `#1a5276`, top center):** "Re-identification Success Rate by Data Type".
- **Rows** (start y=52, 44px row height; type labels 11px `#2c3e50` right-aligned at x=198; bars 22px tall from x=210, max width = canvas−320; fill = color at ~27% alpha (`color + '44'`), 1px stroke in color; bold 11px percentage in color at bar end; 9px `#888` parenthetical auxiliary-info note after it):

| Data type | Rate | Aux needed | Color |
|---|---|---|---|
| Location traces | 95% | 4 points | `#e74c3c` |
| Movie ratings | 99% | 8 ratings | `#e74c3c` |
| Purchase records | 90% | 4 transactions | `#e67e22` |
| Tabular (ZIP+DOB+gender) | 87% | 3 fields | `#e67e22` |
| Search queries | 70% | query content | `#f39c12` |
| Social graph | 60% | degree sequence | `#2980b9` |
| Aggregate heatmaps | 40% | small populations | `#27ae60` |

- **Bottom caption (11px, `#888`, centered):** "Success rates from published research — real attacks on real datasets".

## 3. Landmark Cases & Lawsuits

Full-width cases table (`table.cases`), columns Case / Year / Method / Outcome:

| Case | Year | Method | Outcome |
|---|---|---|---|
| **Massachusetts GIC / Sweeney** | 1997 | Linked "anonymous" state employee health records with voter rolls using ZIP + DOB + gender. Identified Governor Weld's records. | Catalyzed HIPAA Safe Harbor rules. Proved naive de-identification fails. |
| **AOL Search Data** | 2006 | Released 20M "anonymized" search queries (numeric user IDs). NYT journalists identified User 4417749 via query content (names, addresses in searches). | CTO resigned. Class-action lawsuit (settled). Demonstrated that search queries ARE identity. |
| **Netflix Prize** | 2007–2009 | Narayanan & Shmatikov used SVD + nearest-neighbor to link anonymized ratings to public IMDb profiles. 8 ratings → 99% match. | FTC complaint. Class-action (Doe v. Netflix, settled 2010). Netflix cancelled Prize sequel. |
| **NYC Taxi & Limousine Commission** | 2014 | MD5-hashed medallion numbers reversed (weak hash, small keyspace). Full trip data linked to specific drivers and passengers via paparazzi photos. | Revealed driver income, celebrity movements. Showed hashing ≠ anonymization when keyspace is small. |
| **Strava Global Heatmap** | 2018 | Aggregate "anonymous" fitness activity heatmaps revealed secret military base locations and patrol routes in conflict zones. | Pentagon banned fitness trackers in operational zones. Aggregate ≠ safe when population is small (few users in remote areas). |
| **Australian Medicare/PBS (Melbourne Uni)** | 2016 | Researchers re-identified patients in 10% sample of Australian medical records using date-of-service patterns + public info. | Dataset pulled. Triggered parliamentary inquiry. Government revised de-identification guidelines. |
| **Clearview AI (multiple jurisdictions)** | 2020– | Not a statistical attack but demonstrates the endpoint: enough auxiliary data (3B+ scraped photos) makes any visual anonymization breakable. | Fined in EU/UK/Australia. GDPR enforcement. Demonstrates that anonymization is relative to attacker's auxiliary knowledge. |

**Key point (below the table):** **Pattern:** Every major case follows the same structure — an organization assumes removing direct identifiers is sufficient, a researcher or journalist demonstrates linkage using publicly available auxiliary data, and the resulting lawsuit redefines what "anonymous" means legally.

## 4. Why It Keeps Working

The fundamental statistical reasons de-anonymization succeeds despite best efforts:

- **Curse of dimensionality (for privacy)** — Adding features makes records sparser and more unique. 15 binary attributes → 32,768 cells for a city of 50,000. Most cells have 0 or 1 occupants.
- **Auxiliary data is growing** — Social media, public records, data breaches all expand the attacker's side-channel. The defender must protect against all possible joins.
- **Composition attacks** — Multiple "safe" releases about overlapping populations compound. Each release reveals marginals; combined, they over-constrain the solution.
- **Human uniqueness is robust to noise** — Perturbation helps, but behavioral fingerprints survive moderate noise. A commute pattern with ±5 min jitter is still identifiable.

**Key point:** **Defender's dilemma:** You must anonymize against every possible auxiliary dataset — including ones that don't exist yet. The attacker only needs one successful linkage.

*Example: A dataset safe in 2020 may become re-identifiable in 2024 when a new breach provides the missing quasi-identifier.*

### Visualization (canvas `c3`, 720×380)

Conceptual line chart: attacker capability grows over time while static protection stays flat.

- **Title (bold 14px, `#1a5276`, top center):** "The Defender's Dilemma: Auxiliary Data Grows Over Time".
- **Plot area:** x=70, width = canvas−120, y=55, height 240; L-shaped axes `#ccc`; x label "Time since data release" (10px `#666`), y label "Re-identification risk" rotated −90°.
- **Risk curve:** red `#e74c3c` 2.5px bezier curve rising from 20% height at the left to ~95% at the right (control points: from (0, 0.8h) through (0.3w, 0.7h), (0.5w, 0.4h), (0.7w, 0.2h), then (0.85w, 0.1h), (0.95w, 0.06h) to (w, 0.05h) in plot-relative coordinates); area under it filled `rgba(231,76,60,0.08)`. Label near the curve (10px red): "Attacker capability (growing aux data)".
- **Protection line:** dashed (5/5) green `#27ae60` 1.5px horizontal line at 35% plot height, labeled (10px green): "Protection level (static anonymization)".
- **Crossover:** orange `#e67e22` 2px circle (radius 8) at x = 0.42 of plot width on the protection line, labeled below (10px orange): "Breach point".
- **Event markers on the x-axis** (small gray `#999` tick + dot, with 9px `#666` two-line labels below): at 0.2 "New breach / published"; at 0.5 "Social media / profile scraped"; at 0.75 "Public records / digitized".
- **Bottom caption (11px, `#888`, centered):** "Static anonymization degrades — attack surface only grows".

## 5. Implications for Data Practice

What statistical de-anonymization means for pipeline design:

- **Anonymization is not a one-time event** — Re-identification risk must be re-assessed as auxiliary data grows. Threat model is dynamic.
- **k-Anonymity is necessary but not sufficient** — It protects against linkage but not against homogeneity or background knowledge attacks.
- **Differential privacy is the only formal defense** — But ε must be chosen to reflect actual risk tolerance, and composition budgets deplete across releases.
- **Synthetic data is not immune** — If a generative model memorizes training examples (overfitting), synthetic records may exactly match real individuals.
- **Legal "anonymization" ≠ statistical anonymization** — Courts increasingly apply a "motivated intruder" test: could a determined person with reasonable resources re-identify?

**Philosophy callout:** **The honest position:** If your data has enough dimensions to be analytically useful, it probably has enough dimensions to be re-identifiable. The question isn't "is it anonymous?" but "how expensive is the attack?" — and that cost drops every year.

### Visualization (canvas `c4`, 720×380)

Horizontal bar chart: defense layers ranked by cost to attacker.

- **Title (bold 14px, `#1a5276`, top center):** "Defense Layers: Cost to Attacker".
- **Rows** (start y=52, 50px row height; labels 11px `#2c3e50` right-aligned at x=188; bars 24px tall from x=200, max width = canvas−340; fill = color at ~20% alpha (`color + '33'`), 1.5px stroke in color; centered "▮" block glyphs inside the bar (one per 20 effort points, 11px in color); bold 10px cost label in color at bar end):

| Defense layer | Cost to attacker | Effort (bar %) | Color |
|---|---|---|---|
| Remove names only | Minutes | 5 | `#e74c3c` |
| Suppress quasi-IDs | Hours | 20 | `#e67e22` |
| k-Anonymity (k=5) | Days | 40 | `#f39c12` |
| k-Anonymity (k=50) | Weeks | 60 | `#2980b9` |
| Differential privacy (ε=1) | Infeasible* | 90 | `#27ae60` |
| Aggregate-only release | Infeasible | 98 | `#1a5276` |

- **Footnote (10px, `#888`, left-aligned):** "* Infeasible given current auxiliary data; not immune to future quantum/ML advances".
- **Bottom caption (11px, `#888`, centered):** 'The question is not "is it anonymous?" but "how expensive is the attack?"'.

## Regeneration instructions

- **Template/layout:** backlog/kusto-style detail page. Body: h1 with inline `.status` badge, `.subtitle`, then `.card-section` divs each with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border). Sections 1, 2, 4, 5 use `table.layout` with one `<tr>`: left `td.text-col` (45%) with paragraph, `<ul>` bullets, `.key-point` or `.philosophy` callout and optional `.example` line; right `td.viz-col` (55%) with one canvas. Section 3 is a full-width `table.cases` followed by a `.key-point` callout.
- **Page CSS:** body system-ui/-apple-system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. `.key-point`: background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem. `.philosophy`: background `#f0f4f8`, left border 4px `#2980b9`. `.example`: italic `#555`. `.status` badge: background `#fef9e7`, border 1px `#f39c12`, text `#b7950b`. `table.cases`: th background `#1a5276` white text, td 8px 12px padding with `#eee` bottom border, even rows `#f8fafb`. Canvases: 1px `#e0e0e0` border, 4px radius, `width: 100%`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, amber `#f39c12`.
- **Canvas:** a shared `setup(id)` helper sizes each 720×380 backing store by `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates.
- No nav bar, no back/home links. This page has no outbound card links; any regenerated links elsewhere use `.html` extensions.
