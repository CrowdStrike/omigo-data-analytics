# Anonymization

**Page type:** detail page (backlog-style sections: 2-col text/viz layout table per section, one full-width canvas section, one full-width comparison table; BACKLOG status badge next to h1)
**HTML title tag:** Anonymization

**Status badge (in h1):** BACKLOG

**Subtitle:** The gap between "we removed the name" and actual unidentifiability — and why naive anonymization is a false promise.

**Intro callout:** Removing the name field is not anonymization. Quasi-identifiers, behavioral fingerprints, and pipeline side-channels re-identify individuals long after PII is stripped — and every real fix trades directly against analytic utility.

## 1. The Anonymization Spectrum

Anonymization is not binary. It's a spectrum of techniques with different privacy-utility tradeoffs:

- **Pseudonymization** — Replace identifiers with tokens. Reversible with a key. Not anonymous under GDPR.
- **Generalization** — Reduce precision (exact age → age range, ZIP → region). Loses granularity.
- **Suppression** — Remove fields entirely. Loses the dimension.
- **Perturbation** — Add noise to values. Preserves distribution shape but individual records are unreliable.
- **Differential privacy** — Mathematical guarantee on re-identification risk. Calibrated noise injection.

**Key point:** Pseudonymization is NOT anonymization. If you can link back to a person (even probabilistically), it's still personal data under most regulations.

### Visualization (canvas `c1`, 720×340)

Scatter plot: privacy guarantee (x) vs data utility (y) for six techniques, with a dashed diagonal trend line.

- **Title (bold 13px, `#1a5276`, top center):** "Privacy-Utility Tradeoff Spectrum".
- **Plot area:** x=80, y=50, width = canvas−140, height = canvas−100; L-shaped axes `#ccc` 1px. Axis labels (10px `#666`): "Privacy Guarantee →" centered below; "Data Utility →" rotated −90° at the left.
- **Points** (radius 7 filled circles with 10px labels 14px above, both in the point color):

| Technique | Privacy | Utility | Color |
|---|---|---|---|
| Raw PII | 0 | 100 | `#e74c3c` |
| Pseudonymized | 20 | 95 | `#e67e22` |
| k-Anonymity | 50 | 65 | `#f39c12` |
| l-Diversity | 65 | 50 | `#2980b9` |
| Differential Privacy | 85 | 35 | `#27ae60` |
| Full Suppression | 100 | 0 | `#1a5276` |

- **Trend line:** dashed (4/4) 1px `#999` diagonal from (privacy 0, utility 100) to (privacy 100, utility 0).

## 2. Re-identification Risk

The fundamental threat: combining "anonymous" fields to uniquely identify individuals.

- **Quasi-identifiers** — ZIP code + birth date + gender uniquely identifies 87% of the US population (Sweeney, 2000).
- **Linkage attacks** — Join "anonymized" dataset with public records (voter rolls, social media) on shared quasi-identifiers.
- **Temporal patterns** — Even without names, unique behavioral sequences (commute pattern, purchase timing) are fingerprints.
- **Auxiliary information** — Netflix Prize de-anonymization: just a few movie ratings + approximate dates → identity.

**Philosophy callout:** **Sweeney's law:** The more dimensions you keep for utility, the fewer records share the same combination — and the easier re-identification becomes. Anonymization and utility are in direct tension.

### Visualization (canvas `c2`, 720×340)

Horizontal funnel bar chart: population narrowing as quasi-identifiers are added.

- **Title (bold 13px, `#1a5276`, top center):** "Quasi-Identifier Uniqueness (Sweeney)".
- **Bars:** four bars 48px tall with 14px gaps, starting y=55; bar origin x=130, max width = canvas−160; fill `rgba(231,76,60, alpha)` where alpha = 0.2 + i×0.25, 1px stroke `#e74c3c`. Left labels 11px `#2c3e50` right-aligned; right counts bold 11px `#e74c3c`:

| Stage | Count | Relative width |
|---|---|---|
| US Population | 330M | 0.95 |
| + Gender | 165M | 0.48 |
| + Birth Year | 2.1M | 0.12 |
| + ZIP Code | ~1-2 people | 0.01 |

- **Funnel lines:** 1px `#ccc` connectors from each bar's right end to the next bar's right end.
- **Bottom caption (11px, `#888`, centered):** "3 fields → 87% of Americans uniquely identifiable".

## 3. Identifier Tiers

Three tier callout boxes in the text column:

- **PRIMARY** (red tier, tag `PRIMARY`): Identifies alone. One field = one person. Example tags: Full Name, Email, Phone, Passport #.
- **SECONDARY** (blue tier, tag `SECONDARY`): Harmless alone. 2-3 together → unique. Example tags: ZIP Code, Birth Date, Gender, Employer, Job Title.
- **TERTIARY** (orange tier, tag `TERTIARY`): Not PII. But so rare they identify by scarcity. Example tags: Rare Disease, Unusual Hobby Combo, Niche Purchase, Unique Schedule.

**Key point:** Most orgs strip PRIMARY and call it done. The SECONDARY + TERTIARY combo is what re-identifies. (The PRIMARY/SECONDARY/TERTIARY words render as small inline tag pills in red/blue/orange.)

### Visualization (canvas `c5`, 720×380)

Concentric-rings diagram of identifier tiers plus a small population-funnel text column on the right.

- **Title (bold 13px, `#1a5276`, top center):** "Anonymity Set Narrowing".
- **Rings** centered at (0.42×width, height/2+12), drawn largest first:
  - TERTIARY — radius 140, fill `rgba(230,126,34,0.12)`, stroke `#e67e22` 2.5px; label bold 10px `#e67e22` "TERTIARY" near the ring's top, sub-label 9px `#b7770a` "rare = unique".
  - SECONDARY — radius 90, fill `rgba(41,128,185,0.18)`, stroke `#2980b9`; label bold 10px `#2980b9` "SECONDARY", sub-label 9px `#1a5276` "2-3 fields = unique".
  - PRIMARY — radius 38, fill `rgba(231,76,60,0.22)`, stroke `#e74c3c`; label bold 10px `#e74c3c` "PRIMARY" at center, sub-label 9px `#922b21` "1 field = unique".
- **Right-side funnel text** (10px, left-aligned at x = center+170, starting y=60, 50px spacing): "330M people" (`#999`), "↓ tertiary → 1" (`#e67e22`), "↓ secondary combo → 1-3" (`#2980b9`), "↓ primary → 1" (`#e74c3c`).
- **Bottom caption (10px, `#888`, centered):** "Any tier can resolve alone — they don't need to combine".

## 4. Before / After: John Smith's Record

Full-width section: a single canvas, no text column.

### Visualization (canvas `c6`, 1400×420, styled `width:100%; height:420px`)

Two-column before/after record table rendered on canvas with technique badges and tier color bars.

- **Title (bold 14px, `#1a5276`, top center):** 'John Smith — Before & After "De-identification"'.
- **Column headers (bold 12px, `#2c3e50`):** "BEFORE (Raw Record)" centered over the left half; "AFTER (Released Dataset)" centered over the right half. Dashed (4/4) 2px `#ccc` vertical divider at midline.
- **Legend (10px, near top right, three per row):** STRIPPED `#8e44ad`, PSEUDONYMIZED `#2980b9`, MASKED `#16a085`, OBFUSCATED `#e67e22`, KEPT (⚠) `#e74c3c` — each a 10px color square + label.
- **Rows** (start y=72, 32px row height, alternate rows shaded `#f9fafb`; a 4px-wide tier color bar at the left edge of each row; field name bold 11px `#2c3e50`; before/after values 11px monospace `#555` — after-value red `#e74c3c` when technique is KEPT; technique shown as a rounded-rect badge (radius 3, fill = technique color at ~9% alpha `color + '18'`, 1px stroke in technique color, bold 9px text in technique color)):

| Field | Before | After | Technique (color) | Tier bar color |
|---|---|---|---|---|
| Name | John Smith | — | STRIPPED (`#8e44ad`) | `#e74c3c` |
| Email | jsmith@gmail.com | user_8a3f@anon | PSEUDONYMIZED (`#2980b9`) | `#e74c3c` |
| Phone | 617-555-0142 | 617-555-**** | MASKED (`#16a085`) | `#e74c3c` |
| DOB | 1971-06-14 | 1970-1975 | OBFUSCATED (`#e67e22`) | `#2980b9` |
| Gender | Male | Male | KEPT (`#e74c3c`) | `#2980b9` |
| ZIP | 02138 | 021** | MASKED (`#16a085`) | `#2980b9` |
| Employer | MIT | Education Sector | OBFUSCATED (`#e67e22`) | `#2980b9` |
| Diagnosis | Rare Enzyme Disorder | Rare Enzyme Disorder | KEPT (`#e74c3c`) | `#e67e22` |
| Visit Pattern | Tuesdays, 3x in Q4 | Tuesdays, 3x in Q4 | KEPT (`#e74c3c`) | `#e67e22` |

- **Bottom callout (11px, red `#e74c3c`, centered):** '⚠ KEPT fields: "Rare Enzyme Disorder" + "Tuesdays Q4" + "Male" → only 1 person in ZIP 021**'.

## 5. Techniques Comparison

Full-width comparison table (`table.compare`):

| Technique | Privacy Guarantee | Utility Loss | Reversible? |
|---|---|---|---|
| **Pseudonymization** | None (with key access) | None | Yes — with mapping table |
| **k-Anonymity** | Every record matches ≥k others on quasi-identifiers | Moderate (generalization) | No (but homogeneity attacks exist) |
| **l-Diversity** | Each group has ≥l distinct sensitive values | Higher (more suppression) | No (but skewness attacks exist) |
| **t-Closeness** | Group distribution ≈ population distribution | High | No |
| **Differential Privacy** | ε-bounded information leakage per query | Calibrated to ε (tunable) | No — mathematical guarantee |

## 6. The Pipeline Problem

Anonymization at the wrong point in the pipeline creates false safety:

- **Late anonymization** — Raw PII flows through 5 services before anonymization. Every intermediate store is a risk surface.
- **Partial anonymization** — Remove name but keep timestamp + location + device ID. The combination is still unique.
- **Feature engineering leakage** — ML features derived from PII carry identity signal even after PII deletion. The model remembers.
- **Backup/log contamination** — Production data is anonymized, but raw PII lives in backups, debug logs, error reports.

**Key point:** Anonymization is a property of the *entire system*, not a transformation applied to one table. If PII exists anywhere in the pipeline — including derived features, logs, and backups — the data is not anonymous.

### Visualization (canvas `c3`, 720×340)

Vertical pipeline diagram with risk annotations and side-channel leaks.

- **Title (bold 13px, `#1a5276`, top center):** "PII Leakage Surfaces in a Typical Pipeline".
- **Stage boxes:** 120px wide × 36px tall at x = 0.35×width, connected by 1px `#ccc` vertical lines; risk stages fill `rgba(231,76,60,0.1)` with red `#e74c3c` stroke, safe stages fill `rgba(39,174,96,0.1)` with green `#27ae60` stroke; 11px `#2c3e50` centered labels:

| Stage | y | Risk annotation (red, right side) |
|---|---|---|
| Ingest | 60 | ⚠ Raw PII |
| Transform | 110 | ⚠ Intermediate stores |
| Features | 160 | ⚠ Derived identity signal |
| Anonymize | 210 | (none — safe) |
| Serve | 260 | (none — safe) |

- **Side leaks (left side):** dashed (3/3) orange `#e67e22` lines from Ingest/Transform/Features to right-aligned 10px orange labels: "Debug logs", "Backups", "Model weights".
- **Bottom caption (11px, `#888`, centered):** "Anonymizing at stage 4 doesn't erase stages 1-3 or their side-channels".

## 7. When Anonymization Fails Analytics

Aggressive anonymization destroys the signals analysts need:

- **Cohort analysis** — Can't track user journeys without a stable user-level identifier.
- **Deduplication** — Can't detect same-person across sessions without linkable IDs.
- **Longitudinal studies** — Can't measure change over time without re-identification (even pseudonymous).
- **Outlier detection** — Noise injection masks the very anomalies you're looking for.

**Philosophy callout:** **The honest framing:** Most analytics pipelines need pseudonymization (reversible, access-controlled), not true anonymization. The regulatory question is access control and purpose limitation — not mathematical unidentifiability.

### Visualization (canvas `c4`, 720×300)

Grouped horizontal bar chart: capability retained under true anonymization vs pseudonymization.

- **Title (bold 13px, `#1a5276`, top center):** "What Anonymization Breaks for Analytics".
- **Legend (10px):** red swatch `#e74c3c` "True anonymization"; green swatch `#27ae60` "Pseudonymization".
- **Rows** (start y=48, 44px row height; labels 11px `#2c3e50` right-aligned at x=130; per row an anon bar `rgba(231,76,60,0.5)` (12px tall) above a pseudo bar `rgba(39,174,96,0.5)` (12px tall, 16px offset), 9px percentage labels in red/green at bar ends):

| Capability | True anonymization | Pseudonymization |
|---|---|---|
| Cohort tracking | 10% | 95% |
| Deduplication | 5% | 90% |
| Longitudinal study | 0% | 85% |
| Outlier detection | 30% | 80% |
| Aggregate stats | 75% | 95% |

- **Bottom caption (11px, `#888`, centered):** "Most analytics needs are met by pseudonymization + access control, not full anonymization".

## Regeneration instructions

- **Template/layout:** backlog detail page. Body: h1 with inline `.status` badge, `.subtitle`, `.intro` callout, then `.lang-section` blocks each with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border). Sections 1, 2, 3, 6, 7 use `table.layout` (left `td.text-col` 45% text, right `td.viz-col` 55% canvas); section 3's text column uses `.tier` boxes (`.tier-primary` red / `.tier-secondary` blue / `.tier-tertiary` orange: light tinted background `rgba(color,0.06)` with 4px left border) containing a `.tier-tag` pill and `.tag` example pills; section 4 is a full-width 1400×420 canvas; section 5 is a full-width `table.compare`.
- **Page CSS:** body system-ui/-apple-system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. `.intro`/`.philosophy`: background `#f0f4f8`, left border 3px `#2980b9`. `.key-point`: background `#f8f9fa`, left border 3px `#e74c3c`. `.status` badge: background `#fef9e7`, border 1px `#f39c12`, text `#b7950b`. Tag pill colors: `.tag-red` background `#fdecea` text `#c0392b` border `#e74c3c`; `.tag-blue` background `#eaf2f8` text `#1a5276` border `#2980b9`; `.tag-orange` background `#fef5e7` text `#b7770a` border `#e67e22`. `table.compare`: th background `#1a5276` white text, even rows `#f8fafb`. Canvases: 1px `#e0e0e0` border, 4px radius, `width: 100%`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, purple `#8e44ad`, teal `#16a085`, amber `#f39c12`.
- **Canvas:** a shared `setupCanvas(id)` helper multiplies the intrinsic width/height by `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates; canvas c6 is sized inline (1400×420 with fixed 420px CSS height).
- No nav bar, no back/home links. This page has no outbound card links; any regenerated links elsewhere use `.html` extensions.
