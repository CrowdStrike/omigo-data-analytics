# Tribal Knowledge Monopoly

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Tribal Knowledge Monopoly — Common Bad Practices

**Subtitle:** Institutional Risk — Critical pipeline logic, schema semantics, and model configurations held exclusively in one person's memory. When that person is unavailable, teams lose weeks to months reverse-engineering what should have been documented.

## The Practice

- The training pipeline has 47 undocumented edge cases. Only one person knows them. Feature X requires a specific join order or it silently duplicates rows — undocumented.
- Column `user_status` has 14 values. Only values 2, 5, 11 mean "active" — the rest are historical artifacts. No documentation anywhere.
- When someone asks, the answer comes verbally or in ephemeral chat — never committed to a searchable document. The knowledge remains centralized in one individual.

## The ML Model Variant

- Model training requires hyperparameters tuned over months of experimentation. The final configuration was never committed — it exists only in a local notebook.
- Feature preprocessing has 6 transformations applied in a specific order. Change the order and model accuracy drops 4%. Documented nowhere.
- Anyone attempting to retrain, improve, or debug the model must schedule time with this individual. They are consistently "unavailable this week."

## The Data Schema Variant

- Field names are cryptic (`evt_tp`, `usr_sg3`, `flg_a`). Only the schema designer knows what they mean.
- Some fields have overloaded semantics: `amount` means USD in table A, cents in table B, and units-sold in table C. No single source of truth exists.
- New team members spend weeks reverse-engineering the schema. Questions are answered verbally, never with documentation updates.

## The Production Debugging Variant

- Model performance drops. Logs are sparse (by design). Only one person can diagnose production issues because they know the undocumented failure modes.
- "Let me investigate" — hours later a fix appears. No one else could have resolved it. The system remains opaque by default.

**Why it persists:** Knowledge centralization is often rewarded. The individual becomes the "go-to person," receives high impact scores, and is deemed "critical to the team." Performance reviews reward being a bottleneck because it reads as being essential. The incentive structure rewards opacity.

**The indicator:** Bus factor = 1 by design, not by accident. If the person took a 2-week vacation and nothing broke, the knowledge was broadly held. If everything fails when they are unavailable, the knowledge was monopolized.

**The data cost:** In ML/data work, undocumented knowledge is uniquely dangerous. Wrong preprocessing = months of model training invalidated. Wrong join logic = silently corrupted datasets used for business decisions. The blast radius is measured in months, not hours.

### Visualization (canvas `c1`, 720×360)

Incident MTTR timeline: per-incident resolution time over 12 months, with the knowledge-holder's absence window shaded — flat and low while Person A is present, a dramatic spike while they are on leave. Background `#f9f9f9`. All data deterministic (hardcoded arrays, no randomness).

- **Title (bold 14px `#1a5276`, top center):** "Incident Resolution Time (MTTR) Over 12 Months".
- **Layout/axes:** margins left 55, right 20, top 45, bottom 50. Y axis 0–130 hours: light `#ddd` gridlines at 0/25/50/75/100/125 with 11px `#666` labels, rotated 11px `#666` axis title "Hours to resolve". X axis months 0–12, tick labels "1"–"12" (11px `#666`). Axis lines `#999`.
- **Absence window:** vertical band from month 6.5 to 8.5, fill `rgba(231,76,60,0.10)`, dashed `#e74c3c` edge lines; bold 12px red label "Person A on leave" centered above the band (y = 40).
- **Incidents (hardcoded `[month, hours]` pairs, filled dots r=5):**
  - Person A present, green `#27ae60`: [0.3,4], [0.8,6], [1.2,3], [1.7,5], [2.1,4], [2.6,7], [3.0,3], [3.5,5], [3.9,6], [4.4,4], [4.8,5], [5.3,3], [5.7,6], [6.2,4], [8.7,9], [9.2,5], [9.6,4], [10.1,6], [10.5,4], [11.0,5], [11.5,3]
  - During absence, red `#e74c3c`: [6.7,52], [7.0,88], [7.4,120], [7.8,96], [8.2,70]
- **Mean lines (dashed, width 1.5):** green `#27ae60` at 4.8h across the present spans (months 0–6.5 and 8.5–12), bold 11px green label "MTTR ≈ 4.8h" above the left segment; red `#e74c3c` at 85.2h across the absence window, bold 11px red label "MTTR ≈ 85.2h" centered below the line.
- **Legend (top-left inside plot, 11px `#333`, two stacked rows):** green dot "Incident (Person A present)", red dot "Incident (Person A away)".
- **Insight annotation (bold 13px `#e74c3c`, two lines right-aligned just left of the band):** "MTTR jumps 4.8h → 85.2h" / "the moment Person A leaves".
- **Caption (italic 12px `#555`, bottom center):** "Flat for months — until the one knowledge-holder takes two weeks off."

### Visualization (canvas `c2`, 720×320)

Horizontal bar chart of recovery cost when the hoarder is unavailable. Background `#f9f9f9`.

- **Title (bold 14px `#1a5276`, top center):** "Cost When the Hoarder Is Unavailable".
- **Layout:** margins left 180, right 40, top 50; bar height 40, gap 18; bar width = value × available width (values normalized to the 2-month bar = 1.0).
- **Bars (label / time / relative value / fill):**
  - "Software bug (documented)" — "2 hours", 0.02, `#27ae60`
  - "Software bug (undocumented)" — "2 days", 0.08, `#e67e22`
  - "Data pipeline (documented)" — "1 day", 0.04, `#27ae60`
  - "Data pipeline (undocumented)" — "3 weeks", 0.6, `#e74c3c`
  - "Model retraining (undocumented)" — "2 months", 1.0, `#922b21`
- **Bar styling:** stroke `#333` width 1; row labels 11px `#1a5276` right-aligned left of bars; time labels bold 11px — white inside the bar if the bar is >50px wide, otherwise `#333` just right of the bar.
- **Caption (italic 12px `#555`, bottom center):** "Undocumented ML/data knowledge: weeks-to-months recovery. Software: hours-to-days."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title` headings + bullet lists (with inline `<code>` for field names) + closing `<p><strong>` paragraphs, right `<td>` (60%, centered) holds the two canvases stacked.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276` (subsequent ones get inline `margin-top:14px`); `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display:block; margin:0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark red `#922b21`, gray text `#666`/`#555`/`#333`.
- **Links:** this page has no card links; in regenerated HTML any card links elsewhere use `.html` extensions.
