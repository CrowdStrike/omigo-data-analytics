# Correlation Presented as Causation

**Page type:** detail page (two-column obj-table layout: text left 40%, canvases right 60%, single section)
**HTML title tag:** Correlation Presented as Causation — Pseudoscience in Data Analysis

**Subtitle:** Actionable conclusions from observational data without experimental validation

## Section 1: "Data Shows X Correlates with Y — Therefore DO X to Get Y"

- **Health:** "People who eat blueberries have lower cancer rates" → "Eat blueberries to prevent cancer!" But blueberry eaters also exercise, don't smoke, have healthcare access, and are wealthier — the blueberry is a marker of a healthy lifestyle, not the cause of health.
- **Business:** "Companies that do stand-ups grow 2× faster." Fast-growing companies adopt trendy practices, so growth → stand-ups, not stand-ups → growth — cargo cult thinking copies the ritual and expects the outcome.
- **Tech:** "Teams using Kubernetes deploy 3× faster." Teams with the resources and talent to adopt K8s are already high-performing — K8s marks team capability, and handing it to a struggling team won't make them 3× faster.
- **Education:** "Students who take AP courses get into better colleges — so put everyone in AP!" Students who self-select into AP are already high-performing; the AP class didn't make them smart, smart kids chose AP.

**Why it's pseudoscience:** Actionable conclusions from observational data without experimental validation — the intervention "do X" was never tested independently of the selection effect that people who already do X differ from those who don't.

### Visualization (canvas `c1`, 720×340)

Side-by-side comparison boxes: the claimed causal direction vs the actual reversed direction.

- **Title (bold 17px, `#1a5276`, top center):** ""People Who Do X Succeed" ≠ "Doing X Causes Success"".
- **Left box (red):** rectangle at (40,50), 310×160, fill `rgba(231,76,60,0.1)`, stroke `#e74c3c` width 2. Header bold 17px `#e74c3c` centered at x=195: "What they CLAIM:". Body lines in 17px `#333`, centered, at y=100/125/150/175: "Blueberries → Health", "Stand-ups → Growth", "K8s → Fast deploys", "AP courses → Good college".
- **Right box (green):** rectangle at (380,50), 310×160, fill `rgba(39,174,96,0.1)`, stroke `#27ae60` width 2. Header bold 17px `#27ae60` centered at x=535: "What's actually happening:". Body lines in 17px `#333`, centered, at y=100/125/150/175: "Wealth → blueberries + health", "Growth → adopt stand-ups", "Good team → adopt K8s + fast", "Smart kids → choose AP".
- **Bottom annotation (bold 17px `#e74c3c`, centered):** "Direction is REVERSED. The success causes the practice, not the other way around."

### Visualization (canvas `c2`, 720×300)

Three causal DAGs (directed acyclic graphs) drawn side by side, each producing the same observed correlation.

- **Title (bold 17px Arial, `#1a5276`, top center):** "Three Possible Causal Structures (All Produce the Same Correlation)".
- **Node style:** circles radius 22, fill `rgba(26,82,118,0.2)` (Z node uses `rgba(230,126,34,0.25)`), stroke `#1a5276` width 2, bold 17px Arial labels centered in nodes.
- **Arrow style:** red (`#e74c3c`) width 2.5 with filled triangular arrowheads, trimmed at node radii.
- **DAG 1 (left third, node row at y=90):** X node and Y node 80px apart, arrow X→Y. Caption in 16px `#2c3e50` below: "X causes Y" / "(what they claim)".
- **DAG 2 (middle third):** orange Z node above at y=50, X and Y below at y=115, arrows Z→X and Z→Y. Caption: "Confound Z causes both".
- **DAG 3 (right third):** X and Y nodes 80px apart, arrow Y→X (pointing left). Caption: "Reverse causation" / "(Y causes X)".
- **Bottom annotation (bold 16px Arial, `#c0392b`, centered):** "You CANNOT distinguish these from correlation alone. You need an experiment."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one full-width table with a single `<tr>`; left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + trailing `<p>` paragraph, right `<td>` (60%, centered) holds both canvases (`c1` then `c2`) stacked.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Each chart drawn in its own IIFE. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, dark red `#c0392b`, orange `#e67e22` (confound node tint `rgba(230,126,34,0.25)`), grays `#666`/`#333`/`#2c3e50`.
