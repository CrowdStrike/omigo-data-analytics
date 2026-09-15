# Statistical vs English Terminology

**Page type:** detail page (two-column bias-table layout: text left 50%, canvas right 50%, one h2 + table per term)
**HTML title tag:** Statistical vs English Terminology — Pitfalls

**Subtitle:** When everyday language collides with formal statistical meaning

## 1. "Normal"

**Tagline (red):** Not common, but completely okay

**Labeled bullets:**
- **English sense** (red label): "not normal" = deviant, broken, needs fixing — a negative judgment.
- **Statistical sense** (green label): the value simply has low probability under this distribution.
- **Still valid** (blue label): a rare outcome is a legitimate member of the sample space, not an error.
- **Green eyes** (blue label): roughly 2% of people — a tail value, not a medical condition.
- **By construction** (blue label): about 5% of perfectly healthy data sits beyond 2σ on either side.

**Example box (blue left border), labeled bullets:**
- **The damage** (orange label): patient hears "your result isn't normal" and imports the English connotation.
- **Mismatch** (orange label): the doctor spoke descriptively; the patient heard it normatively as "you're broken".

**Comparison box (green left border), labeled bullets:**
- **Correct framing** (green label): "3rd percentile — rare, but let's compare against clinical thresholds."
- **The rule** (green label): rare ≠ pathological, and infrequent ≠ wrong.

### Visualization (canvas `c1`, 720×300)

Bell-curve diagram: an eye-color distribution with the common center shaded green and the rare right tail shaded red.

- **Background:** full-canvas fill `#f9f9f9`.
- **Title (600 14px, `#1a5276`, at 20,25):** "Eye Color Distribution (Illustrative)".
- **Curve:** Gaussian drawn across the full width in `#2980b9`, width 2; center x=360, baseline y=240, sigma scale 120px, peak height 140px above baseline.
- **Center fill:** area under curve from −2σ to +2σ filled `rgba(39,174,96,0.2)` (green).
- **Right-tail fill:** area under curve from +2σ to the right edge filled `rgba(231,76,60,0.3)` (red) — right-tail mass beyond 2σ is 2.3%, matching the "~2%" label. The green ±2σ region is 95.4%, matching "~95%".
- **Labels:** green (`#27ae60`, 12px) near the peak: "Brown/Blue (~95%)" and "Common = Normal"; red (`#e74c3c`, 12px) beside the tail: "Green (~2%)" and "Rare, but healthy".
- **X-axis:** horizontal line `#333` (width 1.5) from x=60 to x=660 at the baseline, with 11px `#333` labels below: "← Common" (left) and "Rare →" (right).

## 2. "Probability"

**Tagline (red):** Set theory vs intuitive prediction

**Labeled bullets:**
- **English sense** (red label): an intuitive guess — "probably going to rain" has no calculation behind it.
- **Formal sense** (green label): a measure on a sample space Ω obeying the Kolmogorov axioms.
- **The axioms** (blue label): P(Ω) = 1, P(A) ≥ 0, and countable additivity over disjoint events.
- **Legitimate sources** (blue label): frequency counts, or a Bayesian posterior updated from a stated prior.
- **The ambiguity** (orange label): "70% chance this lifts retention" — from 100 tests, from a posterior, or from a hunch?
- **Usually the hunch** (orange label): a confidence expression dressed in numerical clothing.

**Example box (blue left border), labeled bullets:**
- **The damage** (orange label): stakeholders read "70% probability" as a rigorous, data-backed claim.
- **Consequence** (orange label): budget and roadmap priority get allocated against a number nobody can audit.

**Comparison box (green left border), labeled bullets:**
- **With a model** (green label): "P(success) = 0.7 given prior data" — and cite the data.
- **Without one** (green label): "I think this is likely to work, but that's a judgment call."
- **The rule** (green label): don't fake precision you cannot reproduce.

### Visualization (canvas `c2`, 720×300)

Side-by-side text-panel comparison: formal probability (left, green) vs intuitive "probability" (right, red).

- **Background:** full-canvas fill `#f9f9f9`.
- **Title (600 14px, `#1a5276`, at 20,25):** "Two Meanings of \"Probability\"".
- **Left panel (green):** heading in `#27ae60` 600 13px: "FORMAL (set theory)"; below it four 12px `#333` lines: "Sample space Ω", "Events = subsets", "Measure P: P(Ω)=1", "P(A) ≥ 0". Then a green-outlined rectangle (`#27ae60`, width 2, at 50,180 size 250×80) containing three 11px `#333` lines: "P(retention | feature) = 0.73", "from 100 A/B tests with similar features", "→ auditable, updateable, defensible".
- **Right panel (red):** heading in `#e74c3c` 600 13px: "INTUITIVE (gut feeling)"; below it four 12px `#333` lines: "\"Feels likely\"", "No calculation", "No prior", "No update rule". Then a red-outlined rectangle (`#e74c3c`, width 2, at 410,180 size 250×80) containing three 11px `#333` lines: "\"There's a 70% chance this works\"", "(source: vibes)", "→ can't audit, can't update, can't defend".

## 3. "Significant"

**Tagline (red):** p < α vs important

**Labeled bullets:**
- **English sense** (red label): important, meaningful, worth acting on.
- **Statistical sense** (green label): p < α — the result is unlikely under the null hypothesis.
- **What it measures** (blue label): detectability of a difference, not its magnitude or its value.
- **Trivial yet significant** (blue label): p = 0.001 on a 0.5% click lift is a real but negligible effect.
- **Sample-size effect** (orange label): with enough users, any non-zero difference crosses the p threshold.

**Example box (blue left border), labeled bullets:**
- **The damage** (orange label): "statistically significant improvement" is read as "important improvement".
- **Consequence** (orange label): a 0.5% lift ships and then costs more to maintain than it generates.

**Comparison box (green left border), labeled bullets:**
- **Correct framing** (green label): "Significant (p = 0.001) but only a 0.5% lift — below our 2% decision bar."
- **Always report** (green label): effect size and a confidence interval alongside every p-value.

### Visualization (canvas `c3`, 720×300)

Scatter diagram on effect-size vs sample-size axes: three annotated dots showing statistical vs practical significance quadrants.

- **Background:** full-canvas fill `#f9f9f9`.
- **Title (600 14px, `#1a5276`, at 20,25):** "Statistical vs Practical Significance".
- **Axes:** `#333` lines (width 1.5): x-axis from (80,250) to (680,250); y-axis from (80,250) to (80,60). Axis labels 11px `#333`: "Sample Size →" (bottom right) and "Effect Size" (top left).
- **Threshold line:** horizontal dashed green line (`#27ae60`, width 2, dash 5/3) at y=120, labeled in green 12px: "Practical significance threshold (2% lift)".
- **Dot 1 (bottom right, red):** circle radius 16 at (550,210), fill `rgba(231,76,60,0.5)`, stroke `#e74c3c` width 2; three red 11px labels to its right: "p=0.001 (stat sig)", "but effect=0.5%", "(not worth it)".
- **Dot 2 (top left, green):** circle radius 16 at (200,90), fill `rgba(39,174,96,0.5)`, stroke `#27ae60` width 2; three green 11px labels to its right: "p=0.08 (not sig)", "but effect=5%", "(collect more data)".
- **Dot 3 (top middle, blue sweet spot):** circle radius 18 at (450,85), fill `rgba(26,82,118,0.6)`, stroke `#1a5276` width 2; two blue (`#1a5276`) 12px labels above: "p=0.002 & effect=4%", "(both significant)".

## Regeneration instructions

To rebuild the HTML from this spec:

- **Layout:** single page, no cards. h1, `.subtitle`, then one section per term: `<h2 id="sN">N. "Term"</h2>` followed by a `table.bias-table` with a single `<tr>` — left `<td class="text-col">` (50%) holds `.tagline`, a `ul.points` list of labeled bullets, then an `.example` box and a `.comparison` box each containing their own `ul.points` list; right `<td class="viz-col">` (50%) holds the canvas.
- **Table style:** `table.bias-table` full width, border-collapse collapse, margin-bottom 48px; cells top-aligned with 12px padding (no cell borders).
- **Text styles:** `.tagline` weight 600, `#e74c3c`, 1.05em; `.description` margin-bottom 10px; `.example` background `#f8f9fa` with left border `3px solid #2980b9`, padding 12px 16px, 0.9em; `.comparison` background `#f0f4f8` with left border `3px solid #27ae60`, padding 12px 16px, 0.9em; `strong` in `#1a5276`.
- **Bullet styles:** `ul.points` margin `0 0 10px 20px`, 0.92em, color `#333`; `li` margin `7px 0`. Each bullet opens with a bold colored `<span>` label: `.lbl-en` red `#e74c3c` (English meaning), `.lbl-stat` green `#27ae60` (formal/correct), `.lbl-key` blue `#1a5276` (mechanism), `.lbl-warn` orange `#e67e22` (failure/consequence) — all weight 600. One bullet = one label + a phrase short enough not to wrap.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#555` 1.05rem, margin-bottom 40px; h2 1.4rem `#1a5276` with bottom border `2px solid #2980b9`, padding-bottom 8px, margin `40px 0 20px 0`.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"` and is displayed block at `width: 100%` via CSS; a shared `setup(id)` helper scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, gray text `#333`.
