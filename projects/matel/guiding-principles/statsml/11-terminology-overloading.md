# Statistical vs English Terminology

**Page type:** detail page (two-column bias-table layout: text left 45%, canvas right 55%, one h2 + table per term)
**HTML title tag:** Statistical vs English Terminology — Pitfalls

**Subtitle:** When everyday language collides with formal statistical meaning

## 1. "Normal"

**Tagline (red):** Not common, but completely okay

In **English**, "not normal" means something is wrong with you — deviant, broken, needs fixing. A negative judgment. In **statistics**, "not normal" just means the value has low probability under this distribution — but it's still a valid, healthy outcome in the sample space.

Having green eyes is not normal (rare, ~2% of population). That doesn't mean green eyes are a medical condition. It means they sit in the tail of the distribution. 5% of perfectly healthy data lives beyond 2σ by definition.

**Example box (blue left border):**
**The damage:** Someone hears "your test result isn't normal" and imports the English connotation — *something is wrong*. The doctor just meant "this sits in a low-probability region" (descriptive). The patient hears "you're broken" (normative).

**Comparison box (green left border):**
**Correct framing:** "Your result is in the 3rd percentile — rare, but not necessarily a problem. Let's compare to clinical thresholds." Rare ≠ pathological. Infrequent ≠ wrong.

### Visualization (canvas `c1`, 720×300)

Bell-curve diagram: an eye-color distribution with the common center shaded green and the rare right tail shaded red.

- **Background:** full-canvas fill `#f9f9f9`.
- **Title (600 14px, `#1a5276`, at 20,25):** "Eye Color Distribution (Illustrative)".
- **Curve:** Gaussian drawn across the full width in `#2980b9`, width 2; center x=360, baseline y=240, sigma scale 120px, peak height 140px above baseline.
- **Center fill:** area under curve from −2σ to +2σ filled `rgba(39,174,96,0.2)` (green).
- **Right-tail fill:** area under curve from +2.5σ to the right edge filled `rgba(231,76,60,0.3)` (red).
- **Labels:** green (`#27ae60`, 12px) near the peak: "Brown/Blue (~95%)" and "Common = Normal"; red (`#e74c3c`, 12px) beside the tail: "Green (~2%)" and "Rare, but healthy".
- **X-axis:** horizontal line `#333` (width 1.5) from x=60 to x=660 at the baseline, with 11px `#333` labels below: "← Common" (left) and "Rare →" (right).

## 2. "Probability"

**Tagline (red):** Set theory vs intuitive prediction

In **English**, "probability" means an intuitive guess about likelihood — "probably going to rain" has no formal calculation behind it. In **statistics/math**, probability is grounded in set theory (Kolmogorov axioms: sample space Ω, events as subsets, probability measure P satisfying P(Ω)=1, P(A)≥0, countable additivity) OR frequency data OR Bayesian updating from priors.

When someone says "there's a 70% chance this feature improves retention," are they saying (a) 70 of 100 similar tests showed positive results, (b) their Bayesian posterior given priors and evidence is 0.7, or (c) it just feels likely? Most of the time: (c). That's not probability — it's a confidence expression dressed in numerical clothing.

**Example box (blue left border):**
**The damage:** Business stakeholders hear "70% probability" and treat it as a rigorous, data-backed claim. They allocate budget and roadmap priority. The number came from intuition, not a probability model. No one can audit it, update it with new evidence, or defend it under scrutiny.

**Comparison box (green left border):**
**Correct framing:** If you have a model: "P(success) = 0.7 given prior data [cite]." If you don't: "I estimate this is likely to work, but that's a judgment call, not a calculated probability." Don't fake precision.

### Visualization (canvas `c2`, 720×300)

Side-by-side text-panel comparison: formal probability (left, green) vs intuitive "probability" (right, red).

- **Background:** full-canvas fill `#f9f9f9`.
- **Title (600 14px, `#1a5276`, at 20,25):** "Two Meanings of \"Probability\"".
- **Left panel (green):** heading in `#27ae60` 600 13px: "FORMAL (set theory)"; below it four 12px `#333` lines: "Sample space Ω", "Events = subsets", "Measure P: P(Ω)=1", "P(A) ≥ 0". Then a green-outlined rectangle (`#27ae60`, width 2, at 50,180 size 250×80) containing three 11px `#333` lines: "P(retention | feature) = 0.73", "from 100 A/B tests with similar features", "→ auditable, updateable, defensible".
- **Right panel (red):** heading in `#e74c3c` 600 13px: "INTUITIVE (gut feeling)"; below it four 12px `#333` lines: "\"Feels likely\"", "No calculation", "No prior", "No update rule". Then a red-outlined rectangle (`#e74c3c`, width 2, at 410,180 size 250×80) containing three 11px `#333` lines: "\"There's a 70% chance this works\"", "(source: vibes)", "→ can't audit, can't update, can't defend".

## 3. "Significant"

**Tagline (red):** p < α vs important

In **English**, "significant" means important, meaningful, worth paying attention to. In **statistics**, "significant" means p < α (the observed result is unlikely under the null hypothesis) — a statement about statistical detectability, not magnitude or importance.

You can have p = 0.001 (highly significant) for a completely trivial effect: "Users who saw the new button clicked 0.02% more often." With a large enough sample, you'll detect any non-zero difference as "statistically significant" even if it's too small to matter in practice.

**Example box (blue left border):**
**The damage:** Researcher reports "statistically significant improvement" in a paper or presentation. Stakeholders hear "important improvement" and launch the feature. The effect size is 0.5% lift — costs more to maintain than it generates. Statistical significance ≠ practical significance.

**Comparison box (green left border):**
**Correct framing:** "Statistically significant (p=0.003) but small effect size (0.5% lift). Below our 2% decision threshold — not worth launching." Always report effect size + confidence interval alongside p-value.

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

- **Layout:** single page, no cards. h1, `.subtitle`, then one section per term: `<h2 id="sN">N. "Term"</h2>` followed by a `table.bias-table` with a single `<tr>` — left `<td class="text-col">` (45%) holds `.tagline`, two `.description` paragraphs, one `.example` box, one `.comparison` box; right `<td class="viz-col">` (55%) holds the canvas.
- **Table style:** `table.bias-table` full width, border-collapse collapse, margin-bottom 48px; cells top-aligned with 12px padding (no cell borders).
- **Text styles:** `.tagline` weight 600, `#e74c3c`, 1.05em; `.description` margin-bottom 10px; `.example` background `#f8f9fa` with left border `3px solid #2980b9`, padding 12px 16px, 0.9em; `.comparison` background `#f0f4f8` with left border `3px solid #27ae60`, padding 12px 16px, 0.9em; `strong` in `#1a5276`.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#555` 1.05rem, margin-bottom 40px; h2 1.4rem `#1a5276` with bottom border `2px solid #2980b9`, padding-bottom 8px, margin `40px 0 20px 0`.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"` and is displayed block at `width: 100%` via CSS; a shared `setup(id)` helper scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, gray text `#333`.
