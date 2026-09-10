# Profile the Entire Column as One Distribution

**Page type:** detail page (card-section layout: h2 section headings, two-column layout table with text left 45% / canvas right 55%)
**HTML title tag:** Profile the Entire Column as One Distribution

**Subtitle:** A mean across two clusters is fiction — it represents a state that never occurs

## The Anti-Pattern

Compute mean, std, shape across ALL data as one population. If data has gaps, spikes, or sub-populations, the global profile is meaningless.

**Key-point callout (red left border):** The computed mean may fall in a region where no data points exist — a phantom statistic describing nothing real.

**Domain examples:** (bold lead-in)

- **Troponin** — two clusters: healthy baseline vs. cardiac event
- **Capital gain** — spike at zero + continuous tail
- **Hemoglobin** — male + female sub-populations

### Visualization (canvas `c1`, 720×300)

Scatter of two separated clusters with a wrong global bell curve and a mean line falling in the empty gap.

- **Clusters:** 60 dots each (4px, fill `rgba(26,82,118,0.35)`, seeded pseudo-random for reproducibility, seed 42): Cluster A centered at screen (150, 180) with spread ±60 x / ±50 y; Cluster B centered at (500, 120) with spread ±65 x / ±45 y. Cluster labels "Cluster A" and "Cluster B" in `#1a5276` 12px centered below each cluster at the bottom.
- **Wrong global bell curve:** red dashed outline (`#e74c3c`, dash 6/4, width 2) spanning x=60 to 590, centered at x=325, width parameter 280, height 120, baseline at h−40 — one broad curve covering both clusters.
- **Mean line:** vertical dashed red line (dash 8/5, width 2) at x=325 — in the empty gap between the clusters — labeled at top in bold 13px red: "mean (in empty gap!)".

## The Design Pattern

**Step 1:** Where does data EXIST? (gaps, spikes, clusters) — value existence mapping.

**Step 2:** Split at structural boundaries.

**Step 3:** Profile each segment independently.

**Key-point callout (red left border):** Each segment gets its own mean, std, and shape — statistics that describe real observations.

### Visualization (canvas `c2`, 720×300)

Two separate clean bell curves, each with its own mean line and a green checkmark.

- **Left bell curve:** centered at screen x=180, sigma 60, height 150, stroke `#1a5276` width 2.5, fill `rgba(26,82,118,0.15)`; solid vertical mean line at center; bold 13px label below in `#1a5276`: "μ=20, σ=5"; bold 20px green `#27ae60` "✓" above the peak.
- **Right bell curve:** centered at x=520, sigma 75, height 130, stroke `#27ae60`, fill `rgba(39,174,96,0.15)`; solid mean line; label "μ=72, σ=8" in green; green "✓" above the peak.
- **Baseline:** thin gray `#ccc` line at y = h−60 from x=40 to w−40.

## Regeneration instructions

- **Layout:** anti-pattern-pairs detail page: h1 with 2px `#2980b9` bottom border, `.subtitle`, then two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"), each an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) above a `table.layout` with one row: `td.text-col` (45%) and `td.viz-col` (55%).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `ul` 0.92rem; `.example` italic `#555` 0.9rem. No nav bar, no back/home links.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and `ctx.scale` back to logical coordinates via a shared `setup(id)` helper.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
