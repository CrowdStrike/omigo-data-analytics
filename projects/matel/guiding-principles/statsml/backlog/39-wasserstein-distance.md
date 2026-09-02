# Wasserstein Distance

**Page type:** detail page (backlog-style two-column layout table: text left 45%, canvas right 55%, one `.lang-section` per numbered h2)
**HTML title tag:** Wasserstein Distance

**Subtitle:** Earth Mover's Distance for distribution comparison

**Intro callout (blue accent):** The minimum cost to move probability mass from one distribution to another. Symmetric, defined even for non-overlapping supports, and it captures the magnitude of a shift — not just its existence. Stub — to be expanded.

## 1. Core Idea

**Problem:** KL divergence is asymmetric and undefined when supports don't overlap. KS test only measures maximum pointwise difference. Neither captures the "work" needed to transform one distribution into another.

**Wasserstein (Earth Mover's) Distance:** Minimum cost to move probability mass from distribution P to distribution Q. Intuition: how much "dirt" do you need to move, and how far?

- Symmetric (W(P,Q) = W(Q,P))
- Defined even for non-overlapping supports
- Captures magnitude of shift, not just existence
- Meaningful for drift detection: small shift vs. catastrophic change

**Key point (red-accent callout):** **Use cases:** Drift quantification, pos/neg class separation strength, comparing distribution fits.

### Visualization (canvas `c1`, 720×300)

Paired-bar histogram of two overlapping distributions with a "move mass" arrow.

- **Title (bold 16px, top center, `#222`):** "Wasserstein Distance: \"Cost\" to Transform P → Q"
- **Data (12 bins each, scale max 44):**
  - P: `[2, 8, 18, 30, 38, 42, 38, 28, 16, 8, 3, 1]` — fill `rgba(41, 128, 185, 0.35)`
  - Q: `[1, 3, 6, 10, 16, 24, 34, 40, 38, 28, 14, 5]` — fill `rgba(231, 76, 60, 0.35)`
- **Layout:** bars start at x=60, bin width = (width−120)/12; each bin split into two half-width bars (P left half, Q right half); baseline at y = height−70; bar height = (v/44) × (height−130).
- **Arrow:** orange `#e67e22`, width 2, horizontal from bin 4 to bin 7 at a height corresponding to value 30, minus 15px; filled triangular arrowhead; 14px centered label above it: "Move mass →".
- **Legend (top right):** swatch `rgba(41,128,185,0.6)` labeled "P (current)"; swatch `rgba(231,76,60,0.6)` labeled "Q (shifted)" — 14px `#222` text.
- **Distance value (bold 17px `#1a5276`, centered below baseline):** "W₁(P, Q) = 2.34"
- **Caption (14px `#555`, centered below that):** "Total work to reshape P into Q — captures shift magnitude"

## 2. Key Questions

(Full-width bullet list, no canvas.)

- 1D Wasserstein is O(n log n) — tractable. Higher dimensions?
- Threshold for "meaningful" distance (depends on scale, units)
- Comparison with existing separation metrics in the pipeline
- Use as a drift alarm: W(this_week, last_week) exceeds threshold → re-profile

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col text/viz layout). Structure: h1, `.subtitle` paragraph, `.intro` callout, then one `.lang-section` per section — each with an h2 (numbered "N. Title") and a `table.layout` with `td.text-col` (45%) and `td.viz-col` (55%) holding the canvas. Section 2 is a plain bullet list without a layout table or canvas. No index number in the page h1/title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.intro` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem, margin 8px 0 8px 20px. Canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic width/height attributes per chart (720×300); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; bar fills `rgba(41,128,185,0.35)` and `rgba(231,76,60,0.35)`; gray text `#555`/`#666`.
- **Links:** none on this page; if any card links exist in regenerated HTML, use `.html` extensions.
