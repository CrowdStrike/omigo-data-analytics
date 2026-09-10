# Prototype-as-Product

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvases right ~60%, one row per section)
**HTML title tag:** Prototype-as-Product — Common Bad Practices

**Subtitle:** Vision Without Evidence — 'Proved it works!' (on 3 examples in a notebook). Ship as v1. Fixing is someone else's problem.

## Section 1: The Practice

- Jupyter notebook with 3 hand-picked examples. Works beautifully. Call it "v1." Ship it. Real data: crashes on 40% of inputs, has no error handling, can't scale past 100 requests.
- **The attribution gap:** Person A demos the "v1"; Person B inherits it and does the real engineering — error handling, scaling, edge cases, monitoring, reliability. That work is under-credited because the feature was "already launched."
- **Variant — "POC that became production":** "It was just supposed to be a proof of concept!" But it got demo'd, leadership liked it, it's in prod now. Built with zero production engineering because it was "just a POC." 2 years later: still running, still breaks.

**Why it persists:** The person who launches gets most of the credit; the person who makes it actually work (hardening, scaling, fixing) gets little. Demo visibility and engineering effort are measured differently, so shipping a fragile v1 registers as more valuable than engineering a solid v2.

**The tell:** Check git history. If "v1" was rewritten 80% in the 3 months after "launch" — the original was a prototype shipped as product. Also: is the person who launched still maintaining it? A hand-off within weeks of launch is a signal the v1 was not production-grade.

### Visualization (canvas `c1`, 720×340)

Two-box comparison diagram: prototype vs production, connected by an arrow.

- **Left box ("Prototype"):** at x=30, width = 0.4×720−40 = 248px, height 130px, vertically centered (top = (h−130)/2−15). Fill `rgba(39,174,96,0.1)`, stroke `#27ae60` width 2. Title "Prototype" bold 16px `#1a5276` centered near top of box. Inside: three green (`#27ae60`) 18px checkmarks "✓" spaced 60px apart starting at box x+40. Below in gray `#666` 16px, two centered lines: "3 curated examples" / "All pass!".
- **Right box ("Production"):** at x = 720×0.5+20 = 380, width 288px (0.4×720), same height/top. Fill `rgba(231,76,60,0.08)`, stroke `#e74c3c` width 2. Title "Production" bold 16px `#1a5276`. Inside: a 17-column × 3-row grid (cell size 14px, starting at box x+15, top+35) of 16px symbols — the first 3 cells are green `#27ae60` checkmarks "✓", the remaining 48 are red `#e74c3c` crosses "✗". Below in red 16px centered: "~40% failure on real data".
- **Arrow between boxes:** horizontal blue `#1a5276` line (width 2) at the boxes' vertical midpoint, from left box's right edge to the right box's left edge, with filled triangular arrowhead; label above the arrow, 16px `#1a5276` centered: "Shipped as v1".
- **Bottom text (16px gray `#666`, centered, y = h−12):** "The shipped demo is visible work; the hardening that follows is not."

### Visualization (canvas `c2`, 720×300)

Line chart of two cumulative curves over months 0-6 — credit received vs engineering effort ("Credit vs Engineering Effort (cumulative)"). Deterministic data, no randomness.

- **Title (bold 15px `#1a5276`, centered at (360, 18)):** "Credit vs Engineering Effort (cumulative)".
- **Legend (top right, 12px `#333`):** two rows; each row has a 26px line swatch (width 2.5) from x=398 to x=424 and left-aligned text at x=432. Row 1 (y=34, orange `#e67e22` swatch): "Cumulative credit received (Person A)". Row 2 (y=50, blue `#1a5276` swatch): "Cumulative engineering effort (Person B)".
- **Plot area:** x from 55 to 690; y from 60 (=100%) to 258 (=0%), so `yFor(v) = 258 − v/100×198`. Data points inset horizontally: `xFor(m) = 70 + m×100` for months m = 0..6.
- **Y-axis:** gridlines at 0/20/40/60/80/100% — horizontal `#e0e0e0` lines (width 1) across the plot; right-aligned 11px `#666` labels "0%".."100%" at x=48. Axis lines in `#999`: vertical at x=55 from y=60 to y=258, horizontal at y=258 from x=55 to x=690.
- **X-axis:** tick labels "M0".."M6" (11px `#666`, centered) at y=274 under each `xFor(m)`.
- **Data arrays (percent):** `months = [0,1,2,3,4,5,6]`; `credit = [90,93,95,97,98,99,100]`; `effort = [20,35,50,63,76,88,100]`.
- **Shaded divergence:** polygon between the two curves (credit on top, effort below, months 0→6 and back), filled `rgba(230,126,34,0.12)` — the scissor-shaped gap.
- **Credit curve (orange `#e67e22`, width 2.5):** launch-day step — vertical segment from (xFor(0), yFor(0)) up to (xFor(0), yFor(90)) — then polyline through the credit points; filled dots radius 3 at each point.
- **Effort curve (blue `#1a5276`, width 2.5):** polyline through the effort points, filled dots radius 3 at each point.
- **Annotation (ONE, bold 14px red `#e74c3c`, centered at (370, 242)):** "90% of the credit on day 0; 80% of the work after."
- **Caption (italic 12px `#666`, centered at (360, 292)):** "Illustrative attribution vs effort."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` (single row here); left `<td>` (40%) holds `.obj-title` "The Practice" + bullets/paragraphs, right `<td>` (60%, centered) holds both canvases stacked (`c1` then `c2`).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` `#333` 0.95em; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
