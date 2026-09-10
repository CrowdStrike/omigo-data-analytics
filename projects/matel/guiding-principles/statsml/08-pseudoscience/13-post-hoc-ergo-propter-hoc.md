# Post Hoc Ergo Propter Hoc

**Page type:** detail page (single-row obj-table layout: text left ~40%, two stacked canvases right ~60%)
**HTML title tag:** Post Hoc Ergo Propter Hoc — Pseudoscience in Data Analysis

**Subtitle:** "After X, Therefore Because Of X"

## Section: "After X, Therefore BECAUSE OF X"

- **Performance review:** "New management training → productivity up 15%!" But the economy also recovered, three underperformers quit, and a long-stalled backlog item finally shipped. The training took credit for everything that followed it temporally.
- **Supplement industry:** "Took vitamin D → cold gone in 5 days!" Colds resolve in 5 days anyway. The supplement took credit for the immune system's natural function.
- **DevOps:** "New monitoring → incidents down 40%!" But the team also hired 2 SREs, deprecated a legacy service, and moved to a more stable cloud region. Monitoring took credit for the combined effect of 4 changes, and nobody isolated which one caused it.
- **Product launch:** "After redesign, signups up 20%!" But a marketing campaign, press coverage, and a competitor outage happened at the same time. The redesign gets credit because it is the most visible change, not because causation was established.

**Why it's pseudoscience:** Temporal sequence ("B came after A") is confused with causation ("A caused B"); without controlling for simultaneous changes, attributing the outcome to any single intervention is storytelling, not analysis.

### Visualization (canvas `c1`, 720×340)

Timeline of simultaneous events with a dashed "claimed causation" arc from the first event to the outcome.

- **Title (bold 17px, top center, `#1a5276`):** "\"After X, Therefore Because of X\" — Temporal ≠ Causal".
- **Timeline:** horizontal blue line (`#2980b9`, width 2) from (60, 120) to (660, 120).
- **Event dots (radius 6, labels 17px centered 25px below the line):**
  - x=120: "Training" — orange `#e67e22`
  - x=220: "Economy recovers" — green `#27ae60`
  - x=340: "3 people quit" — gray `#999`
  - x=460: "Legacy retired" — gray `#999`
  - x=580: "Metric improves" — green `#27ae60`
- **Claimed-causation arc:** dashed red quadratic curve (`#e74c3c`, dash 4/3, width 2) from above "Training" (120, 105) arcing through (350, 50) to above "Metric improves" (580, 105).
- **Annotation (bold red 17px, centered at y=185):** "\"Training caused the improvement!\" (ignoring 3 other changes)".
- **Caption (gray `#555` 17px, centered at y=215):** "4 things changed. 1 got credit. Because it was the most VISIBLE, not the most CAUSAL.".

### Visualization (canvas `c2`, 720×300)

Attribution diagram: four candidate causes with arrows converging on one outcome box; only the visible one gets credit.

- **Title (bold 17px `#2c3e50`, centered at y=18):** "4 causes. 1 gets credit. The visible one, not the biggest one.".
- **Outcome box:** green filled rect `#27ae60` at (width−140, 70), 120×50, containing white bold 16px "OUTCOME" and `#2c3e50` 16px "Metric improved".
- **Cause list (left side, labels at x=40, rows at y=40+i×40):** "Economy improved", "3 people quit", "New monitoring" — all gray `#95a5a6`, 11px, line width 1; "YOUR FEATURE" — red `#e74c3c`, bold, line width 3.
- **Arrows:** a line from each cause (starting x=200) to the left edge of the outcome box at (width−150, 90), stroked in the cause's color with its width; only the red "YOUR FEATURE" arrow gets a filled red triangular arrowhead.
- **Credit labels:** bold red 18px centered at (width/2+40, 175): "← Gets ALL credit (most visible)"; gray `#95a5a6` 16px at (width/2+20, 192): "← These contributed equally or more (invisible)".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title`, bullets, and the "Why it's pseudoscience" paragraph; right `<td>` (60%, centered) holds two stacked canvases (`c1` 720×340, `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; paragraphs `#333` 0.95em; `ul` 0.9em `#333` with 6px item spacing; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#555`/`#999`/`#95a5a6`, dark slate `#2c3e50`.
- In regenerated HTML, any card links use `.html` extensions.
