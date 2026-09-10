# Demo-Driven Development

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Demo-Driven Development — Common Bad Practices

**Subtitle:** Vision Without Evidence — Optimize for the demo, not production. The audience that decides your career saw the demo.

## Section 1: The Practice

- Hand-curate demo inputs. Cherry-pick examples that work perfectly. Hardcode edge cases. Add timing delays so it "feels" real-time. Demo is flawless — leadership is impressed. Production: 60% of demo quality.
- **The extreme version:** Build a DIFFERENT system for the demo than for production. Demo system handles 5 curated examples with perfect results. Production system is the real code — buggy, slow, 60% accuracy.
- **Variant — hardcoded responses:** "Our AI handles these 3 scenarios perfectly!" (because the responses are literally if-else statements matching demo inputs). Real inputs produce garbage output.

**Why it persists:** Decision-makers (VPs, directors, investors) experience your work through DEMOS, not through daily production usage. Their entire impression is formed in a 20-minute meeting. Optimizing for those 20 minutes is rational given the incentive structure. Nobody checks prod after the demo goes well.

**The tell:** Use the system yourself for a week on REAL data, not curated examples. Compare to the demo. If dramatically different, ask: "can I use the demo inputs on production?" — if nervous faces appear, it's demo-driven.

### Visualization (canvas `c1`, 720×340)

Histogram of per-input accuracy across 1,000 production inputs; the 5 demo inputs sit as green dots above the top bin.

- **Title (bold 16px `#1a5276`, centered at y=22):** "Per-input accuracy — 1,000 production inputs".
- **Plot area:** left x=60, right x=700, top y=55, baseline y=295 (h-45).
- **Y axis (counts):** range 0–300; horizontal gridlines every 50 (`#e8e8e8`, width 1, spanning the plot); 11px `#999` right-aligned tick labels at x=52; `#ccc` axis lines along the left edge and the baseline.
- **Bars:** 9 bins "20-30" through "90-100" with counts `[40, 55, 70, 90, 110, 120, 95, 140, 280]` (sums to 1,000); bin slot width = (700−60)/9, each bar inset 6px on each side; fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1.
- **X labels:** 11px `#666` centered under each bin at baseline+16 ("20-30", "30-40", …, "90-100"); axis title "per-input accuracy (%)" 12px `#666` centered at y=330 (h-10).
- **Demo dots:** 5 filled `#27ae60` circles, radius 4, at y=64, spaced 13px apart and centered over the 90-100 bin (bin center ≈ x=664, so x = 638, 651, 664, 677, 690) — just above the top bin's top edge (bar top ≈ y=71).
- **Annotation (the only one):** bold 15px `#27ae60`, right-aligned at (598, 69): "The demo samples only the top bin."; green width-1.5 pointer line from (606, 65) to (630, 64) ending just left of the dots.

### Visualization (canvas `c2`, 720×300)

Dot strip plot: 120 small dots along a 0–100% accuracy axis with density matching c1's histogram; the 5 demo inputs are circled green dots at the extreme right. Deterministic positions only (sin-based jitter, no randomness).

- **Title (bold 16px `#1a5276`, centered at y=22):** "Every input, one dot — demo inputs circled".
- **Axis:** horizontal `#ccc` width-1 line from (50, 205) to (700, 205); 6px tick marks and 11px `#666` labels ("0", "20", "40", "60", "80", "100") at y=222, every 20%; x = 50 + (accuracy/100)×650; axis title "per-input accuracy (%)" 12px `#666` centered at y=244.
- **Production dots:** per-bin dot counts `[5, 7, 8, 11, 13, 14, 11, 17, 34]` (c1's counts ÷ 1000 × 120, rounded; total 120). Dot j of n within a bin starting at b%: accuracy = b + ((j+0.5)/n)×10. Vertical position: y = 138 + sin(i × 2.399) × 52, where i is the global dot index (deterministic jitter for vertical spread). Radius 3, fill `rgba(26,82,118,0.45)`.
- **Demo dots:** 5 dots at accuracies 95.5 / 96.5 / 97.5 / 98.5 / 99.5; y = 138 + sin((200+k) × 2.399) × 36 for k = 0..4; filled `#27ae60` radius 4 with a `#27ae60` width-2 ring of radius 7.5.
- **Annotation (the only one):** bold 15px `#27ae60`, right-aligned at (700, 46): "5 of 1000 — chosen after seeing the answers."; green width-1.5 pointer line from (670, 52) down to (676, 96) toward the demo cluster.
- **Caption:** 11px `#999`, centered at y=282: "Sampled rendering: each dot is one input; 120 of the 1,000 production inputs are shown."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` with left `<td>` (40%) holding `.obj-title` "The Practice" + bullets/paragraphs, right `<td>` (60%, centered) holding both canvases stacked (`c1` 720×340 above `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; p `#333` 0.95em; ul 0.9em `#333`, li margin 6px 0; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#555`/`#999`.
