# Publication Bias / File Drawer Effect

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Publication Bias / File Drawer Effect — Pseudoscience in Data Analysis

**Subtitle:** Only Positive Results Get Published, Creating Systematically Distorted Literature

## Section 1: Only Positive Results Get Published — Creating a Systematically Distorted Literature

- **Pharma:** 10 studies run on Drug X: 8 show no effect, 2 show a positive effect, and only the 2 get published — so "the literature shows Drug X works." The 8 negative studies sit in file drawers, making the published record a biased sample that any meta-analysis inherits.
- **Psychology:** Priming effects, power poses, and ego depletion were all "established" by positive publications while failed replications couldn't get published for years, leaving entire subfields built on the surviving positive results. This isn't a conspiracy — journals are simply incentivized toward novel positive findings.
- **Internal company data:** A/B tests showing no effect are never presented to leadership, while tests showing improvements get full presentations. Leadership concludes "everything we try works" because the negative experiments are invisible.
- **ML papers:** "Our method beats all baselines!" gets accepted; "we tried 50 architectures and none beat logistic regression" does not. The literature makes progress look constant when most attempts fail silently.

**Why it's pseudoscience:** The evidence base is systematically filtered to over-represent positive results, so you're reading "what was publishable," not "what's true" — the file drawer holds the true base rate of failure.

### Visualization (canvas `c1`, 720×340)

Row-of-boxes diagram: 10 studies, 8 hidden negatives and 2 published positives.

- **Title (bold 17px, `#1a5276`, top center):** "You Only See Published Results — The File Drawer Holds the Truth".
- **Subtitle line (17px gray `#999`, centered, y=50):** "10 studies run on Drug X:".
- **Negative studies:** 8 boxes (45×60 each, at x=60 + i×55, y=70), fill `rgba(153,153,153,0.3)`, stroke `#999` width 1; each labeled inside in 16px `#999`, two lines: "No" / "effect".
- **Positive studies:** 2 boxes (55×60 each, at x=510 + i×70, y=70), fill `rgba(39,174,96,0.3)`, stroke `#27ae60` width 2; each labeled inside in bold 16px `#27ae60`, two lines: "Positive!" / "Published!".
- **Takeaway lines (centered):**
  - Bold 17px red `#e74c3c` at y=160: "Published literature: \"Drug X works!\" (2/2 published = 100%)".
  - 17px `#555` at y=185: "Full truth: Drug X works 2/10 = 20% of the time. The other 80% is in the file drawer."
  - 17px `#555` at y=210: "\"The literature\" is a BIASED SAMPLE of all evidence that was ever generated."

### Visualization (canvas `c2`, 720×300)

Two pie charts with a filter arrow between them: all studies vs published literature.

- **Title (bold 17px Arial, `#2c3e50`, top center):** "This is what you READ. It is not what is TRUE."
- **Left pie** (center x=150, y=h/2+15, radius 60): 80% gray slice `#95a5a6`, 20% green slice `#27ae60`, starting at 12 o'clock. Below: bold 18px `#2c3e50` "All Studies Conducted", then 16px "80% null (gray) / 20% positive (green)".
- **Right pie** (center x=w-150, same y/radius): 10% gray slice `#95a5a6`, 90% green slice `#27ae60`. Below: bold 18px "Published Literature", then 16px "10% null (gray) / 90% positive (green)".
- **Filter arrow:** horizontal red `#e74c3c` line (width 2) between the pies at pie-center height, with a filled red arrowhead pointing right; above/on it, bold 17px red centered labels: "Publication filter:" / "positive results only".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title` + bullet list + closing "Why it's pseudoscience" paragraph, right `<td>` (60%, centered) holds two stacked canvases (`c1` 720×340, `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; p `#333` 0.95em; ul 0.9em `#333`, li margin 6px 0; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, dark slate `#2c3e50`, gray `#95a5a6`/`#999`/`#555`/`#333`.
- In regenerated HTML, any card links use `.html` extensions.
