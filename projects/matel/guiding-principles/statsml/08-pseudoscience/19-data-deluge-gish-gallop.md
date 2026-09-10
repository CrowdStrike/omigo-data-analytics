# Data Deluge / Gish Gallop

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Data Deluge / Gish Gallop — Pseudoscience in Data Analysis

**Subtitle:** Overwhelm with Volume So No Single Claim Gets Scrutinized

## Section 1: Overwhelm with VOLUME of Data/Claims So No Single One Gets Scrutinized

- **Executive dashboard:** 47 metrics, 12 charts, 8 comparison tables — the volume creates an impression of rigor while no single number gets examined critically. "Look at all this data" means "don't look too closely at any of it."
- **Research paper:** 15 supplementary tables, 200 statistical tests, 40 figures — no reviewer can check them all. The key result sits in one buried table, barely significant with questionable assumptions, shielded by sheer quantity.
- **Sales pitch:** "We analyzed 10 million data points!" The volume is the credential — whether those points are relevant, clean, representative, or correctly analyzed goes unexamined, because the big number reads as legitimacy.
- **Anti-vax / conspiracy:** "Here are 50 studies showing..." — each one, examined individually, is methodologically flawed, irrelevant, or misquoted, but examining all 50 takes 100 hours. The strategy is to overwhelm with quantity, since refuting one claim just invites "what about the other 49?"

**Why it's pseudoscience:** When each claim takes 10x longer to refute than to state, 50 weak claims win by volume, not validity — so demand their single strongest piece of evidence; if they can't prioritize, they don't have one.

### Visualization (canvas `c1`, 720×340)

Wall-of-charts diagram: a 4×10 grid of small rectangles with one highlighted.

- **Title (bold 17px, `#1a5276`, top center):** "Overwhelm with Volume → No Single Claim Gets Scrutinized".
- **Grid:** 40 rectangles (55×28 each), 4 rows × 10 columns, at x=55 + col×62, y=50 + row×35, fill `rgba(26,82,118,0.15)`.
- **Highlight:** the rectangle at row 2, column 4 (x=55+3×62, y=50+35) is outlined in red `#e74c3c`, width 2.
- **Annotation (17px red `#e74c3c`, centered at y=195):** "↑ The weak finding buried in 47 charts".
- **Counter line (bold 17px `#555`, centered at y=225):** "Counter: \"Show me your SINGLE strongest piece of evidence.\" If they can't pick one → none exist."

### Visualization (canvas `c2`, 720×300)

Two-bar horizontal comparison: cost to state vs cost to refute 50 weak claims.

- **Title (bold 16px Arial, `#2c3e50`, top center):** "The strategy works because refutation costs 100x more than assertion."
- **Bars:** start at x=250, max width 400, height 35.
  - Bar 1 at y=45: width 5% of max (20px), fill orange `#e67e22`, stroke `#d35400`. Left labels (left-aligned at x=50): bold 18px `#2c3e50` "STATE 50 weak claims", then 16px "5 minutes". Value label after the bar in bold 18px orange: "5 min".
  - Bar 2 at y=110: full 400px width, fill red `#e74c3c`, stroke `#c0392b`. Left labels: bold 18px "REFUTE 50 weak claims", then 16px "500 minutes". Inside the bar, centered bold 16px white: "500 min (100x longer)".
- **Ratio label (bold 18px `#2c3e50`, centered at x=230, between the bars):** "100x".
- **Counter strategy (bold 16px green `#27ae60`, centered at h-10):** "Counter: \"What is your SINGLE strongest claim? Let us examine just that one.\""

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title` + bullet list + closing "Why it's pseudoscience" paragraph, right `<td>` (60%, centered) holds two stacked canvases (`c1` 720×340, `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; p `#333` 0.95em; ul 0.9em `#333`, li margin 6px 0; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276` (bar fill `rgba(26,82,118,0.15)`), green `#27ae60`, red `#e74c3c`, orange `#e67e22` (dark orange stroke `#d35400`, dark red stroke `#c0392b`), dark slate `#2c3e50`, gray `#555`/`#333`.
- In regenerated HTML, any card links use `.html` extensions.
