# "Try and try until you succeed"

**Page type:** detail page (single saying-dissection card: why-believed line, flaw table, undefined-terms box, counterexamples box; no canvases)
**HTML title tag:** Try and try until you succeed — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Saying card

**Why-believed line:** Why people believe it: persistence is visible in every success story told in retrospect.

### Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Survivorship bias | You only hear this from people who eventually succeeded. The graveyard of people who tried 100 times and failed is silent. |
| 2 | Sunk cost encouragement | Frames quitting as moral failure, regardless of evidence that the path is wrong. |
| 3 | No stopping criterion | When should you stop? It offers no decision boundary — that's unfalsifiable by design. |
| 4 | Selection on DV | Only examines successes, asks "did they persist?" — never examines persistent failures. |
| 5 | Base rate neglect | For many endeavors, the base rate of success is near zero regardless of attempts. Repeating a 0.1% probability event 100 times gives you ~10% — not certainty. |

### Undefined terms (orange callout)

**Undefined terms:** "try" (same approach? different approach? what counts as a try?), "until" (no time limit?), "succeed" (any positive outcome? full goal?)

### Counterexamples (green callout)

**Counterexamples:**

- Insanity is doing the same thing expecting different results — "try and try" without changing approach is exactly this
- Venture capitalists who pivot (quit the current idea) rather than persist — pivot IS the success strategy
- Gamblers who "try and try" at the casino — the base rate ensures long-run loss

## Regeneration instructions

- **Template:** claim-dissection card layout (see `ui-templates/07-claim-dissection-cards.html`). Single page: h1 (the quoted saying, no index number), `.subtitle` paragraph, then one `.saying-card` containing in order: `.saying-why` line, `.flaw-table`, `.undefined-terms` box, `.counterexamples` box. Page ends at the counterexamples block. No canvases, no nav bar, no back/home links.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` — 0.88em, color `#666`, margin-bottom 14px.
- **Flaw table:** full width, border-collapse, 0.88em; `th` background `#f0f4f8`, padding 8px 12px, left-aligned, border `1px solid #e0e0e0`, color `#1a5276`; `td` padding 8px 12px, border `1px solid #e0e0e0`, vertical-align top; even rows background `#fafcfe`. Columns: # / Fallacy / How it hides.
- **Callout boxes:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em, with a `<ul>` (margin 4px 0 0 16px, li margin 3px 0).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with bottom border `2px solid #2980b9`; p 0.95em `#333`; `.subtitle` `#666` 1.0em; `strong` `#1a5276`; `.philosophy` style available (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but unused on this page.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Canvases (when present in this project) use `window.devicePixelRatio` scaling; this page has none. In regenerated HTML, any card links use `.html` extensions.
