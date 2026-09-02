# "Jack of all trades, master of none"

**Page type:** detail page (claim-dissection card: single `.saying-card` containing why-believed line, flaw table, undefined-terms callout, counterexamples callout; no canvases)
**HTML title tag:** Jack of all trades, master of none — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Why people believe it

Why people believe it: specialization is rewarded in modern economies, and generalists are harder to categorize/hire.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Truncated quote | The full proverb is "Jack of all trades, master of none, but oftentimes better than master of one." The truncation reverses the original meaning. |
| 2 | False dichotomy | Implies you're either a generalist OR a specialist. Ignores T-shaped, π-shaped people — deep in 1-2 areas, broad everywhere else. |
| 3 | Survivorship bias | Celebrated specialists are visible. Generalists who connected dots across domains (Leonardo, Franklin, Musk) are dismissed as exceptions. |
| 4 | Ignores context | In stable, narrow domains: specialization wins. In complex, changing environments: generalists adapt faster. The saying ignores when each strategy is optimal. |
| 5 | Anchoring | "Master of none" anchors the listener to deficiency. Could equally say "competent in everything" — same fact, opposite framing. |

## Undefined terms (orange callout)

**Undefined terms:** "trades" (skills? hobbies?), "master" (top 1%? top 10%? professional?), "none" (really zero mastery in all areas?)

## Counterexamples (green callout)

**Counterexamples:**

- Startup founders need to be generalists — CEO who only knows one domain fails
- David Epstein's "Range" — generalists often outperform specialists in unpredictable environments
- The original full proverb PRAISES generalists — the truncation is itself cherry-picking

## Regeneration instructions

- **Template:** folk-wisdom claim-dissection card (see `ui-templates/07-claim-dissection-cards.html`). Page order: h1 (the quoted saying in quotation marks), `.subtitle` paragraph, then one `.saying-card` containing: `.saying-why` line, `.flaw-table` (columns # / Fallacy / How it hides), `.undefined-terms` callout, `.counterexamples` callout with a `<ul>`. No canvases, no nav bar, no back/home links, no index number in the h1.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 1.0em; `strong` in `#1a5276`; paragraphs `#333` 0.95em.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` 0.88em `#666`.
- **Flaw table style:** `.flaw-table` — full width, collapsed borders, 0.88em; th background `#f0f4f8`, `#1a5276` text, padding 8px 12px, border `1px solid #e0e0e0`; td same padding/border, vertical-align top; even rows `#fafcfe`.
- **Callout styles:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em; ul margin 4px 0 0 16px, li margin 3px 0. `.philosophy` (defined but unused here) — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** none on this page; if any were added they would use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
