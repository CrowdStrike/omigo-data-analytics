# "Money can't buy happiness"

**Page type:** detail page (claim-dissection card: single `.saying-card` containing why-believed line, flaw table, undefined-terms callout, counterexamples callout; no canvases)
**HTML title tag:** Money can't buy happiness — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Why people believe it

Why people believe it: wealthy people can be miserable, and the saying comforts those without wealth.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Ignores threshold effects | Research (Kahneman, Killingsworth) shows money DOES increase happiness up to a substantial threshold. Below that: poverty causes measurable misery. The saying erases this. |
| 2 | Equivocation | "Buy" implies a direct transactional exchange. Money buys security, healthcare, freedom, time — all of which strongly correlate with happiness. The mechanism is indirect, not absent. |
| 3 | Cherry-picking | Selects miserable rich people as evidence. Ignores that happiness surveys consistently show positive correlation between income and life satisfaction. |
| 4 | Survivorship (inverted) | The saying survives because it comforts the majority (who aren't wealthy). A saying that said "money does buy happiness" would be socially unpopular regardless of truth. |
| 5 | All-or-nothing thinking | Money can't buy ALL happiness ≠ money can't buy ANY happiness. The saying conflates partial contribution with zero contribution. |

## Undefined terms (orange callout)

**Undefined terms:** "money" (how much? relative to what?), "buy" (directly transact? or enable?), "happiness" (hedonic? eudaimonic? life satisfaction?)

## Counterexamples (green callout)

**Counterexamples:**

- Poverty reduction → massive happiness gains (food security, shelter, healthcare access)
- Buying time (hiring help, reducing commute) → measured happiness increases
- Financial security → reduced anxiety → measurable wellbeing improvement

## Regeneration instructions

- **Template:** folk-wisdom claim-dissection card (see `ui-templates/07-claim-dissection-cards.html`). Page order: h1 (the quoted saying in quotation marks), `.subtitle` paragraph, then one `.saying-card` containing: `.saying-why` line, `.flaw-table` (columns # / Fallacy / How it hides), `.undefined-terms` callout, `.counterexamples` callout with a `<ul>`. No canvases, no nav bar, no back/home links, no index number in the h1.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 1.0em; `strong` in `#1a5276`; paragraphs `#333` 0.95em.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` 0.88em `#666`.
- **Flaw table style:** `.flaw-table` — full width, collapsed borders, 0.88em; th background `#f0f4f8`, `#1a5276` text, padding 8px 12px, border `1px solid #e0e0e0`; td same padding/border, vertical-align top; even rows `#fafcfe`.
- **Callout styles:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em; ul margin 4px 0 0 16px, li margin 3px 0. `.philosophy` (defined but unused here) — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** none on this page; if any were added they would use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
