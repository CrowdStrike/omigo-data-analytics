# "Slow and steady wins the race"

**Page type:** detail page (claim-dissection card: single `.saying-card` containing why-believed line, flaw table, undefined-terms callout, counterexamples callout; no canvases)
**HTML title tag:** Slow and steady wins the race — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Why people believe it

Why people believe it: Aesop's fable (tortoise and hare) is deeply embedded in culture. Consistency is genuinely useful — but "wins the race" is an overreach.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Cherry-picking (literary) | Based on a fable where the fast competitor was specifically written to be arrogant and lazy. The tortoise wins because the hare naps — not because slow is inherently better. |
| 2 | Ignores domain | In competitive markets, speed IS the advantage. The "slow and steady" competitor often gets disrupted by the fast one who ships first. |
| 3 | Survivorship bias | Slow-and-steady winners are celebrated. Slow-and-steady losers (who were simply too slow) are forgotten. |
| 4 | False dichotomy | Implies fast = reckless, slow = reliable. Ignores: fast AND reliable (the actual winning strategy in most contexts). |
| 5 | Contradicts competing wisdom | "Strike while the iron is hot," "First-mover advantage," "Move fast and break things" — proverbs are not internally consistent. |

## Undefined terms (orange callout)

**Undefined terms:** "slow" (how slow?), "steady" (no variation allowed?), "wins" (always? eventually? in what context?), "race" (competition? personal goal?)

## Counterexamples (green callout)

**Counterexamples:**

- Amazon — moved fast, dominated e-commerce. Slow competitors lost.
- Actual races — the fastest runner wins, not the slowest steady one
- Technology adoption — companies that slowly adopted cloud computing fell behind permanently

## Regeneration instructions

- **Template:** folk-wisdom claim-dissection card (see `ui-templates/07-claim-dissection-cards.html`). Page order: h1 (the quoted saying in quotation marks), `.subtitle` paragraph, then one `.saying-card` containing: `.saying-why` line, `.flaw-table` (columns # / Fallacy / How it hides), `.undefined-terms` callout, `.counterexamples` callout with a `<ul>`. No canvases, no nav bar, no back/home links, no index number in the h1.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 1.0em; `strong` in `#1a5276`; paragraphs `#333` 0.95em.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` 0.88em `#666`.
- **Flaw table style:** `.flaw-table` — full width, collapsed borders, 0.88em; th background `#f0f4f8`, `#1a5276` text, padding 8px 12px, border `1px solid #e0e0e0`; td same padding/border, vertical-align top; even rows `#fafcfe`.
- **Callout styles:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em; ul margin 4px 0 0 16px, li margin 3px 0. `.philosophy` (defined but unused here) — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** none on this page; if any were added they would use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
