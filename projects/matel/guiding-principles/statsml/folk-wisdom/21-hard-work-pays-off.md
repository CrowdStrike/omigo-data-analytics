# "Hard work always pays off"

**Page type:** detail page (claim-dissection card: single `.saying-card` containing why-believed line, flaw table, undefined-terms callout, counterexamples callout)
**HTML title tag:** Hard work always pays off — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Why people believe it

Why people believe it: effort often correlates with outcomes, and successful people retroactively attribute their success to hard work.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Survivorship bias | You hear this from people who worked hard AND succeeded. The millions who worked equally hard and failed don't get interviewed. |
| 2 | Unfalsifiable | "Hard" is undefined. Any amount of effort retroactively qualifies. If you failed, "you didn't work hard enough." |
| 3 | Single-factor attribution | Reduces a multi-causal outcome (timing, connections, luck, market, talent) to one variable. |
| 4 | Selection on DV | Only examines successes and checks "did they work hard?" — never examines hard workers who failed. |
| 5 | Post-hoc | Success happened → work was present → therefore work caused it. |

## Undefined terms

**Undefined terms:** "hard" (hours? intensity? sacrifice?), "work" (any activity?), "always" (no exceptions?), "pays off" (money? fulfillment? recognition?)

## Counterexamples

**Counterexamples:**

- Migrant farm workers — extreme effort, minimal financial return
- Startup founders who worked 100-hour weeks for 3 years and still failed (base rate: ~90%)
- Someone who worked smart for 4 hours/day and outperformed a 12-hour grinder

## Regeneration instructions

- **Template:** claim-dissection card style (see `ui-templates/07-claim-dissection-cards.html`). Single page: quoted-saying `<h1>`, `.subtitle` paragraph, then one `.saying-card` div containing (in order) `.saying-why` line, `.flaw-table`, `.undefined-terms` callout, `.counterexamples` callout. No canvases, no nav bar, no back/home links, no index number in the h1.
- **Flaw table:** `.flaw-table` — full width, collapsed borders, 0.88em; `th` background `#f0f4f8`, text `#1a5276`, `1px solid #e0e0e0` borders, padding 8px 12px; `td` same border/padding, top-aligned; even rows background `#fafcfe`. Columns: # / Fallacy / How it hides.
- **Callouts:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22` (orange), padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60` (green), padding 8px 14px, 0.88em, with a `<ul>` (margin 4px 0 0 16px, items 3px vertical margin).
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` 0.88em `#666`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; p 0.95em `#333`; subtitle `#666` 1.0em; `strong` in `#1a5276`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Canvases (when present on sibling pages) use `window.devicePixelRatio` scaling; this page has none. In regenerated HTML, any card links use `.html` extensions.
