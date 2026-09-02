# "Behind every successful man there is a woman"

**Page type:** detail page (single saying-card: why-believed line, flaw table, undefined-terms callout, counterexamples callout; no canvases)
**HTML title tag:** Behind every successful man there is a woman — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Saying card

**Why people believe it:** many successful men publicly credit a partner, and the pattern-match feels confirming.

Flaw table (class `flaw-table`):

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Survivorship bias | Only looks at men who succeeded. Ignores men with supportive partners who failed, and men who succeeded alone. |
| 2 | Unfalsifiable | "Behind" is vague enough that you can always find some woman (mother, partner, colleague) to confirm it post-hoc. |
| 3 | Post-hoc attribution | Observes success → finds a woman in proximity → declares causality. |
| 4 | Single-factor attribution | Reduces a multi-causal outcome to one variable. |
| 5 | Selection on DV | Only examines successes, never failures — classic flawed research design. |
| 6 | Gendered reductionism | Reduces women to a support role and men to the protagonist. Both are analytically and socially wrong. |
| 7 | Cherry-picking | Selects confirming examples, ignores counterexamples in both directions. |

**Undefined terms:** "successful" (how measured?), "behind" (support? inspiration? proximity?), "a woman" (which one? by what mechanism?)

**Counterexamples:**

- Successful men without partners (Newton, Tesla, many others)
- Men with supportive partners who failed repeatedly
- Successful women — does the saying work in reverse? If not, it's not a universal principle.

## Regeneration instructions

- **Template:** claim-dissection style (`ui-templates/07-claim-dissection-cards.html`). Order: h1 (saying in quotes), `.subtitle`, one `.saying-card` div containing `.saying-why` line, `.flaw-table`, `.undefined-terms` callout, `.counterexamples` callout (bold label + ul). Page ends at the counterexamples block. No canvases, no nav bar, no back/home links.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px. `.saying-why` 0.88em `#666`.
- **Table style:** `.flaw-table` — full width, collapsed borders, 0.88em; th background `#f0f4f8` color `#1a5276`, cells `1px solid #e0e0e0` padding 8px 12px, even rows `#fafcfe`.
- **Callout styles:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em; ul margin `4px 0 0 16px`, li margin `3px 0`.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with bottom border `2px solid #2980b9`; p 0.95em `#333`; `.subtitle` `#666` 1.0em; `strong` `#1a5276`.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- Canvases (none on this page) would use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
