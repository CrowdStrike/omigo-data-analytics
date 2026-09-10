# "What doesn't kill you makes you stronger"

**Page type:** detail page (single saying-card: why-believed line, flaw table, undefined-terms callout, counterexamples callout; no canvases)
**HTML title tag:** What doesn't kill you makes you stronger — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Saying card

**Why people believe it:** some adversity does build resilience. The subset that recovered stronger is vocal about it.

Flaw table (class `flaw-table`):

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Survivorship bias | You hear from survivors. People permanently damaged by trauma don't give TED talks. |
| 2 | Cherry-picking | Selects cases where recovery happened; ignores chronic PTSD, disability, permanent setback. |
| 3 | Unfalsifiable | "Stronger" is undefined — emotionally? physically? In what timeframe? |
| 4 | False dichotomy | Only two outcomes: death or strength. Ignores the vast middle: permanent damage, slow decline, unchanged. |
| 5 | Correlation ≠ causation | Some people are strong AND survived trauma. The strength may predate the trauma (selection effect). |

**Undefined terms:** "kill" (literal? figurative? "almost kill"?), "stronger" (how measured? by when?)

**Counterexamples:**

- PTSD — trauma that creates lasting vulnerability, not strength
- Chronic injuries — a torn ACL doesn't make the knee stronger
- Financial ruin — many never recover to baseline, let alone "stronger"

## Regeneration instructions

- **Template:** claim-dissection style (`ui-templates/07-claim-dissection-cards.html`). Order: h1 (saying in quotes), `.subtitle`, one `.saying-card` div containing `.saying-why` line, `.flaw-table`, `.undefined-terms` callout, `.counterexamples` callout (bold label + ul). Page ends at the counterexamples block. No canvases, no nav bar, no back/home links.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px. `.saying-why` 0.88em `#666`.
- **Table style:** `.flaw-table` — full width, collapsed borders, 0.88em; th background `#f0f4f8` color `#1a5276`, cells `1px solid #e0e0e0` padding 8px 12px, even rows `#fafcfe`.
- **Callout styles:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em; ul margin `4px 0 0 16px`, li margin `3px 0`.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with bottom border `2px solid #2980b9`; p 0.95em `#333`; `.subtitle` `#666` 1.0em; `strong` `#1a5276`.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- Canvases (none on this page) would use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
