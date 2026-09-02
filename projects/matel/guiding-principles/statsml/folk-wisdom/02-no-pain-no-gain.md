# "No pain no gain"

**Page type:** detail page (single saying-card: why-believed line, flaw table, undefined-terms callout, counterexamples callout; no canvases)
**HTML title tag:** No pain no gain — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Saying card

**Why people believe it:** effort and discomfort often accompany growth, so the brain pattern-matches pain → progress.

Flaw table (class `flaw-table`):

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Correlation ≠ causation | Pain often co-occurs with growth, but pain itself isn't the causal mechanism — deliberate practice is. |
| 2 | Survivorship bias | You hear from people whose suffering led somewhere; not from those who burned out, got injured, or quit. |
| 3 | Unfalsifiable | "Pain" is undefined — physical? emotional? mild discomfort? Any effort retroactively qualifies. |
| 4 | False dichotomy | Implies only two states: suffer and grow, or be comfortable and stagnate. Ignores efficient low-friction paths. |
| 5 | Converse error | Even if gain requires some effort, it doesn't follow that all pain produces gain. Pain can produce injury or nothing. |
| 6 | Single-factor attribution | Reduces growth to one input (suffering), ignoring rest, recovery, technique, timing. |

**Undefined terms:** "pain" (discomfort? injury? sacrifice?), "gain" (physical? financial? moral?)

**Counterexamples:**

- Overtraining syndrome — more pain, less gain, actual regression
- Repetitive strain injuries — pain producing permanent damage, not growth
- Flow-state skill acquisition — high gain, low pain

## Regeneration instructions

- **Template:** claim-dissection style (`ui-templates/07-claim-dissection-cards.html`). Order: h1 (saying in quotes), `.subtitle`, one `.saying-card` div containing `.saying-why` line, `.flaw-table`, `.undefined-terms` callout, `.counterexamples` callout (bold label + ul). Page ends at the counterexamples block. No canvases, no nav bar, no back/home links.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px. `.saying-why` 0.88em `#666`.
- **Table style:** `.flaw-table` — full width, collapsed borders, 0.88em; th background `#f0f4f8` color `#1a5276`, cells `1px solid #e0e0e0` padding 8px 12px, even rows `#fafcfe`.
- **Callout styles:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em; ul margin `4px 0 0 16px`, li margin `3px 0`.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with bottom border `2px solid #2980b9`; p 0.95em `#333`; `.subtitle` `#666` 1.0em; `strong` `#1a5276`.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- Canvases (none on this page) would use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
