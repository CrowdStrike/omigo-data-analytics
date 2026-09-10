# "Silence is golden"

**Page type:** detail page (claim-dissection card: single `.saying-card` containing why-believed line, flaw table, undefined-terms callout, counterexamples callout)
**HTML title tag:** Silence is golden — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected — why silence looks free when it is not.

## Why people believe it

Why people believe it: the cost of speaking is immediate and remembered. The cost of *not* speaking is delayed, diffuse, and never traced back to the moment you stayed quiet.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Asymmetric attribution | Speaking produces a traceable event with your name on it. Silence produces a non-event attributable to nobody. Say the concern is warranted 30% of the time, harm avoided 100, social cost of speaking 5: expected value is positive, but the only term ever observed is the −5. Learn from observed feedback and you converge on silence. |
| 2 | Ignores information asymmetry | The one thing that matters is whether you hold information the decision-maker lacks. Same action, opposite verdicts, and the saying cannot tell them apart. |
| 3 | Survivorship among the quiet | Quoted by those whose silence coincided with good outcomes. The engineer who saw the flaw and said nothing is rarely identified as having known. |
| 4 | Restraint ≠ abstention | "Don't say the unnecessary thing" and "don't raise the concern" are different instructions. The first is defensible; the second is how it gets used. |
| 5 | Self-reinforcing | Silence deletes the evidence that would have justified speaking, so the norm strengthens without ever being tested. Unraised concerns are not zeros — they are unobserved values, missing in proportion to how uncomfortable they were to raise. The issues absent from your incident log are precisely the expensive ones. |

## Undefined terms

**Undefined terms:** "silence" (declining to volunteer? withholding when asked? suppressing a known risk?), "golden" (valuable to whom — you, or the organization?), and the omitted terms: *about what, to whom, when*

## Counterexamples

**Counterexamples:**

- Any post-incident review finding that someone knew the failure mode in advance and did not escalate
- Aviation and clinical safety: explicit speak-up protocols exist because the silence norm was killing people
- An analyst who spots a broken pipeline and stays quiet turns a one-day fix into months of decisions on corrupt numbers
- Negotiation is the real counter-case — silence genuinely is optimal where information leakage is the cost. That it is sometimes right is why it needs a stated regime.

## Regeneration instructions

- **Template:** claim-dissection card style (see `ui-templates/07-claim-dissection-cards.html`). Single page: quoted-saying `<h1>`, `.subtitle` paragraph, then one `.saying-card` div containing (in order) `.saying-why` line, `.flaw-table`, `.undefined-terms` callout, `.counterexamples` callout. No canvases, no nav bar, no back/home links, no index number in the h1. Note "not" in the why-believed line and "about what, to whom, when" in undefined-terms are emphasized with `<em>`.
- **Flaw table:** `.flaw-table` — full width, collapsed borders, 0.88em; `th` background `#f0f4f8`, text `#1a5276`, `1px solid #e0e0e0` borders, padding 8px 12px; `td` same border/padding, top-aligned; even rows background `#fafcfe`. Columns: # / Fallacy / How it hides.
- **Callouts:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22` (orange), padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60` (green), padding 8px 14px, 0.88em, with a `<ul>` (margin 4px 0 0 16px, items 3px vertical margin).
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` 0.88em `#666`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; p 0.95em `#333`; subtitle `#666` 1.0em; `strong` in `#1a5276`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Canvases (when present on sibling pages) use `window.devicePixelRatio` scaling; this page has none. In regenerated HTML, any card links use `.html` extensions.
