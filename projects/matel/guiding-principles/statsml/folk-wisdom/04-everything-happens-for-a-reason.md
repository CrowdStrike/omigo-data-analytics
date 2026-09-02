# "Everything happens for a reason"

**Page type:** detail page (single saying-card: why-believed line, flaw table, undefined-terms callout, counterexamples callout; no canvases)
**HTML title tag:** Everything happens for a reason — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Saying card

**Why people believe it:** humans are meaning-making machines. Assigning purpose to random events reduces anxiety.

Flaw table (class `flaw-table`):

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Unfalsifiable | The "reason" is never specified. Any post-hoc narrative qualifies. No possible observation can disprove it. |
| 2 | Post-hoc rationalization | Outcome happens → reason is invented after → feels like prediction. |
| 3 | Narrative fallacy | Imposes story structure on random sequences. Humans can't tolerate meaninglessness. |
| 4 | Equivocation | "Reason" conflates two meanings: causal mechanism (physics) vs purpose/meaning (teleology). The proverb implies the latter using the authority of the former. |

**Undefined terms:** "everything" (trivial events too?), "reason" (causal? purposeful? whose purpose?)

**Counterexamples:**

- Childhood cancer — what "reason" justifies it without being monstrous?
- Random mutations — no purpose, just statistical noise in DNA replication
- Market crashes triggered by cascading technical failures — no agent, no purpose

## Regeneration instructions

- **Template:** claim-dissection style (`ui-templates/07-claim-dissection-cards.html`). Order: h1 (saying in quotes), `.subtitle`, one `.saying-card` div containing `.saying-why` line, `.flaw-table`, `.undefined-terms` callout, `.counterexamples` callout (bold label + ul). Page ends at the counterexamples block. No canvases, no nav bar, no back/home links.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px. `.saying-why` 0.88em `#666`.
- **Table style:** `.flaw-table` — full width, collapsed borders, 0.88em; th background `#f0f4f8` color `#1a5276`, cells `1px solid #e0e0e0` padding 8px 12px, even rows `#fafcfe`.
- **Callout styles:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em; ul margin `4px 0 0 16px`, li margin `3px 0`.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with bottom border `2px solid #2980b9`; p 0.95em `#333`; `.subtitle` `#666` 1.0em; `strong` `#1a5276`.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- Canvases (none on this page) would use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
