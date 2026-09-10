# "Girls grow faster than boys"

**Page type:** detail page (claim-dissection card: single `.saying-card` containing why-believed line, flaw table, undefined-terms callout, counterexamples callout; no canvases)
**HTML title tag:** Girls grow faster than boys — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Why people believe it

Why people believe it: it is genuinely observable. In a room of 11-to-13-year-olds the girls really are taller on average. Unlike the other sayings here, this one is not vague folk sentiment — it is a partially correct empirical observation that gets over-generalized into a false one.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Broad metric over a multi-aspect construct | "Growth" is not one quantity. It is height, weight, bone maturity, muscle mass, organ development, dentition, cognitive and emotional maturation — each on its own timeline, each with a different sex ordering. Ranking two groups on "growth" requires silently picking one aspect and weighting away the rest. That choice *is* the claim, and it is never stated. A single scalar laid over a multi-dimensional process is a misleading metric no matter which side it favours. |
| 2 | Equivocation on "faster" — and on rate vs level | Even after fixing the aspect to height, "faster" still spans three things: earlier onset (girls, ~11–12 vs ~13–14), higher peak rate (boys, ~9.5 vs ~8.3 cm/yr), and greater total (boys, ~13 cm adult gap). Worse, "grows faster" asserts a *derivative* while the evidence offered is a *level* — being taller at 12 is the integral of an earlier start, not proof of a higher rate. |
| 3 | Cross-sectional snapshot of a phase-shifted process | Measure at age 11 and girls are taller. Measure the same cohort at 16 and boys are taller. The underlying trajectories never changed — only the measurement date did. Whenever two groups follow time-offset curves, the sampling date selects the conclusion. |
| 4 | Averaging on the wrong time axis | Average growth by *chronological* age and the sharp pubertal spurt smears into a broad, low bump, because individuals spurt at different ages. No individual grows like the average curve. Aligning each child on their own peak-velocity date restores the true spike. |
| 5 | Group average applied to an individual | Within-sex variance in spurt timing is larger than the between-sex difference in mean timing. A late-maturing girl and an early-maturing boy invert the pattern entirely, so the average tells you almost nothing about any specific pair of children. |

## Undefined terms (orange callout)

**Undefined terms:** "grow" (height? weight? bone age? muscle mass? cognitive or emotional maturity? — and weighted how across them?), "faster" (starts earlier? higher rate? finishes sooner? ends up bigger?), "girls"/"boys" (measured at which age — the claim has no truth value until an aspect and an age are both fixed)

## Counterexamples (green callout)

**Counterexamples:**

- Pick a different aspect and the ordering flips: girls lead on bone maturity and dentition timing, boys on adult muscle mass and final height — "growth" has no single winner
- Peak growth velocity is *higher* in boys, not girls — the opposite of the literal reading
- Girls stop earlier (growth plates fuse ~15–16 vs ~17–18), so "faster" is partly "finishes sooner," which is not the same as "grows more"
- Formal analogue: any composite index ("developer productivity," "customer health," "engagement") reverses its ranking when the sub-metric weights change — and the weights are usually undocumented

## Regeneration instructions

- **Template:** folk-wisdom claim-dissection card (see `ui-templates/07-claim-dissection-cards.html`). Page order: h1 (the quoted saying in quotation marks), `.subtitle` paragraph, then one `.saying-card` containing: `.saying-why` line, `.flaw-table` (columns # / Fallacy / How it hides), `.undefined-terms` callout, `.counterexamples` callout with a `<ul>`. Italicized words above (*is*, *derivative*, *level*, *chronological*, *higher*) are `<em>` in the HTML; em/en dashes appear as `&mdash;`/`&ndash;` entities. No canvases, no nav bar, no back/home links, no index number in the h1.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 1.0em; `strong` in `#1a5276`; paragraphs `#333` 0.95em.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` 0.88em `#666`.
- **Flaw table style:** `.flaw-table` — full width, collapsed borders, 0.88em; th background `#f0f4f8`, `#1a5276` text, padding 8px 12px, border `1px solid #e0e0e0`; td same padding/border, vertical-align top; even rows `#fafcfe`.
- **Callout styles:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em; ul margin 4px 0 0 16px, li margin 3px 0. `.philosophy` (defined but unused here) — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** none on this page; if any were added they would use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
