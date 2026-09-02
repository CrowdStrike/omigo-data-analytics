# "First impressions matter"

**Page type:** detail page (claim-dissection card: single `.saying-card` containing why-believed line, flaw table, undefined-terms callout, counterexamples callout)
**HTML title tag:** First impressions matter — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected — one observation singled out, and nothing else ever measured.

## Why people believe it

Why people believe it: impressions really do influence judgement, so the saying lands as obviously true. The trouble starts with what it leaves unsaid — matter *more than which* other impression, and by how much.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | One observation declared special, the rest never quantified | Second impressions matter. So do the fifth and the one after six months. If all of them matter, the statement is true of the whole class and says nothing. The only version with content is comparative — *first* impressions matter *more* — but no weight is ever assigned to the others, so there is no baseline to be more than. A ranking claim with one entry filled in cannot be checked. |
| 2 | Unfalsifiable either way | With no stated weight on later impressions, both outcomes confirm it. The first read holds up — proof. It gets overturned — first impressions still mattered, they merely got corrected. No observation is inconsistent with the claim, which is what makes it feel so reliably true. |
| 3 | Largest weight on the weakest evidence | Read as comparative, it inverts good estimation. A first impression is the thinnest sample: least exposure, least context, worst signal-to-noise. Weighted sensibly, a thin first sample would be held loosely and later, better-informed observations would override it easily. The proverb does the opposite, then the anchor is protected by discounting everything that follows. |
| 4 | Self-fulfilling measurement | Judge someone weak, give them less support, observe weak performance. The impression caused the outcome it appears to have predicted. Let true validity be 55% and favourable judgements shift real success by 20 points either way: measured accuracy clears 75% while validity stays at 55%. The gap is causation, not prediction. |
| 5 | Influence ≠ accuracy | Separate quantities. An impression can dominate decisions while having poor predictive validity. The evidence establishes influence; the usage implies accuracy. And you never see outcomes for the candidates you rejected, so you cannot learn you were wrong about them. |
| 6 | Gameable signal | Any signal known to be weighted heavily gets optimized for — so the more the proverb is believed, the less informative first impressions become. |

## Undefined terms

**Undefined terms:** "first impression" (first seconds? first meeting? first deliverable?), "matter" (influences the observer? predicts the truth?), *more than what* — the later impressions it is implicitly compared against are never weighted — and *for how long*, since no decay rate is stated, so it can never be shown to have worn off

## Counterexamples

**Counterexamples:**

- Structured interviews with pre-committed criteria outperform impression-led ones — the effect is reducible by design, so it is not a law
- Blind auditions changed orchestra hiring: removing the impression channel changed the result
- Any colleague your considered view of reversed after months — influential and wrong, the combination the proverb cannot express

## Regeneration instructions

- **Template:** claim-dissection card style (see `ui-templates/07-claim-dissection-cards.html`). Single page: quoted-saying `<h1>`, `.subtitle` paragraph, then one `.saying-card` div containing (in order) `.saying-why` line, `.flaw-table`, `.undefined-terms` callout, `.counterexamples` callout. No canvases, no nav bar, no back/home links, no index number in the h1. Note italics ("more than which", "first"/"more", "more than what", "for how long") are `<em>` in the HTML.
- **Flaw table:** `.flaw-table` — full width, collapsed borders, 0.88em; `th` background `#f0f4f8`, text `#1a5276`, `1px solid #e0e0e0` borders, padding 8px 12px; `td` same border/padding, top-aligned; even rows background `#fafcfe`. Columns: # / Fallacy / How it hides.
- **Callouts:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22` (orange), padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60` (green), padding 8px 14px, 0.88em, with a `<ul>` (margin 4px 0 0 16px, items 3px vertical margin).
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` 0.88em `#666`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; p 0.95em `#333`; subtitle `#666` 1.0em; `strong` in `#1a5276`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Canvases (when present on sibling pages) use `window.devicePixelRatio` scaling; this page has none. In regenerated HTML, any card links use `.html` extensions.
