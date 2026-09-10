# "Data doesn't lie"

**Page type:** detail page (claim-dissection card: why-believed line, flaw table, undefined-terms callout, counterexamples callout, plus a closing-note callout after the saying-card)
**HTML title tag:** Data doesn't lie — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected — data is a point of view, not a verdict.

## Why people believe it

Why people believe it: numbers feel objective. A figure on a slide looks like it came from reality directly, with nobody in between. The phrase gained its authority during the big-data wave of the 2000s, when data-driven decisions replaced decisions by seniority — a real improvement that then overshot into something else: if there is data, it must be true.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Data is a perspective, not the truth | Every dataset is the result of choices: what to record, when, from whom, in what units, rounded how. Those choices are a point of view. Change them and the numbers change, though reality did not. There is no view-from-nowhere version to compare against. |
| 2 | The numbers can simply be wrong | A broken tracker, a timezone mismatch, a renamed column, a double-counted event. Bugs do not announce themselves — they produce clean, confident, plausible numbers. "Data doesn't lie" offers no way to tell correct data from convincingly broken data. |
| 3 | You only have what got captured | Missing rows are invisible. Customers who never signed up, users the tracker failed on, people who declined to answer — none appear, and their absence looks like nothing rather than like a gap. What you measured is not what happened. |
| 4 | The same numbers support opposite stories | Split the same table by a different column and the conclusion can reverse — genuinely, with no error anywhere. Nothing lied. Both readings are in there, and the data will not tell you which slice to trust. |
| 5 | It is used to end the conversation | "The data says" borrows the credibility of measurement for a claim that is really an interpretation. The phrase does most of its work by making disagreement sound like denial. |

## Undefined terms (orange callout)

**Undefined terms:** "data" (raw readings? cleaned? aggregated? the subset someone chose to show you?), "lie" (deliberately mislead? or quietly leave out the part that would have changed your mind?)

## Counterexamples (green callout)

**Counterexamples:**

- A dashboard shows engagement climbing after a release; the release also added a retry that fired the same event twice
- Split a hospital's success rates by patient severity and the worse-looking surgeon becomes the better one — same records, opposite answer
- Survey results describe only the people willing to take surveys, and that willingness usually correlates with the thing being asked about
- Once a team is measured on tickets closed, ticket counts improve while the underlying problem does not — the data is accurate and the story it tells is false

## Closing note (blue callout, after the saying-card)

**Don't believe everything you see.** A polished chart is not evidence of a sound number. Titles, axis labels, a confidence band, a clean gradient — all of it is presentation, and presentation is cheap while verification is expensive. So the thing that persuades is the thing easiest to fake. A broken query renders just as beautifully as a correct one, and no amount of design will reveal which you are looking at.

The answer is not to disbelieve everything either — blanket skepticism is the same empty move as blanket trust, a fixed attitude that ignores the evidence in front of it. Ask instead how the number was made: who was counted, who was missed, what was assumed, and what would this look like if it were wrong. Believe it in proportion to what you can check.

## Regeneration instructions

- **Template:** claim-dissection card layout (see `ui-templates/07-claim-dissection-cards.html`). Single page: h1 (quoted saying, no index number), `.subtitle` paragraph, one `.saying-card` containing in order: `.saying-why` line, `.flaw-table`, `.undefined-terms` box, `.counterexamples` box; then a standalone `.closing-note` div after the card (two paragraphs separated by `<br><br>`). No canvases on this page.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` — 0.88em, `#666`, margin-bottom 14px.
- **Flaw table:** `.flaw-table` — full width, border-collapse, 0.88em; th background `#f0f4f8`, padding 8px 12px, left-aligned, border `1px solid #e0e0e0`, color `#1a5276`; td padding 8px 12px, border `1px solid #e0e0e0`, vertical-align top; even rows `#fafcfe`. Columns: # / Fallacy / How it hides.
- **Callouts:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22` (orange), padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60` (green), padding 8px 14px, 0.88em, with a `<ul>` (margin 4px 0 0 16px, li margin 3px 0). `.closing-note` — background `#f0f4f8`, left border `4px solid #2980b9` (blue), padding 12px 16px, margin 20px 0, 0.9em.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; p `#333` 0.95em; subtitle `#666` 1.0em; `strong` in `#1a5276`. Em dashes in body copy are written as `&mdash;` entities in the source. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas note:** this page has no canvases; if any are added, scale by `window.devicePixelRatio`. In regenerated HTML, any card links use `.html` extensions.
