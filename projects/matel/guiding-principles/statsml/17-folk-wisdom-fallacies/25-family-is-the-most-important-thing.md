# "Family is the most important thing"

**Page type:** detail page (claim-dissection card: single `.saying-card` containing why-believed line, flaw table, undefined-terms callout, counterexamples callout)
**HTML title tag:** Family is the most important thing — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected — winning a ranking says nothing about the margin.

## Why people believe it

Why people believe it: it is socially unanswerable. Disagreeing sounds like a confession rather than an argument, so the claim is rarely examined — and for many people, in many circumstances, it is genuinely true.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Rank stated, magnitude assumed | "Most important" claims first place and nothing more. It is heard as overwhelming — something like 90% — but it only requires beating whatever came second. Family 30%, friends 25%, the person who helps raise your children 20%, colleagues 2%: family wins, and is still outweighed by everything else combined. Both readings are true at once, and only one gets spoken. |
| 2 | The winner depends on the buckets | "Family" pools spouse, parents, siblings and children into one category while friends and colleagues are counted individually. Split family into its members and no single member may lead. Pool friendships into one bucket instead and family may lose. The ranking flipped; nothing about anyone's life changed except the grouping. |
| 3 | One ordering asserted for every purpose | Important for emotional support, financial security, career growth, health, identity? These give different orderings for the same person. A single ranking is only meaningful once you say ranked on what — and the saying never does. |
| 4 | Stated as a fact, used as an instruction | As a description of what people value it is measurable. As a rule about how you should allocate your hours it is a different claim needing separate support. The second borrows the first's obviousness, which is how it becomes a lever for guilt. |
| 5 | Assumes the same family for everyone | The word presumes a structure that is present, near, and safe. People are dispersed across cities and continents, and some families are the harm they had to leave. A rule that treats those cases as failures to prioritise is describing a circumstance, not a virtue. |
| 6 | Unfalsifiable in practice | Nobody publishes their weights, so no outcome can contradict it. Long hours away mean you lost sight of what matters; time at home means you knew all along. Both directions confirm. |

## Undefined terms

**Undefined terms:** "family" (household? blood relatives? the people who raised you? the ones you chose?), "most important" (largest share? highest rank by a margin unstated? irreplaceable?), "thing" (compared against what list — and who wrote it?)

## Counterexamples

**Counterexamples:**

- Migrant workers supporting relatives from thousands of miles away invert the saying's own logic: the family is the reason for the distance, so measuring devotion in hours present gets the answer exactly backwards
- Ask what someone would sacrifice for family and the ranking looks decisive; ask how they spent last Tuesday and it usually does not — stated and revealed preferences disagree, and the saying only ever reports the stated one
- Caregivers routinely rank family first while the practical work is carried by paid help, neighbours or friends whose contribution the ranking rounds down
- People estranged from harmful families often build stronger support networks than the ones they left, which the claim has no way to describe

## Regeneration instructions

- **Template:** claim-dissection card style (see `ui-templates/07-claim-dissection-cards.html`). Single page: quoted-saying `<h1>`, `.subtitle` paragraph, then one `.saying-card` div containing (in order) `.saying-why` line, `.flaw-table`, `.undefined-terms` callout, `.counterexamples` callout. No canvases, no nav bar, no back/home links, no index number in the h1.
- **Flaw table:** `.flaw-table` — full width, collapsed borders, 0.88em; `th` background `#f0f4f8`, text `#1a5276`, `1px solid #e0e0e0` borders, padding 8px 12px; `td` same border/padding, top-aligned; even rows background `#fafcfe`. Columns: # / Fallacy / How it hides.
- **Callouts:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22` (orange), padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60` (green), padding 8px 14px, 0.88em, with a `<ul>` (margin 4px 0 0 16px, items 3px vertical margin).
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` 0.88em `#666`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; p 0.95em `#333`; subtitle `#666` 1.0em; `strong` in `#1a5276`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Canvases (when present on sibling pages) use `window.devicePixelRatio` scaling; this page has none. In regenerated HTML, any card links use `.html` extensions.
