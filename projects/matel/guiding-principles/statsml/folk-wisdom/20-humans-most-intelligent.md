# "Humans are the most intelligent animals"

**Page type:** detail page (claim-dissection card: single `.saying-card` containing why-believed line, flaw table, undefined-terms callout, counterexamples callout; no canvases)
**HTML title tag:** Humans are the most intelligent animals — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Why people believe it

Why people believe it: humans built the measuring apparatus, wrote the definition, ran the tests, and graded the results. A metric designed by the winner will find the designer winning.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | Self-assessment / conflict of interest | The evaluator is a member of the class being ranked first. No other species agreed to the criteria, and none can contest the result. In any other setting a benchmark authored and scored by one contestant would be discarded on sight. |
| 2 | Metric designed around the winner's strengths | Language, tool use, abstract symbol manipulation, and written reasoning are the dimensions chosen — precisely the dimensions humans dominate. Score instead on echolocation, magnetoreception, olfactory discrimination, or eight-limb distributed motor control and the ranking inverts. This is target leakage: the outcome was baked into the feature set. |
| 3 | Single-scalar collapse of a multi-dimensional construct | "Most intelligent" presumes intelligence is one totally-ordered quantity. Observed cognition is a vector of weakly-correlated capacities (spatial memory, social modelling, cached-food recall, sensory integration). Ranking requires choosing weights, and the weights are the entire argument — smuggled in unstated. |
| 4 | Unfalsifiable by construction | Any counterexample gets absorbed by redefinition. Corvids solve multi-step tool problems → "that's instinct, not real intelligence." Octopuses show individual learning → "not the same kind of intelligence." The category is edited after each disconfirmation, so nothing can ever refute it. |
| 5 | Measurement in the evaluator's native environment | Tests are administered in human settings, in human modalities, on human-relevant tasks, under human time limits. A dolphin scored on pencil-and-paper reasoning and a human scored on 3D acoustic navigation both fail — and only one of those failures gets recorded as a fact about the species. |

## Undefined terms (orange callout)

**Undefined terms:** "intelligent" (problem-solving? abstraction? social cognition? adaptability? survival fitness?), "most" (on which axis, with which weights?), "animals" (per-individual or per-species? measured how, by whom?)

## Counterexamples (green callout)

**Counterexamples:**

- Chimpanzees beat humans on rapid numeric-sequence working-memory tasks — a dimension we simply exclude from the definition
- Clark's nutcracker recalls thousands of distinct cache sites across seasons; no human comes close
- If intelligence is scored as survival persistence, bacteria and ants outlast us on both duration and biomass
- Formal analogue: any model evaluated on a benchmark authored by its own creators reports inflated accuracy — the test set is not independent of the hypothesis

## Regeneration instructions

- **Template:** folk-wisdom claim-dissection card (see `ui-templates/07-claim-dissection-cards.html`). Page order: h1 (the quoted saying in quotation marks), `.subtitle` paragraph, then one `.saying-card` containing: `.saying-why` line, `.flaw-table` (columns # / Fallacy / How it hides), `.undefined-terms` callout, `.counterexamples` callout with a `<ul>`. Em dashes and arrows appear as `&mdash;`/`&rarr;` entities in the HTML. No canvases, no nav bar, no back/home links, no index number in the h1.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 1.0em; `strong` in `#1a5276`; paragraphs `#333` 0.95em.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` 0.88em `#666`.
- **Flaw table style:** `.flaw-table` — full width, collapsed borders, 0.88em; th background `#f0f4f8`, `#1a5276` text, padding 8px 12px, border `1px solid #e0e0e0`; td same padding/border, vertical-align top; even rows `#fafcfe`.
- **Callout styles:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em; ul margin 4px 0 0 16px, li margin 3px 0. `.philosophy` (defined but unused here) — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** none on this page; if any were added they would use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
