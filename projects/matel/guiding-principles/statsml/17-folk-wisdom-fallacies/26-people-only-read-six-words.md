# "People only read six words"

**Page type:** detail page (claim-dissection card: single `.saying-card` containing why-believed line, flaw table, undefined-terms callout, counterexamples callout; no canvases)
**HTML title tag:** People only read six words — Folk Wisdom Dissected

**Subtitle:** Folk wisdom dissected into its component fallacies — what the saying hides and why it feels true anyway.

## Why people believe it

Why people believe it: it has a true ancestor. Billboard designers really do cap copy at about six words, and that rule genuinely works — so the number arrives carrying the authority of a real industry practice. It also flatters everyone's experience of skimming: we all skip most of what we see, so a small hard number feels like the explanation. What got lost in transit is that the billboard six is an output of arithmetic about a specific situation, not a measurement of human attention.

## Flaw table

| # | Fallacy | How it hides |
|---|---------|--------------|
| 1 | A derived constraint quoted as a constant | The billboard six is a product: roughly 2–3 usable seconds of glance time at highway speed, times roughly 3 words per second of large-type reading. Change any input — speed limit, letter height, viewing distance — and the output moves. A number that moves with its inputs is a property of the *situation*, not of *people*. The saying keeps the output and deletes the formula. |
| 2 | The regime is missing | On the highway the binding constraint is exposure: the sign leaves whether you finished or not. At a desk the reader controls the clock, and the constraint is *willingness*, not seconds. Eye-tracking of on-screen readers shows a decision process — scan the first words of a line, commit or skip — not a six-word buffer that overflows. Same behaviour class, entirely different limiting resource. |
| 3 | Borrowed authority from adjacent science | The six sounds like Miller's famous "7 ± 2" and is often defended with it. But Miller's number is about *chunks in working memory*, measured on recall tasks — not reading, not attention — and later work revised the capacity to about four chunks anyway. The two results share nothing except being small integers. |
| 4 | "Read" is never pinned down | Fixated on? Recalled verbatim? Understood? Acted on? Each definition yields a different number, measured a different way, and none of them yields six. A claim that survives only because its key verb was never defined has not been tested — it has been repeated. |
| 5 | A description enforced until it self-confirms | Once believed, the rule becomes policy: headlines, slides, and bullets get truncated at six words. Readers of six-word headlines duly stop at word six — there is no word seven. The practice manufactures the evidence that appears to support it, and the loop closes. |

## Undefined terms (orange callout)

**Undefined terms:** "read" (fixate? comprehend? recall? act on? — each gives a different count), "words" (six short familiar words and six technical terms are not the same load), "people" (drivers at highway speed? desk readers? which task, which motivation?), "only" (per glance? per headline? per page? the scope is never stated)

## Counterexamples (green callout)

**Counterexamples:**

- Billboards themselves obey the arithmetic, not the constant: posters at red lights, transit platforms, and elevators — where dwell time is minutes, not seconds — routinely carry paragraphs and work
- Motivated readers get through novels, contracts, and 3,000-word articles daily; long-form journalism sustains paid subscriptions, which a six-word attention span would make impossible
- The measured scanning result is a different number entirely: on-screen readers weigh roughly the first two words of a line or link when deciding whether to continue — the evidence supports front-loading, not a six-word cap
- Formal analogue: p&nbsp;&lt;&nbsp;0.05, "30 samples is enough," "no more than 7 menu items" — thresholds derived under specific conditions that escaped into universal law once the derivation was dropped

## Regeneration instructions

- **Template:** folk-wisdom claim-dissection card (see `ui-templates/07-claim-dissection-cards.html`). Page order: h1 (the quoted saying in quotation marks), `.subtitle` paragraph, then one `.saying-card` containing: `.saying-why` line, `.flaw-table` (columns # / Fallacy / How it hides), `.undefined-terms` callout, `.counterexamples` callout with a `<ul>`. Italicized words above (*situation*, *people*, *willingness*, *chunks in working memory*) are `<em>` in the HTML; em/en dashes appear as `&mdash;`/`&ndash;` entities. No canvases, no nav bar, no back/home links, no index number in the h1.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 1.0em; `strong` in `#1a5276`; paragraphs `#333` 0.95em.
- **Card style:** `.saying-card` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px, margin 20px 0. `.saying-why` 0.88em `#666`.
- **Flaw table style:** `.flaw-table` — full width, collapsed borders, 0.88em; th background `#f0f4f8`, `#1a5276` text, padding 8px 12px, border `1px solid #e0e0e0`; td same padding/border, vertical-align top; even rows `#fafcfe`.
- **Callout styles:** `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em. `.counterexamples` — background `#f0fff4`, left border `3px solid #27ae60`, padding 8px 14px, 0.88em; ul margin 4px 0 0 16px, li margin 3px 0. `.philosophy` (defined but unused here) — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Data:** the 2–3 seconds × ~3 words/second glance arithmetic and the ~4-chunk working-memory revision are stated as approximate figures from the outdoor-advertising and cognitive-psychology literatures respectively; no synthetic rates are presented as measured.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** none on this page; if any were added they would use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
