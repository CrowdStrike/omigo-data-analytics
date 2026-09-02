# Search Queries & Navigation

**Page type:** detail page (most-powerful-signals compact style: per-section two-column layout table, text left 45% with tag pills / labeled bullets / example / key-point, canvas right 55%)
**HTML title tag:** Search Queries & Navigation — Queries as a Data Science Signal

**Subtitle:** What a query contributes as a signal — intent labels, relevance feedback, training data, coverage classes, and aggregate demand trends, whether typed into a box or compiled from a profile.

## What a Query Is: Intent Written Down

**Tags:** `signal` (blue), `mechanism` (blue)

- **Definition** — a few words typed into a search box to express a need
- **Short by nature** — most web queries are 2-3 keywords, not full sentences
- **Beyond typing** — category clicks, filters, and voice questions are queries too
- **A log row** — each query is recorded with time, user, results shown, and clicks
- **The raw signal** — the query log is the input to every model on this page

*"pizza near me" states what, where, and when in three words — and becomes one row in the query log.*

**Key point:** A query is user intent written down by the user — the query log is the dataset everything below is mined from.

### Visualization (canvas `c1`, 720×300)

Anatomy diagram: a drawn search box and the query-log row it creates.

- **Title (bold 14px `#1a5276`, top center):** "A Query and the Log Row It Creates".
- **Search box (centered, 360×44 rounded rect at y=48):** stroke `#1a5276` 2px, fill white; magnifier icon (10px-radius circle + handle stroke `#1a5276`) at the left end; 14px `#2c3e50` text "pizza near me".
- **Arrow:** gray `#999` vertical arrow from the search box down to the record card.
- **Log-row card (centered, 560×120 rounded rect at y=140):** stroke `#27ae60` 2px, fill `rgba(39,174,96,0.05)`; bold 11px `#27ae60` header "ONE ROW IN THE QUERY LOG"; two columns of field/value pairs (bold 11px `#1a5276` field names, 11px `#2c3e50` values): "time: 18:42:07", "user: hashed id", "query: pizza near me", "location: city level", "shown: 10 results", "clicked: result #2, 95s dwell".
- **Caption (bottom center, 11px `#1a5276`):** "each search becomes one row — billions of rows a day at a large engine".

## Query Intent: The First Classification

**Tags:** `signal` (blue), `mechanism` (blue)

- **Three classic types** — navigational ("gmail login"), informational ("how to boil eggs"), transactional ("running shoes size 10")
- **Broder taxonomy** — the split comes from Andrei Broder's 2002 web-search study
- **Different results** — each type calls for a different result page and ranking
- **Classify first** — engines run an intent model before ranking documents
- **Structured feature** — the intent label turns raw text into model input

*"amazon" is navigational; "amazon rainforest facts" is informational — two extra words change the intent class.*

**Key point:** Intent classification is the first ML model a query touches — every downstream ranking decision depends on it.

### Visualization (canvas `c2`, 720×300)

Flow diagram: three example query boxes feeding an intent-classifier box that branches into three labeled intent classes.

- **Title (bold 14px `#1a5276`, top center):** "One Query, Three Intent Classes — Classified Before Ranking".
- **Query boxes (left column, 170×36 at x=30, y=70/135/200):** '"gmail login"', '"how to boil eggs"', '"running shoes size 10"' — stroke `#1a5276`, fill `rgba(26,82,118,0.08)`, 11px `#2c3e50` centered text.
- **Classifier box (center, rounded 150×60 centered at 350,153):** fill `rgba(142,68,173,0.10)`, 2px `#8e44ad` stroke; bold 12px `#8e44ad` "INTENT" / "CLASSIFIER".
- **Class boxes (right column, 230×52 at x=460, y=58/126/194):** "NAVIGATIONAL — go to a site" (sub "best result: the site itself") stroke `#1a5276`; "INFORMATIONAL — learn something" (sub "best result: articles, answers") stroke `#27ae60`; "TRANSACTIONAL — do or buy" (sub "best result: products, prices") stroke `#e67e22`; bold 11px class line in stroke color, 10px `#666` sub-line, fills at 0.08 alpha of each stroke color.
- **Arrows:** gray `#999` from each query box to the classifier, and from the classifier to each class box (matching row order).
- **Caption (bottom center, 11px `#1a5276`):** "the intent class decides what kind of result page to build".

## Broad Queries: One String, Many Intents

**Tags:** `signal` (blue), `trade-off` (orange)

- **Ambiguity** — "jaguar": the animal, the car brand, or the sports team
- **Intent distribution** — click logs reveal what share of users mean each sense
- **Diversification** — ranking hedges by covering all major senses in the top results
- **Context narrows** — location, session history, and prior queries shift the odds
- **ML framing** — the model outputs a probability per intent, not a single label

*For "apple", clicks concentrate on the company, yet recipe slots keep the fruit intent served.*

**Key point:** Broad queries force ranking to optimize over a distribution of intents rather than a single answer.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of click share by intent for one broad query, with a diversification note.

- **Title (bold 14px `#1a5276`, top center):** 'Broad Query "jaguar" — Click Share by Intent'.
- **Bars (horizontal, start x=190, max width 340, 34px tall, 56px pitch from y=70):** "Car brand" 55% fill `rgba(26,82,118,0.35)` stroke `#1a5276`; "Animal" 30% fill `rgba(39,174,96,0.35)` stroke `#27ae60`; "Sports team" 15% fill `rgba(230,126,34,0.35)` stroke `#e67e22`. Bold 12px percent labels in stroke color at bar ends; 12px `#2c3e50` intent labels at left.
- **Annotation (bold 11px `#8e44ad`, right side, with arrow toward the bars):** "ranking hedges: all three" / "senses appear in the top results".
- **Caption (bottom center, 11px `#1a5276`):** "the model predicts a probability per intent — context shifts the distribution".

## Sessions & Refinements: Queries as Relevance Feedback

**Tags:** `signal` (blue), `best practice` (green)

- **Session** — one user's queries in one sitting; a ~30-minute gap starts a new one
- **Refinement chain** — each added word shows what the last results were missing
- **Free audit** — 25-33% of session queries are reformulations
- **Implicit labels** — rewrite-then-click pairs train synonym and spelling models
- **Label carefully** — exploration and topic drift look like failure but are not

*"printer not working" → "hp envy 6055 offline windows 11" → satisfied click hands the engine a synonym map.*

**Key point:** The session, not the single query, is the unit of relevance feedback.

### Visualization (canvas `c4`, 720×300)

Two-part diagram: a rewrite chain of query boxes (top) and a survival funnel of sessions still searching (bottom).

- **Title:** "One Session: Rewrite Chain (top) and Sessions Still Searching (bottom)".
- **Chain (140×44 boxes, 42px gaps, centered, y=40, orange arrows between):** '"printer not / working"' → '"hp envy printer / offline"' → '"hp envy 6055 offline / windows 11"' → "CLICK + / long dwell" (final box green `#27ae60` on `rgba(39,174,96,0.15)`; others blue `#1a5276` on `rgba(26,82,118,0.08)`). Arrow labels bold 10px orange: "adds brand", "adds model + OS"; under final box "satisfied endpoint"; blue 10px note "each added term = what the engine was missing".
- **Funnel bars (88px wide, 38px gaps, centered, baseline h-35, max height 105):** Query 1 100% (blue-tint `rgba(26,82,118,0.35)`), Rewrite 1 38% (orange `#e67e22`), Rewrite 2 17% (orange), Rewrite 3 9% (red `#e74c3c`), Rewrite 4+ 5% (red); bold percent labels above, names 11px below.
- **Annotation (bold 11px green):** "62% satisfied first try —" / "each rewrite is a feedback signal".

## Clicks as Labels: The Core Training Data

**Tags:** `signal` (blue), `bias` (orange)

- **Free labels** — billions of (query, click) pairs daily, no human raters
- **Position bias** — rank 1 CTR can be 4-10x rank 5 at equal relevance
- **Debiasing** — propensity weighting and randomized swaps correct the skew
- **Dwell time** — time on the clicked page separates satisfaction from a bounce
- **Risk** — training on raw clicks freezes the current ranking in place

*After propensity weighting, a better result buried at rank 6 finally rises.*

**Key point:** Query-click joins are the largest labeled dataset most companies own — after debiasing.

### Visualization (canvas `c5`, 720×300)

Two-line chart: observed CTR vs true relevance by result position, with a gap annotation.

- **Title:** "Position Bias: Observed Click Rate Falls Far Faster Than True Relevance".
- **Plot:** x=60, width 500, y 45–245, y max 45; positions 1–10 ticked below with axis label "result position" (10px `#666`).
- **Series:** observed CTR solid red `#e74c3c` `[40, 18, 10, 7, 5, 4, 3, 2.5, 2, 1.8]`, width 3; true relevance dashed green `#27ae60` (dash 6/4) `[34, 31, 29, 27, 26, 24, 23, 22, 21, 20]`, width 3.
- **Gap annotation:** orange `#e67e22` vertical arrow at position 5 between the two curves, labeled bold 11px "this gap is pure position bias," / "not a relevance difference".
- **Legend (right of plot):** red swatch "observed CTR %", green swatch "true relevance".
- **Caption (bottom center, bold 11px `#1a5276`):** "Rank 1 clicked 4-10x more than rank 5 at equal relevance — debias before training".

## Head, Torso, Tail: Coverage Classes for ML

**Tags:** `signal` (blue), `best practice` (green)

- **Head** — few queries, most volume, rich click history; can be hand-tuned
- **Torso** — moderately common queries with thin history; needs generalization
- **Tail** — most distinct queries; ~15% of daily queries are brand new
- **Semantic retrieval** — tail coverage is what drove BERT-style ranking models
- **Zero results** — tail queries returning nothing map catalog and synonym gaps

*Search returns nothing for "hoodie" because the catalog says "hooded sweatshirt" — zero-result logs flag it in a day.*

**Key point:** Head performance measures memorization; tail performance measures the model — report them separately.

### Visualization (canvas `c6`, 720×300)

Zipf rank-frequency bar chart with head/torso/tail zones.

- **Title:** "Query Frequency by Rank — Head, Torso, Tail".
- **Bars:** 60 bars, height ∝ 1/(rank+1) (min 3px); plot x=55, width 620, y 55–230. First 6 bars (head) fill `rgba(26,82,118,0.65)`; bars 7-24 (torso) fill `rgba(230,126,34,0.45)`; the rest (tail) fill `rgba(231,76,60,0.35)`.
- **Boundaries:** dashed gray `#bdc3c7` vertical lines (dash 5/4) after bar 6 and bar 24.
- **Head annotation (blue, arrow to the first bars):** bold 11px "HEAD: few queries, most volume" plus 10px "rich click history — memorize and hand-tune".
- **Torso annotation (orange):** bold 11px "TORSO: thin history" plus 10px "needs generalization".
- **Tail annotation (red, arrow into the tail):** bold 11px "TAIL: most distinct intents" plus 10px "~15% never seen before — needs semantic retrieval".
- **X label (10px `#666`):** "query rank by frequency".
- **Caption (bottom center, bold 11px `#e67e22`):** "Volume-weighted averages hide tail failure — report head and tail slices separately".

## Query Trends: Aggregate Demand & Culture

**Tags:** `signal` (blue), `trade-off` (orange)

- **Leading indicator** — query volume moves before sales, visits, or case counts
- **Seasonal rhythm** — "flu symptoms", "tax filing", "gift ideas" cycle every year
- **Regional signature** — top queries differ by city and country, reflecting local culture
- **Nowcasting** — central banks track unemployment-related query volume
- **Forecastable** — launch surges hit every guessed model name, and the date is known in advance
- **Act ahead** — pre-cache the surge, stock inventory early, mine zero-result demand
- **Proxy caution** — media coverage moves queries without underlying change; Google Flu Trends overshot ~2x

*Recipe queries for regional dishes spike before local festivals — the query log sees a culture's calendar.*

**Key point:** Trends are a powerful aggregate signal but still a proxy — recalibrate continuously against ground truth.

### Visualization (canvas `c7`, 720×300)

Two seasonal query-volume curves across twelve months.

- **Title:** "Query Volume by Month — Seasonal and Cultural Rhythms".
- **Plot:** x=60, width 490, y 45–245, y max 80; month initials J-D ticked below, x label "month" (10px `#666`).
- **Series (12 points, width 3):** "flu symptoms" blue `#1a5276` `[70, 55, 35, 20, 12, 8, 7, 8, 14, 28, 45, 65]`; "sunscreen" orange `#e67e22` `[8, 10, 15, 25, 45, 68, 75, 70, 40, 20, 10, 8]`.
- **Annotation (bold 10px `#27ae60`, arrow at the flu curve's autumn rise):** "volume rises before clinic visits —" / "a leading indicator".
- **Legend (right of plot):** blue swatch '"flu symptoms"', orange swatch '"sunscreen"'.
- **Caption (bottom center, bold 11px `#1a5276`):** "Aggregate trends reveal demand and culture — but media can move queries without underlying change".

## Beyond Typed Words: Compiled Queries & Chips

**Tags:** `signal` (blue), `mechanism` (blue)

- **Compiled queries** — a job feed rewrites your profile into an elaborate query you never see
- **Both directions** — the job posting is itself a query run over candidate profiles
- **Chips are words** — "red shoes" + a size-11 filter click equals typing "red shoes size 11"
- **Clarification cards** — for a broad query, the page asks which intent you meant
- **Labels for free** — each chip click logs the chosen intent, training data with no raters

*A "jobs for you" feed and a "because you watched" row are both searches — with a profile, not words, in the query slot.*

**Key point:** The query signal extends past the search box — profiles, items, and chip clicks all take the query's role and feed the same models.

### Visualization (canvas `c8`, 720×300)

Split panel: left shows a profile compiling into a generated query against an index; right shows the chip-click equivalence — typed words and a filter click converging on the same query.

- **Title (bold 14px `#1a5276`, top center):** "Two Query Signals Nobody Typed".
- **Divider:** dashed gray `#bdc3c7` vertical line at x=360, y=42 to y=262 (dash 5/4).
- **Left header (bold 11px `#1a5276`, centered at x=185, y=56):** "PROFILE AS QUERY".
- **Profile box (x=35, y=72, w=120, h=96):** stroke `#1a5276` 2px, fill `rgba(26,82,118,0.08)`; bold 11px `#1a5276` centered "profile" at y=92; three 10px `#2c3e50` lines centered at y=112/128/144: "data engineer", "Spark · SQL", "5 yrs · remote ok".
- **Arrow:** gray `#999` from (158, 120) to (196, 120).
- **Generated-query box (x=200, y=64, w=140, h=112):** stroke `#27ae60` 2px, fill `rgba(39,174,96,0.05)`; bold 10px `#27ae60` centered header "GENERATED QUERY" at y=82; three 10px `#2c3e50` lines left-aligned at x=210, y=104/122/140: "title ~ data engineer", "skills: spark, sql", "location: remote"; caption 10px `#666` centered at x=270, y=162: "never shown to the user".
- **Left caption (11px `#1a5276`, centered at x=185, y=232):** "the posting queries candidates the same way — roles swap".
- **Right header (bold 11px `#1a5276`, centered at x=540, y=56):** "CHIPS ARE QUERY WORDS".
- **Typed+chip row:** query box (x=395, y=76, w=110, h=32, stroke `#1a5276` 2px, white fill) 11px `#2c3e50` centered '"red shoes"'; plus sign bold 13px `#666` at (517, 96); chip (x=530, y=80, w=82, h=24, stroke `#e67e22` 1.5px, fill `rgba(230,126,34,0.15)`) bold 10px `#e67e22` centered "size: 11".
- **Typed-only row:** query box (x=395, y=136, w=180, h=32, stroke `#1a5276` 2px, white fill) 11px `#2c3e50` centered '"red shoes size 11"'.
- **Converge arrows:** gray `#999` from (618, 96) and from (578, 152) to the equals box.
- **Equals box (x=560, y=188, w=140, h=44):** stroke `#8e44ad` 2px, fill `rgba(142,68,173,0.10)`; bold 11px `#8e44ad` centered lines "same query" (y=206), "to the engine" (y=222).
- **Right caption (10px `#666`, centered at x=540, y=252):** "the chip arrives pre-parsed, as an exact constraint".
- **Caption (bottom center, 11px `#1a5276`, y=288):** "profiles, items, and chip clicks all take the query's role in the same models".

## Regeneration instructions

- **Layout:** one `.card-section` per section, each containing an `<h2>` (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` with a single `<tr>`: left `td.text-col` (45%) holding `.tags` pills, `<ul>` bullets, `p.example`, `.key-point`; right `td.viz-col` (55%) with one `<canvas width="720" height="300">` styled `width:100%`, border `1px solid #e0e0e0`, radius 4px.
- **Page style:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem.
- **Canvas:** shared `setup(id)` helper scaling by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); shared `drawArrow(ctx, x1, y1, x2, y2, color)` helper for annotation arrows; one IIFE per chart. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML any links use `.html` extensions.
