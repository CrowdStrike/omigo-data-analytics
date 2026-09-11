# Topic Queries vs Question Queries

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Topic Queries vs Question Queries

**Subtitle:** "rome hotels" names a subject and expects a list to browse; "which hotel is closest to the colosseum" asks for one direct answer — two query styles, two result shapes

## One Need, Two Ways to Type It

**Tags:** `core idea` (blue), `keywords vs questions` (green), `direct answers` (orange)

- **The need** — a traveler wants a place to stay in Rome, and types the need into a search box
- **Topic form** — "rome hotels": two keywords naming a subject; no verb, no grammar, no sentence
- **Question form** — "which hotel is closest to the colosseum": a full sentence asking one thing
- **List expected** — the topic form wants ten results back, to browse, compare, and click around
- **Answer expected** — the question form wants one line back: a hotel name and a distance
- **The split** — topic queries name a subject; question queries request one specific answer

*Example (italic):* Typing "rome hotels" and getting a single hotel back would feel broken; typing the colosseum question and getting ten blue links feels lazy.

**Key point:** The wording alone tells the engine what shape the result should take — a subject name earns a ranked list, a question earns one extracted answer.

### Visualization (canvas `c1`, 720×300)

Split panel: the two queries drawn as search boxes, each with an arrow down to its natural result shape — a ranked list on the left, a single answer card on the right.

- **Title (bold 15px, ink `#1a5276`, top center):** "Same Need, Two Result Shapes".
- **Divider:** vertical dashed `#bdc3c7` line (dash 4/3) at x=360 from y=38 to y=290.
- **Left panel (topic):** label "topic query" bold 12px mute `#6b7280` centered at x=180, y=46; search box rect x=50, y=54, w=260, h=32, white fill, 2px blue `#2a78d6` border, 4px feel; query text `"rome hotels"` bold 13px blue centered in the box; arrow (2px `#6b7280`, filled triangle head) from box bottom center down to y=118.
- **Ranked list:** four rows x=70, w=220, h=26, starting y=124 with 8px gaps, fill `rgba(42,120,214,0.10)`, 1px `#e5e9ef` border; row text 12px `#2c3e50` left-padded 10px: "1. Hotel A", "2. Hotel B", "3. Hotel C", "4. Hotel D"; a mute 12px "…" centered below the last row.
- **Left annotation (bold 12px blue, centered x=180, y=288):** "a ranked list to browse".
- **Right panel (question):** label "question query" bold 12px mute centered at x=540, y=46; search box rect x=395, y=54, w=290, h=32, 2px green `#008300` border; query text `"which hotel is closest to the colosseum"` bold 12px green centered; same style arrow down to y=126.
- **Answer card:** rect x=425, y=132, w=230, h=84, fill `rgba(0,131,0,0.08)`, 2px green border; inside, centered: "Hotel A" bold 14px green at y=160, "about 200 m from the Colosseum" 12px `#2c3e50` at y=180, "one fact pulled out of a page" 11px mute at y=200.
- **Right annotation (bold 12px green, centered x=540, y=288):** "one direct answer".

## Six Needs, Written Both Ways

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **Redo it** — write six everyday needs twice each: once keyword style, once as a question
- **Pair 1** — "france population" (2 words) vs "what is the population of france" (6 words)
- **Pair 2** — "reset router" (2 words) vs "how do I reset my router" (6 words)
- **Question words** — which, how, what, is, will open the question forms, never the topic forms
- **Full grammar** — question forms are complete sentences; topic forms drop verbs and articles
- **Length gap** — the six topic forms average 2.2 words; the six question forms average 5.8

*Example (italic):* "carbonara recipe" vs "how do I make carbonara" — the same need, and the question form adds a verb, a subject, and three extra words.

**Key point:** Three markers separate the forms — an opening question word, full sentence grammar, and well over double the word count.

### Visualization (canvas `c2`, 720×300)

Grouped vertical bar chart: six need pairs on the x axis, each with a blue topic-form bar and an orange question-form bar showing word counts.

- **Title (bold 15px, ink `#1a5276`, top center):** "Query Length in Words: Topic Form vs Question Form".
- **Axes:** origin x=55, baseline y=240, plot width 640, plot height 165; y axis = words 0 to 8, tick labels "0", "2", "4", "6", "8" (12px `#444`) with light `#e5e9ef` gridlines.
- **Groups (six, evenly spaced; bars 26px wide, 6px gap inside each pair):** topic values `[2, 2, 2, 2, 3, 2]` fill `rgba(42,120,214,0.55)` border blue `#2a78d6`; question values `[6, 6, 7, 6, 5, 5]` fill `rgba(217,89,38,0.55)` border orange `#d95926`.
- **Value labels:** bold 12px in the bar's border color above each bar top.
- **Group labels (12px `#444` below the baseline):** "population", "router", "hotels", "weather", "flight", "recipe".
- **Legend (12px, top right under the title):** blue swatch "topic form", orange swatch "question form".
- **Annotation (bold 13px ink `#1a5276`, centered near x=360, y=58):** "topic avg 2.2 words — question avg 5.8".
- **Caption (11px mute `#6b7280`, bottom right):** "word counts of the six example pairs".

## Two Query Styles, Two Kinds of Search Machinery

**Tags:** `where it's used` (blue), `voice search` (green), `direct answers` (orange)

- **Topic machinery** — keyword matching plus ranking: fetch pages about the subject, order them
- **Question machinery** — answer extraction: find the fact inside a page, return the sentence
- **Voice shifted it** — people speak full questions to assistants, not keyword fragments
- **Chat too** — chat-style search interfaces pull even browsing needs into question phrasing
- **Routing first** — engines classify the query style, then send it to the right machinery
- **Wrong tool cost** — a ten-link list for "is flight ba117 on time" buries a yes or no

*Example (italic):* Nobody says "rome hotels" out loud to a voice assistant — spoken queries arrive as full grammatical questions.

**Key point:** One search box now feeds two different systems, so classifying topic vs question is the first model in the pipeline.

### Visualization (canvas `c3`, 720×300)

Single line chart: share of queries phrased as full questions rising over the years, with a marker where voice assistants go mainstream. All numbers invented and labeled illustrative.

- **Title (bold 15px, ink `#1a5276`, top center):** "Share of Question-Form Queries Over Time (illustrative)".
- **Axes:** origin x=60, baseline y=240, plot width 620, plot height 165; y axis = share 0 to 30%, tick labels "0%", "10%", "20%", "30%" (12px `#444`) with light `#e5e9ef` gridlines; x axis = years `[2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024]` labeled 12px `#444` below the baseline.
- **Line:** blue `#2a78d6` 3px through share values `[8, 10, 13, 17, 20, 23, 25, 27]`, 4px blue dots at every point; bold 12px blue labels "8%" above the first point and "27%" above the last.
- **Voice marker:** vertical dashed orange `#d95926` line (dash 5/4) at year 2015; bold 12px orange label beside it, two lines: "voice assistants" / "go mainstream".
- **Annotation (bold 12px ink `#1a5276`, near the right end above the line):** "queries get longer and more grammatical".
- **Caption (11px mute `#6b7280`, bottom right):** "illustrative trend — not measured data".

## A Question Doesn't Need a Question Mark

**Tags:** `common mistake` (red), `keywords vs questions` (orange)

- **No question mark** — "closest hotel to colosseum" has no question word yet wants one answer
- **Disguise markers** — superlatives like closest, cheapest, tallest ask for a single fact
- **Questions wanting lists** — "what are good hotels in rome" is best answered with a list
- **Topic is not worse** — browsing a list is the desired outcome for shopping and research
- **Intent over surface** — classify by the result that satisfies the user, not the grammar

*Example (italic):* "cheapest flight to rome" looks like three keywords, but the user wants exactly one number back.

**Common mistake:** Reading the surface form as the intent. The reliable signal is which result shape would satisfy the user — keyword phrasing can hide a question, and a grammatical question can want a list.

### Visualization (canvas `c4`, 720×300)

2×2 grid of example queries: columns = surface form (keyword style vs full question), rows = wanted result (one answer vs a list), showing that all four combinations exist.

- **Title (bold 15px, ink `#1a5276`, top center):** "Surface Form vs Wanted Result: All Four Combinations Exist".
- **Column headers (bold 12px `#444`):** "keyword style" centered over the left column, "full question" centered over the right column, at y=48.
- **Row labels (bold 12px `#444`, right-aligned at x=98, vertically centered per row):** "one answer" (top row), "a list" (bottom row).
- **Quadrant boxes (w=290, h=95; left column x=105, right column x=415; top row y=58, bottom row y=168; 2px colored border, tinted fill at 0.08 alpha):**
  - Top-left (keyword + one answer, orange `#d95926`): query `"closest hotel to colosseum"` bold 12px, sublabel "a question in disguise" 11px orange.
  - Top-right (question + one answer, green `#008300`): query `"which hotel is closest to the colosseum"` bold 12px, sublabel "the plain question" 11px green.
  - Bottom-left (keyword + list, blue `#2a78d6`): query `"rome hotels"` bold 12px, sublabel "the plain topic" 11px blue.
  - Bottom-right (question + list, violet `#4a3aa7`): query `"what are good hotels in rome"` bold 12px, sublabel "a list IS the answer" 11px violet.
- **Query text:** bold 12px `#2c3e50` centered near each box's vertical middle; sublabel 11px in the box's border color ~20px below it.
- **Bottom annotation (bold 12px ink `#1a5276`, centered at y=290):** "classify by the wanted result, not the surface form".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` (no index number); subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** `P = { blue '#2a78d6', green '#008300', magenta '#d55181', yellow '#c98500', aqua '#199e70', orange '#d95926', violet '#4a3aa7', ink '#1a5276', text '#2c3e50', mute '#6b7280', grid '#e5e9ef' }`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values hardcoded, no randomness. c2 word counts are the true lengths of the six written query pairs: topic `[2, 2, 2, 2, 3, 2]` (avg 2.2), question `[6, 6, 7, 6, 5, 5]` (avg 5.8) for pairs population / router / hotels / weather / flight / recipe. c3 shares `[8, 10, 13, 17, 20, 23, 25, 27]` over years 2010–2024 are invented and labeled illustrative. Hotel names are generic placeholders (Hotel A–D), no invented brand names.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
