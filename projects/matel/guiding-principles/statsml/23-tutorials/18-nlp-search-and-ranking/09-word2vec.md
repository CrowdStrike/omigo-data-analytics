# Word2vec

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Word2vec

**Subtitle:** Word2vec gives every word a point on a map, placed so that words used in the same sentences land near each other — and directions on the map carry meaning, so king − man + woman lands almost exactly on queen

## Guessing the Missing Word in a Fairy Tale

**Tags:** `core idea` (blue), `words by company` (green), `fill in the blank` (orange)

- **The blank** — read a fairy tale aloud and pause: "the ___ sat on the throne" — everyone guesses king or queen
- **The company** — you guessed from the neighbors alone; words used around the same neighbors do the same job
- **The trick** — word2vec plays this fill-in-the-blank game millions of times over a big pile of text
- **The reward** — each round it nudges the word's numbers so the right neighbors get easier to guess
- **The result** — king and queen end up with nearly the same numbers because they keep the same company

*Example (italic):* Across a shelf of fairy tales, "king" appears next to "throne" 30 times and "queen" 28 times — near-twin neighbor counts, so the two words get near-twin positions.

**Key point:** Word2vec never reads a dictionary — it places each word by the company it keeps, and words with the same neighbors end up side by side.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: five neighbor words on the x axis, and for each one a pair of bars — how often it appears next to "king" (blue) vs next to "queen" (green) — showing the two profiles are near twins.

- **Title (bold 15px, `#1a5276`, top center):** "'king' and 'queen' Keep the Same Company".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y axis = times seen next to the word, 0 to 32 with light `#e5e9ef` gridlines at 8, 16, 24, 32 and 12px `#444` tick labels; x axis = five neighbor words with 12px `#444` labels below: `["throne", "crown", "castle", "said", "royal"]`.
- **Bars:** for each neighbor a pair of 34px-wide bars, 6px apart; blue `#2a78d6` = next to "king", counts `[30, 24, 21, 18, 15]`; green `#008300` = next to "queen", counts `[28, 25, 19, 20, 14]`; 12px count labels in the bar's color above each bar.
- **Legend (12px, top right inside plot):** blue swatch "next to 'king'", green swatch "next to 'queen'".
- **Annotation (bold 12px orange `#d95926`, centered near y=70):** two lines: "near-twin neighbor counts →" / "near-twin positions on the map".
- **Caption (12px `#444`, bottom right):** "illustrative counts from a shelf of fairy tales".

## The Word Map: king − man + woman = queen

**Tags:** `worked example` (blue), `word arithmetic` (green)

- **The map** — give each word just two numbers (illustrative): king (8, 9), man (8, 2), woman (2, 2), queen (2, 9)
- **Read the axes** — moving down loses royalty (king → man), moving left swaps gender (man → woman)
- **Subtract** — king − man = (8, 9) − (8, 2) = (0, 7): the arrow that means "royalty" on this map
- **Add** — woman + (0, 7) = (2, 2) + (0, 7) = (2, 9): start at woman, walk up the royalty arrow
- **Check** — (2, 9) is exactly where queen sits; the same trick fetches Paris − France + Italy ≈ Rome
- **Real size** — real word2vec uses 100–300 numbers per word, but the arithmetic is this same add-and-subtract

*Example (italic):* On paper: (8, 9) − (8, 2) + (2, 2) = (2, 9) — three subtractions and additions land you on queen's exact spot.

**Key point:** king − man + woman = (2, 9) = queen — directions on the map act like reusable pieces of meaning you can add and subtract.

### Visualization (canvas `c2`, 720×300)

Single-panel 2D word map: four labeled word dots at their coordinates, with a two-step arrow path showing the walk king → (− man) → (+ woman) landing on queen.

- **Title (bold 15px, `#1a5276`, top center):** "The Word Map: king − man + woman Lands on queen".
- **Axes:** origin x=180, baseline y=250, plot width 400, plot height 200 (map coordinates 0–10 on both axes, so 40px per map unit horizontally, 20px vertically); 2px `#999` axis lines; 12px `#444` axis titles "gender direction →" below the x axis and "royalty direction ↑" rotated along the y axis; light `#e5e9ef` gridlines every 2 map units.
- **Word dots (8px), each with a bold 13px label offset 10px away:** king blue `#2a78d6` at (8, 9); man blue at (8, 2); woman green `#008300` at (2, 2); queen green at (2, 9).
- **Arrow 1 (− man):** violet `#4a3aa7` 3px arrow with arrowhead from king (8, 9) down to (8, 2); 12px violet label midway at its right: "− man".
- **Arrow 2 (+ woman path):** violet 3px dashed (dash 6/4) arrow from (8, 2) to (2, 2), then violet 3px solid arrow from (2, 2) up to (2, 9) with arrowhead ending in a hollow 12px violet ring around queen's dot; 12px violet label "+ woman, walk the royalty arrow" beside the rising arrow.
- **Annotation (bold 13px violet `#4a3aa7`, near map point (4.5, 10), i.e. above the plot center):** "(8,9) − (8,2) + (2,2) = (2,9) = queen".
- **Caption (12px `#444`, bottom right):** "2-number map, illustrative — real vectors use 100–300 numbers".

## Where a Search Box Uses It

**Tags:** `where it's used` (blue), `search & ranking` (green), `features` (orange)

- **The old way** — keyword search for "queen" misses a page that only ever says "her majesty"
- **The new way** — compare positions on the map instead of spellings; nearby words count as matches
- **Similarity score** — closeness on the map becomes a 0-to-1 score: queen scores 0.85 next to king, bread 0.07
- **Ranking** — pages full of words near the query's words float up, even with zero exact keyword hits
- **Beyond search** — the same numbers feed recommenders, spam filters, and any model that eats words

*Example (italic):* A search for "king" also surfaces a page about "the queen's coronation" because queen (0.85) and throne (0.68) sit close to king on the map.

**Key point:** Once words are points on a map, "does this page match the query" becomes "how close are the points" — search by meaning, not spelling.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: five candidate words ranked by their map-closeness score to the query word "king", with a cut line separating real matches from noise.

- **Title (bold 15px, `#1a5276`, top center):** "Closest Words to the Query 'king' (map closeness, 0–1)".
- **Axes:** bars start at x=160, max bar width 480 (score 1.0 = 480px); rows top to bottom at y = 70, 105, 140, 175, 210, each 22px tall; x axis line 2px `#999` at y=240 with 12px `#444` tick labels "0", "0.25", "0.5", "0.75", "1.0"; left-aligned 13px `#2c3e50` word labels at x=20.
- **Bars and scores (13px bold score label in the bar's color at each bar's right end):** queen green `#008300` 0.85; prince green 0.79; throne green 0.68; man orange `#d95926` 0.41; bread mute `#6b7280` 0.07.
- **Cut line:** vertical dashed `#6b7280` (dash 4/3) line at score 0.5 from y=55 to the axis; 11px `#6b7280` label at its top: "match cut-off 0.5".
- **Annotation (bold 12px green `#008300`, near x=420, y=180):** two lines: "'queen' scores 0.85 with zero" / "letters shared with 'king'".
- **Caption (12px `#444`, bottom right):** "illustrative closeness scores".

## What the Map Does Not Know

**Tags:** `common mistake` (red), `close ≠ synonym` (orange)

- **No dictionary** — the map only records shared company; it has never seen a definition of any word
- **Opposites hug** — "hot" and "cold" fill the same blanks ("___ water", "___ weather"), so they sit close: 0.80
- **The trap** — reading closeness as sameness; a synonym-swapper built this way swaps hot for cold
- **One dot per word** — "bank" gets a single point stuck between river banks and money banks
- **Read it right** — closeness means "used in the same slots", which covers synonyms and their opposites

*Example (italic):* On the same illustrative map, hot–warm scores 0.83 and hot–cold scores 0.80 — nearly tied, yet one pair means the same and the other the opposite.

**Common mistake:** Treating map closeness as synonymy. Word2vec puts words with interchangeable neighbors together — and opposites are the most interchangeable words of all.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: four word pairs and their closeness scores to "hot", showing the opposite ("cold") scoring almost as high as the synonym ("warm").

- **Title (bold 15px, `#1a5276`, top center):** "Closeness to 'hot' — the Opposite Scores Almost Like the Synonym".
- **Axes:** bars start at x=170, max bar width 470 (score 1.0 = 470px); rows top to bottom at y = 75, 115, 155, 195, each 24px tall; x axis line 2px `#999` at y=235 with 12px `#444` tick labels "0", "0.25", "0.5", "0.75", "1.0"; left-aligned 13px `#2c3e50` pair labels at x=20: "warm (synonym)", "cold (opposite)", "oven (related)", "sofa (unrelated)".
- **Bars and scores (13px bold label in the bar's color at each bar's right end):** warm green `#008300` 0.83; cold red `#e74c3c` 0.80; oven orange `#d95926` 0.55; sofa mute `#6b7280` 0.05.
- **Bracket:** thin 2px `#e74c3c` bracket joining the right ends of the warm and cold bars, with bold 12px red `#e74c3c` label to its right on two lines: "0.83 vs 0.80" / "— near tie".
- **Annotation (bold 12px red `#e74c3c`, centered near y=265):** "close on the map = same company, not same meaning".
- **Caption (12px `#444`, bottom right):** "illustrative closeness scores".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar counts, map coordinates, and closeness scores are the hardcoded arrays and values above (no randomness); the c2 arithmetic must reproduce (8,9) − (8,2) + (2,2) = (2,9) exactly; all invented numbers keep their "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
