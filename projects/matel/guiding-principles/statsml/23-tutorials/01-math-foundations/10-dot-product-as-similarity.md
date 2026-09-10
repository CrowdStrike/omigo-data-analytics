# Dot Product as Similarity

**Page type:** detail page (tutorial page: h1 + subtitle, 4 `.card-section` blocks each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Dot Product as Similarity

**Subtitle:** Multiply matching entries and add them up — one number that says how much two things point the same way

## Two Friends, Four Movies, One Number

**Tags:** `core idea` (blue), `running example` (green)

- **The ratings** — Ana rates 4 movies [5, 3, 0, 1]; her friend Ben rates them [4, 4, 0, 2]
- **Multiply matching slots** — 5×4, 3×4, 0×0, 1×2 gives 20, 12, 0, 2
- **Add them up** — 20 + 12 + 0 + 2 = 34: that sum is the dot product
- **Why it works** — both love a movie: big × big adds a lot; tastes clash: small products
- **Read it as agreement** — the bigger the total, the more the two lists agree slot by slot

*Example:* Ana and Ben both rate the action movie high (5 and 4), so that single slot contributes 20 of the 34.

**Key point:** The dot product is just "multiply matching entries, add everything". Agreement in the same slots piles up the total; indifference or mismatch adds nearly nothing.

### Visualization (canvas `c1`, 720×300)

Paired rating bars per movie with per-slot products summing to 34.

- **Title (bold 15px, `#1a5276`, top center):** "Multiply Slot by Slot, Then Add: Ana · Ben = 34".
- **Data:** movies `['action', 'comedy', 'horror', 'drama']`; Ana `[5, 3, 0, 1]` in blue `#2a78d6`; Ben `[4, 4, 0, 2]` in aqua `#199e70`; per-slot products `[20, 12, 0, 2]`.
- **Bars:** grouped pairs (34px wide, 6px apart, 0.8 alpha) per movie across a 460px plot starting at x=70; baseline gray line at y=210, chart height 130px, rating scale max 5. Bold 12px value labels in the bar's color above each bar; movie names 12px `#222` below.
- **Product row (bold 13px violet `#4a3aa7`, under each group):** "5×4 = 20", "3×4 = 12", "0×0 = 0", "1×2 = 2"; below that, bold 14px violet centered: "20 + 12 + 0 + 2 = 34".
- **Legend (x=565):** blue swatch "Ana", aqua swatch "Ben"; bold 12px magenta `#d55181` insight: "one shared favorite" / "(action) contributes" / "20 of the 34"; 12px `#444`: "ratings 0-5 per movie".

## Ben vs Carl: Redo the Arithmetic by Hand

**Tags:** `worked example` (green), `cosine similarity` (blue)

- **Carl's ratings** — [0, 1, 5, 4]: he loves exactly the movies Ana skips
- **Ana · Ben** — 20 + 12 + 0 + 2 = 34: high, their high ratings line up
- **Ana · Carl** — 0 + 3 + 0 + 4 = 7: low, their enthusiasm never lands in the same slot
- **Normalize to cosine** — divide by both lengths: 34 / (5.92 × 6.00) ≈ 0.96 for Ben
- **Carl's cosine** — 7 / (5.92 × 6.48) ≈ 0.18: near 0 means unrelated taste

*Example:* Lengths by Pythagoras: |Ana| = √(25+9+0+1) = √35 ≈ 5.92, |Ben| = √36 = 6, |Carl| = √42 ≈ 6.48.

**Key point:** Cosine similarity is the dot product with the lengths divided out — a "same direction" score (1 = identical taste, 0 = unrelated). All-positive ratings keep it between 0 and 1; in general it can go down to −1.

### Visualization (canvas `c2`, 720×300)

Side-by-side product-bar panels, Ben vs Carl, with cosine results.

- **Title (bold 15px, `#1a5276`, top center):** "Same Recipe, Two Partners: 34 vs 7".
- **Divider:** vertical dashed `#bdc3c7` line at x=360.
- **Panels:** each 260px wide (left at x=60, right at x=410), baseline y=205, chart height 120px, product scale max 22, bars 44px wide at 0.75 alpha (minimum 2px height); movie names 11px `#222` below each bar; bold 12px value labels above.
  - Left, aqua `#199e70`, panel title "Ana × Ben products (slot by slot)": bars `[20, 12, 0, 2]`, "sum = 34" (bold 14px), then bold 13px violet: "cosine: 34 / (5.92×6.00) ≈ 0.96".
  - Right, orange `#d95926`, panel title "Ana × Carl products (slot by slot)": bars `[0, 3, 0, 4]`, "sum = 7", then violet: "cosine: 7 / (5.92×6.48) ≈ 0.18".
- **Bottom takeaway (bold 13px magenta, centered, y=292):** "Carl's 5s land where Ana has 0s — the products never get big".

## From One Number to a Recommendation Engine

**Tags:** `where it's used` (blue), `embeddings` (blue), `recommendations` (blue)

- **Recommendations** — score every user against Ana, borrow picks from her top match (Ben)
- **Embeddings** — models turn words, songs, and photos into vectors scored the same way
- **Search** — "find similar documents" is a dot product between the query and each document
- **Speed is the point** — one multiply-and-add per slot; billions of comparisons per second
- **Without it** — you would hand-write taste rules; the dot product learns them from the slots

*Example:* Ben rated the drama Ana has not seen a 5 — her 0.96 match with Ben makes it the obvious recommendation.

**Key point:** Nearly every "similar to" feature you have used — songs, products, people, search hits — is a dot product between two vectors somewhere under the hood.

### Visualization (canvas `c3`, 720×300)

Ranked horizontal cosine bars vs Ana, with a recommendation callout.

- **Title (bold 15px, `#1a5276`, top center):** "Rank Everyone Against Ana, Borrow From the Top Match".
- **Bars:** horizontal, 34px tall with 22px gaps, starting at x=130, full width 380px, 0.75 alpha; names bold 13px `#1a5276` right-aligned left of the axis; cosine values bold 13px in bar color at bar end:
  - Ben — 0.96 — green `#008300`
  - Dan — 0.76 — blue `#2a78d6`
  - Carl — 0.18 — orange `#d95926`
- **Axis caption (12px `#444`, centered, y=260):** "cosine similarity with Ana (0 = unrelated, 1 = identical taste)".
- **Recommendation callout (box at x=560, y=62, 150×96, fill rgba(0,131,0,0.10), 2px green border):** bold 12px green "top match: Ben"; 12px `#333` "Ben rated the" / "drama a 5 —"; bold 12px green "recommend it to Ana".
- **Annotation (bold 12px violet, below callout):** "same trick powers song," / "product, and document" / "search via embeddings".

## The Heavy Rater Trap: Big Score, Wrong Match

**Tags:** `common mistake` (red), `watch out` (orange)

- **Meet Dan** — he rates everything 5: [5, 5, 5, 5], no taste signal at all
- **Raw dot product** — Ana · Dan = 25 + 15 + 0 + 5 = 45: beats Ben's 34!
- **The catch** — Dan wins only because his numbers are big, not because they agree
- **Cosine fixes it** — Dan: 45 / (5.92 × 10) ≈ 0.76; Ben keeps the lead at 0.96
- **Rule** — raw dot product mixes "similar direction" with "just loud"; divide lengths out

*Example:* Dan's length is √100 = 10 — nearly twice Ben's 6 — and that loudness alone inflated his raw score.

**Common mistake:** Ranking matches by raw dot product. A heavy rater (or long document, or active user) tops every list — use cosine when you mean "similar taste", not "big numbers".

### Visualization (canvas `c4`, 720×300)

Two panels: raw dot product vs cosine — Dan wins left, Ben wins right.

- **Title (bold 15px, `#1a5276`, top center):** "Rate-Everything-5 Dan Wins on Raw Score, Loses on Angle".
- **Divider:** vertical dashed `#bdc3c7` line at x=360.
- **Panels:** each 250px wide (left at x=60, right at x=410), baseline y=212, chart height 130px, two bars 70px wide (Ben in aqua `#199e70`, Dan in violet `#4a3aa7`, 0.75 alpha), bold 14px value labels above, names 12px below, bold 12px green "winner" tag under the winning bar:
  - Left, title "raw dot product with Ana": values `[34, 45]`, scale max 50, winner Dan; note (bold 12px orange, y=272): "loudness wins: |Dan| = 10 vs |Ben| = 6".
  - Right, title "cosine (lengths divided out)": values `[0.96, 0.76]` (2-decimal labels), scale max 1.1, winner Ben; note (bold 12px green): "direction wins: Ben agrees, Dan is just loud".
- **Bottom takeaway (bold 13px magenta, centered, y=294):** "45 > 34 but 0.76 < 0.96 — divide out the lengths before you rank".

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference skeleton). `<h1>` (no index number), `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` with bottom border `2px solid #2980b9` and a `table.layout` with `.text-col` (50%) and `.viz-col` (50%) cells, 12px padding.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) whose `<strong>` lead is "Key point:" or "Common mistake:".
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue = bg rgba(26,82,118,0.12)/text `#1a5276`; green = bg rgba(39,174,96,0.15)/text `#27ae60`; red = bg rgba(231,76,60,0.12)/text `#e74c3c`; orange = bg rgba(230,126,34,0.15)/text `#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 0.95rem; h2 1.3rem `#1a5276`; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each 720×300 logical, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Shared data arrays in script scope: `movies = ['action','comedy','horror','drama']`, `ana = [5,3,0,1]`, `ben = [4,4,0,2]`, `carl = [0,1,5,4]`. All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
