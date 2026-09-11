# Positional Encoding

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Positional Encoding

**Subtitle:** Attention looks at all the words at once, like tiles poured out of a bag — positional encoding stamps each word with its seat number so the model can still tell who did what to whom

## Same Words, Opposite Meanings

**Tags:** `core idea` (blue), `word order` (green), `seat stamps` (orange)

- **Two sentences** — "Sam thanked Alex" and "Alex thanked Sam" use exactly the same three words
- **The flip** — one sentence credits Sam with the thanks, the other credits Alex; order is the meaning
- **The bag** — attention reads every word at the same time, like tiles poured from a bag, order lost
- **The fix** — before any reading happens, stamp each word with its seat number: seat 1, 2, 3
- **The name** — that seat stamp, mixed into each word's numbers, is the positional encoding

*Example (italic):* To a bag-of-tiles reader, "Sam thanked Alex" and "Alex thanked Sam" are the same message — only the stamps "Sam, seat 1" vs "Sam, seat 3" keep them apart.

**Key point:** A positional encoding is a number pattern added to each word so a model that sees all words at once still knows the order they arrived in.

### Visualization (canvas `c1`, 720×300)

Tile diagram: the two sentences drawn as rows of word tiles, both feeding into one shared bag that holds the same three tiles — showing that without seat stamps the two sentences collapse into the identical input.

- **Title (bold 15px, `#1a5276`, top center):** "Two Sentences, One Bag of Tiles".
- **Row labels (12px `#444`, left at x=20):** "sentence A" at y=78, "sentence B" at y=138.
- **Row A (tiles y=60, height 34):** three rounded rects 130×34 at x = 110, 260, 410; texts bold 13px centered: "Sam" (fill `rgba(42,120,214,0.20)`, border `#2a78d6`), "thanked" (fill `rgba(107,114,128,0.15)`, border `#6b7280`), "Alex" (fill `rgba(0,131,0,0.15)`, border `#008300`).
- **Row B (tiles y=120, height 34):** same three tile styles in flipped order at x = 110, 260, 410: "Alex" (green), "thanked" (gray), "Sam" (blue).
- **Arrows:** two 2px `#6b7280` arrows with small arrowheads, from (330, 100) and (330, 160) down-right into the bag's top edge near (330, 195).
- **Bag:** rounded rect from (180, 195) to (500, 275), fill `#f8f9fa`, dashed 2px `#6b7280` border; inside, the same three tiles at 60% size, deliberately tilted/staggered ("Alex" near x=205 y=215, "Sam" near x=305 y=232, "thanked" near x=395 y=212); 12px `#6b7280` label under the bag's top edge: "what attention sees".
- **Annotation (bold 13px magenta `#d55181`, right side near x=520, y=210, two lines):** "same three tiles —" / "order is already gone".
- **Caption (12px `#444`, bottom right):** "illustrative — tiles stand for the words' internal numbers".

## Stamping Sam with a Seat Number

**Tags:** `worked example` (blue), `just addition` (green)

- **Sam's numbers** — inside the model "Sam" is a short list of numbers, say [0.5, 0.2], wherever it sits
- **The problem** — unstamped, Sam in seat 1 and Sam in seat 3 look identical: [0.5, 0.2] both times
- **Seat stamps** — each seat gets its own pair: seat 1 = [0.84, 0.54], seat 3 = [0.14, -0.99]
- **Add them** — Sam in seat 1 becomes [0.5+0.84, 0.2+0.54] = [1.34, 0.74]
- **Now different** — Sam in seat 3 becomes [0.5+0.14, 0.2-0.99] = [0.64, -0.79]; the two Sams no longer match

*Example (italic):* Same word, two seats: [1.34, 0.74] vs [0.64, -0.79] — one addition per seat and the flip between the two sentences becomes visible.

**Key point:** Positional encoding is plain addition — word numbers + seat numbers = position-aware numbers, done once before any attention runs.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart with two clusters — "Sam in seat 1" and "Sam in seat 3" — each showing three bar pairs (word, seat stamp, word+stamp) for the word's two numbers, making the addition and the resulting difference visible.

- **Title (bold 15px, `#1a5276`, top center):** "Word + Seat Stamp = Position-Aware Numbers".
- **Axes:** zero line 2px `#999` at y=164 from x=60 to x=680; vertical scale 68px per unit; light `#e5e9ef` gridlines at values +1.0 (y=96) and -1.0 (y=232) with 12px `#444` labels "+1" and "-1" at x=45.
- **Cluster A (centered x=220), cluster B (centered x=540):** bold 13px `#1a5276` labels below the plot at y=272: "Sam in seat 1", "Sam in seat 3".
- **Each cluster:** three groups left to right — "word", "stamp", "sum" (12px `#444` labels at y=254) — each group two bars 16px wide, 4px apart (first number solid fill, second number same color at 0.45 alpha), groups 24px apart.
- **Cluster A values:** word `[0.5, 0.2]` blue `#2a78d6`; stamp `[0.84, 0.54]` orange `#d95926`; sum `[1.34, 0.74]` green `#008300`.
- **Cluster B values:** word `[0.5, 0.2]` blue; stamp `[0.14, -0.99]` orange; sum `[0.64, -0.79]` green.
- **Value labels:** 11px `#444` at each bar's far end (above positive bars, below negative bars): "0.5", "0.2", "0.84", "0.54", "1.34", "0.74" for cluster A; "0.5", "0.2", "0.14", "-0.99", "0.64", "-0.79" for cluster B.
- **Annotation (bold 12px green `#008300`, centered near x=370, y=70, two lines):** "same word, different seats" / "→ different numbers".
- **Caption (12px `#444`, bottom right):** "word numbers illustrative; stamps are sin(seat), cos(seat)".

## A Ruler Made of Waves

**Tags:** `where it's used` (blue), `many scales` (green), `stays small` (orange)

- **Real stamps** — the original transformer read them off waves: sin(pos), sin(pos/3), sin(pos/9)
- **Fast wave** — sin(pos) changes a lot from one seat to the next, so it tells neighbors apart
- **Slow waves** — sin(pos/9) barely moves per seat, so it tells seat 2 from seat 200, chapter-scale
- **Stays small** — every wave lives between -1 and +1, so seat 500 is stamped as gently as seat 5
- **Neighbors alike** — nearby seats get nearly the same stamp, so "the next word" feels close to the model

*Example (italic):* Like a ruler — millimeter ticks tell seat 6 from seat 7, centimeter and meter ticks tell page 1 from page 9 — three wave speeds do all the jobs at once.

**Key point:** Waves at many speeds give every seat a unique, bounded fingerprint — that is why word order still works at seat 10,000, deep into a long document.

### Visualization (canvas `c3`, 720×300)

Line chart of three sine waves over seat positions 0–20 — one fast, one medium, one slow — showing how each seat's stamp is the combination of readings at several scales, all bounded by ±1.

- **Title (bold 15px, `#1a5276`, top center):** "Three Wave Speeds — Every Seat Gets a Unique Fingerprint".
- **Axes:** origin x=60, zero line 1px `#999` at y=150, positions 0–20 across plot width 600 (30px per position); vertical scale 80px per unit (+1 at y=70, -1 at y=230); light `#e5e9ef` gridlines at +1 and -1 with 12px `#444` labels "+1", "0", "-1" at x=42; x tick labels "0", "5", "10", "15", "20" (12px `#444`) below y=245, x-axis title 12px `#444` "seat position" centered at y=272.
- **Fast wave sin(pos):** blue `#2a78d6` 2.5px line through positions 0–20 with values `[0, 0.84, 0.91, 0.14, -0.76, -0.96, -0.28, 0.66, 0.99, 0.41, -0.54, -1.00, -0.54, 0.42, 0.99, 0.65, -0.29, -0.96, -0.75, 0.15, 0.91]`; 12px blue label "sin(pos) — fast" near x-position 2, above the curve.
- **Medium wave sin(pos/3):** orange `#d95926` 2.5px line, values `[0, 0.33, 0.62, 0.84, 0.97, 1.00, 0.91, 0.72, 0.46, 0.14, -0.19, -0.50, -0.76, -0.93, -1.00, -0.96, -0.82, -0.59, -0.28, 0.06, 0.37]`; 12px orange label "sin(pos/3)" near x-position 5, above its peak.
- **Slow wave sin(pos/9):** green `#008300` 2.5px line, values `[0, 0.11, 0.22, 0.33, 0.43, 0.53, 0.62, 0.70, 0.78, 0.84, 0.90, 0.94, 0.97, 0.99, 1.00, 1.00, 0.98, 0.95, 0.91, 0.86, 0.80]`; 12px green label "sin(pos/9) — slow" near x-position 17, below the curve.
- **Seat marker:** vertical dashed `#6b7280` (dash 4/3) line at position 7 from y=60 to y=230; the three curve readings at position 7 marked with 5px dots in their curve colors (blue 0.66, orange 0.72, green 0.70).
- **Annotation (bold 12px violet `#4a3aa7`, near x-position 9.5, y=95, two lines):** "seat 7's fingerprint:" / "(0.66, 0.72, 0.70)".
- **Caption (12px `#444`, bottom right):** "simplified — real models use many more wave speeds".

## Why Not Just Number Them 1, 2, 3?

**Tags:** `common mistake` (red), `bounded stamps` (orange)

- **The obvious idea** — stamp seat 1 with 1, seat 2 with 2, and so on up to seat 500 with 500
- **The blow-up** — word numbers sit near 0.5; by seat 20 a raw counter stamp of 20 drowns the word 40 to 1
- **Shrink it?** — divide by length and the same seat gets different stamps in a 10-word vs 100-word text
- **The wave answer** — sine stamps never leave [-1, +1], so the word's own numbers survive the addition
- **The mistake** — assuming the model reads left to right like a person; without stamps it has no order at all

*Example (italic):* At seat 20 the raw counter stamp is 20 while the word's numbers are about 0.5 — after addition the model mostly sees the seat, barely the word.

**Common mistake:** Thinking the model reads in order the way people do. Attention has no order of its own — with the stamps missing or broken, "Sam thanked Alex" and "Alex thanked Sam" are literally the same input.

### Visualization (canvas `c4`, 720×300)

Line chart comparing the naive counter stamp (1, 2, 3, ...) against the sine stamp over seats 0–20, with a shaded band showing where word numbers live — the counter escapes the band immediately, the wave never does.

- **Title (bold 15px, `#1a5276`, top center):** "Counter Stamps Explode — Wave Stamps Stay in Bounds".
- **Axes:** origin x=60, baseline y=245, positions 0–20 across plot width 600 (30px per position); vertical scale 7.9px per unit with value 0 at y=229 (range -2 to about +22); light `#e5e9ef` gridlines at 0, 5, 10, 15, 20 with 12px `#444` labels at x=45; x tick labels "0", "5", "10", "15", "20" (12px `#444`) below the baseline, x-axis title 12px `#444` "seat position" centered at y=280.
- **Word band:** horizontal band from value -1 to +1 (y=221 to y=237) filled `rgba(42,120,214,0.12)`; 11px `#2a78d6` label inside its right end: "where word numbers live (±1)".
- **Counter stamp:** red `#e74c3c` 2.5px straight line through values `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]`; bold 12px red label "counter stamp 1, 2, 3, ..." along the line near position 13.
- **Wave stamp sin(pos):** green `#008300` 2.5px line hugging the band, values `[0, 0.84, 0.91, 0.14, -0.76, -0.96, -0.28, 0.66, 0.99, 0.41, -0.54, -1.00, -0.54, 0.42, 0.99, 0.65, -0.29, -0.96, -0.75, 0.15, 0.91]`; 12px green label "wave stamp sin(pos)" just above the band near position 4.
- **Marker:** 6px red dot on the counter line at position 20 (value 20) with a short dashed `#6b7280` drop line to the band.
- **Annotation (bold 12px red `#e74c3c`, near x-position 12, y=75, two lines):** "seat 20: stamp = 20, word ≈ 0.5" / "the word is drowned 40 to 1".
- **Caption (12px `#444`, bottom right):** "illustrative — word magnitude taken as 0.5".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded arrays above (no randomness); c3/c4 wave values are sin(pos), sin(pos/3), sin(pos/9) rounded to 2 decimals; the worked-example vectors in c2 match the text's arithmetic exactly ([0.5, 0.2] + stamps = [1.34, 0.74] and [0.64, -0.79]).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
