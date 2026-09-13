# TF-IDF

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** TF-IDF

**Subtitle:** A word matters when it is frequent on this page yet rare everywhere else — TF-IDF turns that one sentence into a number you can rank by

## The Word That Gives a Recipe Away

**Tags:** `core idea` (blue), `running example` (green), `rare words` (orange)

- **The shelf** — a cook keeps 100 recipe cards and wants to find the seafood paella card fast
- **Useless words** — "the" is on all 100 cards and "cup" on 90, so spotting them narrows nothing
- **The giveaway** — "saffron" sits on only 5 of the 100 cards, so seeing it almost names the dish
- **Two forces** — a word matters when it is frequent on this card yet rare across the whole shelf
- **The name** — TF-IDF multiplies term frequency (how often here) by inverse document frequency (how rare everywhere else)

*Example (italic):* A friend says "the recipe with saffron" and the cook narrows 100 cards down to 5; "the recipe with cups" would still leave 90.

**Key point:** TF-IDF gives a word a high score only when it is frequent in this document and rare in the rest of the collection.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: for five words, how many of the 100 recipe cards contain the word at least once — everyday words fill the shelf, "saffron" barely appears.

- **Title (bold 15px, `#1a5276`, top center):** "One Shelf, 100 Recipe Cards: Which Cards Contain Each Word?".
- **Axis:** horizontal 2px `#999` line at y=260 from x=140 to x=660 (width 520), scale 0 to 100 cards; 12px `#444` tick labels "0", "25", "50", "75", "100" below; light `#e5e9ef` vertical gridlines at 25, 50, 75.
- **Rows (bar centers at y = 70, 110, 150, 190, 230), 13px `#444` word labels right-aligned at x=130:** "the", "cup", "add", "rice", "saffron"; bars 22px tall, values `[100, 90, 85, 40, 5]` (cards containing the word).
- **Bar style:** first four bars fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; "saffron" bar solid orange `#d95926`; bold 12px value label at each bar's right end ("100", "90", "85", "40", "5"), ink `#1a5276` except orange for saffron.
- **Annotation (bold 13px orange `#d95926`, near x=330, y=222):** "on only 5 of 100 cards — seeing 'saffron' almost names the dish".
- **Caption (12px `#444`, bottom right):** "illustrative — a made-up shelf of 100 recipe cards".

## Scoring Three Words on the Paella Card

**Tags:** `worked example` (blue), `hand computation` (green)

- **The card** — the seafood paella card has 200 words; "cup" appears 8 times, "saffron" 6, "rice" 5
- **Term frequency** — divide by the 200 words: TF(cup) = 0.040, TF(saffron) = 0.030, TF(rice) = 0.025
- **Document frequency** — across the shelf, "cup" is in 90 cards, "rice" in 40, "saffron" in only 5
- **IDF** — log10(100 / cards with the word): cup 0.046, rice 0.40, saffron 1.30 — rarity is the multiplier
- **The scores** — TF × IDF: cup 0.0018, rice 0.010, saffron 0.039 — saffron wins by about 21×

*Example (italic):* "cup" appears on the card more often than "saffron" (8 vs 6 times), yet scores 21× lower because 90 of the 100 cards also say "cup".

**Key point:** TF-IDF(saffron) = (6/200) × log10(100/5) = 0.030 × 1.30 = 0.039 — two divisions and a multiply, redoable on paper.

### Visualization (canvas `c2`, 720×300)

Three mini bar panels side by side — TF, IDF, and their product — each with the same three words, so the reader watches the multiplication flip the winner from "cup" to "saffron".

- **Title (bold 15px, `#1a5276`, top center):** "Three Words on the Paella Card: TF × IDF = Score".
- **Panels:** three plots on a shared baseline y=245, each 170px wide with plot top y=85; panel origins x=60 (TF), x=290 (IDF), x=520 (TF-IDF); bold 12px `#1a5276` panel subtitles centered above each: "TF = count / 200", "IDF = log10(100 / cards)", "TF-IDF = TF × IDF".
- **Bars per panel (left to right: cup, rice, saffron), 34px wide, 14px gaps; 11px `#444` word labels below each bar:** cup blue `#2a78d6`, rice aqua `#199e70`, saffron orange `#d95926` — same color per word in all three panels.
- **Values (bold 12px labels above each bar, matching the bar color):** TF panel `[0.040, 0.025, 0.030]` scaled to y-max 0.05; IDF panel `[0.046, 0.40, 1.30]` scaled to y-max 1.4; TF-IDF panel `[0.0018, 0.010, 0.039]` scaled to y-max 0.045.
- **Panel separators:** 1px `#e5e9ef` vertical lines at x=270 and x=500 from y=60 to y=260.
- **Annotation (bold 12px orange `#d95926`, over the TF-IDF panel near x=530, y=100):** two lines: "saffron beats cup" / "by ≈21× on score".
- **Caption (12px `#444`, bottom right):** "illustrative — hardcoded from the worked example above".

## Where a Data Scientist Meets It

**Tags:** `where it's used` (blue), `search ranking` (green)

- **Search** — type "saffron" and rank every card by its TF-IDF for that word; only 5 score above zero
- **Ranking** — paella (0.039) beats risotto (0.029) and buns (0.026) because it uses saffron most heavily
- **Free stopword removal** — "the" is on all 100 cards, so IDF = log10(100/100) = 0 erases it automatically
- **Keywords** — a document's top TF-IDF words are a ready-made summary of what makes it special
- **Baseline** — search engines and text classifiers still use TF-IDF vectors as the first honest benchmark

*Example (italic):* For the query "saffron", the five saffron cards score 0.039, 0.029, 0.026, 0.012, 0.007 and the other 95 cards score exactly 0.

**Key point:** TF-IDF turns raw text into a ranking with no labels and no training — it is usually the first scorer to try, and the one to beat.

### Visualization (canvas `c3`, 720×300)

Ranked horizontal bar chart: the five recipes containing "saffron", ordered by TF-IDF score for the query, with the 95 zero-score recipes noted below the axis.

- **Title (bold 15px, `#1a5276`, top center):** "Query 'saffron': Only 5 of the 100 Cards Score Above Zero".
- **Axis:** horizontal 2px `#999` line at y=248 from x=200 to x=660 (width 460), score 0 to 0.045; 12px `#444` tick labels "0", "0.01", "0.02", "0.03", "0.04"; light `#e5e9ef` vertical gridlines at each tick.
- **Rows (bar centers at y = 75, 110, 145, 180, 215), 12px `#444` recipe labels right-aligned at x=190:** "Seafood Paella", "Risotto Milanese", "Saffron Buns", "Bouillabaisse", "Persian Rice"; bars 20px tall, values `[0.039, 0.029, 0.026, 0.012, 0.007]`.
- **Bar style:** top bar (Seafood Paella) solid green `#008300`; the other four fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; bold 12px value labels at bar ends ("0.039" in green, rest in `#1a5276`).
- **Annotation (bold 12px green `#008300`, right-aligned at x=690, y=44):** "6 mentions in 200 words — the heaviest saffron user ranks first".
- **Note (12px `#6b7280`, below the axis near x=200, y=275):** "the other 95 recipes contain no 'saffron' and score exactly 0".
- **Caption (12px `#444`, bottom right):** "illustrative scores from the worked example".

## Counting Is Not Weighing

**Tags:** `common mistake` (red), `raw counts` (orange)

- **The trap** — sorting the card's words by raw count puts "the" (14 times) and "cup" (8) at the top
- **The fix** — TF-IDF reorders them: saffron 0.039, rice 0.010, add 0.0025, cup 0.0018, the 0.000
- **Zero, exactly** — a word on every card gets IDF = log10(100/100) = 0, so "the" scores nothing at all
- **Documents, not occurrences** — IDF counts how many cards contain the word, not its total mentions
- **Long-card bias** — raw counts reward long cards; dividing by the card's 200 words removes that edge

*Example (italic):* "the" appears 14 times on the paella card — more than any other word — and still earns a TF-IDF score of exactly 0.000.

**Common mistake:** Reading "most frequent" as "most important". A word's count only matters relative to how rare that word is across the rest of the collection.

### Visualization (canvas `c4`, 720×300)

Two side-by-side ranked bar panels over the same five words from the paella card: left sorted by raw count, right sorted by TF-IDF — the orders nearly reverse.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Card, Two Rankings: Raw Count vs TF-IDF".
- **Left panel ("sorted by raw count", bold 12px `#1a5276` subtitle centered near x=200, y=62):** horizontal bars, rows at y = 95, 125, 155, 185, 215; 12px `#444` word labels right-aligned at x=120; words top-to-bottom "the", "cup", "add", "saffron", "rice" with counts `[14, 8, 7, 6, 5]`; bars from x=130, scale 0–15 over 200px width; fill `rgba(107,114,128,0.35)` with 2px `#6b7280` border, except "saffron" solid orange `#d95926`; bold 12px count labels at bar ends.
- **Right panel ("sorted by TF-IDF", bold 12px `#1a5276` subtitle centered near x=550, y=62):** same row geometry with labels right-aligned at x=470 and bars from x=480, scale 0–0.045 over 180px width; words top-to-bottom "saffron", "rice", "add", "cup", "the" with scores `[0.039, 0.010, 0.0025, 0.0018, 0.000]`; "saffron" solid orange `#d95926`, others fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; "the" gets a zero-width bar with just its bold label "0.000"; bold 12px score labels at bar ends.
- **Panel separator:** 1px `#e5e9ef` vertical line at x=395 from y=55 to y=240.
- **Annotation (bold 12px magenta `#d55181`, centered near x=360, y=278):** "'the' leads on raw count (14) yet scores exactly 0.000".
- **Caption (12px `#444`, bottom right, above the annotation line at y=255):** "illustrative — same 200-word card, same shelf of 100".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" for sections 1–3, "Common mistake:" for section 4).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values are the hardcoded arrays above (no randomness); IDF values are log10(100/df) rounded to 2–3 significant digits (log10(100/90)=0.046, log10(100/40)=0.40, log10(100/85)=0.071, log10(20)=1.30) and every score is the literal TF × IDF product quoted in the text — chart numbers must match the text exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
