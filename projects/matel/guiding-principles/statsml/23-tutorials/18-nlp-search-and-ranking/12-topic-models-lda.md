# Topic Models (LDA)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Topic Models (LDA)

**Subtitle:** LDA reads a pile of unlabeled documents and hands back the themes running through them — each theme a weighted word list, each document a mixture of themes

## Three Conversations Hiding in 200 Reviews

**Tags:** `core idea` (blue), `unsupervised` (green), `word lists` (orange)

- **The pile** — a café owner has 200 written customer reviews and no time to read them all
- **The hunch** — the reviews seem to circle a few subjects: the drinks, the wifi and seating, the service
- **What LDA does** — fed the raw text and "find 3 themes", it groups words that keep appearing together
- **A topic** — each theme comes back as a weighted word list: latte, bitter, espresso lead topic 1
- **No teacher** — nobody tagged a single review; words traveling together did the sorting on their own
- **The name** — the trick is Latent Dirichlet Allocation: "latent" because the themes were hidden

*Example (italic):* Ask LDA for 3 themes in the 200 reviews and it returns three word lists — the owner instantly recognizes them as drinks, wifi & seating, and service.

**Key point:** A topic model reads unlabeled text and returns themes as weighted word lists — words that co-occur across documents end up in the same topic.

### Visualization (canvas `c1`, 720×300)

Three side-by-side panels of horizontal word bars, one panel per discovered topic, each showing its top-5 words with their weights — the raw output LDA hands back before any human names anything.

- **Title (bold 15px, `#1a5276`, top center):** "What LDA Hands Back: Three Word Lists It Found on Its Own".
- **Panels:** three equal panels with left edges at x=40, x=275, x=510, each 200px wide; panel headers bold 13px at y=55 — "topic 1" in blue `#2a78d6`, "topic 2" in orange `#d95926`, "topic 3" in green `#008300` (no theme names anywhere on this chart).
- **Rows:** five bars per panel at y = 80, 112, 144, 176, 208; each row: 12px `#444` word label left-aligned at the panel edge, then a bar starting 62px in, height 16, length = weight × 1300px (so 0.10 → 130px); 11px `#6b7280` weight label just right of each bar.
- **Topic 1 (blue `#2a78d6` bars, fill `rgba(42,120,214,0.35)`, 1px blue stroke):** words `["latte", "bitter", "espresso", "roast", "milk"]`, weights `[0.09, 0.07, 0.06, 0.05, 0.04]`.
- **Topic 2 (orange `#d95926` bars, fill `rgba(217,89,38,0.30)`, 1px orange stroke):** words `["wifi", "outlet", "laptop", "table", "noisy"]`, weights `[0.10, 0.06, 0.05, 0.05, 0.04]`.
- **Topic 3 (green `#008300` bars, fill `rgba(0,131,0,0.25)`, 1px green stroke):** words `["staff", "friendly", "order", "wait", "smile"]`, weights `[0.08, 0.07, 0.06, 0.05, 0.04]`.
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=250):** "no labels were given — the words grouped themselves".
- **Caption (11px `#444`, bottom right):** "illustrative — word weights from 200 café reviews".

## Scoring One Review by Hand

**Tags:** `worked example` (blue), `topic mixture` (green)

- **One review** — "the latte was bitter and the espresso weak; the wifi dropped; staff were friendly, order came fast"
- **Ten words that matter** — latte, bitter, espresso, weak, wifi, dropped, staff, friendly, order, fast
- **Color each word** — 4 sit on the drinks list, 2 on wifi & seating, 4 on the service list
- **The mixture** — 4/10, 2/10, 4/10: this review is 40% drinks, 20% wifi, 40% service
- **Both directions** — LDA learns the word lists and the document mixtures together, each pass refining the other

*Example (italic):* A review is not filed under one theme — this one is 40% a drinks complaint and 40% a service compliment at the same time.

**Key point:** A document's topic mixture is roughly the share of its words each topic claims — 4 + 2 + 4 words here gives 40% / 20% / 40%.

### Visualization (canvas `c2`, 720×300)

Two-part panel: the review's ten content words drawn as colored chips (color = claiming topic), and below them one stacked bar showing the resulting 40/20/40 mixture — the reader can recount the chips and rebuild the bar.

- **Title (bold 15px, `#1a5276`, top center):** "One Review, Ten Words: Count the Colors, Get the Mixture".
- **Word chips:** ten rounded rects (radius 6, 108×30) in two rows of five, row 1 top y=60, row 2 top y=105, x starting at 70 with 128px spacing; chip words in order `["latte", "bitter", "espresso", "weak", "wifi", "dropped", "staff", "friendly", "order", "fast"]`; chip topics `[1, 1, 1, 1, 2, 2, 3, 3, 3, 3]`; topic 1 chips fill `rgba(42,120,214,0.15)` with bold 12px `#2a78d6` text, topic 2 fill `rgba(217,89,38,0.15)` with bold 12px `#d95926` text, topic 3 fill `rgba(0,131,0,0.12)` with bold 12px `#008300` text; 1px stroke in each topic's color.
- **Stacked bar:** at x=60, y=185, total width 600, height 34; segments left to right — blue `#2a78d6` width 240 (40%), orange `#d95926` width 120 (20%), green `#008300` width 240 (40%); bold 13px white percent labels centered in each segment: "40%", "20%", "40%".
- **Segment captions (12px `#444`, below the bar at y=238):** "drinks" under the blue segment, "wifi & seating" under the orange, "service" under the green.
- **Annotation (bold 12px ink `#1a5276`, centered near y=270):** "4 + 2 + 4 words → 40% / 20% / 40% — the mixture is just counting".
- **Caption (11px `#444`, bottom right):** "illustrative — simplified one-pass assignment".

## Why an Unsupervised Sort of Text Pays Off

**Tags:** `where it's used` (blue), `search & ranking` (green), `rule of thumb` (orange)

- **Scale** — the trick that sorted 200 reviews sorts 2 million support tickets the same way, unread
- **Search** — a query about "slow internet" can match reviews saying "wifi kept dropping": same topic, zero shared words
- **The tally** — score every review's dominant theme: 88 lean drinks, 64 wifi & seating, 48 service
- **The surprise** — wifi & seating is the second-biggest conversation, and no star rating ever said so
- **Choosing K** — you pick the number of themes; 3 may blur subjects, 30 may split hairs — try a few

*Example (italic):* The owner fixed the router before retraining the baristas — the topic tally, not the star average, set the priority.

**Key point:** Topic models turn an unreadable pile of text into a handful of themes you can count, track, and rank — with no labeling budget at all.

### Visualization (canvas `c3`, 720×300)

Single-panel vertical bar chart: the 200 reviews tallied by dominant theme, exposing the wifi conversation the star ratings never surfaced.

- **Title (bold 15px, `#1a5276`, top center):** "200 Reviews Tallied by Dominant Theme".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = review count 0 to 100 with light `#e5e9ef` gridlines at 25, 50, 75, 100 and 12px `#444` tick labels; x axis unticked.
- **Bars:** three bars 110px wide centered at x = 190, 390, 590; heights from counts `[88, 64, 48]` (scale: count × 1.9px); fills — drinks blue `rgba(42,120,214,0.35)` with 2px `#2a78d6` stroke, wifi & seating orange `rgba(217,89,38,0.30)` with 2px `#d95926` stroke, service green `rgba(0,131,0,0.25)` with 2px `#008300` stroke.
- **Value labels:** bold 13px in each bar's stroke color, centered above each bar top: "88", "64", "48".
- **Category labels (12px `#444`, below baseline at y=265):** "drinks", "wifi & seating", "service".
- **Annotation (bold 12px orange `#d95926`, two lines, near x=430, y=85):** "64 of 200 reviews talk wifi —" / "the star rating never said so".
- **Caption (11px `#444`, bottom right):** "illustrative — each review filed under its largest topic share".

## The Topics Come Back Nameless

**Tags:** `common mistake` (red), `mixtures not buckets` (orange)

- **No names** — LDA returns "topic 1: latte, bitter, espresso…"; the label "drinks" was the owner's word
- **Not buckets** — reviews are blends; forcing each into its top theme throws away the rest of the mixture
- **Close calls** — a 34/33/33 review has no honest single home; a hard label there flips on a coin
- **Junk topics** — some returned themes are grab-bags of filler words; you owe them no flattering name
- **Read the words** — always inspect a topic's top words before trusting it; a name can hide a mess

*Example (italic):* One analyst shipped "topic 2 = wifi"; another read its word list — wifi, outlet, laptop, table — and saw it was really a laptop-worker theme.

**Common mistake:** Treating a document's top topic as its category. LDA gives every document a blend — report the mixture, or at least check how close the runner-up is.

### Visualization (canvas `c4`, 720×300)

Five horizontal stacked mixture bars on a shared 0–100% axis, one per review, showing that every review is a blend — including one near-even three-way split that a hard label would file arbitrarily.

- **Title (bold 15px, `#1a5276`, top center):** "Five Reviews, Five Blends — None Lives in One Bucket".
- **Axis:** horizontal 2px `#999` line at y=252 from x=180 to x=660 (width 480), share 0 to 100%; 12px `#444` tick labels "0%", "25%", "50%", "75%", "100%" below.
- **Rows (bar tops at y = 68, 104, 140, 176, 212), each with a 12px `#444` left-aligned label at x=20:** "review A", "review B", "review C (worked above)", "review D", "review E".
- **Bars:** 20px tall, x=180, full width 480 = 100%; three segments per row in order drinks / wifi & seating / service, fills blue `#2a78d6`, orange `#d95926`, green `#008300`.
- **Mixtures (percent triples, hardcoded):** A `[72, 18, 10]`, B `[15, 70, 15]`, C `[40, 20, 40]`, D `[34, 33, 33]`, E `[10, 25, 65]`.
- **Segment labels:** bold 11px white percent centered in every segment 15% or wider (e.g. "72", "70", "40", "34", "65"); narrower segments unlabeled.
- **Highlight:** review D's bar gets a 2px dashed `#e74c3c` outline (dash 4/3) with a bold 12px red `#e74c3c` label to its right: "34/33/33 — no honest single home".
- **Annotation (bold 13px magenta `#d55181`, centered near y=284):** "every review is a blend — one hard label throws half of it away".
- **Caption (11px `#444`, bottom right):** "illustrative mixtures".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all word lists, weights, counts, and mixture triples are the hardcoded arrays above (no randomness); topic colors are consistent across all four charts (drinks blue, wifi & seating orange, service green); c2's 40/20/40 bar, c3's 88/64/48 tally, and c4's row C must match the text's numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
