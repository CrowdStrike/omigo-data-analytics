# Conditional Random Fields

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Conditional Random Fields

**Subtitle:** A CRF tags every word of a sentence in one shot — it scores whole label sequences, so a word's tag can lean on the tags of its neighbors

## The Word That Only Makes Sense Next to Its Neighbor

**Tags:** `core idea` (blue), `sequence labeling` (green), `search queries` (orange)

- **The store** — an online shoe shop reads the query "new balance running shoes" and must decide what to show
- **The slots** — each word gets a tag: BRAND, CATEGORY, or OTHER, so search can filter the catalog by slot
- **Word by word** — a tagger that sees one word alone calls "new" OTHER; by itself it is just an adjective
- **The neighbor** — but "balance" right after flips the story: "new balance" together is one brand name
- **The CRF move** — score entire tag sequences instead of single words, so neighboring tags can agree

*Example (italic):* A shopper typing "new balance running shoes" wants one brand's shoes — not shoes that happen to be new.

**Key point:** A CRF labels the whole sequence at once: each word's tag is chosen together with its neighbors' tags, not in isolation.

### Visualization (canvas `c1`, 720×300)

Two-row token strip: the same four-word query tagged by a word-by-word tagger (top row, "new" wrong) and by a CRF (bottom row, all correct), each word a box with a colored tag pill beneath.

- **Title (bold 15px, `#1a5276`, top center):** "Same Query, Two Taggers: 'new balance running shoes'".
- **Row labels (12px `#444`, left-aligned at x=20):** "one word at a time" at y=100; "whole sequence (CRF)" at y=210.
- **Word boxes (both rows):** rounded rects 120×38, 1px `#e5e9ef` border, fill `#f8f9fa`, at x = 165, 300, 435, 570; row 1 top y=72, row 2 top y=182; word centered inside, bold 13px `#2c3e50`.
- **Tag pills (row 1, centered under each box at y=126):** "OTHER" mute `#6b7280` on `rgba(107,114,128,0.15)` under "new" with a 2px red `#e74c3c` ring and bold 12px red "wrong" at its right; "BRAND" blue `#2a78d6` on `rgba(42,120,214,0.15)` under "balance"; "CATEGORY" green `#008300` on `rgba(0,131,0,0.12)` under "running" and "shoes"; all pill text bold 12px, 3px 10px padding, 9px radius.
- **Tag pills (row 2, y=236):** "BRAND" blue under both "new" and "balance", "CATEGORY" green under "running" and "shoes"; a bold 12px green check-style label "fixed" to the right of the "new" pill.
- **Annotation (bold 12px orange `#d95926`, centered near y=165):** "'balance' next door rescues 'new'".
- **Caption (12px `#444`, bottom right):** "illustrative query".

## Scoring All Four Labelings of "New Balance"

**Tags:** `worked example` (blue), `path scores` (green)

- **The setup** — shrink to two words, "new balance", and two tags, BRAND and OTHER; scores are plain points
- **Word scores** — "new": OTHER 3, BRAND 1; "balance": BRAND 4, OTHER 2 (how brand-like each word looks alone)
- **Pair bonus** — BRAND then BRAND earns +3 because brands run in streaks; OTHER then OTHER +1; mixed pairs +0
- **All four paths** — OTHER-OTHER 3+2+1=6, OTHER-BRAND 3+4+0=7, BRAND-OTHER 1+2+0=3, BRAND-BRAND 1+4+3=8
- **The winner** — BRAND-BRAND totals 8, the highest, so the CRF tags both words BRAND in one decision
- **The trap avoided** — word-by-word picks OTHER for "new" (3 beats 1) and gets stuck on the 7-point path

*Example (italic):* "New" alone loses BRAND 1-to-3, yet the whole-sequence total 1+4+3=8 beats 3+4+0=7 — the pair bonus flips the call.

**Key point:** The CRF adds word scores AND label-pair bonuses across the whole sequence: BRAND-BRAND wins 8 to 7 even though "new" alone prefers OTHER.

### Visualization (canvas `c2`, 720×300)

Two-column trellis: one column per word, one node per tag, node scores inside, transition bonuses on the four connecting edges, the winning BRAND-BRAND path highlighted, and all four path totals listed at the right.

- **Title (bold 15px, `#1a5276`, top center):** "Four Possible Labelings, One Winner".
- **Column headers (bold 13px `#2c3e50`):** "new" centered above x=250 at y=60; "balance" above x=460 at y=60.
- **Tag row labels (bold 12px `#444`, right-aligned at x=150):** "BRAND" at y=115, "OTHER" at y=205.
- **Nodes:** circles radius 26 at (250,115), (250,205), (460,115), (460,205); word score centered inside, bold 13px: "+1" (new/BRAND), "+3" (new/OTHER), "+4" (balance/BRAND), "+2" (balance/OTHER); winning-path nodes (250,115) and (460,115) fill `rgba(0,131,0,0.12)` with 3px `#008300` stroke; the other two fill `#f8f9fa` with 2px `#e5e9ef` stroke.
- **Edges (four lines between column nodes) with bonus labels (11px `#6b7280` at midpoints, offset to avoid crossing):** BRAND-BRAND "+3" drawn 4px `#008300`; OTHER-OTHER "+1", BRAND-OTHER "+0", OTHER-BRAND "+0" drawn 2px `#c9d2dc`.
- **Path totals (right side, x=575, 12px, lines at y=100/130/160/190):** "B-B = 8  best" bold `#008300`; "O-B = 7" `#2c3e50`; "O-O = 6" `#6b7280`; "B-O = 3" `#6b7280`.
- **Annotation (bold 12px green `#008300`, centered near y=265):** "whole-sequence total: 1 + 4 + 3 = 8".
- **Caption (12px `#444`, bottom right):** "illustrative scores".

## One Wrong Word Ruins the Whole Parse

**Tags:** `where it's used` (blue), `search & ranking` (green), `structured output` (orange)

- **Search boxes** — the tagged query drives filters and ranking; tag "new" as OTHER and brand results vanish
- **All or nothing** — a parse is useful only if every word is right; one slip sends the shopper elsewhere
- **The gap** — on a query set, word-by-word gets 71% of whole queries fully right; the CRF gets 89%
- **Beyond queries** — the same trick parses addresses into street/city/zip and resumes into name/skill/date
- **Classic pairing** — CRFs long powered named-entity tagging, and the idea lives on inside neural taggers

*Example (italic):* A store that mis-tags one word in "new balance running shoes" ranks generic new arrivals above the brand the shopper asked for.

**Key point:** Whenever the answer is a sequence of tags that must make sense together, scoring the whole sequence beats scoring words one at a time.

### Visualization (canvas `c3`, 720×300)

Simple three-bar chart: share of whole queries with every word tagged correctly, for a word-by-word tagger, the same tagger with patch rules, and a CRF.

- **Title (bold 15px, `#1a5276`, top center):** "Whole Queries Parsed Fully Right (illustrative)".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 175; y axis 0 to 100% with 12px `#444` tick labels "0%", "25%", "50%", "75%", "100%" and light `#e5e9ef` gridlines at each.
- **Bars (width 110, centered at x=190, 380, 570), values `[71, 76, 89]`:** "word-by-word" fill `rgba(107,114,128,0.35)` with 2px `#6b7280` border; "+ patch rules" fill `rgba(217,89,38,0.25)` with 2px `#d95926` border; "CRF" fill `rgba(0,131,0,0.20)` with 3px `#008300` border.
- **Value labels (bold 13px, above each bar, matching border colors):** "71%", "76%", "89%".
- **X labels (12px `#444`, below baseline, centered under bars):** "word-by-word", "+ patch rules", "CRF".
- **Annotation (bold 12px green `#008300`, near x=480, y=95):** "one wrong word = one lost shopper".
- **Caption (12px `#444`, bottom right):** "illustrative — invented benchmark".

## You Can't Patch It Word by Word

**Tags:** `common mistake` (red), `patch rules` (orange)

- **The temptation** — after the "new balance" bug, add a rule: if "new" comes before "balance", tag BRAND
- **The next bug** — "banana republic dress" arrives and "banana" gets tagged OTHER; no rule covered it
- **Rules don't travel** — each patch fixes exactly one word pair; the query stream invents new pairs daily
- **One bonus, all brands** — the CRF's single BRAND-then-BRAND bonus helps every multi-word brand at once
- **Learned, not written** — the bonus is fit from tagged examples, so nobody maintains an if-then list

*Example (italic):* The team patched "new balance" on Monday and lost "banana republic" on Tuesday — the CRF's pair bonus covers both without a rule.

**Common mistake:** Fixing a word-by-word tagger with hand-written pair rules. Each rule covers one phrase; the CRF learns one label-pair bonus that generalizes to phrases nobody listed.

### Visualization (canvas `c4`, 720×300)

Two-row token strip on a new query, "banana republic dress": the patched word-by-word tagger still tags "banana" wrong (top), while the CRF's learned pair bonus tags it right (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "A Query No Patch Rule Covered: 'banana republic dress'".
- **Row labels (12px `#444`, left-aligned at x=20):** "patched tagger" at y=100; "CRF" at y=210.
- **Word boxes (both rows):** rounded rects 140×38, 1px `#e5e9ef` border, fill `#f8f9fa`, at x = 200, 375, 550; row 1 top y=72, row 2 top y=182; word centered inside, bold 13px `#2c3e50`.
- **Tag pills (row 1, centered under boxes at y=126):** "OTHER" mute `#6b7280` on `rgba(107,114,128,0.15)` under "banana" with a 2px red `#e74c3c` ring and bold 12px red "wrong" at its right; "BRAND" blue `#2a78d6` on `rgba(42,120,214,0.15)` under "republic"; "CATEGORY" green `#008300` on `rgba(0,131,0,0.12)` under "dress"; pill text bold 12px, 3px 10px padding, 9px radius.
- **Tag pills (row 2, y=236):** "BRAND" blue under "banana" and "republic", "CATEGORY" green under "dress".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=165):** "the BRAND-then-BRAND bonus travels to brands no rule ever named".
- **Caption (12px `#444`, bottom right):** "illustrative query".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all scores, bar values, and token layouts are the hardcoded literals above (no randomness); the trellis scores in `c2` must equal the numbers in the worked-example bullets (word scores 1/3/4/2, bonuses +3/+1/+0, path totals 8/7/6/3), and the `c3` bars must equal the 71%/89% quoted in the text; invented numbers keep their "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
