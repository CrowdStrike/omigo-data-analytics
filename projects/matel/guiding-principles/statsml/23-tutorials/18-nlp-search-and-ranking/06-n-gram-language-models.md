# N-gram Language Models

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** N-gram Language Models

**Subtitle:** Your phone predicts the next word by counting what followed the same words in your old messages — and that counting trick works beautifully until the phrases get long enough that the counts run out

## The Phone That Finishes Your Sentence

**Tags:** `core idea` (blue), `predict by counting` (green), `next word` (orange)

- **The keyboard** — you type "see you" and the phone offers "soon", "later", "tomorrow" — how does it know?
- **No grammar** — the phone learned no rules; it kept your old messages and simply counted what came next
- **The counts** — "see you" appeared 40 times: "soon" followed 18 times, "later" 12, "tomorrow" 6, "there" 4
- **The shares** — divide by 40: soon 18/40 = 45%, later 30%, tomorrow 15%, there 10%
- **The name** — a model that predicts a word from the previous n−1 words is an n-gram language model

*Example (italic):* Every time you type "see you", the phone quietly asks "what came next the last 40 times?" and offers the biggest counts first.

**Key point:** An n-gram model predicts the next word by counting what followed the same few words before — no rules, no meaning, just counts turned into shares.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: the four words that followed "see you" in the phone's message history, counts out of 40, longest bar on top.

- **Title (bold 15px, `#1a5276`, top center):** "After 'see you' (40 times in old messages): what came next?".
- **Layout:** word labels ("soon", "later", "tomorrow", "there") right-aligned 13px `#2c3e50` ending at x=150; bars start at x=160, 26px tall, scaled 460px max width for count 18; rows at y = 90, 135, 180, 225.
- **Bars:** "soon" count 18 in solid blue `#2a78d6`; "later" 12, "tomorrow" 6, "there" 4 in `rgba(42,120,214,0.45)` with 1px blue border.
- **Value labels (bold 12px `#1a5276`, 8px right of each bar end):** "18 → 45%", "12 → 30%", "6 → 15%", "4 → 10%".
- **Annotation (bold 13px orange `#d95926`, near x=420, y=62):** "prediction = the biggest count".
- **Caption (12px `#444`, bottom right):** "illustrative — counts from one user's message history".

## Counting Triples in Four Text Messages

**Tags:** `worked example` (blue), `trigram tally` (green)

- **Tiny corpus** — four saved messages are the whole training set; everything here is checkable by hand
- **The walk** — scan the messages; each time "see you" appears, write down the very next word
- **Four hits** — "see you" occurs 4 times, followed by: soon, later, soon, there
- **The table** — soon 2, later 1, there 1 → P(soon | see you) = 2/4 = 50%, later 25%, there 25%
- **Prediction** — next time you type "see you", the model just reads that little table aloud

*Example (italic):* From only four texts the model already bets 50% on "soon" — tallying triples is the entire training step.

**Key point:** Training a trigram model is literally tallying: count each (two words, next word) triple, then divide by how often the two-word phrase appeared.

### Visualization (canvas `c2`, 720×300)

Two-panel walkthrough: the four training messages on the left with each word after "see you" highlighted, an arrow, then the tally as small bars on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Counting by hand: every 'see you ___' in four messages".
- **Left panel (x=30 to x=360), message rows at y = 95, 135, 175, 215, 13px `#2c3e50`:** "running late see you **soon**", "see you **later** tonight", "ok see you **soon**", "see you **there** at 8" — "see you" drawn bold in ink `#1a5276`; the next word sits on a rounded highlight: "soon" `rgba(42,120,214,0.20)`, "later" `rgba(213,81,129,0.20)`, "there" `rgba(25,158,112,0.20)`.
- **Arrow:** 2px `#6b7280` horizontal arrow from x=370 to x=420 at y=155, 11px `#6b7280` label "tally" above it.
- **Right panel tally bars:** left edge x=460, 80px of width per count, 24px tall, rows at y = 110, 155, 200 — "soon" 2 in blue `#2a78d6`, "later" 1 in magenta `#d55181`, "there" 1 in aqua `#199e70`; word labels 12px `#444` left of the bars, bold 12px labels at bar ends: "2/4 = 50%", "1/4 = 25%", "1/4 = 25%".
- **Annotation (bold 12px violet `#4a3aa7`, near x=460, y=255):** "training = this tally, nothing more".
- **Caption (12px `#444`, bottom right):** "illustrative — a four-message training set".

## Longer Memory, Emptier Counts

**Tags:** `where it's used` (blue), `sparsity` (red), `trade-off` (orange)

- **More context helps** — "see you next ___" is a sharper clue than "next ___" alone, so longer n-grams predict better
- **But counts vanish** — the longer the phrase, the fewer times you have ever typed exactly that phrase
- **The cliff** — share of tomorrow's phrases already seen in old messages: 99% (1 word), 87%, 54%, 21%, then 6% at 5 words
- **Zero means zero** — a phrase counted 0 times gets probability 0, so the model calls it impossible
- **The tug-of-war** — picking n trades longer memory against counting tables that are mostly empty

*Example (italic):* "see you next Friday" is a perfectly normal sentence, but if you never typed it, a 4-gram counter scores it exactly 0.

**Key point:** Counting fails by starvation, not by logic: most long phrases have never occurred even once, so their counts — and predictions — are zero. This is sparsity.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: how much of tomorrow's typing was already seen in the message history, by phrase length 1 to 5 words — a steep collapse.

- **Title (bold 15px, `#1a5276`, top center):** "Share of tomorrow's phrases already seen in your old messages".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y axis 0–100% with light `#e5e9ef` gridlines at 25, 50, 75 and 12px `#444` labels "0%", "25%", "50%", "75%", "100%"; x categories "1 word" … "5 words" in 12px `#444` below the baseline.
- **Bars:** values `[99, 87, 54, 21, 6]`, 70px wide, centered at x = 130, 245, 360, 475, 590; bars 1–4 blue `#2a78d6`, bar 5 orange `#d95926`; bold 13px value label above each bar: "99%", "87%", "54%", "21%", "6%".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=90, two lines):** "at 5 words, 94% of phrases" / "are brand new — nothing to count".
- **Caption (12px `#444`, bottom right):** "illustrative — phrase overlap by length, one user's history".

## Zero Count Doesn't Mean Impossible

**Tags:** `common mistake` (red), `smoothing` (orange)

- **The misreading** — people read "count = 0" as "the model learned this is wrong"; it only means "never seen yet"
- **After "see you next"** — the history has it 6 times: week 3, time 2, month 1 — and Friday 0, day 0
- **Raw shares** — week 50%, time 33%, month 17%, Friday 0%, day 0%: two normal words declared impossible
- **Add-one smoothing** — pretend each candidate was seen once more: week (3+1)/(6+5), time (2+1)/(6+5), and so on
- **New shares** — week 36%, time 27%, month 18%, Friday 9%, day 9% — rare now, impossible never

*Example (italic):* One borrowed pseudo-count each turns "Friday: 0%" into "Friday: 9%" — the model stops ruling out words it merely hasn't met.

**Common mistake:** Treating a zero count as evidence of impossibility. Unseen just means uncounted — smoothing sets aside a little probability for exactly those words.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: five candidate words after "see you next", raw share vs add-one smoothed share side by side, showing the zeros come alive.

- **Title (bold 15px, `#1a5276`, top center):** "After 'see you next' (6 times): raw counts vs add-one smoothing".
- **Axes:** origin x=60, baseline y=240, plot width 600, plot height 175; y axis 0–60% with light `#e5e9ef` gridlines at 20 and 40 (12px `#444` labels "0%", "20%", "40%", "60%"); category labels "week", "time", "month", "Friday", "day" centered at x = 130, 245, 360, 475, 590 in 12px `#444`, with an 11px `#6b7280` count line under each: "count 3", "count 2", "count 1", "count 0", "count 0".
- **Paired bars:** 34px wide with a 6px gap per category — raw shares blue `#2a78d6`: `[50, 33, 17, 0, 0]`; smoothed shares green `#008300`: `[36, 27, 18, 9, 9]`; 12px value labels above each bar ("50%", "36%", …); the two raw zeros get a bold 12px red `#e74c3c` "0%" sitting on the baseline.
- **Legend (top right, 12px `#444`):** blue swatch "raw share", green swatch "after add-one".
- **Annotation (bold 13px green `#008300`, near x=450, y=105, two lines):** "0% → 9%:" / "rare, not impossible".
- **Caption (12px `#444`, bottom right):** "illustrative — counts after 'see you next' in the full history".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all counts, shares, and bar values are the hardcoded arrays above (no `Math.random()`); text numbers and chart numbers must stay identical (18/12/6/4 out of 40; 2/1/1 out of 4; 99/87/54/21/6; raw 50/33/17/0/0 vs smoothed 36/27/18/9/9); invented data keeps its "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
