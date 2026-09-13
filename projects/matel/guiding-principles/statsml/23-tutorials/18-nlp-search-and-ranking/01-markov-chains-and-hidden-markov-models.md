# Markov Chains & Hidden Markov Models

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Markov Chains & Hidden Markov Models

**Subtitle:** Weather tomorrow depends only on weather today — that's a Markov chain; when you can't see the weather and must guess it from what people wear, the chain becomes a hidden Markov model

## Weather Follows Weather, But You Only See Outfits

**Tags:** `core idea` (blue), `hidden states` (green), `emissions` (orange)

- **The basement office** — you work with no windows; the only weather clue is what coworkers wear in
- **The chain** — sunny stays sunny 80% of the time, rainy stays rainy 60%: tomorrow only looks at today
- **The Markov property** — given today's weather, last week adds nothing about tomorrow
- **The twist** — you never see the weather itself, only outfits: a t-shirt, a coat, or an umbrella
- **Emissions** — sunny days send in t-shirts 60% of the time; rainy days send in umbrellas 60%
- **The HMM** — a Markov chain you can't see, plus a noisy clue you can: that pair is a hidden Markov model

*Example (italic):* A coworker walks in carrying an umbrella — the sky stays hidden, but the umbrella whispers "probably rain."

**Key point:** An HMM has two layers: a Markov chain of hidden states (the weather) and one visible clue emitted each step (the outfit).

### Visualization (canvas `c1`, 720×300)

Two-layer diagram: the hidden weather chain (two state circles with transition arrows) on top, the three visible outfits below a "hidden / visible" divider, with dashed emission arrows carrying the probabilities.

- **Title (bold 15px, `#1a5276`, top center):** "The Two Layers: a Weather Chain You Can't See, Outfits You Can".
- **Hidden row:** circle radius 40 — SUNNY at center (200, 95), stroke 3px yellow `#c98500`, fill `rgba(201,133,0,0.12)`, bold 13px `#c98500` label; RAINY at (480, 95), stroke 3px blue `#2a78d6`, fill `rgba(42,120,214,0.12)`, bold 13px `#2a78d6` label.
- **Transition arrows (solid 2px `#6b7280`, small arrowheads):** curved arrow SUNNY→RAINY over the top labeled "0.2", curved arrow RAINY→SUNNY underneath labeled "0.4"; self-loop on SUNNY's left labeled "0.8", self-loop on RAINY's right labeled "0.6" (all labels 12px `#444`).
- **Divider:** dashed `#6b7280` (dash 4/3) horizontal line at y=170 from x=50 to x=670; 11px `#6b7280` labels "hidden" just above its left end and "visible" just below.
- **Visible row:** three rounded boxes 100×32, stroke `#6b7280`, centered at (160, 246) "t-shirt", (360, 246) "coat", (560, 246) "umbrella"; 12px `#2c3e50` labels.
- **Emission arrows (dashed 1.5px, dash 4/3, small arrowheads):** from SUNNY's bottom to each box, yellow `#c98500`, 11px labels "0.6" (t-shirt), "0.3" (coat), "0.1" (umbrella); from RAINY's bottom to each box, blue `#2a78d6`, 11px labels "0.1" (t-shirt), "0.3" (coat), "0.6" (umbrella); place each label midway along its arrow.
- **Annotation (bold 12px orange `#d95926`, right-aligned near x=690, y=190, two lines):** "you record the bottom row —" / "the top row must be guessed".
- **Caption (12px `#444`, bottom right):** "illustrative — probabilities chosen for the example".

## Two Days of Outfits, Worked by Hand

**Tags:** `worked example` (blue), `forward pass` (green)

- **Start 50/50** — first day on the job, no forecast: rain and sun start equally likely
- **Umbrella walks in** — rain sends umbrellas 60%, sun only 10%: unnormalized 0.5×0.6=0.30 vs 0.5×0.1=0.05
- **Renormalize** — 0.30 / (0.30 + 0.05) = 0.86: one umbrella lifts belief in rain from 50% to 86%
- **Step the chain** — overnight: 0.86×0.6 + 0.14×0.2 ≈ 0.54, so day two opens at 54% rain
- **T-shirt walks in** — sun sends t-shirts 60%, rain only 10%: belief in rain collapses to 17%
- **That's the forward pass** — weigh by the clue, renormalize, step the chain, repeat every day

*Example (italic):* Umbrella on Monday, t-shirt on Tuesday: belief in rain went 50% → 86% → 54% → 17%, every step by hand.

**Key point:** The forward pass is just multiply-by-clue, renormalize, step-the-chain — here it traces 50% → 86% → 54% → 17%.

### Visualization (canvas `c2`, 720×300)

Single-panel dot-and-line chart: belief in rain across the four stages of the two-day forward pass, with a 50% "no idea" guideline.

- **Title (bold 15px, `#1a5276`, top center):** "Belief in Rain, Step by Step: 50% → 86% → 54% → 17%".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y = belief in rain 0 to 100% with 12px `#444` tick labels "0%", "25%", "50%", "75%", "100%" and light `#e5e9ef` gridlines at 25/50/75.
- **Stages (x positions 145, 305, 465, 625), two-line 12px `#444` labels below the baseline:** "day 1" / "start"; "day 1" / "umbrella seen"; "day 2" / "before looking"; "day 2" / "t-shirt seen".
- **Data:** belief in rain = `[50, 86, 54, 17]` (percent), plotted at the four stage x positions.
- **Line:** 3px blue `#2a78d6` connecting the four points; dots 7px — start `#6b7280`, umbrella `#2a78d6`, chain step `#c98500`, t-shirt `#008300`; bold 13px value labels next to each dot: "50%", "86%", "54%", "17%" (above the first three, below the last).
- **Guideline:** horizontal dashed `#6b7280` (dash 4/3) line at 50%, 11px `#6b7280` label "no idea: 50%" at its left end.
- **Annotation (bold 12px orange `#d95926`, near x=330, y=80, two lines):** "clues move belief sharply —" / "the overnight step only drifts it".
- **Caption (12px `#444`, bottom right):** "illustrative — hand-computed forward pass".

## Where the Hidden Chain Shows Up in NLP

**Tags:** `where it's used` (blue), `part-of-speech tagging` (green), `sequences` (orange)

- **Words as outfits** — in a sentence the words are visible; the parts of speech behind them are hidden
- **'flies'** — a noun in "fruit flies", a verb in "time flies": the hidden tag chain settles it
- **Speech** — audio slices are the outfits; the phonemes being spoken are the hidden weather
- **Search sessions** — queries and clicks are visible; the user's shifting intent is the hidden chain
- **Without the chain** — tagging each word alone throws away the word order that makes 'flies' obvious

*Example (italic):* A tagger reading "time flies fast" one word at a time calls "flies" an insect; the tag chain calls it a verb.

**Key point:** Whenever a sequence you can see is driven by a sequence you can't, an HMM is the classic first model to reach for.

### Visualization (canvas `c3`, 720×300)

Two-layer sequence diagram in the style of c1: hidden part-of-speech tags chained on top, the visible words of "time flies fast" below, with the ambiguous middle word's rejected reading shown crossed out.

- **Title (bold 15px, `#1a5276`, top center):** "Same Trick in NLP: Hidden Tags Behind Visible Words".
- **Hidden row:** three rounded boxes 110×36, stroke 2px violet `#4a3aa7`, fill `rgba(74,58,167,0.10)`, centered at (180, 110) "NOUN", (390, 110) "VERB", (600, 110) "ADVERB"; bold 12px `#4a3aa7` labels.
- **Transition arrows:** solid 2px `#6b7280` arrows NOUN→VERB and VERB→ADVERB with small arrowheads.
- **Rejected reading:** dashed magenta `#d55181` box 110×30 centered at (390, 55) labeled "NOUN (insects?)" in 11px `#d55181`, with a 2px magenta X drawn across it.
- **Divider:** dashed `#6b7280` (dash 4/3) horizontal line at y=175 from x=50 to x=670; 11px `#6b7280` labels "hidden" above left, "visible" below.
- **Visible row:** three boxes 110×36, stroke `#6b7280`, centered at (180, 235) "time", (390, 235) "flies", (600, 235) "fast"; bold 13px `#2c3e50` words; dashed 1.5px `#6b7280` emission arrows from each tag box down to its word.
- **Annotation (bold 12px orange `#d95926`, right-aligned near x=690, y=280, two lines):** "'flies' alone is ambiguous —" / "its neighbors' tags decide".
- **Caption (12px `#444`, bottom left):** "illustrative — tags shown are the chain's pick".

## The Confusion: Chaining the Outfits Themselves

**Tags:** `common mistake` (red), `model structure` (orange)

- **The tempting shortcut** — model outfits directly: "coat today, so coat tomorrow", skipping the weather
- **Why it feels right** — outfits really do repeat day to day, so an outfit-to-outfit chain fits at first glance
- **What it misses** — coats don't cause coats; rain persists, and one rainy spell sends coats two days running
- **The tell** — an outfit chain forgets the sky each morning; the hidden chain carries a belief about rain forward
- **The fix** — put the memory in the hidden layer (weather) and treat outfits as noisy readings of it

*Example (italic):* Two coats in a row isn't a coat habit — it's one rainy spell seen twice through the wardrobe.

**Common mistake:** Putting the Markov chain on the observations instead of the hidden states — the memory belongs to the weather, not to the wardrobe.

### Visualization (canvas `c4`, 720×300)

Side-by-side contrast diagram: left panel shows the wrong model (outfits chained to outfits), right panel shows the right one (a hidden weather chain emitting outfits), separated by a dashed divider.

- **Title (bold 15px, `#1a5276`, top center):** "Where Does the Memory Live: in the Outfits or in the Sky?".
- **Divider:** vertical dashed `#e5e9ef` line at x=360 from y=55 to y=265.
- **Left panel header (bold 13px red `#e74c3c`, centered at x=190, y=70):** "WRONG: outfits drive outfits".
- **Left panel:** three rounded boxes 88×32, stroke `#6b7280`, centered at (95, 150) "coat", (190, 150) "coat", (285, 150) "umbrella", joined by solid 2px red `#e74c3c` arrows with arrowheads; 12px `#444` note centered at (190, 210): "no memory of the sky".
- **Right panel header (bold 13px green `#008300`, centered at x=540, y=70):** "RIGHT: weather drives, outfits report".
- **Right panel:** two circles radius 28 — RAINY at (455, 130), stroke blue `#2a78d6`, and RAINY at (625, 130), stroke blue `#2a78d6`, bold 11px `#2a78d6` labels; solid 2px green `#008300` arrow between them labeled "0.6" (11px `#444`); dashed 1.5px `#6b7280` emission arrows down to two boxes 88×32, stroke `#6b7280`, centered at (455, 225) "coat" and (625, 225) "coat" (12px `#2c3e50`).
- **Annotation (bold 12px green `#008300`, centered at x=360, y=288):** "coats don't cause coats — one rainy spell shows up twice".
- **Caption (12px `#444`, bottom left):** "illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all probabilities and belief values are the hardcoded numbers above (no randomness); the forward-pass series in c2 is exactly `[50, 86, 54, 17]`, matching the text (transitions 0.8/0.2 sunny, 0.6/0.4 rainy; emissions 0.6/0.3/0.1 sunny and 0.1/0.3/0.6 rainy for t-shirt/coat/umbrella).
- Diagrams (c1, c3, c4) are drawn with plain canvas primitives — arcs for circles/self-loops, `quadraticCurveTo` for curved arrows, small filled-triangle arrowheads; every font ≥11px.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
