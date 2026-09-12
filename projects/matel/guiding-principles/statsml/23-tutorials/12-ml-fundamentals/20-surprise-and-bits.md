# Surprise & Bits

**Page type:** detail page (tutorial layout: h1 + subtitle, then card-sections each with an h2 and a text/viz table row, 50% text / 50% canvas)
**HTML title tag:** Surprise & Bits

**Subtitle:** Rare events carry more information than common ones — surprise is measured as log(1/p), in bits

## Rain in the Desert vs Rain in Seattle

Tags: `core idea` (blue), `running example` (green)

- **Seattle winter** — rain every other day (p = 1/2): "it rained" is barely news, 1 bit
- **Desert town** — rain 1 day in 32 (p = 1/32): "it rained" is big news, 5 bits
- **Same words** — the sentence is identical; the information depends on how likely it was
- **The rule** — surprise = log₂(1/p): the rarer the event, the more bits it carries
- **Sure things** — "the sun rose" has p = 1, so log₂(1) = 0 bits: no information at all

*Example:* A friend texts "it rained here!" — from Seattle you shrug, from the desert you call back.

**Key point:** Information is not about the message itself — it's about how improbable the message was before you heard it.

### Visualization (canvas `c1`, 720×300)

Two-bar chart: same sentence, different information.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Sentence Carries Different Information".
- **Data:** labels `['"it rained in Seattle"', '"it rained in the desert"']`, sub-captions `['p = 1/2 — every other day', 'p = 1/32 — one day a month']`, values `[1, 5]` bits, colors `[#2a78d6 blue, #d95926 orange]`.
- **Axes:** L-shaped gray axes (`#999`); y max 5.7; padding top 56, bottom 66, left 62, right 30. Bars 160px wide at 27%/73% of chart width, 0.75 alpha; bold 16px value labels "1 bit" / "5 bits" above bars; bold labels + gray sub-captions below.
- **Y-axis label (rotated):** "surprise = log₂(1/p), bits".
- **Annotation:** bold orange "16× rarer → 4 extra bits of news" above the desert bar.

## The Halving Ladder: log(1/p) by Hand

Tags: `worked example` (green), `rule of thumb` (orange)

- **p = 1/2** — log₂(2) = 1 bit: one fair yes/no question's worth of news
- **p = 1/4** — log₂(4) = 2 bits; p = 1/8 → 3 bits; p = 1/16 → 4 bits
- **p = 1/32** — log₂(32) = 5 bits: the desert rain
- **The pattern** — every time the probability halves, the surprise adds exactly one bit
- **Why log** — probabilities of independent events multiply; their bits simply add

*Example:* Two independent 1/4 events together: probability 1/16, surprise 2 + 2 = 4 bits.

**Key point:** log₂(1/p) counts how many halvings it takes to get down to p — that count is the bits.

### Visualization (canvas `c2`, 720×300)

Staircase bar chart: five bars climbing one bit per halving.

- **Title:** "Halve the Probability, Add One Bit".
- **Data:** x labels `p = 1/2, 1/4, 1/8, 1/16, 1/32`; values `[1, 2, 3, 4, 5]` bits. Bars 84px wide, evenly spaced; first four aqua `#199e70`, last one orange `#d95926`; 0.75 alpha; bold value labels "1 bit" … "5 bits" above bars.
- **Axes:** y max 5.7; padding top 56, bottom 66, left 62, right 30; x-axis label "probability of the event"; rotated y-axis label "surprise, bits".
- **Step annotations:** bold violet (`#4a3aa7`) "+1" between each consecutive pair of bars, placed half a bit above the lower bar.
- **Annotation:** bold orange "desert rain: 5 halvings from certainty" near top at 62% width.

## Why a Data Scientist Counts Bits

Tags: `where it's used` (blue)

- **Constant columns** — a feature that's always "yes" carries 0 bits: useless for prediction
- **Rare flags** — a fraud flag firing 1 time in 1,024 carries 10 bits when it fires
- **Entropy** — the average surprise over all outcomes: the uncertainty of a whole column
- **Log loss** — models are scored by the surprise they assign to the true answer
- **Compression** — frequent things get short codes, rare things long codes: the same math

*Example:* "User is online" predicts nothing; "user logged in from a new country" is the signal.

**Key point:** The informative signals in a dataset are usually the rare ones — bits make that intuition precise.

### Visualization (canvas `c3`, 720×300)

Curve chart: surprise log₂(1/p) vs probability, with markers.

- **Title:** "Surprise Explodes as Events Get Rare".
- **Curve:** log₂(1/p) plotted for p from 1/400 to 1 (400 steps), clipped at y max 10; violet `#4a3aa7`, line width 3. Padding top 52, bottom 52, left 62, right 30.
- **X ticks:** 0, 0.25, 0.5, 0.75, 1; x-axis label "probability p of the event"; rotated y-axis label "surprise log₂(1/p), bits".
- **Markers (6px filled dots):** p=0.5 at 1 bit, blue `#2a78d6`; p=1/32 at 5 bits, orange `#d95926`.
- **Annotations:** bold blue "Seattle rain: p = 1/2 → 1 bit"; bold orange "desert rain: p = 1/32 → 5 bits"; bold magenta (`#d55181`) "fraud flag: p = 1/1024 → 10 bits, off the top of this chart" near the top left; bold mute (`#6b7280`) right-aligned "sure thing: p = 1 → 0 bits" at the bottom right.

## The Confusion: Bits Are a Log Scale

Tags: `common mistake` (red)

- **The trap** — "5 bits vs 1 bit, so desert rain is 5 times rarer than Seattle rain"
- **The truth** — bits count doublings: 4 bits apart means 2⁴ = 16× rarer, not 4×
- **Bigger steps** — 10 bits is not "twice as newsy" as 5 bits: it's 32× rarer
- **Reading bits** — compare bits by subtracting; compare probabilities by dividing
- **Small numbers mislead** — a jump of "just 3 bits" is an 8-fold drop in probability

*Example:* A 20-bit event is not twice a 10-bit event — it's one-in-a-million vs one-in-a-thousand.

**Key point:** Bits grow slowly while rarity explodes — every +1 bit doubles how unlikely the event was.

### Visualization (canvas `c4`, 720×300)

Exponential bar chart: rarity (1 in N) vs bits.

- **Title:** "Bits Climb by Steps, Rarity Doubles Every Time".
- **Data:** x = bits 1–10; y = rarity `[2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]`. Ten 40px bars, evenly spaced; blue `#2a78d6` except the last bar magenta `#d55181`; 0.7 alpha; minimum bar height 2px so tiny values stay visible.
- **Value labels:** "1 in N" shown above the first bar and bars 5–10 only (the last one bold magenta).
- **Axes:** y max 1100; padding top 56, bottom 66, left 72, right 30; x-axis label "surprise, bits"; rotated y-axis label "rarity: the event is 1 in N".
- **Annotation (bold magenta, two lines, top left):** "10 bits ≠ 10× rarer than 1 bit:" / "it is 512× rarer (1 in 1024 vs 1 in 2)".

## Regeneration instructions

- **Template/layout:** tutorial concept page (tutorials category). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%, one 720×300 canvas).
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` line, one `.key-point` callout (`#f8f9fa` background, 3px `#e74c3c` left border).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart titles bold 15px system-ui; labels 12–13px; all data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has no links).
