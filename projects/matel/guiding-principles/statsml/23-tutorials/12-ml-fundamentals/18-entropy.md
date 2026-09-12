# Entropy

**Page type:** detail page (tutorial layout: h1 + subtitle, then card-sections each with an h2 and a text/viz table row, 45% text / 55% canvas)
**HTML title tag:** Entropy

**Subtitle:** A single number for how uncertain you are before you look — the average surprise per outcome, measured in bits

## Three Coins, Three Amounts of Uncertainty

Tags: `core idea` (blue), `running example` (green)

- **Fair coin** — heads and tails equally likely: as unsure as a coin can make you, 1 bit
- **90/10 coin** — you'd bet heads and usually win, so far less uncertainty: 0.47 bits
- **Always-heads coin** — the outcome is known before the flip: 0 bits
- **Entropy** — the name for this number: the average surprise you feel per flip
- **The scale** — 0 bits means "no question at all"; 1 bit means "one fair yes/no question"

*Example:* A customer base where exactly half churn is a fair coin: 1 bit of uncertainty per customer.

**Key point:** Entropy measures how unpredictable outcomes are before you see them — even odds is the maximum, a sure thing is zero.

### Visualization (canvas `c1`, 720×300)

Bar chart: entropy of three coins.

- **Title (bold 15px, `#1a5276`, top center):** "Three Coins: How Uncertain Is the Next Flip?"
- **Data:** labels `['fair coin (50/50)', '90/10 coin', 'always-heads coin']`, values `[1.00, 0.47, 0.00]` bits, bar colors `[#2a78d6 blue, #d95926 orange, #6b7280 mute]`.
- **Axes:** L-shaped gray axes (`#999`); y scale max 1.1; padding top 56, bottom 66, left 62, right 30. Bars 130px wide at 18%/50%/82% of chart width; fill at 0.75 alpha (no bar drawn for 0.00). Value labels "1.00 bits" / "0.47 bits" / "0.00 bits" bold above each bar in the bar's color; bold dark label below each bar; gray sub-caption under each: "a coin flip, a churn label, any yes/no outcome" / "predictable, but not certain" / "no uncertainty at all".
- **Y-axis label (rotated):** "entropy, bits".
- **Annotation:** bold blue (`#2a78d6`) "even odds = maximum uncertainty" near top at 42% of chart width.

## Getting 0.47 by Hand for the 90/10 Coin

Tags: `worked example` (green), `rule of thumb` (blue)

- **Surprise of heads** — log₂(1/0.9) = 0.15 bits: an expected outcome barely registers
- **Surprise of tails** — log₂(1/0.1) = 3.32 bits: a rare outcome is big news
- **Weight by chance** — heads: 0.9 × 0.15 = 0.14; tails: 0.1 × 3.32 = 0.33
- **Add them up** — 0.14 + 0.33 = 0.47 bits of average surprise per flip
- **Check the fair coin** — 0.5 × 1 + 0.5 × 1 = 1 bit, exactly as promised

*Example:* The rare tail is 22× more surprising than heads, but it shows up only 1 flip in 10.

**Key point:** Entropy = each outcome's surprise × how often it happens, summed — literally the average surprise.

This section uses the three-column layout (`table.layout.three`: text 38%, two viz columns 31% each) with two side-by-side canvases.

### Visualization (canvas `c2a`, 420×340)

Bar chart: surprise per outcome for the 90/10 coin.

- **Title:** "Step 1 — Surprise per Outcome"; gray subtitle "90/10 coin: surprise = log₂(1/p)".
- **Data:** labels `['heads (p = 0.9)', 'tails (p = 0.1)']`, values `[0.15, 3.32]` bits, colors `[#008300 green, #d55181 magenta]`.
- **Axes:** L-shaped gray axes; y max 3.7; padding top 60, bottom 70, left 56, right 20; bars 110px wide at 28%/72% width; 0.75 alpha fill; bold value labels "0.15 bits" / "3.32 bits" above bars.
- **Annotations:** bold magenta "rare tails: 22× the surprise" near the tails bar; gray bottom caption "...but how often does each happen?".

### Visualization (canvas `c2b`, 420×340)

Bar chart: weighted contributions summing to entropy.

- **Title:** "Step 2 — Weight by Frequency, Add"; gray subtitle "contribution = p × surprise".
- **Data:** labels `['heads', 'tails', 'entropy']`, sub-captions `['0.9 × 0.15', '0.1 × 3.32', '0.14 + 0.33']`, values `[0.14, 0.33, 0.47]`, colors `[#008300 green, #d55181 magenta, #d95926 orange]`.
- **Third bar is stacked:** green segment for the heads part (0.14) with magenta segment for the tails part (0.33) on top.
- **Axes:** y max 0.56; bars 90px wide at 18%/50%/82% width; bold value labels above bars; bold labels + gray sub-captions below.
- **Annotation:** bold orange "average surprise = 0.47 bits" near the top center.

## Where a Data Scientist Meets It: The Churn Curve

Tags: `where it's used` (blue), `rule of thumb` (orange)

- **Churn label** — a customer base that's 50/50 churn vs stay is maximally uncertain: 1 bit
- **Skewed base** — 90% stay / 10% churn is the 90/10 coin again: 0.47 bits
- **The curve** — entropy peaks at 50/50 and slides toward 0 as the split gets lopsided
- **Decision trees** — every candidate split is scored by how much it lowers this number
- **Class imbalance** — a 99/1 fraud label holds only 0.08 bits: easy to fake accuracy on

*Example:* Guessing "stays" on a 99/1 base is right 99% of the time — there was almost no uncertainty to remove.

**Key point:** Before modeling a yes/no label, the entropy of its base rate tells you how much uncertainty there actually is to predict away.

### Visualization (canvas `c3`, 720×300)

Curve chart: binary entropy H(p) vs base rate, with three markers.

- **Title:** "Entropy of a Yes/No Label vs Its Base Rate".
- **Curve:** H(p) = −(p·log p + (1−p)·log(1−p))/ln 2 plotted for p in [0,1] at 100 steps, blue `#2a78d6`, line width 3. Y max 1.1; padding top 52, bottom 52, left 62, right 30.
- **X ticks:** 0, 0.25, 0.5, 0.75, 1; x-axis label "share of customers who churn"; rotated y-axis label "entropy, bits".
- **Markers (6px filled dots):** p=0.5 at 1.00 orange `#d95926`; p=0.1 at 0.47 green `#008300`; p=0.01 at 0.08 magenta `#d55181` (p is the churn share, so 90% stay puts the marker at 0.1).
- **Marker labels:** bold orange "50/50 churn: 1 bit — maximum uncertainty" above the peak; bold green left-aligned "90% stay: 0.47 bits" below-right of its marker; bold magenta left-aligned "99/1 fraud label: 0.08 bits" right of its marker.

## The Confusion: Average Surprise, Not Biggest Surprise

Tags: `common mistake` (red)

- **The trap** — "tails is 3.32 bits of surprise, so the 90/10 coin must have high entropy"
- **The fix** — that big surprise happens only 1 flip in 10, so it barely moves the average
- **Symmetry** — a 10/90 coin has the same 0.47 bits as a 90/10 coin: only the split matters
- **Labels don't matter** — entropy ignores what the outcomes are, only how probable they are
- **Rare ≠ chaotic** — a coin with one rare outcome is still mostly predictable

*Example:* One earthquake a decade is enormous news, yet "no earthquake today" keeps daily entropy near zero.

**Key point:** Entropy weights each surprise by how often it happens — one huge but rare surprise still averages out small.

### Visualization (canvas `c4`, 720×300)

Bar chart: biggest surprise vs average surprise for the 90/10 coin.

- **Title:** "90/10 Coin: One Big Surprise, Small Average".
- **Data:** labels `['surprise if heads', 'surprise if tails', 'entropy (the average)']`, sub-captions `['happens 9 in 10', 'happens 1 in 10', 'what you feel per flip']`, values `[0.15, 3.32, 0.47]` bits, colors `[#008300 green, #d55181 magenta, #d95926 orange]`.
- **Axes:** y max 3.7; bars 130px wide at 18%/50%/82% width; 0.75 alpha fill; bold value labels ("0.15 bits" etc.) above bars; rotated y-axis label "bits".
- **Annotation:** bold magenta "the 3.32-bit shock is real — but rare, so the average stays at 0.47" near top at 55% width.

## Regeneration instructions

- **Template/layout:** tutorial concept page (tutorials category). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (45%) and right `td.viz-col` (55%, one canvas). Section 2 uses `table.layout.three` (text 38%, two viz columns 31% each) with two canvases.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` line, one `.key-point` callout (`#f8f9fa` background, 3px `#e74c3c` left border).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart titles bold 15px system-ui; axis/data labels 12–13px; all data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has no links).
