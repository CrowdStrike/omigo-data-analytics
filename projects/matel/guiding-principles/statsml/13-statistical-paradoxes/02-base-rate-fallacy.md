# Base Rate Fallacy: Hunt Something Rare and a Great Test Still Cries Wolf

**Page type:** detail page — card-section template (see `tutorials/llms-generative-ai/08-positional-encoding.html`)
**HTML title tag:** Base Rate Fallacy — Statistical Paradoxes

**Subtitle:** A detector that is 99% accurate still gets it wrong on 10 of every 11 people it flags — nothing is broken, the thing it hunts is simply rare

---

## Section 1 — A 99% Accurate Test, and Only 1 Flag in 11 Is Real

**Tags:** `core idea` (blue), `count the people` (green), `rarity` (orange)

**Bullets:**
- **The setup** — 1 account in 1,000 is fake, and the detector is 99% accurate
- **Who is who** — screen 100,000 accounts: 100 are fake, 99,900 are real people
- **Real flags** — the detector catches 99 of the 100 fake accounts and misses one
- **False flags** — it also flags 1% of the 99,900 real accounts, which is 999 people
- **The answer** — about 100 real flags against about 1,000 false ones, so 1 flag in 11 is real

**Example line (italic):** Suspend every flagged account and you lock out 999 real users to catch 99 fakes — 10 innocent people per offender.

**Key point:** The innocent group is so much bigger that its 1% of wrong flags buries every right one.

**Source note (`.src`):** Illustrative Example — a 1-in-1,000 rate and a detector that is 99% accurate on both the fake and the real accounts.

### Visualization — canvas `c1`, 720×340

A waffle of every flag the screen produces, with the real-fake block visibly dwarfed. All counts derived in the draw function from `N=100000`, `prev=0.001`, `sens=0.99`, `fpr=0.01`.

- **Derived:** 100 fake, 99 caught, 1 missed, 99,900 real, 999 wrongly flagged, 98,901 correctly left alone, 1,098 flags, 9.0% of flags real. `99 + 1 + 999 + 98,901 = 100,000` exactly.
- **Waffle grid is exact:** `61 columns × 18 rows = 1,098` cells, one per flag, painted row-major — the first 99 cells are the caught fakes, the remaining 999 are real users. No rounding anywhere.
- **Title (bold 15px `P.ink`, centered, y=22):** "Screen 100,000 Accounts — Then Look at Who Got Flagged"
- **Sub-note (12px `P.mute`, left at `LX=30`, y=44):** "Each square = one flagged account. 61 × 18 = 1,098 flags." — the `1,098` printed from the computed total.
- **Grid geometry:** `LX=30`, `TOP=62`, `cell = floor(min((w − 2·LX − 150) / 61, 10))`, so `GW = 61·cell`; 18 rows tall. Caught fakes `rgba(0,131,0,0.55)` stroked `P.green`; wrongly flagged `rgba(213,81,129,0.30)` stroked `rgba(213,81,129,0.55)`.
- **Legend column** at `LX + GW + 18`: an 11×11 green swatch + bold 12px `P.green` "99 fakes caught" and 12px `P.mute` "the detector was right"; below it a magenta swatch + bold 12px `P.magenta` "999 real users" and 12px `P.mute` "flagged anyway".
- **Big figure (below the grid, left at `LX`):** bold 19px `P.magenta` "1 in 11" then 12px `P.mute` "flagged accounts is really fake", the ratio computed as `positives/tp` and printed to zero decimals.
- **Two derivation lines (12px `P.mute`, left at `LX`, spaced 20px in pixels):** "100 fake accounts → 99 caught, 1 missed" and "99,900 real accounts → 999 wrongly flagged" — counts printed from variables.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The detector is 99% accurate. Only 1 flag in 11 is real."

---

## Section 2 — A Real Case: Physicians Overshot by Tenfold

**Tags:** `real case` (blue), `screening` (green), `documented error` (red)

**Bullets:**
- **The study** — in 1982 David Eddy asked physicians one plain screening question
- **The setup** — 1 woman in 100 in that age group has cancer when the scan is taken
- **The scan** — it spots 80 of every 100 real cancers and wrongly flags about 1 healthy woman in 10
- **The answers** — asked what a positive scan means, most physicians said about 75%
- **The truth** — per 10,000 women: 80 real cancers flagged, about 950 healthy women flagged
- **The gap** — 80 real out of roughly 1,030 flags is 8 in 100, about a tenth of what they answered

**Example line (italic):** A positive scan moves her from 1 in 100 to about 8 in 100 — worth a follow-up, nothing like the near-certainty 75% implies.

**Key point:** They answered with the scan's detection rate instead of asking how many flags are real — Eddy (1982), "Probabilistic reasoning in clinical medicine".

**Source note (`.src`):** Rates as stated in the study — 1% prevalence, 80% detection, 9.6% false-positive rate. Counts are computed from those rates; per 10,000 women the false-flag count is 950.4, so prose says "about 950".

### Visualization — canvas `c2`, 720×340

The positive-scan pile split into real and false on the left; the answer physicians gave against the answer the same rates give on the right. Every figure derived from `N=10000`, `prev=0.01`, `sens=0.80`, `fpr=0.096`.

- **Derived:** 100 cancers, 80 flagged, 20 missed, 9,900 healthy, 950.4 wrongly flagged, 1,030.4 flags, 7.8% real, physician answer 75% is 9.7× the truth. Counts print rounded; the percentage is computed from the unrounded values, so it matches the per-100,000 scaling to the digit.
- **Title (bold 15px `P.ink`, centered, y=22):** "What a Positive Mammogram Actually Means"
- **Left panel header:** bold 12px `P.ink` at x=30, y=48 "THE 1,030 POSITIVE SCANS"; 12px `P.mute` at x=30, y=64 "per 10,000 women screened".
- **Stacked column:** x=48, width 64, from y=76 to y=276 (200px). Real share `tp/positives` drawn at the bottom in `rgba(0,131,0,0.60)` stroked `P.green`; the rest above in `rgba(213,81,129,0.28)` stroked `rgba(213,81,129,0.55)`.
- **Left labels at x=124:** bold 12px `P.magenta` "950 false alarms" (y=150) with 12px `P.mute` "healthy, wrongly flagged" (y=166); bold 12px `P.green` "80 real cancers" (y=262) with 12px `P.mute` "the whole true signal" (y=278).
- **Right panel header:** bold 12px `P.ink` at x=300, y=48 "CHANCE OF CANCER GIVEN A POSITIVE SCAN".
- **Two bars** on a shared 0–100% scale, `SX=310`, `SW=250`, height 28:
  - 12px `P.mute` at x=300, y=76 "what most physicians answered"; bar y=84, width `SW·0.75`, `rgba(213,81,129,0.35)` stroked `P.magenta`; bold 19px `P.magenta` "75%" 10px past the bar end (zero decimals).
  - 12px `P.mute` at x=300, y=146 "the correct answer from those same rates"; bar y=154, width `SW·7.8/100`, `rgba(0,131,0,0.55)` stroked `P.green`; bold 19px `P.green` "7.8%" 10px past the bar end, printed from the computed rate (one decimal).
- **Scale:** `#ccc` line at y=196 from `SX` to `SX+SW`; 12px `P.mute` ticks 0 / 25 / 50 / 75 / 100% at y=212.
- **Annotations:** bold 12px `P.violet` at x=300, y=242 "the common answer was 9.7× the truth" (ratio computed); 12px `P.mute` at x=300, y=264 "80 real ÷ 1,030 flags = 7.8%".
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "A positive scan moves her from 1 in 100 to about 8 in 100."

---

## Section 3 — Make the Target Common and the Same Test Turns Honest

**Tags:** `the boundary` (blue), `when it is safe` (green), `the fix` (orange)

**Bullets:**
- **The condition** — the trap springs only when the thing you hunt is rare in the group you test
- **The turning point** — for a 99% accurate test, a flag is a coin flip at 1 in 100
- **Common target** — at 1 in 10 a flag is 92% likely to be real, and the test never changed
- **Fix one** — test a group where the target is common instead of screening everybody
- **Fix two** — state results as counts in a real crowd: 99 real flags, 999 false, out of 100,000
- **Fix three** — ask how many flags are real, not how often the test is right

**Example line (italic):** The same 99% accurate test: 1 flag in 11 is real when 1 in 1,000 is affected, and 9 flags in 10 are real when 1 in 10 is.

**Key point:** Ask how rare the target is first — the test's own accuracy cannot answer it.

**Source note (`.src`):** Illustrative Example — the curve is computed from the detector used above, so every point on it follows from the stated detection and false-alarm rates.

### Visualization — canvas `c3`, 720×320

How many flags are real, plotted against how common the target is, on a stretched (logarithmic) rarity axis. Curve sampled and every marked value computed in the draw function.

- **Curve:** `right(p) = 100·p·sens / (p·sens + (1−p)·fpr)` with `sens=0.99`, `fpr=0.01`, sampled at 200 points, in `P.blue` width 2.5. One curve only — no second test.
- **Even-odds crossing, computed:** `p = fpr / (sens + fpr)` → exactly 1 in 100 for this test.
- **Title (bold 15px `P.ink`, centered, y=22):** "The Same Test Turns Trustworthy as the Target Gets Common"
- **Plot box:** `PX0=70`, `PX1=w−30`, `PY0=52`, `PY1=h−62`. x is the rate from 0.05% to 60% on a log scale; y is 0–100%.
- **Grid:** `P.grid` lines at 0 / 25 / 50 / 75 / 100 with right-aligned 12px `P.mute` labels; the 50% line drawn `#999` dashed (dash 4/3) with bold 12px `P.mute` "a coin flip" above its left end.
- **Axes:** `#ccc` baseline; 12px `P.mute` x-ticks at 0.1% / 0.3% / 1% / 3% / 10% / 30%; x title "how common the target is in the group you test"; rotated y title "how many flags are real".
- **Curve label:** 12px `P.blue` "a 99% accurate test" placed on the curve near 0.4% on the rate axis.
- **Markers:** `P.magenta` dot at 0.1%, label "1 in 1,000 → 9%"; `P.orange` dot at the even-odds crossing, label "a coin flip at 1 in 100"; `P.green` dot at 10%, label "1 in 10 → 92%" right-aligned below the point. Every printed value read from the curve function, never typed.
- **Caption (bold 13px `P.ink`, centered, `h−8`):** "Rarity, not the test, is what makes a flag meaningless."

---

## Regeneration instructions

- **Template:** the card-section layout from `tutorials/llms-generative-ai/08-positional-encoding.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Three sections, not four:** the mechanism, the documented case, the boundary. A fourth section was removed because it repeated the mechanism's arithmetic in a second medical example.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. The canvas is capped at 720px, so a wide cell leaves slack — centering puts the chart in the middle of the right half instead of flush against the text.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>term</b>` → one italic `.example` line → one `.key-point` callout → a `.src` note. No paragraph blocks, no `.philosophy` box, no data tables, no `.labels` strip, no `.subhead` line.
- **Bullets:** 5–6 per section, each ONE line that does not wrap at 50% column width (~≤100 characters), opening with a `<b>bold term</b>` followed by an em dash and the fact.
- **Language rules this page follows:**
  - One name for the quantity: **"how many flags are real"**. Never "how often it is right when it says yes".
  - Accuracy is stated as a percentage ("99% accurate") — the phrase a reader actually hears — never as "right 99 times in 100".
  - Counts while walking the chain, one percent as the verdict. Ratios only for before/after jumps (1 flag in 11; 1 in 100 → 8 in 100).
  - `.key-point` is ONE short sentence. It names the mechanism; it does not recap the bullets.
  - Bullet labels name their content (*Who is who*, *Real flags*, *False flags*, *The answer*), not drama (*The catch*, *The flood*, *Why it shocks*).
  - Numbers chosen to survive mental arithmetic: 100 versus 1,000, 1 flag in 11, 10 innocent per offender.
- **Deliberately absent:** any claim that a second independent test lifts 9-in-100 to 91-in-100. The arithmetic is right only if the two tests fail independently, and retesting the same person for the same condition is the textbook case where they do not.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.red` `rgba(231,76,60,0.12)`/`#e74c3c`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart `height`. `setup(id)` caches the logical size in `dataset` on the first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart section header bold 12–13px; body/axis labels 12px (floor); the single big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Green is the honest/real quantity, magenta the misleading one, orange the mechanism (the false-alarm rate, the crossing point).
- **Determinism:** no `Math.random()` anywhere. Every chart is arithmetic on stated rates, so no pseudo-random data is needed; a shared `flagsReal(prev, sens, fpr)` helper computes how many flags are real and every printed percentage comes from it. Counts are computed from population size and rates in the draw function so a label can never drift from the plotted bar.
- **Chart order in the document:** `c1`, `c2`, `c3` — one per section, in order.
