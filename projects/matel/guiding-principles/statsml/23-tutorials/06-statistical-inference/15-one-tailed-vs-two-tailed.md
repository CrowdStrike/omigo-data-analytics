# One-Tailed vs Two-Tailed

**Page type:** detail page (tutorial layout: `.card-section` blocks, each with h2 + two-column `table.layout` — text left 50% with tag pills/bullets/example/key-point, canvas right 50%)
**HTML title tag:** One-Tailed vs Two-Tailed

**Subtitle:** Are you asking "is it different?" or "is it better?" — the question decides where you look for surprise, and it changes the answer.

## Two Questions About One Checkout Button

**Tags:** `core idea` (blue), `direction` (green)

- **The test** — a new checkout button converts 5.4% vs the old button's 5.0%
- **Question A** — "is the new button DIFFERENT?" — surprise lives in both directions
- **Question B** — "is the new button BETTER?" — surprise lives only on the high side
- **Same data** — the gap sits 1.8 wobbles above zero (z = 1.8) either way
- **Different p** — two-tailed p = 0.072 ("not significant"); one-tailed p = 0.036 ("significant")

*Example:* Identical numbers, one verdict flips — only the question asked changed.

**Key point:** "tails" are the regions of results you agreed to call surprising — two tails watch both directions, one tail watches one.

### Visualization (canvas `c1`, 720×300)

Two side-by-side standard normal bell panels comparing two-tailed vs one-tailed shading at z = 1.8.

- **Title (bold 15px, `#1a5276`, top center):** "Same Data (z = 1.8), Two Ways to Count Surprise".
- **Panels:** left panel region x=40 y=62 w=300 h=150; right panel x=390 y=62 w=300 h=150. Each panel: standard bell curve (gauss shape `exp(-0.5 z²)`), z from -3.4 to 3.4, drawn in blue `#2a78d6` width 2.5; gray `#999` baseline; z tick labels -3, -2, -1, 0, 1, 2, 3 (11px, `#2c3e50`).
- **Tail shading:** fill `rgba(217,89,38,0.45)` under the curve beyond the cutoff 1.8. Left panel shades BOTH tails (beyond ±1.8); right panel shades only the right tail (beyond +1.8).
- **Observed marker (both panels):** dashed violet `#4a3aa7` vertical line (dash 4/3, width 2) at z = 1.8, labeled above in bold 11px violet: "our z = 1.8".
- **Panel captions (bold 13px, `#1a5276`, centered at x=190 and x=540, y=250):** '"is it DIFFERENT?" — both tails' and '"is it BETTER?" — one tail'.
- **p labels (bold 14px, orange `#d95926`, y=274):** "p = 3.6% + 3.6% = 0.072" (left) and "p = 3.6% = 0.036" (right).
- **Verdict labels (bold 12px, red `#e74c3c`, y=293):** "at the 0.05 bar: not significant" (left) and "at the 0.05 bar: significant" (right).

## The Arithmetic: One Tail Is Exactly Half

**Tags:** `worked example` (green), `small numbers` (blue)

- **The counts** — 20,000 shoppers per arm: 1,000 sales (5.0%) vs 1,080 sales (5.4%)
- **The wobble** — luck alone moves a gap like this by about 0.22 points either way
- **The z** — gap 0.4 points ÷ wobble 0.22 points ≈ 1.8 wobbles above zero
- **One tail** — a fair split lands 1.8+ wobbles HIGH about 3.6% of the time: p = 0.036
- **Two tails** — 1.8+ wobbles high OR low: 3.6% + 3.6% = 7.2%: p = 0.072, exactly double

*Example:* At the 5% bar, one-tailed needs a 0.37-point lift to fire; two-tailed needs 0.44.

**Key point:** for a symmetric test like this z-test, with the effect in the tested direction, the two-tailed p is exactly double the one-tailed p.

### Visualization (canvas `c2`, 720×300)

Number line of conversion lift (percentage points) with the luck-only bell, both significance cutoffs, and the observed lift.

- **Title (bold 15px, `#1a5276`, top center):** "20,000 Shoppers per Arm: Where the 0.4-Point Lift Lands".
- **Axis:** horizontal line at y=170 from x=80 spanning 570px; value range -0.3 to +0.7; tick labels -0.2, 0.0, +0.2, +0.4, +0.6 (12px, positive values with "+" prefix); axis caption below: "lift in conversion, percentage points".
- **Luck bell:** normal curve centered at 0 with sd = 0.222 pt, height 100px, stroked `rgba(42,120,214,0.8)` width 2; labeled above in bold 12px blue `#2a78d6`: "what luck alone produces" / "(wobble ≈ 0.22 pt)".
- **One-tailed cutoff:** dashed green `#008300` vertical line (dash 5/4, width 2) at +0.37, label right-aligned bold 11px green: "one-tailed bar: +0.37".
- **Two-tailed cutoff:** dashed magenta `#d55181` vertical line (dash 5/4, width 2) at +0.44, label left-aligned bold 11px magenta: "two-tailed bar: +0.44".
- **Observed lift:** violet `#4a3aa7` filled dot (radius 8) on the axis at +0.4 with a vertical violet pointer line, labeled bold 13px: "observed: +0.4 (1,080 vs 1,000 sales)".
- **Takeaway (bold 13px orange `#d95926`, centered, y=262):** "+0.4 clears the one-tailed bar but not the two-tailed bar — the whole disagreement".
- **Footnote (12px gray `#6b7280`, centered, y=284):** "z = 0.4 ÷ 0.22 ≈ 1.8;  one tail 3.6%, both tails 7.2%".

## Declare Before You Peek

**Tags:** `where it's used` (blue), `choosing upfront` (orange), `easy to abuse` (red)

- **Legit use** — only one direction is actionable AND you commit to it before the data arrives
- **The abuse** — run two-tailed, see p = 0.072, quietly switch to one-tailed p = 0.036, publish
- **The cost** — picking the friendly tail after peeking fires falsely 10% of the time, not 5%
- **Why 10%** — you kept 5% of surprise in EACH tail and claim whichever one the data lands in
- **The habit** — write the direction (or lack of one) into the test plan before launch

*Example:* "We predicted an increase" written after seeing an increase is the oldest trick in the drawer.

**Key point:** a one-tailed test chosen after seeing the data is a two-tailed test with double the advertised false-alarm rate.

### Visualization (canvas `c3`, 720×300)

Three-bar chart of false-alarm rates: honest declarations vs tail picked after peeking.

- **Title (bold 15px, `#1a5276`, top center):** "False-Alarm Rate When the Button Actually Does Nothing".
- **Axes:** L-shaped gray `#999` axes; padding top 60, bottom 70, left 80, right 40; y scale 0-12%, tick labels "0%", "5%", "10%" right-aligned 12px.
- **Bars (width 120px, fill at 0.7 alpha):**
  - "two-tailed, / declared upfront": 5%, blue `#2a78d6`
  - "one-tailed, / declared upfront": 5%, green `#008300`
  - "tail picked AFTER / seeing the data": 10%, red `#e74c3c`
- Value labels ("5%", "5%", "10%") bold 14px `#2c3e50` above each bar; two-line category labels 12px below the baseline.
- **Advertised line:** dashed gray `#6b7280` horizontal line (dash 5/4, width 1.5) at 5%, labeled bold 11px: "advertised rate: 5%".
- **Caption (bold 13px red `#e74c3c`, bottom center):** "claiming whichever tail the data lands in = 5% + 5% = 10% false alarms".

## One-Tailed Is Not a Discount

**Tags:** `common mistake` (red), `blind side` (orange)

- **The misread** — "one-tailed is easier to pass, so always use it" — the ease is not free
- **The blind side** — a one-tailed "better?" test cannot flag a button that is WORSE
- **Same evidence bar** — you moved all 5% of doubt to one side; you didn't lower the bar
- **Ask honestly** — would a big drop change your decision? If yes, you need two tails
- **Default** — most A/B platforms and journals expect two-tailed for exactly this reason

*Example:* A team ran "is it better?" one-tailed; the new button was 0.5 points worse and the test, by design, stayed silent.

**Key point:** choose one-tailed only when the other direction truly would not change what you do — that is rarer than it sounds.

### Visualization (canvas `c4`, 720×300)

Single wide bell panel showing the one-tailed watched zone vs the blind zone, with a worse-button result landing unflagged.

- **Title (bold 15px, `#1a5276`, top center):** "The One-Tailed Blind Side: A Worse Button Goes Unflagged".
- **Panel:** region x=70 y=62 w=580 h=150; standard bell (gauss) z from -3.4 to 3.4, blue `#2a78d6` curve width 2.5, gray `#999` baseline; z ticks -3…3 (11px); axis caption 12px: 'z (wobbles away from "no difference")'.
- **Watched zone:** right tail beyond z = 1.645 filled `rgba(0,131,0,0.35)` (green).
- **Blind zone:** entire left half (z < 0) shaded `rgba(107,114,128,0.15)` gray rectangle over the panel height.
- **Worse-button marker:** red `#e74c3c` filled dot (radius 8) on the baseline at z = -2.3 with a vertical red pointer line, labeled bold 12px: "new button 0.5 pt WORSE (z = -2.3)".
- **Zone labels:** green `#008300` bold 12px near x(z=2.5): "the only zone" / "the test watches"; gray `#6b7280` bold 12px near x(z=-1.7): 'blind zone: "is it better?" never looks here'.
- **Caption (bold 13px red `#e74c3c`, centered, y=282):** "a drop that would matter lands in the blind zone — by design, the test stays silent".

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each starting with `<b>` term in `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** each 720×300 intrinsic, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
