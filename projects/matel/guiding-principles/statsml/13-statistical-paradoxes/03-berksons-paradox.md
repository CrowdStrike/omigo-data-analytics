# Berkson's Paradox: The Door Invents the Trade-Off

**Page type:** detail page — card-section template (see `tutorials/llms-generative-ai/08-positional-encoding.html`)
**HTML title tag:** Berkson's Paradox — Statistical Paradoxes

**Subtitle:** Why the best coders you hired seem to be the worst communicators — even though, out in the world, the two skills have nothing to do with each other

---

## Section 1 — Look at Your Hires and You'll Swear It's a Trade-Off

**Tags:** `core idea` (blue), `selection` (green), `missing corner` (orange)

**Bullets:**
- **What you notice** — your best coders keep turning out to be your worst communicators
- **The truth** — across all 200 applicants the two skills have no connection at all
- **Your door** — you rejected the 87 people who were weak at both skills
- **The missing corner** — those 87 are gone, so one whole corner of the picture is empty
- **Why it fakes it** — a weak coder who got in must be strong at talking, or he'd be out
- **Nobody changed** — same people, same skills, one corner of the picture cut away

**Example line (italic):** Among the 113 people you hired the two skills look like a trade-off; among all 200 who applied they are unrelated.

**Key point:** Berkson's paradox is a fake link manufactured by the entry rule — screen on "good at either one" and two unrelated skills start to look like a trade-off.

### Visualization — canvas `c1`, 720×300

Two scatter panels: the full applicant pool, then the same pool with the weak-at-both corner removed. Both fitted lines and both printed correlations are computed at render time from the plotted points.

- **Data:** seeded Park–Miller LCG, seed 42; 200 points, `x = rng()`, `y = rng()`. Hired = `x + y > 0.9` (113 points); rejected = the other 87.
- **Title (bold 15px `P.ink`, centered, y=22):** "Same 200 People. One Corner Removed."
- **Panel geometry:** `ML=56`, `MR=24`, `GAP=54`, `PW=(720−ML−MR−GAP)/2`, `PY=92`, `PH=h−PY−44`.
- **Header stack per panel (left-aligned at the panel's x):** bold 13px `P.ink` at `PY−54` — "ALL 200 APPLICANTS" / "THE 113 HIRED"; bold 13px verdict at `PY−38` — "No connection at all" (`P.green`) / "Now: one or the other" (`P.magenta`); 12px `P.mute` at `PY−21` — "(correlation +0.02)" / "(correlation −0.47)", printed from `fit()`.
- **Right panel only:** the cut corner filled `rgba(107,114,128,0.10)` (triangle `(0,0.9) → (0.9,0) → (0,0)`); the 87 rejected as radius-3 dots in `rgba(107,114,128,0.28)`; the hiring bar a dashed `P.orange` line (dash 6/4) along `x + y = 0.9`, labelled "the hiring bar" rotated 45° near `(0.17, 0.79)`; a two-line 12px `P.mute` note at `(0.03, 0.22)` and 15px below it — "weak at both —" / "never hired, never seen".
- **Points:** full pool `rgba(42,120,214,0.45)` radius 4; hired `rgba(42,120,214,0.62)` radius 4.5.
- **Fitted lines:** clipped to the panel, width 2.5 — left `P.green`, right `P.magenta`, drawn from the least-squares slope through the means.
- **Axes:** `#ccc` L-shape; 12px `P.mute` "Coding skill →" under each panel; rotated "Communication skill →" left of the first panel only.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The door created the trade-off, not the people."

---

## Section 2 — The Same Trick Makes a Disease Look Like a Cure

**Tags:** `where it bites` (blue), `two conditions` (green), `admission rule` (orange)

**Bullets:**
- **Two conditions** — in this town one person in ten has A, and one in ten has B
- **Unrelated** — having A tells you nothing at all about whether you have B
- **The door** — the ward admits you if you have A, or B, or got hurt another way
- **Patients with A** — only 10% of them turn out to also have condition B
- **Patients without A** — 64% of them have B, more than six times as many
- **The wrong read** — A looks like it protects against B, though neither touches the other
- **The real reason** — with no A, something graver put you in that bed, usually B

**Example line (italic):** In the ward, 1,000 of the 10,000 patients with A also have B — but 9,000 of the 14,000 without A do.

**Key point:** When admission needs any one of several conditions, those conditions look mutually exclusive inside the ward. Read the rates in the whole town, never in the ward.

### Visualization — canvas `c2`, 720×340

A 10×10 waffle of the whole town beside the ward it feeds, separated by the admission door.

- **Waffle:** 100 squares, each = 1,000 people, painted row-major in group order. `9 + 1 + 9 + 5 + 76 = 100`, exact by construction — no rounding.

  | Group | Squares | Fill | Stroke | Legend |
  |-------|---------|------|--------|--------|
  | has A only | 9 | `rgba(42,120,214,0.50)` | `P.blue` | "has A only — 9,000" |
  | has both | 1 | `rgba(74,58,167,0.55)` | `P.violet` | "has both — 1,000" |
  | has B only | 9 | `rgba(25,158,112,0.50)` | `P.aqua` | "has B only — 9,000" |
  | hurt another way | 5 | `rgba(201,133,0,0.55)` | `P.yellow` | "hurt another way — 5,000" |
  | never admitted | 76 | `rgba(107,114,128,0.16)` | `#dcdfe4` | "never admitted — 76,000 — never seen" |

- **Title (bold 15px `P.ink`, centered, y=22):** "Who Gets In Decides What the Ward Shows"
- **Waffle header (bold 12px `P.ink`, left at `LX=42`, y=`TOP−10`, `TOP=62`):** "THE WHOLE TOWN — each square = 1,000 people". Cell size `= min(0.40w/10, gridMax/10)`.
- **Legend:** five rows on a 17px pitch starting 18px under the waffle; 11×11 swatch, 12px label (`P.text` for admitted groups, `P.mute` for the never-admitted).
- **The door:** dashed `P.orange` vertical line (dash 5/4) at `GX = LX + GW + 0.055w`, spanning the waffle; bold 12px `P.orange` "THE DOOR" at `TOP−32`, then 12px "admitted if A, or B," / "or hurt another way".
- **Ward panel** at `RX = GX + 0.055w`: bold 12px `P.ink` "IN THE WARD: 24,000" above two square-strip rows —
  - "HAVE A — 10,000" (`P.blue`): 10 squares, 1 filled; note "only 1 in 10 also has B"; bold 19px "10%" + 12px "have condition B".
  - "WITHOUT A — 14,000" (`P.magenta`): 14 squares, 9 filled; note "9 of 14 also have B"; bold 19px "64%" + 12px "have condition B".
- **Footnote (12px `P.mute`, left at `LX`, `h−26`):** "Filled square = also has B. Town-wide it is 10% either way." Bottom-left because the ward column has no room beside it.
- **Caption (bold 13px `P.magenta`, centered, `h−9`):** "A looks protective. Neither condition affects the other at all."

---

## Section 3 — A Real Case: Smoking Appeared to Protect Newborns

**Tags:** `real case` (blue), `health data` (green), `decades unexplained` (red)

**Bullets:**
- **The finding** — among underweight newborns, smokers' babies died less often than others
- **Long unexplained** — the reversal was reported for decades and treated as a real puzzle
- **The truth** — counting all births, smoking raises the death rate 1.67× (9.9 vs 6.0)
- **The door** — the comparison kept only babies born underweight and discarded the rest
- **Two ways to be small** — the mother's smoking, or a serious birth defect in the baby
- **The mechanism** — a small baby of a non-smoker far more often has the graver cause
- **By the numbers** — 31% of their small babies had a defect, against 3% of smokers'
- **The fix** — weight sits between smoking and death, so compare all births, never only the small

**Example line (italic):** Underweight babies only: 16.0 deaths per 1,000 for smokers against 95.8 for non-smokers — smoking "reads" six times protective.

**Key point:** Filtering on something that both the cause and the outcome influence — here birth weight — can flip a harm into an apparent benefit. The reversal is real and documented (Hernández-Díaz, Schisterman & Hernán, 2006).

**Source note (`.src`):** Illustrative Example — figures computed from a constructed population in which smoking is genuinely harmful and never protective.

### Visualization — canvas `c4`, 720×340

Two paired-bar panels (all births vs underweight only) over a "why" strip showing what made each group's babies small. Every figure computed in the draw function.

- **Construction:** `N=100,000`, `pSmoke=0.20`, `pDefect=0.01`, independent. `pLow`: baseline 0.02, smoking adds an independent 0.30, a defect adds 0.90. `pDie`: 0.30 with a defect else 0.003, and smoking adds an independent 0.004 on top. Nothing in the construction makes smoking protective.
- **Derived figures:** all births 9.9 vs 6.0 per 1,000 (ratio 1.67×); underweight only 16.0 vs 95.8 (ratio 0.17×); defect share among the underweight 3% for smokers, 31% for non-smokers.
- **Title (bold 15px `P.ink`, centered, y=22):** "Same Babies. Two Ways to Count Them."
- **Scale note (12px `P.mute`, left at x=30, y=70):** "Bar height = deaths per 1,000 births."
- **Panels:** `PW=(720−60−30)/2`, panel *i* at `x = 30 + i·(PW+30)`; `baseY = h−150`, `barMaxH = baseY−74`; bars scaled to the largest of the four rates so the two panels are visually comparable.
  - Left, accent `P.green`: bold 12px "ALL BIRTHS", 12px `P.mute` "the honest comparison", verdict "smoking is worse" + "1.67× the death rate".
  - Right, accent `P.magenta`: "UNDERWEIGHT BABIES ONLY", "one corner of the picture", verdict "smoking looks protective" + "0.17× the death rate".
- **Bars:** width `min(74, PW/2 − 26)`; smokers `rgba(217,89,38,0.45)` stroked `P.orange`, non-smokers `rgba(107,114,128,0.40)` stroked `P.mute`; bold 13px value above each bar to one decimal; 12px `P.mute` "smokers" / "non-smokers" beneath the `#ccc` baseline.
- **Why strip** at `baseY+74`: bold 12px `P.violet` "WHY — among underweight babies, what made them small?"; two 13px-tall bars from x=130, width `w−320`, gray track `rgba(107,114,128,0.20)`, defect share filled `rgba(74,58,167,0.55)` stroked `P.violet`; right-aligned 12px `P.mute` row label; bold 12px `P.violet` "31% had a serious defect" after each bar.
- **Caption (bold 13px `P.magenta`, centered, `h−9`):** "A non-smoker's small baby has a graver cause."

---

## Section 4 — When Does the Trap Actually Spring?

**Tags:** `rule of thumb` (blue), `the boundary` (green), `common mistake` (red)

**Bullets:**
- **One bar, one skill — safe** — screen on coding alone, however harshly, and the link holds
- **A bar in each skill — also safe** — requiring a minimum in both invents no trade-off
- **Good at either one — the trap** — this is the rule that cuts the weak-at-both corner away
- **A total score does it too** — most real hiring rules add the scores up, so most are exposed
- **It can reverse a real answer** — skills that genuinely go together can read as a trade-off
- **Stricter stops mattering** — the distortion deepens, then flattens; being pickier adds little
- **The fix** — keep your rejections and measure the link across everyone who applied

**Example line (italic):** In the seeded pool, "pass if good at either" drags the apparent link down to about −0.5 while the honest answer is zero.

**Key point:** Ask what the entry rule was, and whether it judged both things at once. If the rejected records are gone, say the pattern holds only among those admitted.

### Visualization — canvas `c3`, 720×300

Apparent link versus how picky the filter is, one line per screening rule. All four series swept live from seeded pools.

- **Pools:** `N=4000`. Independent pool from `lcg(42)`. Positively-linked pool from `lcg(42)` with a shared component: `x = 0.35c + 0.65a`, `y = 0.35c + 0.65b`.
- **Sweeps:** each threshold keeps the passing subset (dropped if fewer than 40 remain) and plots `(share kept, fitted r)`.

  | Series | Rule | Pool | Style |
  |--------|------|------|-------|
  | fake trade-off | `x + y > t`, t = 0, .3, .6, .9, 1.2, 1.5 | independent | `P.magenta`, width 2.5 |
  | reads backwards | `x + y > t`, t = 0, .4, .6, .8, 1.0, 1.2 | positively linked | `P.orange`, width 2.5 |
  | coding bar only | `x > t`, t = 0, .3, .5, .7, .8 | independent | `P.green`, width 2.5 |
  | a bar in each | `x > t and y > t`, t = 0, .2, .3, .4 | independent | `P.aqua`, width 2, dash 5/4 |

- **Title (bold 15px `P.ink`, centered, y=22):** "Only a Rule Judging BOTH Skills Invents It"
- **Plot box:** `PX0=70`, `PX1=w−30`, `PY0=48`, `PY1=h−62`; y from `+0.4` down to `−0.6`; x is share kept, 100% at the left to 0% at the right so "pickier" runs rightward.
- **Grid:** `P.grid` lines every 0.2 with 12px `P.mute` right-aligned labels; a darker `#999` zero line labelled bold 12px "no real link — this is the truth".
- **Axes:** 12px `P.mute` x-ticks at 100/80/60/40/20/0%, title "share of applicants you keep  (pickier →)"; rotated y title "apparent link between the two skills".
- **Annotations:** a `P.orange` dot at the interpolated zero crossing of the orange series with "here the finding flips sign"; 12px `P.mute` "gets no worse past here" under the magenta series' computed minimum; bold 12px right-aligned "pass if good at either → fake trade-off" (`P.magenta`) and "real link → reads backwards" (`P.orange`); left-aligned "coding bar only → stays honest" (`P.green`) and "a bar in each → also honest" (`P.aqua`).
- **Caption (bold 13px `P.ink`, centered, `h−8`):** "Judging both skills at once invents the trade-off."

---

## Regeneration instructions

- **Template:** the card-section layout from `tutorials/llms-generative-ai/08-positional-encoding.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. Since the canvas is capped at 720px, a wide cell leaves slack — centering puts the chart in the middle of the right half instead of flush against the text.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>term</b>` → one italic `.example` line → one `.key-point` callout. No paragraph blocks, no `.philosophy` box, no data tables, no `.labels` strip, no `.subhead` line — the h2 and the tags carry that job.
- **Bullets:** 6–8 per section, each ONE line that does not wrap at 45% column width (~≤95 characters), opening with a `<b>bold term</b>` followed by an em dash and the fact.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.red` `rgba(231,76,60,0.12)`/`#e74c3c`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`. These fills are on pills, not behind body text.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart `height`. `setup(id)` caches the logical size in `dataset` on the first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. `max-width` is what keeps a chart inside its column; the logical coordinate system stays 720 wide at every window size, so no chart ever needs to re-lay-itself-out. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart section header bold 12–13px; body/axis labels 12px (floor); the single big callout figure bold 19px; caption bold 13px. Do not push titles to 17–19px — the chart reads as a poster.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Aim for hue variety within a chart; magenta rather than a hard red carries "the misleading view" so red stays for genuine alarm.
- **Determinism:** no `Math.random()` anywhere. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42. Correlations, slopes, rates, and ratios are computed in the draw function and printed from those variables so a label can never drift from the plotted data.
- **Chart order in the document:** `c1`, `c2`, `c4`, `c3` — `c4` is the health case (section 3), `c3` the boundary sweep (section 4).
