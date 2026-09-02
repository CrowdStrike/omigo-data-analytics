# Ecological Fallacy: The Average Belongs to the Group, Not to Anyone In It

**Page type:** detail page — card-section template (see `tutorials/llms-generative-ai/08-positional-encoding.html`)
**HTML title tag:** Ecological Fallacy — Statistical Paradoxes

**Subtitle:** Six school districts that average more screen time also average better reading — yet inside every single one of them, the children who stare at screens the most read the worst

---

## Section 1 — Six Districts Say Screens Help. Every Child Says They Hurt.

**Tags:** `core idea` (blue), `two levels` (green), `group offset` (orange)

**Bullets:**
- **The district picture** — the six districts that average more screen time also average better reading
- **The obvious read** — screens must help, so hand every child a tablet and reading will follow
- **Inside a district** — the heaviest-screen quarter reads about 20 points below the lightest quarter
- **Same data, both ways** — 300 children, one dot each; only the level you read them at changes
- **What else differs** — better-off districts buy more devices and also fund more libraries and tutors
- **The offset** — each district sits at its own height, and the six heights climb with screen time
- **Why the line flips** — the climb between districts is steeper than the fall inside each one
- **Nobody moved** — the children are unchanged; the group average answers a different question

**Example line (italic):** In the lowest-screen district the lightest quarter of children score about 59 and the heaviest about 38 — yet that district's average sits at the bottom of the six.

**Key point:** The ecological fallacy is reading a group average as a statement about a person. Six district averages can slope one way while all 300 children slope the other, and both lines are correct about their own subject.

**Source note (`.src`):** Illustrative Example — a seeded construction in which screen time genuinely hurts every child and richer districts happen to own more devices.

### Visualization — canvas `c1`, 720×320

One scatter of 300 individual children with the six district averages drawn on top as large markers. Both fitted lines and both printed correlations are computed at render time from the plotted points — nothing about either slope is typed in.

- **Data:** seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42. Six districts, 50 children each. District *j* (0-indexed) has centre `a = 1.6 + 0.32j` hours and base score `b = 49 + 2.6j`. Each child: `x = a + (rng() − 0.5) × 3`, `y = b − 9(x − a) + (rng() − 0.5) × 4`. Screen time hurts by construction: the within-district coefficient is −9 everywhere.
- **Derived at render:** individual fit slope ≈ −3.97 points per hour, correlation ≈ −0.45; district-average fit slope ≈ +8.04, correlation ≈ +0.98. District means run from (1.66 h, 48.3) to (3.27 h, 61.3).
- **Title (bold 15px `P.ink`, centered, y=22):** "300 Children, Six District Averages, Opposite Answers"
- **Plot box:** `PX0=66`, `PX1=w−22`, `PY0=74`, `PY1=h−58`. x from 0 to 5 hours, y from 30 to 80 points.
- **Grid:** `P.grid` horizontal lines at 30/40/50/60/70/80 with 12px `P.mute` right-aligned labels; `#ccc` L-shaped axes.
- **Children:** radius 3 dots in `rgba(107,114,128,0.30)` — deliberately faint, since they are the layer everyone skips.
- **District markers:** radius 8 filled `rgba(42,120,214,0.75)` stroked `P.blue` width 1.5, each labelled with its letter A–F in bold 12px white centered inside.
- **Individual fitted line:** `P.magenta`, width 2.5, clipped to the plot box, drawn from the least-squares slope through the means of the 300 dots.
- **District fitted line:** `P.blue`, width 3, dashed 7/4, clipped to the plot box, fitted to the six markers only and drawn across 1.2 to 3.8 hours.
- **Line labels:** bold 12px `P.blue` "district averages: more screens, better reading" right-aligned in the empty top-right corner; bold 12px `P.magenta` "every child: more screens, worse reading" left-aligned in the empty bottom-left corner. Under each, a 12px `P.mute` parenthetical printing the computed correlation via `fmtR()` — the only place a statistic appears. Both corners were checked against the plotted dots and hold at most one.
- **Axes labels:** 12px `P.mute` x-ticks 0–5 at `PY1+16`, title "Daily screen time (hours) →" at `PY1+32`; rotated "Reading score →" left of the box.
- **Caption (bold 13px `P.magenta`, centered, `h−9`):** "Both lines are right. They are answering different questions."

---

## Section 2 — A Model Trained on Region Averages Learns the Wrong Sign

**Tags:** `where it bites` (blue), `feature grain` (green), `pipeline` (orange), `common mistake` (red)

**Bullets:**
- **The join** — tickets were only stored per region, so every customer got their region's average
- **What the model saw** — five distinct values of that feature spread across five hundred customers
- **What it learned** — regions that file more tickets keep customers longer, so tickets read as loyalty
- **What is actually true** — inside any region, each extra ticket costs a customer about four months
- **The reversal** — the learned coefficient points up while every region's own slope points down
- **The cost** — the hundred customers it calls most loyal retain below the company-wide average
- **Why it happens** — busy regions hold bigger accounts that both file more tickets and stay longer
- **The tell** — a feature with only as many distinct values as you have regions is a group fact

**Example line (italic):** The hundred customers this model ranks as most loyal go on to retain 22.3 months, against 24.1 for the customer base as a whole and 34.7 for the hundred who truly last longest.

**Key point:** Joining a feature at a coarser grain than your prediction target imports the between-group pattern and buries the within-group one. Check how many distinct values a feature really has before trusting its coefficient.

**Source note (`.src`):** Illustrative Example — a seeded construction where extra tickets always shorten a customer's life and larger regions simply start higher.

### Visualization — canvas `c2`, 720×330

Five per-region clouds each with its own downward fitted segment, and one upward line fitted to the five region centroids. Every slope drawn and printed is computed from the plotted points.

- **Data:** LCG seed 42. Five regions, 100 customers each. Region *j*: mean tickets `T = 3 + j`, mean retention `R = 18 + 3j`. Each customer: `t = T + (rng() − 0.5) × 5.2`, `r = R − 4(t − T) + (rng() − 0.5) × 3`. Ranges stay positive: tickets 0.40–9.46, retention 6.8–40.1 months.
- **Derived at render:** each region's own slope ≈ −4 months per ticket (−4.03, −3.93, −4.04, −3.93, −3.97); the five-centroid slope ≈ +3.51 with correlation ≈ +0.99.
- **Title (bold 15px `P.ink`, centered, y=22):** "Five Regions Point Up. All Five Insides Point Down."
- **Plot box:** `PX0=66`, `PX1=w−22`, `PY0=96`, `PY1=h−62`. x from 0 to 10 tickets, y from 5 to 42 months.
- **Grid:** `P.grid` lines every 10 months with 12px `P.mute` labels; `#ccc` L-shaped axes.
- **Region clouds:** radius 2.5 dots, region *j* in `rgba(...,0.30)` cycling `P.blue`, `P.aqua`, `P.violet`, `P.yellow`, `P.green` — one hue per region so the offsets read as offsets.
- **Within-region segments:** each region's own least-squares line drawn only across that region's observed ticket range, width 2, in the region's hue at full opacity.
- **Centroid markers:** radius 7 filled `rgba(213,81,129,0.85)` stroked `P.magenta`, one per region.
- **Centroid line:** `P.magenta`, width 3, dashed 7/4, fitted to the five centroids only, drawn across their x-range.
- **Annotations sit ABOVE the box, not in it** — 500 dots leave no clear corner. Bold 12px `P.magenta` "what the model learned from region averages" left-aligned at `PY0−34` with a 12px `P.mute` "(+3.5 months per extra ticket)" under it printed from the centroid fit; bold 12px `P.text` "what happens to an actual customer" right-aligned with "(−4.0 months per extra ticket)" printed from the mean of the five computed within-region slopes.
- **Axes labels:** 12px `P.mute` x-ticks 0/2/4/6/8/10 at `PY1+16`, title "Support tickets filed →" at `PY1+32`; rotated "Months retained →" left of the box.
- **Caption (bold 13px `P.magenta`, centered, `h−9`):** "The coefficient is real. It just describes regions, not customers."

---

## Section 3 — A Real Case: The 1950 Paper That Named the Mistake

**Tags:** `real case` (blue), `1950 census study` (green), `named the fallacy` (red)

**Bullets:**
- **The 1950 comparison** — Robinson set state-level figures beside the individual records behind them
- **By state** — states with more foreign-born residents read better, and the pattern was a strong one
- **By person** — a foreign-born adult was less likely to be literate than a native-born one
- **The gap** — about 94 of every 100 native-born adults could read, against about 86 of the foreign-born
- **The reason** — immigrants settled in industrial states whose literacy already ran above average
- **What the state figure measures** — where immigrants chose to live, not whether they could read
- **The name** — this is the paper that gave the ecological fallacy the name it still carries
- **The general lesson** — a state is not a large person, and its patterns belong to states alone

**Example line (italic):** The eight states with the fewest immigrants averaged 92.7 out of 100 adults literate; the eight with the most averaged 94.2 — while within every one of those states the foreign-born read less.

**Key point:** Robinson (1950) showed that a state-level pattern can carry the opposite sign to the individual-level one it is built from, and that no amount of state data settles a question about people. The literacy reversal is the case that gave the fallacy its name.

**Source note (`.src`):** Illustrative Example — the direction and rough strength follow Robinson (1950); the 48 plotted states are a seeded reconstruction, not the original census table.

### Visualization — canvas `c3`, 720×330

A state scatter with its upward fitted line on the left, and the two person-level literacy rates as bars on the right. Both the state correlation and the person-level association are computed in the draw function.

- **Construction:** LCG seed 42, 48 states. Share foreign-born `f = 0.005 + 0.275 × rng()`. Native literacy `Ln = 0.915 + 0.20f + (rng() − 0.5) × 0.044`. Foreign-born literacy `Lf = Ln − 0.09` — every state penalises the foreign-born, with no exceptions. State literacy `L = (1 − f)Ln + f·Lf`.
- **Derived at render:** state-level correlation ≈ +0.53; person-level association ≈ −0.11, computed as the phi coefficient of the pooled two-by-two table. National rates: native-born 94%, foreign-born 86%.
- **Title (bold 15px `P.ink`, centered, y=22):** "By State They Rise Together. By Person They Do Not."
- **Left panel** (`PX0=60` to `0.56w`, `PY0=84`, `PY1=h−64`): scatter of the 48 states, radius 4 dots `rgba(42,120,214,0.55)`; x = share foreign-born 0% to 30%, y = state literacy 88% to 98%; `P.grid` lines every 2 points with 12px `P.mute` labels; `#ccc` L-shaped axes.
  - Fitted line `P.blue` width 2.5 clipped to the panel, computed from the 48 plotted states.
  - Bold 12px `P.blue` header "MORE IMMIGRANTS, MORE READING" at `PY0−30`, with a 12px `P.mute` "(state-level correlation +0.53)" under it, printed from `fit()`.
  - 12px `P.mute` x-ticks 0/10/20/30% at `PY1+16`, title "Share foreign-born →" at `PY1+32`, rotated "Adults literate (%) →" left of the panel.
- **Right panel** (from `0.63w` to `w−26`): two horizontal bars on a common 0–100 scale, 20px tall, 34px apart, each on a `rgba(107,114,128,0.12)` track.
  - "native-born" `rgba(25,158,112,0.55)` stroked `P.aqua`; "foreign-born" `rgba(213,81,129,0.50)` stroked `P.magenta`. Both lengths computed from the construction, each labelled bold 13px with its own computed percentage to the whole number.
  - Bold 12px `P.magenta` header "A PERSON BORN ABROAD READ LESS" above the bars, with 12px `P.mute` "(person-level correlation −0.11)" under it, printed from the computed phi.
  - Two 12px `P.mute` note pairs under the bars, spaced 15px in pixels: "immigrants settled in states" / "that already read above average", then "bar length = adults per 100 who" / "could read, counted nationally".
- **Caption (bold 13px `P.magenta`, centered, `h−9`):** "The state figure records where immigrants moved, not how they read."

---

## Section 4 — So When Is a Group Average the Right Answer?

**Tags:** `rule of thumb` (blue), `the boundary` (green), `atomistic fallacy` (orange)

**Bullets:**
- **A group answer to a group question** — budgets are set per district, so district averages fit
- **The fallacy is in the reading** — that same average turns wrong the moment it describes one child
- **State the grain out loud** — say districts that watch more read better, never children who do
- **When it is safe** — if groups differ only by chance, averaging costs precision but not the sign
- **What causes it** — each group sitting at its own height, so between-group spread tells its own story
- **The fix** — get one row per person, or keep the conclusion at the grain your rows actually have
- **The reverse error** — individual rows license no claim about groups; that is the atomistic fallacy
- **A cheap check** — refit at two grains, and if the sign moves the coarse answer is about groups only

**Example line (italic):** Averaging the same children in bundles of three already flattens the per-child drop of about 4 points an hour to nothing; by bundles of fifty it has become a rise of 8.

**Key point:** Averaging is only safe when the groups differ by chance alone. Once each group sits at its own height, coarser units first weaken the individual answer, then reverse it — so fit at the grain you intend to speak about.

**Source note (`.src`):** Illustrative Example — the three sweeps reuse the seeded district construction from the first section, changing only how much the districts differ.

### Visualization — canvas `c4`, 720×310

The fitted slope of the same 300 children as they are progressively averaged into coarser units, one line per scenario. Every point is a live refit; nothing is a stored number.

- **Sweep:** for unit sizes 1, 2, 3, 5, 10, 25, 50 children, consecutive children inside a district are averaged into units, then a single least-squares slope is fitted to all the units and plotted.
- **Series:**

  | Series | Districts differ by | Style | Computed slopes (1 → 50) |
  |--------|--------------------|-------|--------------------------|
  | districts differ in screens and money | `da=0.32`, `db=2.6` | `P.magenta`, width 2.5 | −3.97 → +8.04, crossing zero near 3 |
  | districts differ in screens only | `da=0.32`, `db=0` | `P.orange`, width 2.5 | −6.35 → −0.00 |
  | districts differ by chance alone | `da=0`, `db=0` | `P.green`, width 2.5 | −9.01 → −11.84, flat |

- **Title (bold 15px `P.ink`, centered, y=22):** "How Coarse Can You Average Before the Sign Flips?"
- **Plot box:** `PX0=72`, `PX1=w−26`, `PY0=52`, `PY1=h−58`. x is unit size on a log scale from 1 to 50; y from +10 down to −13 points per hour.
- **Grid:** `P.grid` lines every 5 units of slope with 12px `P.mute` right-aligned labels; a darker `#999` zero line labelled bold 12px `P.mute` "no apparent effect at all".
- **Truth note:** a 12px `P.green` line reading "the truth for a child: −9.0 points per hour", positioned at the flat green series and printed from that series' own first computed slope — not from the construction constant.
- **Sign-flip marker:** a `P.magenta` dot at the interpolated zero crossing of the magenta series (near 3 children per unit), with 12px "the sign flips here" beside it — the crossing x is interpolated in log space from the two computed slopes that straddle zero, never typed.
- **Series labels:** bold 12px right-aligned 9px above each series' last point; the green series dips at the final unit size, so its label anchors to the second-to-last point instead. Colors `P.magenta`, `P.orange`, `P.green`.
- **Series dots:** radius 3 in the series color at every swept unit size, so the reader sees the seven refits.
- **Axes:** 12px `P.mute` x-ticks at 1, 2, 3, 5, 10, 25, 50 with title "children averaged into one row  (coarser →)"; rotated "fitted points per hour of screen time".
- **Caption (bold 13px `P.ink`, centered, `h−8`):** "Safe only when the groups differ by chance and nothing else."

---

## Regeneration instructions

- **Template:** the card-section layout from `tutorials/llms-generative-ai/08-positional-encoding.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. Since the canvas is capped at 720px, a wide cell leaves slack — centering puts the chart in the middle of the right half instead of flush against the text.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>term</b>` → one italic `.example` line → one `.key-point` callout → an optional `.src` note. No paragraph blocks, no `.philosophy` box, no data tables, no `.labels` strip, no `.subhead` line — the h2 and the tags carry that job.
- **Bullets:** 6–8 per section, each ONE line that does not wrap at 50% column width (~≤100 characters), opening with a `<b>bold term</b>` followed by an em dash and the fact. No fragments.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.red` `rgba(231,76,60,0.12)`/`#e74c3c`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`. These fills are on pills, not behind body text.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart `height`. `setup(id)` caches the logical size in `dataset` on the first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart section header bold 12–13px; body/axis labels 12px (floor); the single big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Magenta rather than a hard red carries "the misleading view" so red stays for genuine alarm.
- **Shared helpers:** `lcg(seed)` (Park–Miller, `s = (s × 16807) % 2147483647`), `fit(pts)` returning `{r, slope, mx, my}`, and `fmtR(r)` for signed two-decimal correlation labels. No `Math.random()` anywhere on the page.
- **Determinism and computed labels:** every slope, correlation, rate, and month figure printed on a canvas is computed inside its draw function from the points actually plotted. A hardcoded correlation beside a drawn line is a defect.
- **Chart order in the document:** `c1`, `c2`, `c3`, `c4` — matching sections 1 through 4.
