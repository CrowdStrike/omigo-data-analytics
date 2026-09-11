# Simpson's Paradox

**Page type:** detail page (tutorial page: h2 card-sections, each a two-column layout table — text left 50%, canvas right 50%; one section uses a 3-column 38/31/31 layout with two canvases)
**HTML title tag:** Simpson's Paradox

**Subtitle:** A treatment can win inside every single group yet lose in the combined total — because the two sides faced different mixes of easy and hard cases

## One Hospital, Two Treatments, a Flip

**Tags:** `core idea` (blue), `running example` (green)

- **The trial** — 200 patients receive treatment A or B, and we count who recovers
- **Young patients** — B recovers 95% (19 of 20) vs A's 90% (72 of 80): B wins
- **Older patients** — B recovers 60% (48 of 80) vs A's 50% (10 of 20): B wins again
- **The flip** — pooled together, A recovers 82 of 100 and B only 67 of 100: A "wins"
- **The name** — a trend that reverses when groups are combined is Simpson's paradox

*Example:* B beats A inside every age group, yet loses by 15 points when the two groups are added up.

**Key point:** A comparison can flip its sign purely because the two sides face different mixes of easy and hard cases.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: recovery % for A vs B in three groups (young, older, overall).

- **Title (bold 15px, `#1a5276`, top center):** "Recovery Rate: B Wins Both Groups, A Wins the Total"
- **Axes:** L-shaped axis in `#999`; y baseline at 250, chart height 180px mapping 0–100%; y labels "0%" and "100%" in muted gray `#6b7280`, x-axis from x=55 to x=690.
- **Data (three group centers, paired bars 56px wide, 6px from center):**
  - young (cx=165): A=90%, B=95%
  - older (cx=375): A=50%, B=60%
  - overall (cx=585): A=82%, B=67%
- **Colors:** A bars blue `#2a78d6`, B bars aqua `#199e70`.
- **Labels:** bold 13px value labels ("90%" etc.) above each bar in `#2c3e50`; "A"/"B" in muted gray under bars; bold group name ("young"/"older"/"overall") below at baseline+33.
- **Annotation:** red dashed box (`#e74c3c`, dash 6/4, width 2) around the overall pair, spanning both bars plus margin, from y(90)−12 down past the baseline; bold red label "the flip" centered above the box.

## Add Up the Table Yourself

**Tags:** `worked example` (green)

- **A's mix** — 80 of A's 100 patients were young, the easy cases
- **B's mix** — 80 of B's 100 patients were older, the hard cases
- **A's total** — 72 young + 10 older recoveries = 82 of 100
- **B's total** — 19 young + 48 older recoveries = 67 of 100
- **The lever** — each total is a weighted average, and the weights differ wildly

*Example:* Give B the easy mix instead and its total jumps to 88 of 100 — nothing about B changed.

**Key point:** The blend of cases, not the treatment, decides the pooled winner here.

### Visualization (canvas `c2a`, 420×340)

Stacked bar chart: patient mix per treatment.

- **Title (bold 15px, `#1a5276`, top center):** "Who Got Which Patients"
- **Axes:** L-shaped axis in `#999`; baseline y=280, chart height 210px mapping 0–100; y labels "0" and "100" in muted gray.
- **Bars (80px wide):** treatment A at x=110: 80 young (green `#008300`, bottom) + 20 older (orange `#d95926`, top); treatment B at x=250: 20 young + 80 older.
- **Labels:** white bold 13px inside segments: "80 young" / "20 older" / "20 young" / "80 older"; bold names "treatment A" / "treatment B" below bars.
- **Annotation (bottom center, bold 12px, orange `#d95926`):** "B got the hard cases"

### Visualization (canvas `c2b`, 420×340)

Stacked bar chart: recoveries out of 100.

- **Title (bold 15px, `#1a5276`, top center):** "Recoveries out of 100"
- **Axes:** same layout as c2a (baseline y=280, height 210px, 0–100 scale, "0"/"100" labels).
- **Bars (80px wide):** treatment A at x=110: 72 young (green `#008300`) + 10 older (orange `#d95926`), total label "82 total"; treatment B at x=250: 19 young + 48 older, total label "67 total". Segment counts in white bold 13px inside; bold total labels above each stack; bold treatment names below.
- **Legend (left, 12px muted gray):** "green = young, orange = older"
- **Annotation (bold 12px red `#e74c3c`, centered at x=250 near top):** "82 vs 67: the mix decides"

## The Sicker Patients Went to B

**Tags:** `core idea` (blue), `where it's used` (blue)

- **The lurker** — age drives both which treatment doctors picked and the odds of recovery
- **Doctors chose** — B was newer, so the tougher older cases were steered to it
- **Fair contest** — compare at the same mix: at 50/50, A averages 70% and B averages 77.5%
- **B really wins** — once the mix is equalized, the per-group advantage carries through
- **Same trap** — school ratings, airline delays, and hiring rates flip the same way

*Example:* An airline can beat a rival at every airport yet look worse overall by flying into stormier hubs.

**Key point:** When one side systematically gets the harder cases, the pooled comparison answers the wrong question.

### Visualization (canvas `c3`, 720×300)

Split panel: confounder diagram (left) plus standardized bar comparison (right), separated by a vertical dashed divider (`#bdc3c7`, dash 4/3) at x=320.

- **Title (bold 15px, `#1a5276`, top center):** "Equalize the Mix: Recovery Rate at a 50/50 Age Blend"
- **Left — confounder diagram:** three rounded boxes (fill `#f8f9fa`, 2px colored borders, bold 12px `#1a5276` labels):
  - "AGE (severity)" at (90,60) 130×36, orange `#d95926` border
  - "treatment picked" at (30,175) 120×36, blue `#2a78d6` border
  - "recovery odds" at (180,175) 120×36, aqua `#199e70` border
  - Two orange arrows with filled arrowheads from the AGE box down to each lower box.
  - Bold orange caption: "age pushes on both arms"; muted 12px caption below: "a classic confounder".
- **Right — standardized bars:** L-shaped axis (x=385 vertical, baseline y=245, height 165px, 0–100% scale, "0%"/"100%" labels).
  - Bar A at x=425, 80px wide, blue `#2a78d6`, value 70%, sub-label "(90 + 50) / 2"
  - Bar B at x=560, 80px wide, aqua `#199e70`, value 77.5%, sub-label "(95 + 60) / 2"
  - Bold 14px value labels ("70%" / "77.5%") above bars; bold names "A"/"B" below; muted calc strings under names.
- **Annotation (bold 13px green `#008300`, centered at x=537, y=52):** "mix equalized: B wins by 7.5 points"

## So Which Number Is Right?

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Depends on the question** — picking a treatment for a patient needs the within-group view
- **Split view** — for any given young or old patient, B has the better odds: choose B
- **Pooled view** — describes last year's caseload history, not which treatment to pick
- **Not always split** — if the grouping happens after treatment (a side effect), splitting misleads
- **Ask first** — decide which variables to hold fixed before reading the totals

*Example:* "B minus A" reads +5 for young, +10 for old, and −15 pooled — three true numbers, one right question.

**Key point:** Simpson's paradox is not the data lying; it is two different questions getting two different answers.

### Visualization (canvas `c4`, 720×300)

Diverging bar chart: "B minus A" in percentage points for the three views.

- **Title (bold 15px, `#1a5276`, top center):** '"B minus A" in Points: Three True Numbers'
- **Axes:** vertical axis at x=70 in `#999`; horizontal zero line at y=150 (muted gray, 1.5px) from x=70 to x=690; scale 90px per 15 points; y labels "0", "+10", "−15" in muted gray.
- **Bars (90px wide):**
  - x=140: +5, aqua `#199e70`, label "young: B by +5"
  - x=340: +10, aqua `#199e70`, label "older: B by +10"
  - x=540: −15, red `#e74c3c`, label "pooled: A by 15"
  - Bold 14px signed value labels in bar color at the bar tip; bold 12px name labels on the opposite side of the zero line.
- **Annotations:** bold 13px red centered at (400,275): "the sign flips only because the mixes differ"; bold 12px violet `#4a3aa7` at (380,48): "choosing for a patient? read the group bars, not the pooled one"

## Regeneration instructions

- **Template:** tutorial detail page (tutorials style, see `tutorials/CLAUDE.md` and reference `most-powerful-signals/07-social-graph-connections.html`). h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `.text-col` (50%) and `.viz-col` (50%); the "Add Up the Table Yourself" section uses `table.layout.layout3` with `.text-col3` (38%) and two `.viz-col3` (31%) cells holding canvases c2a and c2b (420×340 each).
- **Text cell structure:** `.tags` row of colored pills first, then `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, `<strong>` prefix).
- **Tag pill styles:** 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; bullets 0.92rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Project palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic sizes per chart (720×300 or 420×340); sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) via a shared `setup(id, W, H)` helper that multiplies width/height and calls `ctx.scale` so drawing stays in logical coordinates. All data is hardcoded literal arrays — no `Math.random()`.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
