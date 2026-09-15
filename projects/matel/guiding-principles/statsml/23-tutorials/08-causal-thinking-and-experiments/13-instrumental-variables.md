# Instrumental Variables

**Page type:** detail page (tutorial layout: h1 + subtitle, then `.card-section` blocks each with an h2 and a `table.layout` — text column 50% left with tag pills / bullets / example / key-point, viz column 50% right with a canvas; one section holds both canvases side by side in a `.viz-pair` flex row)
**HTML title tag:** Instrumental Variables

**Subtitle:** When the treatment is tangled up with hidden traits, find a random nudge that moves the treatment — and nothing else — then measure through the nudge.

## Exposed users buy more — but ads chase likely buyers

Tags: `core idea` (blue), `tangled treatment` (orange)

- **The question** — does seeing the ad cause people to purchase?
- **The raw gap** — users who saw the ad buy at 9.4%; users who did not buy at 2.4%
- **The tangle** — the ad system targets heavy shoppers, who would buy more anyway
- **Hidden driver** — shopping intent raises both ad exposure and purchases
- **Stuck** — you cannot adjust for intent because it was never measured

*Example:* The 7-point gap mixes "ads work" with "ads find people who were about to buy" — in unknown shares.

**Key point:** Exposure was not handed out at random, so exposed vs unexposed compares different kinds of people, not different treatments.

### Visualization (canvas `c1`, 720×300)

Split panel: DAG of the hidden confounder on the left, biased comparison bars on the right, separated by a vertical dashed light-gray divider (`#bdc3c7`, dash 4/3) at x=400 from y=40 to bottom.

- **Title (bold 16px, `#1a5276`, centered):** "Hidden Shopping Intent Feeds Both Sides".
- **Left panel — DAG:**
  - Top node at (200, 85): dashed-border box (`#4a3aa7` violet, dash 5/4, 164×38, white fill) labeled "shopping intent" in bold violet 13px, with "(never measured)" in violet 12px above it.
  - Bottom-left node at (105, 205): solid box 120×38, blue `#2a78d6` border, label "ad exposure".
  - Bottom-right node at (305, 205): solid box 110×38, green `#008300` border, label "purchase".
  - Two solid violet arrows from the intent node down to each bottom node; one dashed gray (`#6b7280`) arrow from "ad exposure" to "purchase" labeled "effect = ?" in bold gray 12px above the arrow midpoint.
- **Right panel — bars ("purchase rate by exposure", bold 13px `#1a5276` title at y=58):**
  - Two bars, 85px wide, gap 60, baseline y=225 (thin `#999` line), max scale 12% over 145px height, alpha 0.7 fills.
  - "saw ad": 9.4%, blue `#2a78d6`; "did not": 2.4%, gray `#6b7280`. Value labels "9.4%" / "2.4%" bold above bars, category labels below.
  - Red annotation (`#e74c3c`, bold 13px, two lines under the baseline): "+7 pts — but different people," / "not just different ads".

## The nudge: a random ad slot with only one way to matter

Tags: `the trick` (green), `intuition` (blue)

- **The accident** — on about half of days the ad slot is unavailable (sold off, at random)
- **Rule 1: it nudges** — slot available → exposure jumps from 20% to 60% of users
- **Rule 2: no side door** — slot availability cannot touch purchases except via the ad
- **The instrument** — that is all an instrument is: a random push with one path to the outcome
- **The payoff** — comparing slot-days to no-slot-days is fair; nobody chose their day

*Example:* Users on slot days and no-slot days are the same mix of people — only their chance of seeing the ad differs.

**Key point:** Any purchase difference between slot days and no-slot days can only have traveled through the extra ad exposure — that is what makes the nudge usable.

### Visualization (canvas `c2`, 720×300)

Path diagram of the instrument chain.

- **Title (bold 16px, `#1a5276`, centered):** "The Nudge Has Exactly One Path to Purchases".
- **Nodes (white boxes, 2px colored border, bold 13px colored label):** "slot available?" at (130, 130), 150×40, aqua `#199e70`; "ad exposure" at (370, 130), 138×40, blue `#2a78d6`; "purchase" at (600, 130), 118×40, green `#008300`. Text label "shopping intent (hidden)" in bold violet `#4a3aa7` 12px at (485, 60).
- **Arrows (solid, 2.5px, filled arrowheads):** aqua from slot to exposure; blue from exposure to purchase; two violet arrows from the hidden intent label down to "ad exposure" and "purchase".
- **Annotation under the slot→exposure arrow (bold aqua 12px):** "random: 20% → 60% exposed".
- **Forbidden path:** dashed red (`#e74c3c`, dash 7/5, 2px) quadratic curve from below the slot node arcing under to below the purchase node, crossed out by a red X (3.5px strokes) at its midpoint (~y=236); bold red 13px label below the X: "no direct path allowed: the slot cannot change purchases on its own".
- **Bottom line (bold green 13px, centered):** "intent never touches the slot — the nudge is clean of the confounder".

## Dividing two differences: 1.6 ÷ 40 = 4 points

Tags: `worked example` (green), `by hand` (blue)

- **Slot days** — exposure 60%, purchase rate 6.0%
- **No-slot days** — exposure 20%, purchase rate 4.4%
- **Nudge on exposure** — 60 − 20 = 40 points
- **Nudge on purchases** — 6.0 − 4.4 = 1.6 points
- **Per exposure** — 1.6 ÷ 40 = 0.04 → ads add ~4 points

*Example:* The nudge moved 40 extra exposures per 100 users and 1.6 extra purchases — so each 100 exposures bought ~4 purchases.

**Key point:** The division just rescales the fair day-level comparison into a per-exposure effect: purchase difference ÷ exposure difference.

(This section keeps the 50/50 layout; its two canvases sit side by side in a `.viz-pair` flex row inside the single viz cell.)

### Visualization (canvas `c3a`, 310×320)

Two-bar chart, first stage.

- **Title (bold 15px, `#1a5276`, centered):** "Step 1: Nudge → Exposure".
- **Bars (82px wide, gap 59, centered; scale max 70%, alpha 0.7):** "slot days" 60% in aqua `#199e70`; "no-slot days" 20% in gray `#6b7280`. Bold 14px value labels "60%" / "20%" above bars, 12px category labels below the baseline (thin `#999` line).
- **Difference annotation (bold orange `#d95926` 14px, centered near top):** "difference: 40 pts", with a 2px orange vertical connector line between the two bar tops.
- **Footer (gray `#6b7280` 12px, centered):** "share of users who see the ad".

### Visualization (canvas `c3b`, 310×320)

Two-bar chart, second stage.

- **Title (bold 15px, `#1a5276`, centered):** "Step 2: Nudge → Purchases".
- **Bars (82px wide, gap 59, centered; scale max 7%, alpha 0.7):** "slot days" 6.0% in green `#008300`; "no-slot days" 4.4% in gray `#6b7280`. Bold 14px value labels "6.0%" / "4.4%" above bars, 12px category labels below the baseline.
- **Difference annotation (bold orange `#d95926` 14px, centered near top):** "difference: 1.6 pts".
- **Result line (bold magenta `#d55181` 13px, centered near bottom):** "effect per exposure = 1.6 ÷ 40 = 4 pts".
- **Footer (gray 12px, centered):** "share of users who purchase".

## Why it matters: the naive answer would double your ad ROI

Tags: `where it's used` (blue), `common mistake` (red)

- **Two answers** — naive exposed-vs-unexposed: +7 points; instrument-based: +4 points
- **The gap** — 3 points of the naive number is targeting, not persuasion
- **The stakes** — budget justified at 7 points loses money if the truth is 4
- **Classic instruments** — lottery draws, distance to a store, policy cutoffs, random outages
- **The catch** — a nudge with a side door (slot days = weekends?) quietly breaks the whole trick

*Example:* If slots vanish mostly on weekends, "no side door" fails — weekends move purchases on their own.

**Key point:** The instrument's honesty cannot be proven from the data alone — "random nudge, one path" is a claim about the world that you must argue for.

### Visualization (canvas `c4`, 720×300)

Two-bar comparison of the naive vs IV estimate.

- **Title (bold 16px, `#1a5276`, centered):** "What the Ad Adds per Exposed User: Two Answers".
- **Bars (170px wide, gap 150, centered; scale max 8 points, alpha 0.65, baseline thin `#999` line):**
  - "naive: exposed vs unexposed": +7 pts, magenta `#d55181`, sub-label "9.4% − 2.4%".
  - "instrument: through the random slot": +4 pts, green `#008300`, sub-label "1.6 ÷ 40".
  - Value labels "+7 pts" / "+4 pts" bold 15px in bar color above bars; bold 12px category labels and 12px gray sub-labels below the baseline.
- **Bracket:** violet `#4a3aa7` 2px vertical line to the right of the naive bar spanning from the 7-pt height down to the 4-pt height, with bold violet 13px two-line label: "3 pts = targeting," / "not persuasion".
- **Bottom line (bold red `#e74c3c` 13px, centered):** "an ad budget justified at +7 loses money if the causal truth is +4".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` (border-collapse, full width). All rows: `td.text-col` 50% / `td.viz-col` 50%; the worked-example row places canvases `c3a`/`c3b` (310×320 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Text column structure:** `.tags` row of pill spans (0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`), then a `<ul>` (0.92rem) of one-line bullets each starting with `<b>` in `#1a5276`, an italic `.example` paragraph (`#555`, 0.9rem), and a `.key-point` callout (background `#f8f9fa`, 3px solid `#e74c3c` left border, padding 8px 12px, 0.9rem, `<strong>` lead-in).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases have `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Page palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `arrow(...)` draws lines with filled triangular arrowheads (optionally dashed), `nodeBox(...)` draws white boxes with 2px colored borders and bold 13px centered labels. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No cross-page links; in regenerated HTML any card links elsewhere would use `.html` extensions.
