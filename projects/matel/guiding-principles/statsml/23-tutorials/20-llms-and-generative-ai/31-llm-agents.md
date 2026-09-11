# LLM Agents

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** LLM Agents

**Subtitle:** An LLM agent is a model running in a loop — think about the next step, act with a tool, look at what happened, repeat — and because every step must go right, a 95%-reliable step makes a 10-step errand succeed only 60% of the time

## An Errand Done in Little Loops

**Tags:** `core idea` (blue), `reason-act loop` (green), `tools` (orange)

- **The errand** — you ask an assistant app: "book a table for four this Friday around 8pm"
- **Too big for one answer** — no single reply can do it; it needs the calendar, a search, a booking site
- **Think** — the agent reasons about the next small step: "first check when Friday evening is free"
- **Act** — it takes one action with a tool: open the calendar and read Friday's schedule
- **Observe** — it reads the result ("free after 7pm") and feeds that into the next round of thinking
- **Stop condition** — the loop ends only when the agent judges the goal met and replies "booked, 8pm"

*Example (italic):* One full loop: think "I need Friday's free slots", act by calling the calendar tool, observe "free after 7pm" — then loop again with that fact in hand.

**Key point:** An LLM agent is not one big answer but many small loops — reason about the next step, take one tool action, read what happened, repeat until done.

### Visualization (canvas `c1`, 720×300)

Single-panel flow diagram: the three-box reason-act loop (Think → Act → Observe) with a curved return arrow, an exit to "Done → reply", and the booking errand's first loop written under each box.

- **Title (bold 15px, `#1a5276`, top center):** "The Reason-Act Loop: Book a Table for Four, One Step at a Time".
- **Boxes (150×54, 8px corner radius, 2px border, bold 14px centered label):** "THINK" at x=60 y=70 (border/text blue `#2a78d6`, fill `rgba(42,120,214,0.10)`); "ACT (use a tool)" at x=285 y=70 (orange `#d95926`, fill `rgba(217,89,38,0.10)`); "OBSERVE" at x=510 y=70 (green `#008300`, fill `rgba(0,131,0,0.10)`).
- **Forward arrows:** 3px `#6b7280` arrows with arrowheads from THINK to ACT (y=97) and ACT to OBSERVE (y=97).
- **Loop-back arrow:** 3px blue `#2a78d6` curved arrow from the bottom of OBSERVE (x=585, y=124) down through y=175 and back up to the bottom of THINK (x=135, y=124), arrowhead at the THINK end; bold 12px blue label centered at (360, 192): "not done yet — loop again".
- **Exit path:** 3px green arrow from the right edge of OBSERVE (x=660, y=97) to a green pill (110×36, radius 18) at x=595 y=210 labeled bold 12px white "Done → reply".
- **Concrete step captions (12px `#444`, one under each box at y=145):** under THINK: "\"check Friday first\""; under ACT: "opens the calendar"; under OBSERVE: "\"free after 7pm\"".
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=245):** "one loop = one small step of the errand".
- **Caption (11px `#444`, bottom right):** "illustrative — first loop of the table-booking errand".

## Ten Steps at 95% Each

**Tags:** `worked example` (blue), `compounding` (orange)

- **Count the steps** — the booking errand takes 10 small steps: calendar, search, menus, forms, confirm
- **Per-step odds** — suppose each single step goes right 95% of the time; that sounds like a strong score
- **Chain rule** — the whole errand succeeds only if EVERY step succeeds: 0.95 × 0.95 × ... ten times over
- **By hand** — 0.95 × 0.95 = 0.9025 after two steps; keep multiplying and step ten lands at 0.60
- **Longer is worse** — at 20 steps the same agent finishes cleanly only 36% of the time
- **The shape** — reliability compounds downward: each added step multiplies in another 0.95

*Example (italic):* Two steps: 0.95 × 0.95 = 0.9025, so about 90%; ten steps: 0.95^10 ≈ 0.60; twenty steps: 0.95^20 ≈ 0.36.

**Key point:** Whole-errand success = per-step reliability raised to the number of steps — 0.95^10 ≈ 0.60, so a "95% reliable" agent flips closer to a coin on long errands.

### Visualization (canvas `c2`, 720×300)

Single-panel decay curve: whole-errand success rate versus number of steps when each step is 95% reliable, with the 10-step point called out.

- **Title (bold 15px, `#1a5276`, top center):** "Every Step Is 95% Reliable — the Errand Is Not".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = number of steps 0 to 20 (x-pixel = 60 + steps × 30), 12px `#444` tick labels at 1, 5, 10, 15, 20; x-axis title 12px `#444` "steps in the errand"; y = whole-errand success 0 to 100% (y-pixel = 245 − pct × 1.9), 12px `#444` labels "0%", "25%", "50%", "75%", "100%" with light `#e5e9ef` gridlines at 25/50/75.
- **Curve:** blue `#2a78d6` 3px line through hardcoded points at steps `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20]`, success % = `[95, 90, 86, 81, 77, 74, 70, 66, 63, 60, 46, 36]`; fill under the curve `rgba(42,120,214,0.15)`; 5px blue dots at each point.
- **Callout guides:** dashed `#6b7280` (dash 4/3) vertical line at steps=10 up to the curve and horizontal line from the curve to the y axis at 60%; 7px orange `#d95926` dot on the curve at (10, 60%).
- **Endpoint labels (bold 12px):** blue "95%" above the first point; orange "60%" beside the 10-step dot; magenta `#d55181` "36%" above the 20-step point.
- **Annotation (bold 13px orange `#d95926`, near x=330, y=95):** two lines: "10 small steps →" / "only 60% of errands finish clean".
- **Caption (11px `#444`, bottom right):** "illustrative — steps assumed independent at 95% each".

## Why Long Errands Break in Production

**Tags:** `where it's used` (blue), `per-step reliability` (green), `design lever` (orange)

- **Where you meet it** — data agents that write a query, run it, fix errors, and chart results are 10+ step loops
- **The merciless exponent** — at 10 steps, per-step reliability of 90% / 95% / 99% yields 35% / 60% / 90% overall
- **Small gains, big payoff** — polishing one step from 95% to 99% buys 30 points on the whole errand
- **Fewer steps** — the cheapest fix is shortening the chain: one good tool call beats five clever ones
- **Checkpoints** — letting the agent verify and retry a failed step stops one slip from killing the run

*Example (italic):* 0.90^10 ≈ 0.35, 0.95^10 ≈ 0.60, 0.99^10 ≈ 0.90 — the same 10-step errand, wildly different finish rates.

**Key point:** Agent builders obsess over per-step reliability and step count because both sit in an exponent — nudging either one swings the whole errand.

### Visualization (canvas `c3`, 720×300)

Single-panel bar chart: three bars showing 10-step errand success at three per-step reliability levels, making the exponent's leverage visible.

- **Title (bold 15px, `#1a5276`, top center):** "Same 10-Step Errand, Three Per-Step Reliabilities".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = whole-errand success 0 to 100% (y-pixel = 245 − pct × 1.9), 12px `#444` labels "0%", "25%", "50%", "75%", "100%", light `#e5e9ef` gridlines at 25/50/75; no x gridlines.
- **Bars (width 120, centered at x = 190, 380, 570):** heights from `[35, 60, 90]` — bar 1 fill `rgba(217,89,38,0.35)` with 2px orange `#d95926` border, bar 2 fill `rgba(42,120,214,0.35)` with 2px blue `#2a78d6` border, bar 3 fill `rgba(0,131,0,0.35)` with 2px green `#008300` border.
- **Value labels (bold 14px above each bar, matching border color):** "35%", "60%", "90%".
- **X labels (12px `#444` below baseline, centered under each bar):** "90% per step", "95% per step", "99% per step".
- **Annotation (bold 13px green `#008300`, near x=430, y=70):** two lines: "95% → 99% per step" / "= +30 points on the whole errand".
- **Caption (11px `#444`, bottom right):** "illustrative — success = per-step reliability^10".

## One Bad Step Does Not Stay One Bad Step

**Tags:** `common mistake` (red), `error cascade` (orange)

- **The confusion** — people assume a wrong step costs just that step, and the rest still averages out
- **Errors cascade** — a wrong observation becomes the input to every later thought; the slip propagates
- **The booking slip** — step 3 picks "Luigi's Express" instead of "Luigi's"; steps 4–10 book the wrong place
- **Confidently wrong** — the later steps look flawless in the log; only the final result exposes step 3
- **The lesson** — verify early steps hardest: a check at step 3 is worth more than polish at step 9

*Example (italic):* The confirmation email reads perfectly — right day, right time, four seats — at a restaurant across town nobody asked for.

**Common mistake:** Treating agent errors like independent typos that wash out — in a loop, one wrong observation poisons every step built on top of it.

### Visualization (canvas `c4`, 720×300)

Single-panel step-chain timeline: ten step circles on a horizontal track, correct steps green, the step-3 slip red, and every later step orange to show the cascade.

- **Title (bold 15px, `#1a5276`, top center):** "A Slip at Step 3 Poisons Steps 4–10".
- **Track:** horizontal 2px `#999` line at y=150 from x=60 to x=660; ten 26px-diameter circles centered on it at x = `[80, 144, 208, 272, 336, 400, 464, 528, 592, 656]`, bold 12px white step numbers "1"–"10" inside.
- **Circle colors:** steps 1–2 green `#008300`; step 3 red `#e74c3c` with a bold 16px white "×" instead of its number; steps 4–10 orange `#d95926`.
- **Step captions (11px `#444`, alternating above y=118 and below y=185 to avoid overlap):** step 1 "read calendar", step 2 "search nearby", step 3 (bold 12px red `#e74c3c`) "wrong: Luigi's Express", step 5 "check table", step 7 "fill form", step 10 "confirm email".
- **Cascade bracket:** dashed orange `#d95926` (dash 4/3) bracket line from x=272 to x=656 at y=215, with a small down-tick at each end.
- **Annotation (bold 13px orange `#d95926`, centered at x=464, y=240):** "flawless-looking steps — all built on the step-3 mistake".
- **Legend (11px, top right at y=60):** green dot "correct", red dot "the slip", orange dot "confidently wrong".
- **Caption (11px `#444`, bottom right):** "illustrative — the table-booking errand".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all chart numbers are the hardcoded arrays above (no randomness); the decay-curve percentages are 0.95^steps rounded to whole percents, and the three-bar values are 0.90^10, 0.95^10, 0.99^10 rounded to 35/60/90; text numbers must stay in sync with chart numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
