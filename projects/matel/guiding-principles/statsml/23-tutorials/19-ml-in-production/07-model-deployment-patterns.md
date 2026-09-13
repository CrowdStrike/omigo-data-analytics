# Model Deployment Patterns

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Model Deployment Patterns

**Subtitle:** Shadow, canary, and blue-green are three ways to put a new model in front of real traffic — first nobody sees it, then a small slice does, and there is always a one-switch way back

## Three Ways to Try a New Delivery-Time Model

**Tags:** `core idea` (blue), `safe rollout` (green), `traffic routing` (orange)

- **The app** — a pizza app tells every customer "arrives in 25 minutes", computed by its old model v1
- **The new model** — the team builds v2; it looks better on old data, but live dinner rushes are the real test
- **Shadow** — v2 sees every order and predicts silently into a log; customers still see only v1's answer
- **Canary** — v2 answers for a small slice, say 100 of 1,000 orders, while v1 keeps serving the rest
- **Blue-green** — two full copies run side by side; one switch moves all traffic, one switch moves it back

*Example (italic):* Like a restaurant trying a new recipe — cook it alongside the old one (shadow), serve it to a few tables (canary), and keep the old dish ready to bring back instantly (blue-green).

**Key point:** The three patterns are one idea at three risk levels: nobody sees v2, a few see it, everyone sees it — with an instant way back.

### Visualization (canvas `c1`, 720×300)

Three-panel schematic, one panel per pattern, each showing 1,000 orders flowing from a "customers" box through routing to model boxes, with the share of traffic written on each arrow.

- **Title (bold 15px, `#1a5276`, top center):** "Shadow, Canary, Blue-Green: Who Actually Sees v2".
- **Panels:** three columns of width 220 starting at x=20, x=250, x=480; each has a bold 13px `#1a5276` panel label at its top (y=50): "SHADOW", "CANARY 10%", "BLUE-GREEN".
- **Common boxes:** in every panel a rounded rect "1,000 orders" (fill `rgba(26,82,118,0.12)`, 12px `#1a5276` text) at the top (y≈70), and model boxes near the bottom (y≈190): v1 box stroked blue `#2a78d6`, v2 box stroked green `#008300`, 12px labels "v1" / "v2".
- **Shadow panel:** solid blue 3px arrow from orders to v1 labeled "100%" (12px `#2a78d6`); dashed (dash 5/4) green 2px arrow from orders to v2 labeled "copy" (12px `#008300`); small mute `#6b7280` rect "log only" under v2 with a thin arrow into it.
- **Canary panel:** solid blue 3px arrow to v1 labeled "90%"; solid green 2px arrow to v2 labeled "10%" (both 12px, colored to match).
- **Blue-green panel:** a small switch dot at mid-height; solid green 3px arrow through the switch to v2 labeled "100%"; dashed blue 2px arrow to v1 labeled "0% (kept warm)"; curved mute 11px label "flip back anytime" beside the switch.
- **Annotation (bold 12px orange `#d95926`, centered at y=280):** "risk grows left to right — so teams test in this order".

## A Shadow Week, Then a 10% Canary

**Tags:** `worked example` (blue), `shadow mode` (green)

- **Shadow week** — for 7 days v2 predicts all 1,000 daily orders, but only into a log nobody sees
- **Scoring** — each night, both predictions are compared to the true arrival time of every order
- **The result** — v1 misses by about 8 minutes on average, v2 by about 5: a 3-minute win for v2
- **Canary day** — day 8: v2 answers 100 of the 1,000 orders; complaints run 3 of 100 vs 27 of 900
- **The ramp** — errors lower and complaints even at 3% vs 3%, so the slice grows: 10%, 50%, then all

*Example (italic):* On day 3 of the shadow week v1 missed by 8.4 minutes on average and v2 by 5.3 — both checked against the same 1,000 real deliveries.

**Key point:** v2 earned each step with numbers: about 3 minutes more accurate in shadow, and a complaint rate no worse (3% vs 3%) in the 10% canary.

### Visualization (canvas `c2`, 720×300)

Two-line chart of the shadow week: average prediction miss per day for v1 and v2 over days 1–7, both scored on the same live orders while customers saw only v1.

- **Title (bold 15px, `#1a5276`, top center):** "Shadow Week: Same 1,000 Orders a Day, Two Predictions Scored".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = day 1 to 7, 12px `#444` tick labels "day 1"…"day 7"; y = average miss in minutes 0 to 10, 12px `#444` labels at 0, 2, 4, 6, 8, 10 with light `#e5e9ef` gridlines.
- **v1 line:** blue `#2a78d6` 3px line with 5px dots through daily averages `[8.2, 7.9, 8.4, 8.1, 8.6, 9.0, 8.3]`; bold 12px blue label "v1 — customers see this" above the line near day 2.
- **v2 line:** green `#008300` 3px line with 5px dots through `[5.2, 4.9, 5.3, 5.0, 5.4, 5.1, 4.8]`; bold 12px green label "v2 — log only" below the line near day 2.
- **Gap marker:** vertical dashed `#6b7280` (dash 4/3) segment at day 5 between the two lines (from y of 8.6 down to y of 5.4), 11px `#6b7280` label "≈3 min" beside it.
- **Annotation (bold 13px green `#008300`, near day 5 at y=70):** "v2 misses by ~3 minutes less — and no customer has seen it yet".
- **Caption (12px `#444`, bottom right):** "illustrative — avg |predicted − actual| minutes per day".

## Why Not Just Flip Everyone to v2

**Tags:** `why it matters` (blue), `blast radius` (green), `rollback` (orange)

- **Offline lies** — a model that shone on last month's data can stumble on live traffic it never saw
- **Blast radius** — a broken v2 on day one hits 1,000 orders all-at-once, but only 100 at a 10% canary
- **Shadow costs zero** — a broken v2 in shadow hurts nobody; the log quietly absorbs the mistake
- **Blue-green's gift** — the old copy stays warm, so undo is a 1-minute switch, not a 45-minute redeploy
- **Layered** — real teams chain them: shadow first, then a canary ramp, then blue-green for the final flip

*Example (italic):* One evening a v2 bug doubled every predicted time; at 10% only 100 customers saw "50 minutes" before the slice was cut back to zero.

**Key point:** Deployment patterns don't make v2 better — they cap how many orders a bad day can touch and how long the bad day lasts.

### Visualization (canvas `c3`, 720×300)

Two side-by-side bar panels: left panel shows how many of the day's 1,000 orders a broken v2 reaches under each pattern; right panel shows how long undoing the mistake takes with and without a warm old copy.

- **Title (bold 15px, `#1a5276`, top center):** "A Bad Model's Day One: How Many Orders, How Long to Undo".
- **Left panel (x=50 to x=380):** 13px bold `#1a5276` panel label "orders exposed (of 1,000)" at top; baseline y=245, value axis 0 to 1,000 with light `#e5e9ef` gridlines at 250, 500, 750 and 11px `#6b7280` labels; three vertical bars ~70px wide with 12px `#444` name labels below and bold 13px value labels on top:
  - "shadow" = 0 (draw a 2px green `#008300` tick at the baseline, value label "0")
  - "canary 10%" = 100, fill `rgba(0,131,0,0.35)`, green `#008300` 2px outline
  - "all-at-once" = 1,000, fill `rgba(231,76,60,0.25)`, red `#e74c3c` 2px outline
- **Right panel (x=430 to x=690):** 13px bold `#1a5276` panel label "minutes to switch back" at top; baseline y=245, value axis 0 to 50, gridlines at 15, 30, 45 (11px `#6b7280` labels); two bars:
  - "blue-green flip" = 1, fill `rgba(42,120,214,0.35)`, blue `#2a78d6` outline, bold value label "1"
  - "full redeploy" = 45, fill `rgba(231,76,60,0.25)`, red `#e74c3c` outline, bold value label "45"
- **Annotation (bold 12px orange `#d95926`, over the left panel, two lines centered at x=150, y=70/86):** "canary caps the damage" / "at 100 of 1,000 orders".
- **Caption (12px `#444`, bottom right):** "illustrative — one bad-release scenario".

## The Confusion: Shadow Proved It, Why Canary Too?

**Tags:** `common mistake` (red), `what each test sees` (orange)

- **The trap** — "shadow week showed v2 wins by 3 minutes, ship it everywhere" skips a real question
- **Shadow's blind spot** — in shadow nobody acts on v2's numbers, so it can't show customer reactions
- **Feedback loops** — a shown "20 minutes" changes behavior: more peak orders, hurried drivers
- **Canary's job** — the canary is the first time the world responds to v2; the 3% complaint check tested that
- **Not an A/B test** — a canary asks "is v2 safe to ramp?"; an A/B test asks "is v2 better?" over a longer run

*Example (italic):* v2's shorter promises made canary customers order more at peak hour — a load effect no shadow log could ever reveal.

**Common mistake:** Treating a good shadow result as full proof. Shadow checks the predictions themselves; only a canary checks what happens when real people act on them.

### Visualization (canvas `c4`, 720×300)

Three-by-three comparison grid drawn on canvas: one row per question, one column per pattern, each cell holding a short colored verdict, making shadow's blind spot to real-world reactions visible.

- **Title (bold 15px, `#1a5276`, top center):** "What Each Pattern Can and Cannot See".
- **Grid geometry:** row-label column x=20 to x=230; pattern columns of width 155 at x=240, x=400, x=560; header row at y=70, data rows centered at y=115, y=165, y=215; thin `#e5e9ef` 1px lines between rows and columns.
- **Column headers (bold 13px):** "shadow" in blue `#2a78d6`, "canary 10%" in green `#008300`, "blue-green 100%" in violet `#4a3aa7`.
- **Row labels (12px `#444`, left-aligned):** "Who sees v2's predictions?", "Catches bad predictions early?", "Shows customer reactions?".
- **Cells (bold 12px, centered):** row 1: "nobody" (`#6b7280`), "10% of orders" (`#008300`), "everyone" (`#d95926`); row 2: "yes — silently" (`#008300`), "yes — on 100 orders" (`#008300`), "only after everyone" (`#d95926`); row 3: "no" (`#e74c3c`, on `rgba(231,76,60,0.08)` cell fill), "yes" (`#008300`), "yes" (`#008300`).
- **Annotation (bold 13px red `#e74c3c`, centered at y=265):** "shadow cannot see reactions — that is why the canary comes next".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, line points, and cell verdicts are the hardcoded literal values above (no randomness); the daily-miss arrays in `c2` are the source of the "8 vs 5 minutes" claims in the text, and the 0 / 100 / 1,000 and 1 / 45 values in `c3` match the bullets exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
