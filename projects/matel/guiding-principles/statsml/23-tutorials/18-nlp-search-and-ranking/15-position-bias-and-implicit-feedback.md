# Position Bias & Implicit Feedback

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Position Bias & Implicit Feedback

**Subtitle:** A click says two things at once — the result was seen and it was liked — and since the ranking decides who sees what, raw clicks measure placement as much as preference

## The Top Slot Wins by Default

**Tags:** `core idea` (blue), `position bias` (green), `implicit feedback` (orange)

- **The search** — you type "pizza" into a delivery app and five pizzerias come back in a fixed order
- **The eyes** — nearly everyone looks at slot 1; barely 1 in 8 people ever scrolls down to slot 5
- **The click** — a click means two things happened: the pizzeria was seen AND it looked good
- **Implicit feedback** — nobody rated anything; the app just watches clicks and treats them as votes
- **Position bias** — slot 1 collects extra votes simply for being first, whatever happens to sit there

*Example (italic):* The same pizzeria earns 20 clicks per 100 searches in slot 1 but only 7 in slot 3 — nothing about the pizza changed, only the slot.

**Key point:** A click is a vote cast only by the people who looked — and the slot decides who looks.

### Visualization (canvas `c1`, 720×300)

Single-panel vertical bar chart: how many of 100 searchers ever examine each of the five slots, falling off steeply from slot 1 to slot 5.

- **Title (bold 15px, `#1a5276`, top center):** "Who Even Looks? Examines per 100 Searches, by Slot".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = examines per 100 searches, 0 to 100, light `#e5e9ef` gridlines at 20, 40, 60, 80 with 12px `#444` labels; x = five bars labeled "slot 1" … "slot 5" (12px `#444`, below baseline).
- **Bars:** width 70px, centered at x = 130, 240, 350, 460, 570; heights from `[100, 60, 35, 20, 12]`; fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; bold 13px `#2a78d6` value label above each bar ("100", "60", "35", "20", "12").
- **Annotation (bold 13px orange `#d95926`, near x=430, y=100):** two lines: "slot 1 gets 8× the eyes" / "that slot 5 gets".
- **Caption (12px `#444`, bottom right):** "illustrative — examine rates per 100 searches".

## Two Pizzerias, 100 Searches, One Swap

**Tags:** `worked example` (blue), `the swap test` (green)

- **Two pizzerias** — Aldo's is liked by 20% of the people who see it, Bella's by 40%
- **Examine rates** — out of 100 searches, slot 1 is examined 100 times, slot 3 only 35 times
- **Aldo's in slot 1** — 100 examines × 20% liked = 20 clicks
- **Bella's in slot 3** — 35 examines × 40% liked = 14 clicks
- **The verdict** — raw clicks crown Aldo's (20 vs 14) even though Bella's is liked twice as much
- **The swap test** — trade the slots and Bella's jumps to 40 clicks while Aldo's falls to 7

*Example (italic):* Clicks = examines × liked rate: 100 × 0.20 = 20 and 35 × 0.40 = 14 — two multiplications you can redo on paper.

**Key point:** Bella's is twice as liked yet loses on raw clicks 14 to 20 — the slot, not the pizza, decided the scoreboard.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart with two groups on a shared clicks axis: clicks per 100 searches before the swap (Aldo's in slot 1, Bella's in slot 3) and after (slots traded), showing the clicks moving with the slot.

- **Title (bold 15px, `#1a5276`, top center):** "The Swap Test: Same Pizzerias, Traded Slots".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = clicks per 100 searches, 0 to 45, light `#e5e9ef` gridlines at 10, 20, 30, 40 with 12px `#444` labels.
- **Group 1 (centered x=210), bold 12px `#444` label "before swap" below the baseline:** two 60px bars 20px apart — Aldo's blue `#2a78d6` at height 20, Bella's green `#008300` at height 14; bold 12px matching-color labels above: "Aldo's @1: 20" and "Bella's @3: 14".
- **Group 2 (centered x=490), label "after swap":** Bella's green bar at height 40, Aldo's blue bar at height 7; bold 12px labels "Bella's @1: 40" and "Aldo's @3: 7".
- **Annotation (bold 13px magenta `#d55181`, centered near x=360, y=45):** "the clicks moved with the slot, not the pizza".
- **Caption (12px `#444`, bottom right):** "illustrative — clicks = examines × liked rate".

## Why Raw Clicks Poison the Ranker

**Tags:** `where it's used` (blue), `feedback loop` (red), `the fix` (green)

- **Training on clicks** — search and feed rankers learn from click logs because explicit ratings are rare
- **The loop** — whatever ranks first earns the most clicks, which teaches the model to keep it first
- **Rich get richer** — a mediocre pizzeria placed first can lock in the top slot for months
- **The fix** — divide each click by its slot's examine rate before counting (propensity weighting)
- **Debiased votes** — Aldo's 20 ÷ 1.00 = 20, Bella's 14 ÷ 0.35 = 40; the true order comes back

*Example (italic):* A ranker retrained weekly on raw clicks re-crowns Aldo's every week — its own top slot keeps manufacturing the "proof."

**Key point:** Weight each click by 1 ÷ examine rate and Bella's true two-to-one lead (40 vs 20) reappears from the very same log.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart with two groups on a shared score axis: the raw click scoreboard (Aldo's ahead) versus the same clicks divided by each slot's examine rate (Bella's ahead two to one).

- **Title (bold 15px, `#1a5276`, top center):** "Same Log, Two Scoreboards: Raw Clicks vs Clicks ÷ Examine Rate".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = score, 0 to 45, light `#e5e9ef` gridlines at 10, 20, 30, 40 with 12px `#444` labels.
- **Group 1 (centered x=210), bold 12px `#444` label "raw clicks" below the baseline:** Aldo's blue `#2a78d6` 60px bar at height 20, Bella's green `#008300` bar at height 14; bold 12px matching-color labels above: "Aldo's: 20" and "Bella's: 14".
- **Group 2 (centered x=490), label "clicks ÷ examine rate":** Aldo's blue bar at height 20 labeled "20 ÷ 1.00 = 20", Bella's green bar at height 40 labeled "14 ÷ 0.35 = 40" (bold 12px, matching colors).
- **Annotation (bold 13px green `#008300`, near x=340, y=45):** "debiased, Bella's true 2-to-1 lead returns".
- **Caption (12px `#444`, bottom right):** "illustrative — same 100-search log as the swap test".

## No Click Doesn't Mean No

**Tags:** `common mistake` (red), `unseen vs rejected` (orange)

- **The reading** — analysts read zero clicks as "users rejected it", but most non-clickers never looked
- **Three outcomes** — every search ends unseen, seen-but-passed, or clicked; the log shows only clicks
- **Slot 5 math** — 100 searches, 12 examines, 40% liked: 5 clicks, 7 passes, 88 people who never saw it
- **Skips vs snubs** — only the 7 who examined and passed said "no"; the other 88 said nothing at all
- **Better signals** — scroll depth, hovers, and time-on-result help separate unseen from rejected

*Example (italic):* Bella's parked in slot 5 collects 5 clicks per 100 searches — a 5% "rating" for a pizzeria that 40% of viewers actually like.

**Common mistake:** Counting every no-click as a rejection. Below the fold, "no click" mostly means "never seen" — at slot 5, 88 of the 95 no-clicks never saw the result.

### Visualization (canvas `c4`, 720×300)

Three stacked vertical bars, each splitting the same 100 searches for a 40%-liked pizzeria parked at slot 1, 3, or 5 into clicked, examined-but-passed, and never-examined, showing "never seen" swallow the column down the page.

- **Title (bold 15px, `#1a5276`, top center):** "100 Searches of a 40%-Liked Pizzeria at Slot 1, 3, and 5".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = searches, 0 to 100, light `#e5e9ef` gridlines at 25, 50, 75 with 12px `#444` labels; x = three bars labeled "slot 1", "slot 3", "slot 5" (12px `#444`, below baseline).
- **Bars:** width 90px, centered at x = 170, 370, 570; each stacks to 100; segments bottom-to-top: clicked green `#008300`, examined-but-passed orange `rgba(217,89,38,0.55)`, never examined grey `rgba(107,114,128,0.30)`.
- **Segment values:** slot 1 = `[40, 60, 0]`, slot 3 = `[14, 21, 65]`, slot 5 = `[5, 7, 88]`; bold 12px in-segment count labels (white on green, `#444` on the lighter fills), omitted where the segment is 0.
- **Legend (12px `#444`, top right inside the plot):** three swatches — "clicked" (green), "examined, passed" (orange), "never examined" (grey).
- **Annotation (bold 13px orange `#d95926`, near x=460, y=95, pointing at the slot-5 bar):** two lines: "88 of the 95 no-clicks" / "never saw it".
- **Caption (12px `#444`, bottom right):** "illustrative — same examine rates as the first chart".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights and segment values are the hardcoded arrays above (no randomness); the examine rates `[100, 60, 35, 20, 12]`, the click counts 20/14/40/7, and the slot-5 split 5/7/88 must stay identical between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
