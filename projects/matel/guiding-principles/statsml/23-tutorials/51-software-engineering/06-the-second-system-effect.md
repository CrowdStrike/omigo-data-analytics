# The Second-System Effect

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Second-System Effect

**Subtitle:** v2 is the most dangerous version an architect ever designs — the first system was built humble and constrained; the second gets loaded with every idea deferred along the way (Fred Brooks, *The Mythical Man-Month*)

## Everything Deferred Comes Flooding Back

**Tags:** `core idea` (blue), `Fred Brooks` (orange), `over-engineering` (red)

- **The first system** — a three-person team ships a billing service: 12 features, built lean because nobody knew if it would work
- **The scars** — v1 shipped with a wishlist: 9 ideas deferred, each tagged "we'll fix that someday"
- **The second system** — v2 kicks off and every deferred idea, pet feature, and speculative hook gets loaded in
- **The count** — the v2 plan balloons to 31 features: 12 parity, 9 old wishlist, 6 pet features, 4 "future-proofing"
- **Brooks's warning** — in *The Mythical Man-Month*, Fred Brooks named this the second-system effect: v2 tries to please everyone

*Example (italic):* The v1 architect who once cut scope to survive now approves a plugin system, a rules engine, and multi-currency — for a product with one currency.

**Key point:** The second-system effect (Brooks, *The Mythical Man-Month*): an architect's second system is the most dangerous one they will ever design, because restraint learned under v1's constraints gets replaced by every ambition v1 deferred.

### Visualization (canvas `c1`, 720×300)

Two stacked vertical bars comparing v1 shipped scope against the v2 plan, with the v2 bar segmented by where each feature came from.

- **Title (bold 15px, `#1a5276`, top center):** "v1 Shipped 12 Features — the v2 Plan Carries 31".
- **Axes:** origin x=90, baseline y=250, plot height 190; y = feature count 0 to 35, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` tick labels.
- **v1 bar (centered x=230, width 110):** single blue `#2a78d6` segment of height for 12 features, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border; bold 13px `#2a78d6` label "12 shipped" above the bar; 12px `#444` label "v1 (built humble)" below the baseline.
- **v2 bar (centered x=470, width 110), stacked bottom to top with hardcoded segments `[12, 9, 6, 4]`:**
  - "parity with v1" — 12, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border
  - "v1 wishlist" — 9, fill `rgba(217,89,38,0.25)`, 2px `#d95926` border
  - "pet features" — 6, fill `rgba(213,81,129,0.25)`, 2px `#d55181` border
  - "future-proofing" — 4, fill `rgba(74,58,167,0.22)`, 2px `#4a3aa7` border
  - each segment gets a 12px right-side label at x=540 in its border color ("+9 wishlist", "+6 pet features", "+4 future-proofing"); 12px `#444` label "v2 plan (loaded)" below the baseline.
- **Annotation (bold 13px red `#e74c3c`, upper left near x=110, y=55):** "19 of 31 features never existed in v1".
- **Caption (12px `#444`, bottom right):** "feature counts illustrative".

## The Rewrite, Month by Month

**Tags:** `worked example` (blue), `scope creep` (green), `the classic arc` (orange)

- **The pitch** — month 0: "a 6-month cleanup rewrite, feature parity with v1's 12 features, nothing more"
- **The accretion** — every quarter adds roughly 3 "while we're at it" features; the plan grows 12 → 29 by month 18
- **The build** — the team ships about 3 features a quarter: 17 of 29 done at month 18
- **Hand-check** — month 18 is 3× the 6-month estimate, and the gap to the plan is still 12 features
- **The kicker** — the old v1 system keeps running the business the entire time, at full maintenance cost

*Example (italic):* At month 6 — the original ship date — the plan has grown to 19 features and only 5 are built; the finish line moved faster than the team.

**Key point:** The classic failure arc: a rewrite that starts as a cleanup accretes every wishlist item, takes 3× the estimate, and sometimes never ships — while the old system it was meant to replace keeps doing the actual work.

### Visualization (canvas `c2`, 720×300)

Two-line chart over 18 months: planned scope climbing as features accrete vs features actually built, with the original estimate marked — the lines never meet.

- **Title (bold 15px, `#1a5276`, top center):** "The Finish Line Moves Faster Than the Team".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 18, 12px `#444` tick labels every 3 months ("0"–"18"); y = feature count 0 to 30, gridlines `#e5e9ef` at 10/20 with tick labels.
- **Planned-scope line:** red `#e74c3c` 3px line through months `[0, 3, 6, 9, 12, 15, 18]`, features `[12, 15, 19, 23, 26, 28, 29]` — steady "while we're at it" growth; bold 12px red label "features in the plan" near month 8 above the line.
- **Built line:** blue `#2a78d6` 3px line through the same months, features `[0, 2, 5, 8, 11, 14, 17]`; bold 12px blue label "features built" near month 13 below the line.
- **Estimate marker:** vertical dashed `#6b7280` (dash 4/3) line at month 6, 12px `#6b7280` label "original 6-month estimate" at its top.
- **Gap marker:** thin vertical `#d95926` double-headed arrow at month 18 between built (17) and planned (29), bold 12px orange `#d95926` label "still 12 short at 3× the estimate".
- **Annotation (bold 13px green `#008300`, bottom area near month 3, y=225):** "old v1 system still runs the business".
- **Caption (12px `#444`, bottom right):** "counts illustrative; parity was 12".

## What the Old Ugly Code Knows

**Tags:** `why it matters` (blue), `Joel Spolsky` (orange), `hidden knowledge` (green)

- **The essay** — Joel Spolsky's "Things You Should Never Do" argues the full rewrite is the single worst strategic mistake
- **The reason** — ugly old code is ugly because it is correct: each weird `if` is a production incident someone already paid for
- **The count** — five years of v1 accumulated 83 edge-case fixes: leap years, retries, a partner's malformed invoices
- **The reset** — a from-scratch rewrite starts at zero fixes and must rediscover each one in production
- **The bill** — in its first year the rewrite logs 41 incidents; 32 are regressions the old code already handled

*Example (italic):* The rewrite team deletes a "pointless" special case for negative invoice totals — and re-learns in month 2 why refunds made it exist.

**Key point:** Spolsky's trap: a full rewrite throws away years of accumulated bug fixes and edge-case knowledge encoded in the old code — knowledge nobody wrote down anywhere else.

### Visualization (canvas `c3`, 720×300)

Left panel: cumulative edge-case fixes in the old system over five years. Right panel: first-year production incidents, old system vs rewrite, with the regression share highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "The Rewrite Throws Away 83 Lessons, Then Re-Learns 32 of Them".
- **Left panel (x=60 to x=360):** line chart, baseline y=245, plot height 175; x = years 1 to 5 (12px `#444` ticks), y = cumulative fixes 0 to 90, gridlines at 30/60; green `#008300` 3px line through years `[1, 2, 3, 4, 5]`, fixes `[14, 31, 52, 68, 83]`, area fill `rgba(0,131,0,0.15)`; bold 12px green label "83 edge-case fixes encoded in v1" near the line's end.
- **Right panel (x=420 to x=690):** two vertical bars on the same baseline y=245, y = incidents 0 to 50; bar width 80:
  - "old system" bar at x=460: blue fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, height for 9 incidents, 12px `#2a78d6` value label "9" above
  - "rewrite, year 1" bar at x=590: red fill `rgba(231,76,60,0.20)`, 2px `#e74c3c` border, height for 41; inner darker segment `rgba(231,76,60,0.45)` for the 32 regressions with bold 11px red label "32 were already fixed in v1" right-aligned at the canvas edge beside it; value label "41" above
- **Panel labels (12px `#444`, below baseline):** "fixes accumulated in old code" (left), "first-year incidents" (right).
- **Annotation (bold 13px violet `#4a3aa7`, top left near x=70, y=60):** "the ugly code was the documentation".
- **Caption (12px `#444`, bottom right):** "all counts illustrative".

## Restraint Is the Architect's Second-System Skill

**Tags:** `common mistake` (red), `rule of thumb` (green), `incremental replacement` (blue)

- **The mistake** — treating "big-bang rewrite" as the default fix for old code, and scope as a free add-on
- **Incremental wins** — replace one module at a time behind the same interface; old and new run side by side
- **Parity first** — ruthless rule: no new feature enters the plan until the replacement matches v1's 12
- **Restraint** — Brooks's cure: the architect's core second-system skill is saying no to their own wishlist
- **When it IS justified** — a truly dead-ended platform and a full cost tally, v1 upkeep included

*Example (italic):* The team reroutes 5% of invoices through a new billing module, compares outputs against v1 for a month, then ratchets up — v1 retires piece by piece, never in one night.

**Common mistake:** Believing the big bang is faster because "we finally do it right." The disciplines that survive contact are incremental replacement over big-bang rewrite, feature-parity-first scope, and treating restraint — not ambition — as the mark of the senior architect.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the big-bang rewrite path stalling before cutover vs the incremental path retiring the old system module by module.

- **Title (bold 15px, `#1a5276`, top center):** "Big Bang vs Incremental Replacement".
- **Row 1 (boxes centered on y=95), label 12px `#444` at x=20:** "big bang"; blue `#2a78d6` rounded box at x=150 labeled "freeze v1" (12px), 3px arrow to an orange `#d95926` box at x=330 labeled "build v2: 18 mo, scope ×2.4", 3px arrow to a red `#e74c3c` box at x=545 labeled "one-night cutover" with bold 12px red "✗ rollback, v1 lives on" beneath it.
- **Row 2 (boxes centered on y=205), label:** "incremental"; blue box at x=150 labeled "route 5% to new module", 3px arrow to a green `#008300` box at x=330 labeled "match v1 output, ratchet up", 3px arrow to a green box at x=545 labeled "v1 retired module by module" with bold 12px green "✓ shipping the whole time".
- **Box style:** 150–175px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, 2px borders in the named colors.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "parity first; the wishlist waits until the old system is gone".
- **Caption (12px `#444`, bottom right):** "scope multiplier illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); feature counts (12/31 and the 12→29 scope line vs 0→17 built line), fix counts (83 cumulative) and incident counts (9 vs 41, 32 regressions) are invented and labeled illustrative. Credits — Fred Brooks, *The Mythical Man-Month* (second-system effect) and Joel Spolsky, "Things You Should Never Do" (rewrite trap) — must appear in the body text as above.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
