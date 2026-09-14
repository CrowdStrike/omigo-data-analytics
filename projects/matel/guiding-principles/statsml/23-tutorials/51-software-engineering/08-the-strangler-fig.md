# The Strangler Fig

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Strangler Fig

**Subtitle:** Replace a legacy system by growing the new one around it, one capability at a time, until the old core serves nothing — no big-bang rewrite, no cutover night

## The Fig, the Host Tree, and a 12-Year-Old Monolith

**Tags:** `core idea` (blue), `incremental replacement` (green), `Martin Fowler` (orange)

- **The name** — Martin Fowler's strangler fig: the vine engulfs a host tree until the host dies inside
- **The host** — a bookstore's 12-year-old monolith serves 40,000 orders a day; nobody dares stop it
- **The facade** — a thin router goes in front first; on day one it forwards every request to legacy unchanged
- **One slice** — search moves first: the facade sends `/search` to new code, everything else still to legacy
- **The strangling** — slice by slice the new system takes over routes until legacy serves nothing and is switched off

*Example (italic):* Three months in, search runs on new code while the other five capabilities still run on legacy — customers can't tell the difference.

**Key point:** The strangler fig pattern replaces a legacy system incrementally: a routing facade splits traffic, new code takes one capability at a time, and the legacy core dies only when it serves nothing.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: traffic routing at month 0 (facade forwards everything to legacy) vs month 9 (facade splits traffic 70/30 between new and legacy).

- **Title (bold 15px, `#1a5276`, top center):** "A Routing Facade: All Traffic at First, Then One Slice at a Time".
- **Row 1 (y=95), label 12px `#444` at x=20:** "month 0"; ink `#1a5276` rounded box at x=150 labeled "routing facade" (12px), single 3px `#2a78d6` arrow to a blue box at x=430 labeled "legacy monolith — 100% of traffic".
- **Row 2 (y=205), label:** "month 9"; ink box "routing facade" at x=150; 3px green `#008300` arrow angled up to a green box at (x=430, y=178) labeled "new system — 70%" with a muted 12px "(search, catalog, cart)" line beneath it; 3px blue `#2a78d6` arrow angled down to a blue box at (x=430, y=236) labeled "legacy — 30%".
- **Box style:** 170–210px wide, 34px tall, 8px radius, fills `rgba(26,82,118,0.12)` / `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "same URLs for users — the split is invisible from outside".
- **Caption (12px `#444`, bottom right):** "traffic shares illustrative".

## Six Slices in Eighteen Months

**Tags:** `worked example` (blue), `routing table` (green)

- **The plan** — six capabilities ranked easiest-first: search, catalog, cart, checkout, invoicing, shipping
- **Traffic shares** — search 30%, catalog 25%, cart 15%, checkout 12%, invoicing 10%, shipping 8% of requests
- **The schedule** — one slice roughly every three months: months 3, 6, 9, 13, 16, 18
- **Hand-check** — after cart moves at month 9, new code serves 30 + 25 + 15 = 70% of traffic
- **The end** — at month 18 shipping moves, legacy serves 0%, and the old core is switched off

*Example (italic):* At month 13 checkout crosses over: 30 + 25 + 15 + 12 = 82% of requests hit new code, 18% still hit legacy.

**Key point:** The routing table is the migration plan — every row moved is a slice shipped to production, tested on real traffic, and reversible on its own.

### Visualization (canvas `c2`, 720×300)

Stacked step-area chart over 18 months: traffic served by the new system (green, growing in steps) vs legacy (blue, shrinking), total constant at 100%.

- **Title (bold 15px, `#1a5276`, top center):** "Eighteen Months of Strangling: Legacy Share Steps Down to Zero".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 18 with 12px `#444` tick labels every 3 months; y = % of traffic 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **New-system area (bottom):** green fill `rgba(0,131,0,0.30)` under a 2px `#008300` step line — flat then vertical riser at each migration month; months `[0, 3, 6, 9, 13, 16, 18]`, share `[0, 30, 55, 70, 82, 92, 100]`.
- **Legacy area (top):** blue fill `rgba(42,120,214,0.25)` between the step line and the constant total 100, 2px `#2a78d6` upper edge.
- **Riser labels (11px `#444`, beside each vertical step; the final label right-aligned left of its riser):** "search +30", "catalog +25", "cart +15", "checkout +12", "invoicing +10", "shipping +8".
- **Annotation (bold 13px green `#008300`, near month 12, y=80):** "month 18: legacy serves nothing — switch it off".
- **Caption (12px `#444`, bottom right):** "traffic shares illustrative".

## Why the Slow Way Beats the Big Rewrite

**Tags:** `where it's used` (blue), `risk` (orange)

- **Revenue never stops** — the store sells books every day of the 18 months; a big-bang rewrite ships nothing until cutover
- **Value per slice** — new search goes live at month 3 and gets 15 months of real use before legacy is even off
- **Rollback per slice** — a bad checkout release flips one route back to legacy; big-bang rollback is all-or-nothing
- **Edge cases surface early** — each slice teaches the domain's real rules gradually, not all at once on cutover night
- **The rewrite bet** — a big-bang rewrite stakes 18 months of work on one irreversible cutover going perfectly

*Example (italic):* When new invoicing mishandles VAT on gift cards, the fix is one route flipped back to legacy for two days — not a company-wide outage.

**Key point:** The strangler wins on risk arithmetic: many small reversible releases, each shipping value and derisking the next, against one giant release that must work the first time.

### Visualization (canvas `c3`, 720×300)

Line chart comparing % of traffic on new code over 18 months: big-bang rewrite (flat at 0, then a cliff to 100 at month 18) vs strangler (staircase).

- **Title (bold 15px, `#1a5276`, top center):** "Value Shipped: Big-Bang Waits 18 Months, the Strangler Ships from Month 3".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 18, 12px `#444` tick labels every 3 months; y = % of traffic on new code 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Big-bang line:** red `#e74c3c` 3px, flat at 0 from month 0 to month 18, then a vertical jump to 100 at month 18; 12px red label "cutover night — all or nothing" near the top of the jump.
- **Strangler line:** green `#008300` 3px step line through months `[0, 3, 6, 9, 13, 16, 18]`, values `[0, 30, 55, 70, 82, 92, 100]` (same schedule as c2).
- **Annotation (bold 13px green `#008300`, near month 6, y=105):** "each step is live, tested, and reversible".
- **Caption (12px `#444`, bottom right):** "% of traffic on new code, illustrative".

## The 80% Stall and the Shared Database

**Tags:** `common mistake` (red), `shared state` (orange)

- **The facade tax** — legacy was never designed for a router in front; building that seam is the first project
- **Shared state** — two systems writing one orders database is the boss fight: dual writes, sync jobs, drift checks
- **Data moves too** — migrating code without migrating its data leaves legacy alive as the system of record
- **Double ops** — the whole middle period runs BOTH systems: two deploys, two on-call rotations, two bills
- **The stall** — easy slices go first, so many strangler projects freeze near 80% when only the hardest remain

*Example (italic):* A different, cautionary team migrates 80% of traffic in 13 months, then sits at 81% for 17 more because the invoicing data model won't move.

**Common mistake:** Treating the strangler as done at 80%. Double running costs continue until legacy is fully off — plan the endgame (the ugliest slice and its data migration) first, not last.

### Visualization (canvas `c4`, 720×300)

Line chart of a stalled strangler project over 30 months: fast progress to 81%, then a long flat plateau while both systems keep running.

- **Title (bold 15px, `#1a5276`, top center):** "The Anti-Pattern: Stalled at 81% While Double-Ops Costs Keep Running".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 30, 12px `#444` tick labels every 6 months; y = % of traffic migrated 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Finish line:** horizontal dashed `#6b7280` (dash 4/3) line at y for 100, 12px `#6b7280` label "legacy off" at its left end.
- **Migration line:** orange `#d95926` 3px through months `[0, 3, 6, 9, 13, 18, 24, 30]`, values `[0, 20, 45, 65, 80, 81, 81, 81]`.
- **Stall marker:** vertical dashed `#6b7280` (dash 4/3) line at month 13, 12px `#6b7280` label "easy slices done" at its top.
- **Annotation (bold 13px red `#e74c3c`, near month 21, y=95):** "17 months at 81% — legacy still costs full ops".
- **Caption (12px `#444`, bottom right):** "a cautionary illustrative team — not the section-2 schedule".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); traffic shares, the six-slice schedule, and the stalled-team curve are invented and labeled illustrative; c2 and c3 must use the identical schedule arrays so text and both charts agree (30+25+15 = 70 at month 9, 82% at month 13).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
