# Hot Code Swapping

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Hot Code Swapping

**Subtitle:** Erlang lets you replace a function inside a running program without stopping it — like changing a tire while the car is still driving

## The Phone Switch That Never Hangs Up

**Tags:** `core idea` (blue), `zero downtime` (green), `Erlang` (orange)

- **The switch** — a small-town phone exchange routes 900 live calls at 2pm on a Tuesday
- **The bug** — call billing rounds seconds up twice; the fix is a one-line change
- **The old way** — stop the switch, install, restart: every live call drops for the update
- **The swap** — Erlang loads the new billing function beside the old one while calls keep running
- **The handoff** — each call picks up the new code the next time it calls the function

*Example (italic):* The fix goes live at 2:03pm; all 900 calls stay connected, and callers never learn an upgrade happened.

**Key point:** Hot code swapping replaces a module in a running system — the process keeps its state and simply starts executing the new version at the next call.

### Visualization (canvas `c1`, 720×300)

Timeline chart comparing live calls during an upgrade: restart deployment (calls drop to 0) vs hot swap (calls stay level), on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "Upgrade at 2:03pm: Restart Drops Every Call, Hot Swap Drops None".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "2:00" to "2:10" with 12px `#444` tick labels every 2 minutes; y = live calls 0 to 1000, gridlines `#e5e9ef` at 250/500/750.
- **Restart line:** red `#e74c3c` 3px line through points at minutes `[0, 2, 3, 3.2, 4, 5, 6, 8, 10]`, calls `[900, 905, 903, 0, 120, 340, 560, 810, 895]` — vertical cliff to 0 at 2:03, slow recovery as callers redial.
- **Hot swap line:** green `#008300` 3px line through the same minute grid, calls `[900, 905, 903, 902, 908, 901, 897, 904, 900]` — flat.
- **Swap marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 3, 12px `#6b7280` label "new code loaded" at its top.
- **Annotation (bold 13px green `#008300`, near minute 6, y=90):** "900 calls never notice the upgrade".
- **Caption (12px `#444`, bottom right):** "call counts illustrative".

## Two Versions in Memory at Once

**Tags:** `worked example` (blue), `old and new code` (green)

- **Two copies** — after the load, the VM holds billing v1 and billing v2 side by side
- **The rule** — a fully-qualified call (`billing:rate(...)`) always jumps to the newest version
- **The count** — of 900 live calls, a call re-rates every 60s, so within a minute all 900 have crossed over
- **Hand-check** — at 15s after the swap roughly 225 calls (a quarter of 900) have re-rated onto v2
- **The purge** — once no process runs v1, the old code is deleted; only then is the swap complete

*Example (italic):* 15 seconds after the 2:03pm swap, ~225 calls bill on v2 and ~675 still run v1 — by 2:04pm all 900 are on v2.

**Key point:** The VM keeps old and new code simultaneously; each process migrates at its next qualified call, so the crossover is gradual and no state is lost.

### Visualization (canvas `c2`, 720×300)

Stacked area chart of the 60 seconds after the swap: calls on v1 (blue, shrinking) vs calls on v2 (green, growing), total constant at 900.

- **Title (bold 15px, `#1a5276`, top center):** "The 60-Second Crossover: 900 Calls Migrate One Call at a Time".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds after swap 0 to 60, tick labels "0s"–"60s" every 15s (12px `#444`); y = calls 0 to 900, gridlines at 225/450/675.
- **v2 area (bottom):** green fill `rgba(0,131,0,0.30)` under a 2px `#008300` line through seconds `[0, 15, 30, 45, 60]`, calls `[0, 225, 450, 675, 900]`.
- **v1 area (top):** blue fill `rgba(42,120,214,0.25)` between the v2 line and the constant total 900, 2px `#2a78d6` upper edge.
- **Labels:** bold 12px blue "still on v1" at (x≈12s, y≈100 in the upper band); bold 12px green "migrated to v2" at (x≈45s, y≈200 in the lower band).
- **Annotation (bold 12px violet `#4a3aa7`, near x=30s, y=60):** "total never dips below 900 — no dropped calls".
- **Caption (12px `#444`, bottom right):** "linear migration, illustrative — each call re-rates once per minute".

## Where Always-On Systems Need It

**Tags:** `where it's used` (blue), `uptime` (green)

- **Telecom** — Erlang was built at Ericsson for switches with nine-nines uptime targets
- **Messaging** — chat backends carry millions of open connections that must survive deploys
- **The math** — 99.9999999% uptime allows ~0.03 seconds of downtime per year; a restart costs minutes
- **The trade** — most languages restart instead because two-versions-at-once is hard to reason about
- **Modern echo** — rolling deploys and blue-green releases imitate the idea at the server level

*Example (italic):* A restart deploy that takes 3 minutes uses up ~5,700 years' worth of a nine-nines downtime budget in one shot.

**Key point:** Hot swapping exists for systems where the cost of stopping — dropped calls, lost connections — is worse than the complexity of running two code versions briefly.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: downtime cost of one 3-minute restart deploy expressed against yearly downtime budgets at different uptime levels.

- **Title (bold 15px, `#1a5276`, top center):** "One 3-Minute Restart vs the Yearly Downtime Budget".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "99.9% — budget 526 min/yr": blue `#2a78d6` bar width 440 (budget), overlay green `#008300` bar width 3 (the 3-min deploy)
  - "99.99% — budget 53 min/yr": blue bar width 240, overlay green bar width 14
  - "99.999% — budget 5.3 min/yr": blue bar width 120, overlay orange `#d95926` bar width 68
  - "99.9999% — budget 0.5 min/yr": blue bar width 40, overlay red `#e74c3c` bar width 240 with 12px red label "6× over budget"
- **Bar style:** 14px tall, budget bars fill `rgba(42,120,214,0.30)`, deploy overlay bars solid, 11px width labels at bar ends.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "past four nines, restarts stop being an option".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic, budget minutes exact".

## Not the Same as a Rolling Deploy

**Tags:** `common mistake` (red), `state` (orange)

- **The confusion** — rolling deploys swap whole servers; hot swapping swaps code inside one process
- **State survives** — a hot swap keeps the process's in-memory state (the call's timer keeps ticking)
- **A restart forgets** — a rolled server loses its in-memory state unless it was saved elsewhere
- **The catch** — if v2 changes the shape of the state, you must write a state-upgrade function
- **The mistake** — swapping code that reads old-shaped state and crashes every live process at once

*Example (italic):* v2 adds a field to the call record; without a converter, all 900 calls crash at their next re-rate — worse than a restart.

**Common mistake:** Treating a hot swap as free. New code meeting old in-memory state is a real migration — Erlang runs an explicit `code_change` callback for exactly this.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a swap without a state converter (crash) vs with one (smooth), shown as call-record boxes flowing through the new code.

- **Title (bold 15px, `#1a5276`, top center):** "Old State Meets New Code: the code_change Step".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no converter"; blue `#2a78d6` rounded box at x=180 labeled "state v1 {id, start}" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "code v2 reads .plan — crash" with bold 12px red "✗ 900 processes die".
- **Row 2 (y=205), label:** "with code_change"; blue box "state v1 {id, start}", 3px arrow to a green `#008300` box at x=360 labeled "convert: add plan=default", then arrow to a green box at x=560 labeled "code v2 runs" with bold 12px green "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the swap is instant; the state migration is the real work".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); call counts and migration fractions are invented and labeled illustrative; uptime budget minutes (526 / 53 / 5.3 / 0.5) are the true yearly allowances for those percentages.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
