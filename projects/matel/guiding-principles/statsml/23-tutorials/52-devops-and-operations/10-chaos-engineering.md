# Chaos Engineering

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Chaos Engineering

**Subtitle:** Your system claims it survives a server dying — chaos engineering breaks things on purpose, in controlled conditions, to check the claim before 3am does

## The Monkey That Kills Servers on Purpose

**Tags:** `core idea` (blue), `Netflix origin` (orange), `resilience` (green)

- **The claim** — a checkout service runs on 10 instances and claims any one can die with no harm
- **The problem** — a claim never tested is a hope; nobody knows if failover works until it runs
- **Chaos Monkey** — Netflix (publicly documented, ~2010) built a tool that kills random production instances
- **Business hours** — kills happen during the workday, when engineers are at their desks to respond
- **The effect** — instances died daily, so every team HAD to build auto-recovery; resilience became habit

*Example (italic):* At 10:14am Chaos Monkey kills one of 10 checkout instances; autoscaling replaces it by 10:18am and checkout success never leaves 99.9%.

**Key point:** Chaos engineering tests resilience claims by deliberately injecting the failure — if recovery is automatic, the kill is a non-event; if not, you learn at 10am instead of 3am.

### Visualization (canvas `c1`, 720×300)

Timeline chart of one business-hours Chaos Monkey kill: healthy instance count dips from 10 to 9 and recovers, while a kill marker and a flat-success annotation show the non-event.

- **Title (bold 15px, `#1a5276`, top center):** "Chaos Monkey Kills an Instance at 10:14am — Autoscaling Refills by 10:18am".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "10:00" to "10:30" with 12px `#444` tick labels every 5 minutes; y = healthy instances 0 to 12, gridlines `#e5e9ef` at 3/6/9.
- **Instance line:** blue `#2a78d6` 3px line through minutes `[0, 5, 10, 14, 14.5, 16, 18, 20, 25, 30]`, instances `[10, 10, 10, 10, 9, 9, 10, 10, 10, 10]` — sharp step down to 9 at 10:14, step back to 10 at 10:18.
- **Kill marker:** vertical dashed `#e74c3c` (dash 4/3) line at minute 14, bold 12px `#e74c3c` label "chaos monkey kill" at its top.
- **Recovery marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 18, 12px `#6b7280` label "autoscaler refills".
- **Annotation (bold 13px green `#008300`, near minute 22, y=90):** "checkout success stays 99.9% — kill is a non-event".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## One Experiment: Kill a Zone, Watch Checkout

**Tags:** `worked example` (blue), `blast radius` (green), `hypothesis` (orange)

- **Hypothesis first** — state the steady state: "checkout success rate stays above 99.9% if one zone dies"
- **Blast radius** — start in staging, then 1% of production traffic, then 25%; never everything at once
- **Safety rails** — run in business hours with a hand on the abort switch, not at 2am unattended
- **Measure** — compare the success rate during the injected zone failure against the 99.9% hypothesis
- **The find** — at 25% the rate falls to 99.42%: a retry storm hammers the surviving zone; fix and rerun

*Example (italic):* Staging holds 99.93%, 1% of production holds 99.91%, but the 25% run drops to 99.42% and is aborted — after capping retries, the rerun holds 99.91%.

**Key point:** The published method is a controlled experiment: hypothesis about steady state, bounded blast radius, measurement against the hypothesis, and a fix for whatever broke.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of checkout success rate across four experiment runs against the 99.9% hypothesis line; the failed 25% run is red and marked aborted.

- **Title (bold 15px, `#1a5276`, top center):** "One Zone Killed: Success Rate vs the 99.9% Hypothesis".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = success rate 99.0% to 100.0%, gridlines `#e5e9ef` at 99.25/99.5/99.75, 12px `#444` y labels; x = four bars with 12px `#444` two-line labels under each.
- **Hypothesis line:** horizontal dashed `#4a3aa7` (dash 5/4) line at 99.9%, bold 12px `#4a3aa7` label "hypothesis: ≥ 99.9%" above it at the right.
- **Bars (width 90, centered at x = 140, 280, 420, 560), values hardcoded:**
  - "staging": 99.93 — green `#008300`, fill `rgba(0,131,0,0.30)`
  - "1% prod": 99.91 — green, fill `rgba(0,131,0,0.30)`
  - "25% prod (run 1)": 99.42 — red `#e74c3c`, fill `rgba(231,76,60,0.25)`, bold 12px red label "ABORTED — retry storm" above the bar
  - "25% prod (run 2)": 99.91 — green, fill `rgba(0,131,0,0.30)`, 12px green label "after retry cap"
- **Value labels:** bold 12px matching bar color at each bar top ("99.93%", "99.91%", "99.42%", "99.91%").
- **Caption (12px `#444`, bottom right):** "rates illustrative".

## The Experiment Menu, from Kills to Latency

**Tags:** `where it's used` (blue), `game days` (green)

- **Instance and zone kills** — the classics: terminate a server, then a whole availability zone
- **Injected latency** — add 300ms to a dependency; often nastier than clean death, it trips timeouts and retries
- **Dependency blackhole** — drop all traffic to one downstream service and see who actually handles it
- **Resource exhaustion** — burn CPU or fill a disk to test limits, alerts, and shedding behavior
- **Game days** — the human layer: teams practice incident response on a simulated outage, on the clock
- **Why production** — staging never matches real traffic, real data, or real config; eventually you test live

*Example (italic):* In an illustrative program, injected latency surfaces 9 distinct bugs (5 in staging, 4 more only in production) — more than any clean-kill experiment.

**Key point:** Slow is a different failure than dead — latency experiments expose timeout and retry bugs that clean kills never touch, and some only appear under real production traffic.

### Visualization (canvas `c3`, 720×300)

Horizontal stacked bar chart: five experiment types, bugs surfaced in staging (blue segment) vs additional bugs found only in production (orange segment).

- **Title (bold 15px, `#1a5276`, top center):** "Bugs Surfaced per Experiment Type: Staging vs Production-Only".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 30px per bug; 12px `#444` row labels left-aligned at x=20.
- **Rows (top to bottom at y = 65, 105, 145, 185, 225), segments `[staging, prod-only]` hardcoded:**
  - "instance kill": `[3, 1]` — total 4
  - "zone failure": `[2, 3]` — total 5
  - "injected latency (+300ms)": `[5, 4]` — total 9
  - "dependency blackhole": `[4, 2]` — total 6
  - "resource exhaustion": `[2, 1]` — total 3
- **Bar style:** 18px tall; staging segment fill `rgba(42,120,214,0.35)` with 1px `#2a78d6` border; prod-only segment fill `rgba(217,89,38,0.35)` with 1px `#d95926` border; bold 11px total count at each bar end.
- **Legend (12px, top right under title):** blue swatch "found in staging", orange swatch "found only in production".
- **Annotation (bold 13px `#d95926`, right side near y=150):** "slow beats dead: latency finds the most bugs".
- **Caption (12px `#444`, bottom right):** "bug counts illustrative".

## Chaos Without Observability Is Just Vandalism

**Tags:** `common mistake` (red), `prerequisites` (orange)

- **The prerequisite** — before injecting failure you must be able to SEE its impact and STOP it fast
- **No dashboards** — inject a zone failure blind and you've caused an outage you can't even detect
- **No abort switch** — an experiment you can't halt is not an experiment, it's an incident with a signature
- **The order** — observability and rollback first, chaos second; skipping the order breaks production for nothing
- **The payoff** — with both in place, a breached hypothesis is caught and aborted in under a minute

*Example (italic):* Team A blackholes a dependency with no dashboards and debugs a mystery outage for 45 minutes; Team B runs the same experiment, sees the 99.9% breach at 40 seconds, and aborts.

**Common mistake:** Running chaos experiments before the system has observability and a rollback path — breaking production without the ability to see or stop the damage is vandalism, not engineering.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same injected failure with no observability (mystery outage) vs with dashboards and an abort switch (caught and stopped).

- **Title (bold 15px, `#1a5276`, top center):** "Same Failure Injected, Two Very Different Endings".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no dashboards, no abort"; blue `#2a78d6` rounded box at x=180 labeled "blackhole dependency" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "invisible outage — 45 min of guessing" with bold 12px red "✗ vandalism".
- **Row 2 (y=205), label:** "dashboards + abort switch"; blue box at x=180 labeled "blackhole dependency", 3px arrow to a green `#008300` box at x=360 labeled "breach seen at 40s", then arrow to a green box at x=560 labeled "abort — traffic restored" with bold 12px green "✓ experiment".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "observability and rollback come first; the chaos comes second".
- **Caption (12px `#444`, bottom right):** "durations illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); instance counts, success rates, bug counts, and durations are invented and labeled illustrative; the Netflix / Chaos Monkey history and the hypothesis–blast-radius–measure method are publicly documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
