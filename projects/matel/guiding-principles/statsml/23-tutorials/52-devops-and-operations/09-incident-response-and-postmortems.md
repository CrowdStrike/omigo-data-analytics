# Incident Response & Postmortems

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Incident Response & Postmortems

**Subtitle:** How mature teams handle outages — mitigate first, diagnose later, and never write "human error" as the root cause

## The Checkout Outage at 2:14pm

**Tags:** `core idea` (blue), `mitigate first` (green), `SRE doctrine` (orange)

- **The outage** — a 2:10pm deploy breaks checkout for 40% of shoppers; the alert fires at 2:14pm
- **Severity routes response** — a revenue-stopping failure is a SEV-1: it pages a team, not one engineer
- **The roles** — an incident commander coordinates, an ops lead executes, a comms lead updates stakeholders
- **Why roles beat heroics** — the ops lead debugs uninterrupted because nobody asks them "is it fixed yet?"
- **The prime directive** — the commander orders a rollback at 2:22pm; nobody yet knows why the deploy broke
- **Diagnose later** — root-cause hunting starts after service is restored at 2:26pm, not before

*Example (italic):* The rollback restores checkout at 2:26pm — 16 minutes of impact — while a diagnose-in-place team waiting for the 4:00pm root cause would have eaten 110 minutes.

**Key point:** Restoring service beats understanding root cause in the moment — rollback, failover, or flag off first. This is published doctrine (Google's SRE book, PagerDuty's incident response guide), not improvisation.

### Visualization (canvas `c1`, 720×300)

Step-line timeline comparing checkout failure rate under two responses to the same fault: mitigate-first (rollback) vs diagnose-in-place (wait for root cause).

- **Title (bold 15px, `#1a5276`, top center):** "Same Fault, Two Responses: 16 Minutes of Impact vs 110".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes after 2:00pm, 0 to 130, 12px `#444` tick labels "2:00" / "2:30" / "3:00" / "3:30" / "4:00" at minutes `[0, 30, 60, 90, 120]`; y = % of checkouts failing 0 to 50, gridlines `#e5e9ef` at 10/20/30/40.
- **Diagnose-in-place line:** red `#e74c3c` 3px step line through minute/value pairs `[0,0], [10,0], [10,40], [120,40], [120,0], [130,0]` — failing at 40% from the 2:10 deploy until the 4:00pm root-cause fix.
- **Mitigate-first line:** green `#008300` 3px step line through `[0,0], [10,0], [10,40], [26,40], [26,0], [130,0]` — same fault, rollback ends impact at 2:26pm.
- **Rollback marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 22, 12px `#6b7280` label "rollback ordered 2:22" at its top.
- **Annotation (bold 13px green `#008300`, near minute 55, y=125):** "mitigate first: 16 min of impact, not 110".
- **Caption (12px `#444`, bottom right):** "failure rates illustrative".

## Anatomy of the Postmortem

**Tags:** `worked example` (blue), `postmortem` (green)

- **Timeline first** — deploy 2:10, alert 2:14, commander declared 2:16, rollback 2:22, restored 2:26, root cause 4:00
- **Impact quantified** — 16 minutes, 40% of checkouts failing, ~1,800 lost orders worth ~$54,000
- **Contributing causes, plural** — an untested config path, a canary that skipped checkout, an alert delayed 4 minutes
- **Action items with owners** — every fix gets a named owner and a deadline in the normal work tracker
- **Tracked or theater** — a postmortem that generates no tracked fixes is theater, however well written

*Example (italic):* The checkout postmortem lists 3 contributing causes and 5 action items; 3 close within two weeks and the other 2 carry deadlines and owners.

**Key point:** Timestamped timeline, quantified impact, plural contributing causes, and owned action items — that is the whole anatomy. Big incidents never have exactly one cause.

### Visualization (canvas `c2`, 720×300)

Horizontal event timeline of the checkout incident with the MTTD and MTTR windows drawn as labeled brackets beneath it.

- **Title (bold 15px, `#1a5276`, top center):** "One Incident, Six Timestamps: Detection to Root Cause".
- **Axis:** horizontal 2px `#999` line at y=140 from x=60 to x=680; time scale is schematic (not linear) so the 2:26→4:00 gap compresses.
- **Event markers (10px filled circles on the axis), left to right at x = `[90, 190, 280, 380, 480, 630]`:** deploy 2:10 (blue `#2a78d6`), alert fires 2:14 (orange `#d95926`), IC declared 2:16 (blue), rollback ordered 2:22 (green `#008300`), restored 2:26 (green), root cause confirmed 4:00 (violet `#4a3aa7`); each with a bold 12px label above (event name) and 12px `#444` label below (timestamp), alternating label heights y=105/y=170 to avoid overlap.
- **Axis break:** two short 2px `#999` slashes at x≈555 between "restored" and "root cause" marking the compressed gap.
- **MTTD bracket:** blue `#2a78d6` bracket under the axis from x=90 to x=190 at y=215, bold 12px blue label "MTTD 4 min (fault → alert)".
- **MTTR bracket:** green `#008300` bracket from x=90 to x=480 at y=250, bold 12px green label "MTTR 16 min (fault → restored)".
- **Annotation (bold 13px violet `#4a3aa7`, near x=630, y=200):** "root cause came 94 min after recovery — and that's fine".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative; axis schematic".

## Why "Human Error" Is Never the Root Cause

**Tags:** `why it matters` (blue), `blameless` (green), `systems over blame` (orange)

- **The classic case** — an engineer fat-fingers one command and drops a production table
- **Symptom, not cause** — the engineer isn't the cause; the system where a typo CAN drop a table without confirmation is
- **What blame buys** — punished people learn to hide mistakes, so the next incident loses its evidence
- **What systems buy** — confirmation prompts, dry-run defaults, and scoped prod access outlive any one engineer
- **The doctrine** — blameless postmortems are published SRE practice, with a dedicated chapter in Google's SRE book

*Example (italic):* After the dropped-table incident, the fix isn't firing anyone — it's making destructive commands require a typed table name and a second approver.

**Key point:** "Human error" is where the investigation starts, never where it ends — blaming people teaches hiding, while fixing the system that made the error possible prevents the repeat.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram contrasting the blame path and the blameless path from the same trigger event.

- **Title (bold 15px, `#1a5276`, top center):** "Same Typo, Two Investigations".
- **Row 1 (boxes centered at y=100), label 12px `#444` at x=20:** "blame the person"; blue `#2a78d6` rounded box at x=140 labeled "typo drops prod table" (12px), 3px arrow to an orange `#d95926` box at x=350 labeled "root cause: engineer", 3px arrow to a red `#e74c3c` box at x=560 labeled "next incident: facts hidden" with bold 12px red "✗ nothing fixed" beneath it.
- **Row 2 (boxes centered at y=210), label:** "blame the system"; identical blue box "typo drops prod table" at x=140, arrow to a green `#008300` box at x=350 labeled "root cause: no confirmation step", arrow to a green box at x=560 labeled "fix: typed confirm + approver" with bold 12px green "✓ typo now harmless".
- **Box style:** 160–180px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, centered.
- **Annotation (bold 13px magenta `#d55181`, centered near y=275):** "the person is a symptom; the system is the cause".

## Counting Incidents Is the Wrong Scoreboard

**Tags:** `common mistake` (red), `metrics` (orange)

- **The wrong metric** — driving incident counts down teaches teams to stop reporting incidents
- **MTTD and MTTR** — mean time to detect and to restore measure the response, not the honesty of reporting
- **The healthy pattern** — MTTR falls quarter over quarter even while reported incidents rise
- **Near-misses** — the failover that almost didn't fire is a free postmortem: same lessons, zero downtime
- **Game days** — deliberately injecting failure in rehearsal keeps the roles and the rollbacks practiced

*Example (italic):* Quarterly MTTR falls 58 → 41 → 26 → 14 minutes while reported incidents rise 6 → 9 → 11 → 12 — this team got healthier, not worse.

**Common mistake:** Rewarding a low incident count. Counts measure what gets reported; MTTD/MTTR measure how the system responds. Optimize the count and you get silence, not reliability.

### Visualization (canvas `c4`, 720×300)

Combo chart over four quarters: bars for reported incidents (rising), overlaid line for MTTR in minutes (falling).

- **Title (bold 15px, `#1a5276`, top center):** "More Reports, Faster Recovery: the Healthy Direction".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = quarters "Q1"–"Q4" centered at x = `[135, 285, 435, 585]` (12px `#444` labels); left y = MTTR minutes 0 to 60, gridlines `#e5e9ef` at 15/30/45; right-side 12px `#6b7280` legend "bars: incidents (0–15 scale)".
- **Incident bars:** blue fill `rgba(42,120,214,0.30)`, 70px wide, centered on the quarter x positions; counts `[6, 9, 11, 12]` on an implicit 0–15 scale (bar heights 72/108/132/144 px), 12px `#2a78d6` count labels above each bar.
- **MTTR line:** green `#008300` 3px line with 5px dots through the quarter centers at MTTR values `[58, 41, 26, 14]` on the 0–60 left scale, bold 12px green value labels ("58m", "41m", "26m", "14m") beside each dot.
- **Annotation (bold 13px green `#008300`, near x=430, y=70):** "reporting up, recovery time down — reward this".
- **Second annotation (bold 12px red `#e74c3c`, near x=135, y=60):** "judging by counts alone punishes Q4".
- **Caption (12px `#444`, bottom right):** "quarterly figures illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the incident timestamps, failure rates, order counts, and quarterly MTTR/incident figures are invented and labeled illustrative; text numbers and chart numbers must stay in lockstep (2:10/2:14/2:16/2:22/2:26/4:00, MTTD 4 min, MTTR 16 min, MTTR 58/41/26/14, incidents 6/9/11/12).
- **Attribution:** credit the doctrine in the subtitle sections as written — Google's SRE book (blameless postmortem chapter) and PagerDuty's incident response guide — without adding links.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
