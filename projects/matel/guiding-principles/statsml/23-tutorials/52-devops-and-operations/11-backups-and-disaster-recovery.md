# Backups & Disaster Recovery

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Backups & Disaster Recovery

**Subtitle:** Two numbers turn "we have backups" into engineering — how much data you can afford to lose (RPO) and how long you can afford to be down (RTO); an untested backup is a hope

## The Day the Orders Table Vanished

**Tags:** `core idea` (blue), `RPO` (green), `RTO` (orange)

- **The disaster** — a bad migration corrupts the orders table at 2:47pm on a Tuesday
- **The last good copy** — the nightly backup finished at 1:00am, 13 hours 47 minutes earlier
- **The data bill** — every order from 1:00am to 2:47pm is gone: a 13.8-hour gap — your de-facto RPO
- **The time bill** — the restore takes 6 hours because nobody ever timed it: that is the RTO
- **The definitions** — RPO: how much data you can afford to LOSE; RTO: how long you can afford to be DOWN
- **The design rule** — an RPO of 1 hour means backups at least hourly; RTO is set by restore speed

*Example (italic):* The store is down from 2:47pm to 8:47pm (RTO: 6 hours) and every order placed after 1:00am is gone forever (RPO: 13.8 hours).

**Key point:** RPO is the gap between the disaster and the last good backup; RTO is the time to actually restore. Both are decided by your design long before the disaster — the outage just reveals them.

### Visualization (canvas `c1`, 720×300)

Timeline of the disaster day: last backup at 1:00am, corruption at 2:47pm, back online at 8:47pm, with the RPO gap and RTO window shown as shaded bands on one 24-hour axis.

- **Title (bold 15px, `#1a5276`, top center):** "One Disaster, Two Numbers: 13.8 Hours Lost, 6 Hours Down".
- **Axis:** horizontal 2px `#999` baseline at y=170 from x=60 to x=660; x maps 0:00–24:00 at 25 px/hour; 12px `#444` tick labels "0:00", "6:00", "12:00", "18:00", "24:00" at x = 60, 210, 360, 510, 660 (y=260).
- **RPO band:** red fill `rgba(231,76,60,0.15)` rectangle x=85 to x=430, y=140 to y=200; bold 13px red `#e74c3c` label above it at y=115: "RPO reality: 13.8 h of orders lost".
- **RTO band:** orange fill `rgba(230,126,34,0.18)` rectangle x=430 to x=580, y=140 to y=200; bold 13px orange `#d95926` label below it at y=225: "RTO reality: 6 h to restore".
- **Markers (3px vertical lines y=130–210 with 12px labels):** green `#008300` at x=85 "last good backup 1:00am" (label above, y=132, left-anchored); red `#e74c3c` at x=430 "corruption 2:47pm" (label at y=100 if RPO label shifts left, else beside marker); green `#008300` at x=580 "back online 8:47pm" (label at y=132).
- **Annotation (bold 12px violet `#4a3aa7`, near x=620, y=250):** "1:00am → 2:47pm = 13h47m ≈ 13.8 h".
- **Caption (12px `#444`, bottom right):** "clock times exact for the scenario; scenario illustrative".

## Buying a Smaller RPO: the Backup Taxonomy

**Tags:** `worked example` (blue), `full / incremental / continuous` (green), `3-2-1 rule` (orange)

- **Full backup** — one complete nightly copy; simplest restore, worst-case RPO is 24 hours
- **Incremental** — hourly copies of changes since the last backup; worst-case RPO drops to 1 hour
- **Continuous** — ship the WAL/binlog as writes happen; RPO shrinks to about 1 minute
- **Snapshot vs dump** — a disk snapshot restores in minutes; reloading the 500 GB logical dump took 4 of the 6 hours
- **Restore cost** — incrementals replay a chain, WAL replays a log: smaller RPO can mean a longer restore
- **3-2-1 rule** — keep 3 copies of the data, on 2 kinds of media, with 1 copy offsite

*Example (italic):* With hourly incrementals the 2:47pm corruption would have cost the 47 minutes since the 2:00pm backup — not 13.8 hours.

**Key point:** The schedule sets your RPO — at worst you lose one backup interval — and the restore path sets your RTO. Pick the style from the RPO you can afford, then time the restore to learn your real RTO.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: worst-case RPO for the three backup styles, bar widths log-feel so all three stay visible.

- **Title (bold 15px, `#1a5276`, top center):** "Worst-Case RPO: You Lose, at Most, One Backup Interval".
- **Axis:** bars start at x=250, extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (14px-tall bars at y = 80, 145, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "nightly full — up to 1,440 min": red `#e74c3c` bar width 440, 12px bold red end label "24 h"
  - "hourly incremental — up to 60 min": orange `#d95926` bar width 150, 12px bold orange end label "1 h"
  - "continuous WAL shipping — ~1 min": green `#008300` bar width 20, 12px bold green end label "~1 min"
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=260):** "RPO of 1 hour ⇒ backups at least hourly".
- **Caption (12px `#444`, bottom right):** "bar widths log-feel schematic, minutes exact".

## The Four Ways Backups Betray You

**Tags:** `why it matters` (blue), `restore drills` (green), `ransomware` (red)

- **Silent failure** — the backup job breaks and no one notices for months; alert when a backup does NOT appear
- **Won't restore** — corruption or missing pieces surface only mid-restore; a backup is proven only by restoring it
- **The doctrine** — an untested backup is a hope: schedule real restore drills and time them (that timing IS your RTO)
- **Same blast radius** — the attacker or bug that hit primary also deletes every backup it can reach
- **The ransomware lesson** — keep one immutable or offline copy that no online credential can erase
- **Replication ≠ backup** — the replica applies the DELETE in milliseconds; it protects against hardware loss, not mistakes

*Example (italic):* The 2:47pm corruption reached the replica in under a second — the replica became a perfect copy of the disaster.

**Key point:** Every failure mode has the same cure: prove it. Alert on missing backups, drill the restore end to end, and keep one copy that nothing online — attacker, bug, or admin — can touch.

### Visualization (canvas `c3`, 720×300)

Line chart of nightly backup size over 90 days: healthy ~220 GB until the job silently dies on day 30, then flat zero for 60 days while the status strip above stays green.

- **Title (bold 15px, `#1a5276`, top center):** "The Backup Job Died on Day 30 — the Dashboard Stayed Green".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 170; x = days 0 to 90 with 12px `#444` tick labels every 30 days; y = backup size 0 to 240 GB, gridlines `#e5e9ef` at 60/120/180 with 12px `#444` labels.
- **Size line:** blue `#2a78d6` 3px line through days `[0, 5, 10, 15, 20, 25, 29]`, GB `[214, 215, 217, 218, 220, 221, 222]`, then a vertical drop at day 30 and flat through days `[30, 45, 60, 75, 90]`, GB `[0, 0, 0, 0, 0]`.
- **Status strip:** green fill `rgba(0,131,0,0.25)` rectangle x=60 to x=660, y=52 to y=64, 12px green `#008300` label above at y=48: "monitoring: green all 90 days (it checks the cron, not the file)".
- **Failure marker:** vertical dashed `#6b7280` (dash 4/3) line at day 30, 12px `#6b7280` label "job breaks" at its top.
- **Annotation (bold 13px red `#e74c3c`, near day 55, y=150):** "60 days with zero restorable backups".
- **Caption (12px `#444`, bottom right):** "sizes illustrative".

## A Backup File Is Not a DR Plan

**Tags:** `common mistake` (red), `DR tiers` (orange)

- **The mistake** — treating the dump file as the whole plan: the data survives but there is nothing to restore it INTO
- **Rebuild list** — the runbook, DNS control, secrets, and infrastructure-as-code needed to stand up elsewhere
- **The hidden hours** — 2 of the story's 6 restore hours were spent hunting for DB passwords and DNS access
- **The tiers** — backup-restore → pilot light → warm standby → active-active
- **The ladder** — each tier buys a shorter RTO at a higher standing cost for idle-but-ready infrastructure
- **Choosing** — price your downtime per hour, then buy the cheapest tier whose RTO beats that bill

*Example (italic):* A warm standby would have cut the 6-hour outage to about 30 minutes — for roughly 5× the monthly infrastructure bill.

**Common mistake:** Believing the data is the disaster-recovery plan. A replica copies your mistakes and a dump cannot rebuild your stack — DR is the data plus the runbook, DNS, secrets, and IaC to bring it all back somewhere else.

### Visualization (canvas `c4`, 720×300)

Paired horizontal bars per DR tier: RTO (blue, log-feel widths) and relative standing cost (orange), showing the RTO/cost ladder.

- **Title (bold 15px, `#1a5276`, top center):** "DR Tiers: Buying Down RTO with Standing Cost".
- **Axis:** bars start at x=230, extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 65, 120, 175, 230), each with a left-aligned 12px `#444` label at x=20; per row a blue `rgba(42,120,214,0.85)` RTO bar 14px tall at y, and an orange `#d95926` cost bar 8px tall at y+17:**
  - "backup-restore": RTO bar width 440, end label "RTO ~24 h"; cost bar width 44, end label "cost 1×"
  - "pilot light": RTO bar width 220, end label "RTO ~4 h"; cost bar width 88, end label "cost 2×"
  - "warm standby": RTO bar width 90, end label "RTO ~30 min"; cost bar width 220, end label "cost 5×"
  - "active-active": RTO bar width 8, end label "RTO ~seconds"; cost bar width 440, end label "cost 10×"
- **Bar labels:** 11px, RTO labels blue `#2a78d6`, cost labels orange `#d95926`, drawn just past each bar end.
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=282):** "every rung down in RTO is a rung up in cost".
- **Caption (12px `#444`, bottom right):** "RTO and cost multiples illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the RPO/RTO arithmetic is exact (1:00am → 2:47pm = 13h47m ≈ 13.8 h; 2:00pm → 2:47pm = 47 min; worst-case intervals 1,440 / 60 / ~1 min), while the scenario itself, backup sizes, and DR-tier RTO/cost multiples are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
