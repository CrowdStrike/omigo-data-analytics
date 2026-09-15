# Managed Relational (RDS, Aurora)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Managed Relational (RDS, Aurora)

**Subtitle:** AWS RDS and Aurora run the Postgres or MySQL for you — you pay a premium and give up the server keys, and in exchange you stop being a database administrator

## Someone Else's Postgres

**Tags:** `core idea` (blue), `managed service` (green), `RDS` (orange)

- **The team** — a five-person startup runs its orders database; nobody on the team is a DBA
- **The old job** — self-hosting means provisioning, patching, backups, and failover are their problem
- **The service** — RDS is the same Postgres or MySQL engine, but AWS handles those chores
- **The trade** — no SSH, no OS access, no superuser: the box is theirs, the database is yours
- **The bill** — the same-size machine costs more managed: $140/mo self-run vs $290/mo RDS Multi-AZ (illustrative)

*Example (italic):* The team clicks "create database", picks Postgres and an instance size, and never installs, upgrades, or backs up the server again.

**Key point:** A managed relational database is the stock engine with the server work subtracted — you rent the DBA role from the provider and keep only the data-side work.

### Visualization (canvas `c1`, 720×300)

Two-row task-ownership diagram: six database chores as boxes, colored by who owns them — self-managed (all yours) vs on RDS (four move to the provider, two stay yours).

- **Title (bold 15px, `#1a5276`, top center):** "Six DBA Chores: Who Owns Them Before and After RDS".
- **Rows:** row 1 at y=95 with 12px `#444` label "self-managed" at x=20; row 2 at y=195 with label "on RDS" at x=20.
- **Boxes:** six per row, 85px wide, 40px tall, 8px radius, at x = `[130, 225, 320, 415, 510, 605]`, labeled (12px, centered) `["provision", "patch OS", "back up", "fail over", "tune queries", "schema"]`.
- **Row 1 fills:** all six blue `rgba(42,120,214,0.15)` with 2px `#2a78d6` borders and bold 11px blue "you" tag under each box.
- **Row 2 fills:** first four green `rgba(0,131,0,0.12)` with 2px `#008300` borders and bold 11px green "AWS" tags; last two stay blue with "you" tags.
- **Arrow:** vertical dashed `#6b7280` (dash 4/3) arrow from row 1 to row 2 at x=60, 12px `#6b7280` label "move to RDS" beside it.
- **Annotation (bold 13px green `#008300`, centered near y=265):** "the server chores leave; the data chores stay".

## The Night the Primary Died

**Tags:** `worked example` (blue), `failover` (green), `backups` (orange)

- **The setup** — Multi-AZ RDS keeps a synchronized standby copy in a second availability zone
- **The failure** — at 2:14am the primary's host dies mid-write; nobody on the team is awake
- **The failover** — RDS flips the database address to the standby with no human involved
- **The gap** — orders stop for about 90 seconds (illustrative; AWS documents one to two minutes as typical)
- **The backups** — daily snapshots plus 5-minute transaction logs restore to any second in the last 7 days (a few minutes behind live)

*Example (italic):* Orders run at ~80/min, drop to 0 at 2:14am, recover by 2:15:30, and are back at ~80/min by 2:16 — the team reads about it in the morning.

**Key point:** The failover the team never had to script, test, or run at 2am is the concrete thing the managed premium buys.

### Visualization (canvas `c2`, 720×300)

Line chart of orders per minute around the 2:14am failover: a level line, a 90-second cliff to zero, and a recovery to the same level.

- **Title (bold 15px, `#1a5276`, top center):** "Failover at 2:14am: 90 Seconds of Downtime, Zero Humans Paged".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "2:10" to "2:22" with 12px `#444` tick labels every 2 minutes; y = orders/min 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Orders line:** blue `#2a78d6` 3px line through minutes-after-2:10 `[0, 1, 2, 3, 4, 4.2, 5, 5.5, 6, 7, 8, 10, 12]`, orders/min `[78, 81, 80, 79, 80, 0, 0, 62, 79, 80, 82, 79, 81]` — vertical cliff to 0 just after minute 4 (2:14), recovering at minute 5.5 (2:15:30), back to level by minute 6 (2:16).
- **Failure marker:** vertical dashed red `#e74c3c` (dash 4/3) line at minute 4, bold 12px red label "primary dies" at its top.
- **Recovery marker:** vertical dashed green `#008300` line at minute 5.5, bold 12px green label "standby promoted" at its top.
- **Annotation (bold 13px green `#008300`, near minute 8, y=90):** "back to ~80 orders/min — nobody woke up".
- **Caption (12px `#444`, bottom right):** "order counts illustrative; 60–120s failover is the AWS-documented typical range".

## What Aurora Changes Under the Hood

**Tags:** `where it's used` (blue), `Aurora` (orange), `architecture` (green)

- **The split** — Aurora separates compute from storage: your instances run queries, a shared layer holds the data
- **Six copies** — the storage layer keeps 6 copies of every block across 3 availability zones (exact, AWS-documented)
- **The tolerance** — writes survive losing 2 copies, reads survive losing 3 — a whole AZ can vanish
- **Shared disks** — replicas read the same storage as the writer, so adding one copies no data
- **Auto-grow** — storage expands on its own up to the documented 128 TB ceiling; nobody resizes a volume

*Example (italic):* When the team adds a read replica for reporting, it attaches to the same 6-way-replicated storage in minutes instead of restoring a full copy.

**Key point:** Aurora is AWS rebuilding the database's bottom half — MySQL/Postgres-compatible on top, a replicated distributed storage service underneath.

### Visualization (canvas `c3`, 720×300)

Architecture diagram: writer and reader compute boxes on top, arrows down into one shared storage band containing six copy boxes grouped into three availability zones.

- **Title (bold 15px, `#1a5276`, top center):** "Aurora: Compute on Top, One Shared 6-Copy Storage Layer Below".
- **Compute row (y=70):** blue `rgba(42,120,214,0.15)` rounded boxes with 2px `#2a78d6` borders, 180px wide, 44px tall — "writer instance" at x=150, "reader instance" at x=400; 12px `#2c3e50` labels; 11px `#6b7280` caption "compute — yours to size" above at y=58.
- **Arrows:** 3px `#6b7280` arrows from the bottom center of each compute box down to the storage band top.
- **Storage band:** rounded rect x=60, y=170, width 600, height 95, fill `rgba(25,158,112,0.10)`, 2px `#199e70` border, bold 12px `#199e70` label "Aurora storage layer — auto-grows to 128 TB" at its top left inside.
- **AZ groups:** three dashed `#6b7280` group outlines inside the band at x = `[90, 290, 490]`, each 170px wide, 55px tall at y=200, labeled 11px `#6b7280` "AZ 1" / "AZ 2" / "AZ 3"; inside each, two aqua `#199e70` copy boxes 55×30 (fills `rgba(25,158,112,0.25)`) labeled "copy" (11px).
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=155):** "6 copies across 3 AZs — losing an entire AZ loses nothing".

## Managed Is Not the Same as Maintenance-Free

**Tags:** `common mistake` (red), `still your job` (orange)

- **The confusion** — teams read "managed" as "nothing left to manage" and stop thinking about the database
- **What left** — patching, backups, failover drills: roughly 7 of the team's 12 monthly ops hours (illustrative)
- **What stayed** — slow queries, missing indexes, schema migrations, connection limits are still entirely yours
- **No escape hatch** — with no SSH or superuser, you cannot install OS tools or unsupported extensions to dig yourself out
- **The surprise** — a missing index takes a managed database down exactly as fast as a self-hosted one

*Example (italic):* Three months in, an unindexed lookup on the grown orders table times out checkout — RDS is healthy the whole time, because query design was never AWS's job.

**Common mistake:** Believing the premium bought a DBA for everything. It bought the server half; the data half — schema, indexes, query patterns — never left your desk.

### Visualization (canvas `c4`, 720×300)

Horizontal grouped bar chart: monthly ops hours per task, self-managed (blue) vs on RDS (green), showing the server tasks collapsing to zero while the data tasks are unchanged.

- **Title (bold 15px, `#1a5276`, top center):** "Monthly Ops Hours: What the Premium Erases and What It Doesn't".
- **Axis:** bars extend right from a 2px `#999` baseline at x=210; scale 110 px per hour (max bar 330px for 3 hours); 12px `#444` task labels left-aligned at x=20.
- **Rows (task label y at 70, 110, 150, 190, 230), each with two 12px-tall bars — self-managed (blue `rgba(42,120,214,0.30)`, 2px `#2a78d6` edge) on top and RDS (solid green `#008300`) 15px below, 11px hour labels at bar ends:**
  - "patching": blue width 330 (3 h), green width 0 (0 h)
  - "backups": blue width 220 (2 h), green width 0 (0 h)
  - "failover drills": blue width 220 (2 h), green width 0 (0 h)
  - "query tuning": blue width 330 (3 h), green width 330 (3 h)
  - "schema & indexes": blue width 220 (2 h), green width 220 (2 h)
- **Legend (top right, 12px):** blue swatch "self-managed — 12 h/mo total", green swatch "on RDS — 5 h/mo total".
- **Annotation (bold 13px magenta `#d55181`, right side near y=260):** "the server work vanishes; the data work stays yours".
- **Caption (12px `#444`, bottom right):** "hours illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); order rates, dollar costs, and ops hours are invented and labeled illustrative; the 60–120s Multi-AZ failover range, 5-minute transaction-log backups with point-in-time restore, 6 storage copies across 3 AZs (writes tolerate losing 2 copies, reads 3), and the 128 TB storage ceiling are AWS-documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
