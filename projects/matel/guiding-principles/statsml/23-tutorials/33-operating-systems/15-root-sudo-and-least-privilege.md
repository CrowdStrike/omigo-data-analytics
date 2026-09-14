# root, sudo & Least Privilege

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** root, sudo & Least Privilege

**Subtitle:** One account on every machine can do anything at all — the superuser discipline is keeping everyone off it until a task truly needs it

## The Master Key Behind the Counter

**Tags:** `core idea` (blue), `accounts & permissions` (green), `master key` (orange)

- **The till computer** — a coffee shop's one machine runs the register, the orders table, staff reports
- **Staff logins** — the barista login rings sales, the analyst login writes reports; nothing more
- **root** — the one superuser login that can read, change, or delete every file on the machine
- **sudo** — a staff login borrows root for a single command, and the loan is written to a log
- **Least privilege** — each login gets exactly the access its job needs, and not one file more

*Example (italic):* The analyst login can rewrite anything in its reports folder but cannot touch the till software — a bad report script can never break the register.

**Key point:** root is the account with no limits; least privilege is the discipline of keeping day-to-day work off it, with sudo as the logged, temporary loan for the rare task that needs it.

### Visualization (canvas `c1`, 720×300)

Permission matrix: three accounts (rows) against four areas of the machine (columns), each cell a rounded box saying write, read, or no.

- **Title (bold 15px, `#1a5276`, top center):** "Who Can Touch What on the Till Computer".
- **Geometry:** column headers (bold 12px `#1a5276`) at x = 250, 370, 490, 610: "till software", "orders data", "reports", "system files"; row labels (12px `#444`, left-aligned at x=25) at y = 110, 165, 220: "barista login", "analyst login", "root".
- **Cells:** 92×34px rounded boxes (6px radius) centered under each header on each row; 12px bold cell text.
- **Cell values:** barista row `["write", "read", "no", "no"]`; analyst row `["no", "read", "write", "no"]`; root row `["write", "write", "write", "write"]`.
- **Cell colors:** "write" fill `rgba(0,131,0,0.12)`, text green `#008300`; "read" fill `rgba(201,133,0,0.12)`, text yellow `#c98500`; "no" fill `rgba(231,76,60,0.10)`, text red `#e74c3c`.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "root's row is all green — total power, total blast radius".
- **Caption (12px `#444`, bottom right):** "permissions illustrative".

## One Stray Space, Two Blast Radii

**Tags:** `worked example` (blue), `blast radius` (green), `one typo` (red)

- **The setup** — the till computer's 1,240 files all live under /data; the analyst can write only 12 reports
- **The typo** — a cleanup script means `rm -rf /data/tmp` but a stray space makes it `rm -rf /data/ tmp`
- **As the analyst** — the command wipes the 12 report files, then dies on "permission denied" everywhere else
- **As root** — the same command wipes all of /data: orders, till software, all 1,240 files
- **Hand-check** — blast radius = files the account can write: 12 vs 1,240, roughly a 100× difference

*Example (italic):* Restoring 12 report files takes ten minutes from last night's backup; rebuilding the whole till computer keeps the shop offline for a day.

**Key point:** A mistake's blast radius equals the account's write access — the login you run under, not the care you type with, sets the worst case.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: files deleted by the identical typo'd command, run under the analyst login vs under root.

- **Title (bold 15px, `#1a5276`, top center):** "One Stray Space: Files Deleted as Analyst vs as root".
- **Axis:** vertical 2px `#999` baseline at x=180, bars extend right, max width 480; widths schematic, not to scale.
- **Row 1 (y=110), label 12px `#444` at x=20:** "analyst login"; green `#008300` bar width 60, height 26, 12px green label at bar end "12 files — its own reports folder".
- **Row 2 (y=190), label:** "root"; red `#e74c3c` bar width 480, height 26, 12px red label above the bar "1,240 files — everything in /data".
- **Wall marker:** vertical dashed `#6b7280` (dash 4/3) line at x=252 spanning row 1 only, 11px `#6b7280` label "permission denied stops it here".
- **Annotation (bold 13px green `#008300`, near x=280, y=260):** "least privilege turned a disaster into a ten-minute restore".
- **Caption (12px `#444`, bottom right):** "widths schematic, file counts illustrative".

## Counting How Often root Was Really Needed

**Tags:** `where it's used` (blue), `sudo log` (orange), `daily habits` (green)

- **The month's log** — the shop's sudo log shows 20 borrowed-root commands; an audit finds only 3 needed it
- **The real three** — installing the backup tool, restarting the till service, creating a new staff login
- **The habit 17** — editing own reports, rerunning scripts, reading the orders table: none needed root
- **Databases too** — same idea: dashboards get a read-only database login, never the admin password
- **Why audits care** — every sudo line records who ran what, so the discipline can actually be checked

*Example (italic):* The analyst's dashboard queries run under a read-only database login, so a bad query can never drop the orders table.

**Key point:** Least privilege is measurable — count how often root was borrowed versus how often it was needed; the gap between 20 and 3 is standing risk.

### Visualization (canvas `c3`, 720×300)

Waffle chart: the month's 20 sudo commands as a 10×2 grid of squares, colored by whether root was truly required.

- **Title (bold 15px, `#1a5276`, top center):** "One Month of sudo: 20 Commands, Only 3 Truly Needed root".
- **Grid:** 20 squares (10 columns × 2 rows), 42×42px each with 10px gaps, 6px radius, starting at x=100, y=85.
- **Colors:** first 3 squares green fill `rgba(0,131,0,0.30)` with 2px `#008300` border ("needed root"); remaining 17 squares orange fill `rgba(217,89,38,0.25)` with 2px `#d95926` border ("habit").
- **Legend (12px, y=215):** green swatch + `#008300` text "needed root — 3" at x=100; orange swatch + `#d95926` text "habit, no root needed — 17" at x=300.
- **Annotation (bold 13px green `#008300`, centered near y=258):** "17 of 20 were habit — each one a needless master-key loan".
- **Caption (12px `#444`, bottom right):** "audit counts illustrative".

## sudo Is Not a Fix-It Prefix

**Tags:** `common mistake` (red), `root-owned files` (orange)

- **The reflex** — a script fails with "permission denied", so the fix becomes typing sudo and moving on
- **The trap** — the sudo run writes its output files owned by root, inside the analyst's own folder
- **The next day** — the normal no-sudo run now fails on those root-owned files: more denials, more sudo
- **The spiral** — each sudo run plants more root-owned files, until nothing runs without the master key
- **The real fix** — one `chown` hands the files back, then ask why the first denial happened at all

*Example (italic):* After a week of reflex-sudo, 9 of the analyst's 12 report files are owned by root and every script in the folder demands the master key.

**Common mistake:** Reading "permission denied" as "add sudo" instead of "wrong account or wrong owner" — the prefix silences the message once and converts it into a permanent dependency on root.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the reflex-sudo loop (files end up root-owned, denial returns) vs the one-time ownership fix.

- **Title (bold 15px, `#1a5276`, top center):** "Two Answers to 'permission denied'".
- **Row 1 (y=95), label 12px `#444` at x=20:** "reflex: sudo"; red `#e74c3c` rounded box at x=150 labeled "denied" (12px), 3px arrow to an orange `#d95926` box at x=310 labeled "sudo run — output owned by root", 3px arrow to a red box at x=540 labeled "denied again tomorrow"; dashed red loop arrow from the last box back to the first, 11px red label "after a week: 9 of 12 files root-owned".
- **Row 2 (y=205), label:** "fix: chown once"; red box at x=150 "denied", 3px arrow to a blue `#2a78d6` box at x=330 labeled "chown files back to analyst", 3px arrow to a green `#008300` box at x=545 labeled "runs as analyst every day ✓".
- **Box style:** 130–190px wide, 40px tall, 8px radius, fills `rgba(231,76,60,0.12)` / `rgba(217,89,38,0.15)` / `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "sudo silences the message; it does not fix the owner".
- **Caption (12px `#444`, bottom right):** "file counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the permission matrix cells, file counts (1,240 total / 12 writable / 9 root-owned after a week), the schematic bar widths (60 vs 480), and the sudo audit split (20 commands, 3 needed, 17 habit) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
