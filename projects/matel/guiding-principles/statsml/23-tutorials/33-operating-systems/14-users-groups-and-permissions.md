# Users, Groups & Permissions

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Users, Groups & Permissions

**Subtitle:** Every file answers three questions — read? write? execute? — three separate times: once for its owner, once for its group, once for everyone else

## One File, Three Circles of People

**Tags:** `core idea` (blue), `owner / group / other` (green), `rwx` (orange)

- **The file** — a coffee shop chain keeps `daily_sales.csv` on one shared server everyone logs into
- **The owner** — maya, the analyst who created the file, needs to read it and update it every evening
- **The group** — the `analysts` group (maya, raj) should be able to read it but never edit it
- **Everyone else** — tom the intern and every other account on the server should see nothing
- **The bits** — the mode `rw-r-----` grants exactly that: owner rw-, group r--, other ---
- **The check** — the OS picks ONE circle for you (owner, group, or other) and reads its three bits

*Example (italic):* When raj opens the file the OS asks only one question — is `r` on in the group slot? It is, so he reads; his write attempt hits the `-` and fails.

**Key point:** A permission is not "who trusts whom" — it is nine on/off switches on the file itself, three (read, write, execute) for each of three circles (owner, group, other).

### Visualization (canvas `c1`, 720×300)

Access matrix for the running example: three people (rows) against read / write / execute (columns) under mode `rw-r-----`.

- **Title (bold 15px, `#1a5276`, top center):** "daily_sales.csv (rw-r-----): Same File, Three Different Answers".
- **File strip:** 12px `#6b7280` centered at y=44: "owner maya · group analysts · mode rw-r-----".
- **Column headers (bold 13px `#2c3e50`) at y=78:** "read" x=300, "write" x=440, "execute" x=580.
- **Row labels (12px `#444`, left-aligned at x=30) at y = 120, 175, 230:** "maya — owner (rw-)", "raj — analysts (r--)", "tom — other (---)".
- **Cells:** at each (row y, column x) draw bold 16px "✓" in green `#008300` when granted, 16px "—" in mute `#6b7280` when denied; grants hardcoded as rows `[[1,1,0],[1,0,0],[0,0,0]]` for maya/raj/tom × read/write/execute.
- **Row separators:** 1px `#e5e9ef` horizontal lines at y = 145 and 200 from x=30 to x=650.
- **Annotation (bold 13px violet `#4a3aa7`, near x=380, y=268):** "the OS picks your circle first, then reads its 3 bits".
- **Caption (12px `#444`, bottom right):** "example file and users illustrative".

## Reading rw-r----- as the Number 640

**Tags:** `worked example` (blue), `octal` (green), `the lattice` (orange)

- **Three prices** — each switch has a value: r = 4, w = 2, x = 1
- **One sum per circle** — owner rw- is 4+2+0 = 6; group r-- is 4; other --- is 0
- **The mode** — read the three sums left to right: `rw-r-----` is exactly 640
- **The lattice** — the 8 possible rwx combos are the 8 sums 0 through 7; each combo appears once
- **Hand-check** — 755 unpacks to rwx r-x r-x, because 7 = 4+2+1 and 5 = 4+0+1
- **Round trip** — `chmod 640 daily_sales.csv` sets `rw-r-----`; `ls -l` shows it back as letters

*Example (italic):* Maya types `chmod 640 daily_sales.csv` and `ls -l` answers `-rw-r-----` — the digits and the letters are the same nine switches in two costumes.

**Key point:** Octal modes are not codes to memorize — each digit is just the sum of that circle's switches (r=4, w=2, x=1), so 640 can be rebuilt by hand in seconds.

### Visualization (canvas `c2`, 720×300)

The rwx lattice: all 8 combinations drawn as a diamond (Hasse diagram) from `---` (0) at the bottom to `rwx` (7) at the top, with the two combos used by mode 640 highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "The rwx Lattice: 8 Combos, One Digit Each (r=4, w=2, x=1)".
- **Nodes:** rounded boxes 88×26, 6px radius, 12px centered labels "combo (digit)"; positions hardcoded:
  - top: `rwx (7)` at (330, 62)
  - middle-upper row at y=118: `rw- (6)` x=180, `r-x (5)` x=330, `-wx (3)` x=480
  - middle-lower row at y=174: `r-- (4)` x=180, `-w- (2)` x=330, `--x (1)` x=480
  - bottom: `--- (0)` at (330, 230)
- **Edges:** 1.5px `#e5e9ef` lines connecting each combo to combos one added bit above: 0→{1,2,4}, 1→{3,5}, 2→{3,6}, 4→{5,6}, 3→7, 5→7, 6→7 (edge pairs hardcoded as index list `[[0,1],[0,2],[0,4],[1,3],[1,5],[2,3],[2,6],[4,5],[4,6],[3,7],[5,7],[6,7]]` over nodes ordered by digit).
- **Highlights:** `rw- (6)` box fill `rgba(42,120,214,0.18)` border 2px blue `#2a78d6` with bold 11px blue tag "owner" above it; `r-- (4)` box fill `rgba(0,131,0,0.14)` border 2px green `#008300` with bold 11px green tag "group" below it; `--- (0)` border 2px mute `#6b7280` with 11px mute tag "other" below it; all other boxes fill `#fff`, 1px `#6b7280` border.
- **Annotation (bold 13px ink `#1a5276`, right side near x=560, y=250):** "rw-r----- is just 6-4-0".
- **Caption (12px `#444`, bottom right):** "bit values exact — r=4, w=2, x=1".

## Where Permission Denied Finds a Data Scientist

**Tags:** `where it's used` (blue), `shared servers` (green), `permission denied` (orange)

- **The nightly job** — a cron job running as another user can't read your 600 file; group access fixes it
- **The script** — a fresh `train.sh` won't run until `chmod +x` flips its execute bit on
- **Directories too** — on a folder, `x` means "may enter"; without it even readable files are unreachable
- **Team data** — shared project folders live under a group so teammates read without owning
- **Production files** — 640 with a tight group is the usual shape for data others read but never edit

*Example (italic):* A model retrain fails at 2am with "permission denied" because the data file is 600 — the service account is in the right group, but the group digit is 0.

**Key point:** On any shared machine — lab box, cloud VM, cluster — a large share of mysterious pipeline failures are one wrong digit in a mode, and reading the digit tells you which of the nine switches to flip.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: five common modes ranked by how many of the file's nine switches they turn on.

- **Title (bold 15px, `#1a5276`, top center):** "Nine Switches per File: How Open Are the Common Modes?".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 432 (48px per switch); light gridlines `#e5e9ef` at 3, 6, 9 switches (x = 374, 518, 662) with 11px `#6b7280` tick labels at y=258.
- **Rows (14px-tall bars, top to bottom at y = 70, 110, 150, 190, 230), each with a left-aligned 12px `#444` label at x=20:**
  - "600 rw------- (private)": blue `#2a78d6` bar, 2 switches, width 96
  - "640 rw-r----- (our file)": green `#008300` bar, 3 switches, width 144
  - "644 rw-r--r-- (world-readable)": aqua `#199e70` bar, 4 switches, width 192
  - "755 rwxr-xr-x (program)": violet `#4a3aa7` bar, 7 switches, width 336
  - "777 rwxrwxrwx (wide open)": orange `#d95926` bar, 9 switches, width 432
- **Bar labels:** 11px `#444` switch counts ("2", "3", "4", "7", "9") just past each bar end.
- **Annotation (bold 13px orange `#d95926`, near x=390, y=255):** "777 flips all nine — anyone may overwrite".
- **Caption (12px `#444`, bottom right):** "switch counts exact for each mode".

## chmod 777 Fixes the Error and Breaks the Server

**Tags:** `common mistake` (red), `chmod 777` (orange)

- **The temptation** — tom gets "permission denied", searches the error, and finds `chmod 777` as the top fix
- **What it does** — 777 turns on all nine switches: every account may now read, write, and execute
- **The damage** — any user, buggy script, or compromised service can silently rewrite the sales history
- **The right fix** — widen the circle, not the switches: add tom to `analysts` and keep the mode 640
- **The habit** — when denied, first ask "which circle am I in?" before touching a single bit

*Example (italic):* After `chmod 777 daily_sales.csv`, a typo in someone else's cleanup script truncates the file — the mode said yes to a write nobody intended.

**Common mistake:** Treating the mode as the broken part. More often than not the switches are right and the person is in the wrong circle — fix membership with groups, and let 640 keep doing its job.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the 777 "fix" (opens the file to everyone) vs the group fix (tom gets in, switches unchanged).

- **Title (bold 15px, `#1a5276`, top center):** "Two Fixes for the Same Denied Read".
- **Row 1 (boxes centered on y=110), label 12px `#444` at x=20 (y=110):** "the 777 fix"; blue `#2a78d6` rounded box at x=130 labeled "tom: denied (mode 640)", 3px `#6b7280` arrow to an orange `#d95926` box at x=330 labeled "chmod 777", 3px arrow to a red `#e74c3c` box at x=520 labeled "anyone can edit sales" with bold 12px red "✗ 9 switches on" beneath it at y=150.
- **Row 2 (boxes centered on y=215), label at x=20 (y=215):** "the group fix"; blue box at x=130 labeled "tom: denied (mode 640)", 3px arrow to a green `#008300` box at x=330 labeled "add tom to analysts", 3px arrow to a green box at x=520 labeled "tom reads, mode stays 640" with bold 12px green "✓ 3 switches on" beneath it at y=255.
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.14)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, matching 1.5px borders.
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "widen the circle, not the switches".
- **Caption (12px `#444`, bottom right):** "scenario illustrative; switch counts exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the file, users, and denied-read scenario are invented and labeled illustrative; the bit values (r=4, w=2, x=1), the octal digits (640, 600, 644, 755, 777), the 8 lattice combos with digits 0–7, the c1 grant matrix `[[1,1,0],[1,0,0],[0,0,0]]`, and the switch counts per mode (2 / 3 / 4 / 7 / 9) are exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
