# FAT — The DOS Legacy

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** FAT — The DOS Legacy

**Subtitle:** A FAT disk keeps one big lookup table where entry N says which piece of a file comes after piece N — the whole filesystem is a game of follow-the-chain

## One Table That Maps Every File on the Stick

**Tags:** `core idea` (blue), `allocation table` (green), `DOS era` (orange)

- **The stick** — a coffee shop saves each day's orders to a USB stick formatted with FAT32
- **Clusters** — the stick is chopped into equal 4,096-byte clusters, the smallest unit a file can own
- **The directory** — a folder entry stores the name, the size, and only the first cluster number
- **The table** — the File Allocation Table has one entry per cluster; entry N names the next cluster
- **The chain** — reading a file means hopping: start cluster, look up next, jump, repeat until the end mark
- **8.3 names** — classic FAT allows 8 name chars + 3 extension chars, so DAILY_ORDERS.CSV shows as DAILY_~1.CSV

*Example (italic):* To read Monday's orders the laptop finds "DAILY_~1.CSV starts at cluster 5", then asks the table: after 5 comes 9, after 9 comes 12, and 12 is marked end-of-file.

**Key point:** FAT stores a file's layout not next to the file but in one shared table — the chain of "what comes next" entries IS the file's map, which is why the whole format is named after that table.

### Visualization (canvas `c1`, 720×300)

Diagram of one file lookup: a directory-entry box on top, a 12-cell FAT strip below, and hop arrows tracing the chain 5 → 9 → 12 → EOF.

- **Title (bold 15px, `#1a5276`, top center):** "One File, Three Hops: the Allocation Table Is the Map".
- **Directory box:** rounded box at x=40, y=60, 300×44, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` text "DAILY_~1.CSV · 10,000 B · first cluster 5".
- **FAT strip:** 12 squares 46px wide, 44px tall, side by side starting x=60 at y=170, 1px `#e5e9ef` borders; 11px `#6b7280` index labels "1".."12" centered under each square.
- **Cell contents (13px, centered):** cell 5 shows "9", cell 9 shows "12", cell 12 shows "EOF" — these three filled `rgba(0,131,0,0.15)` with `#008300` text; all other cells show "0" in `#6b7280` (free).
- **Arrows:** 3px `#2a78d6` arrow from the directory box down to cell 5, then arced arrows above the strip from cell 5 to cell 9 and cell 9 to cell 12.
- **Annotation (bold 13px green `#008300`, right side near y=130):** "entry N answers: what comes after cluster N?".
- **Caption (12px `#444`, bottom right):** "cluster numbers illustrative".

## Chasing the Chain for a 10,000-Byte File

**Tags:** `worked example` (blue), `hand-check` (green)

- **The file** — Monday's DAILY_~1.CSV is 10,000 bytes; each cluster holds 4,096 bytes
- **The count** — 10,000 / 4,096 = 2.44, and clusters are all-or-nothing, so the file needs 3 clusters
- **The chain** — the table reads FAT[5] = 9, FAT[9] = 12, FAT[12] = end-of-chain
- **The read** — cluster 5 holds bytes 1–4,096, cluster 9 holds 4,097–8,192, cluster 12 holds 8,193–10,000
- **The slack** — 3 clusters hold 12,288 bytes, so 2,288 bytes at the end of cluster 12 are wasted slack

*Example (italic):* Hand-check the last hop: 10,000 − 8,192 = 1,808 bytes land in cluster 12, leaving 4,096 − 1,808 = 2,288 bytes of slack.

**Key point:** A file always rounds up to whole clusters — the chain length is size ÷ cluster size rounded up, and the unused tail of the last cluster is dead space no other file can use.

### Visualization (canvas `c2`, 720×300)

Horizontal bar per cluster in the chain, drawn to a shared byte scale, showing two full clusters and one partial cluster with its slack tail.

- **Title (bold 15px, `#1a5276`, top center):** "10,000 Bytes Need 3 Clusters — and Waste 2,288 Bytes".
- **Rows (bars 26px tall at y = 95, 155, 215):** left-aligned 12px `#444` labels at x=25: "cluster 5", "cluster 9", "cluster 12"; bars start at x=130; full cluster = 480px = 4,096 bytes.
- **Cluster 5 bar:** fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, width 480, centered 12px `#2c3e50` label "bytes 1–4,096".
- **Cluster 9 bar:** same style, width 480, label "bytes 4,097–8,192".
- **Cluster 12 bar:** used part width 212 (= 1,808 bytes) in the same blue style, label "8,193–10,000"; slack part width 268 (= 2,288 bytes) fill `rgba(107,114,128,0.15)` with diagonal 1px `#6b7280` hatch lines and bold 12px `#d95926` label "slack 2,288 B".
- **Chain labels (12px `#6b7280`, above each bar):** "FAT[5] = 9", "FAT[9] = 12", "FAT[12] = EOC".
- **Annotation (bold 13px orange `#d95926`, near x=340, y=265):** "all-or-nothing clusters: the last tail is dead space".
- **Caption (12px `#444`, bottom right):** "file size illustrative; 4,096 B is a common FAT32 cluster size on small volumes".

## Why Every SD Card Still Speaks DOS

**Tags:** `where it's used` (blue), `limits` (orange), `interoperability` (green)

- **Everywhere** — FAT is so simple that cameras, car stereos, printers, and 3D printers all read it
- **Boot duty** — the PC boot partition (the EFI System Partition) is FAT by specification, on every OS
- **Growing table** — FAT12, FAT16, FAT32 name the bits per table entry: more bits, more clusters addressable
- **The 4 GB wall** — FAT32 stores file size in 32 bits, so a single file tops out at 4 GB minus 1 byte
- **The data hit** — a 6 GB CSV export onto a FAT32 stick fails at the 4 GB mark however empty the stick is

*Example (italic):* The coffee shop's year of order logs zips to 6 GB; the copy to a 64 GB FAT32 stick dies at 4 GB, and the fix is splitting the archive or reformatting to a newer filesystem.

**Key point:** FAT survives because every device agrees on it — but its DOS-era ceilings (8.3 names, the 4 GB file cap) still bite modern data work on SD cards and USB sticks.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of the largest thing each FAT variant can hold, ending with the 6 GB export that fits none of them.

- **Title (bold 15px, `#1a5276`, top center):** "The 4 GB Wall: a 6 GB Export Cannot Land on FAT32".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 430; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (bars 16px tall at y = 75, 125, 175, 225), each with a left-aligned 12px `#444` label at x=20:**
  - "FAT12 — max file ~32 MB (volume cap)": blue `#2a78d6` bar width 60
  - "FAT16 — max file 2 GB (volume cap)": blue bar width 200
  - "FAT32 — max file 4 GB − 1": blue bar width 280
  - "the 6 GB export": red `#e74c3c` bar width 420 with bold 12px white label "does not fit" right-aligned inside the bar end
- **Bar style:** FAT bars fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge; 11px `#444` size labels at bar ends.
- **Marker:** vertical dashed `#6b7280` (dash 4/3) line at the FAT32 bar end (x=530), 12px `#6b7280` label "4 GB cap" at its top.
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "the stick has room; the format does not".
- **Caption (12px `#444`, bottom right):** "bar widths schematic (log-feel); limits are the documented FAT ceilings".

## Yank the Stick Mid-Write and the Chain Breaks

**Tags:** `common mistake` (red), `power loss` (orange), `no journal` (blue)

- **Three writes** — saving a file writes the data clusters, then the FAT chain, then the directory entry
- **No journal** — FAT keeps no log of in-progress work, so a power cut keeps whichever writes finished
- **Orphans** — chain written but directory not: clusters marked used that no file name points to
- **Wrong size** — directory written first: a file that claims 10,000 bytes but chains into garbage
- **Two FATs** — FAT keeps two copies of the table, but that guards bad sectors, not half-done updates
- **The cleanup** — repair tools sweep orphaned chains into FILE0000.CHK files; names and structure are gone

*Example (italic):* The barista yanks the stick while Monday's 10,000-byte file saves; data and chain are on disk, but with no directory entry the laptop shows nothing — a disk check later coughs up FILE0000.CHK.

**Common mistake:** Treating "safely eject" as optional. A journaling filesystem replays or rolls back a half-done save; FAT has no journal, so the half-done state is simply what you keep.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the three writes of a save with a power cut after step 2 (orphaned chain) vs a clean eject after step 3 (readable file).

- **Title (bold 15px, `#1a5276`, top center):** "A Save Is Three Writes — Power Loss Keeps Whichever Finished".
- **Row 1 (y=95), label 12px `#444` at x=20:** "power cut"; blue `#2a78d6` rounded box at x=115 labeled "1. data clusters ✓" (12px), 3px arrow to a blue box at x=300 labeled "2. FAT chain ✓", then a dashed red `#e74c3c` arrow to a red box at x=485 labeled "3. directory ✗" with bold 12px red "orphaned chain — file invisible" beneath.
- **Row 2 (y=205), label:** "clean eject"; the same three boxes at x=115/300/485 all green `#008300` — "1. data clusters ✓", "2. FAT chain ✓", "3. directory ✓" — with bold 12px green "✓ file readable" beneath the last.
- **Box style:** 150–165px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "no journal means no undo — the half-save is permanent".
- **Caption (12px `#444`, bottom right):** "write order simplified".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the running example's file size (10,000 B), cluster chain (5 → 9 → 12 → EOF), and slack math (3 × 4,096 = 12,288; 12,288 − 10,000 = 2,288) are invented and labeled illustrative; the FAT facts are documented — 8.3 short names, 4,096 B as a common FAT32 cluster size, the FAT32 max file size of 4 GB − 1 byte, FAT16's 2 GB-class volumes, two FAT copies, and the absence of a journal.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
