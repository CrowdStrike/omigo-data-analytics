# APFS

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** APFS

**Subtitle:** Apple's copy-on-write filesystem (2017) never copies data it can share — a duplicate is a second name pointing at the same blocks, and new blocks are written only when something changes

## Duplicating 40 GB in the Blink of an Eye

**Tags:** `core idea` (blue), `copy-on-write` (green), `instant clones` (orange)

- **The project** — a video editor's Mac holds a 40 GB project folder for a client film
- **The duplicate** — she duplicates it to try a risky recut; the copy appears in under a second
- **The trick** — APFS writes no data: the "copy" is a second name pointing at the same disk blocks
- **The change** — when she trims one clip, only the changed blocks are written fresh to disk
- **The name** — this is copy-on-write: share everything, copy only at the moment of change

*Example (italic):* On the pre-2017 filesystem a 40 GB duplicate would grind for minutes; the APFS clone is instant and costs roughly zero new bytes.

**Key point:** APFS is a copy-on-write filesystem — duplicating a file clones references to the existing blocks, and real copying happens only when a block is modified.

### Visualization (canvas `c1`, 720×300)

Two-row block diagram: an old-style full copy writes every block twice; an APFS clone adds a second name pointing at the same row of blocks.

- **Title (bold 15px, `#1a5276`, top center):** "One Duplicate, Two Ways: Full Copy vs APFS Clone".
- **Row 1 (full copy), label 12px `#444` at x=20, y=68:** "full copy (old way)"; ten blue `#2a78d6` blocks (fill `rgba(42,120,214,0.30)`, 40px wide, 26px tall, 4px gap) starting at x=170, y=55; directly below at y=88 ten orange `#d95926` blocks (fill `rgba(217,89,38,0.25)`) — the duplicate written again; right-side bold 12px `#2c3e50` label "80 GB on disk" at x=630, y=80.
- **Row 2 (APFS clone), label at x=20, y=215:** "APFS clone"; two small name boxes at y=160 (110px wide, 26px tall, 8px radius, fill `rgba(26,82,118,0.10)`, 12px text): "Project" at x=200 and "Project copy" at x=380, each with a 2px `#6b7280` arrow down to a single shared row of ten blue blocks at x=170, y=210; right-side bold 12px label "40 GB on disk" at x=630, y=222.
- **Annotation (bold 13px green `#008300`, near x=380, y=270):** "the clone costs 0 new bytes".
- **Caption (12px `#444`, bottom right):** "each square = 4 GB of blocks, illustrative".

## Counting the Blocks After an Edit

**Tags:** `worked example` (blue), `block accounting` (green)

- **The blocks** — call the project 10,000 blocks of 4 MB each: 10,000 × 4 MB = 40 GB (illustrative)
- **The clone** — right after duplicating, both names share all 10,000 blocks: disk usage stays 40 GB
- **The edit** — the recut rewrites 500 blocks; APFS writes 500 fresh 4 MB blocks = 2 GB of new data
- **The total** — 9,500 shared + 2 × 500 unique blocks = 42 GB on disk, not a full copy's 80 GB
- **The rule** — extra disk cost equals exactly what you changed, never what you duplicated

*Example (italic):* Editing 5% of the clone (500 of 10,000 blocks) costs 2 GB; the untouched 95% is never copied at all.

**Key point:** Clone cost is hand-checkable: new usage = original + (blocks changed × block size) — here 40 GB + 500 × 4 MB = 42 GB.

### Visualization (canvas `c2`, 720×300)

Line chart of disk usage as more of the clone gets edited: the APFS line climbs from 40 GB toward 80 GB with edits, while the full-copy line sits at 80 GB from second one.

- **Title (bold 15px, `#1a5276`, top center):** "Disk Usage Grows Only With What You Change".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = share of clone edited, tick labels "0%"–"100%" every 25% (12px `#444`); y = GB on disk 0 to 90, gridlines `#e5e9ef` at 20/40/60/80 with 12px labels.
- **Full-copy line:** orange `#d95926` dashed (dash 6/4) 3px line, flat through percents `[0, 25, 50, 75, 100]`, GB `[80, 80, 80, 80, 80]`; 12px orange label "full copy: 80 GB from the start" above its left end.
- **APFS line:** green `#008300` 3px line through percents `[0, 5, 25, 50, 75, 100]`, GB `[40, 42, 50, 60, 70, 80]`.
- **Marker:** filled green 5px-radius dot at (5%, 42 GB) with bold 12px green callout "today's edit: 5% changed → 42 GB" beside it.
- **Annotation (bold 13px violet `#4a3aa7`, near x=60%, y=110):** "the lines only meet if you rewrite everything".
- **Caption (12px `#444`, bottom right):** "10,000 blocks × 4 MB, illustrative".

## Snapshots, Space Sharing, and Safe Crashes

**Tags:** `where it's used` (blue), `snapshots` (green), `crash safety` (orange)

- **Snapshots** — a snapshot freezes the disk's block map in place, so it too costs only future changes
- **Time Machine** — macOS keeps hourly local snapshots this way; a quiet hour costs almost nothing
- **Space sharing** — volumes in one APFS container draw on a shared pool instead of fixed partitions
- **Crash safety** — copy-on-write never overwrites live metadata, so a power cut can't half-write it
- **Data work** — the same trick lets an engineer clone a huge dataset for an experiment in a second

*Example (italic):* Four hourly snapshots of the editor's afternoon cost 0.5 + 1.2 + 0.8 + 2.0 = 4.5 GB — not 4 × 40 GB = 160 GB of full copies.

**Key point:** Once blocks are shared instead of copied, cheap snapshots, flexible volumes, and crash-safe writes all fall out of the same copy-on-write design.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart across four hours: what each hourly backup would cost as a full copy (40 GB every time) vs as an APFS snapshot (only the blocks that changed that hour).

- **Title (bold 15px, `#1a5276`, top center):** "Hourly Backups: Full Copies vs APFS Snapshots".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = four hour groups labeled `["10am", "11am", "12pm", "1pm"]` (12px `#444`), group centers at x = 135, 275, 415, 555; y = GB 0 to 40, gridlines `#e5e9ef` at 10/20/30/40.
- **Full-copy bars:** blue `#2a78d6` fill `rgba(42,120,214,0.30)`, 44px wide, left of each group center, heights for GB `[40, 40, 40, 40]` (full plot height), 11px blue value labels "40" on the bars.
- **Snapshot bars:** solid green `#008300`, 44px wide, right of each group center, heights for GB `[0.5, 1.2, 0.8, 2.0]` (2–9px tall), bold 11px green value labels "0.5", "1.2", "0.8", "2.0" just above each bar.
- **Annotation (bold 13px green `#008300`, near x=300, y=80):** "4 snapshots: 4.5 GB, not 160 GB".
- **Caption (12px `#444`, bottom right):** "hourly change sizes illustrative".

## A Clone Is Not a Backup

**Tags:** `common mistake` (red), `shared fate` (orange)

- **The trap** — a clone shares its blocks with the original on the same disk; it is not a second copy
- **Disk dies** — if the drive fails, the project and its instant "copy" vanish in the same moment
- **Deleting** — trashing the 40 GB clone frees only ~2 GB: just the blocks it had stopped sharing
- **Finder math** — two files can "sum" to 80 GB while the disk shows 42 GB used; sizes stop adding up
- **Real backup** — a true backup writes every block to a different disk; slow again, and worth it

*Example (italic):* The editor's two 40 GB project versions appear to total 80 GB, yet the disk shows 42 GB used — and one failed drive would take both at once.

**Common mistake:** Treating an instant duplicate as a backup. A clone protects against bad edits, not bad disks — shared blocks mean shared fate.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a clone kept on the same disk dies with it; a real backup on a second disk survives the failure.

- **Title (bold 15px, `#1a5276`, top center):** "Shared Blocks Mean Shared Fate".
- **Row 1 (y=95), label 12px `#444` at x=20:** "clone, same disk"; blue `#2a78d6` rounded box at x=170 labeled "Disk A: project + clone (shared blocks)" (12px), 3px arrow to a red `#e74c3c` box at x=440 labeled "Disk A fails" with bold 12px red "✗ both versions gone".
- **Row 2 (y=205), label:** "backup, second disk"; blue box at x=170 labeled "Disk A: project", 3px arrow to a green `#008300` box at x=360 labeled "Disk B: full copy", then arrow to a green box at x=550 labeled "Disk A fails — restore" with bold 12px green "✓".
- **Box style:** 150–210px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the clone saves you from bad edits, not from bad disks".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the project size (40 GB), block accounting (10,000 blocks × 4 MB, 500 edited → 42 GB), usage curve `[40, 42, 50, 60, 70, 80]` GB, and hourly snapshot deltas `[0.5, 1.2, 0.8, 2.0]` GB are invented and labeled illustrative; APFS facts (2017 release, copy-on-write, instant clones, snapshots, container space sharing) are documented Apple platform behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
