# NTFS

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** NTFS

**Subtitle:** Windows' file system keeps one master table where every file is a row — with a journal that survives power cuts and a permission list on every file

## One Table That Knows Every File

**Tags:** `core idea` (blue), `MFT` (green), `since 1993` (orange)

- **The shop PC** — a coffee shop's back-office computer keeps recipes, payroll, and sales on one drive
- **The table** — NTFS keeps a Master File Table (MFT): one record per file, like a card catalog
- **Everything is a file** — the table itself ($MFT) and the journal ($LogFile) are records in the table
- **The record** — each record holds the name, timestamps, permissions, and where the bytes live
- **The lookup** — opening sales-2026.csv means finding its MFT record, then following it to the data

*Example (italic):* When the owner opens tip-policy.txt, Windows reads MFT record 41 and finds the entire note already sitting inside that record.

**Key point:** NTFS — the file system Windows has used since Windows NT shipped in 1993 — organizes a drive as one Master File Table where every file, including the system's own bookkeeping, is a record.

### Visualization (canvas `c1`, 720×300)

Schematic of the MFT as a table of records on the left, with one record's arrow pointing to a strip of data clusters on the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Master File Table: Every File Is a Row".
- **MFT column:** five rounded boxes 270px wide, 32px tall, x=60, at y = 62, 102, 142, 182, 222; 12px `#2c3e50` labels inside: "record 0 — $MFT (the table itself)", "record 2 — $LogFile (the journal)", "record 41 — tip-policy.txt (data inside)", "record 42 — sales-2026.csv (pointer)", "record 43 — payroll.xlsx (ACL attached)".
- **Box fills:** system records 0 and 2 violet `rgba(74,58,167,0.12)` with 2px `#4a3aa7` border; record 41 green `rgba(0,131,0,0.12)` / `#008300`; records 42–43 blue `rgba(42,120,214,0.15)` / `#2a78d6`.
- **Cluster strip:** 8 adjacent cells 30px wide, 26px tall starting at x=430, y=185, fill `rgba(42,120,214,0.30)`, 1px `#2a78d6` borders; 12px `#444` label above at y=175: "data clusters 10,000–10,511".
- **Arrow:** 3px `#2a78d6` line with arrowhead from the right edge of record 42 (x=330, y=198) to the strip's left edge (x=430, y=198).
- **Annotation (bold 13px violet `#4a3aa7`, x=380, y=70):** "even the table and the journal are files in the table".
- **Caption (12px `#444`, bottom right):** "record and cluster numbers illustrative".

## A 600-Byte Note vs a 2 MB Spreadsheet

**Tags:** `worked example` (blue), `resident data` (green), `clusters` (orange)

- **Record size** — an MFT record is 1,024 bytes; about 56 go to the header, the rest to attributes
- **Resident** — the 600-byte tip-policy.txt fits inside its own record: zero extra disk reads
- **Non-resident** — the 2 MB sales-2026.csv cannot fit, so its record stores a map called a data run
- **The math** — 2,097,152 bytes ÷ 4,096-byte clusters = 512 clusters: one run, start 10,000, length 512
- **Hand-check** — 512 × 4,096 = 2,097,152, exactly the file's size; one run means zero fragmentation

*Example (italic):* Reading the note costs one MFT lookup; reading the CSV costs the same lookup plus 512 clusters starting at cluster 10,000.

**Key point:** A file's MFT record either contains the data itself (small, resident) or a compact map of cluster runs (large, non-resident) — the 1,024-byte record is the fork in the road.

### Visualization (canvas `c2`, 720×300)

Two-row schematic of one MFT record each: the resident note's bytes living inside the record, and the CSV record pointing out to a 512-cluster run.

- **Title (bold 15px, `#1a5276`, top center):** "Resident vs Non-Resident: Where the Bytes Actually Live".
- **Scale:** each record drawn as a 300px-wide, 36px-tall box at x=180 representing 1,024 bytes (0.293 px per byte).
- **Row 1 (y=85), label 12px `#444` at x=20:** "tip-policy.txt (600 B)"; record box outlined 2px `#2a78d6`; inside, left-to-right segments: header 56 B ≈ 16px fill `rgba(107,114,128,0.35)`, name/attributes ≈ 108px fill `rgba(42,120,214,0.20)`, data 600 B ≈ 176px fill `rgba(0,131,0,0.30)` with bold 12px `#008300` label "600-byte note fits inside".
- **Row 2 (y=195), label:** "sales-2026.csv (2 MB)"; same record box; data segment replaced by a thin 40px `rgba(217,89,38,0.30)` stub labeled 11px `#d95926` "data run"; 3px `#d95926` arrow from the stub to a cluster strip at x=540, y=195 (6 cells, 24px each, fill `rgba(42,120,214,0.30)`) with 12px `#444` label below: "start 10,000 · length 512 · 512 × 4,096 B = 2,097,152 B".
- **Annotation (bold 13px green `#008300`, x=180, y=145):** "small files never touch the disk's data area".
- **Caption (12px `#444`, bottom right):** "layout schematic; sizes exact: 1,024 B record, 600 B note, 512 clusters".

## Access Denied: Who May Open payroll.xlsx

**Tags:** `where it's used` (blue), `ACLs` (orange), `access denied` (red)

- **The lock** — payroll.xlsx carries an ACL: a list of allow and deny entries naming who can do what
- **Deny first** — NTFS checks deny entries before allow entries, so one deny to Baristas overrides an allow
- **The script** — the nightly job's account may read payroll.xlsx, but its write-back fails: "Access denied"
- **Case blindness** — on Windows Sales.CSV and sales.csv are one file; a Linux pipeline may expect two
- **The tag-along** — a downloaded CSV carries a hidden Zone.Identifier stream marking it as from the web

*Example (italic):* Jo the barista double-clicks payroll.xlsx and gets Access denied; Maria the owner opens it fine — same file, different ACL verdicts.

**Key point:** Permissions live on the file, not on whoever wrote the code — a data job succeeds or fails by the ACL verdict for the account it runs as, the top cause of works-on-my-machine file errors.

### Visualization (canvas `c3`, 720×300)

Flow diagram: three accounts request the same file, pass through its ACL, and get three verdicts.

- **Title (bold 15px, `#1a5276`, top center):** "One File, Three Verdicts: the ACL Decides".
- **Account boxes (left, x=40, 130px wide, 34px tall) at y = 85, 155, 225:** "Maria (owner)", "Jo (Baristas)", "nightly-job", fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` text.
- **ACL box (center, x=270, 180px wide, 120px tall, centered at y=155):** outlined 2px `#1a5276`, 12px `#2c3e50` header "payroll.xlsx ACL", three 11px lines: "deny  Baristas  read", "allow  Maria  full control", "allow  nightly-job  read".
- **Arrows:** 3px `#6b7280` lines from each account box to the ACL box's left edge, and from its right edge to each verdict box.
- **Verdict boxes (right, x=540, 150px wide, 34px tall) at y = 85, 155, 225:** "✓ full control" fill `rgba(0,131,0,0.12)` / `#008300`; "✗ Access denied" fill `rgba(231,76,60,0.12)` / `#e74c3c` bold; "✓ read-only" fill `rgba(0,131,0,0.12)` / `#008300`.
- **Annotation (bold 13px orange `#d95926`, centered near y=278):** "deny entries are checked before allow — one deny wins".
- **Caption (12px `#444`, bottom right):** "accounts illustrative; deny-before-allow is documented NTFS behavior".

## The Journal Is Not a Backup

**Tags:** `common mistake` (red), `journaling` (orange), `metadata only` (blue)

- **The journal** — before touching metadata, NTFS logs its intent to $LogFile, acts, then marks done
- **The crash** — power dies at 2:03pm during a rename; on reboot the log replays and the name is consistent
- **The speed** — replaying the journal takes under a second; pre-journal systems rescanned the whole disk
- **The limit** — only metadata is journaled: names, sizes, timestamps, the MFT — not your file's rows
- **The torn file** — a 40,000-row CSV interrupted mid-write can come back with 18,240 rows, no warning

*Example (italic):* After the 2:03pm power cut the drive mounts clean in under a second, yet sales-2026.csv holds only 18,240 of its 40,000 rows.

**Common mistake:** Trusting journaling as data protection. The $LogFile guarantees the filing structure survives a crash — the contents of a half-written file are your job (write-temp-then-rename, and backups).

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a crash during a metadata rename (journal replays, consistent) vs a crash during a content write (torn file, journal silent).

- **Title (bold 15px, `#1a5276`, top center):** "Power Cut at 2:03pm: What the $LogFile Saves".
- **Crash marker:** vertical dashed `#e74c3c` (dash 4/3) line at x=360 from y=55 to y=255, bold 12px `#e74c3c` label "power cut" at its top.
- **Row 1 (y=95), label 12px `#444` at x=20:** "metadata: a rename"; blue `rgba(42,120,214,0.15)` rounded box at x=130 (200px wide) labeled 12px "log intent: sales.tmp → sales-2026.csv", 3px arrow across the crash line to a green `rgba(0,131,0,0.12)` box at x=440 (240px wide) labeled "reboot: replay log — name consistent in < 1 s" with bold 12px `#008300` "✓".
- **Row 2 (y=205), label:** "contents: 40,000 rows"; blue box at x=130 labeled "writing rows 1 … 40,000", 3px arrow across the crash line to a red `rgba(231,76,60,0.12)` box at x=440 labeled "18,240 rows on disk — torn file" with bold 12px `#e74c3c` "✗ journal does not replay data".
- **Box style:** 40px tall, 8px radius, 12px `#2c3e50` text, 2px borders matching each fill's hue.
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "the journal guards the filing system, not the file's contents".
- **Caption (12px `#444`, bottom right):** "row counts illustrative; metadata-only journaling is documented NTFS design".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); MFT record numbers (0, 2, 41–43), cluster start 10,000, and the 40,000 / 18,240 row counts are invented and labeled illustrative; the 1,024 B record size, 4,096 B default cluster, 2,097,152 ÷ 4,096 = 512 arithmetic, deny-before-allow ACE order, metadata-only journaling, and the 1993 Windows NT ship date are documented public facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
