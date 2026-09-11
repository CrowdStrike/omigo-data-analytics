# Ethernet & LAN File Shares

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Ethernet & LAN File Shares

**Subtitle:** Shared folders turned the office network into the transfer medium — drag a file onto a mapped drive and it lives on another machine

## Alice's Z: Drive Lives Down the Hall

**Tags:** `core idea` (blue), `shared folders` (green), `ethernet` (orange)

- **The running example** — Alice drags results.csv onto Z:\data; it lands on the office server
- **The illusion** — Z: looks like a plain local folder, yet every read and write crosses the wire
- **SMB** — the Windows dialect: "map network drive" gives the share a drive letter like Z:
- **NFS** — the Unix dialect: "mount" grafts the server's folder onto a path like /shared/data
- **One copy** — everyone opens the same file on the server; no sticks couriered desk to desk

*Example (italic):* Bob opens the same results.csv seconds later from /shared/data on his own machine — same file, nothing handed over.

**Key point:** A mapped drive or mounted path is a disguise: the folder behaves like any other to your programs, but the bytes live on another machine.

### Visualization (canvas `c1`, 720×300)

Schematic of one shared folder seen from two machines: Alice's PC and Bob's workstation connect through a switch to a file server; the file exists only on the server.

- **Title (bold 15px, `#1a5276`, top center):** "One File on the Server, Two 'Local' Folders".
- **Alice's PC (rounded 8px box, 168×56 at x=36, y=64):** blue fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border; line 1 bold 13px `#2a78d6` "Alice's PC", line 2 12px `#2c3e50` "sees Z:\data".
- **Bob's workstation (rounded 8px box, 168×56 at x=36, y=180):** aqua fill `rgba(25,158,112,0.12)`, 2px `#199e70` border; line 1 bold 13px `#199e70` "Bob's workstation", line 2 12px `#2c3e50` "sees /shared/data".
- **Switch (rounded 6px box, 92×40 centered at x≈344, y≈150):** grid fill `#e5e9ef`, 1.5px `#6b7280` border, bold 12px `#6b7280` label "switch".
- **File server (rounded 8px box, 186×92 at x=496, y=104):** violet fill `rgba(74,58,167,0.10)`, 2.5px `#4a3aa7` border; line 1 bold 13px `#4a3aa7` "file server"; below it a small document icon (26×32 white rect, 1.5px `#4a3aa7` border, folded corner) beside 12px `#2c3e50` "results.csv".
- **Ethernet lines (2.5px `#6b7280`):** Alice box right edge → switch left edge, Bob box right edge → switch left edge, switch right edge → server left edge; segment labels 11px `#6b7280`: "SMB" on Alice's line, "NFS" on Bob's line, "ethernet" under the switch–server line.
- **Annotation (bold 13px orange `#d95926`, centered near y=282):** "'local' folder, remote bytes — every open and save crosses the wire".

## Copying 2 GB: Climbing the Ethernet Speed Ladder

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The formula** — copy time = size ÷ speed; ethernet quotes bits per second, so divide by 8 first
- **10 Mbps** — 1980s–90s ethernet ≈ 1.25 MB/s; 2,000 MB ÷ 1.25 = 1,600 s ≈ 27 min
- **100 Mbps** — "Fast Ethernet" ≈ 12.5 MB/s; 2,000 ÷ 12.5 = 160 s ≈ 2.7 min
- **1 Gbps** — today's common office port ≈ 125 MB/s; 2,000 ÷ 125 = 16 s
- **10 Gbps** — server rooms ≈ 1,250 MB/s; 2,000 ÷ 1,250 = 1.6 s
- **Ceiling, not promise** — protocol overhead and the server's disk keep real copies below line rate

*Example (italic):* The 2 GB dataset that once cost Alice a 27-minute coffee break now lands on the share before she can switch windows.

**Key point:** Turn Mbps into MB/s by dividing by 8, then divide the file size — the whole speed ladder is two divisions you can redo on paper.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of time to copy 2 GB (2,000 MB) at each ethernet generation, on a log-scale seconds axis so the 1,000× spread stays visible.

- **Title (bold 15px, `#1a5276`, top center):** "Copying a 2 GB Dataset at Each Ethernet Generation".
- **Axis:** log10(seconds) 0–3.4 mapped to px x=215 (1 s) through x=670; baseline 1px `#999` at y=248; vertical gridlines `#e5e9ef` with 12px `#444` tick labels at 1 s / 10 s / 100 s / 1,000 s; axis caption "seconds — log scale" 12px `#444` centered under the baseline.
- **Rows (bars 30px tall from x=215, left-aligned 12px `#444` labels at x=20; each bar tinted fill + 2px solid border + bold 13px value label in the bar color just right of the bar end):**
  - y=72: "10 Mbps — 1.25 MB/s", blue `rgba(42,120,214,0.30)` / `#2a78d6`, length log10(1600)=3.204, label "1,600 s ≈ 27 min" (drawn inside the bar's right end, white-on-blue if outside would clip)
  - y=116: "100 Mbps — 12.5 MB/s", aqua `rgba(25,158,112,0.30)` / `#199e70`, length log10(160)=2.204, label "160 s ≈ 2.7 min"
  - y=160: "1 Gbps — 125 MB/s", violet `rgba(74,58,167,0.30)` / `#4a3aa7`, length log10(16)=1.204, label "16 s"
  - y=204: "10 Gbps — 1,250 MB/s", green `rgba(0,131,0,0.30)` / `#008300`, length log10(1.6)=0.204, label "1.6 s"
- **Annotation (bold 13px orange `#d95926`, near x=445, y=52):** "each rung is 10× — 27 minutes becomes 1.6 seconds".
- **Caption (12px `#6b7280`, bottom right near y=292):** "time = 2,000 MB ÷ line rate — a ceiling; real copies run slower".

## Why Your "Local" Read Is Slow: The Data Scientist's Mount

**Tags:** `where it's used` (blue), `performance` (orange)

- **Network home dirs** — on shared clusters your home folder is often an NFS mount, not a local disk
- **The symptom** — pd.read_csv on the same file: ~1 s from local SSD, ~18 s from the 1 Gbps NAS
- **Small files hurt most** — every open is a network round trip; a million tiny files, a million trips
- **The office NAS** — the modern shared folder: a disk box on the LAN speaking SMB and NFS
- **The fix** — copy the dataset to local scratch once, work there, write results back at the end

*Example (italic):* Alice's notebook "hangs" on a 2 GB CSV — the file sits on the team NAS and is arriving at wire speed, not SSD speed.

**Key point:** When file reads are mysteriously slow, ask where the folder really lives — the mount table or the drive letter will tell you.

### Visualization (canvas `c3`, 720×300)

Two horizontal bars comparing the time for an identical pandas read of a 2 GB CSV from a local SSD vs from a NAS share over 1 Gbps ethernet.

- **Title (bold 15px, `#1a5276`, top center):** "Same pd.read_csv, Two Homes for the File (illustrative)".
- **Axis:** seconds 0–20 mapped to px x=225 (0 s) through x=670 (20 s); 1px `#999` baseline at y=230 with 12px `#444` ticks at 0 / 5 / 10 / 15 / 20 s; axis caption "seconds to read a 2 GB CSV" 12px `#444` centered under the ticks.
- **Rows (bars 40px tall, left-aligned 12px `#444` two-line labels at x=20):**
  - y=92: "local SSD — ~2,000 MB/s", green `rgba(0,131,0,0.30)` bar for 1 s, 2px `#008300` border, bold 13px `#008300` value label "~1 s" right of the bar end
  - y=162: "NAS over 1 Gbps — ~110 MB/s", orange `rgba(217,89,38,0.30)` bar for 18 s, 2px `#d95926` border, bold 13px `#d95926` value label "~18 s" right of the bar end
- **Gap marker:** dashed 1.5px `#6b7280` vertical guide at the 1 s position spanning both rows, with bold 13px `#d95926` annotation near x=430, y=140: "identical code, ~18× slower — the path hid the network".
- **Caption (12px `#6b7280`, bottom right near y=290):** "speeds typical, illustrative; effective NAS rate below the 125 MB/s line rate".

## The Confusion: Same Gigabytes, Very Different Minutes

**Tags:** `common mistake` (red), `best practice` (green)

- **The mistake** — assuming only size matters; on a share, the file count matters just as much
- **Per-file toll** — each file costs round trips to open, check, and close before any data moves
- **One 2 GB archive** — the toll is paid once; the wire streams the rest at full speed: 16 s
- **20,000 × 100 KB files** — the toll is paid 20,000 times: ~100 s of overhead on 16 s of data
- **The tell** — the progress bar crawls on a folder of tiny files but flies on one zip of them

*Example (italic):* Alice zips the 20,000-image folder into one archive first; the copy that crawled for two minutes finishes in sixteen seconds.

**Key point:** On a network share, bundle small files into one archive before copying — you pay the per-file toll once instead of thousands of times.

### Visualization (canvas `c4`, 720×300)

Two stacked horizontal bars: the same 2 GB moved over 1 Gbps as one archive vs as 20,000 small files, splitting each bar into wire time and per-file overhead.

- **Title (bold 15px, `#1a5276`, top center):** "The Same 2 GB Over 1 Gbps: One File vs 20,000 Files (illustrative)".
- **Axis:** seconds 0–120 mapped to px x=225 (0 s) through x=670 (120 s); 1px `#999` baseline at y=222 with 12px `#444` ticks at 0 / 30 / 60 / 90 / 120 s; axis caption "seconds" 12px `#444`.
- **Rows (bars 40px tall, left-aligned 12px `#444` two-line labels at x=20):**
  - y=88: "one 2 GB archive" — blue segment `rgba(42,120,214,0.35)` with 2px `#2a78d6` border for 16 s; bold 13px `#2a78d6` value label "16 s" right of the bar
  - y=158: "20,000 × 100 KB files" — blue segment for 16 s (bytes), then orange segment `rgba(217,89,38,0.35)` with 2px `#d95926` border for 100 s (overhead), ending at 116 s; bold 13px `#d95926` value label "116 s ≈ 2 min" right of the bar
- **Legend (12px, swatches 12×12, top right area near x=470, y=54 and y=72):** blue swatch "bytes on the wire", orange swatch "per-file round trips (~5 ms × 20,000)".
- **Annotation (bold 13px magenta `#d55181`, centered near x=447, y=250):** "the toll booth, not the road, sets the time".
- **Caption (12px `#6b7280`, bottom center near y=290):** "overhead ~5 ms per file — illustrative; real per-file cost varies with protocol and latency".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" or "Common mistake:").
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` appears only in the key-point border CSS, not in any chart.
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Documented facts: ethernet generations 10 Mbps / 100 Mbps / 1 Gbps / 10 Gbps; SMB is the Windows share protocol, NFS the Unix mount protocol; 8 bits per byte. Invented/typical figures (sustained MB/s, NAS read times, ~5 ms per-file overhead) are labeled illustrative in each chart's caption or title. Arithmetic that must stay consistent between text and charts: 2,000 ÷ 1.25 = 1,600 s ≈ 27 min; 2,000 ÷ 12.5 = 160 s ≈ 2.7 min; 2,000 ÷ 125 = 16 s; 2,000 ÷ 1,250 = 1.6 s; 2,000 ÷ 110 ≈ 18 s; 20,000 × 5 ms = 100 s; 16 + 100 = 116 s ≈ 2 min; 20,000 × 100 KB = 2 GB.
- This page has no links.
