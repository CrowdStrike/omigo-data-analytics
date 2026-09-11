# SMB & NFS

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SMB & NFS

**Subtitle:** SMB and NFS make a folder on another machine look like a local folder — every file operation quietly becomes a message across the network

## The Shared Drive That Lives on Another Machine

**Tags:** `core idea` (blue), `network mount` (green), `SMB vs NFS` (orange)

- **The team** — three analysts at a coffee-shop chain all open the same `orders.csv` from a shared drive
- **The trick** — the file lives on one file server; each laptop mounts the folder so it looks local
- **SMB** — the protocol behind Windows shared drives (`Z:\shared\`), born in the Windows/IBM world in the 1980s
- **NFS** — Sun Microsystems' 1984 answer for Unix; mounts appear as ordinary paths like `/mnt/shared`
- **The translation** — every open, read, and write the app makes is turned into a network request and reply

*Example (italic):* An analyst saves `orders.csv` at 9:00am; a colleague on another laptop opens the same path a minute later and sees the update — there is only one real copy, on the server.

**Key point:** SMB and NFS are network filesystems: the folder is an illusion drawn by the operating system, and behind every file operation is a round-trip to a server that holds the only real copy.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three laptops on the left, each with a mount path, arrows converging across a "network" band to one file server box holding `orders.csv`.

- **Title (bold 15px, `#1a5276`, top center):** "One Real Copy: Three Laptops, One File Server".
- **Laptop boxes (left column, x=30, y = 70, 145, 220):** rounded boxes 170px wide, 44px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; 12px `#2c3e50` labels "laptop A — Z:\\shared (SMB)", "laptop B — Z:\\shared (SMB)", "laptop C — /mnt/shared (NFS)".
- **Network band:** vertical light band x=280 to x=420, fill `rgba(107,114,128,0.08)`, 12px `#6b7280` label "network" rotated or horizontal at top of band (y=60); three 3px `#199e70` arrows from each laptop box across the band to the server box.
- **Server box (right, x=470, y=115):** 200px wide, 70px tall, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 13px `#008300` label "file server", 12px `#2c3e50` sublabel "orders.csv (the only copy)".
- **Arrow labels (11px `#6b7280`, above each arrow):** "open / read / write as messages".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "the folder is an illusion — every file call is a network round-trip".
- **Caption (12px `#444`, bottom right):** "setup schematic".

## Counting the Round-Trips to List One Folder

**Tags:** `worked example` (blue), `round-trips` (green)

- **The task** — an analyst runs a detailed listing (`ls -l`) on a shared folder holding 200 daily order files
- **The cost** — the client asks the server for attributes of each file: ~1 directory read + 200 attribute calls
- **Local disk** — 200 lookups at ~0.02 ms each finish in about 4 ms; no network is involved
- **Office LAN** — 201 round-trips at 0.5 ms each take about 100 ms; noticeable but fine
- **Over VPN** — 201 round-trips at 20 ms each take about 4,020 ms; the same listing now takes ~4 seconds
- **Hand-check** — 200 × 20 ms = 4,000 ms; latency per call, not bandwidth, is what you are paying

*Example (italic):* The identical folder listing takes 4 ms on a local disk, ~100 ms on the office LAN, and ~4 seconds from home over a 20 ms VPN — same files, same command, 1,000× slower.

**Key point:** Network filesystems pay one round-trip per operation, so an action that fires hundreds of small calls is multiplied by network latency — distance to the server, not disk speed, sets the pace.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: time to list a 200-file folder on local disk vs office LAN (SMB/NFS) vs VPN, with the per-call arithmetic written on each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Listing 200 Files: Same Command, Three Distances".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 85, 150, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "local disk — 200 × 0.02 ms": green `#008300` bar width 40, 12px `#008300` value label "≈ 4 ms" at bar end
  - "office LAN — 201 × 0.5 ms": blue `#2a78d6` bar width 180, 12px `#2a78d6` value label "≈ 100 ms"
  - "VPN — 201 × 20 ms": orange `#d95926` bar width 440, bold 12px `#d95926` value label "≈ 4,020 ms (~4 s)"
- **Bar style:** 22px tall, fills at 0.30 alpha of each color with solid 2px border in the same color.
- **Annotation (bold 13px magenta `#d55181`, right side near y=45):** "latency × call count — bandwidth never mattered here".
- **Caption (12px `#444`, bottom right):** "per-call latencies illustrative; multiplication exact; bar widths log-feel schematic".

## Where a Data Scientist Trips Over the Mount

**Tags:** `where it's used` (blue), `small files` (green), `data loading` (orange)

- **Invisible mounts** — `read_csv("/mnt/shared/orders.csv")` looks local; the path hides the network
- **Training data** — jobs that stream thousands of small files from a mount crawl for the same reason
- **The math** — one 400 MB file at 100 MB/s moves in ~4 s plus a handful of round-trips
- **The trap** — the same 400 MB as 10,000 × 40 KB files adds ~3 round-trips per file (open, read, close)
- **The bill** — 10,000 × 1.5 ms of per-file overhead ≈ 15 s on top of the 4 s of transfer: ~19 s total
- **The fix** — bundle small files (one parquet, a tar, a zip) so bytes flow in few large requests

*Example (italic):* The nightly job reads 400 MB of orders either way — as one file it takes ~4 seconds, as 10,000 tiny files on the same mount it takes ~19 seconds.

**Key point:** On a network filesystem, per-file overhead dominates small files; the standard cure is fewer, bigger files so the mount spends its time moving bytes instead of exchanging messages.

### Visualization (canvas `c3`, 720×300)

Stacked horizontal bar chart: total time to read 400 MB over the mount as one big file vs 10,000 small files, split into transfer time and per-file round-trip overhead.

- **Title (bold 15px, `#1a5276`, top center):** "Same 400 MB, Two Shapes: One File vs 10,000 Small Files".
- **Axis:** horizontal 2px `#999` baseline at x=200, bars extend right, max width 460; x scale 0–20 s, 12px `#444` tick labels at 0/5/10/15/20 s along y=255, gridlines `#e5e9ef` vertical at each tick.
- **Rows (bar height 34px):**
  - Row 1 (y=95), label 12px `#444` at x=20 "one 400 MB file": blue `#2a78d6` transfer segment width 92 (4 s), aqua `#199e70` overhead segment width 2 (~0.1 s); 12px `#2c3e50` label "≈ 4 s" at bar end
  - Row 2 (y=175), label "10,000 × 40 KB files": blue transfer segment width 92 (4 s, same bytes), orange `#d95926` overhead segment width 345 (15 s of open/read/close round-trips); bold 12px `#d95926` label "≈ 19 s" at bar end
- **Segment fills:** transfer `rgba(42,120,214,0.30)` with 2px `#2a78d6` border; overhead solid-ish `rgba(217,89,38,0.45)` with 2px `#d95926` border; 11px in-bar labels "transfer" and "overhead".
- **Legend (12px, y=60, right side):** blue swatch "moving bytes", orange swatch "per-file round-trips".
- **Annotation (bold 13px violet `#4a3aa7`, near x=330, y=140):** "same bytes — 15 s of pure chit-chat".
- **Caption (12px `#444`, bottom right):** "100 MB/s link, 0.5 ms LAN round-trips, illustrative".

## A Mounted Folder Is Not a Local Disk

**Tags:** `common mistake` (red), `caching & locks` (orange)

- **The confusion** — because the path looks local, people assume local-disk timing, locking, and consistency
- **Caching** — clients cache reads; NFS only guarantees you see others' writes on a fresh open (close-to-open)
- **Stale reads** — laptop B can briefly show yesterday's `orders.csv` after laptop A saved a new one
- **Locks differ** — NFS locks are advisory (only cooperating apps honor them); SMB leans on server-side locks and leases
- **Two writers** — two machines appending to one shared log can interleave or overwrite each other's rows
- **The habit** — write to a temp name, then rename; or give each writer its own file and merge later

*Example (italic):* Two laptops both append the 9:05am order to the shared log; each cached its own view, and the merged file ends up with one row missing and one duplicated.

**Common mistake:** Treating the mount as a local disk. Local rules — instant visibility of writes, cheap tiny operations, safe concurrent appends — quietly weaken over SMB/NFS; design for one writer per file or explicit locking.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: two clients appending to one shared file (rows collide) vs each client writing its own file with a merge step (safe).

- **Title (bold 15px, `#1a5276`, top center):** "Two Writers, One Shared File: Why Appends Collide".
- **Row 1 (y=95), label 12px `#444` at x=20:** "shared append"; two blue `#2a78d6` rounded boxes stacked at x=150 (y=70 and y=112, 130px wide, 34px tall) labeled "laptop A: +row 9:05" and "laptop B: +row 9:05"; two 3px arrows converging to a red `#e74c3c` box at x=420 (y=88, 190px wide, 44px tall) labeled "log.csv — row lost / doubled" with bold 12px red "✗ silent corruption" at its right.
- **Row 2 (y=225), label:** "one file per writer"; two blue boxes at x=150 (y=200 and y=242) labeled "A → log_A.csv" and "B → log_B.csv"; arrows to a green `#008300` box at x=420 (y=218, 190px wide, 44px tall) labeled "merge job → log.csv" with bold 12px green "✓ every row kept".
- **Box style:** 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Divider:** 1px `#e5e9ef` horizontal line at y=165 separating the rows.
- **Annotation (bold 13px orange `#d95926`, centered near y=285):** "client caches and advisory locks make shared appends a gamble".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all numbers are the hardcoded values above (no randomness); per-call latencies (0.02 ms local, 0.5 ms LAN, 20 ms VPN), file counts (200 listing files, 10,000 × 40 KB), and timings (4 ms / 100 ms / 4,020 ms; 4 s vs 19 s) are invented and labeled illustrative — only the multiplications must stay exact; SMB's Windows/IBM origin and NFS's Sun 1984 origin are documented facts and stated as such.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
