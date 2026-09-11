# USB Flash Drives & SD Cards

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** USB Flash Drives & SD Cards

**Subtitle:** A chip of flash memory moves gigabytes between machines with no network and no setup — plug in, copy, walk away

## Alice's Pocket, Bob's Laptop: Ten Gigabytes with No Network

**Tags:** `core idea` (blue), `physical media` (green), `plug and play` (orange)

- **The running example** — Alice copies a 10 GB dataset to a thumb drive and hands it to Bob
- **Flash memory** — bits are trapped charge on a chip; no disk, no motor, no moving parts
- **Survives a pocket** — shrugs off the drops and jostling that wreck a spinning hard drive
- **Plug and play** — the computer mounts it as a plain folder; no install, no setup, no account
- **Capacity collapse** — a 64 MB stick around 2004; a 1 TB microSD today, smaller than a fingernail

*Example (italic):* In 2004 Alice's 10 GB would have needed ~160 of those 64 MB sticks; today it fits on one microSD a hundred times over.

**Key point:** The medium got smaller and cheaper faster than datasets grew, so hand-carrying data never stopped being practical.

### Visualization (canvas `c1`, 720×300)

Line chart of typical consumer flash-drive/card capacity by year on a log scale, with each point labeled with its human-readable size.

- **Title (bold 15px, `#1a5276`, top center):** "Typical Consumer Flash Capacity — Log Scale (illustrative)".
- **Axes:** x = years 2000–2024 mapped to px 95–660; y = log2(capacity in MB), 3 to 20, mapped to baseline y=245 up to y=65; 1px `#999` axis lines.
- **Gridlines (`#e5e9ef`) with 12px `#444` left labels:** at log2 = 3 ("8 MB"), 10 ("1 GB"), 20 ("1 TB").
- **Data points (hardcoded):** (2000, 8 MB, log2 3), (2004, 64 MB, log2 6), (2008, 4 GB, log2 12), (2012, 32 GB, log2 15), (2016, 128 GB, log2 17), (2020, 512 GB, log2 19), (2024, 1 TB, log2 20).
- **Line:** 3px blue `#2a78d6` connecting the points; 4.5px radius blue dots; each dot labeled bold 12px `#1a5276` with its size ("8 MB" … "1 TB"), year labels 12px `#444` under the baseline.
- **Annotation (bold 13px orange `#d95926`, upper left near x=140, y=90):** "~130,000× in 24 years — the medium outran the data".
- **Caption (12px `#6b7280`, bottom right):** "capacities illustrative of typical retail sizes".

## How Long Does 10 GB Take? One Division

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The formula** — copy time = size ÷ sustained speed; one division is the whole calculation
- **USB 2.0** — spec 480 Mbps; sticks sustain ~35 MB/s, so 10,000 MB ÷ 35 ≈ 286 s ≈ 4.8 min
- **USB 3.0** — spec 5 Gbps; a good stick sustains ~300 MB/s, so 10,000 ÷ 300 ≈ 33 s
- **USB-C 3.2** — spec 10 Gbps; SSD-grade drives sustain ~800 MB/s, so 10,000 ÷ 800 ≈ 12.5 s
- **Spec vs real** — the port's rating is a ceiling; the flash chip's write speed sets the pace

*Example (italic):* Bob's laptop port is rated 5 Gbps, but his budget stick's flash chip writes at 35 MB/s — the slower part wins.

**Key point:** The slowest link — port, cable, or flash chip — sets the copy time; divide by measured MB/s, not the number on the box.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: time to copy 10 GB (10,000 MB) at each generation's sustained speed, on a shared seconds axis.

- **Title (bold 15px, `#1a5276`, top center):** "Copying 10 GB: the Same Files, Three Ports".
- **Axis:** seconds 0–300 mapped to px x=215 (0 s) through x=680 (300 s); 1px `#999` baseline at y=250 with 12px `#444` ticks at 0 / 60 / 120 / 180 / 240 / 300 s; axis caption "seconds" 12px `#444`.
- **Rows (bars 34px tall, left-aligned 12px `#444` two-line labels at x=20):**
  - y=88: "USB 2.0 — ~35 MB/s", blue `rgba(42,120,214,0.30)` bar for 286 s, 2px `#2a78d6` border, bold 13px `#2a78d6` value label "286 s ≈ 4.8 min" right of the bar end (drawn inside the bar's right end since it nearly fills the axis)
  - y=148: "USB 3.0 — ~300 MB/s", aqua `rgba(25,158,112,0.30)` bar for 33 s, 2px `#199e70` border, bold 13px `#199e70` value label "33 s"
  - y=208: "USB-C 3.2 — ~800 MB/s", violet `rgba(74,58,167,0.30)` bar for 12.5 s, 2px `#4a3aa7` border, bold 13px `#4a3aa7` value label "12.5 s"
- **Annotation (bold 13px orange `#d95926`, centered near x=450, y=190):** "same 10 GB: 4.8 minutes → 12.5 seconds".
- **Caption (12px `#6b7280`, bottom right):** "spec rates documented (480 Mbps / 5 Gbps / 10 Gbps); sustained speeds typical, illustrative".

## Why Cameras, Drones, and Phones All Chose SD

**Tags:** `where it's used` (blue), `field data` (green), `standards` (orange)

- **One standard** — the SD spec means any card fits any camera, drone, reader, or laptop slot
- **Tiny and passive** — a microSD is about 15 × 11 mm, needs no battery, no cable, no power
- **Capture offline** — the sensor writes to the card in the field; no network at capture time
- **The card travels** — data rides home in a pocket; a card reader is the ingestion step
- **On your desk** — drone imagery, wildlife-camera photos, and sensor logs arrive as cards

*Example (italic):* A field ecologist's ingestion pipeline is a card reader, a folder-naming rule, and a checksum script.

**Key point:** Where networks don't reach, the removable card is the first hop of the data pipeline — plan for it like any other hop.

### Visualization (canvas `c3`, 720×300)

Left-to-right flow diagram of the field-data pipeline: five rounded boxes joined by arrows, with the card stage highlighted as the hop that replaces the network.

- **Title (bold 15px, `#1a5276`, top center):** "The Field-Data Pipeline: the Card Is the Network Link".
- **Boxes (rounded 8px, 118px wide, 52px tall, centered on y=140, bold 12px labels, two lines where needed), left to right at x = 28 / 162 / 296 / 430 / 574:**
  - "drone camera (field)" — green fill `rgba(0,131,0,0.12)`, 2px `#008300` border
  - "microSD card" — orange fill `rgba(217,89,38,0.18)`, 3px `#d95926` border (the highlighted hop)
  - "card reader" — blue fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border
  - "laptop" — blue fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border
  - "cloud / analysis" — violet fill `rgba(74,58,167,0.15)`, 2px `#4a3aa7` border
- **Arrows:** solid 2.5px `#6b7280` arrows between consecutive boxes with small filled arrowheads; 11px `#6b7280` stage labels under each arrow: "capture", "carry", "ingest", "upload".
- **Bracket:** a dashed 2px orange `#d95926` bracket under boxes 1–3 (y≈215) labeled bold 12px `#d95926` "no network anywhere in this stretch".
- **Annotation (bold 13px ink `#1a5276`, centered near y=62):** "capture happens where networks don't reach — the card carries the data home".
- **Caption (12px `#6b7280`, bottom center near y=285):** "schematic — one common shape of a field pipeline".

## The Flip Side: Convenience Cuts Both Ways

**Tags:** `common mistake` (red), `security` (orange), `best practice` (green)

- **Found-drive attack** — a stick left in a parking lot is a classic way in; never plug in strays
- **Fake capacity** — a cheap drive can claim 1 TB but hold far less; verify with a full write-read
- **Eject first** — the OS buffers writes in RAM; yanking the drive mid-flush corrupts copies
- **Flash wears out** — cells survive limited rewrites; fine for a courier, wrong for the only copy
- **Courier, not vault** — check sizes or checksums on arrival before wiping the source machine

*Example (italic):* Bob's 700 MB folder "finished" in 2 seconds — it was sitting in RAM; Eject held on until second 20, when the flash was really done.

**Key point:** Treat a flash drive as a courier, not a vault: confirm delivery, keep another copy, and never plug in a stranger's stick.

### Visualization (canvas `c4`, 720×300)

Two-line time chart of a 700 MB copy to a ~35 MB/s stick: what the progress bar reports vs what has actually reached the flash chip, with the gap shaded.

- **Title (bold 15px, `#1a5276`, top center):** "The Progress Bar Lies Early: RAM Buffer vs Flash (illustrative)".
- **Axes:** x = seconds 0–30 mapped to px 75–670; y = % of 700 MB written, 0–100 mapped to baseline y=240 up to y=70; 1px `#999` axis lines; x ticks 12px `#444` at 0 / 5 / 10 / 15 / 20 / 25 / 30 s with caption "seconds since copy started"; y labels "0%", "50%", "100%" 12px `#444`.
- **Reported line (green `#008300`, 3px):** points (0 s, 0%), (2 s, 100%), (30 s, 100%) — jumps to 100% at 2 s and stays flat; bold 12px green label "progress bar: done at 2 s" above the flat segment.
- **Actual line (orange `#d95926`, 3px):** linear from (0 s, 0%) to (20 s, 100%), then flat to 30 s — 700 MB ÷ 35 MB/s = 20 s; bold 12px orange label "on the flash chip: done at 20 s" below the ramp.
- **Shaded gap:** `rgba(217,89,38,0.12)` fill between the two lines from 2 s to 20 s.
- **Yank marker:** dashed 2px red `#e74c3c` vertical line at 5 s; bold 12px red annotation near it: "yank at 5 s: bar said 100%, flash had 25%".
- **Eject marker:** dashed 2px `#6b7280` vertical line at 20 s, 11px `#6b7280` label "Eject returns here".
- **Caption (12px `#6b7280`, bottom right):** "timings illustrative: 700 MB at ~35 MB/s".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" or "Common mistake:").
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` reserved for the genuine hazard (the yank marker in c4).
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Documented facts: USB spec rates 480 Mbps / 5 Gbps / 10 Gbps; SD standardized by the SD Association; microSD ~15 × 11 mm. Invented/typical figures (capacity timeline, sustained MB/s, copy times, the 700 MB buffering timeline) are labeled illustrative in each chart's caption. Arithmetic that must stay consistent between text and charts: 10,000 ÷ 35 ≈ 286 s ≈ 4.8 min; 10,000 ÷ 300 ≈ 33 s; 10,000 ÷ 800 ≈ 12.5 s; 10 GB ÷ 64 MB ≈ 160 sticks; 1 TB ÷ 8 MB ≈ 130,000×; 700 ÷ 35 = 20 s; 5 s ÷ 20 s = 25%.
- This page has no links.
