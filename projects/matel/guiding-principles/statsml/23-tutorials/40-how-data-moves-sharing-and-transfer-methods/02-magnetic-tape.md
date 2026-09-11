# Magnetic Tape

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Magnetic Tape

**Subtitle:** A kilometer of plastic ribbon in a palm-sized box — slow to wind to a file, fast once it gets there, and still the cheapest place in computing to keep a byte

## One Long Ribbon: Files Live End to End

**Tags:** `core idea` (blue), `reels` (green), `sequential` (orange)

- **The ribbon** — a cartridge holds about 1 km of plastic ribbon coated in magnetic particles
- **End to end** — files sit one after another along the ribbon, like songs on a cassette
- **One head** — the drive reads only the spot under its head; reaching a file means winding
- **No shortcuts** — to read December, Alice's drive winds past January through November first
- **Palm-sized** — a modern LTO-9 cartridge holds 18 TB raw in a box that fits in one hand

*Example (italic):* Alice archives a year of sensor logs to one cartridge; December's logs sit at the far end of the ribbon.

**Key point:** Tape is sequential — data lives along a physical ribbon, and reading a file means physically winding to it.

### Visualization (canvas `c1`, 720×300)

Schematic of the ribbon pulled out of a cartridge: two reels, a monthly file strip between them, the read head at the start, and a winding arrow to December.

- **Title (bold 15px, `#1a5276`, top center):** "Alice's Year on One Ribbon: December Is a Long Wind Away".
- **Reels:** two circles radius 30 (2.5px stroke), left reel violet `#4a3aa7` centered (95, 150), right reel violet centered (625, 150); inner hub circles radius 8 filled violet; 12px `#6b7280` labels "supply reel" / "take-up reel" 48px below each center.
- **Ribbon strip:** horizontal band from x=125 to x=595, y=130 to y=170, split into 12 equal segments (~39px each); segments Jan–Nov filled `rgba(42,120,214,0.18)` with 1px blue `#2a78d6` borders and 12px `#2c3e50` labels "J F M A M J J A S O N"; December segment filled `rgba(0,131,0,0.30)` with 2px green `#008300` border and bold 12px green label "D".
- **Read head:** small ink `#1a5276` filled triangle (12px wide) pointing down at the top edge of the January segment (x≈145, y=122), bold 12px ink label "read head" above it at y=105.
- **Winding arrow:** dashed orange `#d95926` 2.5px arrow (dash 6/4) arcing from the head over the strip to the December segment (control point around y=70), arrowhead at December; bold 13px orange label centered above the arc at y=58: "to read December, wind past 11 months of ribbon".
- **Annotation (bold 13px green `#008300`, centered near y=235):** "18 TB raw in one palm-sized LTO-9 cartridge".
- **Caption (12px `#444`, bottom right):** "layout schematic — real tapes interleave data across many passes".

## Winding to the Middle: 50 Seconds vs 5 Milliseconds

**Tags:** `worked example` (blue), `seek time` (orange)

- **The question** — how long to reach a file sitting at the middle of the ribbon?
- **Tape math** — 1,000 m of ribbon, middle at 500 m, winding at 10 m/s: 500 / 10 = 50 seconds
- **Disk math** — a hard disk swings its arm to any track in about 5 ms, i.e. 0.005 seconds
- **The ratio** — 50 / 0.005 = 10,000: tape takes ~10,000x longer to reach a random spot
- **Then it flies** — once positioned, an LTO-9 drive streams about 400 MB/s in order

*Example (italic):* 500 m at 10 m/s is 50 seconds of winding; the same jump on a disk is 5 milliseconds.

**Key point:** Random access is tape's weakness by a factor of about 10,000 — so tape holds data you write once and read rarely.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart on a log time axis: time to reach a random file on a hard disk vs the middle of a tape.

- **Title (bold 15px, `#1a5276`, top center):** "Time to Reach a Random File (log scale)".
- **Axis:** log10 time axis from 1 ms (log10 = -3) to 100 s (log10 = +2), mapped to x=200…670; 2px `#999` baseline at y=225 with vertical `#e5e9ef` gridlines and 12px `#444` tick labels at "1 ms", "10 ms", "100 ms", "1 s", "10 s", "100 s" (one tick per decade).
- **Rows (bars 34px tall), each with a right-aligned bold 13px `#2c3e50` label at x=190:**
  - y=95 "hard disk seek": blue `#2a78d6` bar from -3 to log10(0.005)=-2.3 (x 200→266), 12px blue value label "5 ms" just right of the bar end
  - y=160 "tape wind to middle": orange `#d95926` bar from -3 to log10(50)=1.7 (x 200→642), bold 12px white value label "50 s" inside the bar's right end
- **Bar style:** fills at 80% opacity with solid 1.5px same-hue borders.
- **Annotation (bold 13px orange `#d95926`, centered near x=430, y=68):** "~10,000x slower to reach a random spot".
- **Caption (12px `#444`, bottom right):** "10 m/s wind speed illustrative; every decade on the axis is a 10x jump".

## Absurdly Cheap Bytes: Why Cold Archives Ride on Tape

**Tags:** `where it's used` (blue), `cold archive` (green), `cost` (orange)

- **Cost per TB** — roughly $5 on tape vs $15 on hard disk vs $60 on SSD (illustrative)
- **No power** — a cartridge on a shelf draws zero watts; a disk array burns power all day
- **Shelf life** — LTO cartridges are rated for about 30 years of archival storage
- **Cold tiers** — the cheapest cloud archive tiers sit on tape robots, so restores take hours
- **Bulk moves** — studios and research labs still ship petabyte archives as boxes of cartridges

*Example (italic):* Alice's archive-restore ticket sat for hours — a robot was fetching cartridges and winding to her files.

**Key point:** For data written once and read almost never, cost per byte decides — and tape's bytes are the cheapest in computing.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of illustrative cost per terabyte for tape, hard disk, and SSD.

- **Title (bold 15px, `#1a5276`, top center):** "Cost per Terabyte: Tape vs Hard Disk vs SSD (illustrative)".
- **Axes:** 2px `#999` y-axis at x=90 and baseline at y=240; y scale 0–$70 mapped to y=240…70; horizontal `#e5e9ef` gridlines with 12px `#444` labels at $0 / $20 / $40 / $60; 12px `#444` rotated y-axis caption "US$ per TB" at x=35.
- **Bars (width 110, centered at x = 200 / 385 / 570):**
  - "tape (LTO)" $5: green `#008300`, height (5/70)·170 ≈ 12px
  - "hard disk" $15: blue `#2a78d6`, height ≈ 36px
  - "SSD" $60: violet `#4a3aa7`, height ≈ 146px
- **Bar style:** fills at 80% opacity, 1.5px same-hue borders; bold 13px same-hue value labels "$5" / "$15" / "$60" above each bar; 13px `#2c3e50` category labels below the baseline at y=262.
- **Annotation (bold 13px green `#008300`, near x=200, y=115, with a thin green arrow down to the tape bar):** "12x cheaper per byte than SSD — and it draws zero watts on the shelf".
- **Caption (12px `#444`, bottom right):** "street prices illustrative — the ordering, not the exact dollars, is the point".

## Slow at Finding, Fast at Streaming

**Tags:** `common mistake` (red), `throughput` (green)

- **The mistake** — hearing "tape is slow" and assuming everything about it is slow
- **Two speeds** — finding a spot takes tens of seconds; reading in order runs 400 MB/s
- **Beats the disk** — a streaming LTO-9 drive outruns a typical hard disk's ~250 MB/s
- **Right job** — full restores and archive sweeps read in order, so the weakness never shows
- **Wrong job** — serve live queries from tape and every request pays the winding every time

*Example (italic):* The drive that needs 50 seconds to find one file can move 24 GB in the minute after finding it.

**Key point:** "Tape is slow" is a claim about latency, not throughput — tape finds data slowly but moves it fast.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of sequential streaming speed for a hard disk, an LTO-9 tape drive, and an entry SSD.

- **Title (bold 15px, `#1a5276`, top center):** "Streaming Speed, Reading in Order (MB/s)".
- **Axis:** 2px `#999` baseline (vertical) at x=210; x scale 0–600 MB/s mapped to x=210…670; vertical `#e5e9ef` gridlines with 12px `#444` labels at 0 / 200 / 400 / 600 at y=250.
- **Rows (bars 36px tall), each with a right-aligned bold 13px `#2c3e50` label at x=200:**
  - y=80 "hard disk": blue `#2a78d6` bar to 250 MB/s (x 210→402), 12px blue value "250" right of the bar
  - y=140 "tape (LTO-9)": green `#008300` bar to 400 MB/s (x 210→517), bold 12px green value "400" right of the bar
  - y=200 "entry SSD": violet `#4a3aa7` bar to 550 MB/s (x 210→633), 12px violet value "550" right of the bar
- **Bar style:** fills at 80% opacity with 1.5px same-hue borders.
- **Annotation (bold 13px green `#008300`, centered near x=440, y=55):** "streaming in order, the 'slow' medium outruns the hard disk".
- **Caption (12px `#444`, bottom right):** "typical rated speeds; LTO-9 native 400 MB/s is the drive's documented figure".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Charts:** each canvas 720×300 logical, `devicePixelRatio`-scaled via a shared `setup(id)` helper, CSS `width:100%`; all data hardcoded literal values, no `Math.random()`; redraw all charts on window resize (debounced).
- **Palette:** `const P = {blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef'}`; navy ink for headings and axes; red reserved for error states (none on this page).
