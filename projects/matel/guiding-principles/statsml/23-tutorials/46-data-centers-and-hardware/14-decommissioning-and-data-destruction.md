# Decommissioning & Data Destruction

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Decommissioning & Data Destruction

**Subtitle:** A retired server is a copy of your data walking out of the building — so every drive leaves through one of three doors: erased, key-destroyed, or shredded into particles

## The Retirement Door Is a Data Path

**Tags:** `core idea` (blue), `decommissioning` (green), `data leakage` (red)

- **Fleet churn** — 100,000 drives on a 4-year refresh means ~25,000 drives leave service every year
- **They still hold data** — a drive pulled from a rack is a full, readable copy until something erases it
- **Resale is the incentive** — used drives and servers have real value, so "throw it in the skip" is not the default
- **The escape rate** — at a 0.2% mishandling rate, 25,000 × 0.002 = 50 drives/yr leave with data intact
- **That is ~1 per week** — 50 ÷ 52 ≈ 0.96, so roughly every week one readable drive walks out unlogged

*Example (italic):* A researcher buys 20 second-hand data-center drives online and finds three that were never wiped — the seller's process worked 85% of the time.

**Key point:** Decommissioning is an exit path for data, not a facilities chore — a small per-drive miss rate times a large fleet means readable drives leave the building every week.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one year of retired drives splitting across four exit routes, with counts that sum exactly to 25,000.

- **Title (bold 15px, `#1a5276`, top center):** "One Year of Retired Drives: Where 25,000 Drives Go".
- **Source box (left, rounded, blue `#2a78d6` stroke, blue 15% fill), centered at x=105, y=150, size 150×54:** two lines — "25,000 drives" (bold) and "retired this year".
- **Four destination boxes on the right at x=430, width 240, height 46, vertical centers y = 60, 118, 176, 240:**
  - green `#008300`: "18,000 — crypto-erased, resold"
  - green `#008300`: "1,950 — degaussed (magnetic only)"
  - blue `#2a78d6`: "5,000 — shredded (dead or unverifiable)"
  - red `#e74c3c`: "50 — unaccounted for"
- **Connectors:** curved or straight 2px lines from the right edge of the source box to the left edge of each destination, colored to match the destination; the red one drawn dashed `[5,4]` and 3px.
- **Annotation (bold 13px red `#e74c3c`, right-aligned near the red box, at y=272):** "0.2% miss rate = ~1 readable drive out the door per week".
- **Caption (12px `#444`, bottom right):** "counts illustrative but exact: 18,000 + 1,950 + 5,000 + 50 = 25,000".

## Clear, Purge, Destroy — Three Doors, Three Assurances

**Tags:** `worked example` (blue), `sanitization levels` (green), `NIST 800-88` (orange)

- **Clear** — overwrite every addressable block; defeats any software recovery, including undelete tools
- **Purge** — use the device's own erase path (crypto-erase, block erase, degauss); defeats lab recovery
- **Destroy** — shred, disintegrate, or incinerate the media so no device remains to read
- **Pick by failure state** — a drive that will not spin up cannot be cleared or purged, so it must be destroyed
- **Verify, then certify** — sample-read the drive after sanitizing and record serial number, method, and operator

*Example (italic):* Of 100 pulled drives, 92 respond to a crypto-erase and get resold; the 8 that fail to power on go straight into the shredder bin.

**Key point:** The three levels are not "good, better, best" — they are matched to media type and drive health, and a dead drive skips straight to physical destruction.

### Visualization (canvas `c2`, 720×300)

Three-tier ladder: each level as a wide box, with what it defeats and what defeats it.

- **Title (bold 15px, `#1a5276`, top center):** "Three Levels of Sanitization, and What Each One Survives".
- **Three horizontal bands, height 56, x from 60 to 660, top edges at y = 55, 130, 205:**
  - Band 1 (blue `#2a78d6`, 12% fill): left label "CLEAR" bold 14px; body 12px "overwrite all addressable blocks"; right-side 12px `#008300` "✓ stops software recovery", below it 12px `#e74c3c` "✗ misses remapped SSD blocks".
  - Band 2 (aqua `#199e70`, 12% fill): "PURGE"; body "crypto-erase / block erase / degauss"; right "✓ stops lab recovery", "✗ trusts the firmware to be honest".
  - Band 3 (violet `#4a3aa7`, 12% fill): "DESTROY"; body "shred, disintegrate, incinerate"; right "✓ no device left to read", "✗ drive cannot be resold".
- **Left gutter labels (12px `#444`, rotated not required, plain at x=20):** "level 1", "level 2", "level 3" aligned to band centers.
- **Annotation (bold 13px `#d95926`, centered, y=283):** "dead drive → skip to level 3; a drive you cannot talk to cannot be erased".
- **Caption (12px `#444`, bottom right):** "levels follow NIST SP 800-88 Rev.1 terminology".

## Overwriting 20 TB vs Destroying One Key

**Tags:** `worked example` (blue), `crypto-erase` (green), `throughput math` (orange)

- **The overwrite cost** — 20 TB at 250 MB/s is 20e12 ÷ 250e6 = 80,000 s = 22.2 hours for one pass
- **Three passes** — 66.7 hours, nearly three days of a drive bay doing nothing but writing zeros
- **Encrypt from birth** — a self-encrypting drive always writes ciphertext under a key held in the drive
- **Crypto-erase** — discard and regenerate that key in under a second; the platters become noise
- **Fleet arithmetic** — 25,000 drives × 22.2 h = 555,000 drive-hours, versus about 7 hours of key wipes

*Example (italic):* A rack of 60 drives takes over two days to zero end to end, or about a minute to crypto-erase drive by drive.

**Key point:** Encrypting from day one turns erasure from a throughput problem into a key-management problem — you destroy 20 TB by destroying 32 bytes.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of time to sanitize one 20 TB drive by method, log-feel widths.

- **Title (bold 15px, `#1a5276`, top center):** "Time to Sanitize One 20 TB Drive".
- **Vertical baseline 2px `#999` at x=250, from y=55 to y=235.**
- **Three rows, bar height 20, left labels 12px `#444` at x=20, tops at y = 75, 130, 185:**
  - "3-pass overwrite" — blue outline bar, width 430, value label "66.7 hours"
  - "1-pass overwrite" — blue outline bar, width 145, value label "22.2 hours"
  - "crypto-erase (SED)" — green `#008300` filled bar, width 6, value label "< 1 second"
- **Annotation (bold 13px `#008300`, left-aligned at x=300, y=232):** "same 20 TB, 80,000× less time".
- **Second annotation (bold 12px `#4a3aa7`, at x=300, y=250):** "fleet: 555,000 drive-hours → ~7 hours".
- **Caption (12px `#444`, bottom right):** "bar widths schematic; 22.2 h = 20e12 B ÷ 250e6 B/s ÷ 3600, exact".

## What Does Not Work

**Tags:** `common mistake` (red), `myths` (orange)

- **Formatting** — a quick format rewrites the index, not the data; files come back with free undelete tools
- **Degaussing an SSD** — flash stores charge, not magnetism, so a magnetic field leaves the data untouched
- **Overwriting an SSD** — spare area is invisible to the host: a 1 TB SSD hides ~70 GB of remapped blocks
- **Thirty-five passes** — the Gutmann pattern targeted 1990s encodings; one pass suffices on modern drives
- **Coarse shredding** — at 1 Tbit/in², a 10 mm × 10 mm fragment is 0.155 in² and holds ~19 GB of readable bits
- **Trusting the vendor** — no serial-numbered certificate of destruction means no evidence anything happened

*Example (italic):* A drive is degaussed, boxed, and signed off — then someone notices it was an SSD, and every byte is still there.

**Key point:** Each shortcut fails for a specific physical reason — wrong layer, wrong physics, or wrong resolution — so the method has to match the medium, not the habit.

### Visualization (canvas `c4`, 720×300)

Four myth/reality pairs as stacked rows: the claimed method on the left in red, the physical reason it fails on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Four Wipes That Leave the Data There".
- **Four rows, tops at y = 55, 105, 155, 205, each: a red-outlined box x=40 width 250 height 40 (red `#e74c3c` 8% fill) and a `#444` 12px reason text starting at x=310, wrapped to at most two lines:**
  - "quick format" → "rewrites the file index; every block still holds its bytes"
  - "degauss an SSD" → "flash holds trapped charge — a magnet changes nothing"
  - "overwrite an SSD" → "~70 GB of spare blocks are unreachable from the host"
  - "10 mm shred fragments" → "0.155 in² × 1 Tbit/in² ≈ 19 GB still readable per piece"
- **A green `#008300` footer box x=40 width 640 height 34 at y=258**, bold 12px centered text: "what works: crypto-erase a healthy SED, or shred flash to ~2 mm particles".
- **Caption (12px `#444`, bottom right):** "70 GB from ~7% over-provisioning on 1 TB; 19 GB from 1 Tbit/in² areal density — both illustrative".

## Regeneration instructions

- **Template:** tutorial detail page matching `05-everything-fails.html` exactly — same CSS block, `.card-section` per concept, `table.layout` with `td.text-col` 50% and `td.viz-col` 50%.
- **Left column order:** `.tags` pill row, `<ul>` of one-line bullets each opening with `<b>`, one italic `.example` line, one `.key-point` callout (`Key point:` or `Common mistake:`).
- **Canvases:** four, ids `c1`–`c4`, logical 720×300, registered in a `__charts` array, drawn through the shared `setup(id)` helper that scales by `window.devicePixelRatio` and re-draws on a debounced resize.
- **Palette:** `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`; red `#e74c3c` only for genuine failure states.
- **Helpers:** reuse `rrPath` (rounded rect) and `hArrow` (horizontal arrow) from the sibling pages.
- **No nav, no back/home links, no cross-page links.**
