# Error-Correcting Codes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Error-Correcting Codes

**Subtitle:** A scratched CD still plays because the disc stores extra check bits alongside the music — enough for the player to work out exactly which bits the scratch destroyed and put them back

## The Scratch That Ate 6 Bits

**Tags:** `core idea` (blue), `check bits` (green), `redundancy` (orange)

- **The scratch** — a 2 mm scratch on a CD wipes out a whole run of bits, yet the song plays without a click
- **The trick** — the disc never stores the music alone; roughly a quarter of its bits are extra check bits
- **Rebuilding** — the check bits let the player compute which bits died and what their values used to be
- **Phone-line intuition** — repeating "three, five, nine" twice on a bad line is the same idea, done crudely
- **The name** — any scheme that adds bits so a reader can repair damage is an error-correcting code

*Example (italic):* In the strip below, a scratch destroys 6 bits in a row out of a 30-bit stretch, and the player restores all 6 — that is error correction, shrunk to hand size.

**Key point:** An error-correcting code stores extra check bits next to the data so damaged bits can be recomputed — not just noticed.

### Visualization (canvas `c1`, 720×300)

Two horizontal bit strips of 30 cells each: the top strip is what the laser reads (a gray scratch band obliterating 6 cells), the bottom strip is what the player rebuilds (the 6 recovered bits highlighted green).

- **Title (bold 15px, `#1a5276`, top center):** "One Track, One Scratch: 6 Bits Gone, 6 Bits Back".
- **Strips:** 30 cells per strip, each cell 20px wide × 26px tall; cells run from x=60 to x=660; top strip at y=80, bottom strip at y=190; 12px `#444` labels left-aligned at x=60 above each strip: "what the laser reads" (y=70) and "what the player rebuilds" (y=180).
- **Bit values (hardcoded, both strips):** `[1,0,1,1,0,0,1,0,1,1,1,0,0,1,0,1,1,0,1,0,0,1,1,0,1,0,1,1,0,0]`; each cell drawn with 1px `#e5e9ef` border, bit text centered 12px `#2c3e50`.
- **Scratch band (top strip only):** cells at indices 12–17 (6 cells, x=300 to x=420) covered by a `rgba(107,114,128,0.35)` rectangle from y=68 to y=112; the 6 bit values replaced by red `#e74c3c` bold 13px "?"; 12px `#6b7280` label "scratch" centered above the band at y=64.
- **Recovered cells (bottom strip):** the same 6 cells (indices 12–17) filled `rgba(0,131,0,0.15)` with their true bits `[0,1,0,1,1,0]` in bold 13px green `#008300`; all other cells plain.
- **Annotation (bold 12px green `#008300`, two lines, centered near x=360, y=150):** "6 bits destroyed —" / "all 6 rebuilt from check bits".
- **Caption (12px `#444`, bottom right):** "illustrative — a real CD rebuilds thousands of bits per scratch".

## The Parity Grid: Finding One Flipped Bit by Hand

**Tags:** `worked example` (blue), `parity` (green)

- **The message** — 16 bits laid out as a 4×4 grid; the sender adds 8 check bits: one per row, one per column
- **Parity rule** — each check bit is chosen so its row or column holds an even number of 1s
- **One flip** — a scratch flips the bit at row 2, column 3 from 0 to 1
- **Two alarms** — row 2 now holds an odd count of 1s and so does column 3; every other line still passes
- **The crossing** — the flipped bit must sit where the failing row meets the failing column: flip it back
- **The cost** — 8 check bits guarded 16 data bits here; real codes are far leaner, but the logic is the same

*Example (italic):* Row 2 arrives as 0 1 1 1 with check bit 0 — three 1s is odd, so its alarm rings; column 3 rings too, and the two alarms cross at exactly one cell.

**Key point:** Row-and-column parity upgrades "something is wrong" to "this exact bit is wrong" — and a located flip is a fixed flip.

### Visualization (canvas `c2`, 720×300)

A 5×5 grid: the 4×4 data bits plus a parity column on the right and a parity row on the bottom; one flipped cell is shown in red, the two failing parity checks in orange, with their row and column highlighted so they visibly cross at the bad bit.

- **Title (bold 15px, `#1a5276`, top center):** "8 Check Bits Point at the One Flipped Bit".
- **Grid geometry:** 5 columns × 5 rows of 40px cells; grid origin x=240, y=55 (grid spans to x=440, y=255); cell borders 1px `#999`; all bit text centered 13px.
- **Data bits as received (rows top to bottom, hardcoded):** row 1 `[1,0,1,1]`, row 2 `[0,1,1,1]` (the third entry is the flipped bit — it was sent as 0), row 3 `[1,1,0,0]`, row 4 `[0,1,1,1]`.
- **Parity column (right, computed at send time):** `[1,0,0,1]`; **parity row (bottom):** `[0,1,0,1]`; corner cell (bottom-right) left empty; parity cells filled `rgba(26,82,118,0.10)`, text `#1a5276`.
- **Flipped cell (row 2, col 3):** fill `rgba(231,76,60,0.20)`, bold 14px red `#e74c3c` "1", 11px red label "was 0" just below the bit inside the cell.
- **Failing checks:** row 2's parity cell and column 3's parity cell drawn with 3px orange `#d95926` borders and bold orange bit text; row 2 and column 3 each overlaid with a `rgba(217,89,38,0.08)` band across the full grid so the two bands visibly cross at the red cell.
- **Alarm labels (bold 12px orange `#d95926`):** "row 2 fails: three 1s, check says even" to the right of the grid at x=460, y=143; "col 3 fails" centered below the grid at the column-3 x-position, y=285.
- **Left margin text (12px `#444`, x=20, wrapped as three short lines starting y=120):** "16 data bits" / "+ 4 row checks" / "+ 4 column checks".
- **Annotation (bold 12px `#1a5276`, two lines at x=460, y=200):** "the failing row and column" / "cross at the flipped bit".

## Check Bits Everywhere: CDs, QR Codes, Deep Space

**Tags:** `where it's used` (blue), `real systems` (green)

- **CDs** — Reed–Solomon codes plus bit-shuffling let a player ride out a scratch thousands of bits long
- **QR codes** — at the strongest setting a code still scans with about 30% of its squares covered
- **Memory and storage** — ECC RAM and SSDs quietly fix bits flipped by cosmic rays and worn-out cells
- **Deep space** — probes spend about half their bits on checks, because nobody can resend from Saturn
- **Data pipelines** — file and packet checksums are the everyday detect-only cousins of these codes

*Example (italic):* A CD hands over roughly 25% of its raw bits to check bits — the price of playing cleanly through fingerprints and scratches.

**Key point:** Every channel that can be damaged — discs, radio, RAM, cables — carries check bits; you rarely see an error because it is fixed in flight.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: four familiar systems and the share of their stored or transmitted bits that are check bits rather than data, showing that riskier channels buy more protection.

- **Title (bold 15px, `#1a5276`, top center):** "Share of Bits Spent on Checks (typical settings, illustrative)".
- **Axis:** bars grow rightward from a vertical 2px `#999` baseline at x=230; value axis 0% to 70% spanning 430px (x=230 to x=660); light `#e5e9ef` gridlines every 10% with 12px `#444` tick labels "0%"–"70%" below at y=262.
- **Bars (18px tall, rounded, top to bottom at y = 80, 125, 170, 215), left-aligned 12px `#444` system labels at x=20:**
  - "SSD flash storage": 12%, fill `rgba(42,120,214,0.75)` (blue)
  - "CD audio (Reed–Solomon)": 25%, fill `rgba(25,158,112,0.75)` (aqua)
  - "deep-space probe link": 50%, fill `rgba(74,58,167,0.75)` (violet)
  - "QR code, level H": 65%, fill `rgba(201,133,0,0.75)` (yellow)
- **Value labels:** bold 13px in each bar's color, 8px right of each bar end: "12%", "25%", "50%", "65%".
- **Annotation (bold 13px `#d95926`, two lines near x=420, y=60):** "harsher channel →" / "more bits spent on checks".
- **Caption (12px `#444`, bottom right):** "illustrative — real overheads vary by product and mode".

## Detecting Is Not Correcting

**Tags:** `common mistake` (red), `detection vs correction` (orange)

- **One copy** — a lone bit that flips just looks like a normal bit; the damage is completely invisible
- **Two copies** — if they disagree you know one is wrong, but a 1-vs-0 tie cannot say which one
- **Three copies** — majority vote: 1 0 1 reads as 1; detection has quietly become correction
- **The confusion** — a checksum or lone parity bit only detects; fixing needs structure that locates
- **Smarter than copies** — real codes buy three-copy safety at far below 3× cost; that is the whole field

*Example (italic):* A checksum tells you the download is corrupted and your only remedy is downloading again — a CD player mid-song never gets that luxury.

**Common mistake:** Assuming "we have a checksum" means the data can be repaired. Detection says something broke; correction needs enough extra structure to point at the broken bit.

### Visualization (canvas `c4`, 720×300)

Three-row diagram: the same stored 1 kept as one, two, and three copies; in each row one copy flips to 0, and the right side states what the reader can conclude — nothing, "conflict", or a corrected 1.

- **Title (bold 15px, `#1a5276`, top center):** "One Flip, Three Defenses: Invisible, Detected, Corrected".
- **Rows (at y = 90, 160, 230), left-aligned 12px `#444` labels at x=20:** "1 copy", "2 copies", "3 copies".
- **Bit boxes:** 34px squares starting at x=150, 12px gaps, bold 15px centered bit text; healthy copies show "1" on `rgba(42,120,214,0.15)` fill with blue `#2a78d6` text; the flipped copy in every row shows "0" on `rgba(231,76,60,0.15)` fill with red `#e74c3c` text and an 11px red "flipped" label below the box.
- **Row contents (hardcoded):** row 1 boxes `[0]` (its single copy is the flipped one); row 2 boxes `[1, 0]`; row 3 boxes `[1, 0, 1]`.
- **Arrows:** 2px `#6b7280` arrow from each row's last box to x=390 at the row's y.
- **Verdicts (at x=400):** row 1 bold 13px red `#e74c3c` "reads 0 — wrong, and nobody notices"; row 2 bold 13px orange `#d95926` "1 vs 0 — broken, but which copy?"; row 3 bold 13px green `#008300` "vote 2-to-1 — reads 1, fixed".
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=280):** "two copies can argue; three copies can vote".
- **Caption (12px `#444`, bottom right, y=295):** "real codes replace brute copies with parity structure".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bit arrays, grid values, bar percentages, and cell positions are the hardcoded literals above (no randomness); the c2 parity bits are true even-parity values for the stated data rows, and the flipped cell at row 2, col 3 is the only inconsistency; overhead percentages in c3 are labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
