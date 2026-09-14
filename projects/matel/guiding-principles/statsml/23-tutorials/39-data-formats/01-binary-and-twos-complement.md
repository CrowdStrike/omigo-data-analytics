# Binary & Two's Complement

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Binary & Two's Complement

**Subtitle:** An integer in memory is a row of on/off switches — and negative numbers are the patterns that make addition wrap around to zero

## Thirteen on Eight Light Switches

**Tags:** `core idea` (blue), `place value` (green), `bits` (orange)

- **The switches** — a byte is eight on/off switches, each worth double the one to its right
- **The weights** — right to left the switches are worth 1, 2, 4, 8, 16, 32, 64, 128
- **Counting 13** — turn on the 8, the 4, and the 1 switches: 8 + 4 + 1 = 13
- **The pattern** — written left to right that is 00001101, the byte for thirteen
- **The definition** — binary is plain place value in base 2: each digit's weight is a power of two
- **The reach** — eight switches make 256 patterns, so one byte can count 0 through 255

*Example (italic):* 13 in one byte is 00001101 — the 8, 4, and 1 switches on, all five others off.

**Key point:** An integer in memory is nothing but a row of switches; its value is the sum of the weights of the switches that are on.

### Visualization (canvas `c1`, 720×300)

Eight bit boxes drawn side by side with their place-value weights above and the digits of 13 inside; the three "on" bits are highlighted and their weights summed below.

- **Title (bold 15px, `#1a5276`, top center):** "13 in a Byte: Three Switches On, 8 + 4 + 1".
- **Boxes:** eight 70×70 rounded boxes (6px radius) at y=110, x = 60, 140, 220, 300, 380, 460, 540, 620 (left to right = weights 128 down to 1).
- **Weights (bold 13px `#1a5276`, centered above each box at y=95):** `[128, 64, 32, 16, 8, 4, 2, 1]`.
- **Bits (bold 22px, centered in each box):** `[0, 0, 0, 0, 1, 1, 0, 1]` — on-bits (the 8, 4, 1 boxes) fill `rgba(0,131,0,0.12)`, 2px `#008300` border, green digit; off-bits fill `#ffffff`, 2px `#e5e9ef` border, `#6b7280` digit.
- **Contributions (bold 13px `#008300`, centered under each on-box at y=210):** "8", "4", "1"; off-boxes get 12px `#6b7280` "0".
- **Annotation (bold 14px green `#008300`, centered at y=250):** "8 + 4 + 1 = 13 → 00001101".
- **Caption (12px `#444`, bottom right):** "exact arithmetic — every digit checkable by hand".

## Making −13: Flip Every Bit, Then Add One

**Tags:** `worked example` (blue), `two's complement` (green), `hand-checkable` (orange)

- **Start** — take 13 = 00001101 and flip every bit: 11110010
- **Add one** — 11110010 + 1 = 11110011, and that byte is −13
- **Why it works** — a byte plus its flip is 11111111 = 255, so flip-plus-one makes the pair sum to 256
- **The wrap** — a byte keeps only 8 bits, and 256 is 100000000, so 256 behaves exactly like 0
- **The check** — 00001101 + 11110011 = 100000000; the ninth bit falls off, leaving 00000000
- **The sign bit** — the leftmost bit is 1 for every negative number, 0 for zero and the positives

*Example (italic):* Add 13 and −13 by hand: 00001101 + 11110011 = 100000000 — drop the ninth bit and eight zeros remain.

**Key point:** −13 is the byte that completes 13 to 256; since a byte drops the 256, the pair sums to 0 — so the hardware subtracts using nothing but ordinary addition.

### Visualization (canvas `c2`, 720×300)

Two-part diagram: a three-box flow across the top showing flip-then-add-one, and a hand-checkable column addition below showing the carry bit falling off.

- **Title (bold 15px, `#1a5276`, top center):** "Two Steps to −13, One Addition to Check It".
- **Flow row (boxes 40px tall at y=60):** blue `#2a78d6` rounded box at x=40, width 180, label "00001101 = 13" (13px monospace); 3px `#2c3e50` arrow labeled 12px `#6b7280` "flip every bit"; violet `#4a3aa7` box at x=290, width 150, label "11110010"; arrow labeled "+ 1"; green `#008300` box at x=510, width 180, label "11110011 = −13". Fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(0,131,0,0.12)`.
- **Column addition (16px monospace `#2c3e50`, right-aligned digits at x=260–420):** line 1 at y=160 "00001101" with 12px `#6b7280` note "(13)"; line 2 at y=185 "+ 11110011" with note "(−13)"; 2px `#1a5276` rule at y=196; result at y=220 "1 00000000" — the leading "1" drawn bold orange `#d95926` with a 12px orange label "carry falls off" and a small arrow pointing off the left edge; the eight zeros bold green `#008300` with 12px green note "= 0".
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=160):** "flip + 1 = the number that completes 256".
- **Caption (12px `#444`, bottom right):** "every digit exact — redo it on paper".

## INT8 to INT64: How Far Each Box Counts

**Tags:** `where it's used` (blue), `integer ranges` (green), `overflow` (red)

- **INT8** — one byte holds −128 to 127: 128 negatives but only 127 positives
- **INT16** — two bytes reach −32,768 to 32,767, the classic short integer
- **INT32** — four bytes reach −2,147,483,648 to 2,147,483,647; row counts and timestamps live here
- **INT64** — eight bytes reach about ±9.2 quintillion, the default id type in most databases
- **The odd man out** — −128 flips to 01111111, plus 1 is 10000000: itself, so it has no positive twin
- **The trap** — a count that outgrows its box does not error, it wraps; Unix time in INT32 runs out in 2038

*Example (italic):* Negating −128 in INT8 hands back −128 — flip 10000000 to 01111111, add 1, and you land on 10000000 again.

**Key point:** Every signed width has exactly one more negative than positive; picking a width the data will outgrow plants a silent time bomb.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of the four signed integer ranges, with schematic log-feel bar widths and the INT8 asymmetry called out.

- **Title (bold 15px, `#1a5276`, top center):** "Signed Integer Ranges: Each Doubling of Bits Squares the Reach".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (bars 16px tall at y = 75, 125, 175, 225), each with a left-aligned 12px `#444` label at x=20:**
  - "INT8 — −128 to 127": bar width 60
  - "INT16 — −32,768 to 32,767": bar width 155
  - "INT32 — −2,147,483,648 to 2,147,483,647": bar width 275
  - "INT64 — ±9.2 quintillion": bar width 430
- **Bar style:** fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border; range endpoint repeated as an 11px `#6b7280` label at each bar's right end.
- **INT8 callout (bold 12px green `#008300`, beside the INT8 bar):** "128 negatives, 127 positives".
- **Annotation (bold 13px magenta `#d55181`, right side near y=260):** "the negative side is always one longer — −128 has no partner".
- **Caption (12px `#444`, bottom right):** "bar widths schematic (log feel); range endpoints exact".

## 127 + 1 = −128: The Silent Wrap

**Tags:** `common mistake` (red), `overflow` (orange), `signed vs unsigned` (blue)

- **The wrap** — an INT8 counting up from 125 goes 126, 127, then −128: no error, no warning
- **Why silent** — the CPU just adds bits; 01111111 + 1 = 10000000, and 10000000 reads as −128
- **Same bits, two readings** — 11110011 is −13 signed but 243 unsigned; the bits never change
- **The misread** — read a signed byte as unsigned and every negative jumps up by exactly 256
- **The tell** — impossible values, like a negative count or a sudden 4-billion id, mean a wrap or a misread
- **The fix** — size the type before the data grows, and never compare signed against unsigned directly

*Example (italic):* An INT8 step counter at 127 takes one more step and reports −128 — the very bits an unsigned reader would call 128.

**Common mistake:** Assuming overflow will crash or warn. It wraps silently, and the same byte read as unsigned shifts every negative up by exactly 256 — so −13 quietly turns into 243.

### Visualization (canvas `c4`, 720×300)

Line chart of one counter incremented past 127, plotted twice: the unsigned reading keeps climbing while the signed reading cliffs from 127 down to −128.

- **Title (bold 15px, `#1a5276`, top center):** "Counting Past 127: Unsigned Keeps Climbing, Signed Falls Off a Cliff".
- **Axes:** origin x=70, baseline y=250, plot width 580, plot height 190; x = raw count 125 to 131, 12px `#444` tick labels at every integer; y = reported value −150 to 150, gridlines `#e5e9ef` with 12px `#6b7280` labels at −128, 0, and 127.
- **Unsigned line:** blue `#2a78d6` 3px line through counts `[125, 126, 127, 128, 129, 130, 131]`, values `[125, 126, 127, 128, 129, 130, 131]` — straight; bold 12px blue label "unsigned reading" above its right end.
- **Signed line:** orange `#d95926` 3px line through the same counts, values `[125, 126, 127, -128, -127, -126, -125]` — vertical cliff between 127 and 128; bold 12px orange label "signed reading" below its right end; 5px orange dots at the cliff points (127, 127) and (128, −128).
- **Bit marker:** vertical dashed `#6b7280` (dash 4/3) line at count 128, 12px `#6b7280` label "bits = 10000000" at its top.
- **Annotation (bold 13px orange `#d95926`, near count 129, y=90):** "127 + 1 = −128 — no error raised".
- **Caption (12px `#444`, bottom right):** "both lines plot the same bit patterns; values exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all digits and arithmetic are exact, not illustrative — 13 = 00001101 (weights 128/64/32/16/8/4/2/1, bits 0 0 0 0 1 1 0 1), flip to 11110010, +1 gives 11110011 = −13, 00001101 + 11110011 = 100000000 with the ninth bit dropped, 127 + 1 wraps to −128, 11110011 reads 243 unsigned; range endpoints (−128/127, −32,768/32,767, −2,147,483,648/2,147,483,647, ±9.2 quintillion) are the true signed widths; only c3's bar widths (60/155/275/430 px) are schematic log-feel.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
