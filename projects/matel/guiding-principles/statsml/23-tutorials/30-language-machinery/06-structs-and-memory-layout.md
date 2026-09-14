# Structs & Memory Layout

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Structs & Memory Layout

**Subtitle:** A struct's fields sit at fixed byte addresses, and the compiler quietly inserts filler bytes so each field lands on a boundary its size allows — which is why the box is often bigger than what you put in it

## Six Bytes of Data, Twelve Bytes of Box

**Tags:** `core idea` (blue), `padding` (orange), `alignment` (green)

- **The record** — a coffee shop till stores one loyalty card as: a vip flag, a points number, a tier letter
- **The sizes** — the vip flag needs 1 byte, points needs 4 bytes, tier needs 1 byte: 6 bytes of real data
- **The rule** — the 4-byte points number must start at an address divisible by 4; that rule is alignment
- **The gap** — vip sits at byte 0, so the compiler inserts 3 dead bytes before points can start at byte 4
- **The tail** — 3 more pad bytes follow tier so the next card in an array also starts on a 4-byte boundary
- **The bill** — 6 bytes of data ride inside a 12-byte box: half of every card is padding

*Example (italic):* The till asks for one card and gets a 12-byte box: 1 byte of flag, 3 bytes of air, 4 bytes of points, 1 byte of tier, 3 bytes of air.

**Key point:** Padding is filler the compiler inserts so every field starts on an address its size allows — the struct grows even though your data didn't.

### Visualization (canvas `c1`, 720×300)

Single-row byte map: the 12 bytes of one loyalty card drawn as boxes, real fields colored, padding grayed out, with offsets numbered underneath.

- **Title (bold 15px, `#1a5276`, top center):** "One Loyalty Card in Memory: char vip, int points, char tier".
- **Byte cells:** 12 boxes, each 48px wide and 56px tall, left edges at x = `[72, 120, 168, 216, 264, 312, 360, 408, 456, 504, 552, 600]`, top y=115; 1px `#1a5276` borders.
- **Cell fills by offset:** offset 0 blue `rgba(42,120,214,0.35)`; offsets 1–3 gray `rgba(107,114,128,0.18)` with a thin diagonal-line hatch; offsets 4–7 green `rgba(0,131,0,0.25)`; offset 8 orange `rgba(217,89,38,0.30)`; offsets 9–11 gray hatch like 1–3.
- **Field labels (bold 12px, centered above each field's cells at y=100):** blue `#2a78d6` "vip" over offset 0, mute `#6b7280` "pad" over 1–3, green `#008300` "points" over 4–7, orange `#d95926` "tier" over 8, mute "pad" over 9–11.
- **Offset numbers:** "0" through "11", 12px `#444`, centered under each cell at y=190.
- **Brace line:** thin `#6b7280` bracket under the offsets from x=72 to x=648 at y=205 with 12px `#6b7280` label "sizeof = 12 bytes" centered below at y=222.
- **Annotation (bold 13px orange `#d95926`, centered near y=255):** "6 of the 12 bytes are padding — half the record is air".
- **Caption (12px `#444`, bottom right):** "byte map for a machine where int wants a 4-byte boundary — illustrative".

## Reordering the Fields: 12 Bytes Down to 8

**Tags:** `worked example` (blue), `field ordering` (green)

- **The reorder** — declare the big field first: points takes bytes 0–3, then vip at byte 4, tier at byte 5
- **Less filler** — only 2 pad bytes remain (6–7), kept so each card in an array starts at a multiple of 4
- **The count** — 4 + 1 + 1 + 2 = 8 bytes per card, down from 12, without changing a single field
- **By hand** — rule of thumb: sort fields from largest to smallest and most of the padding disappears
- **At scale** — 1,000,000 cards shrink from 12 MB to 8 MB just by reordering three lines of code

*Example (italic):* Same three fields declared big-to-small, and sizeof drops from 12 to 8 — a table one-third smaller for free.

**Key point:** sizeof is 12 as written and 8 reordered — largest-first ordering is the hand rule that squeezes out most padding.

### Visualization (canvas `c2`, 720×300)

Two stacked byte maps on a shared byte scale: the as-written 12-byte layout on top, the reordered 8-byte layout below, same color per field so the eye can track the move.

- **Title (bold 15px, `#1a5276`, top center):** "Same Three Fields, Two Layouts: 12 Bytes vs 8".
- **Shared scale:** each byte is a 40px-wide, 50px-tall box; left edges start at x=150.
- **Row 1 (boxes top y=85), left label 12px `#444` at x=20:** "as written — 12 bytes"; 12 cells at x = `[150, 190, 230, 270, 310, 350, 390, 430, 470, 510, 550, 590]`; fills: offset 0 blue `rgba(42,120,214,0.35)` "vip", 1–3 gray hatch `rgba(107,114,128,0.18)` "pad", 4–7 green `rgba(0,131,0,0.25)` "points", 8 orange `rgba(217,89,38,0.30)` "tier", 9–11 gray hatch "pad"; bold 11px field labels inside the cells, 1px `#1a5276` borders.
- **Row 2 (boxes top y=185), left label:** "reordered — 8 bytes"; 8 cells at x = `[150, 190, 230, 270, 310, 350, 390, 430]`; fills: offsets 0–3 green "points", 4 blue "vip", 5 orange "tier", 6–7 gray hatch "pad"; same label and border style.
- **Offset numbers:** 11px `#444` under each row's cells ("0"–"11" and "0"–"7").
- **Annotation (bold 13px green `#008300`, near x=505, y=210, two lines):** "same fields, 4 bytes saved" / "per card".
- **Bottom line (bold 12px violet `#4a3aa7`, centered at y=280):** "1,000,000 cards: 12 MB as written → 8 MB reordered".
- **Caption (12px `#444`, bottom right):** "illustrative".

## Why the Till Feels It: Cache Lines

**Tags:** `where it's used` (blue), `cache behavior` (orange)

- **The trip** — the chip never fetches one byte; it hauls memory in fixed 64-byte chunks called cache lines
- **Fat cards** — at 12 bytes each, one 64-byte line carries only 5⅓ cards, and many straddle two lines
- **Slim cards** — at 8 bytes each, exactly 8 cards ride in every line and none is split across two
- **The scan** — totaling points over 1,000,000 cards: 187,500 line loads at 12 bytes vs 125,000 at 8
- **Where it bites** — loops over big arrays of structs run at memory speed; padding is bandwidth spent on air

*Example (italic):* The nightly points report touches a third fewer memory lines after the reorder — same loop, same data, fewer trips.

**Key point:** Memory arrives 64 bytes at a time, so every pad byte in a hot struct steals room from real data on every single trip.

### Visualization (canvas `c3`, 720×300)

Two horizontal 64-byte cache lines drawn to the same scale: the top line filled with 12-byte cards (one split at the end), the bottom with 8-byte cards packing it exactly, plus the scan totals.

- **Title (bold 15px, `#1a5276`, top center):** "One 64-Byte Cache Line: How Many Cards Fit?".
- **Shared scale:** 1 byte = 8px, so each line is a 512px bar from x=140 to x=652; bars 44px tall, 1px `#1a5276` border.
- **Row 1 (bar top y=80), left label 12px `#444` at x=20 (two lines):** "12-byte cards" / "5⅓ per line"; card boundaries every 96px (12 bytes) at x = `[140, 236, 332, 428, 524, 620]`; cards alternate fills `rgba(42,120,214,0.35)` and `rgba(42,120,214,0.15)`; the sixth card is cut at x=652 with a jagged right edge and bold 11px `#d95926` label "split!" just above the cut.
- **Row 2 (bar top y=170), left label (two lines):** "8-byte cards" / "8 per line"; boundaries every 64px (8 bytes) at x = `[140, 204, 268, 332, 396, 460, 524, 588]`; alternate fills `rgba(0,131,0,0.30)` and `rgba(0,131,0,0.12)`.
- **Card numbers:** bold 11px `#1a5276` "1", "2", "3"... centered in each card segment.
- **Byte ruler:** 11px `#6b7280` ticks "0", "16", "32", "48", "64" under the bottom bar at x = `[140, 268, 396, 524, 652]`, y=232.
- **Annotation (bold 13px green `#008300`, centered near y=262):** "scan of 1,000,000 cards: 187,500 line loads → 125,000 — a third fewer trips".
- **Caption (12px `#444`, bottom right):** "64-byte line typical of desktop chips — illustrative".

## sizeof Is Not the Sum of the Parts

**Tags:** `common mistake` (red), `alignment rules` (orange)

- **The trap** — expecting sizeof to be the sum of the fields: 1 + 4 + 1 = 6, yet the compiler says 12
- **The rule** — the widest field sets the alignment, and total size rounds up to a multiple of it
- **The knob** — packed directives force 6 bytes, but misaligned reads run slower — or crash on some chips
- **The check** — print sizeof and each field's offset once; never assume the layout you wrote is what you got
- **The fix** — reordering cut the struct from 12 bytes to 8 for free, with none of packing's downsides

*Example (italic):* An engineer "saved memory" with a packed struct, then spent a week on a crash that a simple field reorder would have avoided.

**Common mistake:** Treating sizeof as the sum of the parts. The compiler pads and rounds up — measure the real size, and reorder fields before reaching for packed.

### Visualization (canvas `c4`, 720×300)

Four-bar chart of bytes per card under each layout choice, showing that the naive sum is never what you get and that reordering beats packing.

- **Title (bold 15px, `#1a5276`, top center):** "Bytes per Card: What You Expect vs What You Get".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = bytes 0 to 14 with light `#e5e9ef` gridlines at 4, 8, 12 and 12px `#444` tick labels "4", "8", "12"; no x axis line beyond the baseline (2px `#999`).
- **Bars (width 90px, centered at x = `[150, 300, 450, 600]`):** heights from values `[6, 12, 8, 6]` —
  - "sum of fields" = 6, fill `rgba(107,114,128,0.35)`, mute `#6b7280` outline;
  - "compiler, as written" = 12, fill `rgba(217,89,38,0.35)`, orange `#d95926` outline;
  - "reordered" = 8, fill `rgba(0,131,0,0.35)`, green `#008300` outline;
  - "packed" = 6, fill `rgba(213,81,129,0.30)`, magenta `#d55181` outline.
- **Value labels:** bold 13px in each bar's outline color, centered above each bar: "6", "12", "8", "6".
- **Bar labels:** 12px `#444`, centered under the baseline at y=265, wrapping to two lines where needed.
- **Packed warning:** 11px magenta `#d55181` label under the packed bar's value: "slow / unsafe reads".
- **Annotation (bold 13px green `#008300`, near x=370, y=75, two lines):** "the compiler rounds up to alignment —" / "order the fields, don't fight the rule".
- **Caption (12px `#444`, bottom right):** "sizes for a 4-byte-int machine — illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all byte offsets, cell x-positions, bar values, and scan counts are the hardcoded literals above (no randomness); the struct is `char vip; int points; char tier;` on a 4-byte-int, 64-byte-cache-line machine, and the 1,000,000-card / 12 MB / 8 MB / 187,500 / 125,000 figures are the same in text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
