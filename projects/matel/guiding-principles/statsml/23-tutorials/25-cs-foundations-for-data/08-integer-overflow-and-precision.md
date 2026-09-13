# Integer Overflow & Precision

**Page type:** detail page (tutorial page: 4 card-sections, each an h2 + two-column layout table, text left 50%, canvas right 50%)
**HTML title tag:** Integer Overflow & Precision

**Subtitle:** A 32-bit counter sits at 2,147,483,647, one more event arrives, and the dashboard shows −2,147,483,648 — whole numbers have edges too

## The Counter That Went Negative

Tags: `core idea` (blue), `running example` (green)

- **The scene** — an event counter stored as a 32-bit integer climbs for months
- **The ceiling** — a signed 32-bit int tops out at 2,147,483,647 (about 2.1 billion)
- **The wrap** — add 1 more and it becomes −2,147,483,648, the most negative value
- **The odometer** — like a car odometer rolling over from 999999 back to 000000
- **No warning** — many systems wrap silently; the chart just plunges

*Example:* The totals read …2,147,483,640, 2,147,483,647 — and then a negative number on the next tick.

**Key point:** Integers are exact but bounded — cross the edge and the value wraps around; it does not stop or warn.

### Visualization (canvas `c1`, 720×300)

Schematic time-series line chart: a counter climbing to INT max then plunging to INT min and resuming its climb.

- **Title (bold 15px, ink `#1a5276`, top center):** "A 32-bit counter over time (schematic)".
- **Axes/scale:** y spans ±2.5e9 centered on 0 (padding: top 50, bottom 46, left 90, right 30); x is 21 time steps wide with mute 12px caption "time →" centered at the bottom.
- **Reference lines (dashed 5/4, 11px right-aligned labels):** orange `#d95926` at +2,147,483,647 labeled "INT max  2,147,483,647"; orange at −2,147,483,648 labeled "INT min  −2,147,483,648"; solid grid-gray `#e5e9ef` line at 0 labeled "0" (mute).
- **Climb series (blue `#2a78d6`, width 3), values ×10⁹ at steps 0–14:** `[0.1, 0.25, 0.45, 0.7, 0.95, 1.2, 1.42, 1.6, 1.78, 1.9, 2.0, 2.08, 2.13, 2.146, 2.147483647]`.
- **The wrap:** vertical dashed (3/3) magenta `#d55181` line width 2 at step 14 dropping from INT max to INT min.
- **Post-wrap series (magenta, width 3), values ×10⁹ at steps 14–19:** `[-2.147483648, -2.1, -2.0, -1.85, -1.68, -1.5]`.
- **Annotation (magenta bold 13px):** "+1 past the max lands at the minimum" placed left of the wrap in the negative region.

## See the Wrap in 8 Bits

Tags: `worked example` (green)

- **Small version** — same rule, tiny box: a signed 8-bit integer spans −128 to 127
- **At the top** — 127 in binary is 01111111
- **Add one** — the binary carry ripples through: 01111111 + 1 = 10000000
- **The sign bit** — a leading 1 means negative: 10000000 reads as −128
- **Same story** — 32-bit is the identical wheel, just with 4.3 billion positions on it

*Example:* 01111111 + 00000001 = 10000000 — every bit carries, and the result flips sign.

**Key point:** The wrap is not corruption — it is binary addition working exactly as built, inside a fixed-size box.

### Visualization (canvas `c2`, 720×300)

Split-panel diagram: 8-bit binary rows on the left, a signed number wheel on the right; vertical dashed gray divider (`#bdc3c7`, dash 4/3) at x=390.

- **Title (bold 15px, `#1a5276`, top center):** "Signed 8-bit: 127 + 1 = −128".
- **Left panel — bit rows:** each row is eight 26×26 cells (fill `#f8f9fa`, stroke `#c8ced6`, bold 13px monospace digits) with a bold 12px label to the right:
  - "01111110" = 126 (blue `#2a78d6` label)
  - "01111111" = 127 (blue label)
  - mute 12px line between rows: "+ 1  (every bit carries)"
  - "10000000" = −128 (magenta `#d55181` label); its first cell (the sign bit) highlighted magenta: fill rgba(213,81,129,0.15), magenta stroke width 2, magenta digit.
  - mute 11px note below: "first bit = sign: 1 means negative".
- **Right panel — the wheel:** mute circle (radius 78, centered ~x=545, y=158) with labeled ticks: "0" (top, mute), "64" (right, mute), "127" (blue, just left of bottom), "−128" (magenta, just right of bottom, adjacent to 127), "−64" (left, mute). An orange `#d95926` arc arrow (width 2.5, with filled arrowhead) crosses the seam from 127 to −128, labeled bold 12px orange "+1" below the wheel.
- **Right heading (ink bold 13px centered above the wheel):** "on the wheel, 127 and −128 are neighbors".

## JavaScript Mangles Big IDs at 2⁵³

Tags: `common mistake` (orange), `worked example` (green)

- **Doubles for everything** — JavaScript stores all numbers as floats, exact only up to 2⁵³ − 1
- **The edge** — 9,007,199,254,740,991 is the last safe integer (`Number.MAX_SAFE_INTEGER`)
- **The mangle** — `JSON.parse("9007199254740993")` silently returns …740992
- **Collapsed IDs** — two different database IDs can become one number in the browser
- **The fix** — ship big IDs as strings: `{"id": "9007199254740993"}` survives untouched

*Example:* Snowflake-style IDs (~19 digits) are all past the safe edge; auto-increment IDs cross it only past ~9×10¹⁵.

**Key point:** If an ID can exceed 15 digits, treat it as a string everywhere — the number type will quietly rewrite it.

### Visualization (canvas `c3`, 720×300)

Number-line diagram: representable doubles near 2⁵³ with two incoming IDs collapsing onto one tick.

- **Title (bold 15px, `#1a5276`, top center):** "Above 2⁵³, doubles step by 2 — odd integers do not exist".
- **Number line:** horizontal mute line at y=176 from x=70 to x=650.
- **Ticks (width 2.5, monospace 11px labels below):** "…740990" (x=120, mute `#6b7280`), "…740991" (x=230, green `#008300`, with 11px note "last safe integer"), "…740992" (x=340, blue `#2a78d6`), "…740994" (x=480, blue), "…740996" (x=620, blue). Between the blue ticks, mute monospace labels "(no …993)" at x=410 and "(no …995)" at x=550.
- **Incoming IDs:** mute 12px header at top-left: "IDs arriving from the database as JSON numbers:". Two labeled arrows (bold 12px monospace) pointing down to the SAME tick at x=340: "id …740992" (aqua `#199e70`) and "id …740993" (magenta `#d55181`).
- **Annotations:** magenta bold 13px centered: "JSON.parse(\"…740993\") → …740992: two rows become one"; green 12px monospace centered below: "fix: {\"id\": \"9007199254740993\"} — a string survives".

## Why Analysts Should Care

Tags: `where it's used` (blue), `common mistake` (orange)

- **Fingerprints** — negative counts, sudden plunges, or long IDs ending in 0s mean overflow
- **Sums overflow first** — a SUM() over billions of small values can still cross 2.1 billion
- **Type limits** — SMALLINT tops at 32,767; INT at 2.1 billion; BIGINT at 9.2 quintillion
- **Cheap insurance** — use BIGINT for counters and keys; the storage cost is trivial
- **Spreadsheet cousin** — Excel keeps 15 digits: a 16-digit card number ends in 0

*Example:* An "impossible" negative daily count in an analytics table traced back to an INT column, not bad data.

**Key point:** When a metric does something arithmetic says is impossible, check the column type before blaming the data.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart on a log scale: how far each SQL integer type reaches, with a workload marker.

- **Title (bold 15px, `#1a5276`, top center):** "Where each integer type gives out (log scale)".
- **Axis:** x is log10 from 0 to 19; vertical grid-gray gridlines with 11px labels at 10³, 10⁶, 10⁹, 10¹², 10¹⁵, 10¹⁸. Padding: top 54, bottom 50, left 120, right 40.
- **Bars (height 34, 18px gaps, fill = bar color at alpha 0.30 with solid 1.5px stroke; type name bold 12px right-aligned left of the bar, max label bold 12px in the bar color right of the bar end):**
  - SMALLINT — max 32,767, label "32,767", yellow `#c98500`
  - INT — max 2,147,483,647, label "2.1 billion", orange `#d95926`
  - BIGINT — max 9.22e18, label "9.2 quintillion", green `#008300`
- **Workload marker:** vertical dashed (5/4) magenta `#d55181` line width 2 at 3.2e9, with bold 13px magenta two-line label: "one year at 100 events/sec ≈ 3.2B —" / "already past INT".

## Regeneration instructions

- **Template/layout:** tutorial detail page (per `tutorials/CLAUDE.md`, social-graph reference skeleton). Page = `<h1>` + `.subtitle` paragraph, then 4 `.card-section` blocks. Each `.card-section` has an `<h2>` (1.3rem, `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%), cell padding 12px, vertical-align top.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>` "Key point:" prefix.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg rgba(26,82,118,0.12) / `#1a5276`; green: bg rgba(39,174,96,0.15) / `#27ae60`; red: bg rgba(231,76,60,0.12) / `#e74c3c`; orange: bg rgba(230,126,34,0.15) / `#e67e22`.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; `<code>` in ui-monospace on `#f4f6f8`. No nav bar, no back/home links.
- **Canvases:** 720×300 intrinsic, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled via `window.devicePixelRatio` in a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- Grid cards elsewhere linking here use `.html` extensions in regenerated HTML.
