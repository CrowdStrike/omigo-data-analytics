# Pitfall: Timezone / Timestamp Ambiguity

**Page type:** detail page (three `.card-section` blocks, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Timezone / Timestamp Ambiguity

**Subtitle:** Timestamps without timezone, precision, or DST handling cause silent data misalignment

## The Problem

**Tags:** `the trap` (red pill), `timestamps` (blue pill)

- **Naive timestamps** — stored without timezone info, one value can mean several instants
- **UTC vs local mix** — joins shift by hours and silently match the wrong records
- **DST transitions** — fall duplicates an hour and spring deletes one, twice a year
- **Precision mismatch** — seconds in one system vs milliseconds in another drops join matches
- **Ambiguous formats** — 03/04/2024 parses as MM/DD or DD/MM, silently swapping month and day

*Example:* Half a training set is UTC and half is EST; during DST week, 2am events appear twice and the model overfits on the duplicates.

**Impact:** Time-based joins silently drop or duplicate hours, so models learn spurious patterns and predictions misfire around DST transitions.

### Visualization (canvas `c1`, 720×300)

Block diagram of the DST spring-forward gap: five hour blocks, one missing.

- **Title (bold 14px, `#1a5276`, top center):** "DST Spring Forward — 2am Doesn't Exist (Missing Hour)".
- **Layout:** plot region left=60, right=680, top=80, bottom=240; five equal-width hour slots (slot width = 620/5, blocks drawn slot-width minus 10px).
- **Hours:** '12am', '1am', '2am', '3am', '4am'; existence flags `[true, true, false, true, true]`; displayed labels `['12am', '1am', '(gap)', '3am', '4am']`.
- **Existing hour blocks:** fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 2; label in `#2c3e50` 13px centered near top of block; three green (`#27ae60`) 4px-radius data-point dots stacked vertically (30px apart, starting 60px below block top).
- **Missing hour block (2am):** fill `rgba(231,76,60,0.2)`, dashed stroke `#e74c3c` (dash 6/4, width 2); bold red text "MISSING" plus two 11px lines "2am-3am" / "skipped"; a large red X drawn with two 3px-wide diagonal strokes across the block.
- **Caption (gray `#666`, 11px, bottom center):** 'Clock jumps from 1:59am to 3:00am — any events logged as "2:xx" are ambiguous'.

## Why It Happens

**Tags:** `root cause` (orange pill), `naive datetimes` (blue pill)

- **Naive storage** — legacy tables keep no timezone metadata, so the offset is unrecoverable
- **Mixed writers** — servers in different timezones write local times into the same table
- **Library defaults** — parsers assume local time unless a timezone is given explicitly
- **Rare edge cases** — DST bugs surface only two days a year, so tests rarely cover them
- **Deceptive simplicity** — timestamps look trivial, so engineers assume local time is enough

*Example:* A log written as "2024-03-10 02:30:00" falls in the spring-forward gap; some parsers read it as 01:30, others as 03:30.

**Root Cause:** A naive timestamp discards the one piece of metadata — the offset — needed to interpret it, and everything works until two systems with different implicit timezones feed the same table.

### Visualization (canvas `c2`, 720×300)

Diagram of three systems recording the same event with different timestamp interpretations, and a failed join.

- **Title (bold 14px, `#1a5276`, top center):** "Timezone Confusion — Same Event, Different Interpretations".
- **Three system rows** (right-aligned two-line gray labels at x<120, time boxes 250px wide × 35px tall starting at x=120, white fill, 2px colored stroke, bold 12px monospace time text):
  - "Server A / (UTC)" at y=70, time "2024-03-10 14:30:00", stroke `#27ae60`.
  - "Server B / (EST)" at y=130, time "2024-03-10 09:30:00", stroke `#e67e22`.
  - "Database / (naive)" at y=190, time "2024-03-10 14:30:00", stroke `#e74c3c`.
- **Join bracket:** red (`#e74c3c`, width 3) polyline connecting Server A and Server B rows to x=450, and extending down to the Database row.
- **Annotations (red, centered at x=500):** bold 11px "JOIN fails!" at y=110; 10px lines "A and B are same event" (y=125), "(5hr timezone diff)" (y=138), "DB sees as different" (y=190), "Missing timezone info" (y=203).
- **Caption (gray `#666`, 11px, bottom center):** 'Solution: Store as "2024-03-10T14:30:00Z" (UTC with explicit timezone)'.

## The Correct Approach

**Tags:** `the fix` (green pill), `UTC everywhere` (blue pill)

- **Store UTC** — write every timestamp in UTC using TIMESTAMP WITH TIME ZONE columns
- **ISO 8601** — serialize with an explicit offset, e.g. 2024-03-10T14:30:00Z
- **One precision** — standardize on milliseconds as the common denominator across systems
- **Test DST** — cover both spring-forward and fall-back transitions in unit tests
- **Detect drift** — flag duplicate timestamps and volume dips around transition hours
- **Local for display** — convert to user timezone only at read time, never in storage

*Example:* A payment system stores UTC ISO 8601 and converts to the user's timezone only for hour_of_day features, losing no data at DST.

**Fix:** Use timezone-aware datetime libraries, never parse dates without an explicit format string, and log a warning on any ambiguous timestamp.

### Visualization (canvas `c3`, 720×300)

BAD/GOOD table of timestamp formats plus a best-practice box.

- **Title (bold 14px, `#1a5276`, top center):** "Best Practices — UTC + ISO 8601 + Explicit Timezone".
- **Six rows** (starting y=60, row height 35; each row: 50×25 colored badge at x=60 with white bold 10px label, 11px monospace example at x=130, gray 10px issue text prefixed "→ " at x=380):
  - BAD (`#e74c3c`): `2024-03-10 14:30:00` → "No timezone, ambiguous"
  - BAD: `03/10/2024` → "MM/DD or DD/MM?"
  - BAD: `1710079800` → "Seconds or ms? Timezone?"
  - GOOD (`#27ae60`): `2024-03-10T14:30:00Z` → "UTC, unambiguous"
  - GOOD: `2024-03-10T14:30:00+00:00` → "UTC with offset"
  - GOOD: `2024-03-10T09:30:00-05:00` → "EST with offset"
- **Best practice box:** green stroke (`#27ae60`, width 2) rectangle 620×40 at (50,235); bold green 11px label "Best Practice:" then 11px `#2c3e50` text "Store UTC, convert to local only for display. Test DST transitions. Use TIMESTAMP WITH TIME ZONE."

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (full width, border-collapse) with one row: `.text-col` `<td>` (45%) and `.viz-col` `<td>` (55%). Text cell holds a `.tags` div of pill spans, a `<ul>` of `<li><b>Label</b> — sentence</li>` bullets, an italic `.example` paragraph, and a `.key-point` callout (`#f8f9fa` background, 3px `#e74c3c` left border, `<strong>` lead word).
- **Tag pills:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width: 100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each canvas declares `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#444`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
