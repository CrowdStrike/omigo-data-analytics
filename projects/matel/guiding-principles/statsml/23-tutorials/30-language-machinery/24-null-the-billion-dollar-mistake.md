# Null — the Billion-Dollar Mistake

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Null — the Billion-Dollar Mistake

**Subtitle:** Null is absence dressed up as a value — it fits anywhere the real thing fits and only announces itself by crashing; Option types re-legislate absence so the compiler makes you handle it

## The Missing Phone Number

**Tags:** `core idea` (blue), `absence as a value` (green), `runtime crash` (red)

- **The app** — a delivery app texts "your driver is outside" to the phone number on each profile
- **The gap** — Sam never entered a phone, so the profile stores null: a placeholder meaning "nothing here"
- **The trap** — null slips in anywhere a phone number fits; nothing complains at signup or checkout
- **The crash** — Maya and Leo get their texts; Sam's null hits phone.format() and the whole run dies
- **The name** — Tony Hoare added null to ALGOL W in 1965 and later called it "my billion-dollar mistake"

*Example (italic):* Sam's profile sailed through every step of signup and checkout — the null only exploded hours later, in texting code that had nothing to do with either.

**Key point:** Null is absence disguised as a value: it travels wherever the real value could go, and the first you hear of it is a crash far from where it entered.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three customer profiles flow left-to-right through the same format-and-text step; the two real numbers succeed, the null row ends in a bold red crash.

- **Title (bold 15px, `#1a5276`, top center):** "Three Profiles, One Code Path — Null Rides Along Quietly".
- **Layout:** three rows at y = 90, 160, 230; column of 12px `#444` customer names at x=30 ("Maya", "Leo", "Sam"); stored-value boxes at x=150, 140×34, rounded 4px; `format()` boxes at x=380, 120×34; outcome text at x=560.
- **Row 1 (Maya):** value box "555-0142", 2px blue `#2a78d6` border, 12px text; 2px `#6b7280` arrow to format() box (1px `#6b7280` border, 12px label "phone.format()"); arrow to bold 12px green `#008300` outcome "text sent".
- **Row 2 (Leo):** value box "555-0177", same styling; outcome bold 12px green "text sent".
- **Row 3 (Sam):** value box "null" in bold 13px orange `#d95926` with 2px dashed orange border; arrow into the same format() box; outcome bold 14px red `#e74c3c` "CRASH".
- **Annotation (bold 12px red `#e74c3c`, centered near y=272):** "null fit the phone-shaped slot — until someone asked it for digits".
- **Caption (11px `#444`, bottom right):** "illustrative — three profiles from the delivery app".

## One Night's Text Run, With and Without Option

**Tags:** `worked example` (blue), `Option type` (green), `compiler check` (orange)

- **The batch** — the nightly run texts 20 customers; 14 gave a phone, 6 did not (slots 4, 8, 11, 15, 18, 20)
- **With null** — the loop sends 3 texts, hits the null in slot 4, crashes; slots 5–20 are never reached
- **With Option** — the field becomes Option⟨Phone⟩: either Some(number) or None, and code must say which
- **The forced branch** — the compiler refuses to build until the None case is written; forgetting is impossible
- **The result** — the same 20 profiles: 14 texts sent, 6 politely skipped, zero crashes

*Example (italic):* The Option version has one extra line — "None ⇒ skip this customer" — and that one line is exactly the check the null version let the programmer forget.

**Key point:** 14 + 6 = 20 either way; Option adds no information — it moves the missing-phone check from the programmer's memory to the compiler's checklist.

### Visualization (canvas `c2`, 720×300)

Two-panel slot chart: the same 20 customer slots drawn twice — the null run stops dead at slot 4, the Option run completes with 14 sent and 6 skipped.

- **Title (bold 15px, `#1a5276`, top center):** "Same 20 Customers: 3 Texts Then a Crash vs 14 Sent + 6 Skipped".
- **Panels:** left panel titled bold 13px `#e74c3c` "with null" at x=60, right panel titled bold 13px `#008300` "with Option⟨Phone⟩" at x=400; each panel draws 20 slots (12×26 rects, 14px pitch) in a row at y=140, left row starting x=60, right row starting x=400.
- **Missing-phone slots (both panels):** positions `[4, 8, 11, 15, 18, 20]`.
- **Left panel:** slots 1–3 filled green `#008300`; slot 4 filled red `#e74c3c` with bold 14px white "×" centered; slots 5–20 outlined 1px `#e5e9ef` only; 12px `#444` label under the row: "3 of 20 texts sent, then crash — 16 never reached".
- **Right panel:** the 6 missing-phone slots filled mute `#6b7280` at 40% alpha; the other 14 filled green `#008300`; 12px `#444` label under the row: "14 sent, 6 skipped, run completes".
- **Legend (11px `#444`, y=225, under each panel):** green square "text sent", grey square "None — skipped", red square "crash".
- **Annotation (bold 12px green `#008300`, right panel near y=95):** "compiler forced the None branch before the run".
- **Caption (11px `#444`, bottom right):** "illustrative — one night's batch of 20 profiles".

## Where a Data Scientist Meets Null Every Day

**Tags:** `where it's used` (blue), `SQL & pandas` (green), `sentinel values` (orange)

- **In SQL** — NULL compared to anything gives NULL, so a WHERE filter silently drops those rows
- **In NumPy** — one NaN turns a whole sum into NaN; pandas sums skip NaN silently, hiding the gap
- **In the wild** — the same absence hides as NULL, NaN, "", "N/A", 0, or -999, often in a single table
- **The fix that stuck** — Rust's Option, Swift's Optional, Kotlin's `String?`, Haskell's Maybe put absence in the type
- **The payoff** — absence becomes a case the code must handle, not a value it might trip over

*Example (italic):* In the 10,000-row customer table below, 2,300 rows are missing the same fact — a phone number — recorded in six different spellings of "unknown".

**Key point:** Before any analysis, hunt down every disguise absence wears in your data — a -999 that survives into an average does far more damage than an honest NULL.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: the six encodings of "no phone" found in one customer table, sorted by count, showing the same missing fact wearing six costumes.

- **Title (bold 15px, `#1a5276`, top center):** "Six Spellings of 'No Phone' in One 10,000-Row Table".
- **Layout:** vertical 2px `#999` axis at x=150 from y=55 to y=250; six bar rows at y = 70, 102, 134, 166, 198, 230, bar height 20; left-aligned 12px `#444` row labels at x=20: `NULL`, `NaN`, `"" (empty)`, `"N/A"`, `0`, `-999`.
- **Bars (start x=150, scale 0.4 px per row):** counts `[1200, 500, 320, 150, 90, 40]` → widths `[480, 200, 128, 60, 36, 16]`; fills top to bottom blue `#2a78d6`, orange `#d95926`, aqua `#199e70`, violet `#4a3aa7`, magenta `#d55181`, yellow `#c98500`.
- **Value labels:** bold 12px, same color as each bar, 6px right of each bar end: "1,200", "500", "320", "150", "90", "40".
- **Annotation (bold 13px ink `#1a5276`, near x=420, y=200):** two lines: "2,300 missing phones —" / "six different spellings".
- **Caption (11px `#444`, bottom right):** "illustrative — one customer table, phone column audit".

## Null Is Not Zero (and Not Empty Either)

**Tags:** `common mistake` (red), `imputation` (orange)

- **Three strangers** — "no tip recorded" (null), a $0 tip, and an empty text box are three different facts
- **Known tips** — four customers tipped $2, $4, $3, $3; the average of what we know is $12 / 4 = $3.00
- **The fill-in** — a fifth tip is unknown; replacing that null with $0 makes the average $12 / 5 = $2.40
- **The distortion** — the reported average fell 20% not because anyone tipped less, but because of a fill
- **The honest options** — average the known values, or model the missingness; never let null vote as zero

*Example (italic):* One "cleaned" spreadsheet cell — a null turned into $0 — moved the shop's average tip from $3.00 to $2.40 and sparked a week of "why are tips down?" meetings.

**Common mistake:** Filling nulls with 0 (or "") to tidy the data — it silently changes the question from "the average of known tips" to "the average pretending unknown means nothing".

### Visualization (canvas `c4`, 720×300)

Two-bar comparison: the average of the four known tips next to the average after the null is filled with $0, with the raw tip list shown so the reader can redo both by hand.

- **Title (bold 15px, `#1a5276`, top center):** "One Null Filled with $0 Moves the Average from $3.00 to $2.40".
- **Axes:** origin x=170, baseline y=245, plot width 440, plot height 170; y = dollars 0 to 3.5 at 50 px per dollar, light `#e5e9ef` gridlines at $1, $2, $3 with 12px `#444` labels "$1", "$2", "$3" at x=140.
- **Raw data (12px `#444`, left margin at x=20, one value per line from y=100):** "tips:", "$2", "$4", "$3", "$3", then bold orange `#d95926` "? (null)".
- **Bar 1 (center x=290, width 120):** height 150 (=$3.00), fill green `rgba(0,131,0,0.35)`, 2px `#008300` border; bold 13px green value "$3.00" above; 12px `#444` label below baseline: "average of the 4 known tips".
- **Bar 2 (center x=510, width 120):** height 120 (=$2.40), fill red `rgba(231,76,60,0.25)`, 2px `#e74c3c` border; bold 13px red value "$2.40" above; 12px `#444` label below baseline: "null filled with $0 (5 tips)".
- **Drop marker:** dashed `#6b7280` (dash 4/3) horizontal line at the $3.00 level from bar 1 across bar 2, with a small down-arrow to the $2.40 top.
- **Annotation (bold 12px red `#e74c3c`, near x=510, y=90):** "the unknown tip became a $0 vote".
- **Caption (11px `#444`, bottom right):** "illustrative — five checkout receipts, one tip never recorded".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for genuine crash/error states (the c1 crash, the c2 crash slot, the c4 distorted average).
- **Special glyph:** Option⟨Phone⟩ uses angle brackets; in HTML render as `Option&lt;Phone&gt;` inside `<code>` or plain text — never raw `<` in markup.
- **Data:** all counts, slot positions, and bar values are the hardcoded literals above (no `Math.random()`); the missing-slot list `[4, 8, 11, 15, 18, 20]`, the encoding counts `[1200, 500, 320, 150, 90, 40]`, and the tips `[2, 4, 3, 3]` must match between text and chart exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
