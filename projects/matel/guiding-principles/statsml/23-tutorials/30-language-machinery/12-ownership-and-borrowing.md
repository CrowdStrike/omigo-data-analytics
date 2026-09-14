# Ownership & Borrowing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Ownership & Borrowing

**Subtitle:** Rust makes memory safety a compile-time proof: every value has exactly one owner, lending it out follows a strict sign-out sheet, and any program that breaks the rules is refused before it ever runs

## One Drill, One Name on the Sheet

**Tags:** `core idea` (blue), `single owner` (green), `moves` (orange)

- **The tool shed** — an apartment building shares one power drill, tracked on a sign-out sheet
- **The owner** — exactly one name is on the sheet at a time; on day 1 that name is Maya
- **Owner's job** — when the owner's name is crossed off for good, the drill goes back in the shed (freed)
- **The move** — on day 3 Maya hands the drill to Omar; the sheet now says Omar, not Maya
- **Access revoked** — on day 4 Maya reaches for the drill anyway; the sheet says no — she gave it away
- **In Rust** — assigning or passing a value is that handover: the old variable is dead at compile time

*Example (italic):* Maya hands Omar the drill on day 3, then tries to drill a shelf on day 4 — the sheet (the compiler) refuses: "value moved on day 3".

**Key point:** Every value has exactly one owner; handing it over is a move, and the compiler refuses any later use through the old name — that's use-after-free made impossible.

### Visualization (canvas `c1`, 720×300)

Two-row timeline: Maya's ownership bar, a move arrow on day 3, Omar's ownership bar, and a red X where Maya's day-4 attempt is rejected at compile time.

- **Title (bold 15px, `#1a5276`, top center):** "One Owner at a Time: the Move on Day 3".
- **Axis:** horizontal 2px `#999` line at y=250 from x=160 to x=680 (width 520), days 1 to 5; tick labels "day 1" ... "day 5" (12px `#444`) below, evenly spaced at x = `[160, 290, 420, 550, 680]`.
- **Row labels (12px `#444`, left-aligned at x=20):** "Maya" at y=110, "Omar" at y=180.
- **Maya's bar:** 16px-tall rounded bar, fill `rgba(42,120,214,0.30)`, border 2px blue `#2a78d6`, from day 1 (x=160) to day 3 (x=420), centered on y=110; bold 12px blue label "owns the drill" above it.
- **Move arrow:** 3px blue `#2a78d6` diagonal arrow with arrowhead from (420, 118) down to (420, 172); bold 12px blue label "move (handover)" to its right at x≈432, y=145.
- **Omar's bar:** same style in green, fill `rgba(0,131,0,0.30)`, border 2px `#008300`, from day 3 (x=420) to day 5 (x=680), centered on y=180; bold 12px green label "owns it now" above it.
- **Rejected use:** bold red `#e74c3c` X mark (two 3px strokes, 14px tall) on Maya's row at day 4 (x=550, y=110); 12px red label "day-4 use refused" above it.
- **Annotation (bold 13px orange `#d95926`, near x=175, y=65):** two lines: "the old name is dead at compile time —" / "no run needed to catch it".
- **Caption (12px `#444`, bottom right):** "illustrative — days stand for lines of code".

## Seven Lines on the Sign-Out Sheet

**Tags:** `worked example` (blue), `borrow rules` (green)

- **Borrowing** — neighbors can borrow without taking ownership: Maya's name stays on the sheet
- **Two kinds** — a look-only borrow (read the manual) or an exclusive borrow (swap the drill bit)
- **The rule** — at any moment: any number of lookers, or exactly one changer — never both at once
- **Lines 2–3** — Sam then Priya borrow to read: 2 readers, 0 writers — the sheet allows it
- **Line 5** — after both return, Omar signs out to change the bit: 0 readers, 1 writer — allowed
- **Line 6** — Sam asks to read mid-swap: 1 reader + 1 writer — refused; line 7, Omar returns it

*Example (italic):* Check line 6 by hand: readers 1, writers 1 — the rule "at most one writer, and readers × writers = 0" fails, so the request is refused before anyone touches the drill.

**Key point:** The borrow checker is this sheet: many simultaneous readers OR one exclusive writer, never both — and every line is checked before the program runs.

### Visualization (canvas `c2`, 720×300)

Gantt-style borrow chart: one lifetime bar per borrower across sheet lines 1–7, with a rule-check label on each phase and a red X on the refused line-6 request.

- **Title (bold 15px, `#1a5276`, top center):** "The Sign-Out Sheet: Many Readers or One Writer".
- **Axis:** horizontal 2px `#999` line at y=252 from x=200 to x=690 (width 490), sheet lines 1 to 7; tick labels "1"–"7" (12px `#444`) at x = `[200, 281.7, 363.3, 445, 526.7, 608.3, 690]`; 12px `#444` axis caption "line on the sheet" centered below at y=288.
- **Row labels (12px `#444`, left-aligned at x=20):** "Maya (owner)" y=88, "Sam (read)" y=132, "Priya (read)" y=176, "Omar (write)" y=220.
- **Maya's bar:** 12px-tall bar, fill `rgba(42,120,214,0.30)`, border 2px blue `#2a78d6`, lines 1–7 (x=200 to 690), y=88; 11px blue label "ownership never moves" inside.
- **Sam's read bar:** 12px-tall bar, fill `rgba(25,158,112,0.30)`, border 2px aqua `#199e70`, lines 2–4 (x=281.7 to 445), y=132.
- **Priya's read bar:** same aqua style, lines 3–4 (x=363.3 to 445), y=176.
- **Omar's write bar:** 12px-tall bar, fill `rgba(217,89,38,0.30)`, border 2px orange `#d95926`, lines 5–7 (x=526.7 to 690), y=220.
- **Rule checks (bold 12px):** aqua `#199e70` label "2 readers, 0 writers — OK" above line 3.5 at (x≈404, y=112); orange `#d95926` label "0 readers, 1 writer — OK" above line 6 at (x≈608, y=200).
- **Refused request:** bold red `#e74c3c` X mark (two 3px strokes, 14px tall) on Sam's row at line 6 (x=608.3, y=132); bold 12px red label "1 reader + 1 writer — refused" right-aligned to its left at (x≈590, y=136).
- **Annotation (bold 13px violet `#4a3aa7`, near x=210, y=58):** "at most one writer, and readers × writers = 0 — on every line".
- **Caption (12px `#444`, bottom right):** "illustrative — one drill, seven sheet lines".

## Bugs That Never Reach Run Time

**Tags:** `where it's used` (blue), `memory safety` (green), `no garbage collector` (orange)

- **Use-after-free** — using the drill after giving it away; the move rule makes it a compile error
- **Double free** — two people returning the same drill to the shed; one owner means one return, ever
- **Data race** — reading the bit while someone swaps it; readers-or-one-writer forbids the overlap
- **Dangling reference** — a borrow that outlives the owner; borrows must end before the owner does
- **The proof** — the compiler checks every line like the sheet, so a compiled program can't hit these
- **The price** — no garbage collector, zero runtime cost for these checks — proven at compile time

*Example (italic):* The same four bugs in C surface as crashes at run time — sometimes months later in production — while Rust's checker rejects each one at compile time, before the program exists.

**Key point:** Ownership plus the borrow rules turn four classic memory bugs from runtime surprises into compile-time refusals — safety is a proof, not a hope.

### Visualization (canvas `c3`, 720×300)

Dot chart: four classic memory bugs as rows, each with a marker on a "when is it caught" axis — Rust markers at compile time, C markers at run time.

- **Title (bold 15px, `#1a5276`, top center):** "When the Bug Is Caught: Compile Time vs Run Time".
- **Axis:** horizontal 2px `#999` line at y=250 from x=230 to x=680 (width 450); two 13px `#444` zone labels below: "compile time" centered at x=320, "run time (in production)" centered at x=590; vertical dashed `#6b7280` (dash 4/3) divider at x=455 from y=60 to the axis.
- **Row labels (12px `#444`, left-aligned at x=20):** "use-after-free" y=95, "double free" y=135, "data race" y=175, "dangling reference" y=215.
- **Rust markers:** green `#008300` 8px dots at x=320 on each of the four rows; 11px green label "Rust" above the top dot at (320, 78).
- **C markers:** red `#e74c3c` 8px dots at x=590 on each of the four rows; 11px red label "C" above the top dot at (590, 78).
- **Connectors:** light 1px `#e5e9ef` line from each green dot to its red dot across the row.
- **Annotation (bold 13px green `#008300`, near x=240, y=55):** two lines: "all four refused before the program runs —" / "the compiler is the proof".
- **Caption (12px `#444`, bottom right):** "illustrative — catch points, not measurements".

## Borrowing Is Not Copying

**Tags:** `common mistake` (red), `clone vs borrow` (orange)

- **The album** — the value is a 1,000-photo album; three ways to share it with a friend
- **Borrow** — hand over a viewing pass: still 1,000 photos stored, one extra reference card
- **Clone** — print a full duplicate: 2,000 photos stored, and edits to the copy never reach the original
- **Move** — hand over the album itself: still 1,000 photos, but now the friend owns it and you don't
- **The mistake** — cloning everywhere "to make the borrow checker shut up": correct, but 2× the memory
- **Rule of thumb** — reach for a borrow first, move when handing off for good, clone only on purpose

*Example (italic):* Newcomers see "cannot borrow" errors, sprinkle clone on every line, and the 1,000-photo album quietly becomes 2,000 stored photos with two diverging versions.

**Common mistake:** Treating borrow, clone, and move as interchangeable. A borrow shares the one album, a clone doubles it to 2,000 photos, a move changes whose name is on it — pick deliberately.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: photos stored after each of the three ways to share the 1,000-photo album, with the ownership outcome labeled on each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Sharing a 1,000-Photo Album: Borrow vs Clone vs Move".
- **Axis:** horizontal 2px `#999` baseline at y=250 from x=200 to x=680 (width 480), photos stored 0 to 2,000; tick labels "0", "500", "1,000", "1,500", "2,000" (12px `#444`) at x = `[200, 320, 440, 560, 680]`; light `#e5e9ef` vertical gridlines at each tick.
- **Rows (bars 26px tall, centered at y = 100, 150, 200), left-aligned 12px `#444` labels at x=20:** "borrow", "clone", "move".
- **Bar data (hardcoded):** photos stored = `[1000, 2000, 1000]`, so bar right edges at x = `[440, 680, 440]`.
- **Bar styles:** borrow fill `rgba(0,131,0,0.30)` border 2px green `#008300`; clone fill `rgba(231,76,60,0.20)` border 2px red `#e74c3c`; move fill `rgba(42,120,214,0.30)` border 2px blue `#2a78d6`.
- **Value labels (bold 12px, at each bar's right edge + 8px, same hue as border):** "1,000 + a pass", "2,000", "1,000".
- **Outcome labels (11px `#6b7280`, inside each bar at x=210):** "you still own it", "two diverging albums", "friend owns it now".
- **Annotation (bold 13px red `#e74c3c`, near x=460, y=125):** two lines: "reflex-cloning doubles storage —" / "borrow first, clone on purpose".
- **Caption (12px `#444`, bottom right):** "illustrative — one photo, one stored unit".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, violet `#4a3aa7`, orange `#d95926`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red is used only for refused/error marks (compile-time rejections), per palette rules.
- **Data:** all timelines, bar lengths, and dot positions are the hardcoded coordinates and arrays above (no `Math.random()`); the sheet-line story, day numbers, reader/writer counts, and photo counts in the text must match the charts exactly (2 readers + 0 writers OK, 1 reader + 1 writer refused, 1,000 vs 2,000 photos).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
