# Regular Expressions as Machines

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Regular Expressions as Machines

**Subtitle:** Every regex is secretly a little machine — a row of states that reads one character at a time and either walks all the way to the finish circle or falls off

## A Clerk With a Finger on a Diagram

**Tags:** `core idea` (blue), `states` (green), `one character at a time` (orange)

- **The stamp** — a warehouse clerk checks order IDs like "AB-1234": two letters, a dash, four digits
- **The regex** — the rule written compactly is `[A-Z][A-Z]-[0-9][0-9][0-9][0-9]`, one slot per character
- **The machine** — the same rule drawn as 8 circles in a row; each circle is a state, "how far along am I"
- **One step per character** — read a character, follow the arrow it matches, move a finger one circle right
- **Accept or reject** — finger on the double circle at the end means valid; any wrong character means reject
- **No magic** — a regex engine is exactly this: the pattern is compiled into circles and arrows first

*Example (italic):* Checking "AB-1234" is eight finger positions: start, after A, after B, after the dash, then one hop per digit until the finish circle.

**Key point:** A regex is a compact spelling of a finite machine — states for progress, arrows for allowed characters, one hop per character read.

### Visualization (canvas `c1`, 720×300)

Single-row state diagram: the eight states of `[A-Z][A-Z]-[0-9][0-9][0-9][0-9]` drawn as circles connected by labeled arrows, with the accept state double-circled.

- **Title (bold 15px, `#1a5276`, top center):** "The Machine Behind [A-Z][A-Z]-[0-9]{4}".
- **States:** eight circles radius 20, centers at y=150, x = `[60, 146, 232, 318, 404, 490, 576, 662]`; fill white, 2px `#2a78d6` stroke; bold 13px `#1a5276` labels inside: "S0"–"S7"; S7 gets a second concentric circle radius 25 (accept state) in green `#008300`, and a bold 12px green label "accept" below it at y=195.
- **Start marker:** short 2px `#6b7280` arrow from x=20 into S0, 11px `#6b7280` label "start" above it.
- **Transition arrows:** 2px `#2c3e50` arrows with arrowheads between consecutive circles; 12px `#2c3e50` labels above each arrow at y=118: "A–Z", "A–Z", "-", "0–9", "0–9", "0–9", "0–9".
- **Reject note (12px `#e74c3c`, centered at y=235):** "any character that has no arrow → reject (fall off the machine)".
- **Annotation (bold 12px orange `#d95926`, near x=360, y=70):** "one circle per character of progress — the finger only moves right".
- **Caption (12px `#444`, bottom right):** "illustrative — order-ID checker drawn as a finite machine".

## Tracing AB-1234 Through the States

**Tags:** `worked example` (blue), `trace` (green)

- **The good ID** — feed "AB-1234": A moves S0→S1, B moves S1→S2, the dash moves S2→S3
- **The digits** — then 1, 2, 3, 4 move the finger S3→S4→S5→S6→S7: finish circle, so it is valid
- **The bad ID** — feed "AB-12X4": the first five characters land on S5 exactly as before
- **The fall** — character 6 is "X", but S5 only has a 0–9 arrow, so the machine rejects on the spot
- **No rereading** — the machine never looks back at earlier characters; 7 characters cost 7 hops, always

*Example (italic):* "AB-1234" takes the walk 0, 1, 2, 3, 4, 5, 6, 7 and lands on accept; "AB-12X4" walks 0, 1, 2, 3, 4, 5 and falls off at character 6.

**Key point:** Checking a string is one hop per character — "AB-1234" reaches state 7 and is accepted, "AB-12X4" dies at character 6 with no backtracking.

### Visualization (canvas `c2`, 720×300)

Two-trace staircase chart: state number versus characters read, showing the accepted ID climbing to state 7 and the rejected ID falling off at character 6.

- **Title (bold 15px, `#1a5276`, top center):** "Two IDs Walk the Machine: One Reaches Accept, One Falls Off".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = characters read 0 to 7, tick positions x = `[70, 153, 236, 319, 402, 485, 568, 651]` with 12px `#444` labels "start", "A", "B", "-", "1", "2", "3|X", "4"; y = state number 0 to 7, 12px `#444` labels "S0"–"S7" every state, light `#e5e9ef` gridlines.
- **Accepted trace ("AB-1234"):** green `#008300` 3px stepped line through states `[0, 1, 2, 3, 4, 5, 6, 7]` at the eight tick positions; 6px green dots at each step; bold 12px green label "AB-1234 → accept" near the top right (x≈540, y=70).
- **Rejected trace ("AB-12X4"):** blue `#2a78d6` 3px stepped line, dash 6/4, through states `[0, 1, 2, 3, 4, 5]` for the first six positions, then a red `#e74c3c` bold 16px "X" marker at (x=568, y=112) where character "X" has no arrow; bold 12px red label "AB-12X4 → reject at char 6" at (x≈400, y=125).
- **Accept line:** horizontal dashed `#6b7280` (dash 4/3) line at state 7; 11px `#6b7280` label "accept state" at its left end.
- **Annotation (bold 12px orange `#d95926`, near x=180, y=95):** "7 characters = 7 hops — never re-reads".
- **Caption (12px `#444`, bottom right):** "illustrative traces on the order-ID machine".

## Why Fast Search Tools Trust the Machine

**Tags:** `where it's used` (blue), `performance` (green), `catastrophic backtracking` (red)

- **Log scanning** — a data scientist greps millions of lines; the machine view is why that can be fast
- **Linear time** — a true machine costs one hop per character: a 28-character line is 28 hops
- **The other way** — engines that instead guess-and-backtrack can retry paths, doubling work per letter
- **The blowup** — a trap pattern like `(a+)+$` on 20 a's plus a b tries about 1.0M paths; 28 a's, 268M
- **Real outages** — such runaway regexes have frozen production log filters and input validators
- **The fix** — machine-based engines (grep, RE2-style) cap the cost at one state-set hop per character

*Example (italic):* On the trap pattern, adding 8 letters to the line grows backtracking work from 1.0M to 268M tries, while the machine goes from 21 to 29 steps.

**Key point:** Compiled to a machine, matching is one hop per character; simulated by backtracking, a bad pattern can double its work with every extra letter.

### Visualization (canvas `c3`, 720×300)

Bar chart of backtracking tries versus line length for the trap pattern, with the machine's step count drawn as a flat green line hugging the floor.

- **Title (bold 15px, `#1a5276`, top center):** "Trap Pattern (a+)+$: Backtracking Tries vs Machine Steps".
- **Axes:** origin x=80, baseline y=245, plot width 560, plot height 180; x = letters in the line, five bar positions centered at x = `[150, 260, 370, 480, 590]` with 12px `#444` labels "20", "22", "24", "26", "28" and 12px `#444` axis title "letters in the line" at y=280; y linear 0 to 280M with 12px `#444` labels "0", "70M", "140M", "210M", "280M" and light `#e5e9ef` gridlines.
- **Backtracking bars:** widths 70px, fill `rgba(26,82,118,0.35)` with 2px `#1a5276` stroke, tries = `[1.0M, 4.2M, 16.8M, 67.1M, 268M]` (values 1048576, 4194304, 16777216, 67108864, 268435456); bold 12px `#1a5276` value labels above each bar: "1.0M", "4.2M", "16.8M", "67.1M", "268M".
- **Machine line:** green `#008300` 3px horizontal line just above the baseline (y=243) across all bars, steps = `[21, 23, 25, 27, 29]`; bold 12px green label "machine: 21–29 steps (too small to see)" at (x≈150, y=228).
- **Annotation (bold 13px red `#e74c3c`, near x=390, y=85):** two lines: "work doubles with every extra letter —" / "the machine stays one hop per character".
- **Caption (12px `#444`, bottom right):** "tries are 2^n for the trap pattern; scenario illustrative".

## The Machine Has No Memory

**Tags:** `common mistake` (red), `finite states` (orange)

- **The belief** — people imagine the regex "remembers" the whole string it has read so far
- **The truth** — the machine knows only which circle the finger is on; the past is thrown away
- **Same circle, same future** — "AB-12", "ZQ-98", and "KK-00" all land on S5 and get identical treatment
- **What that costs** — counting needs a state per count, so no finite machine can match balanced brackets
- **The mistake** — reaching for a regex to validate nested JSON or HTML; that job needs a real parser

*Example (italic):* After "AB-12" or "ZQ-98" the machine is in the same state S5, so "34" finishes both and "3X" kills both — it cannot treat them differently.

**Common mistake:** Expecting a regex to count or remember. A finite machine only knows its current state, which is exactly why regexes cannot match arbitrarily nested brackets.

### Visualization (canvas `c4`, 720×300)

Funnel diagram: three different prefixes converging by arrow into one shared state circle, which then fans out to the same two futures for all of them.

- **Title (bold 15px, `#1a5276`, top center):** "Three Pasts, One State — the Machine Forgot How It Got There".
- **Prefix labels (bold 13px `#2a78d6`, left-aligned at x=60):** three monospace-styled strings at y = `[95, 150, 205]`: "AB-12", "ZQ-98", "KK-00".
- **Converging arrows:** three 2px `#2a78d6` arrows with arrowheads from x=150 at those same y values to the shared circle edge.
- **Shared state:** circle radius 30 centered at (330, 150), fill `rgba(42,120,214,0.15)`, 3px `#2a78d6` stroke, bold 14px `#1a5276` label "S5" inside, 11px `#6b7280` label "five characters done" below at y=195.
- **Fan-out arrows:** two 2px arrows from the circle's right edge — green `#008300` to (560, 110) with bold 12px green label "reads \"34\" → accept" at (570, 114); red `#e74c3c` to (560, 190) with bold 12px red label "reads \"3X\" → reject" at (570, 194).
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=262):** "same state = same future — no memory of the past, so no counting".
- **Caption (12px `#444`, bottom right):** "illustrative — why regexes cannot match nested brackets".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red used only for genuine reject/error states.
- **Data:** all state positions, trace paths, and bar values are the hardcoded arrays above (no randomness); c3 tries are exact powers of two (2^20 through 2^28) rounded for display; arrows and circles drawn with plain canvas paths (no external libraries).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
