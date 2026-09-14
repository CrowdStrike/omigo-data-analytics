# JSON Schema

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** JSON Schema

**Subtitle:** A rule sheet for data — written on the same kind of form as the data it checks, so a machine can do the checking

**Running example (all four sections):** a bakery takes cake orders through its website. Every order slip carries an order number, a cake from the menu, and how many. A rule sheet at the counter checks each slip before the kitchen sees it.

## The Slip the Kitchen Never Saw

**Tags:** `core idea` (blue), `a rule sheet` (green), `checked at the counter` (orange)

- **The shop** — a bakery takes cake orders from its website, hundreds of slips a day
- **The slip** — every order names an order number, a cake from the menu, and how many
- **The bad slip** — one app version writes how many as the word `two` instead of the digit 2
- **The clerk** — a rule sheet at the counter checks each slip before the kitchen ever sees it
- **The sheet** — plain rules: right shape, from the menu, a number, nothing left blank
- **The twist** — the rule sheet is written on the same kind of form as the slips it checks

*Example (italic):* Slip ORD-0143 says how many: `two`; the counter sends it straight back, and the kitchen only ever sees slips it can actually read.

**Key point:** A JSON Schema is a rule sheet for JSON, itself written in JSON — so the same machine that reads the data can read the rules and enforce them.

### Visualization (canvas `c1`, 720×300)

Side-by-side panels: the order slip on the left, the rule sheet on the right, a check/cross column between them aligned row by row.

- **Title (bold 15px, `#1a5276`, top center):** "One Slip, Four Rules — the Word 'two' Is What Breaks It".
- **Slip panel (x=25, y=52, 290px wide, 200px tall, 8px radius, white fill, 1.5px `#1a5276` border):** bold 13px `#1a5276` header at y=76 "Order slip — the data"; 12px `#6b7280` sub-label at y=96 "what the website sends".
- **Slip rows (label 12px `#6b7280` at x=42, value bold 12px at x=170):** y=126 "Order no." / `ORD-0143` in `#2c3e50`; y=158 "Cake" / `chocolate` in `#2c3e50`; y=190 "How many" / `two` in red `#e74c3c`; y=222 italic 12px `#6b7280` at x=42 "(all three boxes filled)".
- **Mark column (18px marks centered at x=348):** ✓ green `#008300` at y=126; ✓ at y=158; ✗ red `#e74c3c` at y=190; ✓ at y=222.
- **Rule panel (x=380, y=52, 315px wide, 200px tall, 8px radius, fill `rgba(26,82,118,0.06)`, 1.5px `#1a5276` border):** bold 13px `#1a5276` header at y=76 "Rule sheet — the schema"; 12px `#6b7280` sub-label at y=96 "written on the same kind of form".
- **Rule rows (12px `#2c3e50` at x=398, keyword in `#6b7280`):** y=126 "`ORD-` then four digits (pattern)"; y=158 "one of four menu cakes (enum)"; y=190 "a number, not a word (type)"; y=222 "no box left blank (required)".
- **Verdict (bold 13px red `#e74c3c`, left-aligned at x=25, y=276):** "sent back: how many must be a number, not a word".
- **Caption (12px `#444`, bottom right):** "slip illustrative".

## Four Slips Against Four Rules

**Tags:** `worked example` (blue), `check it by hand` (green)

- **Rule 1** — the order number reads `ORD-` then exactly four digits (`pattern`)
- **Rule 2** — the cake is one of four: chocolate, vanilla, lemon, red velvet (`enum`)
- **Rules 3 and 4** — how many is a number, not a word (`type`); no box left blank (`required`)
- **Slip A** — `ORD-0142`, chocolate, 2 — comes back clean on all four rules
- **Slips B and C** — B writes `two` as a word; C wants cheesecake and leaves how many blank
- **Slip D** — its order number is `142`, missing `ORD-`, so the shape rule fails on its own

*Example (italic):* Slip C breaks two rules at once — cheesecake is not on the menu and how many is blank — so the counter can name both faults in one breath.

**Key point:** Each rule is a small separate check, and a slip must pass every one; the report back names the exact rule broken, so the fix is never a guess.

### Visualization (canvas `c2`, 720×300)

Check matrix: 4 slip rows × 4 rule columns, each cell a green check or red cross, verdict column on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Four Slips, Four Rules — Only A Comes Back Clean".
- **Column headers (two lines, centered at x = 270, 370, 470, 570):** bold 12px `#1a5276` at y=56 "right shape" / "on the menu" / "a number" / "nothing blank"; 11px `#6b7280` at y=72 "(pattern)" / "(enum)" / "(type)" / "(required)". Verdict header bold 12px `#1a5276` at x=660, y=56.
- **Row labels (12px `#2c3e50`, left-aligned at x=20, rows at y = 105, 150, 195, 240):** "A  ORD-0142  chocolate ×2"; "B  ORD-0143  chocolate ×'two'"; "C  ORD-0144  cheesecake, blank"; "D  142  lemon ×1".
- **Cells (16px marks on the column x positions):** A `[✓, ✓, ✓, ✓]`; B `[✓, ✓, ✗, ✓]`; C `[✓, ✗, ✓, ✗]`; D `[✗, ✓, ✓, ✓]`. Checks green `#008300`, crosses red `#e74c3c`.
- **Rule note (why C's "a number" cell is a check):** a missing box breaks the nothing-blank rule only — the kind-of-thing rule has nothing to inspect. This is real JSON Schema behaviour: `type` applies only to keys that are present.
- **Verdicts (bold 12px at x=660):** A "PASS" green `#008300`; B, C, D "SENT BACK" red `#e74c3c`.
- **Gridlines:** light `#e5e9ef` horizontal rules between rows spanning x=20 to 700.
- **Annotation (bold 12px violet `#4a3aa7`, bottom center at y=278):** "one broken rule sends the whole slip back".
- **Caption (12px `#444`, bottom right):** "slips illustrative, rules exact".

## Catch It at the Counter or Re-check the Pile

**Tags:** `where it's used` (blue), `data quality` (green), `shared contract` (orange)

- **One shared sheet** — sender and receiver read the same rules, so nobody guesses the fields
- **Nightly loads** — a pipeline checks every row on the way in; bad rows go to a side bin
- **Settings files** — an app checks its own settings at startup, not at 3am when it falls over
- **Free paperwork** — forms, docs, and typed code can be generated straight from the rule sheet
- **The pile grows** — at 6 bad slips a day, noticing a month late means 180 slips to re-check
- **The cheapest place** — the counter is the only spot where the mess is still exactly one slip

*Example (italic):* The word-`two` slip is a one-minute fix at the counter; found in the month-end review it is 180 slips to trace, and the month's cake counts were wrong the whole time.

**Key point:** The check itself costs the same wherever you run it — what grows is the pile of bad records waiting behind it, so run it at the door.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: how many bad slips have piled up by the time the fault is noticed, assuming 6 bad slips a day.

- **Title (bold 15px, `#1a5276`, top center):** "Notice Later, Re-check More: Slips Piled Up at 6 a Day".
- **Axis:** 2px `#999` vertical baseline at x=270 from y=68 to y=252; horizontal axis line at y=252 from x=270 to x=690; ticks at x = 270, 410, 550, 690 with 12px `#444` labels "0", "60", "120", "180" at y=270; axis title 12px `#6b7280` centered at (480, 290) "bad slips to re-check".
- **Rows (y = 85, 130, 175, 220; left labels 12px `#444` at x=20; bars 18px tall starting at x=270; scale 420px ÷ 180 slips = 2.333 px per slip):**
  - "spotted at the counter — same minute": green `#008300` bar, 1 slip, width 2
  - "in the nightly tally — 1 day": blue `#2a78d6` bar, 6 slips, width 14
  - "in the weekly report — 7 days": orange `#d95926` bar, 42 slips, width 98
  - "at the month-end review — 30 days": red `#e74c3c` bar, 180 slips, width 420
- **Value labels (bold 12px matching each bar colour, at bar end + 8px):** "1 slip", "6 slips", "42 slips", "180 slips".
- **Annotation (bold 13px magenta `#d55181`, right-aligned at x=690, y=60):** "waiting a month = 180× the clean-up".
- **Arithmetic:** all counts are 6 slips/day × days elapsed (1 day → 6, 7 days → 42, 30 days → 180); the counter row is the single slip in hand. The 180× annotation is 180 ÷ 1.
- **Caption (12px `#444`, bottom right):** "6 bad slips a day illustrative; counts follow from it".

## Passing the Check Is Not Being Right

**Tags:** `common mistake` (red), `shape vs meaning` (orange)

- **The default** — extras the form never asked for pass silently (`additionalProperties`)
- **The margin note** — "make it large" in the margin passes, and the kitchen never reads it
- **Silent wrong** — the slip is valid, the cake comes out small, and nothing ever errored
- **The other trap** — 500 cakes is a whole number of at least 1, so every rule is satisfied
- **The fix** — forbid boxes the form never asked for, and set a sensible ceiling like 20
- **The limit** — the clerk checks the boxes, not whether the order makes any sense

*Example (italic):* Two slips sail through the counter — one with a margin note nobody reads, one asking for 500 cakes — and both end in the wrong cakes being baked.

**Common mistake:** Reading "passed the rule sheet" as "correct". Unasked-for extras are allowed by default, and in-range nonsense is still nonsense — rules check shape, not meaning.

### Visualization (canvas `c4`, 720×300)

Two-row flow: a slip, the counter stamping it valid, and the wrong cake coming out anyway.

- **Title (bold 15px, `#1a5276`, top center):** "Two Slips the Counter Waves Through — Both Wrong".
- **Row 1 (boxes at y=90, 44px tall, 8px radius; row label 12px `#444` at x=20, y=118 "extra note"):** blue box `rgba(42,120,214,0.15)` at x=120 (185px) reading `"make it large" in margin`; 3px `#6b7280` arrow to green box `rgba(0,131,0,0.12)` at x=330 (205px) reading "stamped ✓ (no rule forbids it)"; arrow to red box `rgba(231,76,60,0.12)` at x=550 (155px) reading "small cake baked"; bold 12px red `#e74c3c` note centered at (627, 152) "✗ wrong size, no error".
- **Row 2 (boxes at y=200; row label at x=20, y=228 "silly number"):** blue box at x=120 reading `how many: 500`; arrow to green box at x=330 reading "stamped ✓ (a number ≥ 1)"; arrow to red box at x=550 reading "500 cakes queued"; red note centered at (627, 262) "✗ nobody wanted 500".
- **Box text:** 12px `#2c3e50`, centred in each box.
- **Annotation (bold 13px orange `#d95926`, left-aligned at x=20, y=285):** "the counter checks the boxes, not whether the order makes sense".
- **Caption (12px `#444`, bottom right):** "slips illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Vocabulary rule:** the page teaches in counter-clerk language — rule sheet, slip, sent back, box left blank — and names each JSON Schema keyword (`type`, `required`, `enum`, `pattern`, `additionalProperties`) exactly once, in parentheses beside its plain-word equivalent.
- **Data:** all values are the hardcoded literals above (no randomness). The four slips A–D and their marks are invented and labelled illustrative. The c3 counts derive from one illustrative rate (6 bad slips/day × days elapsed), so the bars are a real linear scale rather than schematic widths. True JSON Schema behaviour asserted on this page: `additionalProperties` defaults to allowing extras; `type` constrains only keys that are present; `required` catches absent keys; `enum` and `pattern` restrict allowed values.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
