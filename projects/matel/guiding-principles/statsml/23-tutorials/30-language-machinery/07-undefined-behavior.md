# Undefined Behavior

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Undefined Behavior

**Subtitle:** A language's rulebook only promises outcomes for the moves it lists — undefined behavior is any move the rulebook refuses to talk about, where the program is allowed to do absolutely anything

## The Two-Button Press the Manual Never Mentions

**Tags:** `core idea` (blue), `the contract` (green), `no promises` (orange)

- **The machine** — a coffee machine's manual lists exactly what each move does: coin in, button 1 to 6
- **Covered moves** — pay and press button 3 and the manual promises: one cappuccino, every time
- **The gap** — press buttons 3 and 5 at the same instant; the manual has no row for that move
- **Undefined** — a move the manual refuses to describe: the maker owes you no particular outcome
- **Anything goes** — today two coffees, tomorrow a cold espresso, after an update it eats the coin
- **Languages too** — a programming language is a manual for compilers, with exactly this kind of gap

*Example (italic):* The manual's table has a row for every button but none for "two at once" — whatever the machine does then, it isn't breaking any promise.

**Key point:** Undefined behavior is not an error the system promises to catch — it is a move the contract never covers, so any outcome, sensible or absurd, is allowed.

### Visualization (canvas `c1`, 720×300)

The manual drawn as a two-column promise table: four covered moves with green promised outcomes, and a fifth row for the uncovered move whose promise cell is empty.

- **Title (bold 15px, `#1a5276`, top center):** "The Manual: Every Covered Move Has a Promise — One Move Has No Row".
- **Table region:** from (80, 75) to (640, 255); column split at x=380 ("the move" left, "the promise" right); bold 13px `#1a5276` column headers "the move" at x=90 and "the promise" at x=390, baseline y=68; 1px `#e5e9ef` row separators.
- **Rows (each 36px tall, tops at y = 75, 111, 147, 183, 219), 12px `#2c3e50` move text at x=90, vertically centered:**
  - "coin + button 1" → promise "one espresso"
  - "coin + button 2" → promise "one latte"
  - "coin + button 3" → promise "one cappuccino"
  - "coin + refund lever" → promise "coin returned"
  - "buttons 3 + 5 together" → promise cell empty
- **Promise cells (rows 1–4):** fill `rgba(0,131,0,0.10)`, 12px `#008300` text at x=390.
- **Promise cell (row 5):** fill `rgba(217,89,38,0.15)`, 1px dashed `#d95926` border, bold 12px `#d95926` centered text "— no promise —"; move text for this row also bold `#d95926`.
- **Annotation (bold 12px orange `#d95926`, two lines, right-aligned near x=640, y=282):** "no row means no promise —" / "that gap is undefined behavior".
- **Caption (12px `#444`, bottom left at x=80, y=290):** "illustrative — a real manual style, an invented machine".

## 120 Points + 10 Points = −126 Points

**Tags:** `worked example` (blue), `overflow` (orange)

- **The counter** — the machine keeps loyalty points in one signed byte, which holds only −128 to 127
- **The purchase** — a regular sits at 120 points and buys a 10-point bundle: 120 + 10 = 130
- **Doesn't fit** — 130 is past the byte's top of 127, and the rulebook calls that overflow undefined
- **What happened** — this machine wrapped around: 130 − 256 = −126, so the screen shows −126 points
- **No promise** — −126 is what happened, not what was promised; another build may show anything
- **By hand** — redo it: 120 + 10 = 130; 130 > 127 so it cannot fit; the wrap gives 130 − 256 = −126

*Example (italic):* A loyal customer watched 120 points turn into −126 after one purchase — the machine kept the receipt, the rulebook kept silent.

**Key point:** 120 + 10 = 130 does not fit in −128..127, so the addition is undefined; the wrap to −126 is one machine's accident, not an answer.

### Visualization (canvas `c2`, 720×300)

Number-line diagram of the signed byte's range with the balance at 120, an arrow for the +10 purchase arcing over the 127 edge, and the observed landing point at −126 on the far left.

- **Title (bold 15px, `#1a5276`, top center):** "One Byte of Loyalty Points: 120 + 10 Falls Off the Edge".
- **Axis:** horizontal 2px `#999` line at y=185 from x=60 to x=660 (width 600) mapping values −128 to 127 (x = 60 + (value + 128) × 600/255); 12px `#444` tick labels below at values −128, −64, 0, 64, 127 (x ≈ 60, 211, 361, 512, 660).
- **Range shading:** light `rgba(42,120,214,0.08)` band from x=60 to x=660, y=170 to y=200, 11px `#6b7280` label "everything a signed byte can hold" centered below the ticks at y=225.
- **Edge marker:** vertical dashed `#6b7280` (dash 4/3) line at value 127 (x≈660) from y=100 to the axis, 12px `#6b7280` label "top: 127" above it.
- **Start dot:** blue `#2a78d6` 7px dot on the axis at value 120 (x≈644), bold 12px blue label "balance 120" above-left of it.
- **Ghost value:** hollow 7px circle, 1px dashed `#6b7280` stroke, at x=690, y=185 (past the edge), 11px `#6b7280` label "130 doesn't fit" above it.
- **Wrap arrow:** orange `#d95926` 3px curve from the 120 dot up over the edge (control point near x=680, y=120), sweeping left across the top (through y≈115) and down to an orange 7px dot at value −126 (x≈65) with an arrowhead; bold 12px orange label "shows −126" below-right of the landing dot.
- **Annotation (bold 13px orange `#d95926`, two lines, centered near x=340, y=75):** "this machine wrapped: 130 − 256 = −126" / "— the contract promised nothing at all".
- **Caption (12px `#444`, bottom right):** "illustrative — signed one-byte counter, range −128 to 127".

## One Source File, Four Different Endings

**Tags:** `where it's used` (blue), `compilers` (green), `hidden bug` (red)

- **Not a crash** — undefined behavior usually does something quiet; a loud crash is the lucky case
- **Compiler logic** — the compiler may treat "the counter never passes 127" as a fact and build on it
- **Vanishing check** — the firmware's own overflow test can be deleted as "impossible", silently
- **Testing lies** — the factory build showed −126 every single time, so the bug passed a year of tests
- **Fragile luck** — a new compiler, a flag, or a different chip can change the ending with no code edit

*Example (italic):* The 2024 build wrapped to −126 through a year of tests; the 2026 rebuild of the identical source deleted the overflow check instead.

**Key point:** Whatever an undefined program does is a coincidence of one build — upgrade the compiler and the same source can wrap, halt, or drop its own safety check.

### Visualization (canvas `c3`, 720×300)

Four-row outcome chart on a shared displayed-balance axis: the same source compiled four ways, with each build's observed ending marked — two numeric outcomes, one halt, one runaway.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Firmware Source, Compiled Four Ways".
- **Axis:** horizontal 2px `#999` line at y=255 from x=230 to x=680 (width 450) mapping displayed balance −140 to 140 (x = 230 + (value + 140) × 450/280); 12px `#444` tick labels at −126, 0, 127 (x ≈ 253, 455, 659); vertical dashed `#6b7280` (dash 4/3) guide at 127 from y=70 to the axis, 11px `#6b7280` label "cap: 127" at its top.
- **Rows (y = 95, 140, 185, 230), each with a left-aligned 12px `#444` label at x=20:**
  - "factory build (2024)": blue `#2a78d6` 7px dot at −126 (x≈253), bold 12px blue label "−126" above it
  - "optimized build": orange `#d95926` 7px dot at 130 (x≈664), bold 12px orange label "130" above it
  - "checked debug build": red `#e74c3c` bold 14px "✕" at x=455, 12px red label "halts — no number at all" to its right
  - "rebuild, 2026 compiler": violet `#4a3aa7` 3px arrow from x=455 to x=672 with arrowhead, 11px violet label "cap check deleted — keeps counting" above it
- **Annotation (bold 13px magenta `#d55181`, centered near x=455, y=52):** "same source, four endings — none breaks the contract".
- **Caption (12px `#444`, bottom right):** "illustrative — build outcomes invented".

## No Promise, a Short Menu, or a Written-Down Choice

**Tags:** `common mistake` (red), `three labels` (orange)

- **Three phrases** — rulebooks have three different ways of not pinning something down, and they differ
- **Implementation-defined** — the maker must pick one behavior and document it: button 3's cup size
- **Unspecified** — one of a short listed set, no telling which: milk-then-sugar or sugar-then-milk
- **Undefined** — no list exists at all: the two-button press, the overflowing byte; anything is allowed
- **The mix-up** — people read "undefined" as "one of a few sane things"; that menu simply isn't there

*Example (italic):* Cup size is in the appendix (implementation-defined), pour order is either-or (unspecified), the two-button press is off the map (undefined).

**Common mistake:** Treating undefined behavior as a menu of reasonable outcomes. Unspecified has a menu; undefined has none — so a result you saw once, like −126, is not on any list you can rely on.

### Visualization (canvas `c4`, 720×300)

Three-lane diagram on a shared "possible outcomes" strip: one documented dot for implementation-defined, a three-dot menu for unspecified, and an edge-to-edge shaded band for undefined.

- **Title (bold 15px, `#1a5276`, top center):** "Three Kinds of 'We're Not Saying' — Only One Has No Menu".
- **Lanes (center lines at y = 105, 165, 225), each with a left-aligned 12px `#444` two-part label at x=20 (term bold `#1a5276`, example plain):**
  - "implementation-defined — cup size on button 3"
  - "unspecified — milk and sugar order"
  - "undefined — two buttons at once"
- **Outcome strip:** for each lane a 1px `#e5e9ef` baseline from x=260 to x=680; 11px `#6b7280` header "possible outcomes" centered at x=470, y=70.
- **Lane 1 marks:** one green `#008300` 8px dot at x=310; 12px green label "one outcome, written in the appendix" to its right.
- **Lane 2 marks:** three blue `#2a78d6` 8px dots at x=310, 370, 430; 12px blue label "one of a short listed set" to their right.
- **Lane 3 marks:** shaded band `rgba(217,89,38,0.18)` with 1px dashed `#d95926` border from x=270 to x=672, y=211 to y=239; bold 12px `#d95926` centered text "any outcome at all — including none".
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=400, y=272):** "the mistake: reading 'undefined' as if it had a menu".
- **Caption (12px `#444`, bottom right):** "illustrative — three contract strengths, one invented machine".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red only for the genuine failure state (the halting debug build in `c3`).
- **Data:** every table row, value, and marker position is the hardcoded literal above (no randomness); the worked numbers 120, 10, 130, 127, −126, and 130 − 256 = −126 must appear identically in text and charts; invented outcomes carry an "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
