# Macros

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Macros

**Subtitle:** A macro is a rule that rewrites your text into longer text before anything runs — and the big split is between blind find-and-replace and rewriters that actually read the sentence

## A Shortcut Card Taped to the Register

**Tags:** `core idea` (blue), `code writes code` (green), `rewrite first` (orange)

- **The shortcut** — a coffee shop tapes a card to the register: "HALF(x) means x ÷ 2"
- **The rewrite** — a macro is exactly that card: a rule that turns short text into longer text
- **Before running** — the rewrite happens first; the register only ever sees the expanded text
- **HALF(10)** — the card turns it into "10 ÷ 2", and only then does the register compute $5
- **Code writing code** — programmers use macros so one short line can expand into many real lines

*Example (italic):* On a $10 gift box the cashier writes HALF(10), the card expands it to 10 ÷ 2, and the register rings up $5.

**Key point:** A macro is a rewrite rule — it turns the text you wrote into the text that actually runs, before anything is computed.

### Visualization (canvas `c1`, 720×300)

Left-to-right pipeline diagram: what you write, the expansion step, what actually runs, then the computed result — making the two-step nature (rewrite, then run) visible.

- **Title (bold 15px, `#1a5276`, top center):** "A Macro Runs in Two Steps: Rewrite First, Then Run".
- **Box 1 (rounded rect x=40–220, y=115–185):** fill `rgba(42,120,214,0.12)`, 1px `#2a78d6` border; 12px `#6b7280` label "you write:" inside at top, bold 14px `#1a5276` monospace "HALF(10)" centered beneath.
- **Arrow 1 (3px `#6b7280`, x=220→300 at y=150, arrowhead):** bold 12px `#d95926` label above: "step 1: expand"; 11px `#6b7280` label below: "text becomes text".
- **Box 2 (rounded rect x=300–480, y=115–185):** fill `rgba(217,89,38,0.10)`, 1px `#d95926` border; 12px `#6b7280` label "the program becomes:", bold 14px `#1a5276` monospace "10 ÷ 2".
- **Arrow 2 (3px `#6b7280`, x=480→560 at y=150, arrowhead):** bold 12px `#008300` label above: "step 2: run".
- **Result circle:** violet `#4a3aa7` filled circle, radius 28, center (620, 150), bold 16px white "$5" inside; 11px `#6b7280` label "computed at last" beneath at y=195.
- **Annotation (bold 12px `#1a5276`, centered near x=360, y=235):** "the macro never saw the number 5 — it only rearranged text".
- **Caption (11px `#444`, bottom right):** "illustrative — the shop's half-off shortcut on a $10 order".

## The Day HALF(4 + 6) Charged $7

**Tags:** `worked example` (blue), `pencil and paper` (green), `paste trap` (red)

- **The combo** — a $4 muffin plus a $6 coffee, sold as a half-price bundle: HALF(4 + 6)
- **Blind paste** — replace x with the characters "4 + 6": the register now reads "4 + 6 ÷ 2"
- **Order of operations** — division runs before addition, so it computes 4 + 3 and charges $7
- **The right answer** — half of the $10 bundle is $5; the blind paste overcharged by $2
- **The fix** — a syntax-aware expander pastes the bundle as one sealed piece: "(4 + 6) ÷ 2" = $5

*Example (italic):* Same shortcut, same order: character-pasting rings up $7, structure-aware pasting rings up $5 — redo both by hand in ten seconds.

**Key point:** Textual expansion gave 4 + 6 ÷ 2 = 7; treating "4 + 6" as one sealed piece gives (4 + 6) ÷ 2 = 5.

### Visualization (canvas `c2`, 720×300)

Two-lane expansion diagram: the same input HALF(4 + 6) flows through a textual expander (top lane, wrong $7) and a syntax-aware expander (bottom lane, right $5).

- **Title (bold 15px, `#1a5276`, top center):** "Same Shortcut, Two Expanders: $7 vs $5 on the Same Order".
- **Input box (rounded rect x=30–170, y=120–180):** fill `rgba(42,120,214,0.12)`, 1px `#2a78d6` border, bold 13px `#1a5276` monospace "HALF(4 + 6)" centered; 3px `#6b7280` arrows fork from its right edge to the two lanes.
- **Lane 1 (centered y=95), label bold 12px `#e74c3c` at (210, 60):** "textual: paste the characters"; rounded box x=210–390 with 1px `#e74c3c` border, monospace 13px `#2c3e50` "4 + 6 ÷ 2"; 3px `#6b7280` arrow; box x=440–540, 1px `#e74c3c` border, 13px "4 + 3" with 11px `#6b7280` sub-label "÷ ran before +"; arrow to a red `#e74c3c` filled circle radius 24 at (630, 95), bold 15px white "$7", 11px `#e74c3c` label "wrong" beneath.
- **Lane 2 (centered y=210), label bold 12px `#008300` at (210, 175):** "syntax-aware: paste the bundle"; rounded box x=210–390 with 1px `#008300` border, monospace 13px "(4 + 6) ÷ 2"; arrow; box x=440–540, 1px `#008300` border, 13px "10 ÷ 2"; arrow to a green `#008300` filled circle radius 24 at (630, 210), bold 15px white "$5", 11px `#008300` label "right" beneath.
- **Annotation (bold 12px `#e74c3c`, near x=390, y=152, between the lanes):** "a $2 overcharge from a missing pair of parentheses".
- **Caption (11px `#444`, bottom left):** "illustrative — a $4 muffin + $6 coffee, half off".

## Blind Paste vs. Reading the Sentence

**Tags:** `where it's used` (blue), `two families` (green), `hygiene` (orange)

- **Textual macros** — C's #define is find-and-replace on characters; it cannot see any structure
- **Syntax-aware macros** — Rust and Lisp macros rewrite the parsed tree, so pieces stay glued
- **SQL templates** — tools that paste text into queries are textual macros, with the same paste traps
- **Defensive parens** — C programmers wrap every macro argument in parentheses to fake the sealed bag
- **Hygiene** — some syntax-aware macros (Rust, Scheme) keep their names from colliding with yours
- **Where you meet it** — header constants, logging macros, code generators, query templates

*Example (italic):* A pasted SQL filter "a OR b" dropped after "WHERE x AND" silently becomes "x AND a OR b" — the $7 bug wearing a query costume.

**Key point:** The whole textual-vs-syntax-aware split is one question: does the expander see characters, or the sentence's structure?

### Visualization (canvas `c3`, 720×300)

Two-panel contrast: the left panel shows what a textual expander sees in "4 + 6" (a flat row of characters); the right panel shows what a syntax-aware expander sees (a small parse tree where the sum is one branch).

- **Title (bold 15px, `#1a5276`, top center):** "What Each Expander Sees in HALF(4 + 6)".
- **Left panel (x=40–330):** header bold 13px `#d95926` at (60, 65): "textual — a row of characters"; five character cells 30×34 starting at x=70, y=120, gap 8, fill `rgba(217,89,38,0.10)`, 1px `#d95926` border, bold 14px `#2c3e50` contents "4", " ", "+", " ", "6"; 11px `#6b7280` sub-label at (60, 190): "just letters — no idea '+' glues things together".
- **Right panel (x=380–700):** header bold 13px `#008300` at (400, 65): "syntax-aware — a little tree"; nodes as circles radius 16 with bold 13px `#2c3e50` text: "÷" at (545, 105), "+" at (485, 175), "2" at (605, 175), "4" at (445, 240), "6" at (525, 240); 2px `#6b7280` lines connecting parent to children; the "+", "4", "6" nodes filled `rgba(0,131,0,0.15)` with 1px `#008300` border, the "÷" and "2" nodes filled `#fff` with 1px `#6b7280` border; 11px `#6b7280` sub-label at (400, 275): "the whole '4 + 6' branch moves as one piece".
- **Divider:** 1px `#e5e9ef` vertical line at x=355 from y=55 to y=280.
- **Annotation (bold 12px `#4a3aa7`, near x=430, y=130):** "structure in, structure out — no parentheses accidents".
- **Caption (11px `#444`, bottom right):** "same 4 + 6 bundle as the worked example".

## A Macro Is Not a Function

**Tags:** `common mistake` (red), `when work happens` (orange)

- **A function** — is called while the program runs: HALF as a function receives 10 and returns 5
- **A macro** — is expanded before the program runs: it rearranges text and never sees a value
- **Copies** — use a macro in 3 places and 3 pasted copies exist; a function keeps 1 body, 3 calls
- **New syntax** — macros can invent what functions cannot: new loops, new statements, mini-languages
- **Debugging** — errors point at expanded text you never wrote, which makes macros harder to trace

*Example (italic):* The half-off card and a calculator's ÷2 button both produce $5 on the $10 box — but only the card rewrote the sentence to get there.

**Common mistake:** Reaching for a macro when a plain function does the job. Functions see real values and are easy to debug — save macros for when the text itself must change.

### Visualization (canvas `c4`, 720×300)

Timeline diagram with two phase bands (before the program runs, while it runs): the macro does its work in the first band and leaves 3 pasted copies; the function keeps 1 body and does its work in the second band.

- **Title (bold 15px, `#1a5276`, top center):** "Function vs Macro: When the Work Happens".
- **Phase bands (y=65–255):** left rounded rect x=50–360, fill `rgba(42,120,214,0.08)`, bold 13px `#1a5276` header "before the program runs" centered at y=85; right rounded rect x=380–690, fill `rgba(0,131,0,0.08)`, bold 13px `#1a5276` header "while it runs" centered at y=85.
- **Macro row (y=140), bold 12px `#d95926` row label "macro" at x=60:** three small orange `#d95926` 20×20 squares at x=150, 180, 210 with 11px `#6b7280` label beneath at y=170: "3 uses → 3 pasted copies"; 3px `#6b7280` arrow from x=250 to x=430 at y=140; 12px `#444` note at (440, 144): "the pasted text just runs".
- **Function row (y=215), bold 12px `#008300` row label "function" at x=60:** 11px `#6b7280` note at (150, 219): "body written once, nothing happens yet"; 3px `#6b7280` arrow from x=330 to x=430 at y=215; one green `#008300` 20×20 square at x=440 with 11px `#6b7280` label beneath at y=245: "3 uses → 1 body, 3 calls with real values".
- **Divider:** 1px dashed `#6b7280` (dash 4/3) vertical line at x=370 from y=65 to y=255.
- **Annotation (bold 12px `#1a5276`, near x=520, y=110):** "a macro rearranges text; a function computes values".
- **Caption (11px `#444`, bottom right):** "illustrative — the same HALF shortcut used in 3 places".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box labels, expansion strings, and dollar results are the hardcoded literals above (no randomness); HALF(10) → "10 ÷ 2" → $5, HALF(4 + 6) → "4 + 6 ÷ 2" = $7 (wrong) vs "(4 + 6) ÷ 2" = $5 (right), the $2 overcharge, and "3 uses → 3 copies vs 1 body" must agree across all four charts and the text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
