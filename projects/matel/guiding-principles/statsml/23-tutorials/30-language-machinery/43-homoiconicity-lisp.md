# Homoiconicity (Lisp)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Homoiconicity (Lisp)

**Subtitle:** In Lisp a program is written as an ordinary list — so the same everyday tools that edit lists can read, check, and rewrite the program itself

## A Bill, a Tip, and a List of Three Things

**Tags:** `core idea` (blue), `code as data` (green), `lisp` (orange)

- **The bill** — dinner comes to $45 and the diner wants a program that computes a 20% tip
- **In Lisp** — the tip formula is written `(* 0.20 45)`: a bracket, three items, a bracket
- **Run it** — ask Lisp to evaluate that and it multiplies: 0.20 × 45 = 9.00, a $9.00 tip
- **Read it** — the very same expression is also an ordinary 3-item list: `*`, `0.20`, `45`
- **The word** — a language is homoiconic when its code is written in its own everyday data shape

*Example (italic):* The formula `(* 0.20 45)` is simultaneously a program that returns 9.00 and a plain list you could count, copy, or reorder like a shopping list.

**Key point:** In Lisp, code IS a list — the same brackets-and-items structure used for any other data, with no separate "code format" in between.

### Visualization (canvas `c1`, 720×300)

Single-panel "two readings" diagram: the expression drawn once as three list boxes on the left, with two arrows fanning right — one to a "run it as code" result panel, one to a "read it as data" list panel.

- **Title (bold 15px, `#1a5276`, top center):** "One Expression, Two Readings: Run It, or Read It as a List".
- **List boxes (left, centered at y=150):** three rounded 62×40 boxes at x=70, 140, 210; 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.10)`; bold 14px `#2c3e50` centered contents "*", "0.20", "45"; 12px `#6b7280` label below at y=205: "the expression (* 0.20 45)".
- **Arrow 1 (code reading):** green `#008300` 3px arrow with arrowhead from (285, 135) to (415, 85); 12px green label "run it as code" above its midpoint.
- **Result panel:** rounded box x=425–690, y=55–110, fill `rgba(0,131,0,0.10)`, 2px green border; bold 13px green centered text, two lines: "0.20 × 45 = 9.00" / "a $9.00 tip".
- **Arrow 2 (data reading):** orange `#d95926` 3px arrow with arrowhead from (285, 165) to (415, 215); 12px orange label "read it as data" below its midpoint.
- **Data panel:** rounded box x=425–690, y=190–245, fill `rgba(217,89,38,0.10)`, 2px orange border; 13px `#2c3e50` centered text, two lines: "a 3-item list:" / "slot 1 = *, slot 2 = 0.20, slot 3 = 45".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "same object in memory — no translation step between the two readings".
- **Caption (12px `#444`, bottom right):** "illustrative — a $45 dinner bill".

## Editing the Tip Rate with List Surgery

**Tags:** `worked example` (blue), `list edit` (green)

- **Start** — the working tip program is the 3-item list `(* 0.20 45)`; evaluating it gives 9.00
- **The edit** — service was slow, so replace slot 2 of the list: swap `0.20` for `0.15`
- **List tools** — the swap uses the same list operations you'd use on any list, no parsing step
- **Re-run** — evaluate the edited list `(* 0.15 45)` and get 6.75; the edit changed the program
- **By hand** — check it yourself: 0.15 × 45 = 6.75, exactly what the edited list returns

*Example (italic):* One list edit turned a $9.00 tip program into a $6.75 tip program — no text editor, no re-typing, just "replace slot 2".

**Key point:** Editing the list edits the program: swap slot 2 (0.20 → 0.15) and the answer moves from 9.00 to 6.75.

### Visualization (canvas `c2`, 720×300)

Two-row before/after flow: the original list evaluating to 9.00 on the top row, a dashed "replace slot 2" edit arrow dropping down, and the edited list evaluating to 6.75 on the bottom row.

- **Title (bold 15px, `#1a5276`, top center):** "Swap One Slot, Get a New Program: 9.00 → 6.75".
- **Row 1 (boxes centered at y=95):** three rounded 62×38 list boxes at x=60, 130, 200 containing bold 13px "*", "0.20", "45"; 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.10)`; green `#008300` 3px arrow from (275, 95) to (390, 95) labeled 12px green "evaluate" above; result box x=400–560, y=76–114, 2px green border, fill `rgba(0,131,0,0.10)`, bold 15px green centered "9.00".
- **Edit arrow:** orange `#d95926` 3px dashed (dash 6/4) arrow with arrowhead from the "0.20" box bottom (161, 116) down to the "0.15" box top (161, 186); bold 12px orange two-line label to its right at x=180, y=145: "replace slot 2: 0.20 → 0.15" / "(a list edit, not a text edit)".
- **Row 2 (boxes centered at y=207):** same three-box layout at x=60, 130, 200 containing "*", "0.15", "45"; the "0.15" box gets a 3px orange border to mark the edited slot; green 3px arrow from (275, 207) to (390, 207) labeled "evaluate"; result box x=400–560, y=188–226, bold 15px green centered "6.75".
- **Annotation (bold 13px green `#008300`, right side near x=580, y=150, two lines):** "edited the list," / "the answer followed".
- **Caption (12px `#444`, bottom right):** "illustrative — same $45 bill, slower service".

## Programs That Write Programs

**Tags:** `where it's used` (blue), `macros` (green), `tooling` (orange)

- **Macros** — a macro is a function that takes code-as-a-list, rearranges it, and returns new code
- **Service fee** — a `plus-fee` macro wraps any tip expression in `(+ ... 3)`: 9.00 becomes 12.00
- **No parser** — the macro moves list items directly; it never touches strings or regex
- **Real uses** — code generators, linters, refactoring tools, and DSLs all read programs as data
- **Elsewhere** — non-homoiconic languages need a separate AST library to do the same surgery

*Example (italic):* `plus-fee` rewrites `(* 0.20 45)` into `(+ (* 0.20 45) 3)`, so the same bill now answers 12.00 — the macro built that program at compile time.

**Key point:** Because code is data, a program can inspect and rewrite other programs with plain list operations — that is the engine behind Lisp macros.

### Visualization (canvas `c3`, 720×300)

Four-stage left-to-right pipeline: original code as a list, the macro doing list surgery, the new list it produces, and the final run — each stage a labeled box with the concrete expression underneath.

- **Title (bold 15px, `#1a5276`, top center):** "A Macro Is List Surgery: (* 0.20 45) In, 12.00 Out".
- **Stage boxes (four rounded 150×54 boxes, tops at y=100, at x=25, 200, 375, 550):** 2px borders — stage 1 blue `#2a78d6`, stage 2 orange `#d95926`, stage 3 blue, stage 4 green `#008300`; fills at 0.10 alpha of each; bold 13px centered stage names: "code as a list", "macro: plus-fee", "new list", "runs as code".
- **Concrete line under each box (12px `#2c3e50`, centered at y=180):** "(* 0.20 45)", "wrap it in (+ _ 3)", "(+ (* 0.20 45) 3)", "9.00 + 3 = 12.00".
- **Connecting arrows:** three 3px `#6b7280` arrows with arrowheads between consecutive boxes at y=127 (gaps 175–200, 350–375, 525–550).
- **Annotation (bold 13px orange `#d95926`, centered near y=235):** "the macro never saw text — it only moved list items".
- **Caption (12px `#444`, bottom right):** "illustrative — a flat $3 service fee added by a macro".

## It's Lists, Not Strings

**Tags:** `common mistake` (red), `strings vs structure` (orange)

- **The mix-up** — "code as data" does not mean building code by gluing strings together
- **Strings are blobs** — the text `"(* 0.20 45)"` is 11 characters; editing it means parsing first
- **Lists are pieces** — the list version already has 3 separate slots; grab slot 2 and swap it
- **eval-on-strings** — many languages can eval a string, but that is flat text, quoting bugs and all
- **The test** — homoiconic means the structure (list/tree) is the native form, not the flat text

*Example (italic):* Replacing "0.20" inside a string can accidentally hit a different "0.20" elsewhere; replacing slot 2 of a list cannot miss.

**Common mistake:** Thinking string-eval makes a language homoiconic — the point is structured code (lists/trees) you can edit slot by slot, not text you must re-parse every time.

### Visualization (canvas `c4`, 720×300)

Two-panel comparison: the expression as one opaque string blob on the left, and as a three-node structured tree on the right, with a verdict line under each panel.

- **Title (bold 15px, `#1a5276`, top center):** "Same Expression: One Blob of Text vs Three Addressable Pieces".
- **Left panel (x=30–340):** 12px `#6b7280` panel label at top y=60: "as a string"; one rounded 260×46 box centered at (185, 120), 2px `#6b7280` border, fill `rgba(107,114,128,0.12)`, 14px `#2c3e50` monospace centered content `"(* 0.20 45)"`; 12px `#6b7280` line below at y=165: "11 characters, zero slots".
- **Left verdict (bold 12px red `#e74c3c`, centered at x=185, y=215, two lines):** "needs a parser before" / "you can touch any piece".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=55 to y=250.
- **Right panel (x=380–700):** 12px `#6b7280` panel label at top y=60: "as a list (tree)"; root node — 46×34 rounded box centered at (540, 105), 2px blue `#2a78d6` border, bold 14px "*"; two child nodes — 62×34 rounded boxes centered at (465, 180) and (615, 180), 2px blue border, bold 13px "0.20" and "45"; 2px `#6b7280` lines connecting root bottom to each child top; 11px `#6b7280` slot labels beside the children: "slot 2", "slot 3".
- **Right verdict (bold 12px green `#008300`, centered at x=540, y=235):** "grab slot 2 directly — no parsing".
- **Annotation (bold 13px magenta `#d55181`, centered near y=280):** "homoiconic = structured, not stringy".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; code spans in bullets rendered as inline `<code>` with a light gray background; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all four canvases are box-and-arrow diagrams with the hardcoded coordinates and literal expression/result strings above (no randomness); the arithmetic shown must stay exact and consistent across text and charts — 0.20 × 45 = 9.00, 0.15 × 45 = 6.75, 9.00 + 3 = 12.00.
- **Fonts:** nothing below 11px; arrowheads drawn as small filled triangles matching each arrow's color.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
