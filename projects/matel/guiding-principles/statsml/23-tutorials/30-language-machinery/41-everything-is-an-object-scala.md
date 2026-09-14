# Everything Is an Object (Scala)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Everything Is an Object (Scala)

**Subtitle:** In Scala the number 3.50 on a coffee-shop receipt is a full object with methods you can call — there is no separate world of "primitives", just one family of values you talk to the same way

## A Receipt Where Every Number Can Answer Questions

**Tags:** `core idea` (blue), `objects` (green), `no primitives` (orange)

- **The receipt** — a coffee shop receipt lists three prices: latte 3.50, muffin 2.25, sandwich 4.75
- **A number that talks** — in Scala the price 3.50 is an object: ask it `3.50.max(4.75)` and it answers 4.75
- **More questions** — `.round` gives 4, `.toInt` gives 3, `.+(2.25)` gives 5.75 — all asked of a bare number
- **No second class** — strings, lists, booleans, even the digit 3 get the same treatment: object, methods, done
- **The definition (after the example)** — "everything is an object" means every value can receive method calls

*Example (italic):* Type `3.50.max(4.75)` at a Scala prompt and it replies 4.75 — the price itself did the comparing; no helper function needed.

**Key point:** In Scala every value — including a plain number like 3.50 — is an object you can call methods on; there is no separate primitive world.

### Visualization (canvas `c1`, 720×300)

Two-panel diagram: a drawn receipt on the left, and the price 3.50 blown up on the right as an "object card" listing the questions (methods) it can answer, with an arrow connecting them.

- **Title (bold 15px, `#1a5276`, top center):** "One Price on the Receipt Is a Whole Object".
- **Receipt (left):** white box x=40–240, y=55–265, 1px `#999` border, 4px slight corner rounding; header "RECEIPT" bold 13px `#2c3e50` centered at y=75; line items in 13px monospace `#2c3e50` left-aligned at x=55 (y=105, 130, 155): "latte      3.50", "muffin     2.25", "sandwich   4.75"; dashed `#e5e9ef` divider at y=175; "total     10.50" bold 13px at y=200.
- **Highlight:** the "3.50" on the latte line circled with a 2px blue `#2a78d6` ellipse (center x≈157, y≈101, rx=32, ry=13).
- **Arrow:** 3px blue `#2a78d6` arrow with arrowhead from (240, 101) to (300, 110).
- **Object card (right):** box x=300–680, y=60–235, fill `rgba(42,120,214,0.08)`, 2px blue `#2a78d6` border; card title bold 14px blue at (x=320, y=85): "3.50 — a Double object"; four method rows in 13px monospace `#2c3e50` at x=320 (y=115, 143, 171, 199), each with its answer right-aligned near x=660 in bold 13px green `#008300`: `.+(2.25)` → "5.75", `.max(4.75)` → "4.75", `.round` → "4", `.toInt` → "3".
- **Annotation (bold 12px orange `#d95926`, centered near x=490, y=262):** "even a plain price knows how to answer questions".
- **Caption (12px `#444`, bottom right):** "menu prices illustrative".

## The Total, One Method Call at a Time

**Tags:** `worked example` (blue), `operators are methods` (green)

- **The sugar** — `3.50 + 2.25` looks like grade-school math, but Scala reads it as `3.50.+(2.25)`
- **Step 1** — `3.50.+(2.25)` returns 5.75; the first price did the adding itself
- **Step 2** — `5.75.+(4.75)` returns 10.50, the receipt total, again by a method call
- **Same trick elsewhere** — `1 to 3` is really `1.to(3)`, which returns the range 1, 2, 3
- **Operators are methods** — `+`, `*`, `<` are ordinary method names; the pretty spelling is only syntax

*Example (italic):* The barista's total 3.50 + 2.25 + 4.75 = 10.50 is, to Scala, two polite requests: first to 3.50, then to the 5.75 it handed back.

**Key point:** The receipt total 10.50 is two method calls in disguise — `3.50.+(2.25)` gives 5.75, then `5.75.+(4.75)` gives 10.50; the `+` you have used forever was a method all along.

### Visualization (canvas `c2`, 720×300)

Three-row desugaring pipeline: each row shows "what you write" in a box, an arrow to "what Scala reads" (the explicit method call), and a second arrow to the answer, all with hardcoded receipt numbers.

- **Title (bold 15px, `#1a5276`, top center):** "What You Write vs What Scala Reads".
- **Column headers (bold 12px `#6b7280`, y=58):** "you write" centered at x=140, "Scala reads" centered at x=390, "answer" centered at x=625.
- **Rows at y-centers 100, 165, 230; each row:**
  - left box (x=45–235, height 36, fill `#f8f9fa`, 1px `#999` border) with 13px monospace `#2c3e50` code centered;
  - 2px `#6b7280` arrow with arrowhead from x=240 to x=280;
  - middle box (x=285–495, same style, 2px blue `#2a78d6` border) with 13px monospace blue code;
  - 2px `#6b7280` arrow from x=500 to x=540;
  - answer in bold 14px green `#008300` centered at x=625.
- **Row 1:** `3.50 + 2.25` → `3.50.+(2.25)` → "5.75".
- **Row 2:** `5.75 + 4.75` → `5.75.+(4.75)` → "10.50".
- **Row 3:** `1 to 3` → `1.to(3)` → "Range(1, 2, 3)" (13px, it is longer).
- **Annotation (bold 12px orange `#d95926`, centered near x=390, y=272):** "the '+' was a method call all along — 3.50 + 2.25 + 4.75 = 10.50".

## One Family Tree Instead of Two Worlds

**Tags:** `where it's used` (blue), `type hierarchy` (green), `uniformity` (orange)

- **Java's split** — in Java `int` is a primitive and `Integer` an object: two spellings, two rulebooks
- **Scala's answer** — one tree: every value sits under `Any`, numbers under `AnyVal`, classes under `AnyRef`
- **Uniform collections** — `List(3.50, 2.25, 4.75)` works exactly like `List("latte", "muffin")`; no special cases
- **Uniform code** — a function that accepts `Any` takes a price, a name, or a whole receipt without ceremony
- **Fewer traps** — no `int` vs `Integer` comparison surprises, no "a primitive can't go in a list" errors

*Example (italic):* One `printLabel(x: Any)` helper can print the price 3.50, the word "latte", and the whole order list — because all three live in the same family tree.

**Key point:** A single hierarchy rooted at `Any` means every value follows one rulebook — the price and the string on the same receipt are cousins, not strangers.

### Visualization (canvas `c3`, 720×300)

Family-tree diagram: `Any` at the top splitting into `AnyVal` and `AnyRef`, with three example leaves under each, and a grayed side note showing Java's `int` stranded outside any tree.

- **Title (bold 15px, `#1a5276`, top center):** "Scala's One Tree: Every Value Descends from Any".
- **Root box:** "Any" bold 14px white on ink `#1a5276` fill, box x=320–420, y=50–80, 4px rounding, centered text.
- **Branch boxes (y=115–147):** "AnyVal — value types" bold 13px `#2c3e50` on `rgba(42,120,214,0.15)` fill, 1px blue `#2a78d6` border, x=95–305; "AnyRef — reference types" same style with `rgba(0,131,0,0.12)` fill, 1px green `#008300` border, x=435–665.
- **Leaf boxes (y=190–218, 13px monospace `#2c3e50`, fill `#f8f9fa`, 1px `#999` border):** under AnyVal: `Int` (x=85–150), `Double` (x=160–245), `Boolean` (x=255–345); under AnyRef: `String` (x=430–510), `List` (x=520–580), `Receipt` (x=590–675).
- **Connectors:** 2px `#6b7280` lines from the root box bottom (x=370, y=80) to each branch box top center, and from each branch box bottom to its three leaf tops.
- **Java aside (dashed 1px `#6b7280` box, x=30–250, y=245–283):** 12px `#6b7280` text on two lines: "Java: int, double live" / "outside any tree — primitives".
- **Annotation (bold 12px violet `#4a3aa7`, right-aligned near x=680, y=262):** "3.50 and \"latte\" share one rulebook".

## Objects Everywhere, Yet No Slowdown

**Tags:** `common mistake` (red), `boxing` (orange)

- **The worry** — "if 3.50 is an object, isn't every price wrapped in a costly box on the heap?"
- **The trick** — the compiler turns `3.50 + 2.25` into the same raw machine arithmetic Java uses for `double`
- **When boxing happens** — inside generic containers like `List(3.50, 2.25, 4.75)` each price does get a box
- **The mistake** — avoiding Scala numbers "for speed": plain arithmetic compiles down to primitives anyway
- **Language vs machine** — "object" describes how you talk to a value, not how it is stored at runtime

*Example (italic):* The line `val total = 3.50 + 2.25 + 4.75` runs as bare machine addition; only when the three prices are put in a `List` do they each get wrapped.

**Common mistake:** Assuming "everything is an object" means everything is heap-boxed — plain number math compiles to raw primitives; boxes appear only inside generic containers, and the compiler decides, not you.

### Visualization (canvas `c4`, 720×300)

Two-lane diagram: the top lane shows the code you write (prices as objects), the bottom lane shows what actually runs (raw machine numbers), with a "compiler" arrow between them and a side panel showing the one place boxing really happens.

- **Title (bold 15px, `#1a5276`, top center):** "Objects in the Language, Primitives in the Machine".
- **Top lane (x=40–460, y=55–115):** fill `rgba(42,120,214,0.08)`, 1px blue `#2a78d6` border; bold 12px blue label "what you write" at (x=52, y=73); 13px monospace `#2c3e50` code centered at y=98: `val total = 3.50 + 2.25 + 4.75  // 10.50`.
- **Compiler arrow:** 3px ink `#1a5276` vertical arrow with arrowhead from (250, 115) to (250, 175); bold 12px ink label "compiler translates" to its right at (x=262, y=148).
- **Bottom lane (x=40–460, y=175–235):** fill `#f8f9fa`, 1px `#999` border; bold 12px `#6b7280` label "what the machine runs" at (x=52, y=193); 13px monospace `#2c3e50` text centered at y=218: "raw 64-bit adds: 3.50 + 2.25 + 4.75 = 10.50" — no boxes, no heap.
- **Boxing panel (x=490–685, y=55–235):** fill `rgba(217,89,38,0.08)`, 1px orange `#d95926` border; bold 12px orange header "the one boxed place" at (x=502, y=75); 13px monospace `#2c3e50` line `List(3.50,` / `2.25, 4.75)` on two lines (y=100, 118); below, three small 46×26 rounded boxes (2px orange border, y=140–166, x=502, 566, 630) each holding a 12px price "3.50", "2.25", "4.75"; 12px `#6b7280` caption under them at y=192 on two lines: "each price wrapped in" / "a java.lang.Double box".
- **Annotation (bold 12px green `#008300`, centered near x=250, y=268):** "same 10.50 either way — the object view costs nothing here".
- **Caption (12px `#444`, bottom right):** "illustrative — exact bytecode varies by Scala version".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Inline code:** `code` spans in bullets render in monospace, 0.88em, `#1a5276`, background `#f2f5f8`, 1px 4px padding, 3px radius.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all prices, answers, and box positions are the hardcoded literals above (no randomness); the receipt numbers 3.50, 2.25, 4.75, and total 10.50 must appear identically in text and charts; all diagram fonts stay at 11px or larger.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
