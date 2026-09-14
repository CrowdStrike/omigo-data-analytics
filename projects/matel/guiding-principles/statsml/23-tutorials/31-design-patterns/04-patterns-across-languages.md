# Patterns Across Languages

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Patterns Across Languages

**Subtitle:** A design pattern is often a recipe for a feature your language doesn't have — switch languages and the whole pattern can shrink to one line

## Three Discounts, One Till

**Tags:** `core idea` (blue), `missing feature` (orange), `Strategy pattern` (green)

- **The till** — a coffee shop's till applies one of three discounts: student, happy hour, loyalty
- **The rules** — student 10%, happy hour 20%, loyalty 15%; a $4.00 latte rings up $3.60, $3.20, or $3.40
- **Classes-only** — with no functions-as-values, each rule must become a class behind a shared interface
- **The pattern** — interface + three rule classes + a strategy slot in the till: the Strategy pattern
- **With functions** — a language that passes functions as arguments needs three one-line functions
- **The reveal** — the pattern was a workaround; where the feature exists, the workaround disappears

*Example (italic):* At happy hour the till is handed the 20% rule and rings the $4.00 latte as $3.20 — the same receipt in both languages.

**Key point:** A design pattern is often a reusable workaround for a feature the language lacks — move to a language that has the feature and the pattern collapses into it.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the classes-only Strategy scaffolding (five boxes) vs the functions-as-values version (two boxes), producing the same receipt.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Three Discounts: Five Boxes or One Argument".
- **Row 1 (centered on y=100), label 12px `#444` at x=20:** "classes-only"; blue `#2a78d6` rounded box at x=140 labeled "DiscountStrategy (interface)" (12px); three small stacked boxes at x=330, y=60/100/140 labeled "Student −10%", "HappyHour −20%", "Loyalty −15%"; 3px arrow to a blue box at x=540 labeled "Till holds a strategy object".
- **Row 2 (centered on y=215), label:** "functions as values"; green `#008300` rounded box at x=200 labeled "student = p → 0.90·p (one line each)", 3px arrow to a green box at x=500 labeled "till(price, rule) → $3.60".
- **Box style:** 150–180px wide, 36px tall (stacked rule boxes 130×28), 8px radius, fills `rgba(42,120,214,0.15)` for blue and `rgba(0,131,0,0.12)` for green, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the pattern is scaffolding for a feature the language lacks".
- **Caption (12px `#444`, bottom right):** "prices illustrative".

## Counting the Ceremony: 40 Lines vs 8

**Tags:** `worked example` (blue), `line count` (green)

- **The class version** — interface 4 lines, three rule classes at 8 lines each (24), till wiring 12 lines
- **The class total** — 4 + 24 + 12 = 40 lines and five named pieces, all to route three discounts
- **The function version** — three one-line rules (3 lines) plus a till that takes the rule (5 lines)
- **The function total** — 3 + 5 = 8 lines and zero new types; 40 ÷ 8 = 5× less code
- **Hand-check** — 0.90 × $4.00 = $3.60, 0.80 × $4.00 = $3.20, 0.85 × $4.00 = $3.40 in both versions

*Example (italic):* Adding a fourth rule ("staff 25%") costs one more 8-line class in the first version and one more line in the second.

**Key point:** The behavior is identical — the extra 32 lines are the price of saying "pass me a rule" in a language where a rule cannot be passed as a value.

### Visualization (canvas `c2`, 720×300)

Horizontal stacked bar chart: lines of code for the same three-discount till, one bar per language style, segments showing where the lines go.

- **Title (bold 15px, `#1a5276`, top center):** "Line Count for the Same Behavior: 40 vs 8".
- **Axis:** vertical 2px `#999` baseline at x=200, bars extend right, scale 11px per line (max width 440); row labels left-aligned 12px `#444` at x=20.
- **Bar 1 (y=100, 26px tall), label "Strategy pattern (classes)":** stacked segments — blue `#2a78d6` width 44 ("interface 4"), orange `#d95926` width 264 ("3 rule classes 24"), violet `#4a3aa7` width 132 ("wiring 12"); 12px bold `#2c3e50` end label "40 lines".
- **Bar 2 (y=190, 26px tall), label "first-class functions":** stacked segments — green `#008300` width 33 ("rules 3"), aqua `#199e70` width 55 ("till 5"); 12px bold end label "8 lines".
- **Segment labels:** 11px white inside segments wider than 50px, otherwise 11px `#6b7280` above the segment.
- **Annotation (bold 13px green `#008300`, below bar 2 near y=240):** "same $3.60 / $3.20 / $3.40 receipts, 5× less code".
- **Caption (12px `#444`, bottom right):** "line counts illustrative; prices exact from the worked example".

## Norvig's Tally: 16 of 23 Patterns Vanish

**Tags:** `where it's used` (blue), `reading code` (green), `languages grow` (orange)

- **The tally** — Norvig's 1996 study found 16 of 23 GoF patterns invisible or simpler in Lisp/Dylan
- **Reading code** — a data scientist moving from Python to Java pipelines meets this ceremony daily
- **The signal** — spotting a pattern tells you which feature the original language was missing
- **Languages grow** — Java 8 added lambdas; old Strategy hierarchies became legacy ceremony overnight
- **Still earned** — in a classes-only language the pattern remains the right move, not a code smell

*Example (italic):* A 300-line Java Visitor walking a parse tree becomes a 20-line dispatch in a language with multiple dispatch — same tree, same answers.

**Key point:** The pattern catalog is a map of the gaps in 1994's mainstream languages — expect the list to shrink as languages absorb the missing features.

### Visualization (canvas `c3`, 720×300)

Two-column mapping diagram: five classic patterns on the left, each with an arrow to the language feature that replaces it on the right.

- **Title (bold 15px, `#1a5276`, top center):** "When the Language Has the Feature, the Pattern Disappears".
- **Rows (centered on y = 70, 110, 150, 190, 230):** left blue `#2a78d6` rounded box at x=60, 180px wide, 30px tall, fill `rgba(42,120,214,0.15)`; 3px `#6b7280` arrow; right green `#008300` rounded box at x=440, 240px wide, fill `rgba(0,131,0,0.12)`; 12px `#2c3e50` text in both.
- **Row texts (pattern → feature), hardcoded pairs:** `["Strategy","first-class functions"]`, `["Command","closures"]`, `["Iterator","built-in generators"]`, `["Singleton","modules"]`, `["Visitor","multiple dispatch"]`.
- **Annotation (bold 13px magenta `#d55181`, centered near y=265):** "Norvig, 1996: 16 of 23 GoF patterns invisible or simpler in Lisp/Dylan".
- **Caption (12px `#444`, bottom right):** "five sample mappings shown".

## Writing Java in Python

**Tags:** `common mistake` (red), `cargo cult` (orange)

- **The reflex** — porting a Singleton class into Python, where every module is already a singleton
- **The count** — a Config class with an `_instance` guard runs 14 lines; a `config.py` module needs 3
- **The cost** — extra code carries extra bugs and puzzles readers who know the language's own idiom
- **The reverse error** — dismissing patterns entirely; in a classes-only language they earn their keep
- **The test** — name the feature the pattern papers over, then check whether your language has it

*Example (italic):* A reviewer replaces a 14-line Python Singleton with a plain module import; behavior is identical and the diff is −11 lines.

**Common mistake:** Carrying a pattern between languages verbatim. The pattern solved a gap in the source language — in the target language the gap may not exist, and the feature beats the workaround.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the pattern reflex (Singleton class ported into Python) vs using the language's own feature (a module), same behavior.

- **Title (bold 15px, `#1a5276`, top center):** "Porting the Ceremony: a Singleton Where a Module Already Is One".
- **Row 1 (centered on y=95), label 12px `#444` at x=20:** "pattern reflex"; blue `#2a78d6` rounded box at x=170 labeled "Config class + _instance guard (14 lines)", 3px arrow to a red `#e74c3c` box at x=460 labeled "same behavior, more to break" with bold 12px red "✗ ceremony, no benefit" beneath it.
- **Row 2 (centered on y=205), label:** "use the feature"; green `#008300` box at x=170 labeled "config.py module (3 lines)", 3px arrow to a green box at x=460 labeled "every import shares one copy" with bold 12px green "✓" beside it.
- **Box style:** 170–200px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "check the feature shelf before reaching for the pattern book".
- **Caption (12px `#444`, bottom right):** "line counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); discount prices are exact arithmetic (0.90/0.80/0.85 × $4.00 = $3.60/$3.20/$3.40); line counts (4+24+12=40 vs 3+5=8, and 14 vs 3 for the Singleton) are invented and labeled illustrative; the 16-of-23 tally is Norvig's published 1996 result; the c3 mapping pairs are the hardcoded array `[["Strategy","first-class functions"],["Command","closures"],["Iterator","built-in generators"],["Singleton","modules"],["Visitor","multiple dispatch"]]`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
