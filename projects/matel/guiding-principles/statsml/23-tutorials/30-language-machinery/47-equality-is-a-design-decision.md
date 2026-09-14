# Equality Is a Design Decision

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Equality Is a Design Decision

**Subtitle:** "Are these two things equal?" has no single right answer — every language makes you choose between same-object, same-value, and same-enough

## Two Twenty-Dollar Bills

**Tags:** `core idea` (blue), `identity vs value` (green)

- **The bills** — two crisp $20 bills: are they "equal"? For paying rent, yes; as physical objects, no
- **Identity** — same-object equality asks "is this literally the same bill?" (same serial number)
- **Value** — value equality asks "does it spend the same?" ($20 == $20)
- **The split** — Java's `==` checks identity for objects; `.equals()` checks value — two different questions
- **Your call** — for a `Money` class, YOU decide which question `equals` answers; the language won't

*Example (italic):* `new Money(20) == new Money(20)` is false in Java (different objects) while `.equals()` returns true — both answers are correct, to different questions.

**Key point:** Equality isn't discovered, it's designed: you choose whether two objects with the same contents count as "the same thing".

### Visualization (canvas `c1`, 720×300)

Diagram: two $20-bill boxes with different serial numbers, and two question arrows between them — identity (red ✗) and value (green ✓).

- **Title (bold 15px, `#1a5276`, top center):** "Same Object? No. Same Value? Yes.".
- **Bill boxes:** two rounded rectangles 180×90 at (110, 100) and (430, 100), fill `rgba(0,131,0,0.10)`, 2px `#008300` border; inside each: bold 16px "$20" centered, 11px `#6b7280` serial below — "serial JB4471902A" / "serial JB8823410C".
- **Identity arrow (top):** 2px red `#e74c3c` double-headed arrow between box tops at y=90; bold 12px red label above: "identity (==): different objects ✗".
- **Value arrow (bottom):** 2px green `#008300` double-headed arrow between box bottoms at y=210; bold 12px green label below: "value (.equals): both worth $20 ✓".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "two correct answers to two different questions".

## One Class, Three Defensible Answers

**Tags:** `worked example` (blue), `design choice` (orange)

- **The class** — `Receipt(store, total, timestamp)`: when are two receipts equal?
- **Strict** — all three fields match: 2 of the 6 sample pairs below are equal
- **Business** — same store and total, any timestamp: 4 of 6 pairs are equal
- **Identity** — only the exact same object: 0 of 6 pairs are equal
- **Hand-check** — pair ("Cafe", $8, 9:01) vs ("Cafe", $8, 9:03): strict says no, business says yes

*Example (italic):* A duplicate-charge detector wants the business rule (same store, same total, close in time); an audit log wants strict equality — same class, different `equals`.

**Key point:** For the same six receipt pairs, the equal-count is 0, 2, or 4 depending on which rule you pick — the data doesn't decide, the use case does.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: number of the 6 sample pairs judged equal under each of the three rules, with the pair list shown as a small side table.

- **Title (bold 15px, `#1a5276`, top center):** "Six Receipt Pairs, Three Equality Rules, Three Answers".
- **Axes:** origin x=70, baseline y=245, plot width 380, plot height 180; y = pairs judged equal 0 to 6, gridlines `#e5e9ef` at 2/4/6 with 12px labels.
- **Bars (80px wide, centered at x = 140, 260, 380):** "identity" red `#e74c3c` height 0 (draw a 2px baseline tick with bold 12px "0" above); "strict fields" blue `#2a78d6` height for 2; "business rule" green `#008300` height for 4; bold 13px count labels above each bar; 12px `#444` category labels below.
- **Side list (x=480–700, starting y=80, 11px `#444`, 22px line spacing):** six pair rows like "Cafe $8 9:01 ~ Cafe $8 9:03" with a colored ✓/✗ trio to the right (red, blue, green marks per rule per row; hardcode: strict ✓ on rows 1–2, business ✓ on rows 1–4).
- **Annotation (bold 12px orange `#d95926`, near x=260, y=70):** "same data — the rule is the variable".
- **Caption (12px `#444`, bottom right):** "sample pairs illustrative".

## The Contract That Breaks Hash Maps

**Tags:** `where it's used` (blue), `hashCode` (orange), `contract` (red)

- **The pact** — Java/Python/C# all require: if two objects are equal, their hash codes MUST match
- **Why** — hash maps find keys by hash bucket first, `equals` second; equal-but-different-hash keys vanish
- **The bug** — override `equals` to use value equality but forget `hashCode`: lookups silently miss
- **The symptom** — `map.put(key); map.get(equalKey)` returns null even though `equals` says true
- **The fix** — always override the pair together, computed from the same fields

*Example (italic):* A `Money(20)` key goes into bucket 17 (default identity hash); the equal `Money(20)` you look up hashes to bucket 42 — the map checks bucket 42, finds nothing, returns null.

**Key point:** equals and hashCode are one decision expressed twice — change what "equal" means and the hash must follow, or every hash-based collection quietly breaks.

### Visualization (canvas `c3`, 720×300)

Diagram of a hash map's buckets: the stored key sits in bucket 17, the equal lookup key hashes to bucket 42, and the lookup arrow finds an empty bucket.

- **Title (bold 15px, `#1a5276`, top center):** "equals Overridden, hashCode Forgotten: the Key Disappears".
- **Bucket row:** 8 rectangles 70×46 in a row starting x=70 y=130, 1px `#999` borders, 11px `#6b7280` labels "b15"–"b22" underneath (b17 and b21 shown as "17" and "42" conceptually — label them "b17" and "b42", skipping literal continuity).
- **Stored key:** green `#008300` filled chip inside bucket b17 labeled "Money(20)" (11px white); 2px green arrow from a box above at (120, 60) labeled "put: hash→17".
- **Lookup:** red `#e74c3c` 3px arrow from a box above at (520, 60) labeled "get equal Money(20): hash→42" down into bucket b42, which is empty; bold 12px red label below b42: "empty → returns null ✗".
- **Equality note:** dashed `#6b7280` (dash 4/3) line connecting the two Money chips with 11px label "equals() says true — never consulted".
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "the map trusts the hash before it trusts equals".

## Floats, NaN, and == Traps

**Tags:** `common mistake` (red), `edge cases` (orange)

- **NaN** — by IEEE rules `NaN == NaN` is false: the one value not equal to itself
- **JS coercion** — JavaScript's loose `==` says `0 == ""` and `"1" == 1` are true; use `===`
- **Float sums** — `0.1 + 0.2 == 0.3` is false in every IEEE language; compare with a tolerance
- **Interning** — Java's small Integers are cached, so `==` "works" up to 127 then breaks at 128
- **The mistake** — testing equality logic only on friendly values and shipping the edge cases

*Example (italic):* `Integer a = 127, b = 127; a == b` is true (cached objects) — change both to 128 and the same line turns false.

**Common mistake:** Assuming `==` means one thing everywhere. It's identity in Java, coercing in JS, value in Python — and IEEE floats bend it for everyone.

### Visualization (canvas `c4`, 720×300)

Scorecard table drawn on canvas: five equality expressions vs their surprising results, with expected-vs-actual columns.

- **Title (bold 15px, `#1a5276`, top center):** "Five Comparisons That Betray Intuition".
- **Table:** starts y=70, five rows at 38px spacing, columns at x=40 (expression, 13px monospace `#2c3e50`), x=430 ("gut says", 12px `#6b7280`), x=560 ("actually", bold 13px).
- **Rows:** `NaN == NaN` / true / red "false"; `0.1 + 0.2 == 0.3` / true / red "false"; `"1" == 1  (JS)` / false / orange "true"; `127 == 127 (Java Integer)` / ? / green "true"; `128 == 128 (Java Integer)` / same / red "false".
- **Row separators:** 1px `#e5e9ef` lines; header row 12px bold `#1a5276`.
- **Annotation (bold 12px orange `#d95926`, bottom center y=280):** "the last two rows differ by one integer — cache boundary at 127".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all counts, table rows, and bucket labels are the hardcoded literals above (no randomness); receipt pairs and serial numbers are invented and labeled illustrative; the IEEE/NaN/Integer-cache facts are real language behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
