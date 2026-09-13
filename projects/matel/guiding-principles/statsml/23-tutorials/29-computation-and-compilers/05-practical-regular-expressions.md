# Practical Regular Expressions

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Practical Regular Expressions

**Subtitle:** A regular expression describes the shape of text — groups mark the slice to keep, anchors pin where the pattern must sit, and the greedy star grabs more than you expect

## Reading a Thousand Subject Lines at Once

**Tags:** `core idea` (blue), `groups` (green), `pattern shape` (orange)

- **The inbox** — a support inbox holds thousands of emails; every order email hides its number in the subject
- **The pattern** — `order #(\d+)` is a search recipe: plain characters match themselves, symbols stand in for many
- **`\d` and `+`** — `\d` means "any single digit", `+` means "one or more of the thing just before me"
- **The group** — the parentheses don't change what matches; they capture the digits so you can pull them out
- **One recipe** — the same pattern finds #48213, #9917, and #77; the length differs, the shape does not

*Example (italic):* In "Your order #48213 has shipped", the pattern locks onto "order #48213" and hands back just 48213.

**Key point:** A regex describes the shape of text, not exact text — and a group marks which slice of the shape to keep.

### Visualization (canvas `c1`, 720×300)

Text-highlight panel: five real-looking subject lines drawn in monospace, with each match boxed in blue, the captured group shaded green, and the extracted number listed in a right-hand column.

- **Title (bold 15px, `#1a5276`, top center):** "One Pattern, Five Subject Lines: order #(\\d+)".
- **Pattern chip:** the literal text `order #(\d+)` in bold 13px monospace `#2a78d6` at x=40, y=55, on a `rgba(42,120,214,0.10)` rounded rect.
- **Rows (13px monospace, baselines at y = 100, 135, 170, 205, 240):** draw each line as sequential segments starting at x=40, advancing with `ctx.measureText` — text outside the match in `#6b7280`, the matched span on a `rgba(42,120,214,0.15)` rect in `#2a78d6`, the captured digits on a `rgba(0,131,0,0.20)` rect in bold `#008300`:
  - "Your order #48213 has shipped" — match "order #48213", capture "48213"
  - "order #9917 — refund issued" — match "order #9917", capture "9917"
  - "Re: order #33305 delayed" — match "order #33305", capture "33305"
  - "Thanks! order #77 received" — match "order #77", capture "77"
  - "Meeting notes for Tuesday" — no match, whole line in `#6b7280`
- **Extracted column:** vertical light `#e5e9ef` separator at x=580; header "captured" in 12px `#444` at x=600, y=80; per row at x=600: bold 13px `#008300` values "48213", "9917", "33305", "77", and 12px `#6b7280` "—" for the last row.
- **Annotation (bold 12px orange `#d95926`, at x=40, y=272):** "same recipe, any number of digits — the group keeps only the digits".

## Pinning the Pattern: Anchors, Checked by Hand

**Tags:** `worked example` (blue), `anchors` (green)

- **The form** — the returns page asks customers to type a 5-digit order number written like #48213
- **First try** — `#\d{5}` means "a # then exactly five digits" — found anywhere inside the typed text
- **The leak** — "ref #48213 pls" and "#482134" both pass, because a valid-looking piece somewhere is enough
- **Anchors** — `^` pins the pattern to the start and `$` to the end: `^#\d{5}$` demands the whole box be the ID
- **By hand** — for each input ask: start with #, then five digits, then nothing at all? Only 1 of 4 passes

*Example (italic):* "#482134" contains "#48213" inside it, so the unanchored check passes; `^#\d{5}$` sees a sixth digit before the end and rejects it.

**Key point:** Without ^ and $ a regex asks "is this somewhere in there?" — with them it asks "is the whole thing exactly this?".

### Visualization (canvas `c2`, 720×300)

Pass/fail grid: the four typed inputs as rows, the unanchored and anchored patterns as two columns, with the two wrong accepts flagged in orange.

- **Title (bold 15px, `#1a5276`, top center):** "Same Four Inputs, Two Patterns — Anchors Change the Question".
- **Column headers (bold 13px monospace, y=85):** `#\d{5}` in `#2a78d6` centered at x=430; `^#\d{5}$` in `#008300` centered at x=595; 11px `#6b7280` sublabels beneath (y=100): "anywhere inside" and "the whole box".
- **Grid:** light `#e5e9ef` horizontal lines under each row; vertical separators at x=340 and x=510.
- **Input rows (13px monospace `#2c3e50` at x=50, baselines at y = 130, 165, 200, 235):** `#48213`, `ref #48213 pls`, `#4821`, `#482134`.
- **Marks (bold 13px, centered in each column):** unanchored column — green `#008300` "match" for #48213; bold orange `#d95926` "match (!)" for "ref #48213 pls" and "#482134"; `#6b7280` "no match" for #4821. Anchored column — green "match" for #48213; `#6b7280` "no match" for the other three.
- **Annotation (bold 12px orange `#d95926`, centered at x=360, y=272):** "without ^…$ two sloppy inputs sneak through — only #48213 should pass".

## Why It Is the Daily Tool

**Tags:** `where it's used` (blue), `daily tool` (green), `extraction` (orange)

- **Daily filter** — of 2,000 exported support emails, the pattern `order #(\d+)` matches 389 directly
- **Near misses** — 412 emails contain the word "order"; the 23 that don't match are typos like "order 48213"
- **Column in one line** — `str.extract` drops group 1 into a clean order-number column, no loops written
- **Same tool everywhere** — grep, editor find-replace, log filters, and form validators all speak this syntax
- **Minutes, not days** — reading 2,000 emails by eye is a week of work; the pattern finishes before the coffee cools

*Example (italic):* One `grep -E 'order #[0-9]+'` over the export file returns the 389 order emails in about a second.

**Key point:** Regex earns "daily tool" status because the same one-line pattern filters, extracts, and validates — in every editor, shell, and dataframe.

### Visualization (canvas `c3`, 720×300)

Horizontal funnel of four bars on a shared count axis: the full export narrowing to matched emails, plus the small typo remainder flagged for a human.

- **Title (bold 15px, `#1a5276`, top center):** "One Pattern Sorts 2,000 Emails".
- **Axis:** counts 0 to 2,000 mapped to x=230 (0) through x=690 (2,000); 2px `#999` baseline at y=262; 12px `#444` tick labels "0", "500", "1,000", "1,500", "2,000" below, light `#e5e9ef` vertical gridlines at each tick.
- **Bars (22px tall, centered at y = 95, 140, 185, 230; left-aligned 12px `#444` row labels at x=20):** widths from the hardcoded counts `[2000, 412, 389, 23]`:
  - "all exported emails" — 2,000, fill `rgba(107,114,128,0.30)`
  - "contain the word 'order'" — 412, fill `rgba(42,120,214,0.35)`
  - "match order #(\\d+)" — 389, fill `rgba(0,131,0,0.35)`
  - "typos flagged for a human" — 23, fill `rgba(217,89,38,0.45)` (minimum drawn width 5px)
- **Value labels:** bold 13px at each bar's right end, colored to match the bar (`#6b7280`, `#2a78d6`, `#008300`, `#d95926`): "2,000", "412", "389", "23".
- **Annotation (bold 12px green `#008300`, near x=400, y=200):** "389 order numbers extracted with one line".
- **Caption (12px `#444`, bottom right):** "illustrative counts from a made-up support inbox".

## The Greedy Star Grabs Too Much

**Tags:** `common mistake` (red), `greedy vs lazy` (orange)

- **The receipt note** — the line is: He said "large" and "iced" at the counter — the job: pull the first quoted word
- **Greedy try** — `".*"` reads as quote, anything, quote — but `.*` takes as MUCH text as it possibly can
- **The overshoot** — it matches from the first quote to the LAST one: "large" and "iced" — 18 characters
- **The fix** — a `?` after `*` makes it lazy: `".*?"` stops at the first closing quote, giving the 7-character "large"
- **The better fix** — `"[^"]*"` (quote, non-quotes, quote) says exactly what you mean and cannot overshoot

*Example (italic):* Greedy grabbed 18 characters spanning both quotes; lazy grabbed the 7 characters of "large" and stopped.

**Common mistake:** Assuming `.*` stops at the first chance. Quantifiers are greedy by default — they run to the last possible stop, not the first.

### Visualization (canvas `c4`, 720×300)

Span-comparison panel: the receipt line drawn once in monospace, with two bracket bars beneath showing the greedy match overshooting to the last quote and the lazy match stopping at the first.

- **Title (bold 15px, `#1a5276`, top center):** "Greedy vs Lazy on the Same Line".
- **The string (14px monospace `#2c3e50`, baseline y=110):** `He said "large" and "iced" at the counter`, drawn per character at x(i) = 60 + 13*i for character index i = 0..40 (character width fixed at 13px so span brackets align); the two quoted parts ("large" at indices 8–14, "iced" at indices 20–25) in bold.
- **Greedy span (orange `#d95926`):** 10px-tall bracket bar at y=150 from x(8) to x(25)+13 (indices 8–25 inclusive), fill `rgba(217,89,38,0.25)`, 2px orange border; bold 13px orange label beneath (y=178): `".*"  grabs  "large" and "iced"  — 18 chars`.
- **Lazy span (green `#008300`):** 10px-tall bracket bar at y=210 from x(8) to x(14)+13 (indices 8–14 inclusive), fill `rgba(0,131,0,0.25)`, 2px green border; bold 13px green label beneath (y=238): `".*?"  grabs  "large"  — 7 chars`.
- **Quote markers:** thin dashed `#6b7280` (dash 4/3) vertical guides from the string baseline down to y=225 at the first quote x(8) and last quote x(25).
- **Annotation (bold 12px orange `#d95926`, at x=430, y=270):** "greedy runs to the LAST quote; add ? to stop at the first".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Regex patterns inside bullets render in `<code>` with a monospace font.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all strings, match spans, pass/fail marks, character indices, and funnel counts are the hardcoded literals above (no randomness); the funnel counts `[2000, 412, 389, 23]` are invented and labeled illustrative; span pixel positions in c4 derive only from the fixed 13px character width. In chart code, regex patterns are plain string literals to draw, never executed.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
