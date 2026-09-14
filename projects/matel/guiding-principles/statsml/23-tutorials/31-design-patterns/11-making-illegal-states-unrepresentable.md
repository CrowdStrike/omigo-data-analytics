# Making Illegal States Unrepresentable

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Making Illegal States Unrepresentable

**Subtitle:** Shape your types so wrong data cannot even be constructed — a refund on an order that was never paid becomes a compile error, not a bug

## The Coffee Order That Can't Be Refunded Before It's Paid

**Tags:** `core idea` (blue), `sealed hierarchy` (green), `types as guardrails` (orange)

- **The order** — a coffee shop app tracks each order: not yet paid, paid, or refunded
- **The flag version** — one record with four fields: `paid`, `refunded`, `receipt_id`, `refund_reason`
- **The nonsense** — nothing stops `refunded=yes, paid=no`: a refund for money never taken
- **The sealed version** — exactly three variants: `Pending`, `Paid(receipt)`, `Refunded(receipt, reason)`
- **The guarantee** — a `Refunded` value cannot exist without a receipt; the bad state has no constructor

*Example (italic):* Order #212 is `Paid("R-88")`; the only way to refund it is to build `Refunded("R-88", reason)` — there is no other path through the type.

**Key point:** Instead of writing checks that catch bad data after it exists, define the type so bad data cannot be written down at all — this is making illegal states unrepresentable.

### Visualization (canvas `c1`, 720×300)

Two-row diagram: the flag record (which happily holds a nonsense order) vs the sealed hierarchy (which offers only the three real states).

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Store One Order: Flags vs a Sealed Hierarchy".
- **Row 1 (boxes centered on y=88), label 12px `#444` at x=20:** "flag record"; blue `#2a78d6` rounded box at x=130, width 265, labeled "paid · refunded · receipt_id · refund_reason" (12px); 3px arrow to a red `#e74c3c` box at x=455, width 230, labeled "refunded=yes, paid=no" with bold 12px red "✗ nonsense, but it compiles".
- **Row 2 (boxes centered on y=195), label:** "sealed hierarchy"; three green `#008300` rounded boxes at x=130 / x=320 / x=510, each 170px wide, labeled "Pending", "Paid(receipt)", "Refunded(receipt, reason)" (12px), with bold 12px green "✓ only these 3 can be constructed" centered under the row at y=230.
- **Box style:** 42px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "the wrong order can't even be written down".
- **Caption (12px `#444`, bottom right):** "field names illustrative".

## Sixteen Combinations, Three of Them Real

**Tags:** `worked example` (blue), `counting states` (green)

- **Four fields** — `paid` (yes/no), `refunded` (yes/no), `receipt_id` (set/null), `refund_reason` (set/null)
- **The count** — 2 × 2 × 2 × 2 = 16 possible combinations of the flag record
- **The legal ones** — only 3 describe a real order: pending, paid-with-receipt, refunded-with-both
- **The illegal ones** — the other 13 are nonsense, like a receipt sitting on an unpaid order
- **The sealed count** — the three-variant hierarchy has exactly 3 states: legal states = possible states

*Example (italic):* Pending is `paid=no, refunded=no, receipt=null, reason=null` — one shape out of 16; the sealed `Pending` variant is one out of 3.

**Key point:** The flag record can represent 16 states for only 3 real ones — 13 standing chances to be wrong; the sealed type represents 3 for 3, leaving zero.

### Visualization (canvas `c2`, 720×300)

Grid of all 16 bit-combinations of the four flags, with the 3 legal cells in green and the 13 illegal cells in red, plus a legend mapping the legal cells to variants.

- **Title (bold 15px, `#1a5276`, top center):** "2 × 2 × 2 × 2 = 16 Combinations — Only 3 Describe a Real Order".
- **Grid:** 4 columns × 4 rows starting at x=60, y=62; each cell 100×40 with 8px gaps; cells hold the 4-bit pattern `paid refunded receipt reason` in order `["0000","0001","0010","0011","0100","0101","0110","0111","1000","1001","1010","1011","1100","1101","1110","1111"]`, 12px `#2c3e50` monospace, centered.
- **Legal cells:** `0000`, `1010`, `1111` — fill `rgba(0,131,0,0.18)`, 2px `#008300` border.
- **Illegal cells:** the remaining 13 — fill `rgba(231,76,60,0.10)`, 1px `#e74c3c` border.
- **Legend (right side, 12px `#444`):** green swatch "legal (3)" at (x=520, y=75); red swatch "illegal (13)" at (x=520, y=98); mapping lines "0000 = Pending", "1010 = Paid", "1111 = Refunded" at (x=520, y=135/158/181).
- **Annotation (bold 13px magenta `#d55181`, at x=520, y=225, wrapped to two lines):** "13 of 16 states are bugs waiting to happen".
- **Caption (12px `#444`, bottom right):** "bits = paid / refunded / receipt / reason, illustrative".

## From 28 Guard Checks to Zero

**Tags:** `where it's used` (blue), `data pipelines` (green), `exhaustive matching` (orange)

- **The orders table** — the flag record becomes a table with nullable columns; the 13 bad shapes arrive eventually
- **Defensive code** — every function opens with guards: "if refunded but receipt is null, then what?"
- **The count** — 7 pipeline functions × 4 guard checks each = 28 runtime checks in the flags version
- **Exhaustive matching** — the compiler flags each missed variant (Scala warns; Rust/Kotlin refuse)
- **The payoff** — an unhandled case is flagged before the deploy, not a wrong number after it

*Example (italic):* A daily revenue job hits a `refunded=yes, receipt=null` row, silently drops it, and three weeks of totals run low before anyone notices.

**Key point:** Runtime guards catch bad states after they exist; a sealed hierarchy plus exhaustive matching removes the states, so there is nothing left to guard.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart comparing the flag record and the sealed type on two counts: illegal states representable, and runtime guard checks needed across the 7 pipeline functions.

- **Title (bold 15px, `#1a5276`, top center):** "Runtime Guard Checks vs Compile-Time Proof (7 pipeline functions)".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 170; y = count 0 to 30, gridlines `#e5e9ef` at 10 and 20 with 12px `#444` tick labels.
- **Category 1 (centered x=230, 13px `#444` label "illegal states representable" below baseline):** flag-record bar blue `#2a78d6`, 60px wide, value 13; sealed bar green `#008300`, 60px wide, value 0 drawn as a 2px stub; bold 12px value labels "13" and "0" above each bar.
- **Category 2 (centered x=490, label "runtime guard checks"):** flag-record bar blue, value 28; sealed bar green, value 0 drawn as a 2px stub; value labels "28" and "0".
- **Legend (top right, 12px):** blue swatch "flag record", green swatch "sealed type".
- **Annotation (bold 13px green `#008300`, left-aligned at x=495, two lines at y=95/113):** "sealed: nothing left / to check at runtime".
- **Caption (12px `#444`, bottom right):** "7 functions × 4 checks = 28, counts illustrative".

## The `default:` Branch That Eats New Variants

**Tags:** `common mistake` (red), `exhaustive matching` (orange)

- **The temptation** — adding `default: skip` to a match makes the compiler stop complaining forever
- **The cost** — the match is no longer exhaustive; future variants fall silently into the default
- **The scenario** — six months later the shop adds a fourth variant, `Cancelled(reason)`
- **With default** — all 7 matches compile clean and cancelled orders are quietly mishandled
- **Without default** — the compiler flags all 7 match sites that must decide what cancelled means
- **Validation isn't it** — a runtime validator can be forgotten at one call site; the type cannot

*Example (italic):* Cancelled orders count as pending revenue for a month because one `default:` branch swallowed the new variant without a peep.

**Common mistake:** Writing `default:` / `_ =>` catch-alls in matches over a sealed type — it trades the compiler's exhaustiveness guarantee for short-term silence.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the new `Cancelled` variant flowing into a match with a default branch (silent mishandling) vs into an exhaustive match (compile error that lists every site to fix).

- **Title (bold 15px, `#1a5276`, top center):** "Adding Cancelled: the default Branch Hides It, Exhaustive Matching Finds It".
- **Row 1 (boxes centered on y=95), label 12px `#444` at x=20:** "with default:"; violet `#4a3aa7` rounded box at x=150, width 170, labeled "new variant Cancelled"; 3px arrow to a red `#e74c3c` box at x=420, width 250, labeled "falls into default: skip" with bold 12px red "✗ 7 matches silently wrong".
- **Row 2 (boxes centered on y=205), label:** "exhaustive match"; violet box "new variant Cancelled" at x=150, width 170; 3px arrow to a blue `#2a78d6` box at x=390, width 180, labeled "compiler flags 7 sites"; arrow to a green `#008300` box at x=610, width 90, labeled "all fixed" with bold 12px green "✓ before ship".
- **Box style:** 42px tall, 8px radius, fills `rgba(74,58,167,0.12)` / `rgba(231,76,60,0.12)` / `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "the compiler can only protect the matches you leave exhaustive".
- **Caption (12px `#444`, bottom right):** "match-site count illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the 16-cell bit grid is the exact array `["0000",...,"1111"]` with legal cells `0000`/`1010`/`1111`; the combinatorics (2×2×2×2 = 16, 3 legal, 13 illegal) are exact, while the pipeline counts (7 functions, 4 checks each, 28 total) and match-site counts are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
