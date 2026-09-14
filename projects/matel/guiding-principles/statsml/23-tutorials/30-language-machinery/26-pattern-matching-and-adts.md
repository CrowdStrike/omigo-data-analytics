# Pattern Matching & ADTs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Pattern Matching & ADTs

**Subtitle:** An algebraic data type says a value is exactly one of a few named shapes, each carrying its own data — and pattern matching is the compiler-checked way to ask "which shape?" and take it apart

## One Order, Four Possible Shapes

**Tags:** `core idea` (blue), `data as shapes` (green), `sum types` (orange)

- **The pizza shop** — every order on the kitchen board is in exactly one state at any moment, never two
- **Four shapes** — Preparing, OutForDelivery(driver, eta), Delivered(time), Cancelled(reason, refund)
- **Each carries its own data** — only a moving order has a driver; only a cancelled one has a refund
- **The ADT** — the type is just the list of shapes: an order status is one of these four, nothing else
- **Why "algebraic"** — a sum of products: each shape is a product of fields, the type a sum of shapes

*Example (italic):* Order #117 reads OutForDelivery("Ravi", 12) — it has a driver and a 12-minute ETA, and it is meaningless to ask it for a refund amount.

**Key point:** An algebraic data type lists every shape a value can take, and each shape carries exactly the fields that make sense for it — no more, no fewer.

### Visualization (canvas `c1`, 720×300)

Four rounded shape cards in a row, each showing one status constructor with its own field slots filled in with the running example's literal values, separated by bold "or" labels.

- **Title (bold 15px, `#1a5276`, top center):** "One Order Status = Exactly One of Four Shapes".
- **Cards:** four rounded rects (radius 8), width 150, height 140, top y=75, at x = `[30, 200, 370, 540]`; each has a 26px header band with bold 13px white centered text, body filled with a 0.08-alpha tint of the header hue, 1px border in the header hue.
- **Headers and hues:** "Preparing" blue `#2a78d6`; "OutForDelivery" aqua `#199e70`; "Delivered" green `#008300`; "Cancelled" orange `#d95926`.
- **Field lines (12px `#2c3e50`, left-aligned inside each card, one per line):** card 1: italic 12px `#6b7280` "(no extra data)"; card 2: `driver: "Ravi"` and `eta: 12 min`; card 3: `time: "19:42"`; card 4: `reason: "no answer"` and `refund: $18`.
- **"or" separators:** bold 13px `#6b7280` "or" centered in each of the three gaps at y=145.
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=250):** "one order is always exactly one shape — never two, never none".
- **Caption (12px `#444`, bottom right):** "illustrative — a pizza shop's order states".

## Matching Order #117 by Hand

**Tags:** `worked example` (blue), `destructuring` (green)

- **The board asks one thing** — which shape is order #117? Pattern matching tries the cases top-down
- **Case 1: Preparing** — #117 is not that shape, so this case is skipped and the match moves on
- **Case 2: OutForDelivery(driver, eta)** — the shape fits; it binds driver = "Ravi" and eta = 12 at once
- **Cases 3 and 4** — Delivered and Cancelled are never even tried; the first matching shape wins
- **One move, two jobs** — the match picked the branch AND pulled the fields out; no null checks anywhere

*Example (italic):* The display board turns OutForDelivery("Ravi", 12) into the message "Ravi arrives in 12 min" — one match did the branching and the data extraction together.

**Key point:** A match does two jobs at once: it chooses the branch for the value's shape AND unpacks that shape's fields into named variables you can use immediately.

### Visualization (canvas `c2`, 720×300)

Flow diagram: the concrete order value on the left, four case boxes tried top-down on the right, the second lighting up as the match, and the resulting message box below it.

- **Title (bold 15px, `#1a5276`, top center):** "Matching Order #117: Try the Shapes Top-Down".
- **Value box:** rounded rect x=30, y=110, width 210, height 70, 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.08)`; bold 13px `#1a5276` line "order #117" and 12px `#2c3e50` line `OutForDelivery("Ravi", 12)` centered inside.
- **Case boxes:** four rects x=300, width 250, height 34, at y = `[58, 102, 146, 190]`, 12px left-padded labels:
  - case 1: 1px `#6b7280` border, `#6b7280` text "Preparing — no, skip";
  - case 2: 2px green `#008300` border, fill `rgba(0,131,0,0.10)`, bold `#008300` text "OutForDelivery(driver, eta) — MATCH";
  - cases 3–4: 1px dashed `#e5e9ef`-toned grey borders, `#6b7280` text "Delivered(time) — never tried" and "Cancelled(reason, refund) — never tried".
- **Arrows:** 2px `#6b7280` arrow from the value box to case 1 with 11px `#6b7280` label "which shape?"; short 2px `#6b7280` down-arrow from case 1 to case 2 labeled 11px "not this shape".
- **Output box:** rounded rect x=300, y=244, width 300, height 36, 2px green border, fill `rgba(0,131,0,0.10)`; bold 12px `#008300` text `→ "Ravi arrives in 12 min"`; 2px green arrow from case 2 down to it.
- **Annotation (bold 12px green `#008300`, near x=565, y=105):** two lines: `binds driver = "Ravi"` / `eta = 12`.
- **Caption (12px `#444`, bottom right):** "illustrative — one match, branch + extraction".

## Making Wrong Data Impossible to Write

**Tags:** `where it's used` (blue), `illegal states` (red), `fewer bugs` (green)

- **The loose way** — one status string plus three optional fields: driver, deliveredTime, cancelReason
- **32 combinations** — 4 status values × on/off for each of 3 optional fields = 32 constructible records
- **Only 4 valid** — a "delivered" order with a driver but no time is one of the 28 nonsense combos
- **The ADT way** — each shape owns its fields, so exactly the 4 sensible values exist; 28 bugs vanish
- **Where you meet it** — Rust/Swift enums, Kotlin sealed classes, Haskell/OCaml, TypeScript unions

*Example (italic):* With the loose record a teammate can save status = "preparing" with a cancelReason attached; with the ADT that value cannot even be typed in.

**Key point:** ADTs shrink the space of writable values down to the meaningful ones — 32 constructible records become exactly 4, so a whole class of bugs never compiles.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison: the loose record's 32 constructible values drawn as a stacked bar (4 valid + 28 impossible) next to the ADT's bar of exactly 4, on a shared count axis.

- **Title (bold 15px, `#1a5276`, top center):** "Loose Record vs ADT: How Many Values Can You Even Write?".
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 180; y = count 0 to 32 with 12px `#444` tick labels at `[0, 8, 16, 24, 32]` and light `#e5e9ef` gridlines at 8, 16, 24, 32.
- **Bar 1 (loose record):** centered x=260, width 120, stacked: bottom segment green `#008300` fill `rgba(0,131,0,0.55)` for count 4, top segment red `#e74c3c` fill `rgba(231,76,60,0.45)` for count 28 (total 32); bold 12px labels beside the segments: green "4 valid", red "28 impossible".
- **Bar 2 (ADT):** centered x=520, width 120, single green segment for count 4, fill `rgba(0,131,0,0.55)`, 2px `#008300` border; bold 12px green label above: "4 valid — all of them".
- **X labels (bold 13px `#2c3e50`, centered below baseline):** "status string + 3 optional fields" and "ADT: four shapes".
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=85):** "28 nonsense records erased at the type level".
- **Caption (12px `#444`, bottom right):** "illustrative — 4 statuses × 2³ field on/off combos = 32".

## Isn't This Just a Switch Statement?

**Tags:** `common mistake` (red), `exhaustiveness` (orange)

- **The lookalike** — a switch on a status string also picks a branch, so the two get mixed up
- **No unpacking** — after the switch you still reach into nullable fields and hope they were filled
- **No counting** — forget a case in the switch and it compiles fine; the bug waits for a customer
- **The new shape** — add RefusedAtDoor(reason) and every match without a catch-all arm gets flagged
- **Compiler as checklist** — in this codebase that is 3 flagged matches, all fixed before shipping

*Example (italic):* The night the shop added RefusedAtDoor, the string-switch build showed a blank board at 9pm; the ADT build refused to compile until all 3 matches handled it.

**Common mistake:** Treating pattern matching as syntax sugar for switch. The real gift is exhaustiveness: the compiler proves every shape is handled everywhere — as long as matches avoid catch-all arms.

### Visualization (canvas `c4`, 720×300)

Two-panel before/after: the same fifth shape added to both codebases — the string switch compiles and fails at runtime, the ADT match fails at compile time and lists every spot to fix.

- **Title (bold 15px, `#1a5276`, top center):** "New Shape Added: RefusedAtDoor(reason) — Who Notices?".
- **Panels:** left panel x=30–345, right panel x=375–690, both y=55–255, 1px `#e5e9ef` border, white fill; panel headers bold 13px at y=72, left `#6b7280` "switch on status string", right `#1a5276` "match on the ADT".
- **Left panel body:** five 12px `#2c3e50` code-style lines at x=45, y = `[100, 120, 140, 160, 185]`: `case "preparing": ...`, `case "out": ...`, `case "delivered": ...`, `case "cancelled": ...`, then a dashed 1px red `#e74c3c` outlined row with red 12px text `"refused"? — not handled`; status lines below: 12px `#6b7280` "compiles fine" at y=215 and bold 12px red `#e74c3c` "blank board at runtime, 9pm" at y=235.
- **Right panel body:** compile-error box x=390–675, y=90–170, 2px orange `#d95926` border, fill `rgba(217,89,38,0.08)`; 12px `#2c3e50` lines inside: bold `error: non-exhaustive match`, `missing: RefusedAtDoor(reason)`, `board.rs  refunds.rs  sms.rs`; below the box, bold 12px green `#008300` at y=200: "3 matches fixed before shipping".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "the compiler is a checklist that never forgets a case".
- **Caption (12px `#444`, bottom right):** "illustrative — same change, two outcomes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red is used only for genuine error states (impossible records, the runtime crash).
- **Data:** all values are the hardcoded literals above (no randomness): the four shapes with fields Ravi / eta 12 / 19:42 / no answer / $18 refund; the case-order skip-match-never sequence for order #117; the counts 32 = 4 × 2³, split 4 valid + 28 impossible; and the 3 flagged matches (board.rs, refunds.rs, sms.rs). Text numbers and chart numbers must stay identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
