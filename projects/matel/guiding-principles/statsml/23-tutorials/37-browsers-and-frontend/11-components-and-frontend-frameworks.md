# Components & Frontend Frameworks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Components & Frontend Frameworks

**Subtitle:** Instead of hand-editing the page every time something changes, you describe the UI as a function of one state object — change the state, and every widget redraws itself to match

## Three Widgets That Must Always Agree

**Tags:** `core idea` (blue), `UI = f(state)` (green), `one source of truth` (orange)

- **The shop page** — a coffee-shop web store shows a cart badge, an item list, and a total price
- **The rule** — the three widgets must always agree: 3 items in the list means "3" on the badge
- **The old way** — every "Add" button hand-edits all three spots in the page, one line of code each
- **The component way** — each widget is a component: a function that reads one cart object and draws itself
- **The payoff** — clicking "Add" only changes the cart object; the framework redraws all three widgets

*Example (italic):* The cart holds 2 lattes and 1 muffin, so the badge shows 3, the list shows two rows, and the total shows $13 — all computed from the same object.

**Key point:** A component is a function from state to picture. You never edit the page directly — you edit the state, and the UI is recomputed to match it.

### Visualization (canvas `c1`, 720×300)

Two-panel flow diagram: imperative wiring (one button hand-updates three widgets, three arrows) vs component wiring (button updates one state box, framework fans out to three components).

- **Title (bold 15px, `#1a5276`, top center):** "Hand-Edit Three Widgets, or Edit One State Object".
- **Layout:** vertical dashed `#e5e9ef` divider at x=360; left panel labeled "imperative" (bold 12px `#6b7280` at x=30, y=52), right panel labeled "components" (same style at x=390, y=52).
- **Left panel:** orange `#d95926` rounded box at (30, 130) size 120×36 labeled "Add button" (12px); three blue `#2a78d6` boxes at (210, 70), (210, 130), (210, 190), each 120×36, labeled "badge: 3", "list: 2 rows", "total: $13"; three separate 2px orange arrows from the button to each box, each arrow tagged 11px `#d95926` "edit".
- **Right panel:** orange box "Add button" at (390, 70) size 120×36; single 2px green `#008300` arrow down to a green box at (390, 150) size 130×40 labeled "cart {L:2, M:1}" (12px, bold border); three 2px `#199e70` aqua arrows fanning right to blue boxes at (560, 70), (560, 130), (560, 190), each 120×36, labeled "Badge()", "List()", "Total()".
- **Box style:** 8px radius, fills `rgba(42,120,214,0.15)` for widgets, `rgba(217,89,38,0.12)` for buttons, `rgba(0,131,0,0.12)` for state, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, right panel, near y=250):** "one write; the framework redraws the rest".
- **Caption (12px `#444`, bottom right):** "cart contents illustrative".

## Add One Latte: Walking the Re-render by Hand

**Tags:** `worked example` (blue), `re-render` (green)

- **State before** — the cart object is `{latte: 2, muffin: 1}`; latte $5, muffin $3, so total $13
- **The click** — "Add latte" runs one line: set latte from 2 to 3; nothing touches the page yet
- **The re-render** — the framework calls Badge(), List(), and Total() again with the new cart
- **Hand-check** — Badge counts 3+1 = 4; List shows "latte × 3"; Total computes 3×5 + 1×3 = $18
- **The diff** — the framework compares old and new drawings and touches only the changed text

*Example (italic):* One state write (latte 2 → 3) turns into three widget updates: badge 3 → 4, list row "× 2" → "× 3", total $13 → $18 — none written by hand.

**Key point:** You can predict the whole screen from the state object alone: re-run each component on paper with the new cart and you get exactly what the framework draws.

### Visualization (canvas `c2`, 720×300)

Before/after re-render diagram: the state box changing on the left, three component boxes on the right each showing old value struck through and new value highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "One State Write → Three Widgets Recomputed".
- **State column:** blue `#2a78d6` rounded box at (30, 80) size 170×44 labeled "before {latte:2, muffin:1}" (12px); 2px violet `#4a3aa7` arrow down tagged bold 12px violet "+1 latte"; green `#008300` box at (30, 180) size 170×44 labeled "after {latte:3, muffin:1}".
- **Component rows (boxes at x=300, width 170, height 40, y = 70, 135, 200), 8px radius, fill `rgba(42,120,214,0.15)`:** "Badge()", "List()", "Total()" as 12px bold `#1a5276` labels inside each box's left edge.
- **Old → new values (12px `#2c3e50` at x=500, same rows):** "3 → 4" for Badge, "latte × 2 → × 3" for List, "$13 → $18" for Total; the new value in each pair bold green `#008300`.
- **Arrows:** three 2px `#199e70` aqua arrows from the "after" state box to the three component boxes.
- **Gridline:** horizontal `#e5e9ef` 1px separator lines under each component row at y = 118, 183, 248.
- **Annotation (bold 13px violet `#4a3aa7`, near x=470, y=270):** "total 3×$5 + 1×$3 = $18 — check it by hand".
- **Caption (12px `#444`, bottom right):** "prices illustrative".

## Why Dashboards Made Frameworks Win

**Tags:** `where it's used` (blue), `dashboards` (green), `internal tools` (orange)

- **Data-heavy UIs** — dashboards repeat one number in many places: a KPI tile, a chart label, an alert
- **The blow-up** — with hand-wiring, every new widget adds one more update site to every event handler
- **The count** — 3 widgets per metric means 10 metrics need 30 hand-updates; components need 10 writes
- **Internal tools** — filter panels and admin tables are the same pattern: many views, one state
- **Why frameworks won** — updates grow with state size, not with how many widgets display it

*Example (italic):* A dashboard with 10 metrics shown in 3 places each needs 30 correct hand-updates per refactor imperatively, but only the 10 state fields with components.

**Key point:** Components turn the maintenance cost of a UI from "number of places a value appears" into "number of values" — that scaling difference is why frameworks took over.

### Visualization (canvas `c3`, 720×300)

Line chart: update sites to maintain as a dashboard grows from 1 to 10 metrics (3 widgets each) — imperative line climbing steeply vs component line staying on the diagonal.

- **Title (bold 15px, `#1a5276`, top center):** "Update Sites vs Dashboard Size (3 Widgets per Metric)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = metrics 1 to 10, 12px `#444` tick labels at 1/3/5/8/10; y = update sites 0 to 30, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels.
- **Imperative line:** orange `#d95926` 3px line through metrics `[1, 3, 5, 8, 10]`, update sites `[3, 9, 15, 24, 30]`, 4px dots, bold 12px orange label "hand-wired: 3 per metric" near (metrics 7, sites 26).
- **Component line:** green `#008300` 3px line through the same metrics, state writes `[1, 3, 5, 8, 10]`, 4px dots, bold 12px green label "components: 1 per metric" near (metrics 8, sites 6).
- **Gap marker:** vertical dashed `#6b7280` (dash 4/3) line at metrics = 10 between the two lines, 12px `#6b7280` label "20 fewer places to get wrong".
- **Annotation (bold 13px ink `#1a5276`, upper left, near metrics 2, sites 28):** "cost grows with values, not widgets".
- **Caption (12px `#444`, bottom right):** "widget counts illustrative".

## When the Page Itself Becomes the Database

**Tags:** `common mistake` (red), `two sources of truth` (orange)

- **The trap** — storing the latte count only as text inside the badge, then reading it back with code
- **Two truths** — one feature reads the badge text, another keeps its own count: they drift apart
- **The drift** — a coupon handler forgets to update the badge once; badge says 3, cart object says 4
- **The symptom** — total charges for 4 items while the badge shows 3, and no single line is "the bug"
- **The fix** — state lives in exactly one object; the DOM is only ever a drawing of it, never a source

*Example (italic):* After 4 clicks the cart object holds 4 lattes but one skipped update left the badge at 3 — the customer pays $20 while the badge insists on 3 items.

**Common mistake:** Treating the rendered page as storage. The moment two places each hold "the count", every update must hit both forever — components exist so there is only one place to write.

### Visualization (canvas `c4`, 720×300)

Step chart of 4 "Add latte" clicks: cart-object count climbing 1→4 vs badge text stuck at 3 after a missed update on click 4, with the divergence highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Two Sources of Truth Drift Apart".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = clicks 0 to 4, 12px `#444` tick labels at each click; y = latte count 0 to 5, gridlines `#e5e9ef` at 1/2/3/4/5.
- **Cart-object steps:** green `#008300` 3px step line through clicks `[0, 1, 2, 3, 4]`, counts `[0, 1, 2, 3, 4]`, 4px dots, bold 12px green label "cart object (charged)" near (click 2.2, count 3.4).
- **Badge-text steps:** red `#e74c3c` 3px step line through the same clicks, counts `[0, 1, 2, 3, 3]`, 4px dots, bold 12px red label "badge text (shown)" near (click 3.1, count 2.4).
- **Miss marker:** vertical dashed `#6b7280` (dash 4/3) line at click 4, 12px `#6b7280` label "update skipped" at its top.
- **Divergence:** red `#e74c3c` bracket between count 3 and count 4 at click 4, bold 13px red annotation "shows 3, charges for 4 ($20)".
- **Annotation (bold 12px violet `#4a3aa7`, near click 1, count 4.6):** "with components this line cannot exist — the badge is recomputed".
- **Caption (12px `#444`, bottom right):** "click sequence illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); cart contents (2 lattes at $5, 1 muffin at $3, totals $13 → $18 after adding a latte), dashboard update-site counts (`[3, 9, 15, 24, 30]` imperative vs `[1, 3, 5, 8, 10]` component), and drift step counts (`[0,1,2,3,4]` vs `[0,1,2,3,3]`, $20 charge) are invented and labeled illustrative; the imperative counts follow exactly 3 widgets per metric.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
