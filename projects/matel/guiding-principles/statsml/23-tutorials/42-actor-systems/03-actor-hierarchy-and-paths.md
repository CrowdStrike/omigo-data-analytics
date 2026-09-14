# Actor Hierarchy & Paths

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Actor Hierarchy & Paths

**Subtitle:** Every actor is spawned by another actor, forming a tree with filesystem-like paths — and whoever spawns you is the one who supervises you

## The Orders Service That Spawns Its Own Helpers

**Tags:** `core idea` (blue), `tree of actors` (green), `paths` (orange)

- **The service** — an online shop runs an orders actor that receives every incoming order message
- **The spawn** — for the risky work, orders spawns two children: a payment actor and an inventory actor
- **The tree** — the system's root guardian sits at `/`, user actors under `/user`, so orders lives at `/user/orders`
- **The children** — the spawned helpers get paths under their parent: `/user/orders/payment` and `/user/orders/inventory`
- **The rule** — the path records who spawned whom, and the spawner becomes the supervisor of its children

*Example (italic):* When order #17 arrives, `/user/orders` forwards the card charge to `/user/orders/payment` — a child it created and is responsible for.

**Key point:** Actors form a tree exactly like a filesystem forms folders: your path is your parent's path plus your name, and your parent — not some global manager — supervises you.

### Visualization (canvas `c1`, 720×300)

Tree diagram of the actor hierarchy drawn like a filesystem: root guardian at top, `/user` and `/system` branches, orders under `/user`, its two children below.

- **Title (bold 15px, `#1a5276`, top center):** "Who Spawns You Determines Your Path — and Your Supervisor".
- **Layout:** rounded boxes (8px radius, 40px tall) at hardcoded centers: root "/" at (360, 70); "/system" at (170, 140) and "/user" at (470, 140); "/user/orders" at (470, 205); "/user/orders/payment" at (330, 268) and "/user/orders/inventory" at (600, 268).
- **Box style:** root fill `rgba(74,58,167,0.12)` with 2px violet `#4a3aa7` border; "/system" fill `rgba(107,114,128,0.10)` with 1px mute `#6b7280` border; "/user" and "/user/orders" fill `rgba(42,120,214,0.15)` with 2px blue `#2a78d6` border; the two children fill `rgba(0,131,0,0.12)` with 2px green `#008300` border; 12px `#2c3e50` labels, widths 100–190px.
- **Edges:** 2px mute `#6b7280` parent-to-child lines from root to `/system` and `/user`, from `/user` to `/user/orders`, and from `/user/orders` to both children; small 12px mute label "spawns = supervises" beside the two lowest edges.
- **Annotation (bold 13px violet `#4a3aa7`, at x≈70, y≈235):** "a path is a family history".
- **Caption (12px `#444`, bottom right):** "tree structure exact, actor names illustrative".

## Order #17 Crashes Payment: Restart, Resume, or Stop

**Tags:** `worked example` (blue), `supervision` (green), `crash handling` (red)

- **The walk** — order #17's charge travels root → `/user` → `/user/orders` → `/user/orders/payment`
- **The crash** — the payment child throws on a malformed card number and dies mid-message
- **The escalation** — the failure does not go to a global handler; it goes to the parent, `/user/orders`
- **Restart** — the parent replaces the child with a fresh one: state wiped clean, mailbox of 4 waiting orders kept
- **Resume** — the parent keeps the same child and state, skips order #17, and lets the 4 waiting orders proceed
- **Stop** — the parent kills the child for good; the 4 waiting orders in its mailbox are lost

*Example (italic):* `/user/orders` picks Restart: payment #17 is dropped, a fresh payment actor boots, and waiting orders #18–#21 are processed as normal.

**Key point:** A crash is a message to the parent, and only the parent chooses the strategy — restart (fresh state, keep mailbox), resume (keep state, skip the poison message), or stop (child and mailbox gone).

### Visualization (canvas `c2`, 720×300)

Three-row outcome diagram: the same crash on order #17 handled by each supervision strategy, showing what happens to state and to the 4 queued orders.

- **Title (bold 15px, `#1a5276`, top center):** "One Crash at `/user/orders/payment`, Three Parent Decisions".
- **Rows (top to bottom at y = 95, 175, 255), each starting with a bold 13px strategy label at x=20:** "Restart" in green `#008300`, "Resume" in yellow `#c98500`, "Stop" in red `#e74c3c`.
- **Row 1 (Restart):** red-bordered box at x=130 labeled "crash on #17" (fill `rgba(231,76,60,0.12)`), 3px arrow to a green box at x=330 labeled "new child, state wiped", arrow to a blue `#2a78d6` box at x=545 labeled "#18–#21 processed".
- **Row 2 (Resume):** same crash box, arrow to a yellow `#c98500`-bordered box at x=330 labeled "same child, #17 skipped" (fill `rgba(201,133,0,0.12)`), arrow to a blue box at x=545 labeled "#18–#21 processed".
- **Row 3 (Stop):** same crash box, arrow to a red box at x=330 labeled "child terminated", arrow to a red-bordered box at x=545 labeled "#18–#21 lost" with bold 12px red "✗".
- **Box style:** 130–170px wide, 38px tall, 8px radius, 12px `#2c3e50` text; arrows 3px mute `#6b7280`.
- **Annotation (bold 13px green `#008300`, right side near y=55):** "the parent decides — nobody else".
- **Caption (12px `#444`, bottom right):** "order numbers illustrative".

## Failure Stays Inside Its Subtree

**Tags:** `where it's used` (blue), `let it crash` (green), `isolation` (orange)

- **Blast radius** — a payment crash is handled at `/user/orders`; actors outside that subtree never notice
- **Let it crash** — instead of defensive try/catch everywhere, children fail fast and parents restart them
- **Addressing** — any actor can be reached by path, like a URL: send to `/user/orders/inventory` by name
- **Layered risk** — the deeper the actor, the more disposable it is; the root guardian should never die
- **Real systems** — Erlang/OTP supervision trees and Akka actor systems are built on exactly this shape

*Example (italic):* Of the shop's 120 running actors, the payment crash touches exactly 2 — the dead child and the parent that restarts it — while the other 118 keep serving.

**Key point:** The hierarchy turns failure into a local event: each crash is contained and repaired inside its own subtree, which is why "let it crash" is a strategy rather than negligence.

### Visualization (canvas `c3`, 720×300)

Subtree blast-radius diagram: the full actor tree with the crash zone shaded, plus a small actors-affected tally proving containment.

- **Title (bold 15px, `#1a5276`, top center):** "Blast Radius of One Crash: 2 Actors of 120".
- **Left diagram (x 20–460):** the c1 tree redrawn compact — root "/" at (240, 70), "/user" at (240, 130), "/user/orders" at (240, 190), children "payment" at (140, 250) and "inventory" at (340, 250); same box styles as c1 but the payment box gets a 2px red `#e74c3c` border and bold 12px red "✗ crashed" beneath it.
- **Crash zone:** dashed 2px orange `#d95926` rounded rectangle (dash 6/4) enclosing only "/user/orders" and "payment", 12px orange label "handled here" at its top-left corner.
- **Right tally (x 490–700):** two horizontal bars on a 2px `#999` baseline at x=490 — "affected: 2" red `#e74c3c` bar width 4px at y=120, "unaffected: 118" blue `rgba(42,120,214,0.30)` bar width 200px at y=170; 12px `#444` labels left of each bar, 11px counts at bar ends.
- **Annotation (bold 13px orange `#d95926`, at x≈490, y≈230):** "the other 118 actors never notice".
- **Caption (12px `#444`, bottom right):** "actor counts illustrative".

## The Flat Sea of Top-Level Actors

**Tags:** `common mistake` (red), `no supervision` (orange), `design smell` (blue)

- **The smell** — every actor spawned directly under `/user`: 120 siblings, one giant flat layer
- **Lost meaning** — with no depth, a crash escalates straight to the guardian, which knows nothing about billing
- **One policy fits none** — the guardian must pick a single restart strategy for payment, email, and logging alike
- **Risky parents** — the twin mistake: the orders actor parses card numbers itself instead of delegating
- **The fix** — parents hold state and route; disposable children do the work that can fail

*Example (italic):* In the flat design, the same malformed card #17 escalates to `/user`'s guardian, whose blanket policy restarts unrelated actors along with payment.

**Common mistake:** Spawning everything at the top level throws away supervision — the tree only isolates failure if risky work is pushed down into disposable children while parents stay safe and boring.

### Visualization (canvas `c4`, 720×300)

Side-by-side comparison: a flat sea of siblings under the guardian (left) versus a supervised tree with a disposable child (right), same crash in both.

- **Title (bold 15px, `#1a5276`, top center):** "Flat Sea vs Supervised Tree: the Same Crash, Two Fates".
- **Left panel (x 20–340), 13px bold `#e74c3c` header "flat" at (170, 55):** guardian box "/user" at (170, 90) fill `rgba(107,114,128,0.10)`; a row of 6 small sibling boxes (44px wide, 30px tall) at y=170, x centers 55/100/145/190/235/280, labeled "a"–"f"; box "d" gets a 2px red `#e74c3c` border with bold 12px red "✗" above it; thin 1px mute lines from the guardian fanning to all 6; bold 12px red label "escalates to the guardian" at (170, 230).
- **Right panel (x 380–700), 13px bold `#008300` header "tree" at (540, 55):** "/user" box at (540, 90); "orders" blue-bordered box at (540, 155) fill `rgba(42,120,214,0.15)`; children "payment" at (455, 225) and "inventory" at (625, 225) green-bordered fill `rgba(0,131,0,0.12)`; "payment" gets a red "✗" plus a curved 2px green `#008300` arrow looping from "orders" back to it labeled 12px green "restarted by parent".
- **Divider:** vertical 1px grid `#e5e9ef` line at x=360 from y=50 to y=280.
- **Annotation (bold 13px magenta `#d55181`, centered near y=278):** "depth is the supervision policy".
- **Caption (12px `#444`, bottom right):** "actor layout illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all geometry is the hardcoded coordinates above (no randomness); paths `/user/orders/payment` and `/user/orders/inventory`, order numbers #17–#21, the 4 queued orders, and the 2-of-120 blast-radius tally are invented and labeled illustrative; the three supervision strategies (restart / resume / stop) and their state-vs-mailbox effects are the real Erlang/Akka semantics.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
