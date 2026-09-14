# Cross-Platform Frameworks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cross-Platform Frameworks

**Subtitle:** React Native and Flutter let one team write an app once and ship it to both app stores — one codebase, two phones

## One Codebase, Two App Stores

**Tags:** `core idea` (blue), `one team` (green), `mobile` (orange)

- **The shop** — a coffee chain wants its ordering app on both the Apple and Google app stores
- **The old way** — hire an iOS team (Swift) and an Android team (Kotlin) and build every screen twice
- **The framework** — the team writes the app once, and the framework builds both store apps from it
- **Two names** — React Native (JavaScript, from Meta) and Flutter (Dart, from Google) are the big two
- **The split** — of ~50,000 lines, ~46,000 are shared; ~2,000 per platform are native glue code

*Example (italic):* The chain's 4-person team ships v1 to both stores in one release cycle — a Swift-plus-Kotlin rebuild would have needed two teams writing every screen twice.

**Key point:** A cross-platform framework turns one codebase into two real installable apps — the menu, cart, and checkout logic is written exactly once.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one shared-codebase box fanning out through the framework toolchain into an iOS app and an Android app, each with a small native-glue attachment.

- **Title (bold 15px, `#1a5276`, top center):** "One Codebase Becomes Two Store Apps".
- **Source box:** rounded rect at x=40, y=110, 190×70, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 13px `#2c3e50` text "shared codebase" with 12px `#6b7280` line "46,000 lines — menu, cart, checkout".
- **Toolchain box:** rounded rect at x=300, y=118, 130×54, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, 12px text "framework build (RN / Flutter)".
- **Target boxes:** rounded rect at x=500, y=55, 180×56, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px text "iOS app — App Store"; rounded rect at x=500, y=185, 180×56, fill `rgba(25,158,112,0.12)`, 2px `#199e70` border, 12px text "Android app — Play Store".
- **Glue tabs:** small 12px `#d95926` labels "+2,000 native glue" attached under each target box.
- **Arrows:** 3px `#6b7280` arrows source→toolchain, then toolchain fanning to each target box.
- **Annotation (bold 13px blue `#2a78d6`, centered near y=270):** "92% of the code is written once (46,000 of 50,000 lines)".
- **Caption (12px `#444`, bottom right):** "line counts illustrative".

## Native Widgets vs Painted Pixels

**Tags:** `worked example` (blue), `architecture` (green), `RN vs Flutter` (orange)

- **Same button** — the "Order now" button must appear on both phones; the two frameworks differ in how
- **React Native** — JavaScript says "render a button"; a bridge asks the OS to draw its native widget
- **The bridge** — messages cross between the JS engine and native code; the widgets are the platform's own
- **Flutter** — Dart compiles to machine code, and Flutter's rendering engine paints every pixel itself
- **The surface** — to Flutter the OS just supplies a blank drawing surface; Flutter fills it in
- **The trade** — RN buttons look native on each OS automatically; Flutter looks identical everywhere

*Example (italic):* On an iPhone the React Native button IS the platform's own native button; the Flutter button is drawn by Flutter's engine, pixel-identical on both phones.

**Key point:** React Native delegates drawing to each platform's own widgets through a bridge; Flutter bypasses them and rasterizes its own UI with its rendering engine — both are documented architecture.

### Visualization (canvas `c2`, 720×300)

Two-row flow diagram: how one "Order now" button reaches the screen in React Native (via bridge to a native widget) vs Flutter (engine paints pixels onto a surface).

- **Title (bold 15px, `#1a5276`, top center):** "Two Roads to the Same Button".
- **Row 1 (y=95), label 12px `#444` at x=20:** "React Native"; blue `#2a78d6` rounded box at x=140 labeled "JS: <Button>" (12px), 3px arrow to a violet `#4a3aa7` box at x=330 labeled "bridge message", 3px arrow to a green `#008300` box at x=530 labeled "OS draws native button".
- **Row 2 (y=205), label:** "Flutter"; blue box at x=140 labeled "Dart widget", 3px arrow to an aqua `#199e70` box at x=330 labeled "engine paints pixels", 3px arrow to a green box at x=530 labeled "OS shows the surface".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(25,158,112,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "RN borrows the platform's widgets; Flutter brings its own".
- **Caption (12px `#444`, bottom right):** "schematic of documented architecture".

## One Team Instead of Two

**Tags:** `where it's used` (blue), `team economics` (green), `when native wins` (orange)

- **The bill** — the same 10 features cost 12 engineer-months on iOS plus 12 on Android built natively
- **The saving** — the cross-platform build takes ~14 engineer-months: 10 shared plus ~2 of glue each side
- **One fix** — a pricing bug is fixed once and ships to both stores in the same release
- **Who uses it** — startups and small teams that cannot staff two native teams are the core audience
- **When native wins** — games, AR, heavy sensor work, and day-one use of brand-new OS features
- **The ceiling** — a framework exposes an OS feature only after someone wraps it in a plugin

*Example (italic):* The coffee chain ships both apps for 14 engineer-months — about 40% less than the 24 a two-team native build would cost.

**Key point:** Cross-platform trades some peak performance and day-one API access for building and maintaining every feature once instead of twice — for most business apps that trade is a win.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: engineer-months to ship the same 10 features — native iOS, native Android, their combined total, and the cross-platform build.

- **Title (bold 15px, `#1a5276`, top center):** "Same 10 Features: Engineer-Months by Approach".
- **Axis:** bars start at x=230, max width 440 mapping 0–24 engineer-months; vertical gridlines `#e5e9ef` at 6/12/18/24 with 12px `#6b7280` value labels along the bottom.
- **Rows (top to bottom at y = 70, 115, 160, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "native iOS team": blue `#2a78d6` bar width 220 (12 months)
  - "native Android team": aqua `#199e70` bar width 220 (12 months)
  - "both native teams (total)": violet `#4a3aa7` bar width 440 (24 months)
  - "cross-platform" with second label line "(10 shared + 2×2 glue)": green `#008300` bar width 257 (14 months)
- **Bar style:** 22px tall, solid fills, 12px `#2c3e50` value labels ("12", "12", "24", "14") at bar ends.
- **Annotation (bold 13px green `#008300`, right side near y=250):** "14 vs 24 — about 40% cheaper, one bug tracker".
- **Caption (12px `#444`, bottom right):** "engineer-months illustrative".

## "Write Once" Still Leaves Platform Work

**Tags:** `common mistake` (red), `native modules` (orange)

- **The myth** — "write once, run anywhere" gets read as "zero platform-specific code, ever"
- **The reality** — camera, push notifications, Bluetooth, and widgets need per-platform native modules
- **The shares** — menu/cart is 100% shared, payments 90%, push 75%, QR camera scan 60%, widgets 20%
- **OS drift** — each new iOS or Android release can break the glue code; shared logic is not exempt
- **The plan** — even a cross-platform team should include someone who can read Swift and Kotlin

*Example (italic):* The coffee app's loyalty QR scanner needed a native camera module on each platform — the "shared" feature turned out to be only 60% shared.

**Common mistake:** Assuming a cross-platform framework removes the need for native skills. The last 10–80% of hardware-adjacent features is platform code, and someone on the team must be able to write it.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: percent of code shared across platforms per feature of the coffee app, sorted from fully shared to mostly native.

- **Title (bold 15px, `#1a5276`, top center):** "How Much of Each Feature Is Actually Shared".
- **Axis:** bars start at x=230, max width 440 mapping 0–100%; vertical gridlines `#e5e9ef` at 25/50/75/100 with 12px `#6b7280` "%" labels along the bottom.
- **Rows (top to bottom at y = 60, 102, 144, 186, 228), each with a left-aligned 12px `#444` label at x=20:** features `["menu & cart", "payments", "push notifications", "QR camera scan", "home-screen widgets"]`, shared percents `[100, 90, 75, 60, 20]`, bar widths `[440, 396, 330, 264, 88]`.
- **Bar colors:** green `#008300` (100), aqua `#199e70` (90), blue `#2a78d6` (75), orange `#d95926` (60), magenta `#d55181` (20); 20px tall, 12px `#2c3e50` percent labels at bar ends.
- **Annotation (bold 13px magenta `#d55181`, right side near y=260):** "the closer to hardware, the less is shared".
- **Caption (12px `#444`, bottom right):** "shared percents illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); line counts (46,000 shared of 50,000; 2,000 glue per platform), engineer-months (12 / 12 / 24 / 14), and per-feature shared percents (100 / 90 / 75 / 60 / 20) are invented and labeled illustrative; the bridge-vs-own-engine rendering contrast is documented React Native / Flutter architecture, not an invented claim.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
