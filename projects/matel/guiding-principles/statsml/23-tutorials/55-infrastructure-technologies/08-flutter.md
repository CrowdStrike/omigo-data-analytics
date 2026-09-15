# Flutter

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Flutter

**Subtitle:** Google's UI toolkit draws every pixel itself with its own engine, so one Dart codebase looks identical on iOS and Android — by construction, not by careful testing

## One Button, Painted the Same Everywhere

**Tags:** `core idea` (blue), `paint it yourself` (green), `Dart` (orange)

- **The brand button** — a team wants a custom rounded gradient button, identical on iOS and Android
- **The wrap-native way** — frameworks like React Native map the button onto each OS's real widget
- **The drift** — each OS renders its own widget its own way: corner radius, ripple, font all differ subtly
- **The Flutter way** — Flutter ignores native widgets and paints the button pixel by pixel with its engine
- **The engine** — Skia (and now Impeller) rasterizes the whole screen, like a game engine drawing a frame
- **The result** — the button is identical on both phones because the same code drew both screenshots

*Example (italic):* The design review compares iOS and Android screenshots of the button — with Flutter they match pixel for pixel, because the same paint code produced both.

**Key point:** Flutter's core bet is paint-it-yourself: instead of asking each OS to draw its widgets, one engine draws everything, so consistency across platforms is guaranteed by construction.

### Visualization (canvas `c1`, 720×300)

Two-row comparison diagram: the same brand button rendered on iOS and Android, top row wrap-native (two subtly different buttons), bottom row Flutter (two identical buttons).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Brand Button: Wrap-Native Drifts, Flutter Matches".
- **Row 1 (centered y=110), label 12px `#444` at x=20:** "wrap-native"; two rounded button shapes side by side — iOS button at x=200 (width 170, height 44, corner radius 10, fill `rgba(42,120,214,0.30)`, 13px label "Buy now"), Android button at x=440 (width 170, height 44, corner radius 4, fill `rgba(42,120,214,0.45)`, bold 13px label "BUY NOW") — deliberately different radius, fill, and text casing.
- **Row 1 callout (bold 12px orange `#d95926`, right of the Android button):** "radius, shade, casing all drift".
- **Row 2 (centered y=215), label:** "Flutter"; two identical buttons at x=200 and x=440 (width 170, height 44, corner radius 8, fill `rgba(0,131,0,0.30)`, 2px `#008300` border, 13px label "Buy now" on both).
- **Row 2 callout (bold 12px green `#008300`, right of the second button):** "same engine painted both — identical".
- **Column headers (12px `#6b7280`):** "iOS" above x≈285, "Android" above x≈525, at y=55.
- **Caption (12px `#444`, bottom right):** "button shapes schematic".

## Counting the Pixels That Differ

**Tags:** `worked example` (blue), `by construction` (green)

- **The test** — render the 170×44 button on both platforms and diff the two screenshots pixel by pixel
- **The button** — 170 × 44 = 7,480 pixels in the button's bounding box (exact)
- **Wrap-native diff** — different corner radius and ripple shade leave 1,240 differing pixels (illustrative)
- **Flutter diff** — the same paint code ran on both, so 0 of 7,480 pixels differ
- **Hand-check** — 1,240 / 7,480 ≈ 16.6% of the wrap-native button disagrees between platforms (exact from those counts)
- **The lesson** — Flutter's zero is not better testing; it is the same rasterizer producing both images

*Example (italic):* The screenshot diff tool reports 1,240 mismatched pixels for the wrap-native pair and 0 for the Flutter pair — 16.6% vs 0% of the 7,480-pixel button.

**Key point:** Cross-platform consistency in Flutter is not a test you pass but a property you inherit — one engine, one framebuffer recipe, zero pixels left to the OS's taste.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: differing pixels between iOS and Android screenshots of the button, wrap-native vs Flutter, with the 7,480-pixel total as a reference line.

- **Title (bold 15px, `#1a5276`, top center):** "Screenshot Diff of the 7,480-Pixel Button: 1,240 vs 0".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = differing pixels 0 to 1,500, gridlines `#e5e9ef` at 500/1,000/1,500 with 12px `#444` tick labels.
- **Bar 1 (wrap-native):** centered at x=250, width 110, height scaled to 1,240 (≈149px tall), fill orange `#d95926` at `rgba(217,89,38,0.55)` with 2px `#d95926` border; bold 13px `#d95926` value label "1,240 px differ" above the bar; 12px `#444` label "wrap-native" below the baseline.
- **Bar 2 (Flutter):** centered at x=480, width 110, height 0 — draw a 3px green `#008300` tick on the baseline instead; bold 13px `#008300` value label "0 px differ" above it; 12px `#444` label "Flutter" below.
- **Annotation (bold 13px green `#008300`, near x=480, y=110):** "same paint code drew both screenshots".
- **Side note (12px `#6b7280`, top right):** "button box = 170 × 44 = 7,480 px".
- **Caption (12px `#444`, bottom right):** "pixel counts illustrative; 7,480 and 16.6% exact from them".

## Why Teams Pick a Self-Painting Toolkit

**Tags:** `where it's used` (blue), `one codebase` (green)

- **One codebase** — the same Dart code ships to iOS, Android, web, and desktop; Flutter expanded to all four
- **Everything is a widget** — screens are trees of small widgets composed together, styling included
- **Release builds** — Dart compiles ahead-of-time to native machine code for shipped apps
- **Dev builds** — hot reload injects edited code into the running app, keeping state, in about a second
- **Design-heavy apps** — brands with strong custom design gain the most; the OS look was never the goal
- **The payoff** — one UI team, one review, one bug list instead of two parallel native implementations

*Example (italic):* The team edits the button's gradient, hits save, and hot reload repaints the running app in roughly a second — no rebuild, no lost navigation state.

**Key point:** Flutter trades on leverage: one widget tree, painted by one engine, compiled ahead-of-time for release and hot-reloaded in development, reaching every platform from a single codebase.

### Visualization (canvas `c3`, 720×300)

Flow diagram: one Dart codebase box fanning out through the Flutter engine to four platform boxes, with a dev-loop side lane for hot reload.

- **Title (bold 15px, `#1a5276`, top center):** "One Dart Codebase, One Engine, Four Targets".
- **Source box:** rounded box at x=40, y=125 (150×50, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border), bold 13px `#2c3e50` text "Dart codebase\n(widget tree)".
- **Engine box:** rounded box at x=280, y=125 (160×50, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border), bold 13px text "Flutter engine\n(Skia / Impeller)"; 3px `#6b7280` arrow from source box to engine box.
- **Target boxes (x=540, 110×36 each, at y = 60, 120, 180, 240, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px text):** "iOS", "Android", "Web", "Desktop"; a 2px `#008300` arrow from the engine box to each.
- **Dev lane:** dashed violet `#4a3aa7` (dash 5/4) arrow curving from above the source box back into it, bold 12px violet label "hot reload — keep state, ~1s (dev)".
- **Release label (12px `#6b7280`, under the engine-to-targets arrows):** "release: Dart compiled ahead-of-time".
- **Annotation (bold 13px green `#008300`, bottom center y=285):** "the engine paints every pixel on every target".

## The Bill for Painting Everything Yourself

**Tags:** `common mistake` (red), `trade-off` (orange)

- **The assumption** — teams expect a Flutter app to feel native on each OS for free
- **The reality** — nothing is native by default; the OS's own look and feel must be imitated, not inherited
- **Platform behaviors** — scroll physics, context menus, accessibility hooks all need framework support
- **The treadmill** — each new iOS or Android release changes conventions; the framework must catch up
- **The mirror trade** — wrap-native apps inherit new OS looks automatically but drift apart visually
- **The mistake** — choosing paint-it-yourself for an app whose whole value is feeling deeply platform-native

*Example (italic):* A new OS version ships a redesigned context menu; native apps get it on day one, while the Flutter app shows last year's imitation until a framework update lands.

**Common mistake:** Reading "same UI everywhere" as "native UI everywhere." Flutter guarantees consistency, not nativeness — the paint-it-yourself trade means every OS convention is imitated, and every OS release restarts the imitation clock.

### Visualization (canvas `c4`, 720×300)

Two-column trade-off table drawn on canvas: paint-it-yourself vs wrap-native, four rows scoring consistency, native feel, OS-release lag, and codebases, with colored win/lose ticks.

- **Title (bold 15px, `#1a5276`, top center):** "The Trade: Consistency vs Nativeness".
- **Column headers (bold 13px, y=70):** "paint-it-yourself (Flutter)" in `#008300` centered at x≈330, "wrap-native" in `#2a78d6` centered at x≈560; row labels 12px `#444` left-aligned at x=25.
- **Rows (y = 110, 155, 200, 245), separated by 1px `#e5e9ef` lines:**
  - "pixel consistency across OSes": green bold "identical by construction" / orange "drifts per OS widget"
  - "native look and feel": orange "imitated, not inherited" / green bold "real OS widgets"
  - "new OS release": orange "wait for framework update" / green bold "adopts new look day one"
  - "UI codebases to maintain": green bold "one" / orange "one + per-OS quirks"
- **Cell text:** 12px, wins bold in `#008300`/`#2a78d6`, losses in `#d95926`.
- **Annotation (bold 13px magenta `#d55181`, bottom center y=285):** "neither column wins — pick by whether your app's value is brand consistency or platform feel".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the screenshot-diff pixel counts (1,240 and 0) and the ~1s hot-reload time are invented and labeled illustrative; the button box arithmetic 170 × 44 = 7,480 and 1,240 / 7,480 ≈ 16.6% are exact given those counts; Flutter facts (Skia/Impeller engine, Dart AOT release compilation, stateful hot reload, everything-is-a-widget composition, web/desktop expansion) are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
