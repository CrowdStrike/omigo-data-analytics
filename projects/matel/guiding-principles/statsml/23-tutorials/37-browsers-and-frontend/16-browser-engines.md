# Browser Engines

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Browser Engines

**Subtitle:** Dozens of browser brands, but only three engines actually draw web pages — Blink, WebKit, and Gecko

## Seven Browsers, One Coffee Shop Menu

**Tags:** `core idea` (blue), `rendering` (green), `only three` (orange)

- **The menu page** — a coffee shop posts one menu page: HTML, a price table, a photo, some CSS
- **Seven brands** — customers open it in Chrome, Edge, Brave, Opera, Samsung Internet, Safari, Firefox
- **The engine** — inside each browser, a rendering engine turns that HTML and CSS into pixels
- **Only three** — all seven brands draw the same menu with just Blink, WebKit, or Gecko
- **The shell** — the rest of a browser is bookmarks, tabs, sync; painting the page is the engine's job

*Example (italic):* Five of the seven customers see the menu drawn by the exact same engine — the brands differ, the pixel-painting code doesn't.

**Key point:** A browser engine is the component that parses HTML/CSS and paints the page; nearly every browser brand today is a shell around one of three engines — Blink, WebKit, or Gecko.

### Visualization (canvas `c1`, 720×300)

Flow diagram: seven browser boxes on the left funnel into three engine boxes on the right, arrows colored by engine.

- **Title (bold 15px, `#1a5276`, top center):** "Seven Brands, Three Engines".
- **Browser boxes (left column):** rounded boxes at x=70, width 150, height 24, at y = `[46, 78, 110, 142, 174, 210, 246]`, labels 12px `#2c3e50`: `["Chrome", "Edge", "Brave", "Opera", "Samsung Internet", "Safari", "Firefox"]`; fill `rgba(42,120,214,0.10)`, 1px `#e5e9ef` border.
- **Engine boxes (right column):** rounded boxes at x=480, width 170, height 34: "Blink" at y=100 (blue `#2a78d6` border 2px, fill `rgba(42,120,214,0.15)`), "WebKit" at y=192 (aqua `#199e70`, fill `rgba(25,158,112,0.12)`), "Gecko" at y=244 (orange `#d95926`, fill `rgba(217,89,38,0.12)`); labels bold 13px in the border color.
- **Arrows:** 2px lines from each browser box's right edge (x=220) to its engine box's left edge (x=480): Chrome/Edge/Brave/Opera/Samsung Internet → Blink in `#2a78d6`; Safari → WebKit in `#199e70`; Firefox → Gecko in `#d95926`.
- **Annotation (bold 13px blue `#2a78d6`, near x=300, y=270):** "5 of the 7 brands share Blink".
- **Caption (12px `#444`, bottom right):** "brand list representative, engine mapping factual".

## Who Runs What: The Family Tree

**Tags:** `worked example` (blue), `lineage` (green), `forks` (orange)

- **Blink** — Chrome, Edge, Opera, Brave, and Samsung Internet all render with Google's Blink
- **WebKit** — Safari runs Apple's WebKit; on iPhones every browser brand must use it too
- **Gecko** — Firefox is the only major browser still running Mozilla's independent Gecko
- **The fork line** — KDE's KHTML (1998) was forked by Apple into WebKit, which shipped in Safari (2003)
- **Fork again** — Google forked WebKit into Blink in 2013; Opera adopted it then, Edge in 2020
- **Hand-check** — open any browser's About page or user-agent string; one of the three names appears

*Example (italic):* Edge dropped its own EdgeHTML engine in 2020 and rebuilt on Blink — same logo, new rendering core.

**Key point:** Blink and WebKit share one ancestor (KHTML), so two of the "three engines" are cousins; Gecko is the only fully separate lineage.

### Visualization (canvas `c2`, 720×300)

Timeline with fork arrows: Gecko runs alone on the top lane; the bottom lane shows KHTML forking into WebKit, then WebKit forking into Blink.

- **Title (bold 15px, `#1a5276`, top center):** "One Independent Line, One Line That Forked Twice".
- **Axes:** x maps years 1998→x=70 to 2025→x=650 (about 21.5 px per year, so 2003→x≈177, 2013→x≈392, 2020→x≈542); thin 1px `#e5e9ef` vertical gridlines with 12px `#444` year labels at 1998 / 2003 / 2013 / 2020 / 2025 along a baseline at y=265.
- **Gecko lane (y=80):** orange `#d95926` 3px line from x=70 to x=650, dot at start, bold 13px orange label "Gecko (Mozilla, 1998) — powers Firefox today" above the line at x=90.
- **KHTML/WebKit lane (y=170):** mute `#6b7280` 3px segment from x=70 to x=177 labeled "KHTML (KDE)" in 12px `#6b7280`; at x=177 a dot and the line continues as aqua `#199e70` 3px to x=650, bold 13px aqua label "WebKit (Apple fork, Safari 2003)" above at x=200.
- **Blink branch:** at x=392 on the WebKit line, a dot and a blue `#2a78d6` 3px line rising to y=225 then running to x=650, bold 13px blue label "Blink (Google fork, 2013)" below at x=410; small blue dot at x=542 with 11px `#2a78d6` label "Edge joins (2020)".
- **Annotation (bold 13px violet `#4a3aa7`, near x=430, y=120):** "two of the three engines are forks of the same 1998 code".
- **Caption (12px `#444`, bottom right):** "fork years factual, lane positions schematic".

## Test Three Engines, Not Twenty Browsers

**Tags:** `where it's used` (blue), `testing` (green), `monoculture` (red)

- **Testing scope** — layout bugs live in the engine, so testing Blink, WebKit, and Gecko covers most brands
- **The shortcut** — if the coffee shop menu renders in Chrome, it will almost surely render in Brave and Opera
- **The exceptions** — Safari (WebKit) and Firefox (Gecko) can each break independently; test them separately
- **Standards power** — engine makers decide which new CSS and JS features ship; three votes steer the web
- **Monoculture worry** — with Blink near three-quarters of usage, one engine's choices become de facto standards

*Example (italic):* A team that tested only Chrome shipped a menu that broke in Safari — one WebKit check would have caught it before launch.

**Key point:** Cross-browser testing is really cross-engine testing: three engines cover almost the whole web, and losing one to monoculture means fewer independent checks on web standards.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: approximate share of page views rendered by each engine.

- **Title (bold 15px, `#1a5276`, top center):** "Roughly Where Page Views Land, by Engine".
- **Axis:** vertical 2px `#999` baseline at x=200, bars extend right, 420px = 100%; rows at y = `[80, 130, 180, 230]` with left-aligned 12px `#444` labels at x=20.
- **Bars (16px tall, 12px value labels at bar ends):**
  - "Blink (Chrome, Edge, ...)": blue `#2a78d6` fill `rgba(42,120,214,0.35)`, width 315 → "~75%"
  - "WebKit (Safari, all iOS)": aqua `#199e70` fill `rgba(25,158,112,0.30)`, width 76 → "~18%"
  - "Gecko (Firefox)": orange `#d95926` fill `rgba(217,89,38,0.30)`, width 13 → "~3%"
  - "everything else": mute `#6b7280` fill `rgba(107,114,128,0.25)`, width 17 → "~4%"
- **Annotation (bold 13px magenta `#d55181`, near x=380, y=270):** "three engines ≈ 96% of the web — test all three".
- **Caption (12px `#444`, bottom right):** "shares approximate and illustrative; ranking is real".

## Chrome on iPhone Isn't Chrome's Engine

**Tags:** `common mistake` (red), `brand vs engine` (orange)

- **The confusion** — a different browser brand does not mean a different rendering engine
- **iOS rule** — Apple's App Store rules require iPhone browsers to use WebKit, whatever the brand
- **Chrome on iPhone** — iOS Chrome is Google's interface wrapped around Apple's WebKit, not Blink
- **The consequence** — testing "Chrome on iPad" exercises Safari's engine, not desktop Chrome's
- **The flip side** — Edge, Brave, and Opera look independent but inherit nearly every Blink behavior
- **The footnote** — since 2024 EU rules force Apple to permit other engines there; elsewhere WebKit stands

*Example (italic):* A bug reported "only in Chrome on iPad" was really a WebKit bug — it reproduced in Safari and never in desktop Chrome.

**Common mistake:** Counting brands instead of engines. Chrome on Android renders with Blink; Chrome on iPhone renders with WebKit — the same logo can sit on two different engines.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same Chrome brand box feeding two different engine boxes depending on the platform.

- **Title (bold 15px, `#1a5276`, top center):** "Same Logo, Two Engines".
- **Row 1 (y=95), label 12px `#444` at x=20:** "on Android"; rounded box at x=180 labeled "Chrome (brand)" (12px `#2c3e50`, fill `rgba(42,120,214,0.15)`), 3px blue `#2a78d6` arrow to a blue-bordered box at x=430 labeled "Blink — Google's engine" with bold 12px blue "Google code paints the page".
- **Row 2 (y=205), label:** "on iPhone"; identical "Chrome (brand)" box at x=180, 3px aqua `#199e70` arrow to an aqua-bordered box at x=430 labeled "WebKit — Apple's engine" with bold 12px aqua "Safari's engine paints the page".
- **Box style:** 170–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(25,158,112,0.12)`, 12px `#2c3e50` text, 2px borders in the engine color.
- **Annotation (bold 13px red `#e74c3c`, centered near y=270):** "the logo doesn't tell you the engine — the platform can".
- **Caption (12px `#444`, bottom right):** "App Store WebKit rule as documented (EU excepted since 2024)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all coordinates and values are the hardcoded literals above (no randomness); browser-to-engine mapping, fork years (KHTML 1998 → WebKit / Safari 2003 → Blink 2013, Edge 2020), and the iOS WebKit rule are documented public history; the engine share bars (~75 / ~18 / ~3 / ~4) are approximate and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
