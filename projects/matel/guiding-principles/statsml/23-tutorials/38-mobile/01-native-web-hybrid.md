# Native, Web, Hybrid

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Native, Web, Hybrid

**Subtitle:** Three ways to build the same app — write it twice for each phone, once for the browser, or once for a tool that runs on both

## One Coffee Chain, Three Ways to Build Its App

**Tags:** `core idea` (blue), `three paths` (green), `one app` (orange)

- **The chain** — a coffee chain wants an order-ahead app: browse menu, pay, skip the line
- **Path 1: native** — write it twice, once in Swift for iPhone and once in Kotlin for Android
- **Path 2: web** — write one mobile website; any phone opens it in the browser, nothing to install
- **Path 3: hybrid** — write one shared codebase in a cross-platform tool that ships to both app stores
- **Same screens** — a customer sees the same menu and pay button; what differs is what the team wrote

*Example (italic):* The chain's "order ahead" screen exists once as a website, once as a shared hybrid codebase, or twice as separate Swift and Kotlin apps.

**Key point:** Native, web, and hybrid are not three kinds of app — they are three ways to build the same app, trading duplicate work against polish and reach.

### Visualization (canvas `c1`, 720×300)

Three-lane flow diagram: what the team writes (left boxes) flowing to where it runs (right boxes), one lane per approach.

- **Title (bold 15px, `#1a5276`, top center):** "What You Write vs Where It Runs: Three Lanes to the Same App".
- **Lanes:** three horizontal lanes at y = 85, 165, 245, each with a bold 12px lane label at x=20: "NATIVE" (blue `#2a78d6`), "WEB" (green `#008300`), "HYBRID" (violet `#4a3aa7`).
- **Native lane:** two blue `#2a78d6` rounded boxes stacked at x=110 (y=68 and y=98, 130px wide, 26px tall) labeled "Swift codebase" and "Kotlin codebase" (11px), each with its own 2px arrow to two ink `#1a5276` boxes at x=460 labeled "iPhone app" and "Android app".
- **Web lane:** one green `#008300` box at x=110 (y=152, 130×26) labeled "one website", single 2px arrow to one ink box at x=460 labeled "any phone browser".
- **Hybrid lane:** one violet `#4a3aa7` box at x=110 (y=232, 130×26) labeled "one shared codebase", arrow splitting at x=340 into two arrows reaching ink boxes at x=460 labeled "iPhone app" + "Android app" (stacked y=218/248).
- **Box style:** 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(74,58,167,0.12)`, right-side ink boxes fill `rgba(26,82,118,0.10)`, 11px `#2c3e50` text.
- **Annotation (bold 12px orange `#d95926`, near x=300, y=40):** "native = write it twice; web and hybrid = write it once".
- **Caption (12px `#444`, bottom right):** "schematic — lanes illustrative".

## Ten Features, Counted in Dev-Weeks

**Tags:** `worked example` (blue), `dev-weeks` (green), `team size` (orange)

- **The plan** — the chain wants 10 features this year; each takes about 3 dev-weeks per codebase
- **Native** — 2 codebases × 10 features × 3 weeks = 60 dev-weeks, staffed by 6 devs (3 per platform)
- **Web** — 1 codebase × 10 × 3 = 30 dev-weeks, staffed by 2 devs
- **Hybrid** — 1 shared codebase (30) plus ~6 dev-weeks of per-platform glue = 36 dev-weeks, 3 devs
- **Calendar check** — divide by team size: native 60/6 = 10 weeks, web 30/2 = 15, hybrid 36/3 = 12
- **The surprise** — native finishes first on the calendar, but only because it pays for the biggest team

*Example (italic):* Feature #7, "reorder my usual", is built twice for native (6 dev-weeks) but once for web (3) and once plus a little glue for hybrid (~3.6).

**Key point:** Native roughly doubles the build cost (60 vs 30–36 dev-weeks here); whether it also doubles the calendar time depends entirely on how many devs you hire.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: total dev-weeks for the 10-feature year per approach, with team size and calendar weeks annotated at each bar's end.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Features, Three Bills: 60 vs 30 vs 36 Dev-Weeks".
- **Axis:** vertical 2px `#999` baseline at x=180, bars extend right, scale 7 px per dev-week (max width 420 at 60); x gridlines `#e5e9ef` at 15/30/45/60 dev-weeks with 12px `#444` labels at y=270.
- **Rows (bar tops at y = 75, 140, 205), each with a left-aligned 12px `#444` label at x=20:**
  - "Native (2 codebases)": blue `#2a78d6` bar width 420 (60 dev-weeks), 12px label right-aligned above the bar end "60 · 6 devs · 10 wks"
  - "Web (1 codebase)": green `#008300` bar width 210 (30 dev-weeks), label "30 · 2 devs · 15 wks"
  - "Hybrid (1 + glue)": violet `#4a3aa7` bar width 210 (30 shared) with an orange `#d95926` glue segment width 42 (6 dev-weeks) appended, label "36 · 3 devs · 12 wks"
- **Bar style:** 26px tall, fills at 0.35 alpha with solid 2px borders in the same hue; glue segment solid orange with 11px white "glue" text inside.
- **Annotation (bold 13px magenta `#d55181`, right-aligned at x=w-14, y=115):** "native costs 2×, but the 6-dev team lands it first".
- **Caption (12px `#444`, bottom right):** "3 dev-weeks per feature per codebase — illustrative".

## The Build-versus-Reach Tradeoff

**Tags:** `where it's used` (blue), `tradeoff` (orange), `middle ground` (green)

- **The tension** — every product team trades build cost against how polished and capable the app feels
- **Native wins polish** — full speed, full hardware access (camera, push, offline), platform-perfect feel
- **Web wins reach** — one URL works everywhere instantly, but the browser gates hardware and speed
- **Hybrid splits it** — one codebase; React Native drives native widgets, Flutter paints its own pixels
- **The catch** — hybrid still needs native glue for deep platform features, and inherits the tool's limits
- **Picking a lane** — heavy hardware use points native; content and forms point web; most apps sit between

*Example (italic):* The chain's loyalty-card barcode needs the camera and offline wallet — easy native, clunky in a browser, fine in hybrid with one small glue module.

**Key point:** There is no free option: native buys polish with duplicate work, web buys reach with capability limits, and hybrid buys most of both by accepting a framework in the middle.

### Visualization (canvas `c3`, 720×300)

Positioning scatter: x = build cost in dev-weeks (from the worked example), y = polish/device-access score, one labeled dot per approach.

- **Title (bold 15px, `#1a5276`, top center):** "Cost vs Polish: Where Each Approach Lands".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = dev-weeks 0 to 70, 12px `#444` tick labels at 0/35/70, axis title "build cost (dev-weeks)" 12px `#444` centered below; y = polish & device access 0 to 100, gridlines `#e5e9ef` at 25/50/75, axis title rotated 12px `#444`.
- **Dots (10px radius, solid fill, bold 12px label beside each in the same hue):**
  - Web: green `#008300` at (30 dev-weeks, score 55), label "Web — 30 wks, reach everywhere"
  - Hybrid: violet `#4a3aa7` at (36 dev-weeks, score 80), label "Hybrid — 36 wks"
  - Native: blue `#2a78d6` at (60 dev-weeks, score 95), label "Native — 60 wks"
- **Frontier line:** dashed `#6b7280` (dash 4/3) 2px curve through the three dots showing the tradeoff frontier.
- **Annotation (bold 13px aqua `#199e70`, near x=36-dot, y=80):** "hybrid: ~85% of the polish at 60% of the cost".
- **Caption (12px `#444`, bottom right):** "polish scores illustrative; dev-weeks from the worked example".

## A Website in a Wrapper Is Not Flutter

**Tags:** `common mistake` (red), `hybrid ≠ hybrid` (orange)

- **The confusion** — "hybrid" gets used for two very different things, and teams buy the wrong one
- **Webview wrapper** — the old style: your website stuffed inside an app shell, drawn by the browser engine
- **Compiled cross-platform** — shared code drawing native widgets (RN) or its own fast engine (Flutter)
- **Why it matters** — wrappers scroll and animate like a webpage; compiled tools feel like a native app
- **The tell** — if every camera or push call crosses a JavaScript bridge into the shell, it is a wrapper
- **The mistake** — picking a wrapper to "go hybrid", then rewriting natively when users call it sluggish

*Example (italic):* The chain wraps its website in an app shell to hit the stores fast; menu scrolling stutters, ratings sink, and the rewrite costs more than starting hybrid properly.

**Common mistake:** Treating all one-codebase options as equivalent. A webview wrapper is web with an install step; React Native and Flutter are a genuinely different, closer-to-native middle ground.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a webview wrapper (website inside a shell, bridge to hardware) vs a compiled cross-platform app (shared code straight to native widgets).

- **Title (bold 15px, `#1a5276`, top center):** "Two Things Called Hybrid: Wrapper vs Compiled".
- **Row 1 (y=95), label 12px `#444` at x=20:** "webview wrapper"; green `#008300` rounded box at x=150 labeled "your website" (12px), 3px arrow to an orange `#d95926` box at x=340 labeled "browser engine in a shell", 3px arrow to a red `#e74c3c` box at x=555 labeled "JS bridge to camera/push" with bold 12px red "✗ webpage feel".
- **Row 2 (y=205), label:** "compiled cross-platform"; violet `#4a3aa7` box at x=150 labeled "one shared codebase", 3px arrow to a blue `#2a78d6` box at x=340 labeled "compiles per platform", 3px arrow to a green `#008300` box at x=555 labeled "native widgets (RN) / own engine (Flutter)" with bold 12px green "✓ native feel".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(74,58,167,0.12)` / `rgba(42,120,214,0.15)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "ask what draws the pixels — the browser, or the platform".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all numbers are the hardcoded values above (no randomness); the worked example assumes 10 features at 3 dev-weeks per feature per codebase, giving dev-weeks 60 (native, 6 devs, 10 calendar weeks) / 30 (web, 2 devs, 15 weeks) / 36 (hybrid = 30 shared + 6 glue, 3 devs, 12 weeks); polish scores 95/80/55 in c3 are invented and labeled illustrative; c2 bar widths use 7 px per dev-week (420/210/210+42).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
