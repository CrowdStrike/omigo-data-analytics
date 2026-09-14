# The Testing Pyramid

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Testing Pyramid

**Subtitle:** Many fast unit tests at the bottom, some integration tests in the middle, a few end-to-end tests at the top — the shape that keeps a test suite fast and trustworthy

## One Checkout, Three Ways to Test It

**Tags:** `core idea` (blue), `three layers` (green), `Mike Cohn` (orange)

- **The service** — an online bookstore's checkout: cart math, a payments module, an orders database
- **Unit tests** — each checks one function alone (`tax(cart)`): ~10 ms each, and a failure names the exact spot
- **Integration tests** — run the checkout against a real test database: ~3 s each, catch wiring and contract bugs
- **End-to-end tests** — a script buys a book through the real UI: ~2 min each, highest fidelity, culprit anywhere
- **The shape** — the team keeps 1,000 unit, 100 integration, 10 E2E: wide at the fast bottom, narrow at the slow top

*Example (italic):* A rounding bug in `tax()` fails one unit test that names the function; the same bug fails four E2E tests that only say "order total wrong".

**Key point:** That shape is the testing pyramid, published by Mike Cohn in *Succeeding with Agile* (2009): many isolated unit tests at the base, fewer integration tests, fewest end-to-end tests at the top.

### Visualization (canvas `c1`, 720×300)

Pyramid diagram: three stacked trapezoid layers with test counts and per-test times, plus the fidelity/speed trade-off on the margins.

- **Title (bold 15px, `#1a5276`, top center):** "The Pyramid: Many Fast Tests Below, Few Slow Tests Above".
- **Unit layer (bottom):** trapezoid with corners `(120,255) (600,255) (520,185) (200,185)`, fill `rgba(0,131,0,0.20)`, 2px `#008300` edge; bold 13px `#008300` centered label "UNIT — 1,000 tests" at (360, 218), 12px `#444` "one function in isolation, ~10 ms each" at (360, 240).
- **Integration layer (middle):** trapezoid `(200,180) (520,180) (440,115) (280,115)`, fill `rgba(42,120,214,0.20)`, 2px `#2a78d6` edge; bold 13px `#2a78d6` "INTEGRATION — 100 tests" at (360, 143), 12px `#444` "service + real database, ~3 s each" at (360, 165).
- **E2E layer (top):** trapezoid `(280,110) (440,110) (395,55) (325,55)`, fill `rgba(217,89,38,0.20)`, 2px `#d95926` edge; bold 12px `#d95926` "E2E — 10" centered at (360, 88); side callout 12px `#444` "whole system through the UI, ~2 min each" left-aligned at (455, 75) with a thin `#6b7280` connector line to the layer's right edge.
- **Left margin (bold 12px `#6b7280`, left-aligned x=20):** "↑ fidelity, cost per test" at y=60 and "↓ speed, count" at y=268.
- **Caption (12px `#444`, bottom right):** "counts and times illustrative".

## Adding Up the Suite: 1,000 + 100 + 10

**Tags:** `worked example` (blue), `feedback speed` (green)

- **Unit tier** — 1,000 tests × 10 ms = 10 s: fast enough to run on every file save
- **Integration tier** — 100 tests × 3 s = 300 s = 5 min: run before every merge
- **E2E tier** — 10 tests × 2 min = 20 min: run on the main branch a few times a day
- **Hand-check** — whole suite ≈ 10 s + 5 min + 20 min ≈ 25 min, but a developer's inner loop is just the 10 s
- **Test doubles** — unit tests swap the real payment gateway for a stand-in; mock, stub, and fake are their names

*Example (italic):* Flip the ratio — 10 unit and 1,000 E2E tests — and the suite takes 1,000 × 2 min ≈ 33 hours, so nobody runs it before pushing.

**Key point:** The pyramid's shape is really about cost and feedback speed — a suite developers can run in seconds gets run constantly; a slow one gets skipped until release day.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: total runtime of each tier, with log-feel schematic bar widths so the 10-second tier stays visible next to the 20-minute one.

- **Title (bold 15px, `#1a5276`, top center):** "What Each Tier Costs to Run End to End".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; widths are hardcoded log-feel pixels, not a linear scale.
- **Rows (bars 18px tall, top edges at y = 80, 145, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "Unit — 1,000 × 10 ms = 10 s": green `#008300` bar width 60, 11px `#008300` label "10 s" at the bar end
  - "Integration — 100 × 3 s = 5 min": blue `#2a78d6` bar width 260, 11px `#2a78d6` label "5 min" at the bar end
  - "E2E — 10 × 2 min = 20 min": orange `#d95926` bar width 440, 11px `#d95926` label "20 min" at the bar end
- **Annotation (bold 13px green `#008300`, right-aligned near x=660, y=55):** "the 10-second tier is the one that runs on every save".
- **Caption (12px `#444`, bottom right):** "times illustrative; bar widths schematic (log-feel)".

## The Ice-Cream Cone and the Flaky-Test Tax

**Tags:** `why it matters` (blue), `anti-pattern` (red), `flakiness` (orange)

- **The cone** — the named anti-pattern: few unit tests, a mountain of E2E and manual checks on top
- **The cost** — every one-line change waits hours for the E2E mountain plus a manual pass to validate it
- **Flakiness** — E2E tests fail randomly from timing waits, shared environments, and stale test data
- **The math** — at 98% per-test reliability, 200 E2E tests go all-green on only 2% of correct builds
- **Retry culture** — red builds stop meaning "bug"; the team learns to click retry and trusts nothing

*Example (italic):* With 10 E2E tests at 98% each, a correct build shows all-green 82% of the time; with 200 of them, only 2% of the time.

**Key point:** E2E flakiness compounds multiplicatively across tests — the pyramid caps the top layer precisely so that a red build still carries information.

### Visualization (canvas `c3`, 720×300)

Line chart: probability that a fully correct build shows an all-green suite, as the number of E2E tests grows, at a fixed 98% per-test pass rate.

- **Title (bold 15px, `#1a5276`, top center):** "The Flakiness Tax Compounds: Chance a Correct Build Goes All-Green".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = number of E2E tests 0 to 200, 12px `#444` tick labels every 50; y = chance all pass 0 to 100%, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels.
- **Curve:** red `#e74c3c` 3px line through test counts `[0, 10, 25, 50, 100, 150, 200]`, pass chances `[100, 81.7, 60.3, 36.4, 13.3, 4.8, 1.8]` (0.98^n).
- **Pyramid marker:** green `#008300` 5px dot at (10, 81.7) with bold 12px `#008300` label "pyramid: 10 tests → 82% green" to its upper right.
- **Cone marker:** red `#e74c3c` 5px dot at (200, 1.8) with bold 12px `#e74c3c` label "cone: 200 tests → 2% green" above it, right-aligned.
- **Annotation (bold 13px violet `#4a3aa7`, centered near plot x≈100 tests, y=95):** "a red build stops meaning anything — the team clicks retry".
- **Caption (12px `#444`, bottom right):** "assumes each E2E test passes 98% of the time when the code is correct; curve is 0.98^n exact".

## A Heuristic, Not a Law

**Tags:** `common mistake` (red), `testing trophy` (orange)

- **Not a law** — 1,000/100/10 is an illustration, not a commandment; the shape is the message
- **The trophy** — a published counterpoint argues glue-heavy services deserve an integration-heavy bulge
- **When it's right** — a service that mostly wires libraries together has little pure logic to unit-test
- **The invariants** — fast cheap checks at the bottom, few high-fidelity checks at the top, all automated
- **The real mistake** — copying the ratios while letting the suite become slow, flaky, or manual

*Example (italic):* A 200-line API gateway with almost no business logic may honestly need 30 integration tests and only 10 unit tests — a trophy, not a pyramid.

**Common mistake:** Treating the pyramid's ratios as a law to enforce. It is a heuristic about cost and feedback speed; an integration-heavy "testing trophy" suite obeys the same invariants with a different mix.

### Visualization (canvas `c4`, 720×300)

Two shapes side by side: the classic pyramid and the integration-heavy trophy, annotated with the invariant both satisfy.

- **Title (bold 15px, `#1a5276`, top center):** "Different Mixes, Same Invariants".
- **Left shape (pyramid, centered on x=190):** unit trapezoid `(80,250) (300,250) (260,190) (120,190)` fill `rgba(0,131,0,0.20)` edge 2px `#008300` with 11px `#008300` centered label "unit"; integration trapezoid `(120,185) (260,185) (225,125) (155,125)` fill `rgba(42,120,214,0.20)` edge `#2a78d6` label 11px "integration"; E2E trapezoid `(155,120) (225,120) (202,70) (178,70)` fill `rgba(217,89,38,0.20)` edge `#d95926` with 11px `#d95926` label "E2E" beside it at (232, 95); bold 13px `#1a5276` caption "pyramid" centered at (190, 275).
- **Right shape (trophy, centered on x=530), stacked rectangles with the same fills/edges by layer type:**
  - static-checks base: rect x=440 y=236 w=180 h=14, fill `rgba(107,114,128,0.25)`, 11px `#6b7280` label "static checks / lint" to the right, right-aligned at (w-12, 246) — mention-level footer layer
  - unit block: rect x=470 y=186 w=120 h=46, green fill, 11px centered "unit"
  - integration block (the widest — the bulge): rect x=430 y=126 w=200 h=56, blue fill, 11px centered "integration"
  - E2E block: rect x=500 y=76 w=60 h=46, orange fill, 11px `#d95926` label "E2E" beside it at (568, 100)
  - bold 13px `#1a5276` caption "trophy" centered at (530, 275).
- **Annotation (bold 12px violet `#4a3aa7`, centered at (360, 42)):** "both: fast checks at the bottom, few slow checks at the top, everything automated".
- **Caption (12px `#444`, bottom right):** "shapes schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); test counts (1,000 / 100 / 10) and per-test times (10 ms / 3 s / 2 min) are invented and labeled illustrative; the flakiness curve values (81.7 / 60.3 / 36.4 / 13.3 / 4.8 / 1.8) are exact 0.98^n percentages for n = 10 / 25 / 50 / 100 / 150 / 200; c2 bar widths and c4 shapes are schematic. Credit Mike Cohn (*Succeeding with Agile*, 2009) in the first key-point.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
