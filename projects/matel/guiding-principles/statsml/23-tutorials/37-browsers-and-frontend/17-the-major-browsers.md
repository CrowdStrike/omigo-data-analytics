# The Major Browsers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Major Browsers

**Subtitle:** Chrome, Safari, Edge, Firefox — the four names in every analytics report, who builds each one, and what engine actually draws the page

## Four Names on the Analytics Report

**Tags:** `core idea` (blue), `web analytics` (green), `browsers` (orange)

- **The report** — a coffee shop's online-ordering site logs 12,000 visits this month, split by browser
- **Chrome** — 7,800 visits come from Google's browser, the most-used one on the web
- **Safari** — 2,160 visits come from Apple's browser, the default on iPhones, iPads, and Macs
- **Edge** — 600 visits come from Microsoft's browser, the one preinstalled on Windows
- **Firefox** — 360 visits come from Mozilla's browser, the major one not run by a tech giant
- **The rest** — 1,080 visits come from smaller browsers like Opera, Brave, and Samsung Internet

*Example (italic):* When the order page breaks "only for some customers", the browser column is the first place the coffee shop looks — 65% of its visitors are on Chrome.

**Key point:** A browser is the program that fetches a page and draws it on screen — the four big names are just four competing programs doing that same job, each made by a different organization.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart of the coffee shop's 12,000 monthly visits split by browser, one bar per browser plus an "others" bar.

- **Title (bold 15px, `#1a5276`, top center):** "One Month of Orders Site Visits, by Browser".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = visits 0 to 8,000, gridlines `#e5e9ef` at 2,000/4,000/6,000; 12px `#444` y tick labels.
- **Bars (left to right, 80px wide, centered at x = 130, 240, 350, 460, 570):** labels `["Chrome", "Safari", "Edge", "Firefox", "Others"]`, visits `[7800, 2160, 600, 360, 1080]`, fills blue `#2a78d6`, aqua `#199e70`, violet `#4a3aa7`, orange `#d95926`, mute `#6b7280`.
- **Value labels:** bold 12px `#2c3e50` visit counts above each bar; 12px `#444` browser names below the baseline.
- **Annotation (bold 13px blue `#2a78d6`, near x=300, y=70):** "Chrome alone is 65% of all visits".
- **Caption (12px `#444`, bottom right):** "visit counts illustrative".

## Who Makes What: Maker, Engine, Reach, Money

**Tags:** `worked example` (blue), `engines` (green), `business model` (orange)

- **Chrome** — made by Google, renders with the Blink engine, ships on Windows, Mac, Linux, and phones
- **Safari** — made by Apple, renders with WebKit, ships only on Apple devices (Mac, iPhone, iPad)
- **Edge** — made by Microsoft, rebuilt on Chromium in 2020, so it also renders with Blink
- **Firefox** — made by the Mozilla Foundation's corporation, renders with its own Gecko engine
- **The money** — Google and Microsoft fund theirs from their ad and search businesses; Apple from device sales; Mozilla mostly from search-engine default deals
- **The engine** — the engine is the part that turns HTML and CSS into pixels; browsers are wrappers around one

*Example (italic):* Edge looks nothing like Chrome on screen, yet since 2020 both hand the page to the same Blink engine — a layout bug in one usually appears in the other.

**Key point:** Each browser is a maker + an engine + a distribution channel + a funding model — and two of the four big names (Chrome and Edge) now share the same engine.

### Visualization (canvas `c2`, 720×300)

Fact-matrix diagram: four browser rows crossed with four attribute columns, drawn as a grid of rounded boxes.

- **Title (bold 15px, `#1a5276`, top center):** "The Four Majors at a Glance".
- **Column headers (bold 12px `#1a5276`, y=58, at x = 175, 305, 445, 605):** "Maker", "Engine", "Runs on", "Paid by".
- **Row labels (bold 13px, x=20, rows at y = 95, 145, 195, 245):** "Chrome" in blue `#2a78d6`, "Safari" in aqua `#199e70`, "Edge" in violet `#4a3aa7`, "Firefox" in orange `#d95926`.
- **Cell text (11–12px `#2c3e50`, centered under each header), rows top to bottom:**
  - Chrome: `["Google", "Blink", "Win/Mac/Linux/phones", "Google ads"]`
  - Safari: `["Apple", "WebKit", "Apple devices only", "device sales"]`
  - Edge: `["Microsoft", "Blink (Chromium)", "Windows default + more", "Microsoft"]`
  - Firefox: `["Mozilla", "Gecko", "all major platforms", "search deals"]`
- **Engine-column boxes:** rounded 8px boxes, 110px wide, 30px tall; Blink cells filled `rgba(42,120,214,0.15)` with `#2a78d6` border (Chrome and Edge rows), WebKit cell `rgba(25,158,112,0.15)` with `#199e70` border, Gecko cell `rgba(217,89,38,0.15)` with `#d95926` border; other columns plain text on white with a 1px `#e5e9ef` row separator line.
- **Annotation (bold 12px violet `#4a3aa7`, right side near y=120, with a thin bracket joining the two Blink boxes):** "same engine, two browsers".
- **Caption (12px `#444`, bottom right):** "makers, engines, platforms: documented facts".

## Reading User-Agent Data and Picking What to Test

**Tags:** `where it's used` (blue), `testing` (green)

- **The user-agent** — every visit carries a user-agent string; analytics tools parse it into the browser column
- **The question** — the coffee shop can hand-test the checkout on only a few browsers; which ones?
- **Coverage math** — testing Chrome alone covers 65% of visits; adding Safari lifts coverage to 83%
- **Diminishing returns** — adding Edge reaches 88%, adding Firefox 91%; the rest is a long tail
- **The device split** — Safari visits are overwhelmingly phones and tablets, so "test Safari" also means "test mobile"
- **The trap** — testing only on the developer's own browser silently ignores every other engine

*Example (italic):* The coffee shop tests checkout on Chrome and Safari before each release — two browsers, two engines, 83% of its visitors covered.

**Key point:** The browser breakdown turns "works on my machine" into a coverage number — test in order of your own traffic, not in order of personal habit.

### Visualization (canvas `c3`, 720×300)

Cumulative coverage step chart: percent of the coffee shop's visitors covered as each browser is added to the test list.

- **Title (bold 15px, `#1a5276`, top center):** "Visitors Covered as Browsers Are Added to the Test List".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = coverage 0 to 100%, gridlines `#e5e9ef` at 25/50/75, 12px `#444` tick labels; x = four steps labeled `["+ Chrome", "+ Safari", "+ Edge", "+ Firefox"]` at x = 135, 275, 415, 555 (12px `#444`).
- **Coverage bars:** 90px-wide bars at each step, heights for coverage `[65, 83, 88, 91]`, fill `rgba(42,120,214,0.30)` with a bold 2px green `#008300` step line running across the bar tops.
- **Increment labels:** bold 12px `#008300` above each bar: "65%", "+18 → 83%", "+5 → 88%", "+3 → 91%".
- **Threshold line:** dashed `#6b7280` (dash 4/3) horizontal line at 90%, 12px `#6b7280` label "90% target" at its left end.
- **Annotation (bold 13px green `#008300`, near x=340, y=70):** "two browsers already cover 83% of visitors".
- **Caption (12px `#444`, bottom right):** "coverage percentages illustrative".

## Market Share Is Not Engine Share

**Tags:** `common mistake` (red), `engines` (orange), `webviews` (blue)

- **The confusion** — four browser names suggest four rendering worlds; the engine count is really three
- **Regrouped** — by engine, the shop's traffic is Blink 77% (Chrome, Edge, Opera, Brave...), WebKit 19%, Gecko 3%
- **The iOS rule** — Apple has long required iOS browsers to use WebKit, so "Chrome on iPhone" renders like Safari
- **In-app webviews** — links tapped inside social or chat apps open in an embedded webview, not the full browser
- **The mislabel** — analytics often lumps webview traffic under Chrome or Safari, inflating those rows
- **The payoff** — a rendering bug is an engine question; a feature or settings question is a browser question

*Example (italic):* The shop sees a layout bug "in Safari and in Chrome on iPhone" — that is one WebKit bug, not two separate browser bugs.

**Common mistake:** Reading the browser column as the engine column. Chrome's 65% share does not mean 65% Blink and 35% something else — regrouped by engine, Blink is even bigger, WebKit owns nearly all Apple traffic, and only Gecko stands apart.

### Visualization (canvas `c4`, 720×300)

Two stacked horizontal bars over a shared 0–100% axis: the same 12,000 visits split by browser (top) and regrouped by engine (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Same Visits, Two Groupings: Browser Share vs Engine Share".
- **Axis:** bars start at x=140, full width 520 = 100%; 12px `#444` "0%", "50%", "100%" ticks at x = 140, 400, 660 along y=260.
- **Top bar (y=95, 44px tall), left label bold 12px `#2c3e50` "by browser":** segments in percent `[65, 18, 5, 3, 9]` for `["Chrome", "Safari", "Edge", "Firefox", "others"]`, fills blue `#2a78d6`, aqua `#199e70`, violet `#4a3aa7`, orange `#d95926`, mute `#6b7280`; bold 11px white segment labels inside wide segments, 11px `#444` outside narrow ones.
- **Bottom bar (y=185, 44px tall), left label "by engine":** segments `[77, 19, 3, 1]` for `["Blink", "WebKit", "Gecko", "other"]`, fills blue `#2a78d6`, aqua `#199e70`, orange `#d95926`, mute `#6b7280`.
- **Flow hint:** thin dashed `#6b7280` connector lines from the Chrome, Edge, and part of the others segments funneling into the Blink segment.
- **Annotation (bold 13px magenta `#d55181`, near x=380, y=52):** "three engines render nearly all of the web".
- **Caption (12px `#444`, bottom right):** "share percentages approximate and illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the coffee shop's visit counts `[7800, 2160, 600, 360, 1080]` (total 12,000), the coverage steps `[65, 83, 88, 91]`, and the share splits `[65, 18, 5, 3, 9]` by browser and `[77, 19, 3, 1]` by engine are invented and labeled illustrative/approximate; makers, engines, platform reach, funding models, Edge's 2020 Chromium rebuild, and the iOS WebKit requirement are documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
