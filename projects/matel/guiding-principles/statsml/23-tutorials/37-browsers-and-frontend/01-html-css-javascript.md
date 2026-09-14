# HTML, CSS, JavaScript

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** HTML, CSS, JavaScript

**Subtitle:** Every web page is three layers doing three jobs — HTML holds the structure, CSS paints the appearance, JavaScript adds the behavior

## The Coffee Shop Menu, Built Three Times

**Tags:** `core idea` (blue), `three layers` (green), `one page` (orange)

- **The page** — a coffee shop wants its menu online: Espresso $3, Latte $4, Mocha $5
- **HTML first** — plain tags mark what things are: a heading, a list of three drinks, a button
- **Bare result** — the browser shows black text on white; readable, correct, and ugly
- **CSS second** — a stylesheet says how it looks: brown heading, card borders, rounded button
- **JavaScript third** — a script says what it does: clicking a drink adds it to a running order
- **Same content** — all three versions list the identical three drinks; only the layer changes

*Example (italic):* The owner ships the HTML-only menu on day one — customers can already read all three prices before any styling exists.

**Key point:** HTML, CSS, and JavaScript are not three competing tools — they are three layers of one page: structure, appearance, behavior, each in its own file.

### Visualization (canvas `c1`, 720×300)

Three side-by-side page mockups of the same menu: HTML only (plain), HTML+CSS (styled), HTML+CSS+JS (interactive), drawn as rounded panels with mini wireframe content.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Menu Page: Structure, Then Looks, Then Behavior".
- **Panels:** three rounded boxes (8px radius, 1.5px `#e5e9ef` border) at x = `[40, 260, 480]`, each 200px wide, y=60 to 250; panel captions bold 12px centered below each box at y=270: "HTML only", "+ CSS", "+ JavaScript" in `#2a78d6` / `#008300` / `#d95926`.
- **Panel 1 (plain):** 13px `#2c3e50` monospace-feel text lines at x=55: "Café Menu" (bold), then "Espresso $3", "Latte $4", "Mocha $5" spaced 26px apart, and an unstyled rect button outline labeled "Order" (11px).
- **Panel 2 (styled):** same three drink lines but heading bold 13px `#1a5276`, each drink on a light card strip fill `rgba(42,120,214,0.10)` 24px tall, button now a filled green `#008300` rounded pill labeled "Order" in white 11px.
- **Panel 3 (interactive):** same styled layout, plus a violet `#4a3aa7` badge at top right of the panel reading "order: 2 items" (11px white), and a small cursor arrow near the Latte row with 11px `#d95926` label "click adds $4".
- **Annotation (bold 13px `#4a3aa7`, centered near y=45):** "same drinks in all three — only the layer changes".
- **Caption (12px `#444`, bottom right):** "menu prices illustrative".

## One Button Through All Three Layers

**Tags:** `worked example` (blue), `trace it by hand` (green)

- **The element** — one line of HTML: `<button>Order Latte — $4</button>`; structure says a button exists
- **The style** — one CSS rule: `button { background: green; border-radius: 8px }`; appearance only
- **The behavior** — one JS line: on click, `total = total + 4`; nothing about looks or markup
- **Hand-check** — click it 3 times: total goes 4, 8, 12; the HTML and CSS never change during clicks
- **Swap test** — change the CSS to blue and the button still adds $4; each layer edits independently

*Example (italic):* After 3 clicks the order total reads $12 (3 × $4) — the button's markup and green styling are byte-for-byte identical to before the first click.

**Key point:** Trace any element the same way — what it IS lives in HTML, how it LOOKS lives in CSS, what it DOES lives in JavaScript; a click changes state, never the other two layers.

### Visualization (canvas `c2`, 720×300)

Layer-trace diagram: three stacked code rows on the left feeding one rendered button on the right, with a click counter showing the running total.

- **Title (bold 15px, `#1a5276`, top center):** "One Button, Three Layers: Structure, Looks, Behavior".
- **Code rows (left, rounded boxes 300px wide, 36px tall, at x=30, y = 70 / 130 / 190):**
  - blue `#2a78d6` border, fill `rgba(42,120,214,0.10)`, 12px text: `<button>Order Latte — $4</button>`, bold 11px blue label "HTML — structure" above the box
  - green `#008300` border, fill `rgba(0,131,0,0.10)`, 12px text: `button { background:green; radius:8px }`, bold 11px green label "CSS — appearance"
  - orange `#d95926` border, fill `rgba(217,89,38,0.10)`, 12px text: `onclick: total = total + 4`, bold 11px orange label "JS — behavior"
- **Arrows:** three 2px `#6b7280` arrows from the right edge of each code box converging on the rendered button at x≈420.
- **Rendered button:** green `#008300` filled rounded pill at x=420, y=125, 150px wide, 40px tall, white bold 13px label "Order Latte — $4".
- **Click counter (right, x=600):** vertical mini bar chart of the running total after clicks `[1, 2, 3]`, totals `[4, 8, 12]`; bars 24px wide, violet `#4a3aa7`, 12px value labels "4" "8" "12" on top, 11px `#444` tick labels "1 click" "2" "3" below, baseline y=245.
- **Annotation (bold 12px violet `#4a3aa7`, above the bars near y=70):** "3 clicks → $12; HTML and CSS untouched".
- **Caption (12px `#444`, bottom right):** "totals illustrative".

## Every Dashboard Is These Three Files

**Tags:** `where it's used` (blue), `separation of concerns` (green), `data science` (orange)

- **Everywhere** — notebooks, BI dashboards, plotly charts, and internal admin pages all render this way
- **Reading tools** — inspect any dashboard and you find HTML structure, CSS themes, JS chart code
- **The payoff** — separation of concerns: restyle the whole site by editing one CSS file
- **The counterfactual** — with styles baked into every page, a rebrand means editing all 40 pages
- **Debug skill** — knowing which layer owns a problem (missing row vs wrong color vs dead button)

*Example (italic):* The coffee chain rebrands from green to brown: one CSS edit updates all 40 store pages, versus 40 separate edits if colors were written into each page's HTML.

**Key point:** Every browser-based data tool a data scientist touches is these three layers; separating them means one change in one file instead of the same change repeated everywhere.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: number of files to edit for one rebrand, styles-in-every-page vs one shared stylesheet, plus a strip of tools that are all built from the three layers.

- **Title (bold 15px, `#1a5276`, top center):** "Rebrand 40 Pages: Styles Baked In vs One Shared CSS File".
- **Tool strip (top, y=60):** four rounded pills side by side starting x=60, spaced 160px, 11px text, fill `rgba(42,120,214,0.12)`, blue `#2a78d6` border: "notebook", "BI dashboard", "plotly chart", "admin page"; 11px `#6b7280` label above at y=44: "all rendered from HTML + CSS + JS".
- **Bars (left labels 12px `#444` at x=20, bars start x=230, max width 440, 22px tall):**
  - y=140: "styles in every page" — orange `#d95926` bar width 440 (40 files), bold 12px orange value label "40 files to edit" at bar end
  - y=200: "one shared stylesheet" — green `#008300` bar width 11 (1 file), bold 12px green value label "1 file to edit" beside it
- **Gridlines:** vertical `#e5e9ef` at bar widths 110/220/330 with 11px `#6b7280` tick labels "10" "20" "30" at y=250.
- **Annotation (bold 13px green `#008300`, near x=278, y=235):** "separation of concerns: change looks without touching structure".
- **Caption (12px `#444`, bottom right):** "page counts illustrative".

## Writing Looks and Clicks into the Structure

**Tags:** `common mistake` (red), `mixed layers` (orange)

- **The tangle** — beginners write colors and click-handlers inline inside the HTML of every element
- **It works once** — the page renders fine, so nothing looks wrong until the first change request
- **The bill** — the button's green is pasted in 12 places; a color change means 12 edits, not 1
- **The misses** — in a hand-edit of 12 copies, a few are typically missed, leaving mismatched buttons
- **The fix** — move looks to one CSS rule and behavior to one JS function; HTML keeps only structure
- **Name trap** — JavaScript is not Java; the similar name is historical marketing, not a relationship

*Example (italic):* The shop's "make buttons brown" request touches 12 inline copies and misses 2 of them — the same edit against one CSS rule is a single line.

**Common mistake:** Treating HTML as the place where everything goes. Inline styles and inline handlers duplicate one decision across every element — the whole point of CSS and JS files is to state it once.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: changing the button color in a mixed-layer page (12 edits, 2 missed) vs a separated page (1 edit), shown as boxes flowing left to right.

- **Title (bold 15px, `#1a5276`, top center):** "Change the Button Color: Mixed Layers vs Separated Layers".
- **Row 1 (y=95), label 12px `#444` at x=20:** "mixed in HTML"; blue `#2a78d6` rounded box at x=150 labeled "green pasted in 12 places" (12px), 3px arrow to a red `#e74c3c` box at x=400 labeled "12 hand edits — 2 missed" with bold 12px red "✗ mismatched buttons" right-aligned at the canvas right edge.
- **Row 2 (y=205), label:** "separated"; blue box at x=150 labeled "one rule: button { green }", 3px arrow to a green `#008300` box at x=400 labeled "edit 1 line of CSS", then arrow to a green box at x=580 labeled "all 12 update" with bold 12px green "✓".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "state each decision once, in the layer that owns it".
- **Caption (12px `#444`, bottom right):** "edit counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); menu prices (Espresso $3, Latte $4, Mocha $5), click totals (`[4, 8, 12]` after clicks `[1, 2, 3]` at $4 each), the 40-pages-vs-1-file rebrand counts, and the 12-inline-copies / 2-missed edit counts are all invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
