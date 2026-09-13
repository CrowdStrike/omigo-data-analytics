# Null Semantics

**Page type:** detail page (tutorial page: card-sections, each with a two-column layout table — text left 45% with tag pills / bullets / example / key-point, canvas right 55%; section 2 uses a 3-column layout with two canvases)
**HTML title tag:** Null Semantics

**Subtitle:** A NULL in the discount column can mean "never offered", "offered and declined", or "we lost the answer" — three facts, one symbol, and your averages quietly pick one for you

## One Blank Cell, Three Different Stories

Tags: `core idea` (blue), `running example` (green)

- **Anna** — was never shown a coupon; her discount cell is NULL
- **Ben** — saw the coupon and clicked "no thanks"; his cell is also NULL
- **Cara** — clicked something, but a bug lost the answer; NULL again
- **The symbol** — the table stores one identical mark for all three histories
- **The definition** — NULL means "no value here", and stays silent about why

*Example:* Three customers with three different coupon experiences produce three identical blank cells.

**Key point:** NULL is not a value — it is the absence of one. The *reason* for the absence is real information your table may not be keeping.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three customer timelines converging into one NULL cell.

- **Title (bold 16px, `#1a5276`, top center):** "Three Histories Collapse Into the Same Symbol".
- **Lanes** (name bold 13px in lane color at left; step boxes 150×32, fill `#f8f9fa`, 1.5px stroke in lane color, 12px `#2c3e50` text, connected by small colored arrows):
  - y=80, "Anna", blue `#2a78d6`: "visits shop" → "no coupon shown"
  - y=150, "Ben", aqua `#199e70`: "coupon shown" → "clicks \"no thanks\""
  - y=220, "Cara", violet `#4a3aa7`: "coupon shown" → "answer lost in a bug"
- **Convergence:** a colored arrow from each lane's end to a single cell at x=560: 110×44 box, fill `#fdecea`, 2px red (`#e74c3c`) stroke, bold 15px monospace red text "NULL", with 12px `#6b7280` label "discount" above it.
- **Bottom annotation (bold 13px `#e74c3c`, centered):** "three different facts, one symbol — the difference lives only in the app".

## Averaging 10 Orders by Hand

Tags: `worked example` (green), `common mistake` (red)

- **The table** — 10 orders; discount % reads 10, 20, NULL, 0, NULL, 15, NULL, 5, NULL, 10
- **AVG skips** — AVG(discount) = (10+20+0+15+5+10) / 6 = 10
- **COUNT differs** — COUNT(*) = 10 rows, but COUNT(discount) = 6
- **If NULL means zero** — the honest average is 60 / 10 = 6
- **Two answers** — 10 or 6, depending on what the blank means

*Example:* The dashboard says "average discount 10%"; if unoffered means zero, the honest number is 6%.

**Key point:** AVG divides by the filled cells only, while COUNT(*) counts every row. Nobody chose that split — it is the SQL default.

### Visualization (canvas `c2a`, 420×300)

Rendered data table of the 10 orders with NULL cells shaded.

- **Title (bold 15px, `#1a5276`, top center):** "The 10 Orders".
- **Table:** two columns headed "order_id" and "discount %" (bold 12px `#1a5276`); 10 rows, 120px columns, 22px row height, zebra striping `#f8f9fa`/white with `#e5e9ef` cell borders; order_id runs 101–110 (12px monospace); discount values `10, 20, NULL, 0, NULL, 15, NULL, 5, NULL, 10` — NULL cells have fill `#fdecea` and bold red (`#e74c3c`) monospace "NULL".
- **Bottom annotation (bold 12px `#e74c3c`, centered):** "4 of 10 cells are NULL".

### Visualization (canvas `c2b`, 400×300)

Grouped two-panel bar chart: two averages and two counts.

- **Title (bold 15px, `#1a5276`, top center):** "Same Column, Two Answers".
- **Panel 1** (heading bold 12px `#1a5276`: "average discount %"): bars "AVG( ) skips NULLs" = 10 (blue `#2a78d6`) and "NULL treated as 0" = 6 (orange `#d95926`); scale max 12.
- **Panel 2** (heading "rows counted"): bars "COUNT(*)" = 10 (blue) and "COUNT(discount)" = 6 (orange); scale max 12.
- **Bar style:** 52px wide, fill at 50% alpha with 1.5px stroke in the bar color; value bold 13px in bar color above each bar; label 12px `#2c3e50` under each bar; baseline in `#6b7280`.
- **Annotations:** bold 13px `#e74c3c` centered near bottom: "AVG reports 10; the zero-means-no-discount view says 6"; italic 11px `#6b7280`: "sum of filled cells = 60 either way; only the divisor changes".

## Where the Blanks Bite a Data Scientist

Tags: `where it's used` (blue), `common mistake` (red)

- **Filters drop them** — WHERE discount < 15 keeps 4 rows; NULLs fail every comparison
- **Opposites don't add up** — discount < 15 gives 4, NOT discount < 15 gives 2; 4 + 2 ≠ 10
- **Joins lose them** — NULL never equals NULL, so NULL keys silently miss their match
- **Imputation guesses** — filling with 0 says "declined"; filling with the mean says "typical"
- **Models inherit it** — a feature built on the wrong meaning is wrong on every prediction

*Example:* A churn model filled NULL discounts with the mean, treating "never offered" customers as average deal-getters.

**Key point:** Every NULL you fill is a statement about the world. Pick the statement on purpose, not by library default.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: two opposite filters on 10 rows return only 6.

- **Title (bold 16px, `#1a5276`, top center):** "Two Opposite Filters on 10 Rows Return Only 6".
- **Bars** (horizontal, 28px tall, starting at x=250, scale 10 rows = 380px; fill at 50% alpha + 1.5px stroke; bold monospace label right-aligned left of bar; "N rows" bold 13px in bar color after bar; gray note in parentheses after that):
  - "WHERE discount < 15" = 4 rows, blue `#2a78d6`, note "(rows 0, 5, 10, 10)"
  - "WHERE NOT discount < 15" = 2 rows, aqua `#199e70`, note "(rows 15, 20)"
  - "returned by neither" = 4 rows, red `#e74c3c`, note "(the 4 NULL rows)"
- **Reference line:** vertical dashed `#6b7280` line at the 10-row mark labeled "all 10 rows" (12px).
- **Annotations:** bold 14px red centered at y=250: "4 + 2 = 6, not 10 — a NULL answers \"unknown\" to every question, so both filters drop it"; italic 12px `#6b7280` at y=275: "same 10-order table as above: 10, 20, NULL, 0, NULL, 15, NULL, 5, NULL, 10".

## Encode the Meaning, Not Just the Blank

Tags: `rule of thumb` (orange), `fix` (green)

- **Split the column** — keep discount_value, add discount_status with three real values
- **Anna** — status = not_offered, value = 0
- **Ben** — status = declined, value = 0
- **Cara** — status = unknown, value = NULL — the only honest NULL left
- **The payoff** — averages, filters, and models can now choose which rows they mean

*Example:* After the split, "average discount among customers actually offered one" becomes a one-line query.

**Key point:** Reserve NULL for genuinely unknown. Everything you do know — even "nothing happened" — deserves a real value.

### Visualization (canvas `c4`, 720×300)

Before/after table diagram: one ambiguous column becomes status + value.

- **Title (bold 16px, `#1a5276`, top center):** "Before: One Ambiguous Blank — After: The Meaning Is a Column".
- **BEFORE table** (label "BEFORE" bold 12px `#6b7280`; at x=40): columns "customer" (70px) and "discount" (90px), rows 30px for Anna, Ben, Cara; customer cells `#f8f9fa`, discount cells `#fdecea` with bold red monospace "NULL" in all three; caption bold 12px red below: "indistinguishable".
- **Arrow:** thick `#1a5276` arrow from the BEFORE to the AFTER table.
- **AFTER table** (label "AFTER"; at x=320): columns "customer" (70px), "discount_status" (140px), "discount_value" (110px); rows: Anna → `not_offered` (blue `#2a78d6`) / `0`; Ben → `declined` (orange `#d95926`) / `0`; Cara → `unknown` (violet `#4a3aa7`) / `NULL` (bold red on `#fdecea`). Status/value text bold 12px monospace. Caption bold 12px green (`#008300`) below: "the three stories are now three queryable values".
- **Annotations:** bold 13px `#1a5276` centered: "only Cara keeps a NULL — the one row where the truth is genuinely unknown"; italic 12px `#6b7280`: "AVG(discount_value) WHERE discount_status = 'declined' — no guessing required".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + a layout table. Sections 1, 3, 4 use `table.layout` (`td.text-col` 45% / `td.viz-col` 55%); section 2 uses `table.layout3` (text 38%, two viz cells 31% each holding canvases `c2a` 420×300 and `c2b` 400×300). Text cell order: `.tags` pill row, `<ul>` bullets (each starting with `<b>bold term</b>` in `#1a5276`), italic `.example`, `.key-point` callout.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = bg `rgba(26,82,118,0.12)` / `#1a5276`, green = `rgba(39,174,96,0.15)` / `#27ae60`, red = `rgba(231,76,60,0.12)` / `#e74c3c`, orange = `rgba(230,126,34,0.15)` / `#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic width/height read from attributes; shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Monospace chart text uses `ui-monospace, Menlo, monospace`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`, red `#e74c3c`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
