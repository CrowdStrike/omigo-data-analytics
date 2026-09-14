# Decorators

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Decorators

**Subtitle:** A decorator is a function that takes your function and hands back a wrapped version — the same job runs inside, with a little extra work added before or after

## Gift-Wrapping the Order Function

**Tags:** `core idea` (blue), `wrapping` (green), `no edits inside` (orange)

- **The bakery** — a small bakery has one function, `take_order(cake)`, that has worked for years
- **New wish** — the owner wants a log line before and after every order, but won't touch the recipe
- **The wrap** — write `with_logging(fn)`: it returns a NEW function that logs, calls `fn`, logs again
- **The swap** — `take_order = with_logging(take_order)`; every caller now gets the logged version
- **The name** — a function that takes a function and returns a wrapped function is a decorator
- **Gift box** — like gift-wrapping: the present inside is unchanged, the wrapper adds the ribbon

*Example (italic):* Order #1 comes in at 10:04; the wrapper writes "start order #1", runs the untouched order code for 12 minutes, then writes "done order #1 — 12 min".

**Key point:** A decorator wraps extra work around an existing function without editing a single line inside it.

### Visualization (canvas `c1`, 720×300)

Onion diagram: one call arrow enters a large wrapper box, passes through a before-step, the untouched original function, an after-step, and exits as the result.

- **Title (bold 15px, `#1a5276`, top center):** "The Wrapper Around take_order — Same Recipe Inside".
- **Wrapper box:** rounded rect x=150, y=65, w=430, h=195, 3px blue `#2a78d6` border, fill `rgba(42,120,214,0.06)`; bold 13px blue label "with_logging wrapper" just inside its top edge.
- **Before step:** 12px `#6b7280` log-line text inside the wrapper at y=110: "[10:04] start order #1".
- **Inner box:** rounded rect x=250, y=130, w=230, h=60, fill `rgba(42,120,214,0.15)`, 2px `#1a5276` border; bold 13px `#1a5276` centered text "take_order(cake)" with 11px `#6b7280` line "unchanged original" beneath it inside the box.
- **After step:** 12px `#6b7280` log-line text inside the wrapper at y=235: "[10:16] done order #1 — 12 min".
- **Call arrow:** 3px green `#008300` arrow from x=35 to x=150 at y=160 with arrowhead; bold 12px green label above it: "call: 2-layer chocolate".
- **Return arrow:** 3px green arrow from x=580 to x=690 at y=160 with arrowhead; bold 12px green label above it: "receipt".
- **Annotation (bold 12px orange `#d95926`, near x=470, y=95):** "the recipe never changed — the wrapper does the logging".
- **Caption (12px `#444`, bottom right):** "illustrative — one logged order at the bakery".

## One Wrapper, By Hand: $30 Becomes $35

**Tags:** `worked example` (blue), `step by step` (green)

- **The original** — `price(layers) = 15 × layers` dollars, so a 2-layer cake is `price(2)` = $30
- **The wrapper** — `with_delivery(fn)` returns a new function: run `fn(layers)`, then add $5
- **The wrap** — `wrapped = with_delivery(price)`; nothing runs yet, we only built the new function
- **The call** — `wrapped(2)`: the wrapper calls `price(2)` = $30 inside, adds $5, returns $35
- **Untouched** — call `price(2)` directly and it is still $30; the original was never edited

*Example (italic):* Trace it on paper: wrapped(2) → price(2) = 15 × 2 = 30 → 30 + 5 = 35 — two steps, no mystery.

**Key point:** wrapped(2) = price(2) + 5 = 30 + 5 = 35 — the wrapper is just a function call with extra work bolted on.

### Visualization (canvas `c2`, 720×300)

Left-to-right flow diagram: the input pill enters the wrapper box, the inner original computes $30, the wrapper adds $5, and $35 exits on the right.

- **Title (bold 15px, `#1a5276`, top center):** "wrapped(2): the Call Travels Through the Wrapper".
- **Input pill:** rounded rect x=30, y=140, w=120, h=44, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; bold 13px `#1a5276` centered text "layers = 2".
- **Wrapper box:** rounded rect x=205, y=70, w=330, h=185, 3px green `#008300` border, fill `rgba(0,131,0,0.05)`; bold 13px green label "with_delivery wrapper" just inside its top edge.
- **Inner box:** rounded rect x=245, y=115, w=250, h=56, fill `rgba(42,120,214,0.15)`, 2px `#1a5276` border; bold 13px `#1a5276` centered text "price(2) = 15 × 2 = $30".
- **Wrapper step:** bold 13px green text centered at x=370, y=215: "then add $5 delivery → $35".
- **Output pill:** rounded rect x=590, y=140, w=100, h=44, fill `rgba(0,131,0,0.15)`, 2px green border; bold 14px green centered text "$35".
- **Arrows:** 3px `#1a5276` arrows with arrowheads: x=150→205 at y=162 (into wrapper) and x=535→590 at y=162 (out to result).
- **Annotation (bold 12px orange `#d95926`, near x=370, y=280, centered):** "$30 inside, $35 outside — the original still answers $30 on its own".
- **Caption (12px `#444`, bottom right):** "illustrative prices".

## Where the @ Sign Earns Its Keep

**Tags:** `where it's used` (blue), `caching` (green), `one place` (orange)

- **The @ sugar** — writing `@with_delivery` above `def price` is exactly `price = with_delivery(price)`
- **@timed** — a wrapper that stopwatch-times any function; paste it above 20 functions, write it once
- **@cache** — remembers past answers: a model-scoring call takes 800 ms fresh, 1 ms from the cache
- **@retry** — wraps a flaky API call so it quietly tries again before giving up
- **@app.route** — web frameworks decorate a plain function to register it as a page handler
- **One place** — the timing/caching/retry code lives in one wrapper, not copy-pasted everywhere

*Example (italic):* Score the same customer five times: the first call computes for 800 ms, the next four hit the cache and return in 1 ms each.

**Key point:** Decorators put cross-cutting chores — timing, caching, retries, registration — in one wrapper instead of pasted into every function.

### Visualization (canvas `c3`, 720×300)

Bar chart: five identical calls to an @cache-wrapped scoring function; call 1 is a tall computed bar, calls 2–5 are near-zero cache hits.

- **Title (bold 15px, `#1a5276`, top center):** "@cache: Five Identical Calls to score(customer)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = response time 0 to 900 ms with light `#e5e9ef` gridlines and 12px `#444` labels at 0, 300, 600, 900; x = five bars labeled "call 1" ... "call 5" (12px `#444`, centered below each bar).
- **Bars:** width 70, evenly spaced starting x=105; times = `[800, 1, 1, 1, 1]` ms; call 1 fill orange `#d95926` height 169px; calls 2–5 fill green `#008300` drawn 3px tall so they stay visible.
- **Bar labels:** bold 12px above each bar in the bar's color: "800 ms", then "1 ms" over each green bar.
- **Annotation (bold 13px green `#008300`, near x=330, y=110):** two lines: "wrapper returns the saved answer —" / "computed once, reused four times".
- **Caption (12px `#444`, bottom right):** "illustrative timings".

## Stacking Wrappers: Order Changes the Bill

**Tags:** `common mistake` (red), `stacking order` (orange)

- **Two wrappers** — `with_discount` (×0.9, ten percent off) and `with_delivery` (+$5), same 2-layer $30 cake
- **Stack A** — discount applied first, delivery last: 30 → ×0.9 = 27 → +5 = **$32.00**
- **Stack B** — delivery applied first, discount last: 30 → +5 = 35 → ×0.9 = **$31.50**
- **The rule** — the decorator written closest to the function runs first; outer wrappers act on its result
- **The mistake** — stacking decorators as if order never mattered; here it decides if delivery gets discounted

*Example (italic):* Two cashiers ring up the same cake with the same two wrappers and charge $32.00 vs $31.50 — the only difference is which wrapper sat on the outside.

**Common mistake:** Assuming stacked decorators commute. Wrapping is function composition — ×0.9 then +5 is not +5 then ×0.9 — so always trace one call by hand through the stack.

### Visualization (canvas `c4`, 720×300)

Two side-by-side vertical pipelines on one $30 cake: each shows the price flowing down through the two wrappers in a different order, ending at $32.00 vs $31.50.

- **Title (bold 15px, `#1a5276`, top center):** "Same Two Wrappers, Different Order — Different Bill".
- **Left pipeline (centered x=220):** bold 12px `#1a5276` header at y=70 "discount inside, delivery outside"; three rounded boxes (w=200, h=36) stacked at y=85, 145, 205 with 3px `#1a5276` down-arrows between: "price(2) = $30" (fill `rgba(42,120,214,0.15)`), "×0.9 → $27" (fill `rgba(230,126,34,0.15)`), "+$5 → $32.00" (fill `rgba(0,131,0,0.15)`, bold 13px green text).
- **Right pipeline (centered x=500):** header "delivery inside, discount outside"; same three-box layout: "price(2) = $30", "+$5 → $35" (fill `rgba(0,131,0,0.15)`), "×0.9 → $31.50" (fill `rgba(230,126,34,0.15)`, bold 13px orange `#d95926` text).
- **Box text:** 13px `#2c3e50` centered unless noted bold/colored above; all numbers exactly as listed.
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "same wrappers, 50¢ apart — the outer wrapper acts on the inner one's answer".
- **Caption (12px `#444`, bottom right):** "illustrative prices".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all prices, timings, and pipeline values are the hardcoded literals above (no randomness); c3 bar values are `[800, 1, 1, 1, 1]`; c4 pipeline values 30/27/32.00 and 30/35/31.50 must match the text bullets exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
