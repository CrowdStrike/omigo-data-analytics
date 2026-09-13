# Identity Resolution

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Identity Resolution

**Subtitle:** One shopper shows up as a phone cookie, a laptop cookie, and a login id — identity resolution stitches those three ids back into one person

## One Shopper, Three IDs

**Tags:** `core idea` (blue), `id stitching` (green), `identity graph` (orange)

- **The shopper** — Maya browses a store on her phone Monday, then on her laptop Tuesday
- **The ids** — the phone gets anonymous cookie `ph-91`; the laptop gets a second cookie `lt-27`
- **The login** — on Wednesday she logs in on the laptop, adding a third id: account `u-88`
- **The illusion** — the raw event log now holds three ids, so analytics counts three "users"
- **The stitch** — identity resolution links all three ids to one person node in an identity graph

*Example (italic):* Maya is one human with three ids in the warehouse — until the ids are stitched, every dashboard treats her as three separate visitors.

**Key point:** Identity resolution maps the many ids one person leaves behind — cookies, device ids, logins — onto a single person node; the collected links form the identity graph.

### Visualization (canvas `c1`, 720×300)

Identity-graph diagram: three id boxes on a day timeline, each connected down to one person node — two solid deterministic edges, one dashed probabilistic edge.

- **Title (bold 15px, `#1a5276`, top center):** "Three IDs in the Log, One Person Underneath".
- **Id boxes (rounded 8px, 190px wide, 44px tall, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` two-line text), top row at y=70:** "cookie ph-91 / phone — Mon" at x=40, "cookie lt-27 / laptop — Tue" at x=265, "login u-88 / laptop — Wed" at x=490.
- **Person node:** rounded box 200px wide, 44px tall at x=260, y=210, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 13px `#008300` text "one person: Maya".
- **Edges:** 3px solid green `#008300` lines from the lt-27 and u-88 boxes down to the person node, shared 12px green label "deterministic — login event" near (x=470, y=170); 3px dashed orange `#d95926` (dash 6/4) line from ph-91 down to the person node, 12px orange label "probabilistic — score 0.87" near (x=95, y=170).
- **Annotation (bold 13px violet `#4a3aa7`, near x=430, y=272):** "unstitched, dashboards count 3 'users'".
- **Caption (12px `#444`, bottom left):** "ids and score illustrative".

## Stitching Maya's Week Back Together

**Tags:** `worked example` (blue), `deterministic vs probabilistic` (green)

- **The log** — `ph-91` has 4 product views (Mon); `lt-27` has 3 views plus a cart (Tue)
- **The purchase** — Wednesday's login event carries both `lt-27` and `u-88`, then a $60 order
- **Deterministic** — that one login row is hard proof: `lt-27` and `u-88` are the same person
- **Probabilistic** — `ph-91` never logs in; same home network + similar browsing scores 0.87
- **The threshold** — 0.87 clears the 0.80 match bar, so `ph-91` joins the same person node
- **Hand-check** — user count: no stitching 3, deterministic only 2, plus probabilistic 1

*Example (italic):* One login row collapses `lt-27` and `u-88` into one user (3 → 2); the 0.87 probabilistic match on `ph-91` finishes the job (2 → 1).

**Key point:** Deterministic matching uses a shared event (a login seen on both ids) as proof; probabilistic matching scores circumstantial evidence and merges only above a chosen threshold.

### Visualization (canvas `c2`, 720×300)

Bar chart: how many "users" Maya appears to be under three levels of stitching — none, deterministic only, deterministic + probabilistic.

- **Title (bold 15px, `#1a5276`, top center):** "Same Shopper, Three Different User Counts".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = distinct users 0 to 3, gridlines `#e5e9ef` at 1 and 2 with 12px `#444` tick labels.
- **Bars (120px wide, centered at x = 180, 380, 580, heights proportional to 3 / 2 / 1):** "no stitching" blue `#2a78d6` fill `rgba(42,120,214,0.35)` value 3; "deterministic only" aqua `#199e70` fill `rgba(25,158,112,0.35)` value 2; "+ probabilistic" green `#008300` fill `rgba(0,131,0,0.35)` value 1; 2px solid borders in each bar's color.
- **Value labels:** bold 13px in each bar's color, centered above each bar: "3 users", "2 users", "1 user".
- **X labels:** 12px `#444` centered under each bar at y=265.
- **Annotation (bold 13px green `#008300`, above the third bar at y=90):** "all three ids were always one shopper".
- **Caption (12px `#444`, bottom right):** "counts from the worked example, illustrative".

## Why Unstitched IDs Wreck Funnels and Retention

**Tags:** `where it's used` (blue), `funnels` (orange), `retention` (green)

- **Inflated users** — a store with 10,000 real shoppers reports 24,000 "users" from raw cookies
- **Broken funnel** — Maya's view (phone), cart (laptop), buy (login) look like 3 dead-end journeys
- **Diluted conversion** — 1,200 purchases over 24,000 cookie "users" reads 5%; the true rate is 12%
- **Fake churn** — a returning shopper on a new device or cleared cookie is counted as brand new
- **Understated retention** — cookie-level week-2 return looks like 15% when 40% of people came back

*Example (illustrative):* The same 1,200 purchases produce a 5% conversion rate at cookie level and 12% at person level — the product didn't change, only the denominator did.

**Key point:** Unresolved identity inflates the user denominator, so every per-user metric — conversion, funnel step rates, retention — reads worse than reality, and cross-device journeys vanish.

### Visualization (canvas `c3`, 720×300)

Grouped bar funnel: view → cart → buy, cookie-level counts (blue) vs person-level counts (green) side by side per step, same purchases at the end.

- **Title (bold 15px, `#1a5276`, top center):** "One Funnel, Two Denominators: 5% vs 12% Conversion".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = count 0 to 24,000, gridlines `#e5e9ef` at 6,000 / 12,000 / 18,000 with 12px `#444` labels "6k / 12k / 18k".
- **Step groups centered at x = 170, 370, 570, 12px `#444` labels under baseline:** "viewed", "added to cart", "purchased".
- **Cookie-level bars (left of each center, 70px wide, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border):** values `[24000, 3300, 1200]`.
- **Person-level bars (right of each center, 70px wide, fill `rgba(0,131,0,0.35)`, 2px `#008300` border):** values `[10000, 3000, 1200]`.
- **Value labels:** bold 12px in each bar's color above each bar: "24,000 / 3,300 / 1,200" (blue) and "10,000 / 3,000 / 1,200" (green).
- **Legend (12px, top right at y=55):** blue swatch "cookie-level 'users'", green swatch "resolved persons".
- **Annotation (bold 13px magenta `#d55181`, near x=430, y=100):** "same 1,200 buyers — 5% vs 12% conversion".
- **Caption (12px `#444`, bottom right):** "all counts illustrative".

## A Match Score Is Not a Fact

**Tags:** `common mistake` (red), `over-merging` (orange)

- **The confusion** — treating a probabilistic match like a deterministic one; 0.87 is a guess, not proof
- **Over-merge** — a family tablet scores 0.83 for Maya and her partner and fuses them into one "user"
- **The damage** — the merged "user" buys twice as much, skewing lifetime value and segment stats
- **Under-merge** — raise the threshold to 0.90 and Maya's own phone at 0.87 splits back off
- **The trade** — the threshold trades false merges against missed matches; neither side is free

*Example (italic):* At threshold 0.80 the shared tablet (0.83, two people) wrongly merges while a real same-person pair at 0.74 stays split — one knob, both error types.

**Common mistake:** Reporting person-level metrics without stating how identities were resolved. Deterministic links are facts; probabilistic links are threshold-dependent guesses that carry both false merges and misses.

### Visualization (canvas `c4`, 720×300)

Dot-strip chart: candidate id pairs placed by match score on a 0-to-1 axis, colored by whether they are truly the same person, with the 0.80 threshold as a vertical dashed line.

- **Title (bold 15px, `#1a5276`, top center):** "Where You Draw the Threshold Decides Who Gets Merged".
- **Axis:** horizontal 2px `#999` baseline at y=230 from x=60 to x=660; x = match score 0.0 to 1.0, 12px `#444` tick labels every 0.2.
- **Threshold:** vertical dashed `#6b7280` (dash 4/3) line at score 0.80 (x=540), bold 12px `#6b7280` label "merge above 0.80" at its top (y=60).
- **Same-person pairs (green `#008300` filled circles, radius 7) at scores `[0.92, 0.88, 0.87, 0.74, 0.66]`, hardcoded y jitter `[150, 185, 120, 160, 190]`.**
- **Different-people pairs (red `#e74c3c` filled circles, radius 7) at scores `[0.83, 0.71, 0.62, 0.55, 0.41, 0.33]`, hardcoded y jitter `[95, 140, 175, 110, 155, 185]`.**
- **Callouts:** bold 12px red label "shared tablet — false merge" directly above the red dot at 0.83 (near x=560, y=78); bold 12px green label "Maya's phone — 0.87 merges" beside the green dot at 0.87 (near x=585, y=110); bold 12px orange `#d95926` label "same person, missed at 0.74" at (x=330, y=145) with a 1.5px orange leader line to the green 0.74 dot.
- **Legend (12px, top left at y=60):** green circle "truly same person", red circle "actually two people".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=278):** "raising the bar trades false merges for missed matches".
- **Caption (12px `#444`, bottom right):** "scores illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); Maya's ids, event counts, the 0.87/0.83/0.74 match scores, the 3/2/1 user counts, and the 24,000/3,300/1,200 vs 10,000/3,000/1,200 funnel counts are invented and labeled illustrative; the 5% and 12% conversion rates are computed from those funnel counts (1,200/24,000 and 1,200/10,000).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
