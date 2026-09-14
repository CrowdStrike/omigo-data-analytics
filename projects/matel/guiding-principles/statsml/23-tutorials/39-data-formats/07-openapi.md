# OpenAPI

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** OpenAPI

**Subtitle:** One YAML file describes every path, parameter, and response of an API — the schema of an entire API, readable by humans and machines alike

## One File That Describes the Whole Orders API

**Tags:** `core idea` (blue), `API contract` (green), `YAML` (orange)

- **The API** — a coffee shop chain runs an orders API: list orders, place one, fetch one, cancel one
- **The file** — a single `orders-api.yaml` lists all four operations under their paths
- **Per operation** — the file states the method, the parameters, and the exact response shape
- **The schema** — the `Order` object (id, item, price) is defined once and referenced everywhere
- **The payoff** — anyone can read the file and know the full API without reading server code

*Example (italic):* A new mobile developer opens `orders-api.yaml`, sees `GET /orders/{id}` returns an `Order` with `id`, `item`, `price`, and starts coding without asking the backend team anything.

**Key point:** OpenAPI is a machine-readable description of an entire API — every path, method, parameter, and response schema — kept in one YAML (or JSON) file, just as a table schema describes a table.

### Visualization (canvas `c1`, 720×300)

Fan-out diagram: one spec file box on the left, arrows to the four operation boxes it defines, each tagged with its response shape.

- **Title (bold 15px, `#1a5276`, top center):** "One orders-api.yaml Describes Every Endpoint".
- **Spec box:** rounded box at x=40, y=125, 170×56, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 13px `#1a5276` two-line label "orders-api.yaml" / "(one file)".
- **Operation boxes (x=330, 220px wide, 34px tall, 8px radius, 12px `#2c3e50` text), tops at y = 58, 116, 174, 232:**
  - "GET /orders — list orders" fill `rgba(42,120,214,0.12)`, border `#2a78d6`
  - "POST /orders — place an order" fill `rgba(0,131,0,0.12)`, border `#008300`
  - "GET /orders/{id} — fetch one" fill `rgba(42,120,214,0.12)`, border `#2a78d6`
  - "DELETE /orders/{id} — cancel" fill `rgba(217,89,38,0.12)`, border `#d95926`
- **Arrows:** 2px `#6b7280` lines with small arrowheads from the spec box's right edge (x=210, y=153) to each operation box's left edge midpoint.
- **Response tags:** 11px `#6b7280` labels right of each box at x=565: "→ Order[]", "→ Order (201)", "→ Order (200)", "→ 204 no body".
- **Annotation (bold 13px aqua `#199e70`, centered near y=285):** "every path, parameter, and response shape in one place".

## Reading GET /orders/{id} Line by Line

**Tags:** `worked example` (blue), `generated tools` (green)

- **The path** — `/orders/{id}:` declares the URL; the braces mark `id` as a path parameter
- **The method** — `get:` on the next line says this entry describes the GET operation
- **The parameter** — `name: id, in: path, required: true, schema: type: integer` pins the type
- **The response** — `'200':` promises JSON matching the referenced `Order` schema
- **The fan-out** — from these same lines, tools generate docs, a typed client, and a mock server
- **The mock** — the mock server answers `GET /orders/4021` with `{id: 4021, item: "flat white", price: 4.50}`

*Example (italic):* From one 8-line spec entry, the team gets a docs page, a TypeScript function `getOrder(id: number): Order`, and a mock server the app team codes against before the backend exists.

**Key point:** The spec entry is precise enough to execute — documentation, client SDKs, and mock servers are all generated from the same lines, so they can never disagree with each other.

### Visualization (canvas `c2`, 720×300)

Two-part diagram: the YAML snippet as a code panel on the left, arrows to three generated-artifact boxes on the right.

- **Title (bold 15px, `#1a5276`, top center):** "8 Lines of Spec, Three Tools Generated From Them".
- **YAML panel:** rounded box at x=25, y=50, 320×220, fill `#f8f9fa`, 1px `#e5e9ef` border; 12px monospace `#2c3e50` lines at 24px spacing starting y=75:
  - `/orders/{id}:`
  - `  get:`
  - `    parameters:`
  - `      - name: id, in: path`
  - `        required: true`
  - `        schema: {type: integer}`
  - `    responses:`
  - `      '200': schema: Order`
  - highlight the `- name: id` line with a `rgba(201,133,0,0.15)` band and the `'200'` line with a `rgba(0,131,0,0.12)` band.
- **Artifact boxes (x=460, 230px wide, 46px tall, 8px radius, 12px text), tops at y = 60, 130, 200:**
  - "Docs page — parameters + example" fill `rgba(42,120,214,0.12)`, border `#2a78d6`
  - "TS client — getOrder(id): Order" fill `rgba(74,58,167,0.12)`, border `#4a3aa7`
  - "Mock server — {id: 4021, item: \"flat white\", price: 4.50}" (two lines, 11px for the JSON line) fill `rgba(0,131,0,0.12)`, border `#008300`
- **Arrows:** 2px `#6b7280` lines with arrowheads from the panel's right edge (x=345, y=160) to each artifact box's left edge midpoint.
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=290):** "all three come from the same lines — they cannot disagree".
- **Caption (11px `#444`, bottom right):** "order 4021 illustrative".

## The Contract Between the App Team and the Backend Team

**Tags:** `where it's used` (blue), `CI checks` (green), `SDKs` (orange)

- **The contract** — frontend, mobile, and partner teams build against the spec, not against Slack answers
- **SDK generation** — clients in TypeScript, Python, and Java are regenerated on every spec change
- **Request validation** — the server rejects a request with `price: "four"` because the spec says number
- **Spec diffing** — CI compares old and new spec on every pull request and flags breaking changes
- **The year's tally** — of 12 breaking changes proposed, spec diffing caught 9 in CI; only 1 hit production
- **Without it** — the same 12 changes shipped silently: 0 caught in CI, 8 broke clients in production

*Example (italic):* CI blocks a pull request that renames `price` to `amount` because the spec diff marks it breaking — the mobile team never even sees the incident.

**Key point:** The spec turns "does this change break anyone?" into a mechanical CI check instead of a production surprise — that is what makes it a contract rather than documentation.

### Visualization (canvas `c3`, 720×300)

Grouped horizontal bar chart: where 12 breaking changes were caught, with spec diffing in CI vs without.

- **Title (bold 15px, `#1a5276`, top center):** "Where 12 Breaking Changes Got Caught".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 44px per change (max width 396 for 9); light `#e5e9ef` gridlines at x = 230 + 44·[3, 6, 9] with 11px `#6b7280` labels "3", "6", "9" below y=262.
- **Rows (row tops at y = 62, 130, 198), each with a left-aligned 12px `#444` two-line stage label at x=20 ("caught in CI", "caught in staging", "reached production"); within each row two 16px-tall bars, "with spec diff" at row top and "without" 20px lower:**
  - caught in CI: green `#008300` bar width 396 (9), mute `#6b7280` bar width 0 (0, show "0" label only)
  - caught in staging: green bar width 88 (2), mute bar width 176 (4)
  - reached production: orange `#d95926` bar width 44 (1), red `#e74c3c` bar width 352 (8) with bold 12px white label "8 client-breaking incidents" right-aligned inside the bar
- **Bar labels:** 11px `#444` counts at each bar end; legend at top right (x≈540, y=48): green swatch "with spec diff", mute swatch "without".
- **Annotation (bold 13px green `#008300`, near x=280, y=285):** "9 of 12 breaking changes never leave CI".
- **Caption (11px `#444`, bottom right):** "counts illustrative".

## When the Spec Is Written After the Fact

**Tags:** `common mistake` (red), `spec drift` (orange)

- **The trap** — the team ships the API first and hand-writes the YAML afterwards "for the docs"
- **The drift** — each release adds endpoints and fields, but updating the spec is nobody's job
- **The count** — by release 8 the real API has 22 endpoints while the spec still describes 15
- **The damage** — 7 endpoints are invisible to generated SDKs, mocks, and the CI diff alike
- **The fix** — generate the spec from code annotations, or run contract tests that fail on mismatch

*Example (italic):* A partner integrates against the spec's 15 endpoints, then files a bug because the refunds endpoint they were told about "doesn't exist" — it exists, it just was never added to the YAML.

**Common mistake:** Treating the spec as documentation to backfill. A hand-maintained spec drifts from the real API within a few releases — a spec only works as a contract if code and spec are forced to agree by tooling.

### Visualization (canvas `c4`, 720×300)

Two-line drift chart across 8 releases: endpoints in the real API vs endpoints described in the hand-written spec, with the gap widening.

- **Title (bold 15px, `#1a5276`, top center):** "Hand-Written Spec Falls Behind the Real API".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = releases 1 to 8 with 12px `#444` tick labels "r1"–"r8"; y = endpoints 0 to 24, gridlines `#e5e9ef` at 6/12/18/24 with 11px `#6b7280` labels.
- **Real API line:** blue `#2a78d6` 3px line with 3px dots through releases `[1, 2, 3, 4, 5, 6, 7, 8]`, endpoints `[12, 13, 15, 16, 18, 19, 21, 22]`; bold 12px blue label "real API" near (r3, y of 16).
- **Spec line:** magenta `#d55181` 3px line with 3px dots through the same releases, endpoints `[12, 13, 13, 14, 14, 14, 15, 15]`; bold 12px magenta label "spec says" near (r6, y of 12.5).
- **Gap marker:** vertical dashed `#e74c3c` (dash 4/3) segment at r8 between the two lines (22 down to 15), bold 13px red `#e74c3c` label "7 endpoints the spec doesn't know about" to its left.
- **Annotation (bold 12px orange `#d95926`, near r2, y of 21):** "in sync only at launch".
- **Caption (12px `#444`, bottom right):** "endpoint counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded arrays above (no randomness); order 4021 / flat white / 4.50, the 12-breaking-changes tallies (with diff 9/2/1, without 0/4/8), and the drift counts (real API `[12,13,15,16,18,19,21,22]` vs spec `[12,13,13,14,14,14,15,15]`, gap 7 at r8) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
