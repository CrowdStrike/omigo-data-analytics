# Encryption & KMS Rate Limits

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Encryption &amp; KMS Rate Limits

**Subtitle:** Every object is encrypted by default, but choosing your own key adds a call to the key service on every read — and that service has a rate limit of its own

## Four Ways to Encrypt, and Who Holds the Key

**Tags:** `core idea` (blue), `four modes` (green), `who holds the key` (orange)

- **The job** — one training job reads 8,000 small objects a second (illustrative)
- **Always on** — every new object has been encrypted at rest by default since 2023
- **SSE-S3** — S3 holds the key, costs nothing, and adds no call you can be throttled on
- **SSE-KMS** — a key in the key service, either an AWS-managed one or your own key
- **What your own key buys** — a policy you control, an audit trail, instant revoke
- **SSE-C** — you send the key with every request and AWS stores no copy of it at all
- **Losing an SSE-C key** — the bytes are unreadable forever, and no support call helps
- **Client-side** — you encrypt before upload, so key management becomes entirely yours

*Example (illustrative):* The same 8,000-reads-a-second job behaves identically under SSE-S3 and SSE-KMS, until the key service's rate limit enters the picture.

**Key point:** The choice is not encrypted or not — all four are encrypted. It is who holds the key, and whether reading an object calls a second service.

### Visualization (canvas `c1`, 720×300)

Four-mode comparison table: one row per encryption mode, three attribute columns, with the extra-call cell in orange and the key-loss cells in red.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "Four Ways to Encrypt: Who Holds the Key, and What It Costs".
- **Grid geometry:** row-label column starts at x=46. Three value columns of width 170 at x=158, 336, 514 (right edge 684). Header band y=74 height 30; four data rows of height 34 with tops at y = 106, 144, 182, 220; text baseline is `top + 22`.
- **Header cells (bold 11.5px `#1a5276`, centered on each column, two lines at y=87 and y=100):** "who holds / the key", "extra call / on every read", "lose the key → / lose the data".
- **Row labels (bold 12.5px `#1a5276`, left-aligned at x=46, on the row baseline):** `SSE-S3`, `SSE-KMS`, `SSE-C`, `client-side`.
- **Rows (label, then the three cell values):**
  - `SSE-S3` → "S3 manages it", "no", "not possible"
  - `SSE-KMS` → "your own key service key", "yes — one per read", "not possible"
  - `SSE-C` → "you, on every request", "no", "yes — bytes are lost"
  - `client-side` → "you, before upload", "no", "yes — bytes are lost"
- **Extra-call cell:** the single "yes — one per read" cell is emphasised — fill `rgba(217,89,38,0.18)`, 2.5px `#d95926` border, bold 11.5px `#d95926` text.
- **Risk cells:** both "yes — bytes are lost" cells fill `rgba(231,76,60,0.18)`, 2.5px `#e74c3c` border, bold 11.5px `#e74c3c` text.
- **Default cell style:** fill `rgba(42,120,214,0.07)`, 1.2px `#e5e9ef` border, 11.5px `#2c3e50` centered text, 5px corner radius.
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=272):** "only SSE-KMS puts a second service on the read path". It sits at y=272, not y=252, because the last row occupies y=220–254 and a y=252 baseline would land inside it.
- **Caption (12px `#444`, bottom right at y=294):** "read rate illustrative; the four modes are documented".

## Why One Read Becomes Two Calls

**Tags:** `mechanism` (blue), `two keys` (green), `the extra call` (orange)

- **Two keys, not one** — each object is encrypted with its own single-use data key
- **The wrap** — that data key is itself sealed by your key and stored with the object
- **On write** — the key service hands back the data key plus a sealed copy to store
- **On read** — the sealed data key must be unsealed before any bytes can be decrypted
- **The extra call** — so every read is one S3 request plus one call to the key service
- **Why it works this way** — revoking your key locks every object without rewriting bytes
- **Charged per call** — 8,000 tiny reads cost exactly what 8,000 huge reads would cost
- **Sharing across accounts** — an outside reader needs permission on your key as well

*Example (illustrative):* Reading one 200 KB object is two calls — the read itself, plus one call to unseal the data key stored beside that object.

**Key point:** This is why the encryption choice has a throughput story at all: your object read rate becomes a request rate against a different service.

### Visualization (canvas `c2`, 720×300)

Left half: three nested boxes showing your key sealing the data key which encrypts the object bytes. Right half: the three steps of a single read, with the key-service step highlighted.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "Your Key Never Touches the Object Bytes".
- **Column captions (bold 12px `#1a5276`, centered, baseline y=54):** "how it is stored" at x=190; "what one read does" at x=535.
- **Outer box (violet `#4a3aa7`):** x=40, y=68, width 300, height 164, radius 8, fill `rgba(74,58,167,0.10)`, 2px border. Label bold 12px `#4a3aa7` centered at (190, 86): "your key, held in the key service".
- **Middle box (blue `#2a78d6`):** x=64, y=98, width 252, height 122, radius 7, fill `rgba(42,120,214,0.12)`, 2px border. Label bold 12px `#2a78d6` centered at (190, 116): "single-use data key (sealed)".
- **Inner box (green `#008300`):** x=88, y=128, width 204, height 80, radius 6, fill `rgba(0,131,0,0.13)`, 2px border. Two lines of 12px `#2c3e50` centered at (190, 160) and (190, 180): "object bytes" and "(encrypted)".
- **Read-path steps (x=400, width 270, height 38, radius 6, tops at y = 74, 134, 194; text centered at (535, top+24)):**
  - Step 1 fill `rgba(42,120,214,0.10)`, 1.8px `#2a78d6`, 12px `#2c3e50`: "1. the read for the object arrives".
  - Step 2 fill `rgba(217,89,38,0.18)`, 2.5px `#d95926`, bold 12px `#d95926`: "2. the key service unseals the data key".
  - Step 3 fill `rgba(0,131,0,0.11)`, 1.8px `#008300`, 12px `#2c3e50`: "3. the bytes are decrypted and returned".
- **Arrows:** 2px `#6b7280` vertical arrows with heads, from (535, 112) to (535, 132) and from (535, 172) to (535, 192).
- **Extra-call flag (bold 12px `#d95926`, right-aligned at x=690, baseline y=246):** "← the extra call".
- **Annotation (bold 13px aqua `#199e70`, centered at y=270):** "one call to the key service for every object read".
- **Caption (12px `#444`, bottom right at y=294):** "the sealed per-object data key is documented behaviour".

## When the Key Service Throttles Your Reads

**Tags:** `worked example` (blue), `shared limit` (red), `throttling` (orange)

- **One shared limit** — the key service caps crypto requests per region, per account
- **Order of magnitude** — the published per-region cap is tens of thousands per second
- **Our figure** — take 10,000 requests a second as illustrative; check your own region
- **The job's draw** — 8,000 reads a second means 8,000 calls a second to the key service
- **Share of the limit** — 8,000 / 10,000 = 80% of the whole account's regional budget
- **The ceiling** — the line meets the limit at 10,000 reads a second, just 25% more scale
- **Shared, not private** — every other job in that region draws on the same 10,000
- **How it fails** — the key service throttles, so your reads fail while S3 stays healthy

*Example (illustrative):* At 8,000 reads a second the job sits at 80% of the limit; a second job doing 2,500 a second tips the region over and both start failing.

**Key point:** S3 scales to almost any read rate, so nobody watches it. The ceiling you actually hit belongs to the key service, and it arrives looking like failed reads.

### Visualization (canvas `c3`, 720×300)

Line chart: calls to the key service per second against object reads per second, one call per read, with the illustrative regional limit drawn as a ceiling and everything past it shaded.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "One Key-Service Call Per Read: Where the Regional Limit Bites".
- **Plot box and mapping:** x0=66, x1=686, yTop=58, yBase=250, MAX=16,000 on both axes. `X(v) = x0 + (v / MAX) × (x1 − x0)` and `Y(v) = yBase − (v / MAX) × (yBase − yTop)`. Both helpers computed in JS; no hardcoded pixel positions for data points.
- **Constants:** `QUOTA = 10000`, `JOB = 8000` — the only two data literals in the chart.
- **Grid and ticks (1px `#e5e9ef`):** horizontal gridlines and shared tick values 0, 4,000, 8,000, 12,000, 16,000. Tick labels 12px `#6b7280`, Y right-aligned at x=58, X centered at y=270.
- **Axes (1.6px `#1a5276`):** left axis and baseline. Axis titles bold 12px `#1a5276`: "object reads / sec" centered at (376, 292); "key-service calls / sec" rotated −90° centered at (20, 154).
- **Throttled region:** fill `rgba(231,76,60,0.10)` from `X(QUOTA)` to x1, y from yTop to yBase. Bold 12px `#e74c3c` label "throttled — reads start failing" centered at `((X(QUOTA) + x1) / 2, 222)`. It sits low in the band on purpose: the slope line is high on the right, so the clear space inside the shading is *below* it, not above it — a y=76 baseline (the earlier draft) put the label where the line passes.
- **Limit ceiling:** 2.5px `#e74c3c` dashed (7/4) horizontal line at `Y(QUOTA)` across the plot; bold 12.5px `#e74c3c` label "regional limit — 10,000/sec (illustrative)" left-aligned at `(x0 + 6, Y(QUOTA) − 10)`, i.e. baseline y=120, ending near x=362 — clear of both the ceiling line below it and the slope line to its right.
- **The one-call-per-read line (3px `#2a78d6`):** from `(X(0), Y(0))` to `(X(MAX), Y(MAX))` — slope 1. Two-line bold 12px `#2a78d6` label left-aligned at (78, 178) and (78, 194): "one key-service" / "call per read". Both lines sit in the empty wedge left of the slope line, below the limit, and between the 8,000 and 4,000 gridlines so neither sits on one.
- **Job marker:** filled 6px `#4a3aa7` dot at `(X(JOB), Y(JOB))`; a single bold 12px `#4a3aa7` label right-aligned at `X(JOB) − 12` with baseline `Y(JOB) − 6` (y=148), reading "the job: 8,000 calls/sec = 80% of the limit", where 80 is computed at render as `Math.round(JOB / QUOTA × 100)`. One line, not two, so it clears the ceiling label above it.
- **Crossover marker:** white-filled 6px dot with 2.5px `#e74c3c` edge at `(X(QUOTA), Y(QUOTA))`, plus a 1.5px dashed (3/3) `#e74c3c` drop line down to the baseline; bold 12px `#e74c3c` label "ceiling at 10,000 reads/sec" left-aligned at `(X(QUOTA) + 12, Y(QUOTA) + 22)` — below the ceiling line, because above it is where the ceiling's own label and the slope line both run.
- **Number formatting:** the two quota/job figures print via `toLocaleString()` off the `QUOTA` and `JOB` constants, so the labels cannot drift from the values the geometry uses.
- **Caption (12px `#444`, bottom right at y=292):** "limit illustrative; throttling is documented". Kept short deliberately: the centered x-axis title occupies roughly x=321–431 on the same baseline, so a longer caption would run into it.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Draw functions are pushed into a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead` and `rgba` as in `11-cross-region-replication.html`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine risk: the two key-loss cells in `c1`, and the limit ceiling plus the throttled region in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, so each holds one line at the 50/50 split without wrapping while still carrying a full clause. An earlier pass clipped them to ~55–70 characters and was rejected for compromising quality — do not shorten them again. Equally, do not pad them back to the folder default of ~90–100 characters. Eight bullets per section is the ceiling, not a target to exceed.
- **Register and vocabulary.** Plain technical English for a first-time reader. No analogies, no invented scenes, no story framing. Prefer "read" and "write" to the API verbs; prefer "the key service" to naming its API operations; say "the key service unseals the data key", not `Decrypt`; say "the key service hands back a sealed copy", not `GenerateDataKey`; say "throttles" and "reads start failing", not the exception name; say "an audit trail", not the log product's name. The mode names `SSE-S3`, `SSE-KMS`, `SSE-C` and client-side are stated because the reader will meet them in a console dropdown; every other product name is dropped. This is a tutorial, not a reference — the precise API surface lives on other pages.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No bucket-keys section.** A fourth section covered S3 Bucket Keys as the fix: the up-to-99% call reduction, 8,000 → 80 calls/sec, the 0.8%-of-limit headroom, a priced month ($62,208 vs $622.08 at $0.03 per 10,000 requests), the small-file penalty, the audit-granularity trade-off, and `c4`'s paired bars. It was cut because the page's job is to teach why the crypto choice creates a throughput ceiling, not to tune it. The mitigation belongs with the other cost and tuning material on `19-cost-egress-and-data-gravity`.
  - **No bucket-key line in `c3`.** The earlier chart drew a second slope-1/100 series labelled "with bucket key: ÷100". It went with the section — `c3` now shows one line only.
  - **No KMS pricing anywhere on the page.** Per-request pricing and monthly totals belong on the cost page.
  - **No fourth attribute column in `c1`.** The earlier table had an "auditable / revocable" column; the audit and revoke facts are carried by the "What your own key buys" bullet instead.
  - **Three sections, eight short bullets each, one canvas each (`c1`, `c2`, `c3`).**
- **Data:** every value is a hardcoded literal; no randomness anywhere, never `Math.random()`.
  - **Documented behaviour the page stands on:** S3 has encrypted every new object at rest by default since 2023; each object is encrypted with a unique single-use data key that is itself sealed by the chosen key and stored with the object, so a read must unseal it first; the key service charges and counts per request, not per byte; SSE-C requires the caller to send the key on every request with AWS storing no copy, making key loss permanent data loss; client-side encryption leaves all key management with the caller; the key service enforces a shared cryptographic-request limit per region per account, on the order of tens of thousands of requests per second and varying by region; exceeding it throttles the caller, which surfaces as failed reads; cross-account readers need permission on the key in addition to bucket access.
  - **Illustrative and labelled as such:** the 8,000-objects-a-second read rate, the 200 KB object, the 2,500-a-second second job, and the 10,000-requests-a-second regional limit.
  - **Computed arithmetic (exact):** 8,000 ÷ 10,000 = 0.80, so 80% of the limit — printed in `c3` as `Math.round(JOB / QUOTA * 100)` rather than as a literal. The ceiling sits at 10,000 reads a second, and 10,000 ÷ 8,000 = 1.25, i.e. 25% more scale. The two-job overflow closes: 8,000 + 2,500 = 10,500 > 10,000.
  - **Chart geometry is derived, not hardcoded:** `c3` positions every point, gridline, ceiling and marker through `X(v)` and `Y(v)`; `c1` derives its cell rectangles from the column x array plus a single column width and the row-top array. Label positions were checked against those mappings so no text overlaps a line or leaves the 720×300 canvas.
- **Naming:** no key identifiers, ARNs, account ids, passwords, tokens, or any key=value credential syntax anywhere. The key is referred to only as "your key" or "your own key service key".
