# Multi-Tenancy

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Multi-Tenancy

**Subtitle:** One deployment serves thousands of customers who each must believe they have the system to themselves — the design question is how strongly you separate them, and what that costs

## Five Thousand Companies, One Invoices Table

**Tags:** `core idea` (blue), `many customers, one system` (green), `SaaS` (orange)

- **The product** — an invoicing SaaS serves 5,003 companies: 5,000 small firms and 3 huge ones
- **One deployment** — all of them hit the same app servers and the same `invoices` table
- **The illusion** — Acme Tools logs in and sees only Acme's invoices, as if the system were theirs
- **The column** — every row carries a `tenant_id`; that one column is all that separates neighbors
- **The name** — each customer is a *tenant*; a design that shares one system this way is *multi-tenant*

*Example (italic):* Invoice rows for Acme (t-0007), Blue Dental (t-1042), and the giant MegaCorp (t-0001) sit interleaved in the same table, told apart only by `tenant_id`.

**Key point:** Multi-tenancy is one deployment serving many customers who must each experience it as private — isolation is enforced by software convention, not by separate hardware.

### Visualization (canvas `c1`, 720×300)

Flow diagram: many tenant boxes on the left funnel into one app + database, with a drawn table on the right showing interleaved rows separated only by a highlighted `tenant_id` column.

- **Title (bold 15px, `#1a5276`, top center):** "5,003 Companies, One Deployment: a tenant_id on Every Row".
- **Left column (rounded boxes 160×30, x=20, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text) at y = 60, 100, 140, 180:** "Acme Tools (t-0007)", "Blue Dental (t-1042)", "Rex Gyms (t-3288)", "…4,997 more"; fifth box at y=222 in orange fill `rgba(217,89,38,0.15)` labeled "MegaCorp (t-0001) — whale".
- **Middle box:** rounded box x=240..380, y=125..185, fill `rgba(26,82,118,0.12)`, bold 12px `#1a5276` two-line label "one app / one database"; 2px `#6b7280` arrows from each left box to its left edge.
- **Table (right):** x=420..700, header row at y=80 (bold 11px `#1a5276`): "invoice_id | tenant_id | amount"; six 24px data rows (11px `#2c3e50`, row separators `#e5e9ef`): `[["9001","t-1042","$120"], ["9002","t-0001","$8,400"], ["9003","t-3288","$75"], ["9004","t-0007","$310"], ["9005","t-0001","$12,050"], ["9006","t-1042","$95"]]`; the tenant_id column band filled `rgba(42,120,214,0.12)`.
- **Annotation (bold 12px blue `#2a78d6`, under the table near y=270):** "neighbors share the table — one column keeps them apart".
- **Caption (12px `#444`, bottom left):** "tenants and amounts illustrative".

## The Isolation Spectrum: Shared Rows to Dedicated Stacks

**Tags:** `worked example` (blue), `cost vs isolation` (green)

- **Shared everything** — one table, `tenant_id` column: ~$0.40/tenant/mo, but one bad query hurts all 5,003
- **Separate schema** — shared app, each tenant its own schema: ~$4/tenant/mo, blast radius is one tenant
- **Silo** — a dedicated stack per tenant: ~$400/tenant/mo, strongest isolation, for whales and regulated firms
- **The spread** — the two ends of the spectrum differ by 1,000× in cost per tenant (illustrative)
- **Density** — one database server holds 5,000 shared tenants but just 1 silo tenant; SaaS margin is density

*Example (italic):* At $30/mo revenue per small tenant, shared-everything ($0.40 cost) is a business and a silo ($400 cost) is a charity — so the 5,000 small firms share, and only the 3 whales get silos.

**Key point:** Isolation is a dial, not a switch — you buy safety with hardware, so most tenants sit at the cheap shared end and only the biggest or most regulated earn a dedicated stack.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: monthly cost per tenant for the three isolation levels, with isolation-strength labels; log-feel via hardcoded pixel widths.

- **Title (bold 15px, `#1a5276`, top center):** "The Isolation Spectrum: 1,000× Between the Ends".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440; no real log axis — widths hardcoded.
- **Rows (bars 16px tall, top edges at y = 75, 140, 205), each with a right-aligned 12px `#444` two-line label ending at x=240:**
  - "Shared everything / $0.40 per tenant/mo": green `#008300` bar width 30, 11px `#444` end label "weakest — one bad query hurts all 5,003"
  - "Separate schema / $4 per tenant/mo": blue `#2a78d6` bar width 150, end label "blast radius: one tenant"
  - "Silo (dedicated stack) / $400 per tenant/mo": orange `#d95926` bar width 440, end label rendered above the bar: "strongest — the 3 whales live here"
- **Bar fills:** solid at 0.85 alpha equivalents — `rgba(0,131,0,0.75)`, `rgba(42,120,214,0.75)`, `rgba(217,89,38,0.75)`.
- **Annotation (bold 13px magenta `#d55181`, centered near y=260):** "density (tenants per box) is the SaaS business model; isolation is what enterprises demand".
- **Caption (12px `#444`, bottom right):** "costs illustrative; pixel widths schematic (log-feel)".

## The Noisy Neighbor and the Three Whales

**Tags:** `where it's used` (blue), `noisy neighbor` (orange), `quotas` (green)

- **Skewed load** — the 3 whales send 1.8M of the 3M daily requests (60%); 5,000 small tenants share the rest
- **The incident** — at 9:00am MegaCorp starts a year-end export; long-tail p95 jumps from 140ms to 2,400ms
- **The fix** — per-tenant rate limits and quotas cap any one tenant at 20% of database capacity
- **After the cap** — the whale's export runs slower, but everyone else's p95 stays under 150ms
- **Per-tenant metrics** — latency, errors, and cost per tenant is how you spot whales and bill them fairly

*Example (italic):* Once metrics showed 3 tenants driving 60% of load, the whales were moved to dedicated silos — the shared cluster's p95 dropped and the whales stopped being neighbors at all.

**Key point:** In a shared system, one tenant's spike is every tenant's outage unless per-tenant quotas exist — and per-tenant metrics are what tell you which whales to cap or move out.

### Visualization (canvas `c3`, 720×300)

Timeline chart of long-tail p95 latency from 8:00 to 10:00am: without quotas (spike when the whale exports) vs with per-tenant quotas (flat), shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "9:00am Whale Export: Everyone's p95, With and Without Per-Tenant Quotas".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = time "8:00" to "10:00", 12px `#444` tick labels every 30 min; y = p95 latency 0 to 2,500 ms, gridlines `#e5e9ef` at 500/1000/1500/2000 with 11px labels.
- **No-quota line:** red `#e74c3c` 3px line through minutes-after-8am `[0, 15, 30, 45, 60, 75, 90, 105, 120]`, p95 ms `[130, 125, 135, 140, 2400, 2100, 1600, 300, 140]` — cliff up at 9:00, slow recovery as the export drains.
- **Quota line:** green `#008300` 3px line through the same minute grid, p95 ms `[130, 125, 135, 140, 145, 150, 148, 138, 130]` — flat.
- **Export marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 60, 12px `#6b7280` label "whale starts year-end export" at its top.
- **Annotation (bold 13px green `#008300`, near minute 90, y=95):** "quota caps the whale at 20% of capacity — long tail stays under 150ms".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## The Missing tenant_id Filter — the Classic Leak

**Tags:** `common mistake` (red), `data leak` (orange)

- **The rule** — in a shared-everything design, every single query MUST filter by `tenant_id`
- **The bug** — one forgotten `AND tenant_id = ?` and a query returns other companies' rows
- **Not a crash** — the query succeeds and passes tests seeded with one tenant; it fails only in production
- **The stakes** — this is a data breach, not a glitch: Acme reading MegaCorp's invoices ends the contract
- **The guardrail** — make it impossible to forget: database row-level security or a tenant-scoped query layer

*Example (italic):* An "open invoices" report ships without the tenant filter; Acme's screen lists rows for t-0007, t-0001, and t-1042 — three companies' data in one customer's browser.

**Common mistake:** Trusting 200 developers to hand-write the tenant filter in every query forever. The classic multi-tenant bug is one missing WHERE clause — enforce tenancy in one place (RLS or a scoped ORM), not in every query.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same report query without the tenant filter (leak) vs with it (safe), shown as a query box flowing to a result box.

- **Title (bold 15px, `#1a5276`, top center):** "One Missing WHERE Clause Is a Data Breach".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "filter forgotten"; blue `#2a78d6` rounded box at x=150, 240px wide, labeled "SELECT * FROM invoices\nWHERE status='open'" (11px monospace-feel), 3px arrow to a red `#e74c3c` box at x=460, 230px wide, labeled "rows for t-0007, t-0001, t-1042…" with bold 12px red "✗ leak — Acme sees MegaCorp's invoices" beneath.
- **Row 2 (boxes centered on y=215), label:** "tenant-aware"; blue rounded box at x=150, 240px wide, labeled "…WHERE status='open'\nAND tenant_id='t-0007'", 3px arrow to a green `#008300` box at x=460, 230px wide, labeled "only Acme's 214 rows" with bold 12px green "✓ each tenant sees only itself" beneath.
- **Box style:** 46px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 11–12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=278):** "enforce tenancy once — row-level security or a tenant-scoped ORM — not in every query".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); tenant counts (5,000 small + 3 whales), request shares (1.8M of 3M = 60%), per-tenant costs ($0.40 / $4 / $400), and latency series are invented and labeled illustrative; text numbers and chart numbers must stay in sync.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
