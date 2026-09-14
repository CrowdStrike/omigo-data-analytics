# Hyrum's Law

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Hyrum's Law

**Subtitle:** With enough users, every observable behavior of your system becomes a contract — whether you promised it or not

## The Sort Order Nobody Promised

**Tags:** `core idea` (blue), `observable behavior` (green), `APIs` (orange)

- **The endpoint** — an orders API returns a customer's orders; the docs never mention any ordering
- **The accident** — the internal tree index happens to store rows by date, so results come out sorted
- **The dependents** — 63 of 1,024 integrations quietly build "newest first" logic on that accident
- **The change** — the team swaps the tree for a hash store; correct per the docs, order now arbitrary
- **The breakage** — 63 client dashboards show scrambled orders the morning after a "safe" deploy

*Example (italic):* Nothing in the contract changed, every documented field is intact — yet 63 teams file outage tickets because an unpromised ordering vanished.

**Key point:** Hyrum's Law (Hyrum Wright, popularized in the book *Software Engineering at Google*): with enough users of an API, it does not matter what you promise in the contract — every observable behavior will be depended on by somebody.

### Visualization (canvas `c1`, 720×300)

Two-row before/after diagram: the same five order IDs returned sorted (before) vs hash-order (after), with a breakage callout.

- **Title (bold 15px, `#1a5276`, top center):** "Same Contract, Different Observable Behavior".
- **Row 1 (y=105), label 12px `#444` at x=20:** "tree index (accidentally sorted)"; five blue `#2a78d6` rounded boxes starting at x=230, each 84px wide, 40px tall, 10px gap, labeled `#101 Jan`, `#102 Feb`, `#103 Mar`, `#104 Apr`, `#105 May` (12px `#2c3e50`), fills `rgba(42,120,214,0.15)`.
- **Row 2 (y=200), label:** "hash store (arbitrary order)"; same five boxes but ordered `#103 Mar`, `#105 May`, `#101 Jan`, `#104 Apr`, `#102 Feb`, fills `rgba(230,126,34,0.15)` with orange `#d95926` borders.
- **Arrow:** 3px `#6b7280` vertical arrow from row 1 to row 2 at x=190, 12px `#6b7280` label "internal refactor — docs unchanged" beside it.
- **Annotation (bold 13px red `#e74c3c`, near y=265, centered):** "63 of 1,024 callers assumed the order — all broke".
- **Caption (12px `#444`, bottom right):** "order IDs and caller counts illustrative".

## With Enough Users, Someone Depends on Everything

**Tags:** `worked example` (blue), `probability` (green)

- **One behavior** — suppose any single user has only a 0.5% chance of leaning on one quirk
- **The formula** — chance somebody depends on it is 1 − (0.995)^n for n users; redo it by hand
- **Ten users** — 1 − 0.995^10 ≈ 4.9%: a quirk with ten callers is probably still safe to change
- **A thousand users** — 1 − 0.995^1000 ≈ 99.3%: somebody almost certainly depends on it
- **The lesson** — "with enough users" is not rhetoric; the odds race to 1 as the user base grows

*Example (italic):* At 100 users the odds someone depends on your accidental sort are already 39%; at 500 users they are 92% — the quirk is a contract long before you notice.

**Key point:** The law is just compounding probability: each user is unlikely to depend on any one observable quirk, but across enough users and enough quirks, every behavior finds a dependent.

### Visualization (canvas `c2`, 720×300)

Curve chart: probability at least one user depends on a given observable quirk vs number of users, at 0.5% per-user chance.

- **Title (bold 15px, `#1a5276`, top center):** "Odds Somebody Depends on One Quirk: 1 − (0.995)^n".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = users 0 to 1000 with 12px `#444` tick labels at 0/250/500/750/1000; y = probability 0% to 100%, gridlines `#e5e9ef` at 25/50/75%.
- **Curve:** blue `#2a78d6` 3px line through users `[1, 10, 50, 100, 250, 500, 750, 1000]`, probabilities `[0.5, 4.9, 22.2, 39.4, 71.4, 91.8, 97.7, 99.3]` (percent).
- **Point markers:** 4px filled `#2a78d6` dots at users 100, 500, 1000 with 12px `#444` value labels "39%", "92%", "99.3%".
- **Threshold line:** horizontal dashed `#6b7280` (dash 4/3) line at 95%, 12px `#6b7280` label "near-certainty" at its left end.
- **Annotation (bold 13px violet `#4a3aa7`, near x=600, y=90):** "past ~600 users, assume every quirk has a dependent".
- **Caption (12px `#444`, bottom right):** "0.5% per-user rate illustrative; curve values exact for that rate".

## Why Big API Owners Shuffle Their Results

**Tags:** `where it's used` (blue), `defensive design` (green)

- **The consequence** — changing ANY observable behavior of a widely-used interface breaks someone
- **Test everything** — large API owners run a proposed change against all known consumers first
- **Deliberate randomness** — a real, documented technique: shuffle unordered results on every call
- **Why shuffle works** — no client can ever pass tests while depending on order, so the habit dies
- **Interface hygiene** — expose as little as possible; what callers cannot observe cannot be a contract

*Example (italic):* Had the orders API shuffled unordered results from day one, zero of the 63 accidental dependencies could ever have formed — clients would have broken in their own tests.

**Key point:** Mature API owners treat Hyrum's Law as an engineering constraint: verify changes against every consumer, randomize behaviors you refuse to promise, and shrink the observable surface.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: how many of 1,024 callers break when the storage refactor ships, under three ownership strategies.

- **Title (bold 15px, `#1a5276`, top center):** "Same Refactor, Three Strategies: Who Breaks?".
- **Axis:** bars extend right from a 2px `#999` baseline at x=250, max width 420; x scale 0 to 63 broken callers, 12px `#444` end-of-bar value labels.
- **Rows (top to bottom at y = 85, 145, 205), each with a left-aligned 12px `#444` label at x=20:**
  - "ship blind": red `#e74c3c` bar width 420, label "63 broken in production"
  - "test against all consumers": orange `#d95926` bar width 420 with a green `#008300` outline segment, label "63 caught before release, 0 in production" — bar drawn hatched to show breaks found early
  - "shuffle unordered results from day one": green `#008300` bar width 6, label "0 — dependency never forms"
- **Bar style:** 22px tall, fills `rgba(231,76,60,0.30)` / `rgba(217,89,38,0.25)` / `rgba(0,131,0,0.30)`, solid 2px matching borders.
- **Annotation (bold 13px green `#008300`, near y=255, centered):** "randomness turns 'unspecified' into 'undependable'".
- **Caption (12px `#444`, bottom right):** "caller counts illustrative".

## The Fix That Broke a Client

**Tags:** `common mistake` (red), `timing & error text` (orange)

- **The speedup** — an endpoint is optimized from 800ms to 90ms; strictly an improvement, on paper
- **The victim** — one client had a race condition that the old slowness always hid; now it fires
- **Error text too** — another client parses the error STRING "quota exceeded"; rewording it breaks them
- **The joke** — xkcd 1172: change anything and someone's "spacebar heats my computer" workflow dies
- **The flip side** — as a consumer, depend only on the documented contract; the rest can vanish anytime

*Example (italic):* The latency fix ships on day 10; the client's checkout failures jump from 0 to 41 per hour because their unawaited callback finally loses the race it had always won.

**Common mistake:** Believing that only documented behavior can break clients. Timing, ordering, error wording, even bugs are observable — so someone, somewhere, treats each one as API.

### Visualization (canvas `c4`, 720×300)

Dual-line timeline: endpoint latency (drops at the optimization) and one client's failures per hour (jumps at the same moment).

- **Title (bold 15px, `#1a5276`, top center):** "Making It Faster Broke Them: a Hidden Race Surfaces".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days 0 to 20 with 12px `#444` tick labels every 5 days; left y = 0 to 900 (shared pixel scale, series labeled in-chart, no right axis), gridlines `#e5e9ef` at 225/450/675.
- **Latency line:** blue `#2a78d6` 3px line through days `[0, 4, 8, 10, 10.2, 12, 16, 20]`, values `[800, 805, 798, 802, 90, 88, 92, 90]` (ms) — cliff at day 10; bold 12px blue label "latency (ms)" near (day 4, y=95).
- **Failure line:** red `#e74c3c` 3px line through the same day grid, values `[0, 0, 0, 0, 410, 415, 405, 410]` (failures/hr ×10 for pixel scale: plotted as 0 and 410) — bold 12px red label "client failures/hr: 0 → 41" near (day 15, y=150).
- **Deploy marker:** vertical dashed `#6b7280` (dash 4/3) line at day 10, 12px `#6b7280` label "optimization ships" at its top.
- **Annotation (bold 13px orange `#d95926`, near day 13, y=60):** "the slowness WAS their contract".
- **Caption (12px `#444`, bottom right):** "latency and failure counts illustrative; failures plotted ×10 for visibility".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); caller counts, latencies, and failure rates are invented and labeled illustrative; the c2 probabilities (0.5 / 4.9 / 22.2 / 39.4 / 71.4 / 91.8 / 97.7 / 99.3) are exact values of 1 − 0.995^n for the stated n. Credit Hyrum Wright and *Software Engineering at Google* in the section-1 key point; keep the xkcd 1172 reference as a bullet, not a link.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
