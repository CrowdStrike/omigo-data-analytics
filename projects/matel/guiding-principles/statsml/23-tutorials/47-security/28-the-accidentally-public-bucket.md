# The Accidentally Public Bucket

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Accidentally Public Bucket

**Subtitle:** No attacker required — one permission setting turns an export folder into a website anyone can list

## One Permission Setting, and the Export Folder Is a Website

**Tags:** `core idea` (blue), `one wrong setting` (orange), `no break-in` (red)

- **The job** — an analytics team writes a nightly customer export to cloud object storage for a reporting tool
- **The shortcut** — the tool cannot authenticate in time for the deadline, so Bob sets the container to public
- **Nothing else changes** — the export keeps running on schedule and every dashboard downstream stays green
- **The container is now a site** — anyone who asks can list its contents and download every file in it
- **No exploit exists** — there is no vulnerability and no unauthorized access; the system did what it was told
- **So the tools stay quiet** — scanners find no flaw, patching has nothing to patch, intrusion detection sees no intrusion
- **Definition** — public-bucket exposure is data disclosure caused purely by an access-policy setting, not by any attack

*Example (italic):* The nightly file lands at a placeholder path like `exports/2026-03-14-<8 hex chars>.csv`; the container's access setting, not the path, is what decides who may read it.

**Key point:** This is the cleanest security failure with no adversary skill in it — the configuration *is* the incident, which is exactly why every detection tool built to spot broken things reports nothing.

### Visualization (canvas `c1`, 720×300)

Flow diagram: the nightly export job writes into one container, whose access setting opens two doors — an authenticated reader and the open internet.

- **Title (bold 15px, `#1a5276`, top center):** "The Export Keeps Working — the Access Setting Decides Who Else Reads It".
- **Export-job box:** blue rounded box at x=30, y=118, 150×54, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` two-line centered text "nightly export job" / "40,000 rows".
- **Container box:** orange rounded box at x=250, y=110, 170×70, fill `rgba(217,89,38,0.14)`, 2px `#d95926` border, three lines: bold 12px "the container" (y=132), 11px `#6b7280` "180 nightly files" (y=150), bold 11px `#d95926` "access: public" (y=168).
- **Arrow:** 3px `#2a78d6` from x=180,y=145 to x=242,y=145 with arrowhead; 11px `#6b7280` label "writes" centered at (211, 136).
- **Door boxes (x=490, 190×46, 8px radius):** at y=78 green fill `rgba(0,131,0,0.13)`, 2px `#008300` border, 12px text "reporting tool (intended)"; at y=176 red fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, 12px text "anyone on the internet".
- **Fan-out arrows:** 3px `#008300` from (420,145) to (486,101) and 3px `#e74c3c` from (420,145) to (486,199), each with a matching arrowhead.
- **Annotation (bold 13px red `#e74c3c`, left-aligned at x=452, y=246):** "no exploit, no alarm".
- **Annotation (bold 12px `#4a3aa7`, left-aligned at x=40, y=246):** "same job, same file, same schedule".
- **Caption (12px `#444`, bottom right):** "illustrative; path is a generic placeholder".

## Six Months of Nightly Files: Row-Copies Are Not People

**Tags:** `worked example` (blue), `retention` (orange), `arithmetic` (green)

- **Setup** — one export per night, 40,000 customer rows per file, retained 180 days: 180 files sit in the container
- **The naive number** — 40,000 × 180 = 7,200,000, and that total is real but it counts *rows*, not customers
- **Why it double-counts** — the same customer appears in every nightly file, so one person supplies up to 180 rows
- **Distinct people** — today's 40,000 plus those who churned out during the window, about 1,200 per month × 6 = 7,200
- **The honest headline** — roughly 47,200 distinct customers exposed, and 7,200,000 row-copies of their data
- **Average multiplicity** — 7,200,000 ÷ 47,200 ≈ 152.5 copies per person, which is the retention window, not extra victims
- **History still matters** — old files leak address changes, cancelled accounts, and fields since dropped from the export

*Example (italic):* Reporting "7.2 million customers exposed" overstates the harm by a factor of about 152, yet reporting "40,000" hides the 7,200 people who left and are only in the older files.

**Key point:** Retention multiplies the *volume* exposed, not the *population* — quote distinct people for who was harmed and row-copies for how much data moved, and never let the product of the two become the headline.

### Visualization (canvas `c2`, 720×300)

Bar-plus-line chart: cumulative row-copies grow month by month to 7,200,000 while the distinct-people line stays flat on the axis at 47,200.

- **Title (bold 15px, `#1a5276`, top center):** "Row-Copies Climb to 7,200,000; Distinct People Stop at 47,200".
- **Axes:** origin x=78, baseline y=248, plot width 590, plot height 170; y = cumulative row-copies 0 to 7,200,000; gridlines `#e5e9ef` at 1.8M/3.6M/5.4M/7.2M with right-aligned 12px `#444` labels "1.8M", "3.6M", "5.4M", "7.2M" at x=72; x-axis 2px `#999`.
- **Bars (58px wide, centered at x = 126, 216, 306, 396, 486, 576)** from hardcoded row-copy totals `[1200000, 2400000, 3600000, 4800000, 6000000, 7200000]` mapped to heights `[28, 57, 85, 113, 142, 170]` px; fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; x labels 12px `#444` below baseline at y+18: "30d", "60d", "90d", "120d", "150d", "180d".
- **Distinct-people line:** 2.5px `#008300` polyline through the same x centers using hardcoded distinct counts `[41200, 42400, 43600, 44800, 46000, 47200]` on the same scale — heights `[1, 1, 1, 1, 1, 1]` px, so it visibly sits on the axis; 4px `#008300` dots at each point.
- **Annotation (bold 13px `#008300`, left-aligned at x=150, y=232):** "distinct people: 41,200 → 47,200 — flat at this scale".
- **Annotation (bold 13px `#2a78d6`, left-aligned at x=150, y=64):** "7,200,000 row-copies ÷ 47,200 people ≈ 152.5 each".
- **Axis caption (12px `#444`, centered at x=373, y=286):** "retention window (nightly files kept)".
- **Caption (12px `#444`, bottom right):** "illustrative".

## One List Request Versus 4,294,967,296 Guesses

**Tags:** `worked example` (blue), `listable folder` (orange), `read vs list` (green)

- **The naming scheme** — files are named by date plus an 8-character hex suffix, so names look unguessable
- **Guess cost** — 8 hex characters give 16⁸ = 4,294,967,296 candidate suffixes for one specific unknown file
- **List cost** — if listing is permitted, one request returns the index of all 180 names, and the guessing is over
- **The ratio is the control** — 4,294,967,296 tries versus 1 request is precisely what disabling listing buys you
- **Read is not list** — read access hands over files you can name; list access hands over the names themselves
- **Names you'd never guess** — an index also reveals one-off files nobody remembers writing, like a debug dump
- **No usable log** — a public read looks identical to a legitimate one, and access logging is often off entirely
- **Unknowable blast radius** — with no request log, nobody can bound what was taken, so every file must be assumed read

*Example (italic):* Bob's suffix scheme is a 4-billion-to-1 puzzle right up until listing is allowed, at which point the container simply prints the answer key.

**Key point:** Listing is the multiplier — one guessable name costs one file, but a listable container hands over a complete index, and turning listing off is the single highest-leverage change short of making it private.

### Visualization (canvas `c3`, 720×300)

Two-panel comparison: guessing one unknown object name versus one list request that returns the whole index.

- **Title (bold 15px, `#1a5276`, top center):** "Guess One Name, or Ask for the Index".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=48 to y=262.
- **Left panel header (bold 13px `#2c3e50`, centered at x=180, y=68):** "listing OFF — must guess a name".
- **Left panel body:** yellow rounded box at x=58, y=88, 245×62, fill `rgba(201,133,0,0.13)`, 2px `#c98500` border; 12px `#2c3e50` centered "date + 8 hex characters" (y=112) and 11px `#6b7280` centered "16 × 16 × … × 16, eight times" (y=132).
- **Left panel figure (bold 16px `#c98500`, centered at x=180, y=180):** "4,294,967,296"; below it 12px `#444` centered "possible suffixes for ONE file" (y=200).
- **Right panel header (bold 13px `#2c3e50`, centered at x=540, y=68):** "listing ON — ask once".
- **Right panel body:** red-tinted rounded box at x=418, y=88, 245×62, fill `rgba(231,76,60,0.10)`, 2px `#e74c3c` border; 12px `#2c3e50` centered "one list request" (y=112) and 11px `#6b7280` centered "returns all 180 file names" (y=132).
- **Right panel figure (bold 16px `#e74c3c`, centered at x=540, y=180):** "1"; below it 12px `#444` centered "request to learn every name" (y=200).
- **Index strip (right panel only):** six grey ticks representing returned file names — 34×6 rectangles, fill `rgba(107,114,128,0.35)`, at x=452 with y = 214, 224, 234 and at x=560 with y = 214, 224, 234; 11px `#6b7280` centered label "…the index" at (540, 254).
- **Annotation (bold 13px `#4a3aa7`, centered at x=360, y=284):** "obscure names are a 4-billion-to-1 lock that listing simply unlocks".
- **Caption (12px `#444`, top right at y=44 — the bottom edge is occupied by the annotation):** "illustrative naming scheme".

## "We Were Breached" Misdirects the Fix

**Tags:** `common mistake` (red), `default-deny` (green), `configuration control` (orange)

- **Wrong frame** — nobody broke in, so perimeter hardening, patching, and intrusion detection change nothing here
- **Right frame** — this is a configuration defect, and the fix lives in how access policy is set and reviewed
- **Not ignorance** — public-by-accident comes from a convenience decision under deadline, so training alone won't hold
- **Default-deny wins** — blocking public access at the account level fails safe when someone is in a hurry
- **Obscurity is not control** — an "unguessable" name leaks via a shared link, a referrer header, or a search index
- **Pre-signed URLs** — a time-limited link lets one consumer read one object without making anything public
- **Log before you need it** — enable access logging up front, or incident response cannot bound the exposure
- **Scan for drift** — an automated public-container check is among the highest-value configuration tests that exists

*Example (italic):* Alice's fix is not a firewall rule: it is account-level public-access blocking, a time-limited link for the reporting tool, and moving the export out of any container whose purpose is sharing.

**Common mistake:** Calling it a breach and buying detection. Nothing was broken, so nothing detects it — control the configuration, keep production data out of sharing containers, and separate the export destination from the analysis source.

### Visualization (canvas `c4`, 720×300)

Controls ladder: four rungs from public with guessable names to default-deny plus pre-signed URLs, with a verdict per rung.

- **Title (bold 15px, `#1a5276`, top center):** "Which Rungs Actually Hold".
- **Rungs (rounded boxes, 8px radius, 300px wide, 40px tall, left edge x=64, rising bottom to top):** y=232 "public + guessable names", y=182 "public + 8-hex obscure names", y=132 "public read, listing disabled", y=82 "default-deny + pre-signed URLs".
- **Rung colors, bottom to top:** fill `rgba(231,76,60,0.14)` border `#e74c3c`; fill `rgba(217,89,38,0.14)` border `#d95926`; fill `rgba(201,133,0,0.14)` border `#c98500`; fill `rgba(0,131,0,0.14)` border `#008300`. Labels 12px `#2c3e50`, left-aligned at x=78, vertically centered in each box.
- **Verdicts (bold 12px in the rung's border color, left-aligned at x=384):** bottom to top "everything readable", "index still lists every file", "one leaked name leaks one file", "nothing public; access expires".
- **Connector:** 2px `rgba(107,114,128,0.5)` vertical line at x=56 from y=252 up to y=88 with an upward arrowhead at y=82; 12px `#6b7280` label "stronger" left-aligned at (24, 74) (horizontal text, no rotation).
- **Annotation (bold 13px `#008300`, left-aligned at x=384, y=62):** "only the top rung fails safe".
- **Annotation (bold 12px `#4a3aa7`, left-aligned at x=64, y=282):** "the middle two rungs depend on nobody being in a hurry".
- **Caption (12px `#444`, top right at y=44 — the bottom edge is occupied by the annotation):** "illustrative".

## Placeholder footnote

After the last `.card-section`, a single `.example` paragraph: "Footnote: the path shown above is a generic placeholder, not a real address, and contains no credential of any kind."

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are hardcoded literal arrays (no `Math.random()`, no PRNG needed). The scenario is invented and labeled illustrative: 40,000 rows per nightly export, 180 days retained, 1,200 churned customers per month.
  - Derived and verified: 40,000 × 180 = 7,200,000 row-copies; distinct customers = 40,000 + 6 × 1,200 = 47,200; 7,200,000 ÷ 47,200 ≈ 152.5 copies per person; 16⁸ = 4,294,967,296 suffix candidates versus 1 list request.
  - Cumulative row-copies per 30-day step: 1,200,000 / 2,400,000 / 3,600,000 / 4,800,000 / 6,000,000 / 7,200,000 (30 nights × 40,000 = 1,200,000 per month).
  - Cumulative distinct people per step: 41,200 / 42,400 / 43,600 / 44,800 / 46,000 / 47,200. At the row-copy scale these are ≈1 px tall, which is the point of the chart and is annotated as such rather than rescaled.
  - Text numbers must match chart numbers exactly; the naive product 7,200,000 is never presented as a people count.
- **Naming/safety constraints:** no real cloud provider, product, or bucket URL format; generic "cloud object storage", "the container", and `<8 hex chars>` placeholders only. No credential strings or key=value secret syntax. People are Alice and Bob; the organization is "an analytics team". Any depicted path is captioned as a generic placeholder.
- **Framing:** defensive/educational throughout — the page explains why the failure mode produces no alarm and which controls hold; it contains no guidance on locating or enumerating exposed storage.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
