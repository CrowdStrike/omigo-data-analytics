# What a Browser Extension Can See

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** What a Browser Extension Can See

**Subtitle:** "Read and change all your data on all websites" is one click — and it outranks every password you type afterwards

## The One Click That Outranks Every Password

**Tags:** `core idea` (blue), `read all sites` (orange), `permission prompts` (green)

- **The install** — Alice adds a small currency-converter extension; it asks to read and change data on all sites
- **One click** — she clicks Accept once, and the grant applies to every page she opens from then on
- **What it means** — all-sites host access lets it read and modify page content and read those sites' cookies
- **Inside the page** — its script runs in her webmail and her bank dashboard, not outside them looking in
- **After TLS** — HTTPS protects the trip to the server; the extension sees the text after it is decrypted
- **After login** — the password and MFA already passed, so it acts inside an already-authenticated session
- **Same-origin** — the rule keeping her bank and her webmail apart does not bind an all-sites extension

*Example (italic):* The converter never asks again: on Monday it reads a shop's price tag, on Tuesday the same script is running inside her payroll page.

**Key point:** A host permission is a standing grant to run inside those sites' own pages — which places it downstream of HTTPS, her password, her MFA, and the same-origin policy, because all four are already satisfied by the time page code runs.

### Visualization (canvas `c1`, 720×300)

Layered diagram: four controls drawn as gates on the left, all already satisfied, with the extension sitting inside the page context on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Extension Sits: Downstream of Every Control".
- **Gate labels (12px `#2c3e50`, left-aligned at x=30, at y = 95, 140, 185, 230):** "HTTPS: already decrypted", "password: already accepted", "MFA: already passed", "same-origin: exempt by grant"; each followed by a 2px `#6b7280` arrow from x=218 to x=271 at the same y, with a 9px arrowhead.
- **Page-context box:** rounded rect (275, 60) to (690, 265), 8px radius, fill `rgba(42,120,214,0.07)`, 2px `#2a78d6` border; bold 13px `#1a5276` label "inside the page — TLS decrypted, session logged in" at (290, 82), left-aligned.
- **Inner box A (the data):** rounded rect (300, 100)–(660, 148), fill `rgba(25,158,112,0.10)`, 2px `#199e70` border, centered 12px `#2c3e50` text "page content, form fields, cookies".
- **Inner box B (the extension):** rounded rect (300, 178)–(660, 238), fill `rgba(217,89,38,0.12)`, 2px `#d95926` border, centered bold 12px `#d95926` "extension with all-sites grant" at y=203 and 12px `#2c3e50` "reads and modifies both" at y=223.
- **Link:** 3px `#d95926` double-headed vertical arrow at x=480 between y=178 and y=148 (arrowheads at both ends, 9px).
- **Annotation (bold 13px `#4a3aa7`, left-aligned at x=30, y=270):** "all four already satisfied".
- **Caption (12px `#444`, bottom right):** "schematic".

## Counting the Blast Radius of One Week

**Tags:** `worked example` (blue), `blast radius` (orange)

- **The profile** — Alice visits 40 distinct sites in one week, and logs into 6 of them (illustrative profile)
- **Scoped version** — an on-click, single-site grant gives the extension reach into 1 of those 40 sites
- **The ratio** — 40 ÷ 1 = 40× the reach, for a tool whose job needs exactly one page at a time
- **High-value share** — 6 ÷ 40 = 15% of the sites carry the sessions worth stealing: mail, bank, payroll
- **Typed that week** — she types her password at 4 of the 6; the other 2 open on a stored session cookie
- **Both paths** — 4 typed credentials + 2 cookie-carried sessions = all 6 authenticated accounts reachable
- **The other 34** — 40 − 6 = 34 sites hold nothing valuable, yet the same single grant covers them too

*Example (italic):* The converter is genuinely useful on one shopping page; the grant that makes it work there also runs it inside the payroll page (profile illustrative).

**Key point:** A permission decides blast radius, not intent — the same click that covers the 1 site the tool needs covers 40, including the 6 that hold her money.

### Visualization (canvas `c2`, 720×300)

Bar chart: sites reachable under a one-site grant versus an all-sites grant, with the all-sites bar split into authenticated and other sites.

- **Title (bold 15px, `#1a5276`, top center):** "One Grant, Two Reaches: 1 Site vs All 40".
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 175; y = distinct sites 0 to 40, so 1 site = 4.375px; gridlines `#e5e9ef` at 10/20/30/40 with 12px `#444` right-aligned tick labels ending at x=84; x-axis 2px `#999`.
- **Bar 1 (90px wide, centered x=230):** height 1 × 4.375 = 4.4px, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; bold 12px `#1a5276` value "1 site" centered at 14px above the bar top; 12px `#444` category label "one-site grant (on click)" centered at y=265.
- **Bar 2 (90px wide, centered x=470), stacked:** lower segment 34 sites = 148.75px from y=96.25 to y=245, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; upper segment 6 sites = 26.25px from y=70 to y=96.25, fill `rgba(217,89,38,0.45)`, 2px `#d95926` border; bold 12px `#1a5276` value "40 sites" centered at y=60; 12px `#444` category label "all-sites grant" centered at y=265.
- **Segment labels:** 12px `#1a5276` "34 other sites" centered at (470, 175); bold 12px `#d95926` "6 authenticated = 15%" left-aligned at (522, 88).
- **Annotation (bold 13px `#d95926`, left-aligned at x=120, y=125):** "40 ÷ 1 = 40× the reach".
- **Axis note (12px `#444`, left-aligned at x=90, y=288):** "distinct sites Alice visits in one week".
- **Caption (12px `#444`, bottom right):** "browsing profile illustrative".

## The Grant Outlives the Author

**Tags:** `where it's used` (blue), `extension resale` (orange), `defensive` (green)

- **Granted once** — the prompt appears at install; every later version inherits that answer silently
- **Auto-update** — extensions update themselves, so today's reviewed code is not tomorrow's running code
- **Change of owner** — popular extensions get sold, and the install base with its grants is the asset
- **Store review** — a filter on the version submitted, not a proof about the versions that ship later
- **Prefer on-click** — grant access on click, or to named sites, so the default reach is nothing at all
- **Audit** — walk the installed list periodically and remove anything whose job you no longer do
- **Separate profile** — keep one browser profile with no extensions for banking and payroll work

*Example (italic):* Alice accepted v1.0 from an author she had read about; v2.0 arrives eleven months and one ownership change later, running under the very same grant.

**Key point:** The decision is not about the code she installed — it is about every version that will install itself later under a permission she can no longer be asked about.

### Visualization (canvas `c3`, 720×300)

Timeline: install grant, two auto-updates, and an ownership change, with the permission band unchanged beneath all of them.

- **Title (bold 15px, `#1a5276`, top center):** "The Permission Stays While Everything Above It Changes".
- **Top annotation (bold 13px `#4a3aa7`, centered, y=48):** "she reviewed one version; the grant applies to all of them".
- **Axis:** 2px `#999` horizontal line at y=180 from x=70 to x=680.
- **Events (markers = filled circles r=7 at y=180, centered two-line 12px labels above at y=140 and y=157):**
  - x=110, `#008300`: "install" / "v1.0 accepted"
  - x=280, `#2a78d6`: "auto-update" / "v1.4"
  - x=450, `#d55181`: "ownership" / "changes hands"
  - x=620, `#4a3aa7`: "auto-update" / "v2.0"
- **Permission band:** rounded rect (100, 200) to (650, 228), 6px radius, fill `rgba(217,89,38,0.15)`, 2px `#d95926` border, centered bold 12px `#d95926` text "read & change data on all sites — unchanged" at y=219.
- **Annotation (bold 13px `#d95926`, centered, y=254):** "the grant travels with the code, across versions and owners".
- **Caption (12px `#444`, bottom right):** "version timeline illustrative".

## Trusting the Author vs Trusting the Permission

**Tags:** `common mistake` (red), `standing grant` (orange)

- **The confusion** — people judge whether the author looks trustworthy today, at the moment of the prompt
- **The real question** — is this reach safe in the hands of whoever owns and updates it for five years
- **Do the count** — monthly updates for 5 years = 5 × 12 = 60 versions, of which she reviewed 1
- **The share** — 1 ÷ 60 = 1.7% of the versions running under her grant were the version she evaluated
- **Standing grant** — a permission is not a one-time transaction; it is a door left open by default
- **"It's just a small tool"** — usefulness is irrelevant: reach is set by the permission, not the feature
- **Better test** — ask what breaks if it only runs on one site on click; often the honest answer is nothing

*Example (italic):* Of 60 versions that will run under one Accept click, 59 arrive without ever showing her a prompt again (monthly cadence assumed, illustrative).

**Common mistake:** Evaluating the author instead of the permission. The author can change, the code changes monthly, and the grant does not — so size the grant to the smallest job the tool actually does.

### Visualization (canvas `c4`, 720×300)

Grid of version blocks: 60 monthly versions over five years, one marked as reviewed and 59 unreviewed.

- **Title (bold 15px, `#1a5276`, top center):** "One Version Reviewed, Sixty Versions Trusted".
- **Top annotation (bold 13px `#d55181`, centered, y=52):** "the other 59 arrive automatically".
- **Grid:** 5 rows × 12 columns = 60 blocks, each 34×24; column i left edge x = 90 + i × 44 (i = 0..11), row j top edge y = 70 + j × 40 (j = 0..4).
- **Block styling:** the first block (row 0, column 0) fill `rgba(0,131,0,0.35)` with 2px `#008300` border; the remaining 59 fill `rgba(107,114,128,0.10)` with 1.5px `#6b7280` border.
- **Row labels (12px `#444`, right-aligned ending at x=82, vertically centered on each row):** "year 1" … "year 5".
- **Legend (bold 12px `#008300`, left-aligned at x=90, y=285):** "reviewed at install: 1 of 60 = 1.7%".
- **Caption (12px `#444`, bottom right):** "monthly update cadence assumed, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundBoxTL` and `arrowHead` as in the other security pages.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are hardcoded (no randomness). The browsing profile (40 distinct sites, 6 authenticated, 4 passwords typed, 2 cookie-carried sessions) and the 60-version cadence are invented and labeled illustrative. Every derived figure is the stated arithmetic and must match text and chart to the digit: 40 ÷ 1 = 40×; 6 ÷ 40 = 15%; 4 + 2 = 6; 40 − 6 = 34; 5 × 12 = 60; 1 ÷ 60 = 1.7%; 60 − 1 = 59.
- **Technical accuracy:** the page claims only what all-sites host permissions actually allow — reading and modifying page content and reading cookies for those sites. It does not imply access outside the browser. Scope is a granted hole, not a sandbox defect; sandbox/isolation theory belongs to the sandboxing page.
- **Framing:** defensive/educational; no named real extensions, browsers, store policies, or incidents; no example password or token strings; people are Alice and Bob.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
