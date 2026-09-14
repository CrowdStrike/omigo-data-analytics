# VPNs & Zero Trust

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** VPNs & Zero Trust

**Subtitle:** A VPN builds a wall around the network and checks you once at the gate; zero trust drops the wall and checks every single request instead

## The Office Moat and the Coffee Shop Laptop

**Tags:** `core idea` (blue), `castle-and-moat` (orange), `remote access` (green)

- **The office** — a company runs 40 internal servers behind one firewall; anything inside is trusted by default
- **The moat** — the firewall blocks all outside traffic; the only door in is the VPN gateway
- **The tunnel** — an analyst at a coffee shop opens an encrypted VPN tunnel and is now "inside" the wall
- **The catch** — once inside, that laptop can reach all 40 servers, exactly like a desk plugged into the office wall
- **Zero trust** — the alternative model: there is no inside; every request to every server must prove who is asking

*Example (italic):* The analyst connects over VPN from the coffee shop, and the orders database, payroll server, and 38 other machines all accept the laptop's packets without asking who is typing.

**Key point:** Castle-and-moat security authenticates you once at the network edge and trusts you everywhere after; zero trust authenticates and authorizes every request, treating network location as meaningless.

### Visualization (canvas `c1`, 720×300)

Two-panel diagram: the moat model (one gate check, free movement inside the wall) vs the zero-trust model (no wall, a lock on every server).

- **Title (bold 15px, `#1a5276`, top center):** "One Check at the Gate vs a Check at Every Door".
- **Left panel (x 30–345):** 12px `#6b7280` panel label "castle-and-moat" at (40, 60); a rounded wall rectangle (x=60, y=80, w=260, h=185, 3px `#1a5276` stroke, fill `rgba(42,120,214,0.06)`) containing a 2×3 grid of six server boxes (60×32px, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` stroke, 11px `#2c3e50` labels "srv 1"…"srv 5" and "…40"); a gap in the left wall edge at y≈160 with a green `#008300` box labeled "VPN gate ✓ once" (12px bold); a laptop box at x=15, y=150 (11px label "laptop") with a 3px blue `#2a78d6` arrow through the gate; inside, three thin dashed `#6b7280` arrows fanning from the gate to server boxes.
- **Right panel (x 375–690):** 12px `#6b7280` panel label "zero trust" at (385, 60); the same 2×3 grid of six server boxes but no wall; each box gets a small aqua `#199e70` padlock glyph (drawn as a 8×7px rect + arc) at its top-right corner; the laptop box at x=360, y=150 with three separate 2px arrows to three boxes, each arrow crossing a bold 11px aqua "✓ auth" label; the other three boxes marked with 11px `#6b7280` "no access".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=285):** "the wall trusts location; zero trust verifies identity per request".
- **Caption (12px `#444`, bottom right):** "6 boxes drawn, 40 servers implied — schematic".

## One Stolen Laptop, Two Very Different Blast Radii

**Tags:** `worked example` (blue), `blast radius` (red)

- **The setup** — the company has 40 internal servers; the analyst's job needs exactly 3 of them
- **The theft** — the laptop, with its VPN session still live, is stolen off the coffee shop table
- **Moat count** — the gate was already passed, so the thief can probe all 40 of 40 servers
- **Zero-trust count** — every server re-checks identity and role, so the thief reaches only the analyst's 3
- **Hand-check** — 40 reachable vs 3 reachable: 37 servers never see the attacker, a 13× smaller blast radius

*Example (italic):* Under castle-and-moat the thief reaches 40 of 40 servers; under zero trust the same theft exposes 3 of 40, and the other 37 simply refuse the connection.

**Key point:** The worked comparison is countable: same theft, same laptop — 40 servers exposed under one-time gate trust, 3 under per-request checks, because access follows the role, not the network.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: servers reachable by the thief under each model, against the 40-server total.

- **Title (bold 15px, `#1a5276`, top center):** "Servers the Thief Can Reach: 40 vs 3 (of 40 Total)".
- **Axes:** origin x=90, baseline y=245, plot width 540, plot height 180; y = servers 0 to 40 with gridlines `#e5e9ef` at 10/20/30/40 and 12px `#444` tick labels; dashed 2px `#6b7280` (dash 4/3) reference line across the plot at y for 40, labeled "all 40 servers" (12px `#6b7280`, right end).
- **Bars (120px wide, centered at x=250 and x=470):** magenta `#d55181` bar for `castle-and-moat = 40` (fill `rgba(213,81,129,0.30)`, 2px `#d55181` stroke); green `#008300` bar for `zero trust = 3` (fill `rgba(0,131,0,0.30)`, 2px stroke); bold 13px value labels "40" and "3" above each bar top; 12px `#444` category labels under the baseline.
- **Annotation (bold 13px green `#008300`, near x=430, y=110):** "37 servers never see the attacker".
- **Caption (12px `#444`, bottom right):** "server counts illustrative".

## Remote Work Made the Moat Obsolete

**Tags:** `where it's used` (blue), `lateral movement` (red), `defense in depth` (green)

- **Remote work** — when half the staff works from coffee shops and homes, "inside the building" stops meaning anything
- **Lateral movement** — attackers rarely stop at the first machine; they hop sideways to whatever the network allows
- **Containment** — per-request checks turn one breached laptop into a contained incident instead of a full takeover
- **Defense in depth** — the VPN's encryption stays useful; zero trust adds checking layers behind it, not instead of it
- **The trend** — cloud apps already work this way: every API call carries its own credentials, no matter where it comes from

*Example (italic):* On the flat moat network an attacker who lands on one laptop reaches all 40 servers within a workday; behind per-request checks the count stays stuck at the analyst's 3.

**Key point:** Zero trust matters because it limits lateral movement — the sideways hopping that turns a single phished laptop into a company-wide breach — while still letting remote work happen from anywhere.

### Visualization (canvas `c3`, 720×300)

Line chart of an intrusion's first workday: servers reached over time on a flat moat network vs under zero-trust checks.

- **Title (bold 15px, `#1a5276`, top center):** "Lateral Movement Over One Workday: Flat Network vs Zero Trust".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hours since break-in 0 to 8, 12px `#444` tick labels every 2 hours ("0h"–"8h"); y = servers reached 0 to 40, gridlines `#e5e9ef` at 10/20/30/40.
- **Flat-network line:** magenta `#d55181` 3px line through hours `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, servers `[1, 5, 12, 22, 31, 38, 40, 40, 40]` — steep climb, saturating at all 40 by hour 6.
- **Zero-trust line:** green `#008300` 3px line through the same hour grid, servers `[1, 2, 3, 3, 3, 3, 3, 3, 3]` — plateaus at the analyst's 3 by hour 2.
- **Labels:** bold 12px magenta "flat network" near (2.5h, y of 25); bold 12px green "zero trust" near (5h, y of 6).
- **Annotation (bold 13px green `#008300`, near hour 5.5, y=100):** "containment: stuck at 3 servers all day".
- **Caption (12px `#444`, bottom right):** "hop counts illustrative".

## Being on the Network Is Not Being Logged In

**Tags:** `common mistake` (red), `authentication vs authorization` (orange)

- **The confusion** — treating a VPN connection as a login; it only proves the device reached the gate once
- **Encrypt ≠ authorize** — the tunnel hides traffic from outsiders; it says nothing about what the user may touch
- **The IP trap** — a server that allows "any internal address" quietly grants access to everyone the VPN admits
- **Not either/or** — zero trust doesn't ban VPNs; it bans treating "inside the tunnel" as permission
- **The fix** — each service asks who is calling and what their role allows, on every single request

*Example (italic):* A payroll server configured to trust any internal address answers every VPN user alike — analyst, intern, or the thief holding the stolen laptop.

**Common mistake:** "We have a VPN, so we're secure." The VPN answers "can this device reach the network?" — it never answers "should this person read payroll?" Those are two different questions, and only the second protects the data.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same payroll request under an internal-IP trust rule (served) vs under a per-request identity check (denied).

- **Title (bold 15px, `#1a5276`, top center):** "Same Request, Two Rules: Internal IP vs Verified Identity".
- **Row 1 (y=95), label 12px `#444` at x=20:** "IP trust rule"; blue `#2a78d6` rounded box at x=150 labeled "stolen laptop, on VPN" (12px), 3px arrow to an ink `#1a5276` box at x=350 labeled "payroll: internal IP? yes", 3px arrow to a magenta `#d55181` box at x=560 labeled "salary data returned" with bold 12px magenta "✗ breach".
- **Row 2 (y=205), label:** "zero-trust rule"; the same blue box "stolen laptop, on VPN", 3px arrow to an ink box at x=350 labeled "payroll: who are you? role?", 3px arrow to a green `#008300` box at x=560 labeled "request denied" with bold 12px green "✓ contained".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(26,82,118,0.10)` / `rgba(213,81,129,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "reachable is not the same as allowed".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 40-server company, the analyst's 3 permitted servers, the 40-vs-3 blast-radius bars, and the lateral-movement curves `[1,5,12,22,31,38,40,40,40]` vs `[1,2,3,3,3,3,3,3,3]` are invented and labeled illustrative; the derived figures (37 servers spared, 13× smaller) follow arithmetically from 40 and 3.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
