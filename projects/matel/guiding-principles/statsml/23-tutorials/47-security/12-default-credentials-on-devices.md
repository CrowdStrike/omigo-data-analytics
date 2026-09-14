# Default Credentials on Devices

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Default Credentials on Devices

**Subtitle:** The router, printer, and camera all shipped with the same admin login — and nobody changed it

## The Login Was Printed in the Manual

**Tags:** `core idea` (blue), `worked example` (orange), `defensive` (green)

- **The router** — Alice's home router serves an admin page that still accepts the login printed in its manual
- **The manual** — that manual is a public download on the vendor's support site, so the "secret" ships pre-published
- **The list** — documented factory logins get collected, model by model, into freely circulating lookup lists
- **Same for the rest** — the office printer and the network camera each arrived with their own documented login
- **Not a guess** — nothing is being cracked here; the attacker reads the published answer and types it in
- **The definition** — a default credential is the factory login a device ships with, identical on every unit sold

*Example (italic):* Alice's router admin page opens on the first try with the `admin/admin` placeholder its own manual documents on page 4.

**Key point:** A factory login is documented, identical across units, and therefore public — it is a placeholder, not a secret, and it protects nothing until somebody changes it.

### Visualization (canvas `c1`, 720×300)

Flow diagram: a published manual feeds an aggregated login list, which feeds an internet-wide scanner, which fans out to a router, a printer, and a camera.

- **Title (bold 15px, `#1a5276`, top center):** "The Factory Login Is Published, Aggregated, Then Replayed".
- **Manual box:** violet-tinted rounded box at x=30, y=58, 165×52, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, two centered lines 12px `#2c3e50`: "product manual" / "(public download)".
- **List box:** yellow-tinted rounded box at x=30, y=172, 165×52, fill `rgba(201,133,0,0.14)`, 2px `#c98500` border, two centered lines 12px `#2c3e50`: "default-login list" / "e.g. admin/admin".
- **Arrow manual → list:** 3px `#4a3aa7` vertical line from (112, 110) to (112, 166) with arrowhead pointing down.
- **Scanner box:** blue rounded box at x=258, y=114, 170×56, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, two centered lines 12px `#2c3e50`: "internet-wide scanner" / "tries the documented login".
- **Arrows into scanner:** 3px `#6b7280` lines from (195, 84) and (195, 198) to the scanner's left edge at (254, 142), each with an arrowhead.
- **Device boxes:** three aqua rounded boxes at x=492, each 180×44, fill `rgba(25,158,112,0.14)`, 2px `#199e70` border, at y=48 "home router", y=120 "office printer", y=192 "network camera", centered 12px `#2c3e50`.
- **Fan-out arrows:** 3px `#199e70` lines from the scanner's right edge (428, 142) to each device box's left edge (488, y+22), each with an arrowhead.
- **Box style:** 8px corner radius, centered text, two lines where noted.
- **Annotation (bold 13px magenta `#d55181`, x=30, y=258):** "the login was public on the day the device shipped".
- **Caption (12px `#444`, bottom right):** "generic devices, illustrative".

## Sweeping Every Address in Under Half a Day

**Tags:** `worked example` (blue), `rule of thumb` (orange)

- **The address space** — every classic internet address fits in 32 bits, so 2^32 = 4,294,967,296 addresses exist
- **The assumed rate** — take a distributed scanner sending 100,000 probes per second at one admin port
- **The sweep time** — 4,294,967,296 / 100,000 = 42,949.7 seconds, and 42,949.7 / 3,600 = 11.9 hours
- **Faster is cheaper** — at 1,000,000 probes per second the same full sweep closes in 4,295 seconds, 1.2 hours
- **Slow still finishes** — even 10,000 probes per second completes in 429,497 seconds, just under 5 days
- **What it buys** — one sweep enumerates every reachable device, so no device is too small or dull to be found

*Example (italic):* A sweep of the entire address space at 100,000 probes per second starts after dinner and finishes before lunch — 11.9 hours (scan rate assumed, illustrative).

**Key point:** Enumerating the whole internet costs hours, not months — which makes a default-credential attack a sweep of everything rather than a search for anybody in particular.

### Visualization (canvas `c2`, 720×300)

Bar chart: time to sweep all 4,294,967,296 addresses at four assumed probe rates, bar heights on a log time scale, every label computed at render time.

- **Title (bold 15px, `#1a5276`, top center):** "Time to Probe All 4,294,967,296 Addresses Once".
- **Data (hardcoded rates):** `[10000, 100000, 1000000, 10000000]`; seconds computed in JS as `Math.pow(2, 32) / rate`, giving 429,496.7 / 42,949.7 / 4,295.0 / 429.5 seconds.
- **Axes:** origin x=70, baseline y=235, plot width 575, plot height 170; height per bar = `(log10(sec) − 1.5) / 4.5 × 170`, giving about 156 / 118 / 81 / 43 px.
- **Gridlines (`#e5e9ef`, 1px)** at 60 s, 3,600 s and 86,400 s with right-aligned 12px `#444` labels "1 min", "1 hour", "1 day"; x-axis 2px `#999`.
- **Bars (76px wide, centered at x = 130, 275, 420, 565):** the 100,000/s bar is emphasized — fill `rgba(42,120,214,0.45)`, 2px `#2a78d6` border; the other three fill `rgba(107,114,128,0.22)` with 2px `#6b7280` border.
- **Value labels (bold 12px, `#1a5276` on the emphasized bar, `#6b7280` on the rest), centered 8px above each bar:** formatted in JS — ≥1 day as "N.NN days", ≥1 hour as "N.N hours", else "N.N min", yielding "4.97 days", "11.9 hours", "1.2 hours", "7.2 min".
- **X labels (12px `#444`, below baseline):** "10 K/s", "100 K/s", "1 M/s", "10 M/s"; axis title 12px `#444` centered at y=273: "assumed probe rate (probes per second)".
- **Annotation (bold 13px orange `#d95926`, left-aligned at x=300, y=62):** "one full sweep, overnight".
- **Caption (12px `#444`, bottom right):** "scan rates assumed, illustrative".

## The Devices Nobody Counts as Computers

**Tags:** `where it's used` (blue), `forgotten devices` (orange), `defensive` (green)

- **Who answers** — assume 20,000,000 of the swept addresses answer at all on a device admin port
- **Share responding** — 20,000,000 / 4,294,967,296 = 0.47% of the address space replies to the probe
- **Still factory** — if 3% of those responders were never changed, 20,000,000 × 0.03 = 600,000 open devices
- **Opened by accident** — automatic port-opening on the router (UPnP) publishes a device nobody meant to expose
- **Not seen as computers** — printers and cameras run web servers and logins, yet never reach the patch list
- **The reset trap** — "we changed it at setup" silently unwinds the moment a factory reset restores the default

*Example (italic):* 600,000 devices is a rounding error as a percentage and a large working fleet to whoever swept them up (response and factory-login rates illustrative).

**Key point:** The yield is tiny as a rate and enormous as a count — and the largest share of it sits in printers and cameras that nobody ever put on an inventory.

### Visualization (canvas `c3`, 720×300)

Three-step horizontal funnel from the full address space to devices still holding factory logins, bar lengths log-scaled, both percentages computed at render time.

- **Title (bold 15px, `#1a5276`, top center):** "4,294,967,296 Addresses Swept → 600,000 Still on Factory Logins".
- **Data (hardcoded):** `[4294967296, 20000000, 600000]` with left labels "addresses swept", "answer on admin port", "still on factory login".
- **Bars (26px tall, starting at x=300, top edges at y = 78, 138, 198):** length = `(log10(v) − 4) / 6 × 300`, giving about 282 / 165 / 89 px; fills `rgba(42,120,214,0.35)`, `rgba(201,133,0,0.35)`, `rgba(217,89,38,0.40)` with 2px borders `#2a78d6`, `#c98500`, `#d95926`.
- **Row labels:** right-aligned 12px `#444` ending at x=290, vertically centered on each bar.
- **Value labels:** bold 12px in each bar's border color, 8px right of the bar end, from `v.toLocaleString('en-US')` — "4,294,967,296", "20,000,000", "600,000".
- **Derived percentage notes (12px `#6b7280`, 8px below bars 2 and 3, left-aligned at x=302):** computed in JS as `20000000 / 4294967296` → "0.47% of the sweep answers" and `600000 / 20000000` → "3% of responders never changed".
- **Scale note (12px `#6b7280`, left-aligned at x=300, y=252):** "bar length is log-scaled".
- **Annotation (bold 13px orange `#d95926`, left-aligned at x=300, y=52):** "a tiny rate, a huge count".
- **Caption (12px `#444`, bottom right):** "response and factory-login rates illustrative".

## "Nobody Would Bother With My Router"

**Tags:** `common mistake` (red), `economics` (orange)

- **The belief** — "nobody would bother targeting my router" assumes somebody chose it, which never happened
- **Untargeted** — the router was enumerated by an indiscriminate sweep, and being obscure is not being skipped
- **No cost per host** — one more address costs the scanner a fraction of a second, so nothing gets filtered out
- **It sits in front** — every laptop, phone, camera, and printer on the network reaches the internet through it
- **What that grants** — admin access there means name-lookup changes, traffic redirection, and a foothold inside
- **The fix** — change the factory login, re-check it after every reset, and keep admin pages off the internet

*Example (italic):* Alice was never a target; her router was address number 3,001,447,982 in a sweep that visited all of them.

**Common mistake:** Obscurity is not a defense against enumeration — the sweep visits every address, and the router is the most valuable box on the network precisely because everything else sits behind it.

### Visualization (canvas `c4`, 720×300)

Two panels: a dot grid where every address is probed with one marked as the reader's router, and a schematic of the router sitting in front of four devices.

- **Title (bold 15px, `#1a5276`, top center):** "Not Chosen, Just Enumerated — and It Fronts Everything Else".
- **Left panel header (bold 12px `#2c3e50`, x=50, y=58):** "one indiscriminate sweep".
- **Dot grid:** 20 columns × 9 rows, 3.5px radius, x from 50 in steps of 15, y from 70 in steps of 14 (grid spans x 50–335, y 70–182), all filled `rgba(42,120,214,0.45)`.
- **Marked dot:** last row, 12th column (x=215, y=182), 5.5px radius, filled `#d95926`; 2px `#d95926` leader from (215, 190) to (215, 202); label bold 12px `#d95926` centered at (215, 216): "your router".
- **Grid note (12px `#6b7280`, centered at x=192, y=242):** "180 dots drawn; the sweep probes all 4,294,967,296".
- **Right panel header (bold 12px `#2c3e50`, x=392, y=58):** "everything is behind it".
- **Router box:** blue rounded box at x=392, y=118, 132×46, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, centered 12px `#2c3e50` "home router".
- **Device boxes:** four aqua rounded boxes at x=576, each 112×30, fill `rgba(25,158,112,0.14)`, 2px `#199e70` border, at y=62 "laptop", y=104 "phone", y=146 "camera", y=188 "printer", centered 12px `#2c3e50`.
- **Arrows:** 2px `#199e70` lines from (524, 141) to each device box's left edge (572, y+15) with arrowheads.
- **Box style:** 8px corner radius, centered 12px text.
- **Annotation (bold 13px violet `#4a3aa7`, left-aligned at x=392, y=242):** "admin here means the whole network".
- **Caption (12px `#444`, bottom right):** "schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red reserved for genuine alarm states; this page uses orange instead.
- **Data and arithmetic:** no randomness anywhere; the only hardcoded inputs are the probe rates `[10000, 100000, 1000000, 10000000]` and the funnel counts `[4294967296, 20000000, 600000]`. The address-space total is computed as `Math.pow(2, 32)` = 4,294,967,296; sweep durations as `2^32 / rate`; both funnel percentages as their shown numerator over their shown denominator (`20000000/4294967296` = 0.47%, `600000/20000000` = 3%). Every number printed beside a bar is formatted from the computed value at render time, and the prose figures (11.9 hours, 42,949.7 s, 4,295 s, 429,497 s, 0.47%, 3%, 600,000) match those computed values to the digit.
- **Assumptions labeled:** the 100,000 probes/s scan rate, the 20,000,000 responders, and the 3% never-changed share are stated assumptions, marked illustrative in the captions and prose.
- **Framing:** defensive/educational — the page explains why default credentials are a mass-scanning economics problem so readers change factory logins and keep admin pages unexposed; no operational attack guidance, no real vendors, models, or realistic credential strings beyond the generic `admin/admin` placeholder.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
