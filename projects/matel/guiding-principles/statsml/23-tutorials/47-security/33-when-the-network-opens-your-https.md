# When the Network Opens Your HTTPS

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** When the Network Opens Your HTTPS

**Subtitle:** Corporate inspection works by holding a certificate your laptop already trusts — so the padlock now covers a middlebox that sees everything in the clear

## Alice's Padlock and the Certificate Her Laptop Was Told to Trust

**Tags:** `core idea` (blue), `trusted root` (orange), `sanctioned MITM` (red)

- **The browse** — Alice's work laptop loads an external HTTPS site; the padlock appears and the certificate validates
- **The middle** — her employer's inspection tier terminates that session, decrypts it, examines it, re-encrypts it onward
- **The enabler** — the company installed its own root certificate into the laptop's trust store as an authority
- **The signature** — so any certificate the middlebox mints, for any site name, is accepted without a warning
- **The definition** — TLS inspection is a deliberate, disclosed man-in-the-middle run by the device's owner, not an attacker
- **What changed** — the padlock now attests the hop to the middlebox, not the whole trip to the site

*Example (italic):* Alice sees a valid certificate for an external site, but it was issued seconds earlier by her employer's own authority — the real site's certificate was checked by the middlebox, not by her browser.

**Key point:** TLS inspection reuses the exact mechanics of a man-in-the-middle attack, with one difference: the trust store was configured to accept it. That configuration is what turns an attack into a control.

### Visualization (canvas `c1`, 720×300)

Before/after connection diagram: one end-to-end encrypted tunnel above, two tunnels meeting at a plaintext inspection point below, with the installed root certificate drawn as the enabling component.

- **Title (bold 15px, `#1a5276`, top center):** "One Tunnel, or Two Tunnels Meeting in the Middle".
- **Row 1 label (bold 12px `#6b7280`, left at x=60, y=48):** "before: one tunnel, end to end".
- **Row 1 boxes (y=60, height 42, 8px radius, 12px `#2c3e50` centered text):** blue box x=60 w=130 "Alice's laptop", fill `rgba(42,120,214,0.15)` border `#2a78d6`; blue box x=530 w=130 "external site", same style. Green `#008300` 3px double-headed arrow from x=190 to x=526 at y=81; 12px `#008300` label "single encrypted tunnel" centered at (358, 72).
- **Row 2 label (bold 12px `#6b7280`, x=60, y=140):** "after: two tunnels, plaintext where they meet".
- **Row 2 boxes (y=152, height 42):** blue laptop box x=60 w=130 "Alice's laptop"; orange box x=285 w=170, fill `rgba(217,89,38,0.14)` border `#d95926`, two lines 12px "inspection tier" (baseline 170) / "traffic in the clear here" (baseline 187); blue site box x=530 w=130 "external site".
- **Row 2 arrows:** green 3px double-headed arrows x=190→281 and x=455→526 at y=173; 12px `#008300` label "encrypted" centered at (235, 163) and (490, 163).
- **Root certificate box:** yellow rounded box x=60 y=222 w=190 h=44, fill `rgba(201,133,0,0.14)` border `#c98500`, 11px `#2c3e50` two lines "company root certificate" (baseline 240) / "trusted by the laptop" (baseline 256); dashed 2px `#c98500` (dash 4/3) arrow from (155, 222) to (125, 198) pointing at the laptop box.
- **Annotation (bold 13px violet `#4a3aa7`, left-aligned at x=300, y=245):** "trust here = a valid padlock for any site".
- **Caption (12px `#444`, bottom right):** "illustrative schematic".

## Counting the Plaintext: 480,000 Sessions at One Point

**Tags:** `worked example` (blue), `concentration risk` (orange)

- **The deployment** — 4,000 employees, 120 encrypted sessions each per workday, all routed through one inspection tier
- **Daily total** — 4,000 × 120 = 480,000 sessions decrypted at that single component every workday
- **Yearly total** — 480,000 × 250 workdays = 120,000,000 sessions that passed through it in the clear
- **Before inspection** — each session was readable at 2 endpoints, the user's device and the destination server, and at 0 points between
- **After inspection** — exactly 1 new location exists, but it sees all 480,000 instead of one participant's share
- **Per endpoint** — 480,000 ÷ 4,000 = 120 sessions per laptop, against 480,000 at the tier: 4,000× the concentration
- **The lesson** — the risk is not that decryption happens; it is that it happens in one place

*Example (italic):* Alice's laptop can leak 120 sessions a day; the inspection tier can leak 480,000 — the same exposure, concentrated 4,000-fold into one component (illustrative deployment).

**Key point:** Inspection adds one interception point, not many — and that is precisely the problem. Concentration converts a distributed exposure into a single catastrophic one.

### Visualization (canvas `c2`, 720×300)

Horizontal log-scale bar chart: sessions in plaintext per workday at one user's laptop (120) versus at the inspection tier (480,000).

- **Title (bold 15px, `#1a5276`, top center):** "Where the Plaintext Sits: 120 per Laptop vs 480,000 at One Tier".
- **Axis:** log10 scale, origin x=170 (=100 sessions) to x=690 (=1,000,000), 130px per decade; baseline y=240 drawn 2px `#999`. Gridlines 1px `#e5e9ef` from y=70 to y=240 at x = `[170, 300, 430, 560, 690]` with 12px `#444` centered tick labels "100", "1,000", "10,000", "100,000", "1,000,000" at y=258.
- **Bar geometry:** width in px = `(Math.log10(v) - 2) * 130`, so 120 → 10.3px and 480,000 → 478.6px (computed in JS from the hardcoded values, not hardcoded widths).
- **Row 1 (bar y=100, height 26):** right-aligned 12px `#444` label "one user's laptop" ending at x=160; fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; bold 12px `#2a78d6` value "120 sessions/day" at bar right + 8.
- **Row 2 (bar y=160, height 26):** right-aligned 12px `#444` label "the inspection tier" ending at x=160; fill `rgba(217,89,38,0.35)`, 2px `#d95926` border; bold 12px `#d95926` value "480,000 sessions/day" drawn *inside* the bar, right-aligned at bar right − 10 (the bar ends near x=649, so an outside label would overflow the canvas).
- **Annotation (bold 13px orange `#d95926`, left-aligned at x=300, y=212):** "4,000× the concentration (480,000 ÷ 120)".
- **Axis note (12px `#6b7280`, centered at x=430, y=278):** "sessions decrypted per workday — log scale".
- **Caption (12px `#444`, bottom right, y=294):** "illustrative: 4,000 employees × 120 sessions".

## The Trade Both Sides Are Actually Making

**Tags:** `where it's used` (blue), `benefits` (green), `costs` (orange)

- **Opaque traffic** — malware delivery and data exfiltration cannot be inspected inside a channel nobody can read
- **Obligations** — data-loss-prevention and regulatory duties often make the managed device the only place to enforce them
- **Mid-session blocking** — a request to a known-malicious destination can be stopped while the session is still open
- **End to middle to end** — the property that made TLS valuable is deliberately removed on the inspected path
- **A crown-jewel target** — one component holds plaintext for every user and destination, regardless of anyone's intentions
- **Verification disabled** — Alice cannot tell a legitimate certificate from a forged one, so the padlock stops carrying information
- **A documented defect class** — some inspection products have validated upstream certificates less strictly than browsers do
- **Pinning breaks** — certificate pinning and mutual-TLS connections fail, and the resulting exemption list becomes policy

*Example (italic):* If the tier accepts a bad upstream certificate and still presents a good one downstream, Alice ends up less protected than with no inspection at all — a product-defect class, not a claim about any specific system.

**Key point:** Both columns are real. Inspection genuinely buys visibility that opaque traffic denies, and it genuinely spends end-to-end assurance, user verification, and concentration risk to buy it.

### Visualization (canvas `c3`, 720×300)

Balanced two-column panel: what inspection buys on the left, what it costs on the right, with a matching footnote under each column.

- **Title (bold 15px, `#1a5276`, top center):** "A Trade, Not a Free Addition".
- **Divider:** 1px `#e5e9ef` vertical line at x=365 from y=45 to y=285.
- **Column headers (bold 13px, y=62):** "what inspection buys" in `#008300` at x=60; "what it costs" in `#d95926` at x=390 (both left-aligned).
- **Left rows (12px `#2c3e50` at x=76, baselines y=98, 128, 158, 188), each preceded by a filled 4px `#008300` dot at x=64:** "malware and exfiltration become visible", "DLP and regulatory duties satisfied", "block bad destinations mid-session", "policy enforced on managed devices".
- **Right rows (12px `#2c3e50` at x=406, same baselines), each preceded by a filled 4px `#d95926` dot at x=394:** "end-to-end becomes end-to-middle-to-end", "one system holds everyone's plaintext", "the user can no longer verify anything", "pinning and mutual-TLS break".
- **Footnotes (12px `#6b7280`, baseline y=218):** "often the only inspection point available" at x=76; "documented defect class: weak upstream checks" at x=406.
- **Annotation (bold 13px `#1a5276`, centered at x=360, y=262):** "the design question is scope, not yes-or-no".
- **Caption (12px `#444`, bottom right, y=292):** "qualitative summary".

## Two Mistakes, and What a Defensible Deployment Looks Like

**Tags:** `common mistake` (red), `mitigations` (green)

- **The user's error** — reading the padlock as end-to-end privacy, when on a managed device it can mean "encrypted to my employer"
- **The team's error** — treating inspection as pure added visibility, when it also subtracts verification and concentrates plaintext
- **Bypass lists** — exclude banking, health, and personal mail so the inspected scope matches the stated justification
- **Strict upstream checks** — validate destination certificates in the tier and verify that independently rather than assuming it
- **Crown-jewel hygiene** — access control, logging, and short retention on the decrypted plane, hardened accordingly
- **Disclosure** — an undisclosed capability is a governance failure even when the technology is working correctly
- **Endpoint inspection** — examining content on the device, where it fits, avoids creating a central plaintext chokepoint

*Example (italic):* Alice's personal banking session on a managed laptop is a governance question as much as a technical one — a bypass category answers it without giving up inspection everywhere else.

**Common mistake:** Neither "the padlock proves privacy" nor "inspection is free" survives contact with how it works. The first ignores whose certificate signed the session; the second ignores what the trade costs.

### Visualization (canvas `c4`, 720×300)

Panel: the two symmetrical misconceptions across the top, the controls that answer them listed below with green check marks.

- **Title (bold 15px, `#1a5276`, top center):** "What a Defensible Deployment Looks Like".
- **Misconception boxes (y=42, height 46, 8px radius, fill `rgba(213,81,129,0.10)`, 2px `#d55181` border):** x=40 w=310, 12px `#2c3e50` two lines "users: the padlock proves" (baseline 62) / "end-to-end privacy" (baseline 79); x=370 w=310, two lines "teams: inspection only adds" (baseline 62) / "visibility, and costs nothing" (baseline 79).
- **Sub-header (bold 13px `#1a5276`, x=40, y=116):** "controls that keep the scope matched to the justification".
- **Control rows (12px `#2c3e50` at x=70, baselines y=146, 176, 206, 236, 266), each with a bold 14px `#008300` "✓" at x=44):** "bypass sensitive categories: banking, health, personal mail", "validate upstream certificates strictly — verify, don't assume", "treat the decrypted plane as crown jewels: access, logs, retention", "disclose the capability to the people it applies to", "prefer endpoint inspection where it fits".
- **Caption (12px `#444`, bottom right, y=292):** "control list, not a product comparison".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine alarm states and is not used in these charts.
- **Data:** no randomness anywhere; every value is a hardcoded literal. The deployment (4,000 employees × 120 sessions/workday × 250 workdays) is invented and labeled illustrative, and its three derived figures are exact: 4,000 × 120 = 480,000 per day; 480,000 × 250 = 120,000,000 per year; 480,000 ÷ 4,000 = 120 per laptop, giving 480,000 ÷ 120 = 4,000× concentration. Bar lengths in `c2` are computed from the values via `Math.log10`, so the chart cannot drift from the text.
- **Framing:** deliberately balanced. TLS inspection is presented as a legitimate, widely deployed control with genuine benefits and genuine costs; the page argues about scope and hardening, never about legitimacy, and does not suggest employers act in bad faith.
- **Naming:** no vendors, products, or brand names — only generic "an inspection system", "a middlebox", "the inspection tier". People are Alice; the organization is "a company". No credential strings or certificate field values appear anywhere.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
