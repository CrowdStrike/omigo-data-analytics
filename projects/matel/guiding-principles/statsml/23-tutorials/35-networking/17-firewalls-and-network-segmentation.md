# Firewalls & Network Segmentation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Firewalls & Network Segmentation

**Subtitle:** A firewall is a guard with an allow/deny list; segmentation splits one open network into rooms — so one infected machine can't reach everything

## One Open Office Floor, One Infected Laptop

**Tags:** `core idea` (blue), `allow/deny` (green), `segmentation` (orange)

- **The office** — a 40-device company network: 25 staff laptops, 10 guest phones, 4 printers, 1 payroll server
- **The flat network** — with no walls, every device can open a connection to every other device
- **The infection** — one guest phone joins Wi-Fi carrying malware that scans for neighbors
- **The firewall** — a checkpoint between segments that reads each connection against an allow/deny list
- **The walls** — segmentation groups devices into zones (guest, staff, payroll) with a firewall between zones
- **The payoff** — the infected guest phone now sees the internet and nothing else inside the office

*Example (italic):* On the flat network the guest phone can probe all 39 other devices; after segmentation its scan finds 0 machines in the staff and payroll zones.

**Key point:** A firewall decides connection by connection (allow or deny); segmentation decides the map — which groups of machines are even behind the same checkpoint.

### Visualization (canvas `c1`, 720×300)

Two-panel diagram: the same 40-device office as one flat network (left) vs three segments behind a firewall (right), with an infected guest phone in each.

- **Title (bold 15px, `#1a5276`, top center):** "Same 40 Devices: Flat Floor vs Three Walled Segments".
- **Left panel (x 40–340):** one rounded box (y 60–235, fill `rgba(42,120,214,0.10)`, 1.5px `#2a78d6` border) labeled "flat network — 40 devices" (12px `#444` above the box); inside, an 8×5 grid of 5px dots `#2a78d6` at 30px spacing, the top-left dot red `#e74c3c` (infected guest phone) with eight thin 1px red rays fanning to sample dots; bold 12px red label "reaches 39 machines" under the box at y=258.
- **Right panel (x 380–680):** three rounded boxes stacked with gaps — "guest" (y 60–110, 10 dots, top-left dot red), "staff + printers" (y 130–195, 29 dots, fill `rgba(0,131,0,0.08)`, border `#008300`), "payroll" (y 215–235, 1 square dot `#4a3aa7`, border `#4a3aa7`); a vertical 6px `#c98500` bar at x=372 spanning y 60–235 labeled "firewall" rotated 12px `#c98500`, plus thin `#c98500` bars on the gaps between the three boxes; the red dot has no rays.
- **Annotation (bold 13px green `#008300`, right panel, y=45):** "same infection, 0 internal machines reachable".
- **Caption (12px `#444`, bottom right):** "device counts illustrative".

## Five Packets Against the Rule List

**Tags:** `worked example` (blue), `deny by default` (green), `first match wins` (orange)

- **The rule list** — 1) allow guest→internet:443, 2) allow staff→printers:631, 3) allow staff→internet:443, 4) allow clerk-01, clerk-02→payroll:5432, 5) deny all
- **The reading order** — the firewall checks a connection against rules top to bottom and stops at the first match
- **Attempt A** — guest phone → internet:443 matches rule 1 → allow
- **Attempt B** — guest phone → payroll:5432 matches nothing until rule 5 → deny
- **Attempts C, D** — staff laptop → printer:631 hits rule 2 → allow; staff laptop → payroll:5432 falls to rule 5 → deny
- **Attempt E** — clerk-01 → payroll:5432 matches rule 4 → allow; 3 of the 5 attempts get through

*Example (italic):* The payroll server accepts exactly two laptops (clerk-01, clerk-02) on one port (5432); the other 23 staff laptops are denied by the final catch-all rule.

**Key point:** Deny-by-default means the last rule refuses everything not explicitly allowed — a safe list is short, readable, and hand-checkable top to bottom.

### Visualization (canvas `c2`, 720×300)

Rule-trace diagram: five connection attempts on the left, the 5-rule list in a center box, an arrow from each attempt to the rule that decides it, and an ALLOW/DENY badge on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Five Attempts, One Rule List, First Match Wins".
- **Attempt labels (12px `#2c3e50`, left-aligned at x=20, rows y = 80, 120, 160, 200, 240):** "A guest → internet:443", "B guest → payroll:5432", "C staff → printer:631", "D staff → payroll:5432", "E clerk-01 → payroll:5432".
- **Rule box (x 300–480, y 65–255, fill `rgba(26,82,118,0.06)`, 1.5px `#1a5276` border):** five 12px `#2c3e50` lines at y = 90, 125, 160, 195, 230 — "1 allow guest→internet", "2 allow staff→printers", "3 allow staff→internet", "4 allow clerks→payroll", "5 deny all" (line 5 in bold `#d95926`).
- **Arrows:** 2px `#6b7280` line from each attempt row to its matching rule line: A→1, B→5, C→2, D→5, E→4.
- **Verdict badges (rounded 56×22 boxes at x=560 on the attempt rows):** ALLOW badges (A, C, E) fill `rgba(0,131,0,0.12)`, bold 12px `#008300` text; DENY badges (B, D) fill `rgba(231,76,60,0.12)`, bold 12px `#e74c3c` text.
- **Annotation (bold 12px orange `#d95926`, near x=560, y=270):** "rule 5 catches everything unlisted".
- **Caption (12px `#444`, bottom left):** "rules and ports illustrative".

## Blast Radius: What One Breach Can Touch

**Tags:** `why it matters` (blue), `blast radius` (orange), `containment` (green)

- **Blast radius** — the count of machines an attacker can reach from the first box they compromise
- **Flat network** — any starting point reaches all 39 other devices; every breach is a full-network breach
- **Guest breach** — segmented, an infected guest phone reaches 0 internal machines (internet only)
- **Staff breach** — a phished staff laptop reaches 24 other staff laptops + 4 printers = 28, never payroll
- **Clerk breach** — a clerk laptop reaches those 28 plus the payroll server = 29; that risk sits on 2 machines
- **The lesson** — segmentation doesn't stop the first infection; it caps how far the second step spreads

*Example (italic):* The same phishing click costs 39 reachable machines on the flat network but 28 on the segmented one — and payroll is exposed only if one of 2 clerk laptops is the victim.

**Key point:** You size a network breach the way you size any failure — by blast radius — and segmentation is the tool that shrinks it before the breach happens.

### Visualization (canvas `c3`, 720×300)

Horizontal paired-bar chart: machines reachable from a compromised device, flat vs segmented, for three starting points (guest phone, staff laptop, clerk laptop).

- **Title (bold 15px, `#1a5276`, top center):** "Blast Radius From One Compromised Device: Flat vs Segmented".
- **Axis:** vertical 2px `#999` baseline at x=190, bars extend right, scale 39 devices = 400px (10.26 px/device); light `#e5e9ef` gridlines at 10/20/30 devices with 11px `#6b7280` tick labels at y=280.
- **Groups (left-aligned 12px `#444` labels at x=20):** "guest phone" rows y=70 (flat) and y=94 (segmented); "staff laptop" rows y=132 and y=156; "clerk laptop" rows y=194 and y=218.
- **Flat bars (all three):** fill `rgba(231,76,60,0.30)` with 1.5px `#e74c3c` edge, width 400 (39 devices), 11px `#e74c3c` value label "39" at bar end.
- **Segmented bars:** fill `rgba(0,131,0,0.30)` with 1.5px `#008300` edge — guest width 0 (label "0" at x=196), staff width 287 (label "28"), clerk width 297 (label "29").
- **Bar style:** 16px tall; tiny 10px `#6b7280` row tags "flat" / "segmented" just left of x=190.
- **Annotation (bold 13px green `#008300`, near x=210, y=112):** "guest breach: 39 → 0 machines reachable".
- **Caption (12px `#444`, bottom right):** "counts from the worked example, illustrative".

## The Crunchy Shell With a Soft Center

**Tags:** `common mistake` (red), `perimeter only` (orange)

- **The mistake** — one strong firewall at the internet edge, then a flat, wide-open network behind it
- **Why it fails** — phishing, a bad USB stick, or a hijacked update lands the attacker inside the shell
- **Inside is invisible** — the perimeter firewall never sees laptop-to-server traffic, so it can't deny it
- **The soft center** — from that one phished laptop, all 39 other devices — payroll included — are one hop away
- **The fix** — internal segments with deny-by-default rules, so inside traffic is checked like outside traffic
- **The habit** — ask of any zone "if this box is owned, what can it reach?" and shrink that answer

*Example (italic):* The office's edge firewall blocks every inbound scan, yet one phished staff laptop still reaches the payroll server — because no rule ever stood between them.

**Common mistake:** Treating the perimeter firewall as the whole defense. Most real intrusions start inside (one click on one laptop); only internal segmentation puts an allow/deny check on that second step.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a phished laptop's path to the payroll server with a perimeter-only design (reaches it) vs an internally segmented design (denied).

- **Title (bold 15px, `#1a5276`, top center):** "Perimeter Only vs Internal Segmentation: the Second Step".
- **Row 1 (y=95), label 12px `#444` at x=20:** "perimeter only"; yellow `#c98500` 6px vertical bar at x=150 (y 70–120) labeled "edge firewall" (11px), then a blue `#2a78d6` rounded box at x=190 labeled "phished staff laptop" (12px), 3px `#e74c3c` arrow to a red `#e74c3c` box at x=470 labeled "payroll server reached" with bold 12px red "✗ no wall inside".
- **Row 2 (y=205), label:** "segmented"; same edge bar at x=150, blue box "phished staff laptop" at x=190, 3px arrow to a yellow `#c98500` 6px vertical bar at x=430 (y 180–230) labeled "internal firewall — rule 5: deny", then a dashed 2px `#6b7280` arrow stub to a green `#008300` box at x=520 labeled "payroll safe" with bold 12px green "✓ denied".
- **Box style:** 150–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "the perimeter stops step one; segmentation stops step two".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); the office inventory (25 staff laptops, 10 guest phones, 4 printers, 1 payroll server = 40 devices), the 5-rule list with its A–E verdicts (A/C/E allow, B/D deny), and the blast-radius counts (flat 39; segmented guest 0, staff 24+4=28, clerk 28+1=29) are invented and labeled illustrative; bar pixel widths in c3 use 39 devices = 400px.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
