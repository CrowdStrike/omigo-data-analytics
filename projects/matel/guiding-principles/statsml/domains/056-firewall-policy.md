# Firewall/Policy Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Firewall/Policy Pitfalls

**Subtitle:** Why firewall rulesets drift into unauditable, overly permissive, gap-ridden policies that no longer match documented intent.

## Rule Order Matters (First-Match-Wins)

**Rule 50 Matches First, So Rule 200 Never Runs**

- **The mechanism:** Rules evaluate top-down, first-match-wins — evaluation stops at the first hit.
- **The collision:** Rule 50 says "allow traffic X"; rule 200 says "deny traffic X".
- **Shadow rule:** Rule 200 is never evaluated, so its deny intent is silently dead.
- **Reorder effect:** Swapping 50 and 200 flips allow to deny with zero rule edits.
- **Audit trap:** The same rule set in a different order is a completely different policy.

### Visualization (canvas `canvas1`, 720×200)

Flow diagram: a packet evaluated against three rule boxes left to right, with first-match highlighted and the shadow rule flagged.

- **Header text (17px `#1a5276`):** "Packet: src=10.0.0.5, dst=web, port=443".
- **Three rule boxes** (200×60px at y=50, spaced 230px starting x=30, arrows between them):
  1. "Rule 50" / "ALLOW any→web:443" — HIT: green border `#27ae60` 3px, fill `#eafaf1`, rule text green.
  2. "Rule 100" / "ALLOW 10.0.1.0/24→web" — not hit: gray border `#bdc3c7` 1.5px, fill `#f9f9f9`, rule text green `#27ae60`.
  3. "Rule 200" / "DENY 10.0.0.5→web:443" — not hit: gray border, fill `#f9f9f9`, rule text red `#e74c3c`.
  - Rule numbers in bold 17px `#1a5276`; rule text 14px.
- **Result lines (bold 17px):** green `#27ae60` "✓ MATCH → Traffic Allowed"; red `#e74c3c` "✗ Rule 200 NEVER evaluated (shadow rule)".
- **Footnote (italic 14px `#7f8c8d`):** "Reordering rules 50↔200 flips allow→deny with zero rule edits".

## Shadow/Dead Rules

**25–40% of Firewall Rules Are Never Triggered**

- **Where they come from:** Decommissioned servers, closed projects, "temporary" exceptions kept forever.
- **Why they stay:** Nobody knows if removing one breaks something, so they accumulate indefinitely.
- **Cost per rule:** More complexity, slower packet processing, and near-impossible auditing.
- **The sediment:** The ruleset becomes archaeological layers of forgotten intent.
- **On this chart:** Only 40% of rules are active; 60% are dead weight nobody dares remove.

### Visualization (canvas `canvas2`, 720×200)

Pie chart of firewall rule status with a legend and a summary callout.

- **Pie (center x=160, y=105, radius 75, slices start at top, white 2px slice borders):**
  - Active Rules 40% green `#27ae60`.
  - Redundant/Shadowed 20% orange `#f39c12`.
  - Decommissioned Servers 15% red `#e74c3c`.
  - Old Projects 12% purple `#9b59b6`.
  - "Temporary" Exceptions 13% blue `#3498db`.
- **Legend (x=290, 15px `#333`):** color swatch + "label (pct%)" per segment in the order above.
- **Summary (right, x=460):** bold 17px red `#e74c3c` "60% of rules = dead weight"; below it 14px `#7f8c8d` "Nobody dares remove them".

## Overly Permissive Rules Impossible to Tighten

**Proving a Rule Is Safe to Tighten Costs 10TB/Day of Logs**

- **The rule:** "Allow any-to-any on port 443" — should be narrowed to specific IPs.
- **The blocker:** 500 services MIGHT use it, and actual usage is unknown without monitoring.
- **Monitoring price:** Full logging generates 10TB/day, which nobody can store or analyze.
- **The outcome:** With no evidence available, the permissive rule stays permissive.
- **One-way ratchet:** Loosening is easy; tightening is practically impossible.

### Visualization (canvas `canvas3`, 720×200)

Centered funnel of five progressively narrower horizontal bars showing why tightening fails.

- **Steps (top to bottom, each a 30px-tall centered rectangle, rows spaced 38px starting y=10; fill is the step color at 20% alpha with a 2px solid border of the same color; text bold 15px `#1a5276` centered):**
  1. "Rule: allow any→any:443" — width 680, red `#e74c3c`.
  2. "Want to restrict to specific IPs" — width 560, orange `#f39c12`.
  3. "500 services MIGHT use it" — width 440, yellow `#f1c40f`.
  4. "Need monitoring → 10TB/day logs" — width 320, blue `#3498db`.
  5. "Can't store/analyze → Rule stays permissive" — width 200, purple `#9b59b6`.

## Log Asymmetry

**Your Threat Data Contains Only the Attacks That Failed**

- **What gets logged:** Denied traffic, because a block is treated as a reportable event.
- **What does not:** Allowed traffic is often unlogged because the volume is too high.
- **The blind spot:** You see blocked attempts but nothing about what got through.
- **The bias:** Survivorship bias applied to security — you study failures-to-penetrate.
- **Mislabeled output:** The "threat landscape" is really a "things-we-already-handle landscape."

### Visualization (canvas `canvas4`, 720×200)

Two-column comparison of logged events vs actual events, as labeled horizontal bars.

- **Column headers (bold 17px `#1a5276`):** "What You See (Logged)" at x=40; "What Actually Happened" at x=420.
- **Left column (all red `#e74c3c` bars, 25px tall, decreasing widths 280/210/160/120 starting y=40, white 13px labels inside):**
  - "DENIED: Brute force (2,840)"
  - "DENIED: Port scan (1,920)"
  - "DENIED: SQL inject (1,100)"
  - "DENIED: Exfil attempt (680)"
- **Right column (bars starting x=420, widths 240/180/200/160, white labels):**
  - green `#27ae60`: "ALLOWED: Lateral move (NOT LOGGED)"
  - red `#e74c3c`: "DENIED: Port scan (logged)"
  - green: "ALLOWED: Data exfil (NOT LOGGED)"
  - green: "ALLOWED: C2 beacon (NOT LOGGED)"
- **Bottom summary (bold 15px red `#e74c3c`):** "You analyze failures-to-penetrate, not actual breaches".

## Policy Drift from Exceptions

**100 Justified Exceptions Turn "Deny All" Into "Allow Most"**

- **The pattern:** Security sets "deny all"; business requests one exception at a time.
- **Locally reasonable:** Each exception was individually justified and approved on its merits.
- **Collectively fatal:** After 100 of them the policy is swiss cheese and the posture is gutted.
- **Nobody's job:** No one tracks the cumulative effect of the exception list.
- **Doc vs reality:** The documented intent still says "deny all"; the effective policy does not.

### Visualization (canvas `canvas5`, 720×200)

Exponential decay curve of security coverage as exceptions accumulate, against a dashed line for the documented policy.

- **Axes:** L-shape in `#333` 1.5px, x from 60 to 660, y from 30 (top) to 160 (bottom); y labels "100%" at top, "0%" at bottom; x-axis title "Exceptions Added →" centered below; rotated y-axis title "Security Coverage %".
- **Decay curve:** red `#e74c3c` 3px line starting at 100% coverage and decaying as exp(-0.025·i) over i = 0..100 exceptions (with a tiny 2px step-down every 5th point).
- **Dashed reference line:** green `#27ae60` (dash 5/5, 1.5px) horizontal at the 100% level, labeled in 12px green "← Documented policy" at the right.
- **Annotations (bold 14px):** green "\"Deny All\" policy" near the start; orange `#f39c12` "10 exceptions" partway down; red "50 exceptions" mid-curve; bold 15px red "100 exceptions = \"allow most\"" near the tail.

## Multi-Vendor Policy Inconsistency

**3 Syntaxes × 3 Models × 0 Unified View = Boundary Blind Spots**

- **The three layers:** Palo Alto at the perimeter, cloud Security Groups, Kubernetes NetworkPolicies.
- **No common language:** Three syntaxes, three rule models, and no single place to read policy.
- **Divergent defaults:** Deny-all versus allow-specified, plus different granularity and logging.
- **The gap:** Traffic leaving one domain and entering another may match no policy at all.
- **Where it hurts:** The unprotected surface is the transition, not any one vendor's zone.

### Visualization (canvas `canvas6`, 720×200)

Diagram of three vendor policy zones with red hatched GAP strips between them.

- **Three zone boxes (190px wide, 120px tall at y=20; fill is zone color at 15% alpha, border 2.5px solid; centered labels — bold 16px zone-colored name, 14px `#555` scope, 12px `#777` rule model):**
  1. x=30, blue `#2980b9`: "Palo Alto" / "Perimeter" / "Zone-based, deny-all default".
  2. x=270, green `#27ae60`: "cloud provider SG" / "Cloud" / "Stateful, allow-specified".
  3. x=510, purple `#9b59b6`: "K8s NetPol" / "Containers" / "Label-based, ns-scoped".
- **Gap strips (30px wide, full zone height, centered at x=235 and x=475):** red `#e74c3c` fill at 30% alpha with dashed red border (dash 4/4, 2px), bold 14px red label "GAP" centered, and a downward red arrow beneath each strip.
- **Bottom text:** bold 15px red `#e74c3c` centered "Traffic in transition between domains = NO POLICY APPLIES"; below it 13px `#7f8c8d` "3 syntaxes × 3 models × 0 unified view = blind spots at boundaries".

## Regeneration instructions

- **Layout:** standard detail-page pattern — h1 + `.subtitle`, then per pitfall an `<h2>` (1.4em, `#1a5276`, bottom border `2px solid #2980b9`) followed by a single-row `.obj-table`: full-width table, left `<td>` (40%) holds `.obj-title` (the bold one-line punchline) followed by a `<ul>` of 4–5 `<li>` labeled bullets (`<strong>Label:</strong> short phrase`), right `<td>` (60%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` `margin: 8px 0 8px 20px`, 0.9em, `#333` and `li` `margin: 4px 0`, with `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.philosophy` callout style available (background `#f0f4f8`, left border `4px solid #2980b9`) but unused on this page. No nav bar, no back/home links.
- **Canvases:** the `<canvas>` elements carry only ids; a shared `setupCanvas(canvas, 720, 200)` helper sets intrinsic size 720×200, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Default chart font 17px system sans-serif.
- **Palette:** primary blue `#1a5276`, blue accents `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`, yellow `#f1c40f`, purple `#9b59b6`, grays `#555`/`#777`/`#7f8c8d`/`#bdc3c7`.
- Note: in regenerated HTML, any card/page links use `.html` extensions (this page has none).
