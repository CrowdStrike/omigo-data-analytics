# Cloud Security Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a one-row two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** Cloud Security Pitfalls

**Subtitle:** Monitoring and measurement pitfalls in cloud security — ephemeral evidence, uncomputable permissions, and responsibility gaps no party owns.

## Ephemeral Workloads

**The Container Dies at 5 Minutes; the Alert Fires at 7**

- **The lifecycle:** Container runs for 5 minutes, terminates, and takes its evidence with it.
- **The alert lag:** Detection lands near minute 7, two minutes after the workload is gone.
- **Why forensics fails:** You cannot inspect a workload that no longer exists anywhere.
- **Evidence gap:** Logs, memory, and filesystem state are destroyed, not merely stale.
- **The only fix:** Capture at runtime inside the live window, or lose everything.

### Visualization (canvas `canvas1`, 720×200)

Timeline diagram: container lifecycle vs alert timing.

- **Title (top center, `#1a5276`, 17px):** "Container Lifecycle vs Alert Timeline".
- **Timeline base:** horizontal gray line (`#bbb`, width 2) at y=100 from x=60 to x=680, with 9 tick marks labeled "0 min" through "8 min" (gray `#666`, 14px, evenly spaced).
- **Container lifespan block:** green rectangle from 0 min to 5 min above the timeline (y 60–95), fill `rgba(39,174,96,0.3)`, stroke `#27ae60` width 2, centered label "Container Running" in `#27ae60` 15px.
- **Terminated marker:** at the 5-min point, red (`#e74c3c`) label "TERMINATED" (13px) and a vertical dashed red line (dash 4/3, width 2) from the block top to just below the timeline.
- **Alert block:** at ~7 min, 60px-wide rectangle above the timeline, fill `rgba(231,76,60,0.2)`, stroke `#e74c3c`, centered label "ALERT!" in `#e74c3c` 14px.
- **Evidence gap annotation:** dark red `#c0392b` text "Evidence Gap: No forensics possible" (15px) centered below the timeline at y≈155; a solid `#c0392b` arrow (width 2, filled triangular head) runs from just after the terminated marker to just before the alert position at y≈142.
- **Legend (bottom left, gray `#666`, 13px):** "Runtime capture window missed = total evidence loss".

## IAM Policy Explosion

**2,000 Roles and 500 Policies Make Effective Permissions Uncomputable**

- **The inventory:** One cloud provider account carrying 2,000 IAM roles and 500 policies.
- **The multiplier:** Nested assume-role chains push reachable paths to roughly 8,000.
- **What breaks:** Effective permissions per principal are nearly impossible to compute.
- **The theory gap:** "Least privilege" stays a goal; auditing it is an operational nightmare.
- **The stake:** One misconfigured role equals full account compromise.

### Visualization (canvas `canvas2`, 720×200)

Bar chart: IAM complexity growth across four categories.

- **Title (top center, `#1a5276`, 17px):** "IAM Complexity Growth".
- **Bars:** width 100, gap 50, starting at x=100, baseline y=165, max bar height 110, values normalized to max 8000.
  - "Roles" = 2,000, fill `#3498db`, bold value label "2,000" above bar.
  - "Policies" = 500, fill `#2ecc71`, value label "500".
  - "Assume-Role / Chains" (two-line label) = 8,000, fill `#e67e22`, value label "8,000".
  - "Effective / Permissions" (two-line label) = Unknown: full-height bar with fill `rgba(231,76,60,0.3)`, dashed red border (`#e74c3c`, dash 5/5, width 2), a large bold red "?" (28px) centered in the bar, and label "Uncomputable" above the bar (13px red).
- **Category labels:** below each bar, `#333` 13px, split across lines where noted.
- **Annotation:** dark red `#c0392b` quadratic curved arrow (width 2) rising from above the second bar toward the fourth, with 12px label "Complexity explosion".

## Multi-Account Blast Radius

**One Credential Leak Puts 200 Accounts at Risk**

- **The estate:** An organization spread across 200 cloud provider accounts.
- **The connector:** Cross-account roles let Account A reach into Account B directly.
- **The cascade:** Compromise of one account means potential access to all of them.
- **Why it is unmeasured:** Blast radius is undefined without full trust-graph analysis.
- **What to change:** Enumerate the role graph before quoting any containment number.

### Visualization (canvas `canvas3`, 720×200)

Network graph: accounts as nodes in two concentric rings showing blast radius from one compromised credential.

- **Title (top center, `#1a5276`, 17px):** "Blast Radius: Single Credential Leak".
- **Inner ring:** 10 nodes on a circle of radius 65 centered at (360, 115), starting at top (-π/2). **Outer ring:** 14 nodes at radius 120 (65+55) around the same center.
- **Connections:** thin lines `rgba(231,76,60,0.3)` width 1 between inner node pairs where (i+j) is even, plus lines from each outer node to its nearest inner node (index mapping i·10/14).
- **Compromised node:** first inner node (top) drawn as a solid `#e74c3c` circle radius 12 with white bold 10px "!!!" text.
- **Other inner nodes:** `rgba(231,76,60,0.6)` circles radius 9. **Outer nodes:** `rgba(231,76,60,0.35)` circles radius 7.
- **Left labels:** bold red 13px "Compromised" at (30, 45); gray `#666` 13px "1 credential leak" and "= 200 accounts at risk" beneath it.
- **Right legend (right-aligned, `#c0392b`, 13px):** "Cross-account roles create" / "undefined blast radius" at bottom right.

## Shared Responsibility Confusion

**Neither Party Monitors the Boundary Between Their Responsibilities**

- **Provider side:** Physical infrastructure, network backbone, and hypervisor are theirs.
- **Customer side:** Data encryption, IAM configuration, and bucket policies are yours.
- **The unclear case:** A misconfigured storage bucket sits ambiguously between the two.
- **The gap:** Each side monitors its own half; the seam between them is unmonitored.
- **Consequence:** Incidents in the seam surface late because no party owns detection.

### Visualization (canvas `canvas4`, 720×200)

Gap diagram: two responsibility blocks with an unmonitored gap between them.

- **Title (top center, `#1a5276`, 17px):** "Shared Responsibility Model: The Unmonitored Gap".
- **Left block (x=50, w=250, y=50, h=100):** fill `rgba(255,153,0,0.15)`, stroke `#ff9900` width 2; bold 15px header "cloud provider Responsibility" in `#ff9900`; gray `#666` 13px items: "Physical infrastructure", "Network backbone", "Hypervisor".
- **Right block (x=420, w=250, same y/h):** fill `rgba(52,152,219,0.15)`, stroke `#3498db` width 2; bold header "Customer Responsibility" in `#3498db`; items: "Data encryption", "IAM configuration", "S3 bucket policies".
- **Gap block (between the two, x=300 to 420):** fill `rgba(231,76,60,0.2)`, dashed red border (`#e74c3c`, dash 5/5, width 2); centered bold red "GAP" (14px) with "Unmonitored" (11px) beneath.
- **Bottom label (centered, `#c0392b`, 14px):** "Neither party monitors the boundary between their responsibilities".
- **Chevron:** red `#e74c3c` V-shaped arrow (width 2) pointing up at the gap from just below the blocks.

## IaC Drift from Deployed State

**Security Audits the Declared Config, Not the Port That Is Actually Open**

- **What the code says:** Terraform declares the security group allows port 443 only.
- **What happened:** Someone clicked in the console and added port 22 to the live group.
- **The divergence:** Terraform state and deployed reality drift apart from that moment on.
- **The audit error:** Reviews read the IaC (wrong) instead of the deployed state (actual).
- **What to change:** Compare declared against deployed; treat drift as a finding, not noise.

### Visualization (canvas `canvas5`, 720×200)

Two parallel timelines: Terraform state vs deployed reality diverging after a manual change.

- **Title (top center, `#1a5276`, 17px):** "IaC State vs Deployed Reality".
- **Top line (y=75):** green `#27ae60` line width 3 from x=80 to x=650, left labels "Terraform" / "State" in green (14px, right-aligned). Four 40×20 boxes evenly spaced on the line, fill `rgba(39,174,96,0.2)`, stroke `#27ae60`, each labeled "Port 443" (12px green).
- **Bottom line (y=145):** red `#e74c3c` line width 3, left labels "Deployed" / "Reality" in red. Four 60×20 boxes labeled "Port 443", "Port 443", "Port 443, 22", "Port 443, 22" — first two green-styled (`rgba(39,174,96,0.2)` fill, `#27ae60` stroke/text), last two red-styled (`rgba(231,76,60,0.2)` fill, `#e74c3c` stroke/text).
- **Manual change marker:** orange `#e67e22` 12px label "Console click: +port 22" below the deployed line at ~x=365, with a short vertical orange tick (width 2).
- **Drift indicator:** vertical dashed `#c0392b` line (dash 4/4, width 1.5) between the two timelines after the change point, with bold 12px "DRIFT" label at its midpoint.
- **Bottom caption (centered, `#c0392b`, 13px):** "Security audits IaC (green) but reality is red".

## Serverless Visibility Gap

**Serverless Drops Security Visibility from 100% to Roughly 0%**

- **No host:** A serverless function has no OS on which to install a monitoring agent.
- **No processes:** There is nothing to watch, so process monitoring returns nothing.
- **No disk:** With no persistent file system, a file scanner has no surface to scan.
- **Tooling mismatch:** Tools built for VMs and containers simply do not work here.
- **The measured drop:** 100% visibility on a VM, ~60% in a container, ~0% serverless.

### Visualization (canvas `canvas6`, 720×200)

Three-column comparison: security visibility by execution model.

- **Title (top center, `#1a5276`, 17px):** "Security Visibility by Execution Model".
- **Columns (each 190 wide, 30 gap, from x=60, y=45, h=130):** "VM / EC2" (border/header `#27ae60`, bg `rgba(39,174,96,0.1)`), "Container" (`#f39c12`, bg `rgba(243,156,18,0.1)`), "Serverless / Lambda" (`#e74c3c`, bg `rgba(231,76,60,0.1)`). Headers bold 14px.
- **Checklist items (13px, left-aligned):** "OS Agent", "Process Mon.", "File Scanner", "Network Tap".
  - VM/EC2: all four "✓" in `#27ae60`.
  - Container: first three "✓" green, "Network Tap" is "✗" in `#e74c3c`.
  - Serverless: all four "✗" in `#e74c3c`.
- **Visibility labels (bold 15px, centered below each column):** "100% visibility" (`#27ae60`), "~60% visibility" (`#f39c12`), "~0% visibility" (`#e74c3c`).
- **Bottom caption (centered, `#c0392b`, 13px):** "Traditional tools fail completely in serverless: no OS, no filesystem, no processes".

## Regeneration instructions

- **Template/layout:** domains detail page. h1 + `.subtitle`, then per pitfall an `<h2>` (blue `#1a5276`, 1.4em, bottom border `2px solid #2980b9`) followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` (1.05em, weight 600, `#1a5276`) holding a one-line punchline, then a `<ul>` of labeled `<li>` bullets (`ul` = margin `8px 0 8px 20px`, 0.9em, `#333`; `li` = margin `4px 0`), each bullet a `<strong>` label plus a short phrase; right `<td>` (60%, centered) with the canvas. Even table rows have background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `strong` in `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but unused on this page. No nav bar, no back/home links.
- **Canvases:** six canvases (`canvas1`–`canvas6`) with no intrinsic width/height attributes; a shared `setupCanvas(canvas, width, height)` helper sets each to 720×200 CSS pixels and scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Default chart font 17px system sans-serif.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; supporting colors `#2980b9`, `#3498db`, `#2ecc71`, `#f39c12`, `#ff9900`, `#c0392b`, grays `#666`/`#333`/`#bbb`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
