# Mobile Security Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a one-row two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** Mobile Security Pitfalls

**Subtitle:** Monitoring and measurement pitfalls in mobile security — post-review malware, untrustworthy device self-reports, and constantly shifting network identities.

## App Store Review ≠ Safe

**Malware Arrives 2 Weeks After Approval, Not At Submission**

- **The sequence:** App passes initial review, then pushes a malicious update 2 weeks later.
- **Weaker second gate:** Review of updates is far less rigorous than the initial review.
- **False comfort:** "It's on the App Store" reads to users as a safety guarantee.
- **Trust exploited:** Attackers monetize the trust users place in store-reviewed apps.
- **What approval proves:** One snapshot passed a check — not that the app stays safe.

### Visualization (canvas `canvas1`, 720×200)

Timeline with five stages and a review-rigor bar.

- **Timeline:** horizontal `#2980b9` line (width 2) at y=100 from x=40 to x=680.
- **Stages (dots radius 10 on the line, bold 14px name above in `#1a5276`, 13px sub-caption below in `#666`):**
  - x=100: "Submit App" (dot `#27ae60`), sub "Clean code".
  - x=250: "Review Pass" (dot `#27ae60`), sub "Approved ✓".
  - x=400: "Published" (dot `#f39c12`), sub "Users trust it".
  - x=550: "Update Push" (dot `#e74c3c`), sub "Malware added".
  - x=650: "Live" (dot `#e74c3c`), sub "Less scrutiny".
- **Rigor bar (bottom):** label "Review Rigor:" in `#1a5276` 17px at left; green `#27ae60` 150×18 block with white bold 13px text "HIGH (initial)"; red `#e74c3c` 150×18 block with white text "LOW (updates)".

## On-Device vs Cloud Detection Tradeoff

**Threats Hide in the Gap Between On-Device and Cloud**

- **On-device wins:** Low latency and no network dependency for every check.
- **On-device limits:** Limited compute and small model size, so complex threats slip past.
- **Cloud wins:** Powerful models and full threat intelligence behind every lookup.
- **Cloud costs:** Needs connectivity, raises privacy concerns, and misses offline attacks.
- **Why neither suffices:** Their blind spots do not overlap — the gap between them is exploitable.

### Visualization (canvas `canvas2`, 720×200)

Two-box pros/cons comparison with a labeled gap between them.

- **On-Device box (x=50, w=280, y=20, h=160):** fill `#eaf2f8`, stroke `#2980b9` width 2; bold 17px centered header "On-Device" in `#1a5276`. Green `#27ae60` 14px items: "+ Low latency", "+ No network needed". Red `#e74c3c` items: "- Limited compute", "- Small model size", "- Misses complex threats".
- **Cloud box (x=390, w=280, same y/h):** fill `#fef9e7`, stroke `#f39c12` width 2; header "Cloud". Green items: "+ Powerful models", "+ Full threat intel". Red items: "- Requires network", "- Privacy concerns", "- Misses offline attacks".
- **Gap indicator (between the boxes, centered ~x=360):** bold red 15px "GAP" with a dashed red bracket outline (`#e74c3c`, dash 4/3, width 2, open-topped rectangle from y=70 to y=140).

## Jailbreak/Root Detection Cat-and-Mouse

**A Compromised OS Decides What Your Detector Is Allowed to See**

- **The loop:** App checks for jailbreak, attacker hides it, app adds a check, attacker patches it.
- **No stable end:** The cycle is endless; detection sits permanently one step behind.
- **Root of the problem:** The compromised OS controls what the app can observe.
- **What that means:** A device's self-report on its own integrity cannot be trusted.
- **Modeling caution:** A "not jailbroken" flag is an attacker-controlled feature, not a fact.

### Visualization (canvas `canvas3`, 720×200)

Circular cycle diagram with four numbered nodes on a dashed ellipse.

- **Ellipse:** dashed gray `#999` (dash 5/5, width 2) centered at (360, 100), radii 140×60.
- **Nodes (circles radius 12 with white bold numbers 1–4):**
  - Top (angle -π/2): `#2980b9`, label "App checks / for jailbreak" (two lines, above).
  - Right (angle 0): `#e74c3c`, label "Attacker hides / jailbreak" (to the right).
  - Bottom (angle π/2): `#2980b9`, label "App adds / new check" (below).
  - Left (angle π): `#e74c3c`, label "Attacker / patches check" (to the left).
  - Labels in `#1a5276` 13px.
- **Center label:** bold red `#e74c3c` 17px "∞ Endless Cycle".
- **Side note (bottom right, gray `#666`, 13px, right-aligned):** "Detection always one step behind".

## Permission Creep Invisible to User

**Consent Granted at Install Still Holds 6 Months Later**

- **The sequence:** App requests camera on install, user grants, user forgets it ever happened.
- **The surprise:** The app uses the camera in the background 6 months later.
- **What changed:** The grant is still valid but the context around it no longer is.
- **The false equation:** Static permission is not real-time appropriate access.
- **Diverging models:** The user's memory of what they allowed drifts from actual app behavior.

### Visualization (canvas `canvas4`, 720×200)

Two-line time chart: declining user awareness vs constant app access.

- **Axes:** black `#333` L-shaped axes, origin (50, 160), chart width 620, chart height 130; rotated y-axis label "Awareness" and x-axis label "Time" in `#1a5276` 13px.
- **X markers (gray `#666`, 12px):** "Install", "1 mo", "2 mo", "3 mo", "4 mo", "5 mo", "6 mo" evenly spaced.
- **User Awareness line:** solid `#2980b9` width 3 through normalized points `[1.0, 0.6, 0.35, 0.2, 0.12, 0.08, 0.05]` (declining curve).
- **App Access Level line:** dashed `#e74c3c` (dash 6/4, width 3) flat at `1.0` for all seven points.
- **Legend (top right, 14px):** blue line swatch + "User Awareness" (`#2980b9`); dashed red swatch + "App Access Level" (`#e74c3c`).
- **Danger zone:** `rgba(231,76,60,0.1)` rectangle over the last 40% of the chart, with bold red 13px annotation "DANGER: Access without awareness" centered at ~80% width.

## Sideloading Bypasses All Controls

**Every Store Guardrail Is Optional on Android**

- **What sideloads:** Android APKs installed outside Play Store — enterprise, regional, pirated apps.
- **Controls skipped:** No store review, no safety scanning, no update mechanism.
- **What runs anyway:** Completely unmanaged software holding full device permissions.
- **A parallel universe:** Zero-guardrail apps coexist with vetted apps on the same device.
- **Why data lies:** Store-derived safety signals simply do not cover these installs.

### Visualization (canvas `canvas5`, 720×200)

Two-path diagram: gated store path vs unguarded sideload path.

- **Play Store path (y=60):** left label "Play Store Path:" bold 15px `#1a5276`. Six green `#27ae60` 80×24 gate blocks with white 12px labels "Submit", "Review", "Scan", "Sign", "Publish", "Update", connected by green arrows (width 2 with filled triangular heads), starting at x=50 with 110px spacing.
- **Sideload path (y=150):** left label "Sideload Path:" bold 15px `#1a5276`. A single straight red `#e74c3c` line width 3 from x=50 to x=600 ending in a filled red arrowhead.
- **Missing controls:** four bold 18px red "✗" marks above the sideload line at x=130/260/390/520, each with a 12px caption below the line: "No Review", "No Scan", "No Sign", "No Updates".
- **Result label (right-aligned, bold red 14px):** "Full permissions, zero oversight".

## Network Switching Disrupts Monitoring

**Same Device, Five Network Identities in One Afternoon**

- **The hop chain:** Phone switches WiFi → cellular → a different WiFi → VPN → cellular again.
- **What each switch breaks:** A different IP, different routing, different visibility.
- **Continuity lost:** Monitoring a constantly re-identified device is extremely hard.
- **The invariant:** The device never changes; only its network fingerprint does.
- **Analysis risk:** Keying sessions on network identity splits one device into many.

### Visualization (canvas `canvas6`, 720×200)

Device-hopping diagram plus a fragmented monitoring-continuity bar.

- **Device icon (left, x=60, y=100):** simple phone drawn as `#1a5276` 30×70 rectangle with white 24×50 screen inset, labeled "Device" (10px) below; a dashed gray `#999` connector (dash 3/3) links it to the first network node.
- **Network hops (circles radius 20 at y=80, starting x=140, spacing 110; white bold 11px name inside, gray `#666` 11px IP below):**
  - "WiFi-A" `#2980b9`, IP "192.168.1.x".
  - "Cellular" `#e67e22`, IP "10.0.x.x".
  - "WiFi-B" `#27ae60`, IP "172.16.x.x".
  - "VPN" `#8e44ad`, IP "10.8.x.x".
  - "Cellular" `#e67e22`, IP "10.1.x.x".
- **Transitions:** red `#e74c3c` arrows (width 2, filled heads) between consecutive nodes, each with a red 16px "⚡" break indicator above the midpoint.
- **Monitoring status bar (bottom):** label "Monitoring Continuity:" bold 14px `#1a5276`; seven 40×18 blocks from x=340 alternating `#27ae60` / `#e74c3c` (green, red, green, red, green, red, green); gray 12px legend below: "■ Visible  ■ Gap (switch)".

## Regeneration instructions

- **Template/layout:** domains detail page. h1 + `.subtitle`, then per pitfall an `<h2>` (blue `#1a5276`, 1.4em, bottom border `2px solid #2980b9`) followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` (1.05em, weight 600, `#1a5276`) holding a one-line punchline, then a `<ul>` of 4-5 labeled `<li>` bullets (`<strong>Label:</strong> phrase`, each fitting one line); `ul { margin: 8px 0 8px 20px; font-size: 0.9em; color: #333; }` and `li { margin: 4px 0; }`. Right `<td>` (60%, centered) holds the canvas. Even table rows have background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px. HTML entities used in headings/text: `&ne;` (≠), `&rarr;` (→), `&mdash;` (—).
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `strong` in `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but unused on this page. No nav bar, no back/home links.
- **Canvases:** six canvases (`canvas1`–`canvas6`) with no intrinsic width/height attributes; a shared `setupCanvas(canvas, width, height)` helper sets each to 720×200 CSS pixels and scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Default chart font 17px system sans-serif.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; supporting colors `#2980b9`, `#f39c12`, `#8e44ad`, grays `#666`/`#999`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
