# Endpoint Security Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Endpoint Security Pitfalls

**Subtitle:** Why endpoint detection breaks on fileless attacks, dual-use tools, and sensors that trust the compromised host they run on.

## Fileless Malware

**File-Hash AV Catches 95% of File Malware and 5% of Fileless**

- **The mechanism:** Runs only in memory; no file on disk for a scanner to hash.
- **The abuse path:** Hijacks PowerShell, WMI, and other built-in scripting engines.
- **Why hashes fail:** No disk artifact means signature scanners find nothing to match.
- **In the chart:** File-hash AV falls from 95% on file-based malware to 5% on fileless.
- **What to change:** Score behavior — memory patterns, API calls, script execution — not files.

### Visualization (canvas `canvas1`, 720×200)

Grouped bar chart comparing detection rates for file-based vs fileless malware across five detection technologies.

- **Title (bold 17px, `#1a5276`, centered at top):** "Detection Rate: File-Based vs Fileless Malware".
- **Categories (x-axis):** File-Hash AV, Heuristic AV, Behavior Analysis, Memory Scanning, EDR (Advanced) — starting at x=100, spacing 125px, baseline y=175, max bar height 120px scaled to 100%.
- **Data:** File-based malware detection rates `[95, 85, 80, 70, 90]` (%) in green `#27ae60`; fileless rates `[5, 25, 65, 55, 70]` (%) in red `#e74c3c`. Bars 25px wide, paired side by side (green left of center, red right).
- **Value labels:** each bar's percentage above it, in the bar's color (11px); category names below baseline in `#333` (12px).
- **Legend (top left):** green swatch "File-Based Malware", red swatch "Fileless Malware" (17px, `#333`).

## Living-off-the-Land Binaries (LOLBins)

**Same Binary, 300 Admin Runs and 8 Attacks — Nothing Separates Them**

- **The tools:** certutil, mshta, regsvr32, rundll32, wmic — all shipped with Windows.
- **Dual use:** IT admins run these same binaries daily for legitimate administration.
- **Where the signal is:** Context makes a run malicious, never the binary itself.
- **Buried in noise:** rundll32 shows 300 legitimate executions daily against 8 malicious.
- **The false choice:** Blocking the binary breaks admin work; allowing it enables attacks.

### Visualization (canvas `canvas2`, 720×200)

Grouped bar chart showing legitimate vs malicious daily executions per LOLBin — the malicious bars are tiny and hidden in the noise.

- **Title (bold 17px, `#1a5276`, centered):** "LOLBin Usage: Legitimate Admin vs Attacker (daily executions)".
- **Tools (x-axis):** certutil, mshta, regsvr32, rundll32, wmic — starting x=80, spacing 130px, baseline y=170, max height 110px scaled to max value 300.
- **Data:** legitimate executions `[120, 15, 85, 300, 95]` in blue `#3498db`; malicious executions `[3, 2, 4, 8, 5]` in red `#e74c3c` (minimum 3px bar height so tiny bars stay visible). Bars 22px wide, paired.
- **Value labels:** raw counts above each bar in the bar's color (11px); tool names below baseline in `#333` (13px).
- **Legend (top left):** blue swatch "Legitimate Use", red swatch "Malicious Use (hidden in noise)".

## Process Tree Context Required

**The Same cmd.exe Scores 5% or 95% Depending on Its Parents**

- **Alone:** cmd.exe with no notable parent is ordinary — 5% suspicion.
- **One level up:** cmd.exe spawned by winword.exe is suspicious — 72%.
- **Two levels up:** outlook.exe to winword.exe to cmd.exe, from an attachment — 95%.
- **Why isolation fails:** Judged individually, every process in that chain looks benign.
- **What to collect:** The full ancestry tree, not a single process record.

### Visualization (canvas `canvas3`, 720×200)

Bar chart of suspicion scores rising with process ancestry depth, each bar annotated with a monospace process-tree label.

- **Title (bold 17px, `#1a5276`, centered):** "Suspicion Score by Process Ancestry Depth".
- **Four bars** (80px wide, starting x=50, spacing 175px, baseline y=170, max height 100px = 100%):
  1. Tree label "cmd.exe" / "(alone)" — score 5%, green `#27ae60`.
  2. Tree label "explorer.exe" / "  └ cmd.exe" — score 10%, green `#27ae60`.
  3. Tree label "winword.exe" / "  └ cmd.exe" — score 72%, orange `#f39c12`.
  4. Tree label "outlook.exe" / "  └ winword.exe" / "    └ cmd.exe" — score 95%, red `#e74c3c`.
- **Score labels:** bold white percentage inside the top of each bar; tree labels rendered in 12px monospace `#333` above the bars.
- **Bottom axis caption (17px, `#666`, centered):** "More ancestry context → Higher confidence in detection".

## Signature Delay

**Signature Coverage Arrives 4-72 Hours After the First Victim**

- **The pipeline:** Vendor obtains a sample, analyzes it, writes a signature, tests, ships.
- **The clock:** That full cycle runs 4-72 hours from release to protected endpoints.
- **First victims:** Infections start around T+1h, long before the vendor has a sample.
- **Coverage during the gap:** Signature-based detection contributes exactly zero.
- **Where damage lands:** Concentrated inside the window, not after the update.

### Visualization (canvas `canvas4`, 720×200)

Horizontal timeline with six milestone dots and a shaded danger zone covering the unprotected window.

- **Title (bold 17px, `#1a5276`, centered):** "Signature Update Timeline: The Unprotected Window".
- **Danger zone:** light red rectangle `rgba(231,76,60,0.1)` from x=50 spanning 480px wide (y 45–155), labeled in bold 14px red `#e74c3c`: "ZERO PROTECTION WINDOW (4-72 hours)".
- **Timeline line:** blue `#2980b9`, 3px wide, from x=50 to x=680 at y=100.
- **Events (dot at each x, two-line label below, bold blue time label above):**
  - x=50: "Malware / Released", T+0h, red dot.
  - x=180: "First / Victims", T+1h, red dot.
  - x=310: "Vendor Gets / Sample", T+4h, red dot.
  - x=430: "Signature / Created", T+24h, red dot.
  - x=530: "Update / Pushed", T+48h, green dot.
  - x=640: "Endpoints / Protected", T+72h, green dot.
- **Dot colors:** first four events red `#e74c3c`, last two green `#27ae60`; 6px radius.
- **Bottom annotation (17px red `#e74c3c`, centered at x=290):** "Damage occurs HERE ↓".

## Per-Machine Behavioral Baseline

**One Universal Model Alerts on 85% of Developer Machines**

- **Normal for developers:** Compilers, debuggers, and scripts run constantly and legitimately.
- **Abnormal elsewhere:** The identical tool set on an accountant machine is highly suspicious.
- **Tuned loose:** A universal model hits 85% false positives on developers, 60% on IT admins.
- **Tuned tight:** The same threshold then misses real attacks on non-developer endpoints.
- **What to change:** Per-role baselines flatten alert rates to roughly 6-10% across roles.

### Visualization (canvas `canvas5`, 720×200)

Grouped bar chart comparing false positive rates of a universal model vs per-role baselines across five roles.

- **Title (bold 17px, `#1a5276`, centered):** "Alert Rates: Universal Model vs Per-Role Baseline".
- **Roles (x-axis):** Developer, Accountant, Executive, IT Admin, Intern — starting x=70, spacing 135px, baseline y=175, max height 110px = 100%.
- **Data:** universal-model FP% `[85, 5, 8, 60, 12]` in red `#e74c3c`; per-role-baseline FP% `[8, 7, 6, 10, 9]` in green `#27ae60`. Bars 20px wide, paired.
- **Value labels:** percentage above each bar in the bar's color (11px); role names below baseline in `#333` (13px).
- **Legend (top left):** red swatch "Universal Model (FP%)", green swatch "Per-Role Baseline (FP%)".

## Kernel/Ring-0 Rootkit Blindness

**A Ring-0 Rootkit Decides What the Ring-3 Sensor Is Allowed to See**

- **The asymmetry:** The rootkit holds Ring 0; the EDR agent runs in user-space Ring 3.
- **What gets hidden:** Process list, file system, and network stack are all filtered.
- **The reported result:** The agent says "all clear" because that is all the kernel shows it.
- **Why it is unfalsifiable:** Absence of alerts is indistinguishable from suppressed alerts.
- **The rule:** Never trust a sensor running inside the system it is meant to monitor.

### Visualization (canvas `canvas6`, 720×200)

Diagram: concentric privilege-ring circles on the left, bullet lists of what each layer sees on the right, with a warning banner.

- **Title (bold 17px, `#1a5276`, centered):** "Privilege Rings: What the EDR Agent Can See".
- **Ring diagram (left, centered at x=180, y=120):** three concentric circles of radii 75/55/35px, colors red `#e74c3c` (outer, Ring 0), orange `#f39c12` (Ring 1/2), blue `#3498db` (inner, Ring 3); each stroked 2px in its color and filled with the color at ~20% alpha (`color + '33'`). Ring labels ("Ring 0", "Ring 1/2", "Ring 3") in 12px in each ring's color; bold `#333` label "EDR Agent" at the center.
- **Right column (starting x=350):**
  - Bold 14px red heading "Rootkit (Ring 0) controls:" followed by three 17px `#333` bullets: "• Process list (hides malware)", "• File system (hides payloads)", "• Network stack (hides C2)".
  - Bold 14px green heading "EDR Agent (Ring 3) reports:" followed by one 17px `#666` bullet: "• \"All clear!\" (sees only what kernel allows)".
- **Connector:** dashed red arrow line (dash 5/3, 2px) from the ring diagram (x=270) to the right column (x=340) at y=100.
- **Bottom warning (bold 17px red, centered):** "⚠ Cannot trust sensor inside compromised system".

## Regeneration instructions

- **Layout:** standard detail-page pattern — h1 + `.subtitle`, then per pitfall an `<h2>` (1.4em, `#1a5276`, bottom border `2px solid #2980b9`) followed by a single-row `.obj-table`: full-width table, left `<td>` (40%) holds `.obj-title` (the one-line punchline) plus a `<ul>` of labeled bullets (`<strong>Label:</strong> phrase`, one line each), right `<td>` (60%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `ul { margin: 8px 0 8px 20px; font-size: 0.9em; color: #333; }` and `li { margin: 4px 0; }`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `strong` `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border `4px solid #2980b9`) but unused on this page. No nav bar, no back/home links.
- **Canvases:** each declared with inline `style="width:720px;height:200px;"`; a shared `setupCanvas(canvas, 720, 200)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Default chart font 17px system sans-serif; titles bold 17px.
- **Palette:** primary blue `#1a5276`, blue accents `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`, gray text `#666`/`#333`.
- Note: in regenerated HTML, any card/page links use `.html` extensions (this page has none).
