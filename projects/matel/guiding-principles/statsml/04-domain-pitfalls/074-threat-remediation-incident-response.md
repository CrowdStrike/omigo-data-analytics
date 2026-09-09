# Domain Pitfalls: Threat Remediation

**Page type:** detail page (h2 section headings, each followed by a one-row two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** Domain Pitfalls: Threat Remediation

**Subtitle:** Critical failure modes in incident response and threat elimination workflows

## Evidence Destroyed by Remediation

**Obj-title:** Evidence Destroyed by Remediation

- Security team kills malicious process → volatile memory evidence GONE forever.
- Wipes disk → forensic artifacts destroyed.
- The act of fixing destroys the ability to understand what happened.

### Visualization (canvas `canvas1`, 720×200 — declared 720×300 in markup, resized to 720×200 by the setup helper)

Line chart with filled area: evidence availability decaying across a remediation-action timeline.

- **Background:** full-canvas fill `#fdf2f2`.
- **Title (bold 14px `#1a5276`, at 20,22):** "Evidence Availability Over Remediation Timeline".
- **Axes:** L-shaped axis in `#2980b9` (width 1.5) from (60,40) down to (60,170) then right to (690,170). Rotated y-axis label "Evidence %" in `#5d6d7e` 12px; x-axis label "Remediation Actions" in `#5d6d7e` 12px at bottom center.
- **Y ticks:** 0%, 25%, 50%, 75%, 100% (11px `#5d6d7e`), each with a light horizontal gridline `#dfe6e9` (width 0.5) across the plot.
- **Data (x position, label, evidence %):** Detect (80, 100), Kill Process (180, 72), Wipe RAM (290, 40), Reimage Disk (400, 15), Reset Logs (510, 5), Restore (620, 2). Evidence scaled over 130px above baseline y=170.
- **Series:** filled area under curve `rgba(231,76,60,0.15)`; line `#e74c3c` width 2.5; 4px-radius red `#e74c3c` dots at each point; action labels 11px `#2c3e50` below the baseline at y=183.
- **Annotation:** bold 12px `#c0392b` text "IRREVERSIBLE LOSS" at (450,55) with a dashed `#c0392b` pointer line (dash 4/3, width 1) from (450,60) to (520,130).

## Scope Unknown

**Obj-title:** Scope Unknown

- Attacker compromised server A — did they ALSO compromise B, C, D? Maybe.
- Unknown without investigation that could take weeks.
- Business says "fix it now" → you patch A, attacker is still in B-D.

### Visualization (canvas `canvas2`, 720×200 — declared 720×300, resized by setup helper)

Network node diagram: known vs unknown compromise across connected servers.

- **Background:** `#f4f6f9`.
- **Title (bold 14px `#1a5276`):** "Compromise Scope: Known vs Unknown".
- **Nodes (22px-radius circles, `#2c3e50` outline width 1.5, white bold 10px centered labels):** Server A (120,100) compromised red `#e74c3c`; Server B (260,60) unknown orange `#f39c12`; Server C (260,140) unknown; Server D (400,80) unknown; Server E (400,150) clean green `#27ae60`; Server F (540,100) clean; Server G (540,50) unknown.
- **Connections:** gray `#bdc3c7` lines (width 1.5) between node pairs A-B, A-C, B-D, C-E, D-F, D-G, B-G.
- **Attack path:** red `#e74c3c` dashed lines (dash 5/3, width 2) overlaying A→B, A→C, B→D.
- **Legend (top right at 600,40, 12px):** color squares with labels — `#e74c3c` "Known compromised", `#f39c12` "Unknown / Maybe", `#27ae60` "Assumed clean".
- **Annotations:** italic 12px `#8e44ad` "Patching only A leaves attacker in B, C, D" at (100,185); 11px `#e74c3c` "--- Lateral movement" at (600,115).

## Re-infection from Persistence

**Obj-title:** Re-infection from Persistence

- Cleaned malware from host, but attacker planted: scheduled task, registry key, cron job, firmware implant.
- Reboot → re-infected within minutes.
- Must find ALL persistence before declaring "clean".

### Visualization (canvas `canvas3`, 720×200 — declared 720×300, resized by setup helper)

Bar sequence showing an infection-level cycle over repeated cleanups.

- **Background:** `#f9fdf4`.
- **Title (bold 14px `#1a5276`):** "Persistence Mechanism Re-infection Cycle".
- **Bars (70px wide, 105px segment pitch starting x=60+10, baseline at y=160, height = level × 0.9, `#2c3e50` outline width 0.5):** Infected 90% `#e74c3c`; Cleaned 10% `#27ae60`; Reboot 15% `#f39c12`; Re-infected 85% `#e74c3c`; Cleaned Again 10% `#27ae60`; Re-infected! 88% `#c0392b`. Each bar has its label (10px `#2c3e50`) below the baseline and its percent value above the bar. Thin `#2980b9` baseline under the bars.
- **Persistence list (right side, starting at 450,45):** heading "Hidden persistence:" in 12px `#8e44ad`, then bullets in `#c0392b`: "• Scheduled Task", "• Registry Key", "• Cron Job", "• Firmware Implant".
- **Loop indicator:** red `#e74c3c` circular arc arrow (radius 15, centered 360,30, width 2) with filled red arrowhead, next to bold 11px `#c0392b` text "CYCLE REPEATS" at (382,35).

## Business Pressure to Restore vs Investigate

**Obj-title:** Business Pressure to Restore vs Investigate

- Every hour of downtime = $X revenue loss.
- Business demands "restore service NOW"; security needs time to investigate scope.
- Compromise: restore service with attacker still possibly inside = worse than the original breach.

### Visualization (canvas `canvas4`, 720×200 — declared 720×300, resized by setup helper)

Dual-axis line chart: downtime cost vs investigation completeness over hours.

- **Background:** `#fefdf5`.
- **Title (bold 14px `#1a5276`):** "Downtime Cost vs Investigation Completeness".
- **Axes:** L-axis in `#2980b9` (width 1.5) from (70,40) to (70,170) to (650,170). X label "Hours of Downtime" (11px `#5d6d7e`), ticks 0h, 4h, 8h, 12h, 16h, 20h, 24h at 95px spacing. Left rotated axis label "Cost ($K)" in red `#e74c3c`; right rotated axis label "Scope Known %" in green `#27ae60`.
- **Cost series (red `#e74c3c`, width 2.5, exponential rise, scaled max 400):** [0, 15, 45, 95, 160, 250, 380] at the seven x ticks.
- **Investigation series (green `#27ae60`, width 2.5, logarithmic growth, scaled max 100):** [5, 25, 50, 68, 78, 85, 90].
- **Tension zone:** rectangle from (260,40) sized 200×130, fill `rgba(243,156,18,0.15)`, dashed `#f39c12` border (dash 4/3); centered labels in `#d35400`: bold 11px "TENSION ZONE" and 10px "Business vs Security".
- **Legend (right, ~x=510):** red line swatch + "Revenue Loss" (`#2c3e50` 12px); green line swatch + "Investigation %".

## Attribution Uncertainty

**Obj-title:** Attribution Uncertainty

- Was it nation-state, criminal gang, insider, or script kiddie?
- Attribution determines response: law enforcement? Diplomacy? Fire employee?
- If you attribute wrong: wrong response, wrong lessons learned.

### Visualization (canvas `canvas5`, 720×200 — declared 720×300, resized by setup helper)

Bar chart: probability distribution across candidate threat actors, each with a response label.

- **Background:** `#f5f0fa`.
- **Title (bold 14px `#1a5276`):** "Attribution Confidence Distribution".
- **Bars (110px wide, 30px gap, starting x=95, baseline y=150, height = prob × 100 × 2.8, fill at 0.8 alpha with solid outline of the same color, width 1.5):**
  - "Nation-State (APT)" 30%, `#8e44ad`, response label "Diplomacy".
  - "Criminal Gang" 35%, `#2980b9`, response "Law Enforcement".
  - "Insider Threat" 20%, `#d35400`, response "HR/Fire Employee".
  - "Script Kiddie" 15%, `#27ae60`, response "Patch & Ignore".
- **Labels:** bold 14px `#2c3e50` percent above each bar; two-line actor label (11px `#2c3e50`) below the baseline; italic 10px `#7f8c8d` response label beneath the actor label.
- **Warning (top right):** bold 11px `#c0392b` "Wrong attribution = Wrong response!" at (510,45) with a red `#e74c3c` dashed underline (dash 3/2, width 1.5) from (510,50) to (680,50).

## Collateral Damage from Containment

**Obj-title:** Collateral Damage from Containment

- Isolate compromised subnet → 500 legitimate users lose access → productivity loss → helpdesk overwhelmed.
- Containment actions have BUSINESS IMPACT; "just isolate everything" is not free.
- Every security action has cost.

### Visualization (canvas `canvas6`, 720×200 — declared 720×300, resized by setup helper)

Ripple/cascade diagram: containment action at center radiating business impacts, plus a vertical impact gauge.

- **Background:** `#fdf6f0`.
- **Title (bold 14px `#1a5276`):** "Containment Action: Cascading Business Impact".
- **Center node at (150,110):** filled red `#e74c3c` circle radius 25 with white bold 9px two-line label "ISOLATE" / "SUBNET"; three concentric red ripple circles (radii 50, 75, 100) at fading alpha (0.75, 0.5, 0.25).
- **Impact cascade (gray `#bdc3c7` arrows from center to labels at x=320):**
  - "500 Users Blocked" (y=40, `#c0392b`), metric "-100% productivity" (11px `#7f8c8d`).
  - "Helpdesk Overloaded" (y=85, `#d35400`), metric "+800% tickets".
  - "Revenue Pipeline Stalled" (y=130, `#8e44ad`), metric "-$2.1M/day".
  - "Customer SLA Breached" (y=175, `#2c3e50`), metric "3 contracts at risk".
- **Impact gauge (right, at 590,40, 30×140):** vertical linear gradient `#27ae60` → `#f39c12` (midpoint) → `#e74c3c`, `#2c3e50` outline; side labels "Low" (top), "Med" (middle), "Critical" (bottom) in 10px `#2c3e50`; two-line header above the bar in bold 11px `#1a5276`: "Business" / "Impact"; a red `#e74c3c` triangle marker on the left edge at 75% down the bar.

## Regeneration instructions

- **Layout:** standard domains detail page (139-style): h1, `.subtitle` paragraph, then per pitfall an unnumbered `<h2>` followed by a one-row `.obj-table` — left `<td>` (40%) with `.obj-title` + `<ul>` bullets, right `<td>` (60%, centered) with one `<canvas>`. No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-table` cells border `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border 4px solid `#2980b9`) but unused on this page.
- **Canvas:** markup declares `width="720" height="300"`; a shared `setupCanvas(id)` helper overrides to 720×200 CSS pixels and scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), default font 17px system sans-serif.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, dark red `#c0392b`, purple `#8e44ad`, grays `#5d6d7e`/`#7f8c8d`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
