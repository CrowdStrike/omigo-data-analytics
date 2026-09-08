# Cybersecurity Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Cybersecurity Data Pitfalls

**Subtitle:** Security data presents unique challenges: adversaries actively work to defeat models, attacks are vanishingly rare in normal traffic, and ground truth often arrives months after the fact.

## Adversarial Adaptation

**Obj-title:** 97% Recall Fell to 61% With No Change to the Model

- Attackers observe detection signals and modify their techniques specifically to evade models
- Model accuracy degrades predictably as threat actors iterate on evasion strategies each quarter
- Static models trained on historical attacks become progressively blind to new TTPs
- The attacker-defender feedback loop means past performance never guarantees future detection
- Retraining cadence must outpace adversary adaptation speed or detection gaps emerge

**Example:** A phishing detection model trained on 2022 campaigns achieved 97% recall. By Q3 2023, adversaries shifted to QR-code-based payloads and AI-generated text, dropping recall to 61% without any change in model parameters.

### Visualization (canvas `canvas1`, 720×240)

Line chart of model accuracy decaying over quarters as attackers evolve.

- **Title (top center, `#1a5276`, 17px):** "Model Accuracy vs Attacker Evolution".
- **Data:** quarters `['Q1-23','Q2-23','Q3-23','Q4-23','Q1-24','Q2-24','Q3-24','Q4-24']`; accuracy `[0.97, 0.93, 0.85, 0.88, 0.79, 0.72, 0.76, 0.64]`; new-technique flags `[1, 0, 1, 0, 1, 0, 1, 0]` (1 = new attack technique introduced that quarter).
- **Axes:** margins top 30 / right 20 / bottom 40 / left 60; y-axis from 50% to 100% with labels at 50%, 62%/63% steps (5 gridlines at 0.5 + i*0.125, i=0..4, shown as whole percents), light `#eee` gridlines; x-axis one label per quarter; axis lines `#333`.
- **Series:** accuracy line `#2980b9`, width 2.5, dots at each point — radius 7 red `#e74c3c` on new-technique quarters with red label "New TTP" above the dot, radius 5 blue `#2980b9` otherwise.
- **Legend (top right):** blue dot "Detection Accuracy"; red dot "New Attack Technique"; label text `#333` 12px.

## Extreme Class Imbalance

**Obj-title:** A 0.001% False Positive Rate Buries 5 Attacks in 500K Alerts

- Real attack traffic constitutes roughly 1 in 10 million network events
- Even extremely low false positive rates generate thousands of daily alerts at scale
- Precision collapses when the base rate of attacks is orders of magnitude below normal traffic
- Analyst fatigue from false positives leads to genuine alerts being ignored or deprioritized

**Example:** A SOC processing 50 billion events/day with a 0.001% false positive rate still generates 500,000 false alerts daily. The 5 real attacks are buried in noise, and analysts develop "alert blindness."

### Visualization (canvas `canvas2`, 720×240)

Horizontal bar chart on a log10 scale comparing alert volumes.

- **Title (top center, `#1a5276`, 17px):** "Alert Volume: Real Attacks vs False Positives".
- **Bars (log10-scaled widths, max = log10(50,000,000,000), bar width = logVal/logMax × 70% of plot width):**
  - "Total Events/Day" — value 50,000,000,000, gray `#bdc3c7`, value label "50B".
  - "Flagged Alerts" — value 500,000, red `#e74c3c`, value label "500K".
  - "True Attacks" — value 5, green `#27ae60`, value label "5".
- **Layout:** margins top 35 / right 20 / bottom 45 / left 80; category labels right-aligned left of bars (`#333`, 13px); value labels bold `#1a5276` at bar ends.
- **Annotation (bottom center, red `#e74c3c`, 12px):** "At 0.001% FP rate: 500K false alerts bury 5 real attacks".

## Ground Truth Delay

**Obj-title:** Breach on Day 0, Training Label on Day 240

- The median time from initial compromise to detection is approximately 200 days
- Labels for training data only become available months after the actual intrusion event
- Models trained on "clean" data may actually be trained on silently compromised environments
- Retroactive labeling introduces survivorship bias - only detected breaches get labeled
- Continuous validation is impossible when you cannot confirm true negatives in real time

**Example:** The SolarWinds SUNBURST backdoor was active for 14 months before discovery. Any model trained on "normal" network data from that period was unknowingly learning attacker C2 patterns as legitimate traffic.

### Visualization (canvas `canvas3`, 720×240)

Horizontal timeline with event markers from breach to label.

- **Title (top center, `#1a5276`, 17px):** "Ground Truth Timeline: From Breach to Label".
- **Timeline:** horizontal line `#333` width 2, from x=80 to x=660 at y=120.
- **Events (8px-radius colored circle markers; labels alternate above/below the line, day labels bold in the marker color):**
  - x=80 (start): "Initial Compromise" / "Day 0" — red `#e74c3c`.
  - 30% along: "Lateral Movement" / "Day 45" — orange `#e67e22`.
  - 55% along: "Data Exfiltration" / "Day 120" — red `#e74c3c`.
  - 75% along: "Detection" / "Day 200" — green `#27ae60`.
  - x=660 (end): "Labeled for Training" / "Day 240" — blue `#2980b9`.
- **Dwell bracket:** red `#e74c3c` bracket (width 1.5) below the timeline spanning from Day 0 to Day 200 (start to 75% mark), labeled in bold red 12px centered under it: "200 days undetected (median dwell time)".

## Base Rate Amplification

**Obj-title:** 99.9% Accuracy Still Leaves an Alert Queue 99% Noise

- Positive Predictive Value (PPV) depends heavily on prevalence, not just accuracy
- At 0.001% attack rate, even 99.9% sensitivity and specificity yields PPV of only ~1%
- Most "detected attacks" are actually false positives masquerading as real threats
- Reporting sensitivity/specificity without base rate context misleads stakeholders

**Example:** A vendor claims 99.9% detection accuracy. In a network with 0.001% actual attack traffic: for every true positive, there are ~100 false positives. The alert queue is ~99% noise despite "99.9% accuracy."

### Visualization (canvas `canvas4`, 720×240)

Line chart of PPV as a function of attack prevalence on a log-scale x-axis.

- **Title (top center, `#1a5276`, 17px):** "Positive Predictive Value vs Attack Prevalence (99.9% Sens & Spec)".
- **Curve data:** computed as PPV = (sens × prev) / (sens × prev + (1−spec) × (1−prev)) with sens = 0.999, spec = 0.999, prevalence swept over 10^p for p from −5 to −1 in steps of 0.1; line `#2980b9`, width 2.5.
- **Axes:** margins top 30 / right 20 / bottom 40 / left 60; y-axis 0–100% with gridlines every 25% (`#eee`), rotated y-axis label "PPV"; x-axis log-scale tick labels `0.001%`, `0.01%`, `0.1%`, `1%`, `10%`, axis caption "Attack Prevalence (log scale)" bottom center.
- **Critical point:** red `#e74c3c` 6px dot at the leftmost point (0.001% prevalence) with bold red label "PPV ≈ 1% at 0.001% prevalence!".
- **50% reference:** horizontal dashed orange `#e67e22` line (dash 5/5) at PPV = 50%, with bold orange 11px label "PPV reaches 50% only at 0.1% prevalence" placed at ~52% of plot width just above the line.

## Environment-Specific Baselines

**Obj-title:** An Office-Trained Model Alerts on Every Modbus Packet

- "Normal" network behavior varies dramatically between organizations and even internal segments
- Models trained on one environment produce excessive false positives when deployed to another
- Port usage, protocol distribution, and traffic patterns are organization-specific fingerprints
- Threat models that work for finance may be meaningless for healthcare or manufacturing
- Transfer learning from one network to another requires careful domain adaptation

**Example:** A model trained on corporate office traffic flags all OT/SCADA communication as anomalous when deployed to a manufacturing plant. Modbus and DNP3 protocols, perfectly normal in ICS environments, trigger constant alerts.

### Visualization (canvas `canvas5`, 720×240)

Grouped vertical bar chart comparing protocol distributions across two environments.

- **Title (top center, `#1a5276`, 17px):** ""Normal" Traffic: Corporate Office vs Manufacturing Plant".
- **Categories (x-axis, 11px labels):** `['HTTP/S', 'DNS', 'SSH', 'SMB', 'Modbus', 'DNP3', 'MQTT']`.
- **Series values (percent):** Corporate Office `[65, 15, 8, 10, 0.1, 0.05, 0.2]` in blue `#2980b9`; Manufacturing Plant `[10, 5, 3, 2, 40, 25, 15]` in red `#e74c3c`.
- **Layout:** margins top 40 / bottom 30 / left 60; bar group width 80, bar width 30, 3px gap between the paired bars; y-axis 0–65% with labels at 0%, 22%, 43%, 65% (i × 65/3 rounded).
- **Legend (top right):** blue square "Corporate Office"; red square "Manufacturing Plant".

## Regeneration instructions

- **Layout:** standard detail-page structure: h1, `.subtitle` paragraph, then per pitfall an `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `.obj-table` with a single `<tr>`: left `<td>` (40%) contains `.obj-title` + `<ul>` bullets + `.example` callout, right `<td>` (60%, centered) contains the canvas. Even table rows have background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; bullets 0.9em `#333`; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Example callout:** `.example` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 10px 14px, 0.9em. (A `.philosophy` class with 4px border also exists in the stylesheet but is unused on this page.)
- **Canvas:** all canvases 720×240, declared with intrinsic `width`/`height` attributes and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), one IIFE per chart.
- **Palette:** primary blue `#1a5276`, chart blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray `#bdc3c7`, text `#333`/`#666`.
