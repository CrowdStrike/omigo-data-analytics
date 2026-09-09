# Email/Spam Detection Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%; left cell holds a bold one-line punchline plus labeled bullets)
**HTML title tag:** Email/Spam Detection Pitfalls

**Subtitle:** Why spam filtering is an adversarial, label-noisy, non-stationary classification problem.

## Adversarial Text Evolution

**Accuracy Falls 97% → 68% in Eight Weeks Without Retraining**

- **The evasion kit:** Unicode lookalikes, zero-width characters, image-only bodies, HTML tricks.
- **The mechanism:** Every rule you ship tells the spammer exactly what to change next.
- **Decay curve:** 97% at launch, 93% by week 2, 85% by week 4, 76% by week 6, 68% by week 8.
- **Why it fails:** The adversary iterates in days; retraining cycles run in weeks.
- **What to change:** Treat a deployed model as perishable, not as a finished artifact.

### Visualization (canvas `canvas1`, 720×200)

Line chart: model accuracy decaying over the weeks after deployment.

- **Background:** full-canvas fill `#f8f9fa`.
- **Title (17px `#1a5276`, centered):** "Model Accuracy Decay After Deployment".
- **Axes:** left=80, right=w-30, top=40, bottom=h-35; dark axes `#333` width 1.5; y-axis labels (17px `#555`) 100%, 90%, 80%, 70%, 60% with light gridlines `#ddd`; x-axis labels: "Week 0", "Week 2", "Week 4", "Week 6", "Week 8".
- **Data (red `#e74c3c` line, width 3, with radius-5 dots):** accuracy per week label: `[97, 93, 85, 76, 68]` (y scaled 60–100).
- **Annotation (17px red, near top right):** "Spammers adapt".

## Legitimate Marketing ≈ Spam

**Opted-In Newsletters Score 0.85 on "Buy Now" — Spam Scores 0.92**

- **The overlap:** Urgency, links, and images appear at near-identical rates in both classes.
- **Not actually spam:** Promotional mail from opted-in services is consented, yet users report it.
- **Only real separator:** Unsubscribe presence — 0.95 for marketing vs 0.30 for spam.
- **Why it fails:** The boundary is a user preference, not a property of the message.
- **The consequence:** One person's wanted coupon is another person's junk, same bytes.

### Visualization (canvas `canvas2`, 720×200)

Grouped bar chart: feature-presence scores for spam vs legitimate marketing across five features.

- **Background:** `#f8f9fa`. **Title (17px `#1a5276`, centered):** "Feature Overlap: Marketing vs Spam".
- **Chart area:** left=100, right=w-40, top=40, bottom=h-30; bars 25px wide, paired per feature group.
- **Features (17px `#555` x labels):** `"Buy Now"`, `Urgency`, `Links`, `Images`, `Unsubscribe`.
- **Spam bars (fill `rgba(231,76,60,0.7)`):** 0.92, 0.88, 0.95, 0.80, 0.30.
- **Marketing bars (fill `rgba(46,204,113,0.7)`):** 0.85, 0.72, 0.90, 0.85, 0.95.
- **Legend (17px, top-left):** red swatch "Spam"; green swatch "Marketing".

## User Report Bias

**Same 15% Real Spam Gets Reported at 85%, 14%, or 2%**

- **Over-reporters:** Flag 85% of mail, including legitimate receipts and order confirmations.
- **Under-reporters:** Flag 2% — they delete annoyances instead of pressing the button.
- **Ground truth is flat:** All three user types actually receive the same 15% spam.
- **What the label means:** "Reported as spam" encodes who the user is, not what the mail is.
- **Why it fails:** Training on these labels inherits every bit of that human variance.

### Visualization (canvas `canvas3`, 720×200)

Grouped bar chart: reported spam rate vs actual spam rate for three user behavior types.

- **Background:** `#f8f9fa`. **Title (17px `#1a5276`, centered):** "Spam Report Rate by User Behavior Type".
- **Chart area:** left=120, right=w-40, top=45, bottom=h-35; y-axis 0–100% with labels every 25% (17px `#555`) and gridlines `#ddd`; bars 35px wide, paired per group.
- **Groups (17px x labels):** "Over-reporter", "Normal User", "Under-reporter".
- **Reported bars (solid red `#e74c3c`):** 85, 14, 2 (%).
- **Actual Spam bars (solid blue `#2980b9`):** 15, 15, 15 (%).
- **Legend (17px, top-right):** red swatch "Reported"; blue swatch "Actual Spam".

## Ham Corpus Decay

**A 2020 Ham Corpus Is 28% Relevant by 2024**

- **What ages:** The "good email" set, not just the spam set — new services, formats, platforms.
- **Relevance decay:** 100% in 2020, then 82%, 61%, 43%, down to 28% in 2024.
- **The mirror trend:** Unseen legitimate patterns rise from 5% to 88% over the same span.
- **Why it fails:** Unfamiliar legitimate mail looks anomalous, so it gets scored as spam.
- **The damage:** False positives on real mail, which users forgive far less than misses.

### Visualization (canvas `canvas4`, 720×200)

Dual line chart: ham-corpus relevance decaying while new unseen patterns rise, 2020–2024.

- **Background:** `#f8f9fa`. **Title (17px `#1a5276`, centered):** "Ham Corpus Relevance Over Time".
- **Axes:** left=80, right=w-30, top=40, bottom=h-35; dark axes `#333`; y-axis labels 100%, 75%, 50%, 25%, 0% with gridlines `#ddd`; x-axis labels 2020–2024.
- **Corpus relevance line (solid purple `#8e44ad`, width 3, radius-5 dots):** `[100, 82, 61, 43, 28]`.
- **New unseen patterns line (dashed green `#27ae60`, dash 6/4, width 3):** `[5, 22, 45, 68, 88]`.
- **Legend (17px, inside top-left as short line swatches):** purple "Corpus relevance"; green "New unseen patterns".

## Phishing vs Spam: Different Problems

**Missing One Phishing Mail Costs 100x Missing One Spam Mail**

- **Missed spam:** Annoying but harmless — the user deletes it and moves on, 1x cost.
- **Missed phishing:** Credential theft and account takeover — catastrophic, 100x cost.
- **Same channel:** Both arrive in one inbox, so both hit the same scoring pipeline.
- **Why it fails:** A uniform threshold optimizes an average that neither class actually wants.
- **What to change:** Separate the decision thresholds so cost asymmetry is priced in.

### Visualization (canvas `canvas5`, 720×200)

Horizontal bar chart: relative false-negative cost of missed spam vs missed phishing.

- **Background:** `#f8f9fa`. **Title (17px `#1a5276`, centered):** "False-Negative Cost Comparison".
- **Bars (35px tall, starting x=160, scaled to max 100):**
  - "Missed Spam" — 1x, amber `#f39c12`, value label "1x" in `#333` right of the bar.
  - "Missed Phishing" — 100x, dark red `#c0392b`, white in-bar label "100x cost".
- **Row labels:** 17px `#333`, right-aligned left of the bars.
- **Annotation (17px `#c0392b`, centered at bottom):** "Uniform threshold fails: catastrophic vs annoying".

## Sender Reputation Gaming

**A Burned Domain Costs Less Than the Two Weeks of Trust It Bought**

- **The lifecycle:** New domain at 50, trust built to 80, spam sent, detected at 30, burned at 5.
- **The payload window:** Roughly two weeks of sending before reputation collapses.
- **The reset:** Register a fresh domain and the same five phases start over from 50.
- **Why it fails:** No history is scored as clean history — trust-by-default is the exploit.
- **The root cause:** Reputation systems have a bootstrapping hole for every new identity.

### Visualization (canvas `canvas6`, 720×200)

Lifecycle line chart: domain reputation across five phases, with a dashed loop arrow showing the cycle repeating.

- **Background:** `#f8f9fa`. **Title (17px `#1a5276`, centered):** "Domain Reputation Gaming Lifecycle".
- **Chart area:** left=50, right=w-50, top=50, bottom=h-40; dark line `#2c3e50` width 3 connecting five evenly spaced points.
- **Phases (17px `#333` labels below; radius-8 colored dots; reputation score 0–100):**
  - "New Domain" — 50, blue `#3498db`
  - "Build Trust" — 80, green `#27ae60`
  - "Send Spam" — 75, amber `#f39c12`
  - "Detected" — 30, orange `#e67e22`
  - "Burned" — 5, red `#e74c3c`
- **Cycle arrow:** dashed gray quadratic curve (`#7f8c8d`, dash 4/4, width 2) from the "Burned" point arcing over the top back to the "New Domain" point; 17px gray label centered near the top: "Repeat with new domain".

## Regeneration instructions

- **Layout:** per pitfall, an `<h2>` section heading followed by a single-row `.obj-table`: full-width table, left `<td>` (40%) with `.obj-title` (a bold one-line punchline, not a repeat of the h2) and a `<ul>` of 4-5 `<li>` labeled bullets (`<strong>Label:</strong>` + short phrase, one line each); no example box. Right `<td>` (60%, centered) holds one canvas. Canvases have no intrinsic width/height attributes in the HTML — dimensions (720×200) are passed to the setup helper.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` margin 8px 0 8px 20px, 0.9em `#333`; `li` margin 4px 0; `strong` `#1a5276`. A `.philosophy` style (background `#f0f4f8`, left border `4px solid #2980b9`) is defined but unused. No nav bar, no back/home links.
- **Canvas:** shared `setupCanvas(canvas, width, height)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); all charts use a `#f8f9fa` background fill and 17px -apple-system font for titles, labels, and annotations.
- **Palette:** primary blue `#1a5276`, mid blue `#2980b9`, light blue `#3498db`, green `#27ae60` / `rgba(46,204,113,0.7)`, red `#e74c3c`, dark red `#c0392b`, amber `#f39c12`, orange `#e67e22`, purple `#8e44ad`, gray `#7f8c8d`/`#555`, gridlines `#ddd`.
