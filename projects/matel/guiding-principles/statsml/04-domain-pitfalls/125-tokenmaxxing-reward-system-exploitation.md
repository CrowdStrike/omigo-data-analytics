# Tokenmaxxing / Reward System Exploitation

**Page type:** detail page (h2 heading per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 125. Tokenmaxxing / Reward System Exploitation

**Subtitle:** When any token/credit/point system exists, rational actors will optimize for maximum extraction — creating data that reflects gaming strategy, not genuine behavior.

## LLM Prompt Gaming for Max Token Output

**Obj-title:** Prompts Optimized to Extract Tokens, Not Quality

- **The tactic:** Content farms craft prompts like "Write a comprehensive 5000-word analysis..."
- **Verbose filler:** The model pads with restatement and hedging purely to hit the demanded length.
- **Training poison:** Training on this data teaches the model that verbosity = quality.
- **Platform cost:** $0.06/1K tokens times millions of gamed queries adds up to a massive spend.
- **Nothing bought:** That spend buys token volume, not one extra unit of quality in the output.

### Visualization (canvas `canvas1`, declared 720×300, script renders at 720×200)

Two-line chart: output tokens vs prompt length for normal users vs gaming prompts.

- **Background:** full `#eee` fill. **Axes:** `#2980b9` width 2; x-axis (60,170)–(700,170), y-axis (60,170)–(60,20).
- **Axis labels (`#1a5276`):** "Prompt Length (tokens)" at (300,195); rotated "Output Tokens" on the left.
- **Normal users line:** `#7fb3d3`, width 2, points `[[80,150],[150,140],[220,130],[290,125],[360,120],[430,118],[500,115],[570,112]]` (nearly flat).
- **Gaming prompts line:** `#c0392b`, width 3, points `[[80,145],[150,120],[220,80],[290,50],[360,35],[430,28],[500,22],[570,18]]` (steeply rising output).
- **Labels:** "Normal users" in `#7fb3d3` at (580,108); "Gaming prompts" in `#c0392b` at (560,35).
- **Font:** 17px -apple-system, sans-serif.

## API Pricing Arbitrage (Input Cheap, Output Expensive)

**Obj-title:** The Pricing Model Creates an Arbitrage That Rational Users Exploit

- **The asymmetry:** GPT-4 charges $0.03/1K input tokens against $0.06/1K on output — 2× the price.
- **The play:** Stuff the context with cheap input that forces expensive long output back out.
- **The prompt:** "Given this 50K word document, rewrite every paragraph..." is the canonical form.
- **The math:** That turns roughly $1.50 of input tokens into about $30 of billed output tokens.
- **The data trap:** Usage looks like genuine long-form work but is really a cost-optimization play.

### Visualization (canvas `canvas2`, declared 720×300, script renders at 720×200)

Grouped bar chart: input vs output cost per user, output growing much faster.

- **Background:** `#eee` fill; axes `#2980b9` width 2, x-axis (60,170)–(700,170), y-axis (60,170)–(60,20); x label "Users (sorted by input size)" in `#1a5276` at (280,195).
- **Bars:** 6 user groups at x = 100 + i*100; input bar (`#3498db`, 35px wide) height = 20 + i*5; output bar (`#c0392b`, 35px wide, offset +40px) height = 15 + i*20; baseline y=170.
- **Legend:** "Input $" in `#3498db` at (600,60); "Output $" in `#c0392b` at (600,80).
- **Annotation:** "Arbitrage zone" in `#1a5276` at (450,40) with a dashed `#c0392b` bracket (dash 4/3) from (440,45) to (540,45) down to (540,170).
- **Font:** 17px -apple-system, sans-serif.

## Loyalty Points/Miles Optimization Distorts "Customer Value"

**Obj-title:** "High-Value Customer" = "Best Arbitrageur"

- **Mileage runs:** A frequent flyer buys $200 mileage-run tickets to earn 50K miles.
- **The payoff:** Those 50K miles are worth roughly $500 in upgrades — a clean arbitrage margin.
- **Misleading data:** Airline data reads "valuable customer, flew 20 segments!" and ranks them top tier.
- **Cheapest routes:** They picked the cheapest possible flights specifically for mile accrual.
- **No real loyalty:** Their "loyalty" is mathematical optimization, not any brand preference.
- **Easily poached:** They switch to a competitor the day that program's math looks better.

### Visualization (canvas `canvas3`, declared 720×300, script renders at 720×200)

Paired bar chart: apparent value vs actual revenue across customer segments.

- **Background:** `#eee` fill; axes `#2980b9` width 2, x-axis (60,170)–(700,170), y-axis (60,170)–(60,20); x label "Customer Segments" in `#1a5276` at (320,195).
- **Bars:** 4 segments at x = 120 + i*145, 30px wide each; apparent value (`#2980b9`) heights `[120, 140, 90, 150]`; actual revenue (`#e74c3c`, offset +35px) heights `[120, 25, 90, 15]`; baseline y=170. (Segments 2 and 4 show large apparent/actual gaps — the arbitrageurs.)
- **Legend:** "Apparent value" in `#2980b9` at (550,40); "Actual revenue" in `#e74c3c` at (550,60).
- **Font:** 17px -apple-system, sans-serif.

## Crypto Airdrop Farming Creates Fake Usage

**Obj-title:** "10,000 Unique Users" = 10 Farmers With 1000 Wallets Each

- **The mechanism:** Protocols hand out tokens by "usage" — transactions, volume, unique wallets.
- **The farm:** One farmer spins up 1000 wallets and makes minimum transactions on every one.
- **All qualify:** Each wallet clears the threshold, so all 1000 collect the airdrop allocation.
- **Inflated metrics:** TVL, DAU, and transaction count all read about 100x above real usage.
- **The cliff:** When the airdrop ends, the "users" vanish overnight and the metrics collapse.

### Visualization (canvas `canvas4`, declared 720×300, script renders at 720×200)

Line chart: active users rising then collapsing off a cliff when the airdrop ends.

- **Background:** `#eee` fill; axes `#2980b9` width 2, x-axis (60,170)–(700,170), y-axis (60,170)–(60,20); x label "Active Users" (rotated y label) and "Time (weeks)" in `#1a5276` at (350,195).
- **Growth line:** `#27ae60`, width 3, points `[[80,150],[140,130],[200,100],[260,60],[320,35],[380,30],[410,30]]`.
- **Collapse line:** `#c0392b`, width 3, points `[[410,30],[415,145],[500,155],[580,158],[650,160]]` (near-vertical drop then flat low tail).
- **Annotation:** "Airdrop ends" in `#c0392b` at (415,20), with a vertical dashed `#999` line (dash 4/3) at x=410 from y=25 to y=170.
- **Font:** 17px -apple-system, sans-serif.

## Credit Card Churning Pollutes Acquisition Data

**Obj-title:** "Customer Acquisition Cost" Looks Great — Unit Economics Are Terrible

- **The offer:** A $500 sign-up bonus once the new cardholder spends $3000 within 3 months.
- **The play:** The churner manufactures that $3K spend with gift cards and prepaid bill payments.
- **The exit:** They collect the $500, then close the card in month 7 and stop spending entirely.
- **Misleading data:** Bank sees "acquired customer, spent $3K!" and books a healthy acquisition.
- **Real economics:** It is a net loss — the $500 bonus against roughly $50 earned back in fees.
- **The scale:** Churners run 15-20% of new sign-ups, so the funnel looks great and margin does not.

### Visualization (canvas `canvas5`, declared 720×300, script renders at 720×200)

Bar chart: acquisition funnel splitting genuine vs churner retention.

- **Background:** `#eee` fill; axes `#2980b9` width 2, x-axis (60,170)–(700,170), y-axis (60,170)–(60,20); x label "Acquisition Funnel" in `#1a5276` at (300,195).
- **Bars (80px wide, baseline y=170):** "New Sign-ups" at x=120, height 140, `#27ae60`; "Churners (18%)" at x=250, height 140, `#e67e22`; "Genuine Retained" at x=400, height 120, `#27ae60`; "Churner Retained" at x=530, height 25, `#e74c3c`. Bar labels in 14px `#333` below at y=185.
- **Legend (17px):** "Genuine" in `#27ae60` at (600,40); "Churners" in `#e67e22` at (600,60).

## Free Tier Abuse Creating False Engagement

**Obj-title:** A Vanity Metric Layer That Obscures True Business Health

- **The gap:** A SaaS free tier reports "1000 active users!" as its headline engagement number.
- **Never convert:** Of those, 950 are developers on personal projects who never become paying users.
- **Hidden costs:** Free users consume support resources and fill the forums with tickets and threads.
- **Vanity inflation:** That activity inflates "community" metrics reported upward as business health.
- **Two answers:** "How many users?" → 1000; "how many paying?" → 50, and only the second is real.

### Visualization (canvas `canvas6`, declared 720×300, script renders at 720×200)

Centered funnel chart: four horizontally-centered stage bars narrowing sharply.

- **Background:** `#eee` fill; title "Conversion Funnel" in 17px `#1a5276` at (300,18).
- **Stages** (35px tall bars, centered horizontally, stacked from y=30 with 42px steps, white label text inside):
  - "Free Sign-ups (1000)" — width 600, `#3498db`
  - "Active Free (950)" — width 500, `#2980b9`
  - "Trial Start (80)" — width 120, `#e67e22`
  - "Paying (50)" — width 50, `#27ae60`
- **Annotations (15px `#c0392b`):** "95% never convert" at (520,75); "5% paying = true metric" at (520,165).

## Referral Program Gaming (Self-Referral Loops)

**Obj-title:** "Viral Coefficient 1.8!" — Mostly Circular Gaming

- **The offer:** "Invite a friend, both get $10" pays out on each side of a claimed referral.
- **Self-referral:** A user creates a second account, "refers" themselves, and collects the full $20.
- **At scale:** Referral rings form where 5 people refer each other cyclically to farm the payout.
- **The reported number:** Viral coefficient reads 1.8, which looks like textbook organic growth.
- **The real number:** Genuine referrals are only 0.3 once circular and self-referred pairs are removed.
- **Negative economics:** The company pays $20 per fake referral, so the best-looking metric loses money.

### Visualization (canvas `canvas7`, declared 720×300, script renders at 720×200)

Network diagram: genuine referral tree (left) vs circular self-referral ring (right).

- **Background:** `#eee` fill; title "Referral Network" in 17px `#1a5276` at (300,18).
- **Genuine tree (green `#27ae60`):** root node (radius 8) at (150,100); 5 branch nodes (radius 6) at `[[180,60],[210,120],[180,150],[240,80],[240,140]]`, each connected to the root by a green line (width 2). Label "Genuine (tree)" in `#333` at (100,185).
- **Gaming ring (red `#c0392b`):** 6 nodes (radius 8) at `[[450,60],[530,60],[560,100],[530,140],[450,140],[420,100]]` connected in a closed cycle by red lines (width 2). Label "Gaming (circular)" in `#333` at (430,185); "Self-referral loops" in `#c0392b` at (420,20).
- **Font:** 17px -apple-system, sans-serif.

## Any Points/Credits System Changes the Behavior It Measures

**Obj-title:** You're Measuring Your Own Incentive Design, Not Organic Behavior

- **The principle:** The moment you reward behavior X, people do X FOR THE REWARD, not the original reason.
- **Q&A reputation:** People answer easy questions (high volume), not hard questions (high value).
- **Contribution graphs:** People make trivial commits for no reason but to keep the streak green.
- **Surge pricing:** Drivers log off during surge to INCREASE the surge, then log back on to collect it.
- **Manufactured pattern:** The incentive system CREATES the pattern you then measure in the data.

### Visualization (canvas `canvas8`, declared 720×300, script renders at 720×200)

Side-by-side histogram panels: behavior distribution before vs after an incentive is introduced.

- **Background:** `#eee` fill; two axis frames in `#2980b9` width 2: left panel x-axis (60,170)–(340,170) with y-axis (60,170)–(60,20); right panel x-axis (390,170)–(700,170) with y-axis (390,170)–(390,20).
- **Panel titles (17px `#1a5276`):** "Before Incentive" at (130,18); "After Incentive" at (480,18).
- **Before bars (`#3498db`, 22px wide at x = 70 + i*26):** heights `[20,35,55,70,80,75,60,40,25,12]` (smooth organic bell shape).
- **After bars (`#c0392b`, 24px wide at x = 400 + i*28):** heights `[5,8,10,8,5,5,8,90,95,85]` (flat, then spike bunched at the reward threshold).
- **Annotation:** "Reward threshold" in 14px `#c0392b` at (580,60), with a vertical dashed `#999` line (dash 4/3) at x=595 from y=65 to y=170.

## Regeneration instructions

- **Layout:** for each pitfall: an `<h2>` section heading (1.4em, `#1a5276`, 2px solid `#2980b9` bottom border), then a `.obj-table` (full-width, border-collapse) containing one `<tr>`; left `<td>` (40%) holds `.obj-title` div + `<ul>` bullets with bold lead-in labels (`<strong>` in `#1a5276`); right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px. (This page's bullets carry the example inline; there is no separate Example paragraph.)
- **Page style:** body -apple-system/BlinkMacSystemFont/'Segoe UI' sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; p 0.95em `#333`; `.obj-title` 1.05em weight 600 `#1a5276`. A `.philosophy` class exists (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but is unused on this page. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"`, but each chart's IIFE sets the backing store to 720×200 × `window.devicePixelRatio` to 720×200px, and calls `ctx.scale` so drawing stays in logical coordinates. All charts paint an `#eee` full-canvas background. Default font 17px -apple-system, sans-serif.
- **Palette:** primary blue `#1a5276`, secondary blues `#2980b9`/`#3498db`/`#7fb3d3`, green `#27ae60`, red `#e74c3c`, dark red `#c0392b`, orange `#e67e22`, gray text `#666`/`#333`.
- Note: in regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
