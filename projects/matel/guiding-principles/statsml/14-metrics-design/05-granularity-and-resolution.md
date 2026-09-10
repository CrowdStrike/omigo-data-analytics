# Metric Granularity — Right Denominator & Aggregation Level

**Page type:** detail page (numbered h2 sections, each with a two-column obj-table row: text left 40%, canvas right 60%; closing philosophy callout)
**HTML title tag:** Metric Granularity — Choosing the Right Denominator & Aggregation Level

**Subtitle:** The SAME metric computed at different levels (page, session, user, day) gives different numbers AND different conclusions. Choosing wrong = wrong decisions.

## 1. The Problem: Same Metric, Different Level, Different Answer

**CTR at Page Level vs Session Level vs User Level**

- **Page-level CTR:** clicks / impressions per page load. User sees 10 items, clicks 1 → CTR = 10%. Same user, 5 page loads, 1 click total → page-level CTR = 2% (1 click / 50 impressions).
- **Session-level CTR:** did the user click ANYTHING this session? Yes/No. Same user: clicked 1 item → session CTR = 100% (they clicked!).
- **User-level CTR:** across ALL sessions this month, what fraction of impressions got clicks? Maybe 0.5% (10 clicks / 2000 impressions).
- **Three numbers:** 2%, 100%, 0.5%. All "correct." All measuring "CTR." All lead to different conclusions about product health.

**The question that determines the level:** What DECISION are you making? If "is this page layout good?" → page-level. If "did the user find what they wanted?" → session-level. If "how engaged is this user over time?" → user-level. Wrong level = wrong answer to the question you're actually asking.

### Visualization (canvas `c1`, 720×300)

Three labeled value boxes plus a warning line.

- **Title (bold 17px `#1a5276`, centered at top):** "Same \"CTR\" — Three Levels, Three Different Numbers".
- **Boxes:** three 190×90 rectangles at x=80/300/520, y=55, each with a 20%-alpha fill and 2px stroke of its color, a bold 24px value and a 17px `#333` name: Page-level "2%" blue `#2980b9`; Session-level "100%" green `#27ae60`; User-level (monthly) "0.5%" purple `#8e44ad`.
- **Bottom line (bold 17px red `#e74c3c`, centered):** "All \"CTR.\" All \"correct.\" All lead to different decisions."

## 2. Common Aggregation Levels

**E-Commerce: At Least 7 Levels That Give Different Answers**

- **Impression level:** each item shown = one row. CTR per impression. Denominator = item views.
- **Page level:** each page load = one row. Actions per page. Denominator = page views.
- **Search query level:** each search = one row. Click-through per query. Denominator = searches.
- **Session level:** each visit = one row. Conversion per session. Denominator = sessions.
- **User level:** each unique user = one row. Conversion per user. Denominator = unique users.
- **Order level:** each transaction = one row. AOV, items per order. Denominator = orders.
- **Day level:** each calendar day = one row. Revenue/day, sessions/day. Denominator = days.

**Conversion rate:** At session level = 3% (3 purchases per 100 sessions). At user level = 8% (8 of 100 users bought this month, some in multiple sessions). At page level = 0.1% (1 purchase per 1000 page views). ALL "conversion rate." Completely different numbers.

### Visualization (canvas `c2`, 720×240)

Bar chart of "conversion rate" values across seven aggregation levels.

- **Title (bold 17px `#1a5276`, centered):** "E-Commerce: 7 Aggregation Levels for \"Conversion Rate\"".
- **Bars:** seven equal-width slots across w−100 px starting at x=50, fill `rgba(41,128,185,0.4)`, baseline y=200; heights proportional to the numeric rate ×15 (clamped 15–140px). Levels and value labels (bold 17px `#1a5276` above bar; names 17px `#333` at y=210): Impression 0.02%; Page 0.1%; Query 0.8%; Session 3%; User 8%; Order "—"; Day "$50K/day".
- **Bottom line (bold 17px red, centered):** "Same behavior measured at different levels = 0.02% to 8% (400× difference)".

## 3. Denominator Errors That Change Conclusions

**Wrong Denominator = Misleading Metric**

- **"Revenue per user" using total users (includes inactive):** $50M revenue / 1M total users = $50/user. But: 700K haven't been active in 6 months. Active users: 300K. Revenue per ACTIVE user = $167. Which guides decisions better? Depends on whether you're evaluating monetization (active) or total base value (all).
- **"Click-through rate" denominator = page views vs impressions:** Page has 20 items. User clicks 1. CTR by page = 100% (clicked on this page). CTR by impression = 5% (1/20 items clicked). Same behavior, 20× different CTR.
- **"Conversion rate" denominator = visits vs visitors:** User visits 5 times, buys on visit 5. Visit-based: 20% conversion (1/5). Visitor-based: 100% (they converted!). The visit-based makes product look bad; visitor-based makes it look great.
- **"Error rate" denominator = requests vs users:** 1 user hits error 100 times (retry loop). Request-level: 100 errors / 10,000 requests = 1%. User-level: 1 user / 1000 users = 0.1%. The request metric inflates one user's problem into a "significant error rate."

**Rule:** Always state the denominator explicitly. "Conversion rate" alone is meaningless — "conversions per unique visitor per 30 days" is precise enough to be useful.

### Visualization (canvas `c3`, 720×300)

Text-and-bars diagram of the revenue-per-user denominator switch.

- **Title (bold 17px `#1a5276`, centered):** "Wrong Denominator Changes the Conclusion".
- **Fact line (17px `#333` at (60, 55)):** "Revenue: $50M. Total users: 1M. Active users: 300K.".
- **Bars:** red `#e74c3c` filled bar (300×35 at (60,70)) with white bold 17px centered text "Revenue/total user: $50"; green `#27ae60` bar (300×35 at (60,115)) with "Revenue/active user: $167".
- **Explanation (17px `#333`):** "Same revenue. Different denominator. 3× different \"per user\" value.".
- **Bottom line (bold 17px red, centered):** "$50 says \"low ARPU, need more users.\" $167 says \"monetization is strong.\"".

## 4. Simpson's Paradox from Wrong Aggregation

**Metric Improves at Every Sub-Level But WORSENS in Aggregate**

- **Conversion rate by device:** Mobile: 2.0% → 2.2% (improved!). Desktop: 4.0% → 4.3% (improved!). Overall: 3.5% → 3.4% (DECLINED!)
- **How?** Mobile traffic share grew from 40% → 60%. Mobile converts lower. Even though BOTH improved individually, the MIX shifted toward the lower-converting channel.
- **The trap:** Report "overall conversion declined" → panic → investigate → wrong conclusion ("something broke"). Reality: nothing broke. Mix shifted. Each channel is BETTER. The aggregate hides this.
- **Reverse trap:** "Overall improved!" But: desktop (declining traffic) improved, mobile (growing traffic) got worse. Aggregate looks fine but mobile users (your future) are having a worse experience.

**Rule:** NEVER report only aggregate metrics for heterogeneous populations. Always segment by: device, channel, new vs returning, geo. If segments move opposite to aggregate → Simpson's Paradox is active. The aggregate is lying.

### Visualization (canvas `c4`, 720×300)

Text diagram of the paradox numbers.

- **Title (bold 17px `#1a5276`, centered):** "Simpson's Paradox: Each Segment Improves, Aggregate Declines".
- **Lines (left-aligned at x=60):** 17px `#333` "Mobile: 2.0% → 2.2% ✓ improved" (y=55) and "Desktop: 4.0% → 4.3% ✓ improved" (y=80); bold 17px red "Overall: 3.5% → 3.4% ✗ declined!" (y=110); 17px `#555` "How? Mobile share grew 40% → 60%. Lower-converting channel grew → aggregate dropped." (y=145).
- **Bottom line (bold 17px red, centered):** "Nothing broke. Mix shifted. Aggregate lies. ALWAYS segment.".

## 5. Which Level for Which Metric?

**Decision Guide: Match Level to Question**

- **"Is this search result page good?"** → Query-level (did THIS search lead to a click?)
- **"Is this user finding value?"** → User-level over time (do they keep coming back?)
- **"Is this page layout effective?"** → Page-level (actions per page view)
- **"How's the business doing?"** → Day/week level (revenue trend, not individual sessions)
- **"Is this product listing converting?"** → Item-level (views of THIS item → purchases of THIS item)
- **"Did this A/B test work?"** → User-level (because session-level double-counts returning users)
- **"Is our ranking algorithm good?"** → Query-level (per-search satisfaction)
- **"Should we invest more in mobile?"** → Device-level split (never blended with desktop)

**The mistake:** Picking the level that makes the number look best. "Conversion rate" at user-level (higher) reported when session-level (lower, more honest) would show the real friction. The LEVEL is a choice with political implications.

### Visualization (canvas `c5`, 720×240)

Question-to-level mapping list.

- **Title (bold 17px `#1a5276`, centered):** "Match Level to Question".
- **Rows (36px pitch from y=45):** question 17px `#333` left-aligned at x=60, answer bold 17px green `#27ae60` right-aligned at x=660 with "→ " prefix: "Is this page good?" → "Page-level"; "Is user finding value?" → "User-level"; "Is search working?" → "Query-level"; "How's business?" → "Day/week"; "Did A/B test work?" → "User-level (not session!)".
- **Bottom line (bold 17px red, centered):** "Wrong level = answering the wrong question with a precise wrong number.".

## 6. Time Granularity: Same Metric, Different Timeframe

**Hourly vs Daily vs Weekly vs Monthly — Different Stories**

- **Hourly revenue:** Spiky. 80% concentrated in 4 hours. Looks volatile/unstable.
- **Daily revenue:** Smooth-ish. Weekday/weekend pattern visible. Looks predictable.
- **Monthly revenue:** Smooth line going up. Looks like steady growth. Hides all intra-month drama.
- **Annual revenue:** One number. "Revenue grew 15%." Hides: which months? Which products? Which channels?

**The trap:** Monthly churn = 5% sounds manageable. Annual: (1-0.05)^12 = 54% annual retention = lost HALF your customers. Same metric, different timeframe, opposite FEELING.

**Rule:** Report at the FINEST granularity where patterns are visible, then aggregate UP for executive summary. Never report ONLY the aggregate — it hides the structure that drives decisions.

### Visualization (canvas `c6`, 720×300)

Text diagram of the churn compounding math.

- **Title (bold 17px `#1a5276`, centered):** "Monthly Churn 5% Sounds OK. Annual = Lost Half Your Customers.".
- **Lines (17px `#333` left-aligned at x=60):** "Monthly churn: 5%. Sounds manageable. \"Only 5%!\"" (y=55); "Annual retention: (1 - 0.05)^12 = 0.54 → LOST 46% of customers per year." (y=85).
- **Callout (bold 17px red, centered, y=120):** "Same metric. Different timeframe. Opposite emotional reaction.".
- **Rules (17px `#555`, centered):** "Rule: report finest granularity where patterns visible. Aggregate UP for summary." (y=155); "Never ONLY the aggregate — it hides the structure that drives decisions." (y=178).

## 7. Real-World Disasters from Wrong Granularity

**Cases Where Granularity Choice Caused Wrong Decisions**

- **Uber "surge pricing":** Measured demand at CITY level → "demand is high" → surge everywhere. Reality: demand high in ONE neighborhood, normal everywhere else. Granularity too coarse → over-surged 90% of city.
- **Streaming service "watch time":** Measured per-ACCOUNT. Household of 4 → looks like one VERY active user. Personalization: confused (mixing preferences of 4 people). Should be: per-profile, not per-account.
- **Hospital "average wait time":** Measured across all departments. ER: 3 hours. Radiology: 10 minutes. Combined "average wait: 45 min" — a number nobody experiences. Should be: per-department.
- **Ad campaign ROI:** Measured at CAMPAIGN level = positive. But: 80% of conversions came from brand-search ads (people already going to buy). Non-brand ROI = negative. Campaign-level hid that most spend was wasted on non-brand. Should be: per-audience/keyword segment.

**Pattern:** Coarse granularity → average across heterogeneous segments → average is misleading → decisions based on average hurt the specific segments that needed different treatment.

### Visualization (canvas `c7`, 720×300)

Case list with mistake and fix per row.

- **Title (bold 17px `#1a5276`, centered):** "Wrong Granularity → Wrong Decision".
- **Rows (38px pitch from y=45; company bold 17px red at x=50, mistake 17px `#333` at x=130, fix green `#27ae60` below it):** Uber — "City-level demand → surge everywhere" / "Fix: Neighborhood-level"; Streaming service — "Account-level → mixed 4 people's prefs" / "Fix: Profile-level"; Hospital — "All-dept avg wait → meaningless 45min" / "Fix: Per-department"; Ad campaign — "Campaign-level ROI positive" / "Fix: Per-keyword (brand vs non-brand)".
- **Bottom line (bold 17px `#555`, centered):** "Coarse granularity averages away the signal that would inform the RIGHT decision.".

## 8. A/B Testing: Where Granularity Errors Are FATAL

**Session-Level vs User-Level Randomization**

- **Session-level randomization:** Each session randomly assigned to A or B. Same user can be in A on Monday, B on Tuesday. "Conversion rate" = conversions/sessions. Problem: user saw BOTH variants → you don't know which caused the purchase.
- **User-level randomization:** User permanently assigned to A or B. All their sessions are in one group. "Conversion rate" = converters/users. Correct: user only ever saw one variant → causal attribution possible.
- **The mistake that's everywhere:** Teams randomize at session-level for "faster data collection" (more sessions than users). But: returning users contaminate both groups. Effect size measured is DILUTED or INFLATED depending on whether crossover users convert more or less.
- **Real example:** Variant B showed a bigger "Buy Now" button. Session-level test: B wins by 0.3% (p=0.04). Switch to user-level: effect disappears. Why? Power users visited 8× more, were in BOTH groups, bought regardless of button size. Their session volume inflated B's numbers.

**Rule:** ALWAYS randomize at user-level for A/B tests. Session-level = contaminated by crossover. The extra "speed" from more sessions is fake — it's measuring noise from the same users appearing in both groups.

### Visualization (canvas `c8`, 720×220)

Side-by-side comparison boxes: session-level vs user-level randomization.

- **Title (bold 17px `#1a5276`, centered):** "A/B Test: Session vs User Randomization".
- **Left box (310×80 at (30,45), red `#e74c3c` 15%-alpha fill, 2px red stroke):** bold red "✗ Session-level"; `#333` 17px lines "User₁ Mon→A, Tue→B, Wed→A" and "Contaminated: saw BOTH variants".
- **Right box (310×80 at (380,45), green `#27ae60` 15%-alpha fill, 2px green stroke):** bold green "✓ User-level"; lines "User₁ → always A (all sessions)" and "Clean: causal attribution possible".
- **Bottom lines (centered):** bold 17px red "Session randomization = \"faster\" but CONTAMINATED. Effect size = fiction." (y=155); 17px `#555` "Power users (8× sessions) dominate both groups → dilute real effect to noise." (y=180) and "Always randomize at USER level. Always measure at USER level." (y=200).

## 9. A/B Test Metric Level: What Are You Actually Measuring?

**Per-Session Metric vs Per-User Metric in A/B Tests**

- **Revenue per session:** Group A = $12/session. Group B = $14/session. B wins! But: B just happened to get users who visit less frequently but buy more per visit. Per-USER revenue: A = $85/month, B = $84/month. A actually wins. The session metric was confounded by visit frequency.
- **Pages per session:** A = 4.2 pages. B = 3.8 pages. "B reduced engagement!" But: B's change made users find what they wanted FASTER. Fewer pages = better experience. Pages/session is wrong metric for "did we help the user?"
- **Click-through rate per impression:** A = 2.1%. B = 2.4%. B wins! But: B just shows FEWER impressions per page (less clutter). Same NUMBER of clicks, smaller denominator → higher rate. Clicks per USER per day: identical. B didn't improve clicking — it improved CTR by shrinking the denominator.

**The A/B trap:** When you change the DENOMINATOR between variants (more/fewer impressions, sessions, page loads), you change the metric mechanically without changing user behavior. Always: same denominator definition in both groups, and that denominator is "users" or "user-days" — not sessions/impressions which the variant itself can inflate.

### Visualization (canvas `c9`, 720×220)

Text comparison of the same test scored at two metric levels.

- **Title (bold 17px `#1a5276`, centered):** "Same A/B Test: Metric Level Flips the Winner".
- **Lines (left-aligned at x=60):** 17px `#333` "Revenue/session:  A = $12   B = $14  →  B wins!" (y=55) and "Revenue/user/mo:  A = $85   B = $84  →  A wins!" (y=80); bold 17px red "Why? B users visit less often (high $ per visit, fewer visits)." (y=110); `#333` "CTR/impression:   A = 2.1%  B = 2.4%  →  B wins!" (y=145) and "Clicks/user/day:  A = 3.2   B = 3.2   →  Tie." (y=170); bold red "B just shows fewer items → smaller denominator → higher \"rate\" = fake win." (y=195).

## 10. A/B Test Duration & Novelty: Time Granularity Traps

**When You Measure Changes Everything**

- **Day 1-3 results:** Novelty effect. Any change looks like improvement because users explore the new thing. CTR +15% day 1 → back to +0% by day 14. If you called the test on day 3, you shipped a novelty artifact.
- **Week 1 only:** Misses: returning users who haven't come back yet, weekend behavior, paycheck cycles. A test that ran Mon-Fri looks totally different if it included Sat-Sun (different user population on weekends).
- **First-session vs nth-session users:** New users react differently to changes than veterans. If your test is short, you over-weight new users (they're always "new" to both variants). Long tests include more returning users → different answer.
- **Compounding effects:** A small improvement in Day 1 retention (+2%) compounds: by Day 30, the cohort is 18% larger. A 7-day test sees +2%. A 30-day test sees +18%. Same change, 9× different measured impact depending on when you look.

**Rule:** Minimum 2 full business cycles (usually 2 weeks for consumer, 2 months for B2B). Report BOTH early and late metrics — if they diverge, novelty is active. Never ship on Day 3 results.

### Visualization (canvas `c10`, 720×220)

Exponential decay curve of the novelty effect toward a zero true effect.

- **Title (bold 17px `#1a5276`, centered):** "A/B Test Duration: Novelty Effect Decays".
- **Curve:** blue `#2980b9` width 2 over 28 days from x=80 to x=640; y = 160 − 100·exp(−d/4) (starts high, decays to baseline).
- **Baseline:** dashed gray `#aaa` (dash 5/5) at y=160, right-aligned 17px gray label "True effect = 0".
- **Labels:** bold 17px red "Day 1-3: +15% (novelty)" at (100, 55); bold green `#27ae60` "Day 14+: +0% (real effect)" at (350, 130); 17px `#333` x labels "Day 1", "Day 7", "Day 14", "Day 28" at y=185.
- **Bottom line (bold 17px red, centered, y=210):** "Shipped on Day 3 = shipped a novelty artifact, not a real improvement.".

## 11. A/B Test: The Independence Violation (Pure Math)

**T-Test Requires i.i.d. — Page Events in a Session Are NOT Independent**

- **The math:** t-test assumes x₁, x₂, ..., xₙ are i.i.d. SE = σ/√n works ONLY under independence. With intra-cluster correlation ρ, the true SE = σ/√n × √(1 + (m-1)ρ). This "design effect" inflates SE by √(1 + (m-1)ρ).
- **SRP page events are correlated:** User searches "shoes" → sees 10 results → clicks result 3 → reduces probability of clicking result 4. Same user, same query, same intent. These observations share a common latent state. ρ ≈ 0.2-0.4 within session. NOT independent draws.
- **User monthly totals ARE independent:** User A's purchases and User B's purchases have no causal link. Different people, different decisions. One aggregated number per user → n users → n independent observations → t-test valid. SE = σ/√n works because the i.i.d. assumption actually holds.
- **The consequence:** 1000 users × 10 pages = 10,000 rows. With ρ=0.3, m=10: design effect = 1+(9)(0.3) = 3.7. True SE = 1.92× the naive SE. Naive p=0.001 → real p ≈ 0.08. The "significant" result disappears when you account for correlation. Aggregate to user-level → n=1000 independent observations → statistics become valid.

**This is not a semantic choice — it's a mathematical requirement.** The t-test formula is derived assuming independence. Violate that assumption → the formula produces wrong numbers. User-level aggregation restores independence → restores validity of all parametric tests.

### Visualization (canvas `c11`, 720×220)

Formula walkthrough plus a naive-vs-true confidence-interval comparison.

- **Title (bold 17px `#1a5276`, centered):** "Independence Violation: Design Effect Formula".
- **Formula lines (17px `#333` left-aligned at x=60):** "True SE = σ/√n × √(1 + (m-1)ρ)" (y=50); "m = 10 pages/user,  ρ = 0.3 (intra-user correlation)" (y=75); "Design effect = 1 + (10-1)(0.3) = 3.7" (y=100); bold red "True SE = 1.92× naive SE.  Naive p=0.001 → Real p≈0.08" (y=130).
- **CI comparison (right side):** red `#e74c3c` line (width 3) from (420,55) to (520,55) with a 4px center dot, labeled 17px "Naive CI (page-level)"; green `#27ae60` line from (370,85) to (570,85) with center dot, labeled "True CI (user-level)".
- **Bottom lines (centered):** bold 17px `#1a5276` "Page events in session: CORRELATED → violate i.i.d. → t-test invalid" (y=165); bold green "User totals per month: INDEPENDENT → i.i.d. holds → t-test valid" (y=190); 17px `#555` "Not semantic preference — pure mathematical requirement for SE formula to work." (y=212).

## Callout (philosophy box)

**The rule:** Every metric needs an explicit DENOMINATOR and explicit TIME WINDOW. "Conversion rate" is meaningless. "Purchases per unique visitor within 30 days of first session, by device type" is a metric you can act on. The denominator IS the decision about what you're measuring. Choose wrong = measure wrong = decide wrong.

## Regeneration instructions

- **Layout:** h1 + `.subtitle` paragraph, then eleven numbered `<h2>` sections ("1." through "11.", 1.4em `#1a5276` with a 2px `#2980b9` bottom border). Each section holds one `.obj-table` (full width, `border-collapse: collapse`) with a single `<tr>`: left `<td>` (40%) contains `.obj-title` (1.05em, weight 600, `#1a5276`) plus a `<ul>` (0.9em) and a closing `<p>` (0.95em); right `<td>` (60%, centered) contains the canvas with explicit `width`/`height` attributes. Cell borders `1px solid #e0e0e0`, padding 20px 24px, `vertical-align: middle`; even rows `#fafcfe`. Page ends with a `.philosophy` callout — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes vary per chart (720×300, 720×240, 720×220); shared `setup(id)` helper reads the width/height attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Most visuals are text-diagram style (17px `-apple-system` text, bold 17px titles) rather than data plots.
- **Palette:** primary blue `#1a5276`, chart blue `#2980b9`, green `#27ae60`, red `#e74c3c`, purple `#8e44ad`, grays `#555`/`#333`/`#aaa`.
