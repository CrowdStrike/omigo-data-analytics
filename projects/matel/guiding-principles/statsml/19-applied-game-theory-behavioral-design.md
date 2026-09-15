# Applied Game Theory & Behavioral Design

**Page type:** grid page (4-column card grid, philosophy callouts above and below)
**HTML title tag:** Applied Game Theory & Behavioral Design

**Subtitle:** How math, statistics, game theory, and psychology are applied in modern product design — from pricing structures to engagement mechanics to auction systems.

## Callout (philosophy box, top)

**What this section is about:** The products and platforms around us are built on well-studied principles from game theory, behavioral economics, and cognitive psychology. This section dissects those mechanisms — understanding what's being applied, why it works, and what the underlying math looks like.

## Cards

Each card links to a detail page under `applied-game-theory-behavioral-design/`. The card shows a colored uppercase category label, a numbered title, and a description.

| # | Category | Title | Link | Description |
|---|----------|-------|------|-------------|
| 1 | PRICING (#e67e22) | Decoy Effect (Asymmetric Dominance) | [19-applied-game-theory-behavioral-design/01-decoy-effect-asymmetric-dominance.md](19-applied-game-theory-behavioral-design/01-decoy-effect-asymmetric-dominance.md) | Popcorn at $3.99 / $7.99 / $8.99. Nobody buys the medium — it exists so you compare medium vs large ($1 gap) instead of small vs large ($5 gap). |
| 2 | BEHAVIORAL LOOPS (#8e44ad) | Streaks, Coins & Variable Rewards | [19-applied-game-theory-behavioral-design/02-streaks-coins-and-variable-rewards.md](19-applied-game-theory-behavioral-design/02-streaks-coins-and-variable-rewards.md) | Duolingo streaks, Snapchat streaks, daily login bonuses. Slot machine reward schedules inside learning apps. Loss aversion weaponized as engagement. |
| 3 | GAME THEORY (#27ae60) | Auctions & Bidding Mechanics | [19-applied-game-theory-behavioral-design/03-auctions-and-bidding-mechanics.md](19-applied-game-theory-behavioral-design/03-auctions-and-bidding-mechanics.md) | Google Ads second-price auctions. eBay proxy bidding. Why the mechanism forces you to be honest — and why that's profitable for the platform. |
| 4 | SOCIAL PRESSURE (#e74c3c) | Leaderboards & Social Proof | [19-applied-game-theory-behavioral-design/04-leaderboards-and-social-proof.md](19-applied-game-theory-behavioral-design/04-leaderboards-and-social-proof.md) | Ranked lists that make you compete against strangers. "X people are viewing this right now." Manufactured scarcity and urgency that exists only in the UI. |
| 5 | PRICING (#2980b9) | Subscription & Commitment Traps | [19-applied-game-theory-behavioral-design/05-subscription-and-commitment-traps.md](19-applied-game-theory-behavioral-design/05-subscription-and-commitment-traps.md) | Annual vs monthly pricing. Free trials that auto-convert. The math of why "first month free" is profitable even when 40% cancel immediately. |
| 6 | PSYCHOLOGY (#e67e22) | Charm Pricing & Number Psychology | [19-applied-game-theory-behavioral-design/06-charm-pricing-and-number-psychology.md](19-applied-game-theory-behavioral-design/06-charm-pricing-and-number-psychology.md) | $19.99, $97, $199. Left-digit effect. Why the brain reads $19.99 as "nineteen" not "twenty." How pricing tiers are built around cognitive thresholds, not cost. |

## Callout (philosophy box, bottom)

**More candidates:** Gamification loops in fitness apps, dynamic pricing (surge, airline), dark patterns in unsubscribe flows, the attention economy (infinite scroll, autoplay), recommendation algorithms that optimize for time-on-site not satisfaction, loyalty programs as switching cost generators, freemium conversion funnels, A/B tested manipulation at scale.

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, top `.philosophy` callout, one `.grid` of `.card` anchors, bottom `.philosophy` callout.
- **Layout:** `.grid` is CSS grid, `repeat(4, 1fr)`, 20px gap, margin `20px 0 30px`; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="card" href="...">` containing `<div class="card-label" style="color:HEX">CATEGORY</div>` (hex given per row in the Category column), `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`. No mental-model line on this page.
- **Card style:** background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 20px 24px; hover: shadow `0 4px 12px rgba(0,0,0,0.1)`, border `#2980b9`. Label 0.72em bold uppercase letter-spacing 0.5px, h3 `#1a5276` 1.05em, description 0.85em `#555` margin 0.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, margin 20px 0, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; `p` margin 10px 0, `#333`, 0.95em; subtitle `#666` 1.05em, margin-bottom 30px. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus per-card label colors listed in the table. No canvases on this page; any canvases elsewhere use `window.devicePixelRatio` scaling.
