# Dating Apps & Platform Markets

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Dating Apps & Platform Markets

**Subtitle:** Two-sided platforms live or die by thickness and congestion — and rationing attention is a design tool, not a bug

## Thickness: Enough People on Both Sides

**Tags:** `core idea` (blue), `market thickness` (green)

- **Two sides** — a matching market only works when both sides show up at the same time
- **Thickness** — enough active participants on each side that good matches actually exist
- **Chicken-and-egg** — no riders without drivers, no drivers without riders: the cold start
- **Thin market** — with 50 profiles in town, the best algorithm on earth cannot invent a good match
- **The subsidy** — platforms pay the scarce side to show up: free rides, waived fees, promoted profiles

*Example (italic):* A dating app launching in a new city seeds one side first — thickness comes before any ranking model matters.

**Key point:** Match quality is capped by the pool, not the algorithm — a thin market fails no matter how clever the ranking, so platforms thicken first and optimize second.

### Visualization (canvas `c1`, 720×300)

Two rising curves of match quality vs pool size, nearly identical in the thin region and diverging as the pool thickens; the thin region is shaded and annotated.

- **Title (bold 15px, `#1a5276`, top center):** "Match Quality Rises with Pool Size (illustrative)".
- **Axes:** 1px `#999`, origin (66, 236), x to (690, 236), y top at 56; y-axis caption "match quality index (0–100)" 11px mute at (66, 48).
- **X points (log spacing, evenly drawn):** x = 100/180/260/340/420/500/580/660, labels `50 100 200 400 800 1.6k 3.2k 6.4k` 12px `#444` at y=252; x-axis caption "active profiles per side (log spacing)" 11px mute centered at (378, 270).
- **Thin-region shading:** rect from x=88 to x=300, y 56–236, fill `rgba(231,76,60,0.06)`.
- **Thin-region annotation (bold 13px red `#e74c3c`, centered at x=194):** two lines — "thin market →" at y=84, "any algorithm fails" at y=100.
- **Smart-ranking curve:** green `#008300`, 2.5px polyline with 3.5px dots, values `[12, 22, 38, 55, 68, 78, 84, 88]` scaled 0–100 over 180px.
- **Simple-ranking curve:** blue `#2a78d6`, 2.5px polyline with 3.5px dots, values `[10, 18, 30, 44, 54, 62, 66, 68]` scaled 0–100 over 180px.
- **Legend (top area, textAlign left):** 22px line swatch + text at (356, 70) green bold 12px "smart ranking"; swatch + text at (492, 70) blue bold 12px "simple ranking".
- **Gap annotation (bold 12px `#1a5276`, centered at (520, 200)):** "pool size lifts both — the gap is the algorithm".
- **Caption (12px `#6b7280`, centered at y=290):** "below a few hundred profiles the two curves are nearly identical — the pool is the bottleneck".

## The Popularity Pile-Up

**Tags:** `worked example` (blue), `congestion` (orange)

- **Scarce attention** — messages are free to send but costly to read; attention is the real currency
- **The pile-up** — when contacting is free, everyone contacts the same most-attractive profiles
- **The numbers** — the top decile gets 600 of every 1,000 first messages but answers only 24 (4%)
- **The unseen middle** — decile 5 gets just 40 messages and replies to 16 of them (40%)
- **Wasted effort** — most messages land where they cannot be answered: collisions, not conversations

*Example (italic):* Decile 2 receives a quarter of decile 1's messages yet returns more replies — 30 versus 24.

**Key point:** Free contacting piles attention onto the same few profiles — most messages are wasted on collisions while the high-reply middle of the pool sits unseen.

### Visualization (canvas `c2`, 720×300)

Two side-by-side bar panels split by a light divider at x=360: first messages received per profile decile on the left, replies sent back per decile on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Where 1,000 First Messages Go — and What Comes Back (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=40 to y=262.
- **Left panel header (bold 13px blue `#2a78d6`, centered at (192, 52)):** "FIRST MESSAGES RECEIVED".
- **Left bars:** baseline y=232, 10 bars at x = 66 + i×28 (width 22, i = 0..9), values `[600, 150, 90, 60, 40, 25, 15, 10, 6, 4]` scaled 0–620 over 150px; fill `rgba(42,120,214,0.5)`, 1px blue stroke; value labels bold 11px blue above the first five bars only (600, 150, 90, 60, 40); decile labels `1..10` 11px mute at y=246; sub-caption "profile decile (1 = most popular)" 11px mute centered at (192, 262).
- **Left annotation (bold 12px red `#e74c3c`, textAlign left at (150, 96)):** "← decile 1 takes 60%".
- **Right panel header (bold 13px green `#008300`, centered at (552, 52)):** "REPLIES SENT BACK".
- **Right bars:** baseline y=232, 10 bars at x = 426 + i×28 (width 22), values `[24, 30, 27, 23, 16, 10, 6, 4, 2, 1]` scaled 0–34 over 150px; fill `rgba(0,131,0,0.4)`, 1px green stroke; value labels bold 11px green above every bar; same decile labels and sub-caption centered at (552, 262).
- **Right annotation (bold 12px magenta `#d55181`, centered at (552, 78)):** "decile 2 out-replies decile 1".
- **Caption (12px `#6b7280`, centered at y=290):** "most messages collide on the same few profiles; the replies come from the middle of the pool".

## Five Likes a Day: Scarcity as a Signal

**Tags:** `rationing` (green), `costly signal` (blue), `market design` (orange)

- **The cap** — the app allows only a handful of likes per day, so each like costs the others
- **Costly signal** — a like now means "I chose you over everything else today", so it carries information
- **The effect** — reply rates on first contacts jump from 4% to 22% once likes are rationed
- **Same trick elsewhere** — admissions and job markets let applicants attach a few priority signals
- **Curated exposure** — the feed interleaves popularity tiers so attention spreads instead of piling up

*Example (italic):* The feed is quietly doing congestion control — who you see is a design choice, not a neutral list.

**Key point:** Scarcity restores the information that free-for-all destroyed — a rationed like is believable precisely because it could have gone to someone else.

### Visualization (canvas `c3`, 720×300)

Two-panel arrow diagram split at x=360: unlimited likes on the left (all senders converge on one node), rationed likes on the right (fewer, spread arrows), with reply-rate labels under each panel.

- **Title (bold 15px, `#1a5276`, top center):** "Unlimited vs Rationed Likes (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=40 to y=250.
- **Left header (bold 13px orange `#d95926`, centered at (180, 52)):** "UNLIMITED — free to send".
- **Right header (bold 13px green `#008300`, centered at (545, 52)):** "RATIONED — 10 likes a day".
- **Sender dots:** 8 filled blue `#2a78d6` circles radius 5, left column at x=105 and right column at x=465, y = 80/102/124/146/168/190/212/234; column label "senders" 11px mute centered above at y=70.
- **Profile nodes:** 4 circles radius 10 at x=280 (left) and x=640 (right), y = 90/138/186/234; stroke 2px ink `#1a5276`, fill `#fbfcfd`; node letters `A B C D` 11px ink centered inside; node A gets fill `rgba(213,81,129,0.2)` and magenta `#d55181` stroke on the LEFT panel only (the pile-up target); column label "profiles" 11px mute centered above at y=70.
- **Left arrows:** all 8 senders → node A at (280, 90); 1.5px mute `#6b7280` lines from (112, sender y) to (268, 92) with small filled arrowheads at the target end.
- **Right arrows:** spread — senders 1–3 → A (640, 90), senders 4–5 → B (640, 138), senders 6–7 → C (640, 186), sender 8 → D (640, 234); 1.5px green `#008300` lines from (472, sender y) with arrowheads at the target end.
- **Left footer:** bold 13px red `#e74c3c` centered at (180, 272): "reply rate: 4%"; 11px mute at (180, 288): "everyone contacts the same profile".
- **Right footer:** bold 13px green centered at (545, 272): "reply rate: 22%"; 11px mute at (545, 288): "each like says 'I chose you today'".

## The Levers Every Two-Sided Product Owns

**Tags:** `where it's used` (blue), `metrics trap` (red)

- **Market-design levers** — subsidize a side, batch arrivals, cap contacts, curate exposure
- **Not an ML problem** — when a marketplace metric sags, the fix is often a matching lever, not a model
- **The metrics trap** — messages sent can RISE while match quality falls; congestion inflates activity
- **Read both sides** — track replies and mutual matches, not just how much contacting happens
- **Same playbook** — freelance markets, ride platforms, and admissions pull the same levers

*Example (italic):* A team celebrates messages per user climbing for eight straight weeks while mutual matches quietly fall by a third.

**Key point:** Raw engagement is a congestion gauge as much as a health metric — a two-sided product is a market first, and its levers are market-design levers.

### Visualization (canvas `c4`, 720×300)

A dual-axis two-line chart over eight weeks: messages per user rising (left axis) while mutual matches per user fall (right axis), with a red trap annotation.

- **Title (bold 15px, `#1a5276`, top center):** "The Metrics Trap: Activity Up, Matching Down (illustrative)".
- **Axes:** 1px `#999`; left axis at x=66 from y=60 to y=232, baseline from (66, 232) to (690, 232), right axis at x=690 from y=60 to y=232; left caption "messages / user" bold 11px blue `#2a78d6` textAlign left at (66, 52); right caption "mutual matches / user" bold 11px magenta `#d55181` textAlign right at (690, 52).
- **Week points:** x = 100/180/260/340/420/500/580/660, labels `w1..w8` 12px `#444` at y=250.
- **Messages line (blue `#2a78d6`, 2.5px, 3.5px dots):** values `[8, 10, 12, 15, 18, 22, 26, 30]` scaled 0–32 over 172px against the left axis; end value "30" bold 12px blue above the last dot.
- **Matches line (magenta `#d55181`, 2.5px, 3.5px dots):** values `[1.8, 1.75, 1.65, 1.55, 1.45, 1.35, 1.25, 1.2]` scaled 0–2 over 172px against the right axis; end value "1.2" bold 12px magenta below the last dot.
- **Trap annotation (bold 12px red `#e74c3c`, centered at (380, 92)):** "the dashboard says growth — the market says congestion".
- **Caption (12px `#6b7280`, centered at y=288):** "messages nearly quadruple while matches fall by a third — the extra activity is collisions".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for genuine alarm states: the thin-market annotation (c1), the pile-up share and 4% reply rate (c2, c3), and the metrics-trap annotation (c4). Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** hardcoded arrays only — pool sizes `[50..6400]` with quality `[12,22,38,55,68,78,84,88]` (smart) and `[10,18,30,44,54,62,66,68]` (simple); messages per decile `[600,150,90,60,40,25,15,10,6,4]` (sums to 1,000) with replies `[24,30,27,23,16,10,6,4,2,1]` (24/600 = 4%, 16/40 = 40%); reply rates 4% vs 22%; weekly messages `[8,10,12,15,18,22,26,30]` vs matches `[1.8,1.75,1.65,1.55,1.45,1.35,1.25,1.2]` (1.8 → 1.2 = down a third). All invented numbers carry "(illustrative)" in chart titles.
- **Naming:** no real or invented brand names — "a dating app", "the app", "a marketplace"; no named people needed on this page.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
