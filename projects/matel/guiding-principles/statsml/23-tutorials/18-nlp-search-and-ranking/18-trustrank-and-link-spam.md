# TrustRank & Link Spam

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** TrustRank &amp; Link Spam

**Subtitle:** Any score built from links can be farmed with fake links — TrustRank fights back by letting importance flow only from a small set of hand-checked trusted pages

## Buying Votes for Free

**Tags:** `core idea` (blue), `link spam` (orange), `adversarial` (red)

- **The loophole** — PageRank's random jump gives every page a small floor score, even a page nobody visits
- **The farm** — a spammer creates 100 empty pages in a 600-page web; each holds one link to a target page
- **The floor** — the jump alone hands each farm page 0.15/600 = 0.00025 of PageRank
- **The harvest** — the target collects 0.85 × 100 × 0.00025 ≈ 0.021, versus 0.0017 for the average page
- **The result** — a page no human ever linked to ranks around 13× the average, built entirely from dust

*Example (italic):* Each farm page is worth almost nothing — but "almost nothing, one hundred times, all pointed at one place" is a top-tier score.

**Key point:** PageRank counts link structure, not honesty — anyone who can mint pages can mint the tiny floor scores those pages carry and pipe them to one target.

### Visualization (canvas `c1`, 720×300)

Diagram of the spam farm: a cluster of tiny gray dots (the 100 farm pages) all pointing arrows at one swollen target node, next to a small honest page for scale.

- **Title (bold 15px, `#1a5276`, top center):** "100 Worthless Pages, One Inflated Target".
- **Farm cluster (left, centered near x=180, y=160):** ~24 small gray `#9aa4b0` dots (3px radius) at hardcoded jittered positions inside an invisible 130×110 region; 11px `#6b7280` label under the cluster: "spam farm — 100 pages, 0.00025 each"; 8–10 thin `#c3cad3` arrows from cluster-edge dots converging on the target.
- **Target node (center-right, x=430, y=150):** circle radius 40, fill `rgba(217,89,38,0.18)`, 2.5px orange `#d95926` border; bold 14px orange "target" with 12px `#444` "0.021" beneath.
- **Honest page for scale (x=610, y=150):** circle radius 12, fill `rgba(42,120,214,0.18)`, 2px blue border; bold 12px blue "average page" above, 12px `#444` "0.0017" below.
- **Annotation (bold 12px `#e74c3c`, near x=430, y=245):** "no human ever linked here — 13× the average score".
- **Caption (11px `#444`, bottom right):** "600-page web, damping 0.85 — illustrative".

## Start the Jumps From Pages You Trust

**Tags:** `worked example` (blue), `trusted seeds` (green), `trust decay` (orange)

- **The one change** — keep PageRank's machinery, but the random jump lands only on hand-checked seed pages
- **The seeds** — a few hundred pages humans verified: major institutions, well-run directories, known-good sites
- **Trust flows** — a seed passes 0.85 of its trust along each hop, so trust fades with distance
- **The chain** — seed news site 1.00 → city directory 0.85 → bakery blog 0.72 → new recipe page 0.61
- **The farm's problem** — no trusted page links toward it, so no trust ever arrives: the whole farm scores 0.00

*Example (italic):* The recipe page three hops from a seed keeps a healthy 0.61 — the spam target, unreachable from every seed, keeps exactly nothing.

**Key point:** TrustRank inverts the burden of proof — instead of every page getting free score to redistribute, score only exists where a path from a hand-verified page delivers it.

### Visualization (canvas `c2`, 720×300)

Trust-decay chain: four linked pages with bars shrinking 1.00 → 0.85 → 0.72 → 0.61 left to right, and the spam farm floating below with a flat 0.00 bar and no incoming arrow.

- **Title (bold 15px, `#1a5276`, top center):** "Trust Fades With Each Hop — and Never Reaches the Farm".
- **Chain row (y baseline 165):** four nodes at x = 120, 280, 440, 600; each a rounded 110×32 box (fill `rgba(0,131,0,0.12)`, 2px green `#008300` border) labeled bold 12px `#0a5c0a`: "seed news site", "city directory", "bakery blog", "new recipe page"; 1.8px `#9aa4b0` arrows between boxes with 11px `#6b7280` "× 0.85" above each arrow.
- **Trust bars:** above each box a vertical bar (26px wide, scale 1.0 = 90px) filled `rgba(0,131,0,0.35)` with 2px green border and bold 13px green value on top: "1.00", "0.85", "0.72", "0.61"; the seed bar gets an 11px `#6b7280` "hand-checked" note above its value.
- **Spam farm (bottom left, x=120, y=245):** rounded 110×32 box, fill `rgba(107,114,128,0.12)`, 2px dashed `#6b7280` border, bold 12px mute label "spam farm"; beside it bold 13px `#e74c3c` "trust 0.00 — no path from any seed".
- **Annotation (bold 12px green `#008300`, near x=600, y=250):** "three hops out still keeps 0.61".
- **Caption (11px `#444`, bottom right):** "decay 0.85 per hop, single-link chain — illustrative".

## Same Web, Two Leaderboards

**Tags:** `where it's used` (blue), `reputation systems` (green)

- **The flip** — ranked by PageRank the spam target sits 2nd of 5; ranked by TrustRank it falls to last
- **Honest pages barely move** — the news site, directory, and blog keep their relative order under both
- **The recipe generalizes** — pick trusted anchors, propagate along edges, distrust what stays unreachable
- **Email** — sender reputation flows from known-good mail servers; unknown bursts start near zero
- **Marketplaces** — seller trust seeded from verified accounts resists rings of fake five-star reviewers

*Example (italic):* A fraud ring of new accounts all vouching for each other looks exactly like a link farm — dense links inside, no inbound path from any trusted account.

**Key point:** "Propagate trust from verified seeds" is a general defense: it re-ranks any graph so that mutual admiration among unknowns stops counting as evidence.

### Visualization (canvas `c3`, 720×300)

Bump chart between two leaderboards: five pages ranked by PageRank on the left and TrustRank on the right, with the spam target's line plunging from 2nd to 5th while honest pages keep their order.

- **Title (bold 15px, `#1a5276`, top center):** "Ranked by PageRank vs Ranked by TrustRank".
- **Columns:** left slots centered x=210, right slots centered x=510; bold 13px `#2c3e50` headers at y=60: "by PageRank" and "by TrustRank"; five slot rows at y = 95, 133, 171, 209, 247; 12px `#6b7280` slot numbers "1."–"5." at x=120 and x=655.
- **Left order (top to bottom, 12px labels right-aligned at x=270 with 7px dots at x=285):** "news site 0.030" blue `#2a78d6`, "spam target 0.021" red `#e74c3c`, "city directory 0.012" green `#008300`, "bakery blog 0.005" aqua `#199e70`, "recipe page 0.002" violet `#4a3aa7`.
- **Right order (top to bottom, dots at x=435, 12px labels left-aligned at x=450):** "news site 1.00 (seed)", "city directory 0.85", "bakery blog 0.72", "recipe page 0.61", "spam target 0.00"; same color per page as the left side.
- **Connecting lines:** 2.5px line per page between its two dots in the page color; the spam target's red line drops from slot 2 to slot 5 and is drawn last (on top).
- **Annotation (bold 12px `#e74c3c`, centered near x=360, y=280):** "the farmed score evaporates when votes must trace back to a seed".
- **Caption (11px `#444`, bottom right):** "scores from the sections above — illustrative".

## Low Trust Is Not Proof of Spam

**Tags:** `common mistake` (red), `seed leaks` (orange)

- **False accusations** — a brand-new honest page far from every seed also scores near zero at first
- **Seed bias** — trust radiates from the seed list, so whoever picks the seeds picks who can win
- **The counterattack** — spammers stop building farms and start hunting for one link from trusted turf
- **The leak** — an unmoderated comment on a trusted blog hands its trust straight to the farm
- **The lesson** — trust propagation moves the battle from "mint pages" to "guard every trusted outlink"

*Example (italic):* One spam comment on the bakery blog (trust 0.72) leaks 0.85 × 0.72 ÷ 3 links ≈ 0.20 to the farm — more trust than a thousand fake pages could ever mint.

**Common mistake:** Reading a low trust score as evidence of spam. It only means "no trusted path found yet" — new honest pages start there too, which is why trust scores punish slowly and forgive as real links accumulate.

### Visualization (canvas `c4`, 720×300)

Leak diagram: the trusted chain from before, with a red "spam comment" arrow escaping the bakery blog into the spam farm, which now shows a nonzero trust bar.

- **Title (bold 15px, `#1a5276`, top center):** "One Unguarded Link Re-Opens the Door".
- **Chain (y=120):** three rounded 110×32 green-bordered boxes at x = 110, 290, 470 labeled "seed news site 1.00", "city directory 0.85", "bakery blog 0.72" (bold 12px `#0a5c0a`, value on a second 12px `#444` line); gray 1.8px arrows between them.
- **Legit outlinks:** from the bakery blog, two thin `#9aa4b0` arrows down-right to small 11px `#6b7280` labels "recipe post" and "supplier page" near y=200 — plus one thick 2.5px red `#e74c3c` arrow to the spam farm box.
- **Red arrow label (bold 12px `#e74c3c`, midway on the arrow):** "spam comment link".
- **Spam farm box (x=560, y=205):** rounded 120×36 box, fill `rgba(231,76,60,0.10)`, 2px `#e74c3c` border, bold 12px `#e74c3c` "spam farm"; beside/below it bold 13px `#e74c3c` "trust ≈ 0.20" with 11px `#6b7280` second line "0.85 × 0.72 ÷ 3 links".
- **Annotation (bold 12px violet `#4a3aa7`, near x=110, y=272):** "the fight moves to guarding trusted outlinks — comment moderation, nofollow".
- **Caption (11px `#444`, bottom right):** "decay 0.85, trust split over 3 outlinks — illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; redraw all charts on debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` reserved for the spam/leak elements — a genuine alarm state.
- **Data:** all numbers are hardcoded literals — farm math: 0.15/600 = 0.00025 per page floor, target ≈ 0.85 × 100 × 0.00025 ≈ 0.021 vs 1/600 ≈ 0.0017 average (≈13×); trust chain 1.00, 0.85, 0.72, 0.61 (0.85 per hop); leak 0.85 × 0.72 ÷ 3 ≈ 0.20; farm cluster dot positions hardcoded (no `Math.random()`); text and chart numbers identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
