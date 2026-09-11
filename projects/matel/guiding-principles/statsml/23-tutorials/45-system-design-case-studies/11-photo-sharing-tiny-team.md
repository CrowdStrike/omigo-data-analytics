# Photo Sharing, Tiny Team

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Photo Sharing, Tiny Team

**Subtitle:** An early photo-sharing service served roughly 14 million users with about 3 engineers — by treating team size as a design constraint and building on the most boring, proven stack they could find

## Fourteen Million Users, Three Engineers

**Tags:** `core idea` (blue), `radical simplicity` (green), `2011` (orange)

- **The launch** — the service opened and hit 1 million users in about two months
- **The team** — through 2011 the whole engineering team was about 3 people, founders included
- **The curve** — users kept doubling; the team deliberately did not, reaching ~14M by December 2011
- **The choice** — every component was picked so a tiny team could run it and still sleep at night
- **The name** — the founders blogged the whole setup, and it became the classic case for simplicity

*Example (italic):* By December 2011 the ratio was roughly 14,000,000 users to 3 engineers — about 4.7 million users per engineer, on entirely off-the-shelf parts.

**Key point:** The early team treated engineer count like latency or cost — a hard design constraint — and chose boring, proven, managed technology so 3 people could serve 14 million users.

### Visualization (canvas `c1`, 720×300)

Dual line chart on a shared time axis: registered users (millions) climbing steeply vs engineering team size staying flat near 3.

- **Title (bold 15px, `#1a5276`, top center):** "Users Doubled and Doubled; the Team Stayed at ~3".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months since Oct 2010 launch, 0 to 14, 12px `#444` tick labels "Oct '10", "Dec '10", "Jun '11", "Sep '11", "Dec '11" at months `[0, 2, 8, 11, 14]`; y = 0 to 15, gridlines `#e5e9ef` at 5/10 with 12px `#444` labels "5M", "10M".
- **Users line:** blue `#2a78d6` 3px line through months `[0, 2, 8, 11, 14]`, users in millions `[0, 1, 5, 10, 14]`, filled dots radius 4 at each point with bold 12px blue labels "1M", "5M", "10M", "14M" above the last four points.
- **Engineers line:** green `#008300` 3px line through the same month grid, engineer counts `[2, 2, 3, 3, 3]` plotted on the same 0–15 scale (so it hugs the baseline), 12px green label "engineering team: ~3 people" just above the line near month 8, with an 11px muted note "(people — not on the millions axis)" just below the line.
- **Annotation (bold 13px violet `#4a3aa7`, near month 11, y=70):** "~4.7M users per engineer by Dec 2011".
- **Caption (12px `#444`, bottom right):** "milestones as publicly reported; team size approximate".

## A Whole ID Service in One Postgres Function

**Tags:** `worked example` (blue), `64-bit IDs` (green), `sharding` (orange)

- **The need** — every photo needs a unique, time-sortable ID across thousands of logical shards
- **The non-build** — instead of running a separate ID service, they generated IDs inside Postgres
- **The layout** — 64 bits: 41 bits of milliseconds since Jan 1 2011, 13 bits shard ID, 10 bits sequence
- **The pieces** — shard = user ID mod the shard count (up to 8,192); sequence = counter mod 1,024
- **Hand-check** — 8,192 shards × 1,024 sequence values = ~8.4 million unique IDs per millisecond

*Example (italic):* A photo from user 31,341 lands on shard 31,341 mod 8,192 = 6,765 (hand-check uses the 13-bit max as the shard count); with sequence 517 in that millisecond, the three fields pack into one sortable 64-bit ID.

**Key point:** The published ID scheme packs timestamp, shard, and sequence into one 64-bit integer built by a small PL/PGSQL function — no new service, no new machine, nothing extra to page anyone at 3am.

### Visualization (canvas `c2`, 720×300)

Segmented horizontal bar showing the 64-bit ID layout, with a worked decode of one example ID underneath.

- **Title (bold 15px, `#1a5276`, top center):** "One 64-Bit ID: Timestamp + Shard + Sequence, Made Inside Postgres".
- **Bit bar (y=95, height 44):** one bar from x=60, total width 640, split proportionally by bits — blue `rgba(42,120,214,0.30)` segment width 410 (41 bits), orange `rgba(217,89,38,0.25)` segment width 130 (13 bits), green `rgba(0,131,0,0.25)` segment width 100 (10 bits), each with a 2px border in `#2a78d6` / `#d95926` / `#008300`.
- **Segment labels (bold 12px, matching colors, centered inside each segment):** "41 bits — ms since Jan 1 2011", "13 bits — shard", "10 bits — sequence"; 11px `#6b7280` bit-position labels "63", "22", "9", "0" under the segment boundaries.
- **Decode row (y=200):** three 12px `#2c3e50` lines left-aligned at x=60 — "shard = 31,341 mod 8,192 (13-bit max) = 6,765", "sequence = counter mod 1,024 = 517", "timestamp = upload time, ms since the custom epoch"; example values marked illustrative.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "8,192 × 1,024 = ~8.4M IDs per millisecond — no ID service needed".
- **Caption (12px `#444`, bottom right):** "bit layout as blogged; example user/sequence values illustrative".

## A Stack Chosen So Three People Could Sleep

**Tags:** `why it matters` (blue), `boring stack` (green), `managed services` (orange)

- **The app** — plain Django on EC2 app servers: a mainstream framework with answers on every forum
- **The data** — PostgreSQL with read replicas held the metadata; scaling reads meant adding replicas
- **The speed** — memcached in front of the database, Redis for feeds and fast ID/session-style data
- **The photos** — image files went straight to managed object storage behind a managed CDN
- **The principles** — their blog spelled it out: keep it simple, don't reinvent, use proven technology

*Example (italic):* When photo traffic spiked, the fix was more EC2 app servers and another Postgres read replica — capacity problems became checkbook problems, not engineering projects.

**Key point:** Every box in the diagram was old, documented, and either community-proven or vendor-managed — so the scarce resource, 3 engineers' attention, went to the product instead of to infrastructure.

### Visualization (canvas `c3`, 720×300)

Architecture flow diagram: phones through Django to four boring backing stores, each tagged with why it was safe for a tiny team.

- **Title (bold 15px, `#1a5276`, top center):** "The Whole 2011 Stack: Nothing Invented, Everything Proven".
- **Left box:** rounded box at x=25, y=130, 90×44, fill `rgba(26,82,118,0.10)`, 12px `#2c3e50` label "phones".
- **Middle box:** rounded box at x=160, y=118, 170×64, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 12px label "Django app servers" with 11px `#6b7280` second line "(EC2, stateless — just add more)".
- **Right column (four rounded boxes at x=430, width 250, height 46, at y = 45, 108, 171, 234):** "PostgreSQL + read replicas — metadata" (blue fill `rgba(42,120,214,0.15)`), "Redis — feeds, ID-adjacent data" (green fill `rgba(0,131,0,0.12)`), "memcached — hot-read cache" (aqua `#199e70` border, fill `rgba(25,158,112,0.12)`), "object storage + CDN — photo files" (orange fill `rgba(217,89,38,0.12)`); each with an 11px `#6b7280` tag underneath: "boring since the 90s", "one proven tool, two jobs", "one job, does it well", "the provider's ops, not ours".
- **Arrows:** 2px `#6b7280` arrow from phones to Django; four 2px `#6b7280` arrows fanning from the Django box's right edge to each right-column box.
- **Annotation (bold 13px green `#008300`, centered near y=285):** "every arrow points at something 3 people can run — or pay a provider to run".
- **Caption (12px `#444`, bottom right):** "components as publicly blogged, 2011".

## Boring Is Not the Same as Unscalable

**Tags:** `common mistake` (red), `ops burden` (orange)

- **The confusion** — small teams often assume real scale requires clever, custom-built infrastructure
- **The bill** — a bespoke datastore makes you its only vendor: you own every 3am page, forever
- **The evidence** — Postgres, memcached, and managed object storage ran unchanged from zero to 14M users
- **The constraint** — with 3 engineers, an hour of ops toil is a real fraction of total capacity
- **The mistake** — spending scarce engineers building infrastructure the shelf already sells

*Example (italic):* A custom photo store might save milliseconds, but its first corrupted-index incident costs a week — a third of the team — while managed-storage incidents cost the provider's week instead.

**Common mistake:** Reading "boring stack" as "won't scale". The service scaled to 14M users precisely because the stack was boring — proven components fail in known ways, and managed ones fail on someone else's pager.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the clever + bespoke path ending at a team-owned pager vs the boring + managed path ending with the team shipping features.

- **Title (bold 15px, `#1a5276`, top center):** "Clever + Bespoke vs Boring + Managed, Seen from a 3-Person Team".
- **Row 1 (y=95), label 12px `#444` at x=20:** "clever + bespoke"; violet `#4a3aa7` rounded box at x=160 labeled "custom photo datastore" (12px), 3px arrow to a red `#e74c3c` box at x=400 labeled "you are its only vendor" with bold 12px red "✗ every incident is yours, forever" to its right at 11px wrap.
- **Row 2 (y=205), label:** "boring + managed"; blue `#2a78d6` rounded box at x=160 labeled "Postgres + object storage + memcached", 3px arrow to a green `#008300` box at x=400 labeled "known failures, provider's pager" with bold 12px green "✓ 3 engineers ship product".
- **Box style:** 170–200px wide, 44px tall, 8px radius, fills `rgba(74,58,167,0.12)` / `rgba(231,76,60,0.12)` / `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "engineer count is a design input — spend it on the product, not the plumbing".
- **Caption (12px `#444`, bottom right):** "schematic; incident costs illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); user milestones (1M / 5M / 10M / 14M) and the ID bit layout (41 + 13 + 10 bits, 8,192 shards, mod-1,024 sequence) follow the publicly blogged figures from an early photo-sharing service; the example user ID, sequence value, and incident costs are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
