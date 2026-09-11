# Microblog Timeline

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Microblog Timeline

**Subtitle:** Designing a large-scale microblog home feed — build each user's timeline at read time (pull) or at write time (push)? Accounts with 50M followers break both, so real designs use a hybrid

## Building a Timeline by Asking Everyone You Follow

**Tags:** `core idea` (blue), `pull model` (green), `read path` (orange)

- **The feed** — a user opens the app and expects the newest posts from everyone they follow
- **The naive read** — fetch recent posts from each followed account, merge-sort by time, show top 50
- **The cost** — a user following 300 accounts triggers 300 lookups on every single refresh
- **The power user** — following 5,000 accounts means 5,000 lookups before one screen renders
- **The name** — computing the feed at read time is the pull model, also called fan-out on read

*Example (italic):* Following 300 accounts, a refresh takes ~40 ms; following 5,000, the same refresh takes ~650 ms — the read cost grows with the follow count.

**Key point:** Pull means the timeline is assembled fresh at read time by merging the posts of everyone you follow — writes are cheap (one row per post), but reads get slower the more you follow.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart: home-timeline refresh latency vs number of accounts followed, under a pure pull model.

- **Title (bold 15px, `#1a5276`, top center):** "Pull Model: Refresh Latency Grows with Accounts Followed".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = latency 0 to 700 ms, gridlines `#e5e9ef` at 175/350/525 with 12px `#444` labels; x = four bars centered at x = 130, 280, 430, 580, width 90.
- **Bars:** follow counts `[100, 300, 1000, 5000]`, latencies ms `[15, 40, 130, 650]`; first three bars blue fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, last bar red fill `rgba(231,76,60,0.20)` with 2px `#e74c3c` border.
- **Labels:** 12px `#444` follow count under each bar ("100", "300", "1,000", "5,000 follows"); bold 12px value labels above each bar ("15 ms", "40 ms", "130 ms", "650 ms").
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=70):** "5,000 lookups per refresh — the reader pays".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## One Post, Fifty Million Timeline Writes

**Tags:** `worked example` (blue), `push model` (green), `fan-out on write` (orange)

- **The flip** — push precomputes: when someone posts, insert it into every follower's stored timeline
- **The read** — opening the app now reads one precomputed list; a refresh is a single cheap fetch
- **A friend** — a user with 200 followers posts: 200 timeline inserts, done in milliseconds
- **A high-follower account** — an account with 50,000,000 followers posts: 50 million inserts for one post
- **Hand-check** — at 500,000 inserts/sec, 50,000,000 ÷ 500,000 = 100 seconds to deliver one post

*Example (italic):* The friend's post costs 200 writes; the high-follower account's costs 50,000,000 — a 250,000× write amplification, and late followers see the post over a minute late.

**Key point:** Push (fan-out on write) makes reads O(1) by moving all the work to write time — which is exactly where a high-follower account's follower count detonates it.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: timeline inserts triggered by one post, for four account sizes; log-feel via hardcoded pixel widths.

- **Title (bold 15px, `#1a5276`, top center):** "Push Model: Writes Caused by One Single Post".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "friend — 200 followers": blue `#2a78d6` bar width 8, 11px label "200 writes"
  - "micro-influencer — 20k followers": blue bar width 90, 11px label "20,000 writes"
  - "big account — 1M followers": orange `#d95926` bar width 230, 11px label "1,000,000 writes"
  - "high-follower account — 50M followers": red `#e74c3c` bar width 440, bold 12px red label "50,000,000 writes"
- **Bar style:** 14px tall, blue bars fill `rgba(42,120,214,0.30)` with solid 2px border, orange/red bars solid.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "100 s to deliver at 500k inserts/s".
- **Caption (12px `#444`, bottom right):** "follower counts and insert rate illustrative".

## The Hybrid: Push for Most, Pull for the Famous

**Tags:** `where it's used` (blue), `hybrid design` (green), `caching` (orange)

- **The skew** — feeds are read-heavy: illustratively ~300 timeline reads for every post written
- **The split** — accounts under a follower threshold (say 1M) are pushed; above it, pulled
- **The read** — fetch the user's precomputed timeline, then merge in fresh posts from high-follower accounts
- **The win** — a user follows 300 accounts but only 3 are high-follower accounts: 1 cached fetch + 3 pulls
- **The cache** — hot timelines live in memory, so the common read touches no disk at all

*Example (italic):* With a 1M-follower threshold, a reader following 297 normal accounts and 3 high-follower accounts does 4 fetches per refresh instead of 300 pulls or forcing 50M-write fan-outs.

**Key point:** The classic answer is hybrid fan-out — push for the many small accounts, pull for the few huge ones, merged at read time — because read-heavy skew makes precomputation pay everywhere except at the high-follower account extreme.

### Visualization (canvas `c3`, 720×300)

Two-lane flow diagram: the push path (normal post fanned out to follower timelines) and the pull path (high-follower account post stored once), converging into a merge step at read time.

- **Title (bold 15px, `#1a5276`, top center):** "Hybrid Fan-Out: Two Write Paths, One Merge at Read".
- **Row 1 (y=95), label 12px `#444` at x=20:** "push (≤1M followers)"; blue `#2a78d6` rounded box at x=150 labeled "friend posts" (12px), 3px arrow to a blue box at x=330 labeled "fan-out: 200 timeline inserts".
- **Row 2 (y=195), label:** "pull (>1M followers)"; orange `#d95926` rounded box at x=150 labeled "high-follower account posts", 3px arrow to an orange box at x=330 labeled "high-follower account store: 1 write".
- **Merge box:** green `#008300` rounded box at x=560, y=145 (vertically between rows) labeled "read: cached timeline + merge 3 high-follower account feeds"; 3px arrows from both x=330 boxes into it.
- **Box style:** 150–180px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=275):** "4 fetches per refresh instead of 300 — reads stay cheap, no 50M-write storms".
- **Caption (12px `#444`, bottom right):** "threshold and counts illustrative".

## The Mistake: One Model for Every User

**Tags:** `common mistake` (red), `write amplification` (orange)

- **The trap** — benchmarking with average users (200 follows, 200 followers) makes either pure model look fine
- **Pure pull fails** — the 5,000-follow power reader waits 650 ms on every refresh, forever
- **Pure push fails** — one 50M-follower post is 50M inserts, and it also writes to dormant accounts
- **The waste** — pushing to a follower who logs in once a month spends millions of writes nobody reads
- **The lesson** — the tail users (heavy followers, huge followings) set the design, not the average

*Example (italic):* A design reviewed only against the median user ships pure push, then the first 50M-follower account signs up and every post becomes a 100-second write storm.

**Common mistake:** Choosing push or pull from the average user's numbers. Feed systems are broken by their extremes — the pull model dies at high follow counts, the push model dies at high follower counts, and only a hybrid survives both.

### Visualization (canvas `c4`, 720×300)

Verdict grid: three model columns (pull / push / hybrid) crossed with two stress-case rows (power reader, high-follower account write), each cell a check or cross box with a one-line cost.

- **Title (bold 15px, `#1a5276`, top center):** "Stress Test: Each Pure Model Fails One Extreme".
- **Grid:** column headers bold 13px `#1a5276` "pure pull" / "pure push" / "hybrid" centered at x = 250, 430, 610 on y=70; row labels 12px `#444` at x=20: "power reader (5,000 follows)" at y=135, "high-follower account post (50M followers)" at y=215.
- **Cells (boxes 150px wide, 48px tall, 8px radius, centered on column x, rows y=135 and y=215):**
  - pull × power reader: red `#e74c3c` box, fill `rgba(231,76,60,0.12)`, bold 12px "✗ 650 ms reads"
  - pull × high-follower account: green `#008300` box, fill `rgba(0,131,0,0.12)`, bold 12px "✓ 1 write"
  - push × power reader: green box "✓ 1 cached fetch"
  - push × high-follower account: red box "✗ 50M writes"
  - hybrid × power reader: green box "✓ fetch + few pulls"
  - hybrid × high-follower account: green box "✓ 1 write, pulled"
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=275):** "design for the tails — the average user never breaks anything".
- **Caption (12px `#444`, bottom right):** "costs illustrative, from the sections above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); latencies, follower counts, insert rates, and read/write ratios are invented and labeled illustrative; the one exact figure is the arithmetic 50,000,000 ÷ 500,000 = 100 seconds. Frame everything as a generic large-scale design exercise — publicly known architecture patterns only, no claims about the real company's internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
