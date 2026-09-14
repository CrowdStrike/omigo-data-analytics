# Cache Eviction

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cache Eviction

**Subtitle:** A full cache must forget something — LRU forgets what was touched longest ago, LFU forgets what is least popular, TTL forgets what is oldest, and the choice decides your hit rate

## Four Slots, Six Products: Someone Must Go

**Tags:** `core idea` (blue), `eviction` (orange)

- **The cache** — a product-image cache holds 4 images; the store sells 6 products, A through F
- **The hot pair** — bestsellers A and B account for most image requests on a normal afternoon
- **The full moment** — request 9 (product E) arrives; LRU and LFU are full: someone must go
- **Three answers** — LRU evicts A (touched longest ago), LFU evicts C (used once), TTL drops B (aged out)
- **The definition** — an eviction policy is the rule a full cache uses to pick which entry to drop

*Example (italic):* At request 9 each rule names a different victim — LRU says A, LFU says C, and TTL, with A already expired, drops B — the policy, not the data, decides.

**Key point:** Eviction is forced; the only choice is the rule. Each policy encodes a bet about which entry is least likely to be needed again.

### Visualization (canvas `c1`, 720×300)

Three-row slot diagram showing the cache state just before request 9, with each policy's victim highlighted in red and the incoming item E in green.

- **Title (bold 15px, `#1a5276`, top center):** "Request 9: E Arrives at a Full Cache — Each Policy Picks a Different Victim".
- **Rows at y = 80 (LRU), 150 (LFU), 220 (TTL);** bold 13px `#1a5276` policy label left-aligned at x=20.
- **Slot boxes:** four per row, 92px wide, 44px tall, 8px radius, at x = 115, 217, 319, 421; 12px `#2c3e50` centered labels; kept slots fill `rgba(42,120,214,0.15)` with 1px `#2a78d6` border; the victim slot fills `rgba(231,76,60,0.15)` with 2px `#e74c3c` border.
- **LRU row labels:** "A · last t5" (victim, red), "B · last t6", "C · last t7", "D · last t8".
- **LFU row labels:** "A ×3", "B ×3", "C ×1" (victim, red — tie with D broken toward least recent), "D ×1".
- **TTL row labels:** "B · age 7s" (victim, red — expired, TTL 6s), "C · age 2s", "D · age 1s", fourth slot empty with dashed 1px `#6b7280` border and 11px `#6b7280` "empty".
- **Incoming item:** green `rgba(0,131,0,0.15)` box 60px wide, 44px tall at x=560 in each row labeled "E in" (12px `#008300`), 2px `#008300` arrow from it to that row's red victim slot.
- **Caption (12px `#444`, bottom right):** "state just before request 9 of the worked trace; one request per second".

## Fourteen Requests, Three Traces

**Tags:** `worked example` (blue), `hit rate` (green)

- **The sequence** — 14 requests, one per second: A B A B A B, then a scan C D E F, then A B A B
- **LRU trace** — the scan pushes A and B out at requests 9–10, so requests 11–12 miss: 6 hits
- **LFU trace** — A and B hold count 3 against the scan's 1, so LFU drops C then D instead: 8 hits
- **TTL trace** — with a 6-second TTL, A (cached at t=1) expires by request 8 and B by 9: 6 hits
- **Hand-check** — everyone misses the first 2 and the scan's 4; the traces differ only on 11–12

*Example (italic):* Same 14 requests, same 4 slots: LFU scores 8 hits, LRU 6, TTL 6 — LRU and TTL both miss requests 11–12, but one flushed the pair and the other expired it.

**Key point:** The trace is fully determined once the rule is fixed: LFU ties break toward the least recently used, a TTL entry expires 6 seconds after insert with no refresh on read, and a full TTL cache with nothing expired evicts the oldest entry.

### Visualization (canvas `c2`, 720×300)

Hit/miss grid: 3 policy rows by 14 request columns, each cell a green check (hit) or red cross (miss), with the hit total at the row's right end.

- **Title (bold 15px, `#1a5276`, top center):** "Fourteen Requests: Hit or Miss Under Each Policy".
- **Column headers (bold 12px `#2c3e50`, y=72):** the request letters `["A","B","A","B","A","B","C","D","E","F","A","B","A","B"]`, centered over column i at x = 80 + i*40 + 19.
- **Rows:** LRU at y=90, LFU at y=150, TTL at y=210; bold 13px `#1a5276` row labels at x=20; cells 36px wide, 48px tall with a 4px gap (column pitch 40, grid from x=80 to x=640).
- **Cell patterns (hardcoded strings, H=hit, M=miss):** LRU `"MMHHHHMMMMMMHH"`, LFU `"MMHHHHMMMMHHHH"`, TTL `"MMHHHHMMMMMMHH"`.
- **Cell style:** hit fill `rgba(0,131,0,0.25)` with bold 13px `#008300` "✓"; miss fill `rgba(231,76,60,0.18)` with bold 13px `#e74c3c` "✗".
- **Row totals (bold 13px, x=655):** "6/14" (`#2c3e50`), "8/14" (`#008300`), "6/14" (`#2c3e50`).
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=285):** "the three policies differ only on requests 11–12".

## Picking a Policy: Hits vs Freshness

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **Temporal locality** — LRU wins when whatever was just asked for will be asked for again soon
- **Stable popularity** — LFU wins when a few items stay hot for hours, like bestseller images
- **Slow to adapt** — a cooled-off bestseller keeps its high count and squats in an LFU cache
- **Freshness** — TTL is the only one of the three that bounds staleness: no image served older than 6s
- **Hybrids** — real caches mix them: segmented LRU, LFU with count decay, TTL layered on either

*Example (italic):* If the sneaker photo A is re-shot, an LFU cache can keep serving the old image as long as it stays popular; the 6-second TTL caps the damage at 6 seconds.

**Key point:** LRU and LFU optimize hit rate under different traffic patterns; TTL optimizes correctness by bounding staleness. Production caches usually pair a hit-rate policy with a TTL.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of hit counts from the worked trace, with a right-hand staleness-bound column showing what each policy promises about freshness.

- **Title (bold 15px, `#1a5276`, top center):** "Hit Count vs Staleness Bound on the 14-Request Trace".
- **Rows (top to bottom at y = 90, 148, 206):** LFU, LRU, TTL; bold 13px `#1a5276` labels at x=20.
- **Bars:** start x=110, 26px tall, 40px of width per hit — LFU green `#008300` width 320 (8 hits), LRU blue `#2a78d6` width 240 (6 hits), TTL orange `#d95926` width 240 (6 hits); bold 12px value labels "8 hits" / "6 hits" / "6 hits" just past each bar end.
- **Reference line:** vertical dashed `#6b7280` (dash 4/3) line at x=670 (14 hits = every request), 11px `#6b7280` label "all 14" at its top.
- **Staleness column (12px, right-aligned at x=700):** "staleness: unbounded" in `#e74c3c` beside LFU and LRU rows; "staleness ≤ 6 s" in `#008300` beside the TTL row.
- **Annotation (bold 13px magenta `#d55181`, centered at y=262):** "LFU wins the hits; only TTL promises freshness".
- **Caption (12px `#444`, bottom right):** "hit counts exact from the trace; TTL = 6 s; traffic illustrative".

## The Scan That Flushes LRU

**Tags:** `common mistake` (red), `scan flush` (orange)

- **The mistake** — treating LRU as the universal default because it is the easiest policy to picture
- **One scan** — a single pass over 4 cold items (requests 7–10) flushed both bestsellers out of LRU
- **The cost** — the two most valuable entries became misses exactly when demand for them returned
- **Scan resistance** — hybrid policies demand a second touch before a newcomer can evict a hot entry
- **The flip side** — LFU fails differently: a stale high count keeps a cooled-off item pinned for hours

*Example (italic):* The crawler that requested C, D, E, F never comes back for them, yet under LRU those four one-off images displaced A and B, the only items with a future.

**Common mistake:** Believing a policy that is optimal for one access pattern is good for all of them. One sequential scan turns LRU's strength — recency — into the thing that evicts your hottest data.

### Visualization (canvas `c4`, 720×300)

Three cache snapshots from the LRU trace shown as stacks of slot boxes, with arrows marking the scan and the re-requests that force A and B to be fetched again.

- **Title (bold 15px, `#1a5276`, top center):** "LRU Cache Snapshots: One Scan Evicts Both Bestsellers".
- **Snapshot groups at x = 40, 280, 520,** each a 12px bold `#444` header at y=70 ("after request 6", "after the scan (t10)", "after requests 11–12") above 4 stacked slot boxes 130px wide, 32px tall, 6px vertical gap, starting y=90, 6px radius, 12px centered labels.
- **Snapshot 1 slots:** "A" and "B" filled `rgba(42,120,214,0.15)` with 1px `#2a78d6` border; two empty slots with dashed 1px `#6b7280` border and 11px `#6b7280` "empty".
- **Snapshot 2 slots:** "C", "D", "E", "F" filled `rgba(217,89,38,0.15)` with 1px `#d95926` border; bold 12px `#e74c3c` note "A, B evicted" just below the stack.
- **Snapshot 3 slots:** "E", "F" in the orange style; "A" and "B" filled `rgba(0,131,0,0.15)` with 2px `#008300` border and bold 12px `#008300` note "re-fetched: 2 extra misses" below the stack.
- **Arrows:** 2px `#6b7280` arrow from group 1 to group 2 labeled "scan C D E F" (12px `#444`), and from group 2 to group 3 labeled "A, B asked again".
- **Annotation (bold 13px red `#e74c3c`, centered at y=282):** "four items nobody wants twice pushed out the two everyone wants".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all chart values are the hardcoded arrays and strings above (no randomness). The eviction traces are exact by construction — 4-slot cache, request sequence `A B A B A B C D E F A B A B` at one request per second, LFU ties broken toward least recently used, TTL of 6 seconds with no refresh on read and FIFO fallback when nothing has expired — giving 6 hits (LRU), 8 hits (LFU), 6 hits (TTL). Product traffic framing is invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
