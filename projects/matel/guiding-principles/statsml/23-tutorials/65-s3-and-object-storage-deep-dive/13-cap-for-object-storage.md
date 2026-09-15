# CAP for Object Storage

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** CAP for Object Storage

**Subtitle:** Write a new price to an object and read it back in the same region, and you always get the new price; read the copy in another region and you can still get the old one

## Same Region: The Reader Gets What The Writer Just Wrote

**Tags:** `core idea` (blue), `strong in-region` (green), `made to wait` (orange)

- **The setup** — one object holds a price; Alice writes a new price to it, Bob reads the same object
- **What Bob gets** — the new price, on his first read, whether he reads a millisecond later or an hour later
- **No retry needed** — he does not have to read twice, sleep, or check whether the value looks current
- **Overwrites included** — this holds for replacing an existing value and for deleting it, not just new objects
- **When the service is busy** — it answers with a slow-down error and asks the caller to retry the read
- **It never answers with the old price** — an error you can see is the price paid for never being quietly wrong
- **That is the CAP trade** — a system either always answers or always answers correctly; here, correctness wins

*Example (illustrative):* Alice writes price 42 and her write is acknowledged; Bob reads the object a heartbeat later and gets 42 — not because he was lucky with timing, but because the older value is no longer available to serve.

**Key point:** The guarantee is about which value you see, not how quickly you see it. Under load the service delays you with a retryable error, and a visible delay is a far cheaper failure than a silently stale number.

### Visualization (canvas `c1`, 720×300)

The three answers a same-region read can produce: the new value, a retryable slow-down error, and the older value that is never returned.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "Same Region: Three Possible Answers, One That Never Happens".
- **Reader chip:** x=36, y=110, w=160, h=54, 6px radius, fill `rgba(42,120,214,0.14)`, 2px `#2a78d6`; two centred lines at x=116 — bold 12.5px `#2c3e50` "Bob reads the" at y=132, 12px `#6b7280` "price object" at y=150.
- **Service chip:** x=250, y=100, w=170, h=74, 6px radius, fill `rgba(26,82,118,0.10)`, 2px `#1a5276`; centred at x=335 — bold 12.5px `#1a5276` "object storage" at y=128, 12px `#6b7280` "same region as the write" at y=148.
- **Arrow (2px `#6b7280`):** from (200, 137) to (244, 137) with arrowhead.
- **Answer A — normal (x=470, y=66, w=224, h=52):** fill `rgba(0,131,0,0.14)`, 2px `#008300`, 6px radius; centred at x=582 — bold 12.5px `#008300` "the new price: 42" at y=90, 12px `#6b7280` "always, on the first read" at y=108.
- **Answer B — busy (x=470, y=132, w=224, h=52):** fill `rgba(201,133,0,0.14)`, 2px `#c98500`, 6px radius; bold 12.5px `#c98500` "slow down — retry" at y=156, 12px `#6b7280` "an error, not a value" at y=174.
- **Answer C — never (x=470, y=198, w=224, h=52):** fill `#f4f6f9`, 2px dashed (`setLineDash([5,4])`) `#6b7280`, 6px radius; 12.5px `#6b7280` "the old price: 37" at y=222 with a 1.5px `#6b7280` strike-through line drawn across the measured text width at y=218, and 12px `#6b7280` "never returned in-region" at y=240.
- **Branch arrows (2px, from the service chip's right edge):** `#008300` from (420, 120) to (466, 92); `#c98500` from (420, 137) to (466, 158); `#6b7280` dashed 4/3 from (420, 155) to (466, 224). Arrowheads on each.
- **Annotation (bold 13px `#d55181`, centered at y=278):** "it will make the caller wait — it will not make the caller wrong".
- **Caption (12px `#444`, bottom right):** "prices illustrative; same-region read-after-write and retryable throttling are documented behaviour".

## Another Region: The Copy Arrives Later

**Tags:** `worked example` (blue), `replication delay` (orange), `stale reads` (red)

- **The setup** — the bucket is copied to a second region, and that copying happens after the write is acknowledged
- **The gap** — Alice's write succeeds in region A well before the new price exists in region B at all
- **What each reader gets** — for a while, a reader in A gets 42 while a reader in B still gets 37
- **Both reads succeed** — neither is an error, and nothing in the response marks region B's value as out of date
- **Missing, not just old** — a newly created object is simply absent in region B until the copy lands
- **How long is the gap** — it depends on the copy backlog; you can pay for a bound on it, never for zero

*Example (illustrative):* Alice writes 42 in region A, then a dashboard configured against region B renders 37. The dashboard is not broken — 37 is genuinely the value region B holds at that moment.

**Key point:** The software is identical in both regions; only which copy the read reached is different. That is the whole trade — read one region and get the current value, or spread across regions and accept disagreement.

### Visualization (canvas `c2`, 720×300)

A two-lane timeline for one object: region A holds the new price from the moment of the write, region B holds the old price until the copy lands, and one instant is marked where two readers get two different prices.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "The Window Where Both Prices Are Live".
- **Time axis:** x0=110 at t=0 ms, x1=660 at t=600 ms, so 1 ms = 0.9166667 px; axis line 1.5px `#e5e9ef` at y=216 from x=110 to x=660.
- **Ticks (12px `#6b7280`, centered, baseline y=236):** 0, 150, 300, 450, 600 at x = 110, 247.5, 385, 522.5, 660; 2px `#e5e9ef` marks up 5px from the axis.
- **Axis title (12px `#6b7280`, centered at (385, 256)):** "milliseconds after the write is acknowledged in region A (illustrative)".
- **Replication window shading:** fill `rgba(231,76,60,0.08)` rect from x=110 to x=522.5, y=80 to y=198.
- **Lane A:** bar x=110 to x=660, y=90, h=34, 4px radius, fill `rgba(0,131,0,0.20)`, 2px `#008300`; centred bold 12.5px `#008300` "holds 42 from the moment the write returns" at (385, 112).
- **Lane B:** old segment x=110 to x=522.5, y=150, h=34, fill `rgba(231,76,60,0.16)`, 2px `#e74c3c`, centred bold 12.5px `#e74c3c` "still holds 37" at (316.25, 172); new segment x=522.5 to x=660, fill `rgba(0,131,0,0.20)`, 2px `#008300`, centred bold 12.5px `#008300` "42" at (591.25, 172).
- **Lane labels (bold 12px `#1a5276`, right-aligned at x=102):** "region A" on baseline y=112, "region B" on baseline y=172.
- **Copy lands:** dashed 2px `#e74c3c` (dash 5/4) vertical line at x=522.5 from y=72 to y=202; bold 12px `#e74c3c` "copy lands" left-aligned at (526, 68).
- **The instant of the reads:** 2px `#4a3aa7` vertical line at x=220 (t=120 ms) from y=72 to y=202; bold 12px `#4a3aa7` centred "two reads, here" at (220, 64).
- **Callouts (bold 12px, left-aligned at x=226):** `#008300` "reader in A → 42" on baseline y=108; `#e74c3c` "reader in B → 37" on baseline y=168.
- **Annotation (bold 13px `#d55181`, centered at y=278):** "both reads succeed and disagree — neither response says which value is stale".
- **Caption (12px `#444`, bottom right):** "timings illustrative; cross-region copying happens after the write returns, with no default bound".

## What You Can Count On, and Where

**Tags:** `rule of thumb` (blue), `same region vs another` (green), `common mistake` (red)

- **Same region, all of it holds** — your own write, someone else's write, a delete, and the bucket's listing
- **Another region, none of it holds** — each can be out of date, returned without an error and without a hint
- **Neither does two objects at once** — changing two objects all-or-nothing is not offered in either arrangement
- **So ask one question** — before trusting a value, ask whether the read was served from a copy in another region
- **The rule that follows** — a job that must see its own writes reads from the same bucket in the same region
- **Since December 2020** — the same-region guarantee is documented; guidance older than that says otherwise

*Example (illustrative):* A failover plan assumed switching reads to region B loses nothing; the copy backlog was running a minute behind, so a minute of writes were simply not there after the switch.

**Common mistake:** Two beliefs keep circulating — that a same-region read is still only *eventually* current, which stopped being true in December 2020, and that a front door routing across both regions makes them agree. Routing changes which copy answers; it does not make the copies match.

### Visualization (canvas `c3`, 720×300)

A guarantee inventory: one row per thing a reader might rely on, one mark column for a same-region read and one for a read served from another region, closing on the single design rule.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "What Holds in the Same Region, and What Survives the Copy".
- **Column headers (bold 12.5px `#1a5276`, centered, baseline y=52):** "same region" at x=470; "another region" at x=610.
- **Header rule (1.5px `#e5e9ef`):** horizontal from (400, 56) to (676, 56).
- **Rows:** six rows x=36, w=640, h=26, 3px radius, tops at y = 64, 92, 120, 148, 176, 204 (pitch 28); text baseline = top + 17, so 81, 109, 137, 165, 193, 221.
- **Row fills and 3px left accent bar (x=36, row top, h=26):** rows 1–4 fill `rgba(0,131,0,0.07)`, accent `#008300`; row 5 fill `rgba(231,76,60,0.09)`, accent `#e74c3c`; row 6 fill `rgba(201,133,0,0.10)`, accent `#c98500`.
- **Row labels (12.5px `#2c3e50`, left-aligned at x=50, on the row baseline):** "read the value you just wrote", "read someone else's latest write", "read a delete you just made", "the bucket listing is current", "two objects change together, or neither", "how stale can the value be?".
- **Marks (bold 14px, centered at x=470 and x=610, on the row baseline):** rows 1–4 are "✓" `#008300` in the same-region column and "✗" `#e74c3c` in the other-region column; row 5 is "✗" `#e74c3c` in both columns; row 6 carries text instead of glyphs — bold 12px `#008300` "never stale" at x=470 and bold 12px `#c98500` "no bound by default" at x=610.
- **Column dividers (1px `#e5e9ef`, dashed 4/3):** vertical at x=400 and x=540, each from y=48 to y=234.
- **Rule strip:** x=36, y=242, w=640, h=30, 6px radius, fill `rgba(25,158,112,0.12)`, 2px `#199e70`; centred bold 12.5px `#199e70` "if the value has to be current, read the region you wrote to" at (356, 261).
- **Caption (12px `#444`, bottom right):** "every row states documented behaviour; no illustrative values here".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `01-buckets-keys-and-the-flat-namespace.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `roundRect`, `arrowHead` and `arrow` helpers as in the sibling pages.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`, red `#e74c3c`. Red is reserved for genuine wrongness: region B's stale value in `c2`, and the not-guaranteed rows in `c3`. Yellow carries "made to wait", which is a cost but not an error.
- **Register and vocabulary — this is the deliberate level of the page.** Plain technical English about real objects, buckets, regions, reads and writes. **No analogies or invented scenes** (an earlier draft framed the whole page as a filing room with a courier and was rejected for reading as a children's story). Equally, **no API surface**: say "read"/"write", not GET/PUT/LIST/HEAD; say "a slow-down error, retry", not the status code; say "a bound you can pay for", not Replication Time Control; say "a front door routing across regions", not Multi-Region Access Point. Named status codes, header names, per-prefix request rates and product feature names all belong on the sibling pages, not here.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No PACELC.** It is a taxonomy, not a guarantee; it is covered on the distributed-systems CAP-spectrum page.
  - **Three sections, 6–7 bullets each.** An earlier four-section version was cut for being denser than the concept needs.
  - The mechanics that were removed live on sibling pages: `08-from-eventual-to-strong-consistency` (the December 2020 change and the pre-2020 traps), `09-conditional-writes-and-compare-and-swap`, `11-cross-region-replication`, `12-replication-lag-in-practice`.
- **Data:** all values are hardcoded literals, no randomness anywhere. The running example is one object holding a price, old value 37 and new value 42; those two numbers appear in section 1's text and `c1`, and in section 2's text and `c2`, and must stay in agreement. Computed geometry: `c2` maps 600 ms onto 550 px (0.9166667 px/ms) from x0=110, so t = 120 ms → x = 220.0 and the copy landing at t = 450 ms → x = 522.5, which coincides with the 450 ms tick; lane B's old-value label centres at (110 + 522.5)/2 = 316.25 and its new-value label at (522.5 + 660)/2 = 591.25. `c3` rows use a 28 px pitch from y=64, giving tops 64…204 and baselines top+17. Illustrative and labelled as such: the prices 37 and 42, the 450 ms copy delay, the 120 ms read instant, and the one-minute backlog in section 3's example. Documented behaviour the page stands on: strong read-after-write consistency for a single object within one region since December 2020, covering reads, overwrites, deletes, listings and metadata changes; retryable throttling errors rather than stale values under load; cross-region copying that happens after the write is acknowledged, with no default bound on the delay; the absence of any multi-object transaction in either arrangement.
