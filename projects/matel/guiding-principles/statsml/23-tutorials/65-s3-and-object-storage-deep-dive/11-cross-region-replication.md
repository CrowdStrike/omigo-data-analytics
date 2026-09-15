# Cross-Region Replication

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cross-Region Replication

**Subtitle:** The copy into the second region starts after your write already succeeded, covers only what your rules select, and has no deadline unless you pay for one

## The Copy Starts After the Write Succeeds

**Tags:** `core idea` (blue), `asynchronous` (green), `rules not a switch` (orange)

- **The setup** — a bucket in region A and a second copy of it kept in region B
- **After the fact** — the copying only begins once your write has already succeeded
- **What success means** — region A definitely holds the object; region B may not yet
- **Versioning first** — replication only starts when both buckets have versioning on
- **A role does the copying** — you hand the service a role it uses to read and write
- **Rules, not a switch** — each rule picks a prefix or tag, so you choose what travels
- **Same-region works too** — identical machinery, just a destination that is nearer

*Example (illustrative):* The write to region A returns in 43 ms; the object shows up in region B about four seconds later.

**Key point:** A successful write tells you region A has the object. It says nothing at all about region B.

### Visualization (canvas `c1`, 720×300)

Two region lanes on one time axis: region A holds the object from the moment the write returns, region B does not hold it until the copy lands, and the gap between the two is shaded.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "The Write Succeeds Before the Copy Exists".
- **Time axis:** x0=110 at t=0 s, x1=660 at t=5 s, so `x(s) = 110 + s × 110`; axis line 1.5px `#1a5276` at y=232 from x=110 to x=660. Ticks 5px up at 0…5 s with 12px `#2c3e50` labels centered at y=250; axis title 12px `#6b7280` centered at (385, 272) reading "seconds after the write returns (illustrative)".
- **Gap shading:** fill `rgba(231,76,60,0.08)` from x=110 to x=550, y=76 to y=214.
- **Lane A:** bar x=110 to x=660, y=88, h=34, 5px radius, fill `rgba(42,120,214,0.22)`, 2px `#2a78d6`; centred bold 12.5px `#2a78d6` "object is here the moment the write returns" at (385, 110).
- **Lane B:** absent segment x=110 to x=550, y=152, h=34, 5px radius, fill `#f4f6f9`, 2px dashed (5/4) `#e74c3c`, centred bold 12.5px `#e74c3c` "object is not here yet" at (330, 174); arrived segment x=550 to x=660, fill `rgba(25,158,112,0.25)`, 2px `#199e70`, centred bold 12.5px `#199e70` "here" at (605, 174).
- **Lane labels (bold 12px `#1a5276`, right-aligned at x=102):** "region A" on baseline y=110, "region B" on baseline y=174.
- **Write-returns marker:** 2px `#2a78d6` vertical line at x=110 from y=70 to y=88 with arrowhead up; bold 12px `#2a78d6` "write returns" left-aligned at (116, 64).
- **Copy-lands marker:** dashed 2px `#199e70` (5/4) vertical line at x=550 from y=70 to y=218; bold 12px `#199e70` "copy lands · 4 s" left-aligned at (554, 64).
- **Queue arrow:** dashed (4/4) 1.8px `#6b7280` from (120, 122) to (546, 150) with arrowhead; 12px `#6b7280` "queued in the background" left-aligned at (150, 142).
- **Annotation (bold 13px `#d55181`, centered at y=204):** "the caller already believes the write is done everywhere".
- **Caption (12px `#444`, bottom right at y=292):** "timings illustrative; copying after a successful source write is documented behaviour".

## What Gets Copied, and What Quietly Does Not

**Tags:** `common mistake` (red), `coverage` (green), `backfill` (orange)

- **New objects** — anything written after the rule is on, along with its metadata
- **Tags and permissions** — these travel across together with the object's bytes
- **Deletes** — a delete crosses over only if you explicitly enable that option
- **Encrypted objects** — need a key configured in the destination region first
- **Older objects** — anything written before the rule stays where it already is
- **Deleting one version** — never crosses over, by design, so one copy survives
- **Each bucket expires on its own** — cleanup rules are never shared between them

*Example (illustrative):* Turning the rule on Monday leaves every object written before Monday in region A only, until a separate backfill job copies them.

**Common mistake:** "Replication is on" is not "the two buckets match." The default covers new objects; the older ones, the deletes, and the encrypted ones each need a deliberate step.

### Visualization (canvas `c2`, 720×300)

Coverage table: one row per case, a colored verdict pill, and the condition attached to it.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "What a Replication Rule Actually Copies".
- **Column headers (bold 12px `#1a5276`, left-aligned, baseline y=44):** "case" at x=54, "verdict" at x=300, "condition" at x=424.
- **Seven rows:** rect x=44, w=632, h=24, 4px radius, tops at y = 52, 80, 108, 136, 164, 192, 220 (pitch 28). Row fill is the verdict color at 0.07 alpha, 1px border in the verdict color. Case text bold 12.5px `#2c3e50` at (54, top+16); condition 12px `#6b7280` at (424, top+16).
- **Verdict pill:** rounded rect x=300, w=112, h=17, at top+4, 8px radius, fill the verdict color at 0.20 alpha, 1.5px border; label bold 11.5px in the verdict color, centered at (356, top+16).
- **Rows (case, verdict, color, condition):**
  - "New objects" → COPIED, green `#008300`, "what a rule does by default"
  - "Metadata, tags, permissions" → COPIED, green `#008300`, "they travel with the bytes"
  - "Deletes" → IF ENABLED, yellow `#c98500`, "off until you turn it on"
  - "Encrypted objects" → IF ENABLED, yellow `#c98500`, "needs a destination key"
  - "Objects older than the rule" → NEVER, red `#e74c3c`, "run a backfill job instead"
  - "Deleting one version" → NEVER, red `#e74c3c`, "by design: protects the copy"
  - "Already-copied objects" → NEVER, red `#e74c3c`, "not passed to a third bucket"
- **Annotation (bold 13px `#e74c3c`, centered at y=264):** "only the top two rows happen on their own".
- **Caption (12px `#444`, bottom right at y=288):** "every row is documented replication behaviour".

## How Long It Takes, and the Deadline You Can Buy

**Tags:** `rule of thumb` (blue), `SLA` (green), `paid tier` (orange)

- **Usually seconds** — most objects land in the second region within a few seconds
- **No promise by default** — there is no published time limit on the copy at all
- **Why that bites** — a fast median hides a slow tail that goes unnoticed for months
- **The paid option** — a real deadline: 99.99% of objects inside fifteen minutes
- **Metrics come with it** — pending bytes, pending object count, and copy latency
- **You get told** — an event fires for any object that misses the fifteen minutes
- **Bounded, not faster** — you are buying the deadline and the measurement, not speed
- **Set it per rule** — buy the deadline for the audit prefix and skip it for logs

*Example (illustrative):* Without the paid option, the slowest 1 in 10,000 objects has no bound at all; with it, that object is inside 15 minutes or you get an event.

**Key point:** The paid tier buys a measurable deadline, not raw speed. Without it, the tail of the copy time is both unbounded and unmeasured.

### Visualization (canvas `c3`, 720×300)

Copy time by percentile on a log time axis, one line with the paid deadline and one without, with the 15-minute line drawn and the missing tail annotated.

- **Title (bold 15px, `#1a5276`, top center at y=20):** "Copy Time by Percentile: With and Without the Paid Deadline".
- **Legend (baseline y=42):** 2.5px 18px swatch lines — green `#008300` at x=90 with bold 12px "with the deadline" at x=114; orange `#d95926` at x=250 with bold 12px "without it" at x=274.
- **X axis (log10 seconds):** plot x=90→660 maps 1 s → 3600 s, `x(s) = 90 + (log10(s) / log10(3600)) × 570` with log10(3600) = 3.55630. Axis line 1.5px `#1a5276` at y=246; ticks 5px down with 12px `#2c3e50` labels centered at y=264 — 1s (x=90.0), 10s (x=250.3), 60s (x=375.1), 15min (x=563.5), 1h (x=660.0).
- **Y axis (percentile rows, top to bottom):** p99.99 y=96, p99.9 y=136, p99 y=176, p50 y=216. Labels bold 12px `#1a5276` right-aligned at x=80; horizontal dashed (3/4) `#e5e9ef` gridline across each row from x=90 to x=660.
- **15-minute line:** vertical 2px dashed (6/4) `#1a5276` at x=563.5 from y=66 to y=246; bold 12px `#1a5276` "15 min" centered at (563.5, 60).
- **Without-the-deadline line (2.5px `#d95926`, 5px dots):** p50 4 s (x=186.5), p99 180 s (x=451.5), p99.9 1560 s (x=601.9). No p99.99 point exists.
- **Missing tail (2.5px dashed 6/5 `#e74c3c`, arrowhead):** from (601.9, 136) to (690, 96); bold 12px `#e74c3c` "no bound" right-aligned at (692, 82).
- **With-the-deadline line (2.5px `#008300`, 5px dots):** p50 3 s (x=166.5), p99 60 s (x=375.1), p99.9 240 s (x=471.5), p99.99 780 s (x=553.5) — the last point sits left of the 15-minute line.
- **Point labels (11.5px in the line color, centered 10px above each dot):** without "4s", "3m", "26m"; with "3s", "60s", "4m", "13m".
- **Annotation (bold 12.5px `#e74c3c`, left-aligned at (300, 112)):** "nothing measured out here".
- **Caption (12px `#444`, bottom right at y=290):** "percentiles illustrative; the 99.99%-within-15-minutes commitment is documented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `01-buckets-keys-and-the-flat-namespace.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section two's callout uses the label "Common mistake:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead` and `rgba` as in page 01.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine risk: the shaded gap and the not-here segment in `c1`, the never-copied rows in `c2`, the unmeasured tail in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, so each holds one line at the 50/50 split without wrapping while still carrying a full clause. That is shorter than the folder default (~90–100) but deliberately **not** clipped to stubs — an earlier pass cut them to ~55–70 characters and was rejected for compromising quality. Do not shorten them further, and do not restore the eight-bullet reference-style sections.
- **Register and vocabulary.** Plain technical English, fewer product names than the earlier draft: say "the write succeeded", not "the PUT returns 200"; say "a deadline you can pay for" / "the paid option", not Replication Time Control or RTC; say "the delete crosses over only if you enable that option", not `DeleteMarkerReplication`; say "a backfill job", not S3 Batch Replication; say "a role you hand the service", not an IAM role ARN. The precise API and configuration names live on the sibling pages.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No cost section.** A fourth section priced a replicated month (destination storage, cross-region transfer, per-object replication requests, the 2.32× multiple, `c4`'s stacked bars); it was cut for making the page longer than the concept needs. Cost belongs on `19-cost-egress-and-data-gravity`.
  - **No active-active or single-endpoint bullets.** They travelled with the cost section and are configuration trivia at this level.
  - **Three sections, 7–8 short bullets each.**
  - Measuring the gap — lag metrics, stale reads at the destination, failover risk — is out of scope; `12-replication-lag-in-practice` covers it.
- **Data:** all values are hardcoded literals, no randomness anywhere.
  - **Documented behaviour the page stands on:** copying is asynchronous and begins after the source write succeeds; versioning must be enabled on both buckets; the service assumes a role you supply; configuration is a set of rules scoped by prefix or tag; cross-region and same-region replication share the same machinery; metadata, tags and permissions replicate; deletes replicate only when that option is enabled; encrypted objects replicate only with a destination key configured; objects predating a rule require a separate backfill job; a delete scoped to one version is not replicated by design; objects that arrived by replication are not forwarded onward; expiry rules are evaluated per bucket; the paid tier commits to 99.99% of objects within 15 minutes and adds pending-bytes, pending-operations and latency metrics plus a breach event.
  - **Illustrative and labelled as such:** the 43 ms write, the four-second copy in the text and the 4 s copy-lands marker in `c1`, and every percentile in `c3`.
  - **Computed geometry:** `c1` maps 5 s onto 550 px from x0=110 (110 px/s), so the 4 s copy lands at x = 550, which coincides with the 4 s tick; lane B's absent-segment label centres at (110 + 550)/2 = 330 and its arrived label at (550 + 660)/2 = 605. `c2` uses a 28 px row pitch from y=52, giving tops 52…220 and baselines top+16. `c3`'s x positions are `90 + (log10(s)/log10(3600)) × 570` evaluated at each stated second count, computed in JS at render time rather than hardcoded.
