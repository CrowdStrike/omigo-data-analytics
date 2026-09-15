# Versioning &amp; Delete Markers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Versioning &amp; Delete Markers

**Subtitle:** With versioning on, a write never replaces anything and a delete never frees anything — the old versions stay stored, and the stored total keeps growing

## One Key, Three Versions

**Tags:** `core idea` (blue), `version stack` (green), `newest is current` (orange)

- **The one key** — one file name that is written once and then overwritten twice more
- **Nothing is replaced** — each write stores a brand new version of the same file
- **Version ids** — every version is given its own id at the exact moment it is stored
- **The stack** — v1 at 40 MB, v2 at 42 MB and v3 at 45 MB all exist at the same time
- **The current one** — v3 is the newest one, so the older two are called noncurrent versions
- **A plain read** — asking for the file with no version named gives back v3 every time
- **Ask for a version** — name v1's id and you get that exact 40 MB file back, unchanged
- **Once on, always on** — you can pause versioning later but you can never remove it

*Example (illustrative):* A job rewrote the same file three times and believes it stored 45 MB; the bucket is holding 40 + 42 + 45 = 127 MB.

**Key point:** Versioning turns a key into the top of a stack. Every write adds to the stack instead of replacing what was there.

### Visualization (canvas `c1`, 720×300)

Vertical version stack for one key, the current version marked, sizes shown, and the stored-vs-newest arithmetic annotated underneath.

- **Title (bold 15px, `#1a5276`, centered, y=24):** "One Key, Three Versions — Only v3 Is Current".
- **Key caption (12.5px monospace `#6b7280`, centered, y=46):** `reports/daily.csv`.
- **Stack data (literal):** sizes `[40, 42, 45]` in MB for v1, v2, v3.
- **Stack boxes:** x=210, width 250, height 46, 6px radius, drawn bottom-up with `yFor(i) = 210 − i × 58`, so v1 at y=210, v2 at y=152, v3 at y=94. Labels are baselined at `yFor(i) + 28` (the vertical centre of the box).
  - v3 (current) fill `rgba(42,120,214,0.24)`, border 2.5px `#2a78d6`; label bold 13px `#1a5276` "v3 — 45 MB" centered at x=335.
  - v2 and v1 fill `rgba(107,114,128,0.10)`, border 1.5px `#6b7280`; labels 13px `#2c3e50` "v2 — 42 MB" and "v1 — 40 MB" centered at x=335.
- **Side labels — inside the box, right-aligned at x=450:** bold 12px `#2a78d6` "current" on v3; 12px `#6b7280` "noncurrent" on v2 and v1. They sit inside the boxes deliberately, so the left margin stays free for the write arrows (right-aligning them at x=200 collided with the arrow column).
- **Write arrows (2px `#199e70`, x=180, pointing up):** for i = 0 and 1, from `(180, yFor(i) + 10)` to `(180, yFor(i+1) + 36)` — so y 220→188 and y 162→130. A 12px `#199e70` label "write" is left-aligned at x=118 on the midpoint baseline (y=208 and y=150).
- **Current pointer:** 2.5px `#2a78d6` arrow from (600, 117) to (466, 117); bold 12.5px `#2a78d6` right-aligned at (700, 108) "a plain read" and (700, 126) "gives v3".
- **Version pointer:** 1.8px dashed (4/3) `#4a3aa7` arrow from (600, 233) to (466, 233); bold 12.5px `#4a3aa7` right-aligned at (700, 224) "ask for v1's" and (700, 242) "own id".
- **Arithmetic annotation (bold 13px magenta `#d55181`, centered, y=278):** built at render time from the sizes array — "stored = 40 + 42 + 45 = 127 MB · newest = 45 MB · 2.82×".
- **Caption (12px `#444`, right-aligned at y=294):** "sizes illustrative; the version stack is documented behaviour".

## The Delete That Deletes Nothing

**Tags:** `delete marker` (blue), `undelete` (green), `looks gone, still stored` (red)

- **A delete frees nothing** — it only adds a small delete marker on top of the stack
- **The marker is current** — so a plain read on that name now reports the file is gone
- **The bytes stay** — all three of v1, v2 and v3 still sit underneath the marker at 127 MB
- **Listings hide it** — a normal listing of the bucket skips over the key completely
- **Undelete** — remove that marker by its own id and v3 becomes the current file again
- **Real deletion** — bytes leave the bucket only when you delete a version by its own id
- **One at a time** — each version you want gone needs its own separate delete command
- **Why people want this** — an accidental overwrite stays recoverable instead of being lost

*Example (illustrative):* Alice deletes the key, the command reports success, the next read finds nothing — and the same 127 MB is still stored.

**Key point:** A delete is just another write. Bytes leave the bucket only when you delete a version by its own id.

### Visualization (canvas `c2`, 720×300)

Four states of the same key, left to right: each is a small stack, a read verdict, and the bytes still stored underneath.

- **Title (bold 15px, `#1a5276`, centered, y=24):** "Four States of One Key — The Bytes Only Leave in State 4".
- **Caption (12px `#444`, right-aligned at y=44):** "sizes illustrative; marker behaviour documented".
- **Columns:** width 135, x from `xFor(i) = 30 + i × 175`, giving x = 30, 205, 380, 555 (last column ends at 690).
- **Stored total:** computed at render time from the same `[40, 42, 45]` sizes as `c1` → 127 MB for states 1–3, 0 MB for state 4.
- **State headers (bold 12px `#1a5276`, centered on the column, y=64):** "1. three writes", "2. delete the key", "3. delete the marker", "4. delete each version".
- **Sub-headers (12px `#6b7280`, centered, y=80):** "no version named", "no version named", "by its own id", "by its own id".
- **Read verdict strip (y=96, height 26, 5px radius, full column width):** bold 12.5px centered at y=114 — states 1 and 3 → fill `rgba(0,131,0,0.14)`, border 1.8px `#008300`, text `#008300` "read → found"; states 2 and 4 → fill `rgba(231,76,60,0.14)`, border 1.8px `#e74c3c`, text `#e74c3c` "read → not found".
- **Stacks (boxes height 30, 5px radius, bottom-up from y=222 in **32px** steps):** four-box state 2 therefore tops out at y = 222 − 3 × 32 = 126, clearing the verdict strip's bottom edge at y=122. (A 34px step put the marker box at y=120 and overlapped the strip — do not restore it.)
  - State 1: v1, v2, v3 — v3 is current.
  - State 2: v1, v2, v3, then a delete-marker box on top — the marker is current.
  - State 3: v1, v2, v3 — v3 current again.
  - State 4: empty — a dashed `#e5e9ef` outline in the bottom slot with 12px `#6b7280` centered text "(gone)".
- **Box style:** noncurrent fill `rgba(107,114,128,0.10)` border 1.5px `#6b7280`; current data version fill `rgba(42,120,214,0.24)` border 2.5px `#2a78d6`; the marker fill `rgba(231,76,60,0.16)` border 2.5px `#e74c3c` with bold 12px `#e74c3c` label "delete marker".
- **Version labels (12px `#2c3e50`, centered):** "v1", "v2", "v3" — the current one bold `#1a5276`.
- **Stored line (bold 12px, centered under each column at y=272):** magenta `#d55181` "stored 127 MB" for states 1–3; `#008300` "stored 0 MB" for state 4.
- **Annotation (bold 13px violet `#4a3aa7`, centered, y=292):** "states 1 to 3 only change which version is current — the 127 MB never moves".

## Storage Grows With Versions, Not Keys

**Tags:** `worked example` (blue), `per version` (green), `common mistake` (red)

- **The setup** — 10,000 files of 20 MB each, every one rewritten once a day for a month
- **What you expect** — 10,000 × 20 MB = 200 GB, and the same total on every single day
- **What is stored** — after 30 days of daily rewrites every key holds 30 separate versions
- **Day 30 total** — 30 × 200 GB = 6,000 GB stored, thirty times the total you expected
- **Measured by the month** — the average across the 30 days is 3,100 GB, not 200 GB
- **The multiple** — that is 15.5 times the amount of storage you thought you had
- **The count hides it** — the file count sits at 10,000 all month and never hints at this
- **Deleting does not help** — deleting all 10,000 keys at once still frees no bytes at all

*Example (illustrative):* The team deleted a terabyte of stale keys and the stored total did not move at all.

**Common mistake:** Reading storage size off the file count. Storage counts every version, so a daily rewrite grows it without bound while the file count never changes.

### Visualization (canvas `c3`, 720×300)

Stacked bars of stored data across the month — the current version against the older versions piling up — against the flat "what you expected" line.

- **Title (bold 15px, `#1a5276`, centered, y=22):** "Stored Data Day by Day: Current Version vs Older Ones (illustrative)".
- **Caption (12px `#444`, right-aligned at y=44):** "file count, size and daily rewrite illustrative; arithmetic computed". It sits at the top right, not the bottom: at the bottom it collided with the centered "day of month" axis title.
- **Plot box:** x from 66 to 690, y from 58 (yMax = 6,000 GB) down to 252 (0 GB), so `yOf(v) = 252 − (v / 6000) × 194`.
- **Gridlines (1px `#e5e9ef`) and y labels (12px `#6b7280`, right-aligned at x=58):** 0, 1,500, 3,000, 4,500, 6,000 GB. Baseline axis line 1.5px `#1a5276` at y=252.
- **Bars for days 1, 5, 10, 15, 20, 25, 30** — width 46, centred in evenly spaced slots (`slotW = (690 − 66) / 7 = 89.14`, centre `66 + slotW × (i + 0.5)`):
  - current segment = 200 GB, fill `rgba(0,131,0,0.55)`, border 1px `#008300`.
  - older segment = (*d* − 1) × 200 GB stacked above, fill `rgba(213,81,129,0.55)`, border 1px `#d55181`.
  - Totals: day 1 = 200, day 5 = 1,000, day 10 = 2,000, day 15 = 3,000, day 20 = 4,000, day 25 = 5,000, day 30 = 6,000 GB.
- **x labels (12px `#2c3e50`, centered, y=270):** "d1", "d5", "d10", "d15", "d20", "d25", "d30"; axis title 12px `#6b7280` centered at y=292 "day of month".
- **"What you expected" line:** 2px dashed (6/4) `#1a5276` horizontal line at `yOf(200)` = 245.5 across the plot. It coincides exactly with the top of every green current segment, which is the point. **It carries no in-plot label** — a label there sat inside the day-5 bar; it is named in the legend instead.
- **Legend (left of the tall bars, swatches at x=100, text left-aligned at x=118):** green 11×11 swatch at y=66 with 12px "current version (200 GB, flat)" baselined at y=76; magenta swatch at y=84 with "older versions (grow daily)" at y=94; a 2px dashed (6/4) `#1a5276` line swatch drawn at y=108 from x=100 to x=111, with "200 GB — what you expected" at y=112.
- **Average callout (bold 13px `#d55181`, left-aligned at (100, 138)):** computed at render time by averaging *d* × 200 over days 1…30 — "month average 3,100 GB = 15.5× the 200 GB".
- **Day-30 callout (bold 12.5px `#d55181`, left-aligned at (100, 162)):** "day 30: 6,000 GB = 30× the 200 GB". Both callouts sit in the left half, where the bars are short; at x=270 the day-25 bar overlapped them.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `01-buckets-keys-and-the-flat-namespace.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section three's callout uses the label "Common mistake:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead` and `rgba` as in page 01.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine risk: the delete marker and the "read → not found" verdicts in `c2`.
- **Bullet length — this is the deliberate calibration of the page.** Eight bullets per section, each roughly **75–90 characters** including the bold lead term, so every one carries a full clause and still holds a single line at the 50/50 split.
  - An earlier pass clipped them to ~55–70 characters; that was **rejected** for compromising quality. Do not shorten them back to stubs.
  - Equally, do not pad them out to the folder default of ~90–100 characters. This page sits deliberately below it.
- **Register and vocabulary.** Plain technical English for a first encounter, with much less API surface than the earlier draft: say "write" and "read", not PUT / GET / HEAD; say "a normal listing", not the listing API names; say "the read finds nothing", not "returns 404"; say "version id" in plain lower case, never a realistic id string; say "you can pause versioning", not the bucket state names. **No analogies, no invented scenes, no story framing.** The exact API and configuration names live on the sibling pages.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No lifecycle section.** A fourth section, "The Lifecycle Rule That Caps It", covered the cleanup rule that bounds the growth: a 7-day noncurrent-expiry window, the keep-N-newest variant, the expired-marker sweep, the 1,600 GB plateau, MFA Delete and Object Lock, and a `c4` chart of straight-climb vs sawtooth. It was cut for turning a tutorial into a configuration reference. Lifecycle and expiry belong on a lifecycle page; the money side of it belongs on `19-cost-egress-and-data-gravity`. Do not fold any of it back into sections one to three.
  - **No dollar figures anywhere.** The earlier draft priced the month at $0.023 per GB-month ($71.30 vs $4.60, then $209.30, then $32.51 / $36.80 with the rule). Section three teaches the growth in GB only; pricing belongs on `19-cost-egress-and-data-gravity`.
  - **No bucket-state trivia.** The three-state model and "suspend is not purge" collapse into one bullet, "you can pause versioning later but you can never remove it".
  - **No marker-overhead bullet.** "Markers are not free / each one is a version lifecycle must walk" went with the lifecycle section.
  - **Three sections, eight bullets each, one canvas each (`c1`, `c2`, `c3`).**
- **Data:** all values are hardcoded literals, no randomness anywhere. Version labels are short symbolic strings (`v1`, `v2`, `v3`) — no realistic version-id strings.
  - **Illustrative and labelled as such:** the key `reports/daily.csv`, the 40 / 42 / 45 MB version sizes, and the 10,000-file × 20 MB × daily-rewrite workload.
  - **Documented behaviour the page stands on:** a write stores a new version rather than replacing the old one; each version carries its own id and can be read by naming it; a plain read returns the newest version; a delete with no version id adds a zero-byte delete marker that becomes current so a plain read finds nothing; a normal listing shows only current versions; removing the marker restores the previous version; only a delete that names a version id removes bytes, and only that one version; versioning cannot be switched back off once enabled, only paused.
  - **Computed at render time:** `c1` and `c2` both derive the stored total from the literal sizes array — 40 + 42 + 45 = 127 MB, and `c1`'s ratio 127 / 45 = 2.82×. `c3` sums *d* × 200 GB over days 1…30 (200 × 465 = 93,000 GB-days), divides by 30 for the 3,100 GB month average, and divides by 200 for the 15.5× multiple; the day-30 bar is 30 × 200 = 6,000 GB, thirty times the 200 GB line. Note 200 GB = 10,000 × 20 MB uses 1,000 MB per GB throughout.
  - **Computed geometry:** `c1`'s boxes come from `yFor(i) = 210 − i × 58` and its write arrows are derived from the same function; `c2`'s columns from `xFor(i) = 30 + i × 175` with a 32px stack step; `c3`'s bars from evenly spaced slots of `(690 − 66) / 7` px with `yOf(v)` mapping 0…6,000 GB onto y 252…58.
  - **Collision fixes to preserve:** `c1`'s side labels sit inside the boxes (right-aligned x=450) so the write-arrow column at x=180 is clear; `c2` uses a 32px stack step so the four-box state clears the verdict strip; `c3` moves its caption to the top right and its legend and callouts to the left half, and gives the 200 GB line no in-plot label.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
