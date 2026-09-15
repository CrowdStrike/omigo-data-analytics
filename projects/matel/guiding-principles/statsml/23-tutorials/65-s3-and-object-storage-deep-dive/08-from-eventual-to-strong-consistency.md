# From Eventual to Strong Consistency

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** From Eventual to Strong Consistency

**Subtitle:** For years a file you had just written could read back as missing, so pipelines carried retries and side databases to cope — since December 2020 a write is visible to the next read

## The Write Succeeded, The Read Said Nothing Was There

**Tags:** `core idea` (blue), `before / after` (green), `visibility` (orange)

- **The setup** — one job writes a file, a second job reads that same file moments later
- **The old answer** — the read could come back saying that no such file exists yet
- **Even so** — the write had already succeeded and the data was safely stored on disk
- **Why it happened** — the read reached a copy of the file index that lagged behind
- **Not the data** — only the record of the file's name was late, never the bytes themselves
- **The new answer** — since December 2020 the next read always finds the file just written
- **What changed** — the guarantee now covers writes, overwrites, deletes and listings
- **Why it matters** — write a partition then read it back is what every pipeline does

*Example (illustrative):* The write succeeds, the read comes 40 ms later; the old model could answer "no such file", the new one always answers with the file.

**Key point:** Consistency here is about *when a write becomes visible*, not about whether it was saved. The old model could tell an honest reader that a saved file did not exist.

### Visualization (canvas `c1`, 720×300)

Two stacked panels for the same writer/reader pair — the old model where the read reaches a lagging copy of the index and is told the file is missing, and the new model where the same read returns the file.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "The Same Two Jobs, Before and After".
- **Panel offset:** both panels share one geometry, panel B drawn 124 px below panel A. Panel A baselines: header y=46, writer chip top y=56, reader chip top y=100, note baseline y=138. Panel B = each of those + 124, derived in JS as `dy = i * 124`.
- **Chip helper:** rounded rect h=24, 6px radius, bold centred label, text baseline chip top+16.
- **Writer chip:** x=150 w=120, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`, bold 12px `#2c3e50` "write (t = 0)".
- **Reader chip:** x=300 w=120, fill `rgba(74,58,167,0.13)`, 2px `#4a3aa7`, bold 12px `#2c3e50` "read (t = 40 ms)".
- **Response chip:** x=450 w=210, 2.5px border, bold 12px in the border colour. Panel A: fill `rgba(231,76,60,0.14)`, `#e74c3c`, "no such file — stale". Panel B: fill `rgba(0,131,0,0.14)`, `#008300`, "the file, every time".
- **Panel headers (bold 12.5px, left at x=56):** A in `#e74c3c` "before Dec 2020 — the index copies could lag"; B in `#008300` "since Dec 2020 — every copy answers with the write".
- **Lane labels (12px `#6b7280`, right-aligned at x=142, on each chip's text baseline):** "writer job", "reader job".
- **Arrows (2px, arrowheads):** `#6b7280` diagonal from (270, writer top+18) to (298, reader top+4); horizontal in the response colour from (420, reader top+12) to (448, reader top+12).
- **Notes (12px `#6b7280`, left at x=300):** A "the read landed on index copy 2, which had not seen the write"; B "index copy 2 now answers with the write too".
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=278):** "the data was safe in both eras — only its visibility changed".
- **Caption (12px `#444`, bottom right at y=294):** "timing illustrative; the Dec 2020 change is documented".

## The Existence Check That Broke The Read

**Tags:** `common mistake` (red), `old behaviour` (green), `legacy bug` (orange)

- **The old rule** — only a read of a brand-new file name was ever guaranteed to work
- **Everything else** — overwrites, deletes and listings could all return old answers
- **The catch** — checking whether the name existed first voided even that guarantee
- **Why** — the failed lookup was remembered by one index copy as "no such file here"
- **The result** — the next read was answered from that memory, not from the new file
- **The irony** — a check written to keep the data safe was exactly what broke the read
- **Gone now** — check the name, then write, then read is safe on today's storage
- **Why learn it** — it explains the sleeps and retries you find in old pipeline code

*Example (illustrative):* Alice's job checks the name so it will not overwrite anything, is told nothing is there, writes the file, reads it back — and is told again that nothing is there.

**Key point:** Under the old model the safest-looking code was the most fragile. Checking for a name before creating it was exactly what gave up the one guarantee on offer.

### Visualization (canvas `c2`, 720×300)

The trap as four steps down the page, with step 2 marked as the cause and a dashed link from it to the stale answer in step 4.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "Four Steps, and the One That Causes the Stale Answer".
- **Rows:** x=62 w=500 h=44, 6px radius; row `i` top = 60 + i × 50, giving 60, 110, 160, 210.
- **Row text:** main line bold 12.5px `#2c3e50` at x=74, baseline top+19; detail line 12px `#6b7280` same x, baseline top+36; step number 12px `#6b7280` right-aligned at x=54 on baseline top+19.
- **Row 1 (fill `rgba(201,133,0,0.12)`, 2px `#c98500`):** "check the name — does this file exist?" / "answer: no such file, which is true at this moment" · verdict "no" in `#c98500`.
- **Row 2 (fill `rgba(231,76,60,0.16)`, 2.5px `#e74c3c`):** "the answer is remembered as \"no such file\"" / "this step is the cause — one index copy now holds that answer" · verdict "remembered" in `#e74c3c`.
- **Row 3 (fill `rgba(42,120,214,0.12)`, 2px `#2a78d6`):** "write the file — succeeds" / "the data is safe; the writer has no reason to doubt it" · verdict "saved" in `#008300`.
- **Row 4 (fill `rgba(231,76,60,0.16)`, 2.5px `#e74c3c`):** "read the file — told nothing is there" / "answered from the remembered \"no\", not from the new file" · verdict "stale" in `#e74c3c`.
- **Verdict glyphs:** bold 13px, left-aligned at x=586, on each row's main text baseline.
- **Cause link:** 2.5px dashed (4/3) `#e74c3c` vertical line at x=576 from y=132 to y=226, with an arrowhead pointing down at y=232.
- **Annotation (bold 13px `#d55181`, centered at y=272):** "the check before the write is what voided the guarantee".
- **Caption (12px `#444`, bottom right at y=294):** "the pre-2020 remembered-miss caveat is documented".

## The Workarounds, Now Dead Weight

**Tags:** `where it's used` (blue), `legacy code` (green), `removed` (orange)

- **Retry loops** — read again with a growing pause until the file finally showed up
- **Fixed sleeps** — pause a few seconds before reading and hope that was long enough
- **An outside index** — a separate database listing every file written to the bucket
- **How it worked** — the writer recorded the name there and the reader trusted it
- **The cost** — one more service to run, pay for and keep in step with the bucket
- **Temp name then rename** — so a reader never saw a half-written file in its place
- **Now removed** — the tools that kept that outside index have since been deleted
- **Reading old code** — a sleep or a side database next to a bucket is a 2020 fossil

*Example (illustrative):* A job asked the side database which of the day's 24 hourly files existed, because listing the bucket could miss files written minutes earlier.

**Key point:** These workarounds bought consistency with an extra database and extra code. Today they buy nothing — keeping them pays twice for what the storage already gives.

### Visualization (canvas `c3`, 720×300)

The old arrangement: the writer records each name in a side database as well as writing the file, the reader asks the database before reading, and the whole database path is struck out as removed.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "The Side Database Readers Used Instead of Listing".
- **Box helper:** rounded rect, 6px radius, 2px border; one centred bold 12.5px `#2c3e50` line vertically centred, or two lines at top+22 (bold 12.5px `#2c3e50`) and top+40 (12px `#6b7280`).
- **Writer box:** x=50 y=70 w=130 h=50, fill `rgba(42,120,214,0.12)`, `#2a78d6`, "writer job".
- **Bucket box:** x=250 y=52 w=190 h=52, fill `rgba(0,131,0,0.10)`, `#008300`, "the bucket" / "listing could lag behind".
- **Database box:** x=250 y=136 w=190 h=52, fill `rgba(74,58,167,0.12)`, `#4a3aa7`, "side database" / "one row per file written".
- **Reader box:** x=520 y=92 w=140 h=50, fill `rgba(213,81,129,0.12)`, `#d55181`, "reader job".
- **Arrows (2px, arrowheads):** `#2a78d6` (180, 88) → (248, 78); `#4a3aa7` (180, 100) → (248, 158); `#d55181` (518, 124) → (442, 162); `#d55181` (518, 110) → (442, 84).
- **Arrow labels (12px, left-aligned):** `#2a78d6` "write the file" at (188, 66); `#4a3aa7` "record the name" at (188, 132); `#d55181` "1. ask the database" at (452, 196); `#d55181` "2. read the bytes" at (456, 44).
- **Strike-out (3px `#e74c3c`):** an X across the database box — (250, 188)→(440, 136) and (250, 136)→(440, 188).
- **Removal note (bold 12.5px `#e74c3c`, left at x=60, baseline y=216):** "removed after Dec 2020 — the tools that kept this index are gone".
- **Dead-weight lines (bold 12px, left at x=60):** aqua `#199e70` at y=240 "retry loops and sleeps: safe to delete"; yellow `#c98500` at y=260 "temp name then rename: still used to finish a job cleanly".
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=278):** "one extra database, one extra failure mode, nothing gained today".
- **Caption (12px `#444`, bottom right at y=294):** "diagram illustrative; the tool removal is documented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `01-buckets-keys-and-the-flat-namespace.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). All three callouts use the label "Key point:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead`, `arrow` and `rgba` as in page 01.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine failure: the stale answers in `c1` and `c2`, the remembered-miss step, and the struck-out database path in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, so each holds one line at the 50/50 split without wrapping while still carrying a full clause. That is shorter than the folder default (~90–100), and it is deliberately **not** clipped to stubs — an earlier pass cut them to ~55–70 characters and was rejected for compromising quality. Do not shorten them further, and do not pad them back out to the folder default either. Eight bullets per section.
- **Register and vocabulary.** Plain technical English for a first-time reader, with far less API surface than the earlier draft: say "write" and "read", not PUT/GET/HEAD/LIST; say "listing the bucket", not a LIST call; say "told nothing is there", not `404 Not Found`; say "a copy of the index", not a named metadata replica; say "the side database" and "the tools that kept it", not S3Guard, EMRFS consistent view, DynamoDB, or a Hadoop release number. No analogies, no invented scenes, no story framing — the mechanism is described directly. This is a tutorial, not a reference course: the precise API names live on the sibling pages.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No coverage-matrix section.** A fourth section inventoried what strong consistency does and does not guarantee (new key, overwrite, delete, listing covered; cross-region, two racing writers, and building a lock not covered) with a `c4` check/cross matrix. It was cut: the same-region-versus-another-region guarantee inventory is already taught in plainer language on `13-cap-for-object-storage`, and compare-and-swap belongs on `09-conditional-writes-and-compare-and-swap`.
  - **No last-writer-wins or ordering bullets.** They travelled with the cut section and are concurrency, not visibility — out of scope here.
  - **No cross-region lag material.** `11-cross-region-replication` and `12-replication-lag-in-practice` cover it.
  - **No rollout trivia** (all regions, no extra cost, no API change) and no partial-read bullet — details the concept does not need.
  - **Three sections, eight bullets each, one canvas per section (`c1`, `c2`, `c3`).**
- **Data:** all values are hardcoded literals, no randomness anywhere.
  - **Documented behaviour the page stands on:** before December 2020 the guarantee covered only reads of a brand-new name, while overwrites, deletes and listings could return an older answer; that new-name guarantee was void if the name had been looked up before it was written, because the miss was cached; in December 2020 the storage gained strong read-after-write for reads, writes and listings; pipelines worked around the old model with retry loops, sleeps, and an external database of written names, and those tools have since been removed because the storage guarantee replaced them.
  - **Illustrative and labelled as such:** the 40 ms gap between the write and the read in `c1`, the "index copy 2" naming, the 24-hourly-files anecdote, and Alice as the actor.
  - **Computed geometry:** `c1` draws one panel body twice with a 124 px vertical offset derived in JS (`dy = i * 124`) rather than two hardcoded coordinate sets; `c2` derives row tops as `60 + i * 50`; every chip and box position in all three charts comes from the named constants above, not from repeated magic numbers.
  - **Label collisions:** each chart's centred annotation sits at y=272–278 and its right-aligned caption at y=294, so the two never share a baseline; chip labels are kept short enough to fit their box width (hence "read (t = 40 ms)", not a longer phrase in a 120 px chip).
