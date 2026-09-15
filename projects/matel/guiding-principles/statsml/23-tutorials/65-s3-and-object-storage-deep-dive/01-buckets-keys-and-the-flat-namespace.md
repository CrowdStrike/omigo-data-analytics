# Buckets, Keys & the Flat Namespace

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Buckets, Keys & the Flat Namespace

**Subtitle:** A bucket is one flat map from a key string to bytes — how a bucket name is checked, and why the 1,024-byte key limit counts bytes and not characters

## The Bucket Is One Map from Key to Bytes

**Tags:** `core idea` (blue), `key → bytes` (green), `flat map` (orange)

- **The bucket** — a retail shop writes all of its hourly click files into one bucket
- **The key** — a file's whole name is its key, slashes and all, not just the last part
- **One map** — the bucket is one map from that whole key string to the file's bytes
- **No folders** — nothing named `clicks/` exists on its own — only whole keys are stored
- **Slashes** — they are ordinary characters inside the key, with no special meaning
- **Metadata** — size, content type and last-modified time sit beside the file's bytes
- **The address** — the bucket name plus the key together name exactly one object
- **One step** — you read a file by giving the whole key, never by opening a folder first

*Example (illustrative):* Reading the 2pm file means asking for the exact key `clicks/date=2026-08-26/hour=14.parquet`.

**Key point:** A bucket is not a small filesystem. It is one flat map whose keys happen to contain slashes.

### Visualization (canvas `c1`, 720×300)

Two-column mapping table: the key string on the left, the file's bytes plus metadata on the right, with the 2pm row highlighted and an arrow between the two cells.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "One Bucket Is One Map: Key String → Bytes + Metadata".
- **Column headers (bold 12px `#1a5276`, left-aligned, baseline y=68):** "key (one string)" at x=50, "value (bytes + metadata)" at x=400.
- **Four rows, height 40, pitch 48 derived in JS as `y(i) = 80 + i × 48`** → tops 80, 128, 176, 224. Key cell x=50 w=340, value cell x=400 w=270, 6px radius.
  - `clicks/date=2026-08-26/hour=13.parquet` → "4.2 MB · 12,801 rows"
  - `clicks/date=2026-08-26/hour=14.parquet` → "5.1 MB · 15,470 rows" — highlighted
  - `clicks/date=2026-08-26/hour=15.parquet` → "4.7 MB · 14,102 rows"
  - `manifest/latest.json` → "2.0 KB · 1 pointer"
- **Row style:** normal key cell fill `rgba(42,120,214,0.10)` with 1.5px `#2a78d6`; normal value cell fill `rgba(0,131,0,0.09)` with 1.5px `#008300`. The highlighted row uses 0.22 and 0.18 alpha and 2.5px borders in the same two colors.
- **Text:** key in 12px monospace `#2c3e50` at cell x+10, value in 12px system-ui `#2c3e50` at cell x+10, both on the row's centre line (top+26).
- **Arrows:** 2px `#6b7280` short horizontal arrow from x=391 to x=399 on each row's vertical middle (top+20).
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=278):** "one lookup, no folder to open — the key is the whole address".
- **Caption (12px `#444`, bottom right):** "keys and sizes illustrative; the flat key → object model is documented".

## What Makes a Bucket Name Legal

**Tags:** `rule of thumb` (blue), `naming rules` (green), `globally unique` (orange)

- **Length** — a bucket name is 3 to 63 characters long, so `sc` is rejected outright
- **Lowercase only** — `Shop-Clicks` is refused because capital letters are not allowed
- **Allowed characters** — only letters, numbers, dots and hyphens; underscores fail
- **Start and end** — it must begin and end with a letter or number, so `shop-clicks-` fails
- **Not an IP** — a name shaped like an address, `192.168.10.5`, is refused as well
- **Unique everywhere** — no two accounts anywhere can hold the same bucket name
- **One region** — a bucket is created in one region and stays there for its whole life
- **Dots hurt** — dots break the certificate match over HTTPS, so hyphens are the norm

*Example (illustrative):* The name `shop-clicks` passes every rule at 11 characters; `shop.clicks.data` is legal but its dots break the certificate match.

**Key point:** Bucket names are name-server shaped, unique everywhere, and permanent. Pick one plain hyphenated name — renaming means copying every object.

### Visualization (canvas `c2`, 720×300)

Validator list: eight candidate names in two columns, each with a mark, the name, and the one rule it hits.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "Which Bucket Names Are Accepted".
- **Column header (bold 12px `#1a5276`, left-aligned at x=40, baseline y=68):** "candidate name → verdict and rule".
- **Layout:** two columns at x=40 and x=380, four rows derived in JS as `y(i) = 92 + i × 46` → 92, 138, 184, 230.
- **Entries (name, mark, color, reason):**
  - `shop-clicks` → ✓ green `#008300` — "valid: 11 characters, lowercase and hyphen"
  - `Shop-Clicks` → ✗ red `#e74c3c` — "capital letters are not allowed"
  - `shop_clicks` → ✗ red `#e74c3c` — "underscore is not an allowed character"
  - `sc` → ✗ red `#e74c3c` — "2 characters: the minimum is 3"
  - `192.168.10.5` → ✗ red `#e74c3c` — "shaped like an IP address"
  - `shop-clicks-` → ✗ red `#e74c3c` — "must end in a letter or number"
  - `shop clicks` → ✗ red `#e74c3c` — "spaces are not allowed"
  - `shop.clicks.data` → ⚠ yellow `#c98500` — "legal, but dots break the certificate"
- **Entry style:** mark bold 13px in its color at the entry x; name 12.5px monospace `#2c3e50` at x+22; reason 12px `#6b7280` at x+22 on the next line (+17px).
- **Divider:** vertical dashed (4/3) 1px `#e5e9ef` line at x=362 from y=76 to y=258.
- **Annotation (bold 13px orange `#d95926`, centered at y=278):** "3 to 63 characters, and unique across every account".
- **Caption (12px `#444`, bottom right):** "candidate names illustrative; every rule shown is documented policy".

## A Key Is 1,024 Bytes, Not 1,024 Characters

**Tags:** `worked example` (blue), `byte budget` (green), `UTF-8` (orange)

- **The limit** — a key can be at most 1,024 bytes long, however long it looks
- **Bytes, not letters** — the cap counts bytes of UTF-8 text, not characters
- **The plain key** — the hourly key is 38 characters and exactly 38 bytes of ASCII
- **Add Japanese text** — inserting `地域=東京/` makes the same key 44 characters
- **But 52 bytes** — each of those 4 Japanese characters costs 3 bytes, not 1
- **Worst case** — a key of only 3-byte characters fits 341 characters, since 341 × 3 = 1,023
- **Emoji are worse** — those cost 4 bytes each, so 256 characters is the hard cap
- **Safe names** — keep to letters, digits, `/`, `=`, `-` and `.` to avoid surprises

*Example (illustrative):* The plain key uses 38 of 1,024 bytes (3.7%); with the Japanese part it uses 52 bytes (5.1%) — 6 more characters cost 14 more bytes.

**Key point:** Count bytes, not characters. A key that looks short can be much closer to the limit once non-ASCII text enters the naming scheme.

### Visualization (canvas `c3`, 720×300)

Byte-budget bars: the 1,024-byte limit drawn as a full-width track, with the plain key and the Japanese key filling different amounts, plus the two worst-case caps below.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "The 1,024-Byte Key Budget: Characters Are Not Bytes".
- **Track geometry:** x=180, width 480 = 1,024 bytes, so px per byte = 480/1024 = 0.46875; track height 26; rows at y = 90 and y = 150; empty track fill `#f4f6f9` with 1.5px `#e5e9ef`.
- **Counts are computed at render time from the key strings themselves** — a `utf8len(s)` helper sums 1/2/3/4 bytes per code point, and `Array.from(s).length` gives the character count. Nothing about the two bars is a hardcoded number: bar width, row label, byte label and percentage all derive from the key string.
- **Row 1 (blue `#2a78d6`, alpha 0.85):** key `clicks/date=2026-08-26/hour=14.parquet` → 38 characters, 38 bytes, 17.8 px of track.
- **Row 2 (orange `#d95926`, alpha 0.85):** key `clicks/地域=東京/date=2026-08-26/hour=14.parquet` → 44 characters, 52 bytes, 24.4 px of track.
- **Row labels (12px `#2c3e50`, right-aligned at x=170, on the track's centre line):** the computed "38 characters" and "44 characters".
- **Byte labels (bold 12px in the row color, left-aligned at track x + bar width + 12):** the computed "38 bytes (3.7%)" and "52 bytes (5.1%)", percentage rounded to one decimal from bytes/1024.
- **Key strings (11.5px monospace `#6b7280`, under each row at +18px below the track):** the two keys above.
- **Right-end marker:** vertical 2px `#1a5276` at x=660 from y=80 to y=190; bold 12px `#1a5276` "1,024 bytes" centered above at (660, 72).
- **Difference annotation (bold 13px magenta `#d55181`, left-aligned at (180, 208)):** text built from the two rows' computed counts — "+6 characters cost +14 bytes".
- **Worst-case lines (bold 12px, left-aligned at x=60, y=240 and y=262):** aqua `#199e70` "3-byte characters only → 341 characters (341 × 3 = 1,023)" and violet `#4a3aa7` "4-byte characters only → 256 characters (256 × 4 = 1,024)", with both products computed in JS.
- **Caption (12px `#444`, bottom right):** "key strings illustrative; byte counts computed, 1,024-byte limit documented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). All three callouts use the label "Key point:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead` and `rgba` exactly as in `11-cross-region-replication.html`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine rejection: the crosses in `c2`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, holding one line at the 50/50 split while still carrying a full clause rather than a clipped stub. An earlier pass clipped them to ~55–70 characters and was rejected for compromising quality. Do not shorten them further, and do not pad them back out to the folder's ~90–100 default either.
- **Register and vocabulary.** Plain technical English for a first-time reader, fewer product and API names than the earlier draft: say "reading the file" and "asking for the key", not GET/PUT/LIST/HEAD; say "the whole name is the key", not "the object key"; say "metadata sits beside the bytes", not content-type and last-modified header names; say "dots break the certificate match", not virtual-hosted-style TLS. No analogies and no invented scenes — the shop's click files are named data, not a story. The exact API and configuration names live on the sibling pages.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No sorted-listing section.** A fourth section covered lexicographic key order (unpadded `hour=2` / `hour=9` / `hour=10`, the zero-padding fix, the byte values 0x31 and 0x32, its `c4` two-column order comparison, plus per-account bucket quota and prefix-separation bullets). It was cut for making the page longer than the concept needs. Sort order and paging belong on `03-listing-pagination-and-prefix-scans`; the "there are no folders" argument belongs on `02-directories-are-a-lie`.
  - **No bucket-quota or prefix-per-team bullets.** They travelled with the cut section and are capacity-planning trivia at this level.
  - **No reserved-prefix or reserved-suffix naming bullets.** They are lookup-table detail, not something a first-time reader needs.
  - **Three sections, 8 short bullets each, one canvas per section (`c1`, `c2`, `c3`).**
- **Data:** all values are hardcoded literals or computed at render time; no randomness anywhere.
  - **Illustrative and labelled as such:** the shop, the bucket's object keys, and the MB sizes and row counts in `c1`; the candidate names in `c2`; the two key strings in `c3`.
  - **Documented behaviour the page stands on:** a bucket is a flat key → object map with no real directories; slashes are ordinary key characters; bucket plus key identifies an object; bucket names are 3–63 characters of lowercase letters, numbers, dots and hyphens, must start and end with a letter or number, may not be formatted as an IP address, are unique across all accounts, belong to one region, and lose certificate matching if they contain dots; a key is a UTF-8 string of at most 1,024 bytes.
  - **Computed, and verifiable by hand:** `clicks/date=2026-08-26/hour=14.parquet` is 38 characters and 38 UTF-8 bytes; inserting `地域=東京/` adds 6 characters (4 Japanese at 3 bytes, `=` and `/` at 1 byte) giving 44 characters and 38 + 12 + 2 = 52 bytes; 38/1024 = 3.7% and 52/1024 = 5.1%; the difference is +6 characters and +14 bytes; 341 × 3 = 1,023 and 256 × 4 = 1,024. `c3` recomputes every one of these from the key strings at render time via `utf8len` and `Array.from(...).length` rather than printing literals.
  - **Computed geometry:** `c1` rows come from `80 + i × 48`; `c2` rows from `92 + i × 46`; `c3` bar widths from `bytes × 480/1024` and each byte label's x from the track x plus that width.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
