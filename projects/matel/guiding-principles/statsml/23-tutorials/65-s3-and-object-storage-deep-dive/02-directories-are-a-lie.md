# Directories Are a Lie

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Directories Are a Lie

**Subtitle:** The slash is an ordinary character inside a name — the console builds the folder tree from the sorted list of names every time you look, so an empty folder is really a zero-byte object

## Running example — the five names carried through every section

One bucket, `shop-clicks`, holding exactly five objects. Sorted, which is the order a listing returns them in. The five names and their sizes live in the charts (`c1` and `c3`), not in a table in the text column.

| # | Name | Size | What it is |
|---|------|------|------------|
| 1 | `clicks/date=2026-08-26/hour=13/part-0000.parquet` | 1.4 MB | data file |
| 2 | `clicks/date=2026-08-26/hour=13/part-0001.parquet` | 1.1 MB | data file |
| 3 | `clicks/date=2026-08-26/hour=14/part-0000.parquet` | 1.6 MB | data file |
| 4 | `clicks/date=2026-08-26/hour=15/` | 0 B | folder marker |
| 5 | `clicks/date=2026-08-26/manifest.json` | 2 KB | index file |

Sort check (all five share `clicks/date=2026-08-26/`; compare what follows): `hour=13/part-0000.parquet` < `hour=13/part-0001.parquet` < `hour=14/part-0000.parquet` < `hour=15/` < `manifest.json`, because `h` sorts before `m`. Name 4 is the zero-byte marker — section three explains it.

## Five Names, Zero Folders

**Tags:** `core idea` (blue), `flat names` (green), `the slash` (orange)

- **The bucket** — one bucket named shop-clicks holding exactly five objects, nothing else
- **The names** — three data files, one small index file, and one odd name ending in a slash
- **One flat string** — each object's whole name is a single string, slashes and all
- **The slash** — an ordinary character inside that name, no more special than a dash
- **No folder command** — there is no way to create a directory, only to write objects
- **Sorted, not nested** — the names are kept in one sorted list, and that is the index
- **No parent and child** — nothing anywhere records one name as living inside another
- **The console draws it** — the folder tree is built from that list, then thrown away

*Example (italic):* The name `clicks/date=2026-08-26/hour=13/part-0000.parquet` is one string of 48 characters; the three slashes inside it are just characters.

**Key point:** A bucket is a sorted list of names, each pointing at some bytes. Every folder anyone has seen in a storage console was drawn from names like these.

### Visualization (canvas `c1`, 720×300)

Two panels: the flat sorted list of names on the left, the tree the console draws on the right, with an arrow between them.

- **Title (bold 15px, `#1a5276`, centered at y=24):** "The Same Five Names: the Sorted List vs the Tree the Console Draws".
- **Panel labels (12px `#444`, left-aligned):** "what the storage holds — five names, sorted" at (16, 56); "what the console shows" at (392, 56).
- **Left panel:** rounded rect x=14, y=64, 332×200, 8px radius, fill `rgba(42,120,214,0.06)`, 1px `#e5e9ef` border. Five rows of 11px monospace text at x=20, baseline `90 + i × 28` (90, 118, 146, 174, 202) — the five names verbatim; name 4 in magenta `#d55181`, the other four in `#2c3e50`. A 12px `#6b7280` note "one name per object" at (20, 236).
- **Right panel:** rounded rect x=388, y=64, 318×200, 8px radius, fill `rgba(25,158,112,0.05)`, 1px `#e5e9ef` border. Boxes 8px radius, 2px border, 12px `#2c3e50` centered label:
  - `clicks/` at (400, 76, 96×28), blue `#2a78d6`, fill `rgba(42,120,214,0.15)`
  - `date=2026-08-26/` at (420, 112, 148×28), blue, same fill
  - four child boxes at x=446, w=140, h=24, tops `146 + i × 30` (146, 176, 206, 236): `hour=13/` and `hour=14/` aqua `#199e70` fill `rgba(25,158,112,0.15)`; `hour=15/` magenta `#d55181` fill `rgba(213,81,129,0.15)` (it is the folder marker); `manifest.json` green `#008300` fill `rgba(0,131,0,0.12)`.
- **Connectors (2px `#6b7280`):** elbow from (414, 104) down to (414, 126) then right to (420, 126); a trunk at x=434 from y=140 down to each child's vertical middle (`top + 12`), then right to x=446.
- **Arrow (3px violet `#4a3aa7`):** (350, 164) → (384, 164), filled arrowhead.
- **Annotation (bold 12px violet `#4a3aa7`, centered at y=284):** "the tree on the right is built from the list on the left — it is never stored".

## Where the Folder Tree Comes From

**Tags:** `mechanism` (blue), `grouping` (green), `per request` (orange)

- **Reading the list** — you ask for the names that start with some piece of text
- **The stop character** — you also name a character to stop reading each name at
- **Without it** — all five names come back in full, one by one, in their sorted order
- **With it** — names that share the next part collapse into one group instead
- **The groups** — here hour=13/, hour=14/ and hour=15/ come back as the groups
- **Groups are the folders** — the console draws one folder for each group it gets
- **One real object** — only manifest.json comes back as a file you can read
- **Recomputed each time** — the grouping is text work, and it is stored nowhere

*Example (italic):* Asking for names that start with `clicks/date=2026-08-26/` and stopping at `/` returns three groups and one object — even though five names live under that text.

**Key point:** The folder tree is something you ask for, not something stored. Change the character you stop at and the same five names group a different way.

### Visualization (canvas `c2`, 720×300)

Request on the left, response split into groups and objects on the right, using the example's literal strings.

- **Title (bold 15px, `#1a5276`, centered at y=24):** "One Read of the List: What You Ask For, What Comes Back".
- **Request box:** rounded rect x=16, y=70, 268×120, 8px radius, fill `rgba(42,120,214,0.10)`, 2px `#2a78d6`. Bold 12px `#1a5276` "what you ask for" at (30, 92); 12px `#2c3e50` "names starting with" at (30, 116); 11px monospace `#2c3e50` `clicks/date=2026-08-26/` at (30, 136); 12px `#2c3e50` "stop reading at the next /" at (30, 162).
- **Arrow (3px `#6b7280`):** (288, 130) → (340, 130), filled arrowhead; 12px `#6b7280` label "one call" centered at (314, 118).
- **Groups box:** rounded rect x=348, y=56, 356×112, fill `rgba(25,158,112,0.10)`, 2px `#199e70`. Bold 12px `#199e70` label "groups — N" at (362, 76), where N is the length of the groups array at render time (3). Three 11px monospace `#2c3e50` lines at x=362, baselines `100 + i × 22` (100, 122, 144): `clicks/date=2026-08-26/hour=13/`, `…hour=14/`, `…hour=15/` (each written out in full).
- **Objects box:** rounded rect x=348, y=180, 356×62, fill `rgba(0,131,0,0.10)`, 2px `#008300`. Bold 12px `#008300` label "objects — N" at (362, 200), N computed from the objects array (1). One 11px monospace `#2c3e50` line `clicks/date=2026-08-26/manifest.json` at (362, 224).
- **Annotation (bold 12px magenta `#d55181`, left-aligned):** "the three data files are absent —" at (16, 218); "they sit inside hour=13/ and hour=14/" at (16, 236).
- **Caption (12px `#444`, right-aligned at y=292):** "grouping by a stop character is standard object-storage listing behaviour".

## The Zero-Byte Object Behind an Empty Folder

**Tags:** `worked example` (blue), `folder marker` (green), `zero bytes` (orange)

- **Create folder** — the button has no folder to make, so it writes a tiny object
- **The odd name** — it ends with a slash and the object itself holds nothing at all
- **Zero bytes** — there are no contents, so the listing shows a size of 0 B
- **Why it works** — that name groups into a folder when you stop at the slash
- **A real object** — you can read it, see it in the full list, and delete it
- **The only way** — an empty folder that survives is always an object like this
- **Reader hazard** — a job may hand this empty object to a data reader and fail
- **Delete the last file** — with no marker left, the folder vanishes from the tree

*Example (italic):* The object named `clicks/date=2026-08-26/hour=15/` holds zero bytes; delete it and the empty folder disappears from the console.

**Key point:** An empty folder in object storage is a zero-byte object whose name ends with a slash. Nothing else makes a folder with no files inside it appear.

### Visualization (canvas `c3`, 720×300)

The whole list, with no stop character, as a three-column table; the folder-marker row is highlighted.

- **Title (bold 15px, `#1a5276`, centered at y=24):** "The Whole List: the Folder Marker Is Just Another Object".
- **Header (bold 12px `#1a5276`, baseline y=64):** "Name" left-aligned at x=56, "Size" right-aligned at x=520, "What it is" left-aligned at x=548; 1px `#1a5276` rule from (52, 72) to (700, 72).
- **Rows:** five rows, baseline `ys[i] + 4` where `ys[i] = 100 + i × 32` (100, 132, 164, 196, 228). Name in 11px monospace at x=56; size 12px right-aligned at x=520; "what it is" 12px at x=548. Sizes `1.4 MB`, `1.1 MB`, `1.6 MB`, `0 B`, `2 KB`; notes "data file", "data file", "data file", "folder marker", "index file". Non-highlighted text: name `#2c3e50`, size and note `#6b7280`.
- **Highlight:** row 4 (index 3) on a `rgba(213,81,129,0.12)` band at x=52, y=`ys[3] − 14` (182), 648×28, 6px radius, with a 2px `#d55181` left edge; its three cells in bold magenta `#d55181`.
- **Row separators:** 1px `#e5e9ef` from x=52 to x=700 at `ys[i] + 12` under rows 1, 2, 3 and 5 (not the highlighted row).
- **Annotation (bold 12px magenta `#d55181`, left-aligned at (56, 262)):** "'Create folder' wrote this one — no bytes, name ends in '/', deletable".
- **Caption (12px `#444`, right-aligned at y=292):** "sizes illustrative; a zero-byte name ending in a slash is a real object".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the siblings `01-buckets-keys-and-the-flat-namespace.html` and `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — …`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>Key point:</strong>` label on all three sections).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No `table.keys` rule — the key table is no longer in the text column. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead` and `rgba` as on page 11.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Magenta is the folder marker throughout: name 4 in `c1`'s list, the `hour=15/` box in `c1`'s tree, and the highlighted row in `c3`. Red `#e74c3c` only for the `.key-point` border.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, so each holds one line at the 50/50 split while still carrying a full clause. An earlier pass clipped them to ~55–70 and was rejected for compromising quality. Do not shorten them further, and do not pad them back out to the folder's ~90–100 default either.
- **Register and vocabulary.** Plain technical English for a first-time reader, with far less API surface than the earlier draft: say "reading the list" and "the character you stop at", not LIST with `prefix=` and `delimiter=`; say "groups", not `CommonPrefixes`; say "the object has no contents", not `Content-Length: 0` on a HEAD; say "the button writes a tiny object", not "the console PUTs a zero-byte object". No analogies and no invented scenes — the five names are the whole example. The exact parameter and header names live on the sibling pages.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No rename section.** A fourth section costed renaming `clicks/` to `raw-clicks/` over 240,000 objects and 1,800 GB (240 listing calls, 480,240 requests, $1.20, 480 s, `c4`'s two-lane diagram); it was cut for turning a tutorial into a pricing exercise. Request-and-byte cost belongs on `19-cost-egress-and-data-gravity`, and the copy-then-delete mechanics belong with the commit-protocol material.
  - **No directory-bucket exception bullet.** The one storage class that does have a real hierarchy is the whole subject of `07-directory-buckets-and-express-one-zone`; naming it here invites a digression.
  - **No pagination arithmetic.** The 1,000-names-per-page cap and the call counts that follow from it are `03-listing-pagination-and-prefix-scans`.
  - **No key table in the text column.** The five names and sizes are shown once, in `c1` and `c3`. Re-adding a `table.keys` block duplicates them and invites drift.
  - **Three sections, eight bullets each.**
- **Data:** all values are hardcoded literals; no randomness anywhere.
  - **Documented behaviour the page stands on:** the flat key space with no directory objects; names held in one sorted order; listing filtered by a leading string and grouped by a stop character, returning groups plus objects; the grouping recomputed per request and stored nowhere; zero-byte names ending in a slash acting as folder markers and being ordinary, deletable objects; a prefix vanishing from a listing once its last object is gone.
  - **Illustrative and labelled as such:** the bucket name, the five names and the per-object sizes (`c3`'s caption carries the label).
  - **Computed and verifiable:** `clicks/date=2026-08-26/hour=13/part-0000.parquet` is 48 characters (7 + 16 + 8 + 17). The sorted order of the five names is the order shown, since `h` precedes `m`. `c2`'s "groups — 3" and "objects — 1" counts are the array lengths read at render time, not literals.
  - **Computed geometry:** `c1`'s list baselines are `90 + i × 28` and its tree children are `146 + i × 30` with h=24, so the last child ends at 260, inside the panel's 264 bottom edge — and `manifest.json` sits on its own row rather than beside `hour=15/`, so no connector crosses a box. `c2`'s group lines are `100 + i × 22`. `c3`'s row baselines are `100 + i × 32` and the highlight band is derived as `ys[3] − 14`, never a hardcoded pixel.
