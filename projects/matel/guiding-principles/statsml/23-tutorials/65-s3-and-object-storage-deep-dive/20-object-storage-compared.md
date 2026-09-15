# Object Storage Compared

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Object Storage Compared

**Subtitle:** Every object store is the same key-to-bytes map behind HTTP, so what you learn once travels — the differences that survive are how names are scoped and what happens when you write the same key twice

## What Every Object Store Shares

**Tags:** `core idea` (blue), `the model` (green), `portable knowledge` (orange)

- **Key to bytes** — every store is one flat map from a key string to a blob of bytes
- **HTTP is the wire** — you read and write objects over HTTP, with no mount, no file handle
- **Whole-object writes** — a write replaces the whole object; you cannot patch bytes in place
- **Range reads work** — you can read the first 64 KB of a 5 GB object without the rest
- **No atomic rename** — moving a key means copy it, then delete the old one: two steps
- **Listing is a prefix scan** — keys come back in sorted order, not as a folder listing
- **Durability by copies** — the bytes are kept on several machines rather than behind RAID
- **Learn it once** — these come from the model, so they hold on every vendor's store

*Example (italic):* A 5 GB Parquet file behaves the same everywhere — one write to create it, a small range read for its footer, and a copy plus a delete to move it.

**Key point:** Separate the model from the vendor. The flat map, the whole-object write, the sorted listing and the missing rename are inherent; nearly everything else is a product decision.

### Visualization (canvas `c1`, 720×300)

The shared model: a three-box flow from the HTTP request to the key to the bytes, with six shared properties as chips underneath.

- **Title (bold 15px, `#1a5276`, centered at y=24):** "The Shared Model: One Key String → One Blob of Bytes, Over HTTP".
- **Flow row — geometry computed, not hardcoded.** Widths `[210, 200, 190]`, gap 38; total = 676, so the row starts at `x = (720 − 676) / 2 = 22` and each box's x is the running sum. Boxes h=40 at y=60, 8px radius, 2px border, 12.5px centered label on the box's mid-line (y=85). Each box's fill is its border colour at 0.11 alpha via the `rgba` helper.
  - Box A "read · write · list over HTTP", fill `rgba(42,120,214,0.12)`, border `#2a78d6`.
  - Box B "one key: a plain string", fill `rgba(74,58,167,0.12)`, border `#4a3aa7`, monospace.
  - Box C "bytes + a little metadata", fill `rgba(0,131,0,0.10)`, border `#008300`.
  - Two 2px `#6b7280` arrows with heads across each gap at y=80, drawn from `boxRight` to `nextBoxLeft`.
- **Property chips (2 columns x=45 and x=375, 3 rows y=140,180,220; w=300 h=32, 6px radius, 1.5px border, 12px text at x+12).** Chip fill is the chip colour at 0.10 alpha, border the chip colour, label text `#2c3e50`. Every chip label is ≤44 characters so it clears the 300px chip.
  - "whole-object writes, no in-place edit" (blue)
  - "range reads on any object" (aqua)
  - "no atomic rename: copy, then delete" (magenta)
  - "listing is a sorted prefix scan" (violet)
  - "durability by copies, not RAID" (green)
  - "flat keys: `/` is just a character" (orange)
- **Annotation (bold 13px `#1a5276`, centered y=272):** "true of all of them — this is the model, not one vendor's choice".
- **Caption (12px `#444`, bottom right):** "structural properties of the model; no vendor-specific claims here".

## Naming: Two Levels or Three

**Tags:** `worked example` (blue), `naming scope` (green), `vendor difference` (orange)

- **S3 buckets** — a bucket name must be unique across all of AWS, not just your account
- **S3 keys are flat** — the bucket sits in one region and the key is a plain string
- **GCS buckets** — names are globally unique here too, and the key is one flat string
- **Azure adds a level** — a storage account holds containers, and a container holds blobs
- **Azure uniqueness** — only the account name is global; a container is unique inside it
- **Azure blob types** — block, append and page blobs are types with different write rules
- **Append does exist** — Azure's append blob disproves "object storage cannot append"
- **S3-compatible stores** — MinIO and others serve the S3 API over disks you run yourself

*Example (italic):* A container named `clicks` is free inside Alice's own Azure storage account, while the same name as an S3 or GCS bucket was claimed globally years ago.

**Key point:** Two names get you to an object on S3 and GCS, three on Azure. And "S3-compatible" is a range, not a yes or no: the basic calls match, the newer ones often do not.

### Visualization (canvas `c2`, 720×300)

Nested boxes drawn to scale: two levels on the left, three on the right, so the extra Azure level is visible, each level labelled with the scope its name must be unique in.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "Two Names Reach an Object on S3 and GCS, Three on Azure".
- **Column headers (bold 12.5px, left-aligned, y=52):** "Amazon S3 / Google Cloud Storage — 2 levels" in `#2a78d6` at x=45; "Azure Blob Storage — 3 levels" in `#d95926` at x=385.
- **Left stack (2 boxes, 8px radius):**
  - Outer x=45 y=64 w=300 h=176, fill `rgba(42,120,214,0.09)`, 2px `#2a78d6`. Bold 12.5px `#1a5276` "bucket" at (60, 86); 12px `#6b7280` "name is global · lives in one region" at (60, 104).
  - Inner x=67 y=124 w=256 h=64, fill `rgba(0,131,0,0.10)`, 2px `#008300`. Bold 12.5px `#008300` "key" at (80, 146); 11.5px monospace `#2c3e50` "clicks/hour=14.parquet" at (80, 166).
  - 12px `#6b7280` note at (60, 216): "no middle level — the key is the rest".
- **Right stack (3 boxes, 8px radius):**
  - Outer x=385 y=64 w=300 h=176, fill `rgba(217,89,38,0.09)`, 2px `#d95926`. Bold 12.5px `#d95926` "storage account" at (400, 86); 12px `#6b7280` "name is global" at (400, 104).
  - Middle x=405 y=112 w=260 h=128, fill `rgba(201,133,0,0.10)`, 2px `#c98500`. Bold 12.5px `#c98500` "container" at (418, 132); 12px `#6b7280` "unique inside the account only" at (418, 150).
  - Inner x=424 y=160 w=222 h=64, fill `rgba(0,131,0,0.10)`, 2px `#008300`. Bold 12.5px `#008300` "blob name" at (436, 182); 11.5px monospace `#2c3e50` "clicks/hour=14.parquet" at (436, 202).
- **Annotation (bold 13px violet `#4a3aa7`, centered y=264):** "only the Azure account name is global, so "clicks" is free inside it".
- **Caption (12px `#444`, bottom right):** "naming levels as documented; the sample key is illustrative".

## The Differences Worth Knowing

**Tags:** `comparison` (blue), `consistency` (green), `conditional writes` (orange)

- **Consistency today** — every major store now lets you read back the object you just wrote
- **What changed** — S3 was eventually consistent for reads until December 2020; not since
- **Real folders** — regular S3 buckets have none, while S3 directory buckets do have them
- **Optional folders** — GCS and Azure both offer a real hierarchy you turn on at creation
- **Conditional writes** — a write that lands only if the key is absent, or has not changed
- **Who had it first** — GCS and Azure had these checks years before S3 added them in 2024
- **Why it mattered** — S3 table formats needed an outside lock to commit; the others did not
- **Compatible endpoints** — an S3-compatible store may not support conditional writes; test

*Example (italic):* A commit that must not overwrite an existing key is one conditional write on GCS or Azure, and on S3 needed an outside lock table before 2024.

**Key point:** The differences that survive are whether a real folder hierarchy exists and how long conditional writes have been there. This surface still moves — check the current docs.

### Visualization (canvas `c3`, 720×300)

Comparison table: five stores as rows, four behaviours as columns, a coloured glyph plus a short word in each cell so it reads without hunting for a legend.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "Same Model, Different Behaviour: Five Stores × Four Questions".
- **Grid geometry (computed in JS from these constants):** `x0 = 200`, `colW = 118`, 4 columns → grid width 472, right edge 672. `y0 = 96`, `rowH = 28`, 5 rows → bottom edge 236.
- **Column headers (bold 12px `#1a5276`, centered on each column, two lines at y=62 and y=78):** "read back / what you wrote", "real folder / hierarchy", "conditional / writes", "append to / an object". Longest header word is under 90px, well inside the 118px column.
- **Row labels (12px `#2c3e50`, right-aligned at x0−8 = 192, on the row centre line):** "Amazon S3", "S3 directory bucket", "Google Cloud Storage", "Azure Blob Storage", "S3-compatible stores". The longest is about 130px, so it starts near x=62 and stays on canvas.
- **Cell marks (glyph bold 13px left-aligned at column centre −24, word 11.5px left-aligned at column centre −8, both on the row centre line):**
  - yes → `✓` green `#008300`, word "yes"
  - no → `–` mute `#6b7280`, word "no"
  - newer feature → `✓` yellow `#c98500`, word "yes*"
  - unsure or implementation-dependent → `○` yellow `#c98500`, word "check" or "varies"
- **Cell values by row:**
  - Amazon S3: yes · no · yes · no
  - S3 directory bucket: yes · yes · yes · yes*
  - Google Cloud Storage: yes · opt · yes · check
  - Azure Blob Storage: yes · opt · yes · yes
  - S3-compatible stores: yes · no · varies · varies
  - "opt" uses the yellow `○` glyph with the word "opt".
- **Row striping:** odd-indexed rows filled `#f7f9fb`; 1px `#e5e9ef` rules between rows and between columns, plus the outer border.
- **Footnote (11.5px `#6b7280`, left-aligned at x=48, y=258):** "\"opt\" = turned on at creation · \"check\" = verify in the current docs · * newer feature".
- **Annotation (bold 12.5px magenta `#d55181`, centered at y=278):** "GCS and Azure had conditional writes years before S3 did".
- **Caption (12px `#444`, bottom right, y=294):** "behaviour as documented at authoring; recheck before relying on a cell". The annotation is centred on its own line above the caption, so the two never overlap.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead` and `rgba` as in page 11.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is deliberately unused in the charts: "not supported" is a neutral model fact, not an error, so it is drawn in mute grey.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, so each holds one line at the 50/50 split while still carrying a full clause. Do **not** clip them to ~55–70 characters — an earlier pass did that and it was rejected as compromising quality. Do **not** pad them out to the folder default of ~90–100 either. Eight bullets per section, three sections.
- **Register and vocabulary.**
  - Plain technical English for a first reader. No analogies, no invented scenes, no story framing.
  - Say "read", "write", "list" — not GET, PUT, LIST or HEAD. Say "a write that lands only if the key is absent" — not `If-None-Match`, generation-match or ETag preconditions. Say "a real folder hierarchy" — not hierarchical namespace or ADLS Gen2.
  - **Product names legitimately stay.** The page's subject *is* a comparison of Amazon S3, Google Cloud Storage, Azure Blob Storage and S3-compatible stores such as MinIO. What was cut is the per-product API and parameter minutiae, not the product names.
  - Fewer details overall: this is a tutorial, not a reference course. Shorten wording, never drop a real fact.
- **Third-party accuracy rule — this page makes comparative claims about other vendors' products.** Never assert a capability, consistency guarantee or limit unless it is confidently documented. Where a claim is stale, vague or version-dependent, state it more carefully or drop it: a wrong comparative cell is worse than a missing one. Cells that are uncertain get the yellow `○` "check" mark and the footnote, not a green tick.
  - **Softened or dropped on purpose (do not restore):**
    - **Multi-region bucket column removed.** The old matrix marked Azure "opt", which conflated account-level geo-redundant storage with a genuinely multi-region bucket namespace. GCS multi-region buckets are real, but the column had only one confident cell, so the whole column went.
    - **"GCS never had an eventually consistent era" removed.** Too strong once list operations are included; the page now states only the S3 change in December 2020.
    - **GCS append is "check", not "opt".** Appending to an existing object on GCS is a newer and moving surface; the cell defers to the docs rather than claiming a behaviour.
    - **S3 directory-bucket append is marked "yes\*"** — documented, but new enough to carry the footnote.
    - **Azure ADLS Gen2 dropped as its own row.** The hierarchy fact it carried is now the single bullet "GCS and Azure both offer a real hierarchy you turn on at creation", which is true of both without naming the product tier.
    - **The "unique across an AWS partition" nuance became "unique across all of AWS."** Deliberate simplification for a first reader; the partition detail is exactly the kind of minutia this page cuts.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No lock-in / portability section.** A fourth section ("What Actually Locks You In") argued that the core API is portable while identity, event notifications, lifecycle rules and replication configuration are not, and that egress pricing is the real migration cost, with a `c4` portability-stack diagram. It was cut for making the page longer than the concept needs. Egress and data gravity belong on `19-cost-egress-and-data-gravity`; identity belongs on `14-iam-policy-evaluation`; notifications on `18-event-notifications`; replication on `11-cross-region-replication`.
  - **No "keep a thin adapter layer" advice.** It travelled with the cut section and is design guidance, not the concept.
  - **No exact API names, headers or parameters** anywhere on the page — pages `08` and `09` carry those.
  - Three sections, one canvas each (`c1`, `c2`, `c3`), eight bullets each.
- **Data:** all values are hardcoded literals; no randomness, and no `Math.random()`. There are no computed statistics on this page — the charts are structural diagrams and a behaviour table — so the accuracy work is in the geometry and in the cell values.
  - **Documented behaviour the page stands on:** every store is a flat key-to-bytes map addressed over HTTP; writes replace the whole object; byte-range reads; no atomic rename; listing is a sorted prefix scan; durability by replicating bytes across failure domains; S3 bucket names globally unique and buckets regional; S3 keys flat with `/` an ordinary character; GCS bucket names globally unique with a single flat key; Azure's storage account → container → blob levels with a global account name and account-scoped container names; Azure block, append and page blob types, the append blob being a genuine counterexample to "object storage cannot append"; S3 eventually consistent for reads until December 2020 and strongly consistent since; S3 directory buckets having a true folder hierarchy while general-purpose buckets do not; a real hierarchy available as a create-time option on GCS and on Azure; conditional writes available on GCS and Azure years before S3 added them in 2024; table formats on S3 needing an external lock for safe commits before that.
  - **Illustrative:** the sample key `clicks/hour=14.parquet` and the 5 GB Parquet file in the example — both labelled as illustrative in the caption or read plainly as an example.
  - **Computed geometry:** `c1`'s flow row derives each box x from the width array and gap rather than hardcoding pixel positions, and centres the row on the canvas. `c2`'s nested boxes are literal coordinates chosen so each inner box clears its parent's label lines; the longest inner text (the 22-character monospace key at 11.5px, about 152px) fits its 222px box. `c3` derives every cell x from `x0 + colW * i` and every row y from `y0 + rowH * i`; the longest row label is checked against the 192px available to its left and the longest cell word ("varies", "check") against the 118px column.
