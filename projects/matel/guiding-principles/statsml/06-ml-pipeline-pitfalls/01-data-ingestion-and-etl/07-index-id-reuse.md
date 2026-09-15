# Pitfall: Index / ID Reuse

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Index / ID Reuse

**Subtitle:** IDs get recycled or reset, causing joins to match wrong records and corrupt training labels.

## The Problem

Tags: `the trap` (red), `join keys` (blue)

- **Recycled IDs** — counters reset after deletions or migrations, so one ID names two entities
- **Cross-period collisions** — session_id 1234 in January and February are different sessions
- **Bare joins** — joining on ID without a timestamp merges a deleted user with a new one
- **Orphaned children** — a recycled parent ID leaves old child rows tied to the wrong parent
- **Partition-scoped IDs** — unique per partition, not globally, so cross-partition joins fan out

*Example:* Monthly-reset session IDs match January's mobile session 1234 to February's desktop session 1234, and accuracy drops 22% on real mobile traffic.

**Impact:** Training rows mix features from unrelated entities, and the corruption looks exactly like valid data.

### Visualization (canvas `c1`, 720×300)

Flow diagram: ID reuse causing a wrong join between two monthly partitions.

- **Title (bold 14px `#1a5276`, top center):** "ID Reuse: Join Matches Wrong Records".
- **January partition:** label "JANUARY PARTITION" in bold 12px `#3498db` at (180, 60); white record box (60, 75, 240×60) with `#3498db` 2px border containing three 11px lines: `session_id: 1234`, `user_type: "mobile"`, `region: "US-West"`.
- **February partition:** label "FEBRUARY PARTITION" in bold 12px `#9b59b6` at (180, 165); white record box (60, 180, 240×60) with `#9b59b6` 2px border containing: `session_id: 1234`, `user_type: "desktop"`, `region: "EU-Central"`.
- **Reset marker:** orange `#e67e22` dashed line (dash 3/3) between the two boxes at y=150 (x 100–260), labeled "ID counter reset" in bold 10px `#e67e22` at (180, 155).
- **Wrong-join arrow:** red `#e74c3c` dashed bezier curve (dash 8/4, width 3) looping from the right edge of the January box (300, 105) out to x≈370 and back to the February box (300, 210), with a red arrowhead; labeled "JOIN ON" / "session_id" in bold 11px red at x=370.
- **Result box:** white box (420, 75, 260×165) with 3px red border. Header "CORRUPTED TRAINING ROW" in bold 13px red. Body lines in 11px `#2c3e50`: `session_id: 1234`, `user_type: "mobile" (Jan)`, `region: "US-West" (Jan)`; then in bold 11px red: `+ events: "desktop" (Feb)`, `+ events: "EU-Central" (Feb)`. Below, in 10px red centered: "Features from two different entities" / "Model learns spurious patterns".

## Why It Happens

Tags: `root cause` (orange), `uniqueness` (blue)

- **Rarely intentional** — reuse emerges from designs that assume local uniqueness is global
- **Auto-increment resets** — counters restart when tables are recreated or databases migrated
- **Session recycling** — session IDs return to the pool once their TTL (time-to-live) expires
- **Shared namespaces** — test and production environments issue IDs from the same range
- **Uncoordinated services** — microservices generate IDs independently with no coordination

*Example:* After a migration user_id 1001 stops meaning Alice and starts meaning Bob, so pre/post joins silently merge two different people.

**Root Cause:** Assuming IDs are globally unique across time and systems when they are only unique within one context.

### Visualization (canvas `c2`, 720×300)

Diagram: two database cylinders whose records share the same ID but name different people, colliding in the middle.

- **Title (bold 14px `#1a5276`, top center):** "Same ID, Different Entities After Migration".
- **DB v1 cylinder (left, centered at x=130, top y=70):** database cylinder shape (ellipse caps 60×15, body 120×70) filled `#eaf2f8` with `#1a5276` 2px stroke; label "DB v1 (Pre-Migration)" in bold 12px `#1a5276`. Record box below (80, 165, 100×50), white with `#1a5276` 1.5px border: `ID: 1001` / `Name: Alice` / `Role: Admin` in 11px `#2c3e50`.
- **DB v2 cylinder (right, centered at x=530, top y=70):** same cylinder shape filled `#f5eef8` with `#9b59b6` stroke; label "DB v2 (Post-Migration)" in bold 12px `#9b59b6`. Record box (480, 165, 100×50) with `#9b59b6` border: `ID: 1001` / `Name: Bob` / `Role: Viewer`.
- **Collision zone (center):** rectangle (250, 155, 160×70) filled `rgba(231,76,60,0.1)` with red dashed border (dash 5/3); red arrows point into it from both record boxes; a thick red X (line width 4) drawn inside; label "COLLISION" in bold 11px red at (330, 215).
- **Bottom explanation (centered at x=330):** "JOIN ON user_id = 1001" in bold 12px red at y=250; then in 11px `#2c3e50`: "Merges Alice's history with Bob's profile" (y=270) and "Two people become one corrupted record" (y=285).

## The Correct Approach

Tags: `the fix` (green), `provenance` (blue)

- **Invisible damage** — a corrupted join looks valid afterward, so keys must carry identity
- **Use UUIDs** — globally unique keys instead of recycled counters remove collisions by design
- **Composite keys** — include source system and timestamp so same-ID rows cannot collide
- **No bare joins** — never join across systems or time periods on an auto-increment ID alone
- **Row provenance** — stamp source_id and ingestion_date on every row to keep origins traceable
- **Verify uniqueness** — test key uniqueness in the data itself and log every recycled ID

*Example:* JOIN ON user_id AND source='prod_v2' AND created_date >= '2025-01-01' eliminates cross-migration collisions.

**Fix:** Design join keys to be collision-proof across time, environment, and source system — add qualifying dimensions where uniqueness can't be guaranteed.

### Visualization (canvas `c3`, 720×300)

Diagram: two UUID-based records joined safely, plus a bad-vs-good join pattern comparison.

- **Title (bold 14px `#1a5276`, top center):** "Safe Joins with Qualifying Dimensions".
- **Left UUID record box (40, 50, 200×100):** fill `#eafaf1`, `#27ae60` 2px border, header "UUID-BASED RECORD" in bold 11px green; 10px monospace lines: `id: a3f7...c9e2`, `source: "prod_v2"`, `created: 2025-03-15`, `user: Alice`.
- **Right UUID record box (480, 50, 200×100):** same style, lines: `id: b8d1...4f7a`, `source: "prod_v2"`, `created: 2025-03-15`, `user: Alice`.
- **Safe join:** green horizontal arrow between the boxes at y=100 with arrowhead; green checkmark (3px stroke) above the arrow; label "SAFE JOIN" in bold 10px green at (360, 95).
- **Correct join pattern box (40, 175, 640×55):** fill `#f8f9fa`, `#1a5276` 2px border; label "Correct Join Pattern:" in bold 11px `#1a5276`; monospace 10px line: `JOIN ON user_id AND source = 'prod_v2' AND created_date >= '2025-01-01'`.
- **Bad approach box (40, 250, 280×35):** white fill with red 2px border; centered red 10px monospace "JOIN ON user_id" and bold 10px "Collisions possible"; red X mark (3px stroke) just right of the box.
- **Good approach box (370, 250, 310×35):** fill `#eafaf1` with green 2px border; centered green 10px monospace "JOIN ON user_id + source + date" and bold 10px "Collision-proof"; green checkmark (3px stroke) just left of the box.

## Regeneration instructions

- **Layout:** three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"), each with an h2 underlined by `2px solid #2980b9` and a `table.layout` (border-collapse, one `<tr>`): left `<td class="text-col">` (45%) holds `.tags` pills, a `<ul>` of labeled bullets, a `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (55%) holds one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; `li b` and `.metric strong` in `#1a5276`; `ul` 0.92rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Callouts:** `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, `1px solid #e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; accents `#3498db`, `#9b59b6`, `#2980b9`; text `#2c3e50`/`#666`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
