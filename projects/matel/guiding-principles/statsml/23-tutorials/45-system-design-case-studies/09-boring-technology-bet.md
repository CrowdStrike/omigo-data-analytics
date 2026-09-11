# Boring Technology Bet

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Boring Technology Bet

**Subtitle:** During hypergrowth an image-sharing site threw out its zoo of trendy datastores and bet everything on manually sharded MySQL — boring technology whose failure modes were already known

## The Datastore Zoo That Kept Failing

**Tags:** `core idea` (blue), `boring technology` (green), `hypergrowth` (orange)

- **The growth** — traffic was roughly doubling every six weeks, per the company's engineering blog
- **The zoo** — the stack had accreted MySQL plus Cassandra, Membase, and MongoDB, each added for "scale"
- **The failures** — each trendy store failed in a novel way the team had never seen and could not debug fast
- **The bet** — they consolidated onto manually sharded MySQL with memcached and Redis in front
- **The reason** — MySQL's failure modes were 15 years documented; the new stores' failures were surprises

*Example (italic):* A clustered datastore rebalances itself during peak traffic and the site goes down — nobody on the team has seen that failure before, so nobody knows the fix.

**Key point:** During hypergrowth, mature technology with well-known failure modes beats novel technology with unknown ones — you have time to operate, not time to pioneer.

### Visualization (canvas `c1`, 720×300)

Before/after flow diagram: the 2011 datastore zoo on the left (novel stores marked failed), a fat arrow, and the consolidated 2012 stack on the right.

- **Title (bold 15px, `#1a5276`, top center):** "2011: Six Datastores, Novel Failures → 2012: Three Boring Ones".
- **Left column ("the zoo", 12px `#6b7280` label at x=70, y=58):** six rounded boxes at x=60, width 170, height 30, at y = 70, 106, 142, 178, 214, 250, labels 12px `#2c3e50`: "MySQL", "Memcached", "Redis", "Cassandra", "Membase", "MongoDB"; the bottom three boxes get fill `rgba(231,76,60,0.12)`, 1px `#e74c3c` border, and a bold 12px red `#e74c3c` "✗ novel failure" at their right edge (x=240); the top three get fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border.
- **Arrow:** 4px `#1a5276` arrow from x=310 to x=420 at y=160, bold 12px `#1a5276` label "consolidate" above it.
- **Right column ("the boring stack", 12px `#6b7280` label at x=470, y=90):** three rounded boxes at x=460, width 200, height 34, at y = 105, 150, 195, fills `rgba(0,131,0,0.12)`, 1px `#008300` border, labels 12px `#2c3e50`: "MySQL (sharded)", "Memcached", "Redis", each with a bold 12px green `#008300` "✓" at its right edge.
- **Annotation (bold 13px green `#008300`, centered near x=560, y=255):** "known failure modes only".
- **Caption (12px `#444`, bottom left):** "stack lists from public scaling talks; layout schematic".

## One 64-Bit Number Says Where Everything Lives

**Tags:** `worked example` (blue), `ID packing` (green)

- **The scheme** — every object ID is 64 bits packing three fields: shard ID, type, local ID
- **The split** — 16 bits of shard ID, 10 bits of type, 36 bits of local ID; the top 2 spare bits stay 0
- **The decode** — shard = ID >> 46; type = (ID >> 36) & 0x3FF; local = ID & 0xFFFFFFFFF
- **Hand-check** — image ID 241294492511762325 decodes to shard 3429, type 1, local ID 7075733
- **The payoff** — any server computes the shard from the ID alone: no lookup service to call or to fail

*Example (italic):* Given image 241294492511762325, shifting right 46 bits yields 3429 — the request goes straight to shard db3429 with zero lookups.

**Key point:** Packing the shard ID into the object ID makes placement a pure function of the ID — the routing "service" is two bit-shifts that can never be down.

### Visualization (canvas `c2`, 720×300)

Bit-packing diagram: one 64-bit ID drawn as a segmented bar, each segment decoded into its field value below.

- **Title (bold 15px, `#1a5276`, top center):** "Image ID 241294492511762325, Decoded by Bit Position".
- **ID bar:** one horizontal bar at y=95, height 44, from x=60 to x=660 (600px = 64 bits, 9.375px/bit), split into four segments left to right: spare 2 bits (width 19, fill `#e5e9ef`, no label), shard ID 16 bits (width 150, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border), type 10 bits (width 94, fill `rgba(217,89,38,0.25)`, 2px `#d95926` border), local ID 36 bits (width 337, fill `rgba(0,131,0,0.20)`, 2px `#008300` border).
- **Segment labels (bold 12px, inside each segment, matching border colors):** "shard · 16 bits", "type · 10", "local ID · 36 bits".
- **Decoded values:** three arrows (2px, segment colors) dropping from segment centers to y=190; boxed values 13px bold at y=200: "shard = 3429" (blue `#2a78d6`), "type = 1 (image)" (orange `#d95926`), "local = 7075733" (green `#008300`).
- **Formula line (12px `#6b7280`, mono, centered at y=245):** "shard = id >> 46    type = (id >> 36) & 0x3FF    local = id & 0xFFFFFFFFF".
- **Annotation (bold 13px violet `#4a3aa7`, right side at y=70):** "no lookup table anywhere".
- **Caption (12px `#444`, bottom right):** "ID and decode values from a public sharding blog post".

## 4096 Virtual Shards, Rebalanced by Moving Them Whole

**Tags:** `where it's used` (blue), `rebalancing` (green)

- **The layout** — they launched with 4096 virtual shards: 8 MySQL hosts, each holding 512 databases
- **The map** — a small config maps shard ranges to physical hosts; the IDs never mention hosts
- **The split** — to grow, replicate a host, point half its shard range at the copy, flip the config
- **No re-hash** — rows never move between shards; whole shards move between machines instead
- **Locality** — a user's boards and images are created on the user's shard, so their joins stay local

*Example (italic):* When host 1 gets hot, databases db0257–db0512 are replicated to a new host 9 and the config map flips — 4096 shards before, 4096 after, zero rows re-hashed.

**Key point:** Far more virtual shards than machines turns rebalancing into "copy some databases and edit a map" — capacity grows while every ID keeps meaning what it always meant.

### Visualization (canvas `c3`, 720×300)

Before/after host diagram: one overloaded host splitting its 512 shards across two hosts by moving whole databases, with the config map as the only thing that changes.

- **Title (bold 15px, `#1a5276`, top center):** "Growing Capacity: Move Whole Shards, Never Re-Hash Rows".
- **Before panel (left, 12px `#6b7280` label "before" at x=80, y=62):** one rounded box at x=60, y=75, width 230, height 70, fill `rgba(217,89,38,0.18)`, 2px `#d95926` border, 13px bold `#2c3e50` two-line label "host 1 — 512 shards / db0001–db0512", with bold 12px orange `#d95926` "hot" tag at its top right.
- **Arrows:** two 3px `#1a5276` arrows from the box's right edge (x=290) fanning to the after-panel boxes; 12px `#1a5276` label "replicate, then flip config" between them at x=340, y=110.
- **After panel (right, label "after" at x=480, y=62):** two rounded boxes width 210, height 54: at x=470, y=75, fill `rgba(42,120,214,0.18)`, 2px `#2a78d6` border, "host 1 — db0001–db0256"; at x=470, y=145, fill `rgba(0,131,0,0.15)`, 2px `#008300` border, "host 9 (new) — db0257–db0512".
- **Config strip:** full-width rounded box at x=60, y=225, width 620, height 34, fill `#f8f9fa`, 1px `#e5e9ef` border, 12px mono `#2c3e50` text "shard map:  1–256 → host 1   257–512 → host 9   ...   3841–4096 → host 8" with the "257–512 → host 9" span drawn bold green `#008300`.
- **Annotation (bold 13px green `#008300`, centered at x=370, y=205):** "4096 shards before and after — only the map changed".
- **Caption (12px `#444`, bottom right):** "8 hosts × 512 databases at launch, per the blog; split shown illustrative".

## The Price: Your Joins Stop at the Shard Border

**Tags:** `common mistake` (red), `trade-off` (orange)

- **The deal** — computable placement was bought by giving up cross-shard joins and transactions
- **App joins** — a image on shard 3429 owned by a user on shard 12 means two queries, joined in code
- **No global TX** — nothing atomically updates rows on two shards; the app orders writes to tolerate it
- **The design answer** — colocate what you join: a user's boards and images live on the user's shard
- **The mistake** — sharding first and discovering later that every screen needs a five-shard join

*Example (italic):* A "popular images by friends" page can no longer be one JOIN — it becomes N shard queries merged in application code, so the team designed pages around per-shard data.

**Common mistake:** Treating sharding as a transparent scaling knob. It deletes cross-shard JOINs and transactions from your toolbox — the team paid that price knowingly and restructured its data layout around it.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a same-shard query that keeps its JOIN (green) vs a cross-shard query that becomes two round-trips merged in the application (red).

- **Title (bold 15px, `#1a5276`, top center):** "Same Shard: One JOIN. Cross-Shard: Two Queries + App Code".
- **Row 1 (y=95), 12px `#444` label "colocated" at x=20:** one rounded box at x=130, width 220, height 46, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px two-line label "shard 3429 / user 42 · boards · images"; 3px `#008300` arrow to a green-bordered box at x=440, width 220, height 46, label "one local JOIN" with bold 12px green `#008300` "✓ 1 round-trip" at its right.
- **Row 2 (y=200), label "cross-shard" at x=20:** two stacked small boxes at x=130, width 160, height 34 (y=178 and y=222): "image → shard 3429" (fill `rgba(42,120,214,0.15)`, 1px `#2a78d6`) and "owner → shard 12" (fill `rgba(217,89,38,0.18)`, 1px `#d95926`); two 3px `#e74c3c` arrows converging on a red-bordered box at x=420, width 240, height 46, fill `rgba(231,76,60,0.10)`, label "merge in application code" with bold 12px red `#e74c3c` "✗ no SQL JOIN, no shared TX" beneath it at y=262.
- **Annotation (bold 13px magenta `#d55181`, top right at y=65):** "the price of lookup-free placement".
- **Caption (12px `#444`, bottom left):** "shard numbers reuse the worked example; query flow illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all geometry and values are the hardcoded literals above (no randomness). The ID decode (241294492511762325 → shard 3429, type 1, local 7075733), the 16/10/36 bit split, and the 8 hosts × 512 databases layout are from a public sharding blog post; the datastore lists are from its public scaling talks; box positions, the host-split scenario, and query flows are schematic and labeled illustrative.
- Stick to the publicly-blogged design only; do not attribute undocumented internals to any company.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
