# Semantically Informative Unique IDs

**Page type:** detail page (backlog-style two-column layout table: text left 45%, canvas right 55%, one `.card-section` per numbered h2; section 6 also has a full-width comparison table)
**HTML title tag:** Semantically Informative Unique IDs

**Subtitle:** IDs that carry meaning — type, origin, time, hierarchy — while remaining globally unique.

**Philosophy callout (blue accent):** **Core tension:** UUIDs guarantee uniqueness but are opaque. Sequential IDs are readable but collide across systems. The goal: an ID scheme where seeing the ID tells you what kind of thing it is, roughly when it was created, and where it came from — without a lookup.

## 1. Anatomy of a Semantic ID

A semantic ID is a concatenation of segments, each with one job. Nothing is decorative:

- **Type prefix** — fixed-length code (`run_`, `feat_`, `mod_`) sufficient on its own to route the ID to the right subsystem or table.
- **Time segment** — epoch-based (compact, sortable) or date-string (readable). Makes lexicographic sort equal chronological sort.
- **Scope / entity** — dataset, tenant, or parent handle when the hierarchy is genuinely fixed.
- **Random suffix** — the only part carrying the uniqueness guarantee. Its width is a probability budget, not a formatting choice.

**Key point (red-accent callout):** **The contrast:** a UUID gives you uniqueness and nothing else — every question about the row costs a lookup. A segmented ID answers "what, when, where" from the string itself, and pays for that with a schema you now have to keep true.

**Example (italic):** Example: `run_20260811_143022_7f3a` — pipeline run, date-sortable, short random suffix.

### Visualization (canvas `c1`, 720×300)

Segmented-ID anatomy diagram contrasted with an opaque UUID box.

- **Title (bold 14px `#1a5276`, centered):** "Anatomy: Segmented ID vs Opaque UUID"
- **Segments (row of boxes starting x=44, y=56, height 32, 15px monospace text, tinted fills at 0.18 alpha, 1.5px stroke in segment color; each with a gray leader line down to alternating two-line labels — bold 10px label in segment color + 10px gray sub-label):**
  - `run_` — "type prefix" / "routes to subsystem" — `#1a5276` (fill `rgba(26,82,118,0.18)`)
  - `20260811` — "date" / "sort = chronological" — `#27ae60` (fill `rgba(39,174,96,0.18)`)
  - `143022` — "time" / "intra-day ordering" — `#27ae60`
  - `7f3a` — "random suffix" / "collision budget" — `#e67e22` (fill `rgba(230,126,34,0.18)`)
- **Opaque UUID contrast (at y=210):** a single gray box (fill `#f8f9fa`, 1.5px `#999` stroke) containing the 13px monospace text `EXAMPLE_UUID_redacted` (a masked placeholder — the source deliberately uses a redacted stand-in rather than a real 8-4-4-4-12 hex UUID). To its right, 10px annotations: "one undivided segment" in red `#e74c3c`, and "every question needs a lookup" in gray `#888`.
- **Caption (11px `#888`, bottom center):** "Each segment does one job; only the suffix carries the uniqueness guarantee"

## 2. The Parseability Ladder

Meaning packed into a key is not monotonically good. There is a workable middle:

- **Fully opaque** — `EXAMPLE_UUID_redacted`. Zero coupling, zero debuggability.
- **Typed prefix** — `feat_01J9Z8…`. Cheap win: type is visible, nothing mutable is asserted.
- **Structured semantic** — `feat:ames:lot_area:profile_v2`. Readable in logs and stack traces; scope is now a contract.
- **Over-encoded** — tier, region, and status crammed in too. Every extra encoded attribute is another fact that can go stale.

(HTML comment in source next to "Fully opaque" bullet: "Note: masked for illustration; real UUIDs are 8-4-4-4-12 hex format".)

**Key point (red-accent callout):** **The rule:** encode only facts that are true for the lifetime of the row. Debuggability saturates early on this ladder; brittleness keeps climbing.

**Example (italic):** Also on the ladder: `mod-cnn-v3-shape-20260801` — model artifact with version and date, readable but coupled to a naming convention.

### Visualization (canvas `c2`, 720×300)

Two-series line chart: debuggability vs brittleness across four ladder rungs.

- **Title (bold 14px `#1a5276`, centered):** "Parseability Ladder: Meaning Gained vs Facts That Can Go Stale"
- **Plot area:** x=56, width = canvas−100, y=52, height 186; light `#eee` horizontal gridlines at 0/25/50/75/100 with 9px gray y-labels (0, 25, 50, 75, 100); `#ccc` L-shaped axes.
- **X categories (bold 10px `#2c3e50` label + 9px monospace gray sub-label):**
  1. "fully opaque" / `uuid4`
  2. "typed prefix" / `feat_01J9Z8…`
  3. "structured" / `feat:ames:lot_area`
  4. "over-encoded" / `…:gold:us_east`
- **Data (y values 0-100 scale):**
  - debuggability (green `#27ae60`, 2px line, 3.5px dots): `[8, 52, 84, 88]`
  - brittleness / "encoded facts that can go stale" (red `#e74c3c`): `[2, 6, 34, 90]`
- **Workable band:** the second rung's column band shaded `rgba(39,174,96,0.07)` with green 10px label "workable range" near the top.
- **Legend (top right, small color bars + 10px `#2c3e50` text):** green "debuggability from the string"; red "encoded facts that can go stale".
- **Caption (11px `#888`, bottom center):** "Past the third rung you buy almost no clarity and a lot of coupling"

## 3. The Mutability Hazard

This is the central failure mode. An ID encoding a mutable attribute becomes a false statement the moment the entity changes — and the ID cannot be rewritten, because it is a key.

- **Region** — customer relocates; the shard hint in the key now points at the wrong place.
- **Tier / status** — an upgrade or churn event makes the encoded grade wrong for every historical join.
- **Parent handle** — re-parenting an entity orphans the embedded lineage.
- **Stale-ID trail** — every consumer that parsed the key instead of reading the column silently inherits the lie.

**Key point (red-accent callout):** **Immutability test:** can this attribute ever change for an existing row? If yes, it belongs in a column, not in the key. Only type, creation time, and randomness are safely immutable.

**Example (italic):** Example: `cust-useast-gold-00417` after the customer moves to eu-west and drops to silver — the row is correct, the key is not.

### Visualization (canvas `c3`, 720×380)

Timeline diagram: a key that starts true and becomes false, feeding three downstream consumers.

- **Title (bold 14px `#1a5276`, centered):** "Encoding a Mutable Attribute: the Stale-ID Trail"
- **t0 box (left, 210×46 at y=50, fill `rgba(39,174,96,0.10)`, 2px green `#27ae60` stroke):** 13px monospace `cust-useast-gold-00417` with green 10px caption "t0: key agrees with the row".
- **Change event (center, between the boxes):** bold 10px orange `#e67e22` "attribute change", then 10px `#555` lines "region: useast → euwest" and "tier: gold → silver". Gray arrow into the event, orange arrow out.
- **t1 box (right, 210×46, fill `rgba(231,76,60,0.10)`, 2px red `#e74c3c` stroke):** same ID text with a red strike-through line over the `useast-gold` portion; red 10px caption "t1: key unchanged — now false".
- **Downstream consumers (heading bold 11px `#1a5276`, centered at y≈138):** "downstream consumers that parse the key instead of reading the column". Three dashed-red rows (fill `rgba(231,76,60,0.06)`, 1px dashed `#e74c3c` border, width = canvas−140, starting y=150, spacing 54), each fed by a pale `#f0c9c4` connector line from the t1 box; bold 11px red name + 10px `#555` effect:
  - "shard router" — "parses region → routes to the wrong replica"
  - "billing rollup" — "groups by tier → revenue lands in the wrong bucket"
  - "residency audit" — "infers jurisdiction → reports the wrong region"
- **Caption (11px `#888`, bottom center):** "Keys cannot be corrected in place — encode only facts that never change"

## 4. Collision Budget of the Random Suffix

Suffix width is the whole uniqueness argument. Under the birthday approximation, for *n* random keys of *b* bits, collision probability is roughly *n²/2^(b+1)* while it stays small:

- **Slope 2** — risk grows with the *square* of key count, so doubling volume quadruples risk.
- **Bits buy volume cheaply** — 20 extra suffix bits allow about 1000× more keys at the same risk.
- **Short suffixes are for narrow scopes** — a 16-bit tail is fine for keys within one pipeline run, not for a global namespace.
- **UUIDv4** — 122 random bits; the design target is not approached at any realistic key count. Its weakness is opacity, not collisions.

**Key point (red-accent callout):** **Design constraint:** collision probability below 1 in 10⁹ within a single pipeline execution context. Pick the suffix width from the expected key count in that scope — not from how pretty the string looks.

### Visualization (canvas `c4`, 720×300)

Log-log line chart of birthday-bound collision probability vs key count, one curve per suffix width.

- **Title (bold 14px `#1a5276`, centered):** "Collision Probability vs Key Count (birthday bound)"
- **Plot area:** x=66, width = canvas−106, y=46, height 176. X axis spans 10⁰…10⁹ (log), labels "1, 10, 10², 10³, 10⁴, 10⁵, 10⁶, 10⁷, 10⁸, 10⁹"; Y axis spans 10⁻²⁴…10⁰ (log), labels "10⁻²⁴, 10⁻²⁰, 10⁻¹⁶, 10⁻¹², 10⁻⁸, 10⁻⁴, 1". Light `#f0f0f0` gridlines per decade tick; `#ccc` L axes. Axis titles (10px `#666`): x "number of keys generated in one scope (n)"; y (rotated) "P(at least one collision)".
- **Design target line:** dashed gray `#999` horizontal line at p = 10⁻⁹, labeled "design target 10⁻⁹" (9px `#888`).
- **Curves (2px, p = 1 − exp(−n²/2^(b+1)), sampled at 100 points per decade):**
  - 16-bit suffix — red `#e74c3c`
  - 32-bit suffix — orange `#e67e22`
  - 64-bit suffix — blue `#1a5276`
  - 122-bit (uuid4) — green `#27ae60`
- **Crossing markers:** a 3.5px dot in the curve color where each curve crosses the 10⁻⁹ target line, annotated with "n≈<value>" (n = sqrt(10⁻⁹ · 2^(b+1)); rounded below 10⁴, exponential notation above).
- **Legend:** horizontal row below the x-axis labels — small color bars + 10px labels "16-bit suffix", "32-bit suffix", "64-bit suffix", "122-bit (uuid4)".
- **Caption (11px `#888`, bottom center):** "Log-log slope of 2: p ≈ n²/2ᵇ⁺¹ — 20 extra bits buy ~1000× the keys at equal risk"

## 5. Sortability and Write Locality

A time-prefixed ID (ULID/KSUID/Snowflake style) makes lexicographic order match creation order. That is useful for humans and brutal for index pages:

- **Monotonic keys** — inserts land at the right edge of the index. Few pages touched, excellent cache behaviour, but one hot page or shard.
- **Random keys** — inserts scatter across the whole keyspace. Load spreads evenly, at the cost of page churn and split-heavy indexes.
- **Range scans** — "last hour of runs" is a contiguous scan with a time prefix, and a full scan without one.
- **Hybrid** — coarse time prefix plus random tail: sortable to the bucket, spread within it.

**Key point (red-accent callout):** **The tradeoff:** sortability and write spread are opposites. Choose by workload — read-heavy time-range queries favour monotonic; write-heavy ingest favours randomness or a hybrid.

### Visualization (canvas `c5`, 720×300)

Two stacked bar panels showing insert distribution across 12 index pages.

- **Title (bold 14px `#1a5276`, centered):** "Where Inserts Land: Time-Prefixed vs Random Keys"
- **Both panels:** 12 index-page slots (thin `#ccc` outlined boxes, 14px tall) along the bottom of each panel; bars above each slot scaled to that panel's peak (max bar height 62), fill = panel color at 0.35 alpha with solid stroke, 9px value labels above nonzero bars; rotated 9px `#666` "inserts" y-label at left; 9px `#999` footer "index pages, low key → high key" (left) and the panel note (right, `#888`).
  - Panel 1 (top at y=48, blue `#1a5276`, fill `rgba(26,82,118,0.35)`), title "time-prefixed (ULID / Snowflake style)": writes per page `[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 42]`; note "lexicographic = chronological; range scans are contiguous, tail page is hot"
  - Panel 2 (top at y=168, orange `#e67e22`, fill `rgba(230,126,34,0.35)`), title "random (uuid4)": writes per page `[4, 5, 3, 4, 4, 5, 4, 3, 5, 4, 4, 3]`; note "load spreads evenly; no time-range locality, more page splits"
- **Caption (11px `#888`, bottom center):** "Same insert volume, opposite distribution — sortability and write spread trade off"

## 6. Design Constraints and Open Questions

Hard constraints any candidate scheme must satisfy:

- Valid in filenames, URLs, JSON keys, and SQL identifiers without escaping.
- Lexicographic sort equals chronological sort, at least within one type.
- Prefix alone sufficient to route to the correct subsystem or table.
- Collision probability below 1 in 10⁹ within a single pipeline execution context.
- Only immutable facts encoded — everything else stays a column.

**Key point (red-accent callout):** **Where this lands:** a typed prefix plus a time-ordered, random-tailed body clears every constraint. Fully semantic paths read best and fail the escaping, sorting, and immutability tests at once.

### Visualization (canvas `c6`, 720×380)

Compliance grid: candidate ID schemes (rows) vs hard constraints (columns), with ✓/~/✗ cells.

- **Title (bold 14px `#1a5276`, centered):** "Candidate Schemes Against the Hard Constraints"
- **Column headers (two lines each — 10px `#1a5276` over 10px `#888`):** "URL / file" / "safe unescaped"; "sort =" / "time order"; "prefix" / "routes"; "safe when" / "distributed"; "immutable" / "facts only".
- **Rows (11px right-aligned `#2c3e50` names; cells are tinted rounded rects with 1px `#e0e0e0` border and a bold 13px centered mark — ✓ green `#27ae60` fill `rgba(39,174,96,0.10)`, ✗ red `#e74c3c` fill `rgba(231,76,60,0.10)`, ~ orange `#e67e22` fill `rgba(230,126,34,0.10)`):**
  | Scheme | URL/file safe | sort = time | prefix routes | safe distributed | immutable only |
  |---|---|---|---|---|---|
  | sequential integer | ✓ | ✓ | ✗ | ✗ | ✓ |
  | uuid4 | ✓ | ✗ | ✗ | ✓ | ✓ |
  | ULID / KSUID | ✓ | ✓ | ✗ | ✓ | ✓ |
  | typed prefix + ULID | ✓ | ✓ | ✓ | ✓ | ✓ |
  | fully semantic path | ~ | ~ | ✓ | ~ | ✗ |
- **Highlight:** the "typed prefix + ULID" row is outlined with a 2px `#1a5276` rectangle.
- **Legend (below the grid):** "✓ satisfied" (green), "~ partly / with caveats" (orange), "✗ fails" (red).
- **Caption (11px `#888`, bottom center):** "The highlighted scheme clears every constraint without asserting a mutable fact"

### Comparison table (full-width `table.compare`, below the section)

| Question | Options |
|---|---|
| Structure | `prefix:timestamp:random` vs `type-source-sequence` vs ULID/KSUID variants |
| Type prefix | Fixed-length codes (`feat_`, `run_`, `mod_`) vs free-form |
| Time encoding | Epoch-based (sortable), date-string (readable), or hybrid |
| Uniqueness guarantee | Random suffix bits, sequence counter, or content hash? |
| Human readability | Base62 vs hex vs readable words (hashids-style) |
| Hierarchy encoding | Parent ID embedded? Or purely relational? |

**Status footer (12px `#999`):** Status: stub. Needs brainstorming session.

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col text/viz layout). Structure: h1, `.subtitle` paragraph, `.philosophy` callout, then one `.card-section` per section — each with an h2 (numbered "N. Title") and a `table.layout` with `td.text-col` (45%) and `td.viz-col` (55%) holding the canvas. Section 6 appends a full-width `table.compare` after the layout table. Ends with a small gray status paragraph. No index number in the page h1/title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.philosophy` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.9rem. `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `code` ui-monospace/SFMono-Regular/Menlo, background `#f4f4f4`, padding 1px 5px, radius 3px, 0.85rem. `table.compare` full width, 1px `#e0e0e0` cell borders, padding 8px 12px, 0.88rem; th background `#f8f9fa` color `#1a5276` weight 600. `ul` 0.92rem, margin 8px 0 8px 20px. Canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic width/height attributes per chart (c1, c2, c4, c5 are 720×300; c3 and c6 are 720×380 with inline dpr setup); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; monospace font stack `ui-monospace, SFMono-Regular, Menlo, monospace` for ID text inside charts. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; tinted fills at 0.10-0.18 alpha of the same colors; gray text `#555`/`#666`/`#888`.
- **Links:** none on this page; if any card links exist in regenerated HTML, use `.html` extensions.
