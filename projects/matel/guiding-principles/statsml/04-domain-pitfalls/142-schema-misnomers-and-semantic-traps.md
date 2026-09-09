# Schema Misnomers & Semantic Traps

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 142. Schema Misnomers & Semantic Traps

**Subtitle:** Field names that say one thing and mean another. The schema becomes a source of silent wrong interpretation — not because the data is bad, but because the NAME misleads everyone who reads it.

## Callout (philosophy box)

**The core problem:** A field named `revenue` might mean gross, net, projected, or recognized — depending on who created it and when. Everyone reads the name, assumes the obvious meaning, and builds analysis on a wrong interpretation. The data is correct. The interpretation is wrong. The name is the bug.

## Overloaded Names — Same Word, Different Meaning Per Context

**`amount` Means 5 Different Things in 5 Tables**

- **Tables A and B:** `amount` = USD total ($42.50) in one table, cents integer (4250) in the other.
- **Tables C and D:** `amount` = quantity of items (3 units), or percentage as a decimal (0.15 = 15%).
- **Table E:** `amount` = delta from the previous value, and it can be negative.
- **The damage:** JOIN tables A and B on a shared key, sum `amount` → dollars added to cents.
- **Silent failure:** Off by 100×, no error thrown; the dashboard shows "revenue up 9,800%."
- **Why nobody asks:** The number is directionally plausible ("we grew!"), so weeks go by.
- **How it happened:** Each team named its field reasonably in isolation, no global naming standard.
- **Why it persists:** At 200 tables, renaming is a breaking change nobody wants to own.

### Visualization (canvas `c1`, 720×300)

Five table-schema boxes each containing the same field name with a different meaning. Light gray `#f9f9f9` canvas background.

- **Title (bold 14px sans-serif `#1a5276`, centered):** "\"amount\" field across 5 tables".
- **Boxes (equal columns across the width, 160px tall from y=44, fill `rgba(26,82,118,0.08)`, stroke `#1a5276` 1.5px):** each shows the table name (bold blue), the field name "amount" (bold red `#e74c3c`), the meaning (`#555`), the value (bold 16px `#333`), and the unit (`#999`):
  - orders — "= USD ($42.50)" — 42.5 — $
  - payments — "= cents (4250)" — 4250 — ¢
  - inventory — "= quantity (3)" — 3 — units
  - tax_rates — "= decimal (0.15)" — 0.15 — %
  - adjustments — "= delta (-5.00)" — -5 — Δ$
- **Warning (bold red, bottom center):** "SUM(amount) across tables = 42.50 + 4250 + 3 + 0.15 + (-5) = 4290.65 ← meaningless".

## Temporal Ambiguity — When Did This Number Become True?

**`created_at` — When the Event Happened or When We Recorded It?**

- **`created_at`:** When the user acted, when the event hit the server, or when the row was inserted?
- **The drift:** Those three moments differ by seconds, hours, or days under batch ingestion.
- **`updated_at`:** Last row update — a data correction, a status change, or a reprocessing artifact?
- **Empty diffs:** Filter `updated_at > yesterday` and get rows where only the ETL timestamp changed.
- **`date`:** The most dangerous name. Date of the transaction, the settlement, the reporting period?
- **Finance case:** transaction_date vs settlement_date differ by T+2, shifting the revenue curve 2 days.
- **The analysis mistake:** "Revenue by day" on `created_at` (ETL insert) not `transaction_date` (money moved).
- **The artifact:** Monday's batch inserts weekend events → real pattern uniform, measured 3× Monday spike.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: revenue by day-of-week under ETL timestamp vs transaction timestamp. Light gray `#f9f9f9` background; black axes.

- **Title (bold 14px `#1a5276`):** "\"Revenue by day\" — using ETL timestamp vs transaction timestamp".
- **Days:** Mon–Sun. **ETL series (red, fill `rgba(231,76,60,0.4)`, stroke `#e74c3c`):** `[300, 100, 100, 100, 100, 20, 20]`. **Real series (green, fill `rgba(39,174,96,0.4)`, stroke `#27ae60`):** `[100, 100, 100, 100, 100, 100, 100]`. Scale max 320; margins left 60 / top 50 / bottom 50; paired half-width bars per day.
- **Legend (bottom-left swatches):** "By ETL created_at (misleading Monday spike)" (red) and "By transaction_date (real: uniform)" (green).

## Status Fields With Undocumented Historical Semantics

**`status = 3` — What Does That Mean?**

- **The enum graveyard:** Status holds 0-14, but only 0, 1 and 2 are documented anywhere.
- **How it grew:** Values 3-14 accrued over 5 years, added by engineers since departed.
- **No definition:** No enum definition exists, and code comments only say "// legacy, don't touch."
- **Semantic drift:** `status=5` meant "churned" in 2019, then was reused for "paused" in 2021.
- **Age dependence:** "Paused" means no login for 30 days — so a row's meaning depends on its age.
- **Model damage:** Filtering `status=5` mixes real churn with users who will return; churn accuracy garbage.
- **The "active" trap:** Is active 1? 1 and 2? 1, 2 and 7 (trial)? Each dashboard picks its own set.
- **Contradiction:** Leadership sees three active-user counts, and the name encodes no semantics to settle it.
- **Booleans that aren't:** `is_deleted = true` rows still return, since only some pipelines honor soft-delete.
- **Creation flags:** `is_active` is set true at account creation and never set false — not current state.

### Visualization (canvas `c3`, 720×300)

Matrix of status values vs three dashboards with check/cross marks and diverging "active user" totals. Light gray `#f9f9f9` background.

- **Title (bold 14px `#1a5276`):** "Status field: what counts as \"active\"?"
- **Columns:** Dashboard A (`#3498db`), Dashboard B (`#e67e22`), Dashboard C (`#8e44ad`); row header column "Status".
- **Rows (status label at left; per dashboard a green `#27ae60` ✓ or red `#e74c3c` ✗):**
  - 0 = new — ✓ ✓ ✓
  - 1 = confirmed — ✓ ✓ ✓
  - 2 = subscribed — ✓ ✓ ✓
  - 3 = trial — ✗ ✓ ✓
  - 4 = paused — ✗ ✗ ✓
  - 5 = churned/paused? — ✗ ✗ ✗
  - 6 = deleted — ✗ ✗ ✗
- **Totals row ("Active users"):** 12,400 (A, blue), 15,800 (B, orange), 18,200 (C, purple).
- **Caption (italic red, bottom center):** "Same data, same field, 3 different \"active user\" counts. Name doesn't encode definition."

## Unit Ambiguity — The Number Is Right, the Scale Is Wrong

**Dollars vs Cents vs Basis Points vs Percentage**

- **The Mars Climate Orbiter problem:** One system sends meters, another expects feet, spacecraft crashes.
- **The data version:** A pipeline writes milliseconds, downstream reads seconds — latency looks 1000× worse.
- **False alarm:** The alert fires, on-call wakes at 3am, finds a unit mismatch and no real incident.
- **Currency fields:** Is `price` USD, EUR, or local — and if local, pre- or post-conversion?
- **Meaningless total:** A global dashboard sums JPY (≈150 per USD) with USD and GBP, then graphs it.
- **Rate fields:** `conversion_rate = 0.03` is 3% if it is a probability on a 0 to 1 scale.
- **The other reading:** 0.03% if the value is already a percentage — projection off by 100×.
- **Duration fields:** `duration = 3600` — seconds (1 hour), milliseconds (3.6 sec), or minutes (60 hours)?
- **The panic:** Dashboard reads "average session: 1 hour," assuming `duration` is in seconds.
- **The real number:** 3.6 seconds, while product chases "why an hour on checkout?"

### Visualization (canvas `c4`, 720×300)

Two field scenarios, each with horizontal bars sized by unit interpretation. Light gray `#f9f9f9` background.

- **Title (bold 14px `#1a5276`):** "Same number, different unit interpretation".
- **Scenario 1 — `duration = 3600` (bold blue field label):** bars starting at x≈220, max width 380, 20px tall, interpretation label after each bar:
  - "seconds → 1 hour" — bar 25% of max — `#27ae60`
  - "milliseconds → 3.6 sec" — bar 1.5% (min 4px) — `#e74c3c`
  - "minutes → 60 hours" — bar 100% — `#e67e22`
- **Scenario 2 — `rate = 0.03`:**
  - "probability → 3%" — bar 50% — `#27ae60`
  - "already percentage → 0.03%" — bar 0.5% (min 4px) — `#e74c3c`
- **Caption (italic red, bottom center):** "Field name doesn't encode units. Analyst guesses. Revenue off by 100×."

## Naming Creates False Equivalence Across Systems

**`user_id` in System A ≠ `user_id` in System B**

- **Identity fragmentation:** Marketing's `user_id` = email hash, Product's = account UUID, Analytics' = cookie.
- **Broken counts:** JOIN them → one person becomes three, three become one; user count inflated and deflated.
- **The attribution disaster:** "User who saw ad also purchased," but marketing's cookie ≠ product's account.
- **Cross-device split:** The cookie saw the ad on a phone; the purchase happened on a laptop, other cookie.
- **Wrong verdict:** The attribution model says "ad didn't convert." Reality: it did; the stitching failed.
- **Root cause:** Both systems call the identifier `user_id`, so someone assumed they are the same thing.
- **Granularity mismatch:** System A's `transaction_id` is per order, System B's per line item.
- **Fanout on join:** A 5-item order appears 5×, so revenue looks 5× higher than it is.
- **No clean fix:** Aggregate system B first — but SUM or MAX depends on what `amount` means in each system.

### Visualization (canvas `c5`, 720×280)

Three system circles all labeled `user_id` with broken-join dashed lines between them. Light gray `#f9f9f9` background.

- **Title (bold 14px `#1a5276`):** "\"user_id\" means different things in different systems".
- **Circles (radius 30, centers spaced 200px around page center at y=110, white bold text inside):**
  - Marketing — cookie_hash — example "a3f2..." — `#3498db`
  - Product — account_uuid — example "usr_7x..." — `#27ae60`
  - Analytics — device_id — example "dev_m3..." — `#e67e22`
- Beneath each circle: bold red field label "user_id" and gray `#999` example value ("= a3f2...", etc.).
- **Broken joins:** dashed (4/4) red lines width 2 connecting adjacent circles, with a bold red "✗" over each connection midpoint.
- **Result text (centered, `#555`):** "JOIN on \"user_id\" across systems → broken identity stitching" and "Same person = 3 rows. Three people = sometimes 1 row (collision)."
- **Caption (italic red):** "Same field name ≠ same entity. The name is a lie."

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle` + one `.philosophy` callout, then one `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` div + `<ul>` of labeled bullets, right `<td>` (60%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links.
- **Canvas:** charts are 720×300 except `c5` at 720×280; every chart first fills the canvas with a `#f9f9f9` background; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart text uses 9-16px sans-serif (plus 12px monospace where showing code/filters). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#3498db`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#555`/`#333`/`#999`, box fill `rgba(26,82,118,0.08)`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
