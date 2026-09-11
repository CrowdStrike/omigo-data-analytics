# k-Anonymity & Re-identification

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** k-Anonymity & Re-identification

**Subtitle:** Removing names does not anonymize a dataset — a few ordinary columns combine into a fingerprint, and k-anonymity is the first formal defense

## Three Harmless Columns That Name You

**Tags:** `core idea` (blue), `quasi-identifiers` (orange), `published research` (green)

- **The release** — a hospital shares patient records with names and SSNs stripped, keeping ZIP, birth date, sex
- **The finding** — Latanya Sweeney's published research: 87% of the US population is unique on just those three fields
- **Why it works** — each column is common, but the combination narrows 300 million people down to one
- **The name** — columns that identify only in combination are called quasi-identifiers
- **The scale** — even coarser combos identify: ~53% unique on {city, birth date, sex}, ~18% on {county, birth date, sex}

*Example (italic):* A birth date picks 1 day in ~29,000, sex halves it, a ZIP holds ~10,000 people — multiplied together, most people stand alone.

**Key point:** "We removed the PII columns" is not anonymization — the remaining ordinary columns combine into a near-unique fingerprint for most people.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart of the share of the US population uniquely identified by three quasi-identifier combinations, from Sweeney's published study.

- **Title (bold 15px, `#1a5276`, top center):** "Share of US Population Unique on Three Ordinary Fields".
- **Layout:** left-aligned 12px `#444` row labels at x=20, bars start at x=250, max bar width 400 (= 100%), 26px tall, rows at y = 90, 150, 210.
- **Rows (label → bar):**
  - "{ZIP, birth date, sex}": red `#e74c3c` bar width 348 (87%), bold 13px red value label "87%" at bar end
  - "{city, birth date, sex}": orange `#d95926` bar width 212 (53%), 12px `#444` label "53%"
  - "{county, birth date, sex}": blue `#2a78d6` bar width 72 (18%), 12px `#444` label "18%"
- **Reference frame:** thin 1px `#e5e9ef` vertical gridlines at 25/50/75/100% with 11px `#6b7280` tick labels along a 2px `#999` baseline at y=245.
- **Annotation (bold 13px `#e74c3c`, above the top bar, y=65):** "no name, no SSN — still 87% unique".
- **Caption (12px `#444`, bottom right):** "percentages from published research (Sweeney, 2000) — not illustrative".

## Blurring Nine Rows Until Every Row Has Two Twins

**Tags:** `worked example` (blue), `generalization` (green), `k = 3` (orange)

- **The raw table** — 9 hospital rows (illustrative) with ZIP, age, sex, diagnosis; every row is unique on the first three
- **The rule** — a table is k-anonymous when every row matches at least k−1 others on all quasi-identifiers
- **Generalize ZIP** — 02138, 02139, 02141, 02142 all coarsen to 021**
- **Generalize age** — 34 becomes 30–39; exact ages collapse into 10-year bands
- **Hand-check** — after coarsening, the 9 rows fall into 3 groups of 3 identical (ZIP, age band, sex) rows: k=3
- **The cost** — the diagnosis column is untouched, but analysts now see 021** instead of a real ZIP

*Example (italic):* Row "02139, 34, F, flu" becomes "021**, 30–39, F, flu" — and two other rows now look exactly the same, so a linker can no longer tell which of the 3 is her.

**Key point:** k-anonymity is achieved by generalizing or suppressing quasi-identifiers until every row hides in a crowd of at least k — you buy anonymity by paying with precision.

### Visualization (canvas `c2`, 720×300)

Before/after table diagram: the raw 9-row table on the left (every row its own color-coded unique group) and the generalized table on the right (three groups of three), with an arrow between them.

- **Title (bold 15px, `#1a5276`, top center):** "Nine Unique Rows → Three Groups of Three (k = 3)".
- **Left table (x=25, width 280, header y=60):** bold 11px `#1a5276` header "ZIP | age | sex"; 9 rows, 19px tall, 11px `#2c3e50` mono text, values top to bottom: `02138 34 F`, `02139 36 F`, `02141 38 F`, `02142 55 M`, `02139 58 M`, `02138 57 M`, `02141 23 F`, `02142 26 F`, `02138 29 F`; each row's left edge carries a 4px red `#e74c3c` tick — 9 groups of size 1.
- **Arrow:** 3px `#6b7280` arrow from x=315 to x=395 at y=155, bold 12px `#6b7280` label "generalize" above it.
- **Right table (x=405, width 280, header y=60):** same row geometry, values: three rows `021** 30-39 F`, three rows `021** 50-59 M`, three rows `021** 20-29 F`; rows grouped with background fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(201,133,0,0.15)` per group of 3.
- **Group labels (bold 11px, right of the right table at x=692):** "k=3" beside each of the three groups in the group's edge color (`#2a78d6`, `#008300`, `#c98500`).
- **Annotation (bold 12px green `#008300`, centered below tables at y=280):** "every row now matches 2 others — smallest group size is k = 3".
- **Caption (11px `#444`, bottom left):** "9-row table illustrative".

## The Voter Roll That Unmasked a Governor

**Tags:** `where it's used` (blue), `linkage attack` (red)

- **The release** — Massachusetts published "anonymized" state-employee hospital records: no names, but ZIP, birth date, sex kept
- **The link** — Sweeney bought the public Cambridge voter roll, which lists name, address, ZIP, birth date, sex
- **The join** — matching the shared columns pinned the governor's own hospital record; she mailed it to his office
- **Netflix, same move** — the Netflix Prize ratings were linked to public IMDb reviews: a handful of movies plus rough dates re-identified users (Narayanan & Shmatikov, published research)
- **The lesson** — linkage attacks use OTHER datasets you don't control; auxiliary data keeps growing after you publish

*Example (italic):* The hospital data alone named no one; the voter roll alone revealed no diagnosis — joined on {ZIP, birth date, sex}, together they did both.

**Key point:** Re-identification is a join. You cannot judge a release by what it contains — only by what it can be linked against, and that set is open-ended.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram of linkage attacks: the Massachusetts health-records join on top, the Netflix–IMDb join below, each showing two source boxes merging into a re-identified result.

- **Title (bold 15px, `#1a5276`, top center):** "A Linkage Attack Is Just a Join".
- **Row 1 (y=100), label 12px `#444` at x=15:** "health data"; blue `#2a78d6` rounded box at x=115 (170×46) labeled "hospital records\nZIP · birth date · sex · dx" (11px, two lines); second source box at x=305 (150×46) labeled "voter roll\nname · ZIP · DOB · sex"; 3px `#6b7280` arrows from both into a red `#e74c3c` box at x=515 (180×46) labeled "governor's record\nname + diagnosis" with bold 12px red "✗ re-identified" beneath it.
- **Row 2 (y=205), label:** "movie data"; blue box at x=115 (170×46) "Netflix ratings\nmovies · dates · stars"; source box at x=305 (150×46) "public IMDb reviews\nname · movies · dates"; arrows into a red box at x=515 (180×46) "named subscriber\nfull rating history" with bold 12px red "✗ re-identified".
- **Join labels (bold 11px violet `#4a3aa7`):** "join on quasi-identifiers" centered above the row-1 arrows; "join on a few ratings + rough dates" above row-2 arrows.
- **Box style:** 8px radius, source fills `rgba(42,120,214,0.15)`, result fills `rgba(231,76,60,0.12)`, 11px `#2c3e50` text.
- **Annotation (bold 13px `#d95926`, centered at y=278):** "the attacker's second table is public — you never controlled it".
- **Caption (11px `#444`, bottom right):** "both cases are documented published demonstrations".

## When All k Rows Share the Secret

**Tags:** `common mistake` (red), `homogeneity` (orange)

- **The trap** — k-anonymity hides WHICH row is yours, but not the sensitive value, if all k rows agree on it
- **Homogeneity** — a k=3 group where all 3 diagnoses read "heart disease" leaks the diagnosis with certainty
- **Background knowledge** — knowing a neighbor's rough age and ZIP plus one outside fact can eliminate rows
- **The successors** — l-diversity demands varied sensitive values per group; t-closeness bounds each group's value distribution (mention level)
- **The modern answer** — differential privacy adds calibrated noise and gives a mathematical guarantee independent of auxiliary data
- **The mistake** — declaring a dataset safe because k≥5 held at release, ignoring what the groups actually contain

*Example (italic):* An attacker knows a 30-something woman from 021** is in the table; her k=3 group all say "heart disease" — k-anonymity held, her diagnosis leaked anyway.

**Common mistake:** Treating k-anonymity as a finish line. It defeats one attack (identity linkage) under one assumption (the attacker knows only quasi-identifiers) — homogeneity and background knowledge break it, which is why l-diversity, t-closeness, and ultimately differential privacy exist.

### Visualization (canvas `c4`, 720×300)

Diagram of three k=3 groups shown as stacked row blocks with their sensitive column visible: two diverse groups (safe) and one homogeneous group (leaks), sensitive values color-coded.

- **Title (bold 15px, `#1a5276`, top center):** "k Held in All Three Groups — One Still Leaked".
- **Groups (three columns at x = 60, 290, 520, each 180px wide, top y=70):** each column headed by a bold 12px `#1a5276` label — "021** · 30–39 · F", "021** · 50–59 · M", "021** · 20–29 · F" — above 3 stacked rows (170×34, 6px gap, 8px radius) showing only the sensitive value.
- **Column 1 rows:** "heart disease", "heart disease", "heart disease" — all filled `rgba(231,76,60,0.12)` with 2px `#e74c3c` borders.
- **Column 2 rows:** "cancer", "flu", "heart disease" — fills `rgba(0,131,0,0.12)`, 1px `#008300` borders.
- **Column 3 rows:** "asthma", "cancer", "asthma" — fills `rgba(0,131,0,0.12)`, 1px `#008300` borders.
- **Verdict labels (bold 12px, centered under each column at y=225):** red `#e74c3c` "✗ homogeneous — value leaks" under column 1; green `#008300` "✓ diverse" under columns 2 and 3.
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=260):** "l-diversity fixes this group; differential privacy stops asking the question".
- **Caption (11px `#444`, bottom right):** "groups from the worked example — diagnoses illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all chart values are the hardcoded arrays/rows above (no randomness); the 87% / 53% / 18% uniqueness figures are Sweeney's published results and must be labeled as published research; the 9-row hospital table, its k=3 grouping, and the group diagnoses are invented and labeled illustrative; text numbers must match chart numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
