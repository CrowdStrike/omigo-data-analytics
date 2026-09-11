# Residents & School Choice

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Residents &amp; School Choice

**Subtitle:** The same deferred-acceptance algorithm assigns doctors to hospitals and children to schools — at national scale, for decades

## Offers Two Years Before Graduation

**Tags:** `real history` (blue), `unraveling` (orange)

- **The prize** — in the 1940s US hospitals competed fiercely for a small pool of medical students
- **The creep** — each hospital offered a bit earlier than rivals; dates slid back year after year
- **Two years early** — by 1945 offers reached students two full years before graduation
- **Exploding deadlines** — accept within hours or lose the slot, before grades even existed
- **The 1952 fix** — one clearinghouse, the NRMP: every offer resolved on a single match day
- **The punchline** — decades later, economists proved the NRMP algorithm was deferred acceptance

*Example (italic):* A student in 1945 could hold an offer that expired in twelve hours — for a job starting two years later.

**Key point:** Markets without stable matching unravel — offers creep earlier and deadlines explode — and the fix that worked, the NRMP, turned out to be deferred acceptance independently reinvented.

### Visualization (canvas `c1`, 720×300)

A timeline of offer dates creeping earlier from 1900 to 1945, then a dashed green reset line at the 1952 centralized match.

- **Title (bold 15px, `#1a5276`, top center):** "Offer Dates Unravel, Then One Match Day Resets Them (illustrative years)".
- **Axes:** 1px `#999`, origin (70, 230), x to (690, 230), y top at 70; y-axis caption "months before graduation" 11px mute at (70, 58), left-aligned.
- **X scale:** years 1900–1955 mapped to x = 70 + (year − 1900) / 55 × 620; year labels `1900 1910 1920 1930 1940 1950` 11px mute at y = 248.
- **Y scale:** months 0–24 mapped to y = 230 − m / 24 × 160 (0 → 230, 24 → 70).
- **Creep polyline (orange `#d95926`, 2.5px, 3.5px filled dots):** points (year, months) = `[1900,0], [1915,3], [1930,6], [1940,12], [1943,18], [1945,24]`.
- **Creep annotation (bold 12px orange, right-aligned at (566, 76)):** "two years early".
- **Reset line:** dashed (6,4) green `#008300` 2px vertical at x(1952) from y = 70 to 230; green 4px dot at (x(1952), y(0.5)).
- **Reset labels (green, right-aligned at x = 688):** bold 12px "1952: NRMP" at y = 62; 11px "one match day" at y = 78.
- **Caption (12px `#6b7280`, centered at y = 286):** "each year of competition pulled offers earlier — until 1952, when everyone matched at once".

## Running the Match with Capacity

**Tags:** `worked example` (blue), `deferred acceptance` (green)

- **The setup** — four students (Alice, Bob, Carol, Dan); Hospital A and Hospital B, two slots each
- **Student lists** — Alice, Bob, and Carol all rank Hospital A first; Dan ranks Hospital B first
- **Hospital lists** — A ranks Carol > Alice > Bob > Dan; B ranks Bob > Dan > Alice > Carol
- **Round 1** — Alice, Bob, Carol propose to A; A holds its best two (Carol, Alice), rejects Bob
- **Round 1, cont.** — Dan proposes to B; B holds him with one slot still open
- **Round 2** — Bob proposes to his #2, Hospital B; a slot is free, B holds him — no one moves again

*Example (italic):* Final match: Hospital A takes Alice and Carol, Hospital B takes Bob and Dan — Bob is the only student not at his first choice.

**Key point:** Capacity changes nothing fundamental: each hospital simply holds its best q proposers so far, and deferred acceptance still ends in a stable match.

### Visualization (canvas `c2`, 720×300)

A two-panel round-by-round proposal bracket: Round 1 on the left (three proposals to A, one rejection), Round 2 on the right (Bob lands at B; final assignment shown).

- **Title (bold 15px, `#1a5276`, top center):** "Deferred Acceptance with Capacity 2 — the Rounds".
- **Divider:** 1px `#e5e9ef` vertical line at x = 360 from y = 44 to 266.
- **Panel headers (bold 13px `#1a5276`, centered):** "ROUND 1" at (188, 48); "ROUND 2 — FINAL" at (536, 48).
- **Student boxes (both panels):** width 76, height 26, fill `#fbfcfd`, 1.5px `#1a5276` border, 12px `#2c3e50` centered names; left panel x = 36, right panel x = 384; y = 62 (Alice), 104 (Bob), 146 (Carol), 188 (Dan).
- **Hospital boxes (both panels):** width 104, height 58, fill `#fbfcfd`, 2px blue `#2a78d6` border; left panel x = 236, right panel x = 584; Hospital A at y = 70, Hospital B at y = 178; bold 12px `#1a5276` name at box top + 22, "capacity 2" 11px mute at top + 40.
- **Round 1 arrows (from student box right edge x = 112 to hospital box left edge x = 232, filled arrowheads):**
  - Alice (y 75) → A (232, 84): blue `#2a78d6` 2px solid; label bold 11px blue "held" at (170, 68).
  - Bob (y 117) → A (232, 99): red `#e74c3c` 2px dashed (5,4); label bold 11px red "✕ rejected" at (170, 116).
  - Carol (y 159) → A (232, 114): blue 2px solid; label bold 11px blue "held" at (170, 146).
  - Dan (y 201) → B (232, 207): blue 2px solid; label bold 11px blue "held" at (170, 196).
- **Round 2 arrows (from x = 460 to x = 580):** settled Round-1 holds redrawn faint — 1.5px `#c9d2dd`, Alice (75) → A (580, 84), Carol (159) → A (580, 114), Dan (201) → B (580, 207); the new proposal Bob (117) → B (580, 192) in green `#008300` 2.5px solid with arrowhead; label bold 11px green "held — slot open" at (516, 166).
- **Final labels (bold 12px green, centered at x = 636):** "final: Alice, Carol" at y = 146 under Hospital A; "final: Bob, Dan" at y = 254 under Hospital B.
- **Caption (12px `#6b7280`, centered at y = 286):** "two rounds, one rejection — Bob lands his second choice; everyone else gets their first".

## The Mechanism That Punished Honesty

**Tags:** `school choice` (orange), `strategy trap` (red)

- **Immediate acceptance** — the Boston mechanism: schools take round-1 top applicants permanently
- **The gamble** — rank a popular school first and lose, and your #2 has already given seats away
- **The strategy** — savvy parents ranked a safe school first; honest parents were left far down
- **The switch** — NYC (2003) and Boston (2005) replaced it with deferred acceptance
- **Safe honesty** — under DA every hold stays tentative, so listing true favorites cannot hurt

*Example (italic):* Erin's true list is School A > B > C; the honest list lands her at C under the Boston mechanism, but at B under deferred acceptance.

**Key point:** Under student-proposing deferred acceptance, reporting your true ranking is a dominant strategy — no gaming, no gamble, no regret.

### Visualization (canvas `c3`, 720×300)

Two side-by-side step diagrams: the same student with the same honest list under immediate acceptance (left, ends at her #3) and deferred acceptance (right, ends at her #2).

- **Title (bold 15px, `#1a5276`, top center):** "One Honest List, Two Mechanisms (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x = 360 from y = 44 to 262.
- **Panel headers (bold 13px, centered):** red `#e74c3c` "IMMEDIATE ACCEPTANCE (BOSTON)" at (182, 50); green `#008300` "DEFERRED ACCEPTANCE" at (540, 50).
- **Step boxes:** width 276, height 40, fill `#fbfcfd`; left panel x = 42, right panel x = 402; rows at y = 64, 128, 192; line 1 is 12px `#2c3e50` at top + 17, line 2 is bold 11px at top + 33.
  - Left row 1 (1.5px `#6b7280` border): "Round 1 — Erin applies to School A" / red "rejected — A is oversubscribed".
  - Left row 2 (1.5px `#6b7280` border): "Round 2 — she tries School B (her #2)" / red "FULL — B's seats gone in round 1".
  - Left row 3 (2px red border, single centered line): bold 12px red "Placed at School C — her #3" at top + 24.
  - Right row 1 (1.5px `#6b7280` border): "Round 1 — Erin applies to School A" / red "rejected — A is oversubscribed".
  - Right row 2 (1.5px `#6b7280` border): "Round 2 — she applies to School B" / green "held — B's seats were only tentative".
  - Right row 3 (2px green border, single centered line): bold 12px green "Final: School B — her #2" at top + 24.
- **Down arrows:** 1.5px `#6b7280` vertical segments with filled downward arrowheads between rows, at x = 180 (left) and x = 540 (right), from y = 106 to 124 and from y = 170 to 188.
- **Caption (bold 12px violet `#4a3aa7`, centered at y = 284):** "same student, same true list (A > B > C) — the mechanism alone moves her from C to B".

## The Mechanism Decides What the Data Means

**Tags:** `why it matters` (blue), `data integrity` (green)

- **Rankings as data** — districts read submitted rank lists to measure demand and plan seats
- **Under Boston** — a submitted list is a strategy; families hide demand for popular schools
- **Under DA** — honest ranking is safe, so submitted lists can be read as true preferences
- **The analyst's trap** — demand estimated from gamed lists measures gaming, not what parents want
- **Couples break it** — two doctors, one city: stability can fail; real matches add heuristics

*Example (italic):* The same families submit different rank lists under the two mechanisms — only one version is usable as demand data.

**Key point:** The mechanism decides what the data means: a strategy-proof match turns rank lists into trustworthy demand data; a manipulable one turns them into a record of gaming.

### Visualization (canvas `c4`, 720×300)

Three bars comparing the share of families ranking the oversubscribed school first: true demand, submitted lists under the Boston mechanism, submitted lists under deferred acceptance.

- **Title (bold 15px, `#1a5276`, top center):** "Who Ranks the Oversubscribed School First (illustrative)".
- **Axes:** 1px `#999`, origin (80, 230), x to (680, 230), y top at 64; y-axis caption "% of families" 11px mute at (80, 56), left-aligned.
- **Bars (width 120, baseline y = 230, scale v / 70 × 160 px):** values `[58, 31, 58]` at x = 120, 330, 540:
  - "true demand" 58% — fill `rgba(0,131,0,0.4)`, 1.5px green `#008300` stroke, bold 13px green "58%" above the top.
  - "submitted — Boston" 31% — fill `rgba(217,89,38,0.4)`, 1.5px orange `#d95926` stroke, bold 13px orange "31%" above the top.
  - "submitted — DA" 58% — fill `rgba(42,120,214,0.5)`, 1.5px blue `#2a78d6` stroke, bold 13px blue "58%" above the top.
- **Bar labels (12px `#444`, centered at y = 248):** "true demand", "submitted — Boston", "submitted — DA".
- **Gap bracket:** dashed (5,4) red `#e74c3c` 1.5px vertical line at x = 390 from the true-demand level (y of 58%) down to the Boston bar top, with 8px horizontal ticks at both ends; label bold 12px red centered at (390, y of 58% − 10): "27-point gap = strategy, not demand".
- **Caption (12px `#6b7280`, centered at y = 284):** "under the manipulable mechanism submitted lists hide demand; under DA they reveal it".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize; a shared `arrowHead(ctx, x, y, dir, color)` helper draws filled triangles for `right` and `down`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for genuine failures: Bob's Round-1 rejection in c2, the Boston-mechanism outcomes in c3, the gap bracket in c4. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** hardcoded arrays only — c1 creep points `[1900,0], [1915,3], [1930,6], [1940,12], [1943,18], [1945,24]` (illustrative years); c2 match: Alice→A, Carol→A, Dan→B in Round 1, Bob rejected by A then held by B in Round 2, final A = {Alice, Carol}, B = {Bob, Dan} — must match the text bullets exactly; c4 shares `[58, 31, 58]` percent (illustrative), gap 27 points. Documented facts kept factual: NRMP founded 1952, NYC switched 2003, Boston switched 2005.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
