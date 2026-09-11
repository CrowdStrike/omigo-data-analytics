# Stability & Blocking Pairs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Stability & Blocking Pairs

**Subtitle:** A matching survives only if no two participants would rather run off together — one willing pair is enough to unravel the whole assignment

**Shared running example (whole matching-markets series):** 4 candidates × 4 companies, one opening each.
Candidates — Alice: A>B>C>D; Bob: A>D>C>B; Carol: B>A>C>D; Dan: A>B>C>D.
Companies — A: Dan>Alice>Bob>Carol; B: Carol>Alice>Dan>Bob; C: Alice>Bob>Carol>Dan; D: Bob>Alice>Carol>Dan.

## The Handshake That Kills a Matching

**Tags:** `core idea` (blue), `matching markets` (green)

- **The market** — four candidates (Alice, Bob, Carol, Dan), four companies (A, B, C, D), one opening each
- **A trial matching** — pair them alphabetically: Alice–A, Bob–B, Carol–C, Dan–D
- **Dan's view** — Dan ranks A>B>C>D, so Company D is his very last choice
- **Company A's view** — A ranks Dan>Alice>Bob>Carol, so it prefers Dan to Alice, its current hire
- **The defection** — Dan and Company A each prefer the other; one phone call and both walk away

*Example (italic):* The moment Dan and Company A compare notes, the alphabetical assignment is dead — no rule book can hold a pair that both want out.

**Key point:** A pair like (Dan, A) — where each side prefers the other to what it currently has — is a blocking pair, and any matching that contains one is called unstable.

### Visualization (canvas `c1`, 720×300)

Two columns of labeled boxes (candidates left, companies right) with the bad alphabetical matching drawn as gray horizontal lines and the blocking pair (Dan, A) as a bold red dashed diagonal with arrowheads at both ends.

- **Title (bold 15px, `#1a5276`, top center):** "One Blocking Pair Unravels the Matching".
- **Column headers (bold 13px, y=52):** "candidates" in blue `#2a78d6` centered at x=165; "companies" in green `#008300` centered at x=550.
- **Candidate boxes:** x=90, width 150, height 40, tops at y = 62/112/162/212 (centers 82/132/182/232), fill `#fbfcfd`, 2px blue `#2a78d6` border; name bold 13px `#2c3e50` at top+16 ("Alice", "Bob", "Carol", "Dan"); preference sub-line 11px mute `#6b7280` at top+32: "A>B>C>D", "A>D>C>B", "B>A>C>D", "A>B>C>D".
- **Company boxes:** x=470, width 160, same rows, fill `#fbfcfd`, 2px green `#008300` border; name bold 13px ("Company A".."Company D") at top+16; preference sub-line 11px mute at top+32: "Dan>Alice>Bob>Carol", "Carol>Alice>Dan>Bob", "Alice>Bob>Carol>Dan", "Bob>Alice>Carol>Dan".
- **Highlight:** the Dan box and the Company A box get a 3px red `#e74c3c` border instead of blue/green.
- **Matching lines (gray, the bad matching):** 2px `#b8bfc9` solid from (240, center) to (470, center) for the four rows — Alice–A, Bob–B, Carol–C, Dan–D.
- **Blocking pair:** 3px red `#e74c3c` dashed line (dash [8,5]) from (240, 232) (Dan) to (470, 82) (Company A), filled red arrowheads at both ends.
- **Annotation (bold 13px red, centered at (355, 140)):** "both prefer each other".
- **Bottom caption (12px `#6b7280`, centered at y=286):** "gray: the alphabetical matching — red dashed: the blocking pair (Dan, A)".

## Checking All Sixteen Pairs by Hand

**Tags:** `worked example` (blue), `blocking check` (orange)

- **The test** — for every candidate–company pair not matched together, ask two questions
- **Question 1** — does the candidate prefer this company to the one they currently hold?
- **Question 2** — does the company prefer this candidate to the one it currently holds?
- **The verdict** — yes + yes means the pair blocks; a single "no" means the pair is safe

Small check table (compact `.check-table` in the text column, verified against the preference lists):

| Pair | Candidate prefers? | Company prefers? | Verdict |
|------|--------------------|-------------------|---------|
| (Bob, A) | yes — A is Bob's #1 | no — A keeps Alice | OK |
| (Carol, A) | yes — A beats C for Carol | no — A keeps Alice | OK |
| (Dan, C) | yes — C beats D for Dan | no — C keeps Carol | OK |
| (Dan, A) | yes — A is Dan's #1 | yes — Dan is A's #1 | BLOCKING |

*Example (italic):* The full sweep finds five blocking pairs in the alphabetical matching: (Bob, C), (Bob, D), (Carol, B), (Dan, A), (Dan, B).

**Key point:** One blocking pair already makes a matching unstable — here the alphabetical assignment has five different ways to fall apart.

### Visualization (canvas `c2`, 720×300)

A 4×4 grid/heatmap of every candidate–company pair, cells marked matched / OK / blocked, computed from the preference lists against the alphabetical matching.

- **Title (bold 15px, `#1a5276`, top center):** "All 16 Pairs Checked Against the Alphabetical Matching".
- **Column headers (bold 13px `#2c3e50`, y=66):** "A", "B", "C", "D" centered above each column.
- **Row labels (13px `#2c3e50`, right-aligned at x=132):** "Alice", "Bob", "Carol", "Dan" at each row's vertical center.
- **Grid:** x0=148, cell width 114, cell height 44, row tops at y = 76/120/164/208 (grid spans 148–604 horizontally).
- **Cell states (rows Alice/Bob/Carol/Dan × columns A/B/C/D):**
  - Alice: matched, ok, ok, ok
  - Bob: ok, matched, blocked, blocked
  - Carol: ok, blocked, matched, ok
  - Dan: blocked (highlight), blocked, ok, matched
- **Cell styles:** matched — fill `#eef1f4`, 1px `#c8cdd4` border, "matched" 12px mute `#6b7280`; OK — fill `rgba(25,158,112,0.12)`, 1px aqua `#199e70` border, "OK" bold 12px aqua; blocked — fill `rgba(231,76,60,0.15)`, 1px red `#e74c3c` border, "blocked" bold 12px red; the (Dan, A) cell — fill `rgba(231,76,60,0.3)`, 3px red border, "BLOCKED" bold 13px red.
- **Bottom annotation (bold 12px red `#e74c3c`, centered at y=278):** "five blocking pairs — any one of them makes this matching unstable".
- **Caption (12px `#6b7280`, centered at y=294):** "(Dan, A) is the pair traced in the first section".

## Why Unstable Assignments Really Unravel

**Tags:** `where it's used` (blue), `the 1962 result` (green)

- **Side deals** — a blocking pair simply ignores the official assignment and contracts on its own
- **Exploding offers** — firms race to lock candidates early before the system can undo them
- **Trust collapses** — after a few defections nobody believes the assignment, and the market unravels
- **Clearinghouses** — stable-output matching systems have run for decades; unstable ones were abandoned
- **The 1962 result** — a stable matching always exists in two-sided markets, which is not obvious at all
- **Deferred acceptance** — the algorithm that proves it, by directly constructing a stable matching

*Example (italic):* The medical residency match has survived for decades because its output leaves no doctor–hospital pair that would rather defect together.

**Key point:** Stability is a survival property — the 1962 theorem guarantees a stable matching exists in every two-sided market, and deferred acceptance is the algorithm that finds one.

### Visualization (canvas `c3`, 720×300)

Two horizontal timeline lanes of four boxes each: an unstable system decaying step by step (top, colors escalating to red) vs a stable system holding (bottom, green).

- **Title (bold 15px, `#1a5276`, top center):** "Unstable Systems Decay, Stable Systems Hold (illustrative)".
- **Top lane header (bold 13px red `#e74c3c`, left-aligned at (60, 58)):** "unstable matching".
- **Top lane boxes:** x = 60/225/390/555, width 150, height 46, top y=66, fill `#fbfcfd`; two centered 12px `#2c3e50` text lines at top+19 and top+35; borders: box 1 — 2px mute `#6b7280`, boxes 2 and 3 — 2px orange `#d95926`, box 4 — 2px red `#e74c3c` with fill `rgba(231,76,60,0.08)`.
  - "assignment" / "released"
  - "Dan & Company A" / "strike a side deal"
  - "others react," / "more deals follow"
  - "system" / "abandoned"
- **Bottom lane header (bold 13px green `#008300`, left-aligned at (60, 168)):** "stable matching".
- **Bottom lane boxes:** same x positions/size, top y=176, all 2px green `#008300` borders, box 4 fill `rgba(0,131,0,0.08)`:
  - "assignment" / "released"
  - "no pair can" / "jointly improve"
  - "everyone" / "stays put"
  - "trusted" / "for decades"
- **Arrows:** 1.5px mute `#6b7280` between consecutive boxes in each lane, from (box right + 3, mid) to (next left − 10, mid), filled arrowhead at the right end; lane mids y=89 (top) and y=199 (bottom).
- **Caption (bold 12px violet `#4a3aa7`, centered at y=262):** "stability is the difference between a clearinghouse that lasts and one that unravels".
- **Sub-caption (12px `#6b7280`, centered at y=284):** "a single blocking pair is the first domino in the top lane".

## Stable Does Not Mean Happy

**Tags:** `common mistake` (red)

- **A stable matching here** — Alice–C, Bob–D, Carol–B, Dan–A contains no blocking pair at all
- **Alice's lot** — she gets Company C, only her third choice, yet the matching is stable
- **One-sided envy** — Alice prefers A and B, but A keeps Dan and B keeps Carol — both say no
- **Blocking needs two** — a pair blocks only when BOTH sides prefer each other; a wish alone can't
- **The bar** — stability promises no joint defection, not high satisfaction

*Example (italic):* Alice can want Company A all she likes — A ranks Dan first and Dan is already there, so nothing moves.

**Common mistake:** Reading "stable" as "everyone got a good match." Stability only rules out pairs that can jointly improve; individual envy where the other side says no does not block anything.

### Visualization (canvas `c4`, 720×300)

Alice on the left; Companies A, B, C stacked on the right. A solid green line marks her stable match to C; gray dashed one-sided wishes toward A and B end in red crosses because those companies refuse.

- **Title (bold 15px, `#1a5276`, top center):** "Stable, but Alice Still Gets Her Third Choice".
- **Alice box:** x=70, y=120, width 150, height 50, fill `#fbfcfd`, 2px blue `#2a78d6` border; "Alice" bold 13px `#2c3e50` at (145, 140); sub-line "wants A>B>C>D" 11px mute at (145, 158).
- **Company boxes:** x=470, width 180, height 44, tops at y = 60/128/196 (centers 82/150/218), fill `#fbfcfd`; A and B — 2px green `#008300` border; C — 2px blue `#2a78d6` border. Bold 13px name at top+18, 11px mute sub-line at top+34:
  - "Company A" / "holds Dan — its #1 pick"
  - "Company B" / "holds Carol — its #1 pick"
  - "Company C" / "matched with Alice"
- **Wish arrows (one-sided, refused):** 2px `#9aa2ad` dashed (dash [6,5]) from (220, 132) to (452, 84) and from (220, 142) to (452, 150); a bold 15px red `#e74c3c` "✕" at (461, 88) and (461, 154).
- **Wish labels (bold 12px red):** "A says no — it prefers Dan" centered at (340, 96); "B says no — it prefers Carol" centered at (340, 168).
- **Stable match line:** 2.5px green `#008300` solid from (220, 156) to (466, 218); label "matched — her #3 choice" bold 12px green centered at (330, 220).
- **Caption (bold 13px magenta `#d55181`, centered at y=268):** "envy without reciprocation is not a blocking pair".
- **Sub-caption (12px `#6b7280`, centered at y=288):** "the full stable matching: Alice–C, Bob–D, Carol–B, Dan–A".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section two additionally carries a compact `.check-table` (0.82rem, 1px `#dfe4ea` cell borders, `#f3f6f9` header row, 4px 8px padding) between the bullets and the example line; its BLOCKING row text is bold red `#e74c3c`.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` marks genuine instability (blocking pair, blocked cells, decay lane, refusals) only.
- **Data integrity:** all pair verdicts are derived by hand from the fixed preference lists above (no random data). Bad matching checked: blocking pairs are exactly (Bob, C), (Bob, D), (Carol, B), (Dan, A), (Dan, B). Stable matching Alice–C, Bob–D, Carol–B, Dan–A gives every company its #1 candidate, hence no blocking pair. Worked-example numbers in text match the c2 heatmap cells exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
