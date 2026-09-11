# Honesty & Who Proposes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Honesty & Who Proposes

**Subtitle:** The proposing side gets its best stable outcome — and only the proposing side can safely tell the truth

**Running data (illustrative), used identically in every section — a 3×3 market:**

| Side | Rankings |
|------|----------|
| Alice | X > Y > Z |
| Bob   | Y > Z > X |
| Carol | Z > X > Y |
| Company X | Bob > Carol > Alice |
| Company Y | Carol > Alice > Bob |
| Company Z | Alice > Bob > Carol |

Candidate-proposing deferred acceptance ends in round 1: Alice–X, Bob–Y, Carol–Z (every candidate 1st choice, every company 3rd).
Company-proposing ends in round 1: Bob–X, Carol–Y, Alice–Z (every company 1st choice, every candidate 3rd).
Both are stable; a third stable matching Alice–Y, Bob–Z, Carol–X gives everyone their 2nd choice.

## One Market, Two Endings

**Tags:** `core idea` (blue), `who proposes` (green)

- **The market** — three candidates, three companies, each side fully ranks the other
- **Candidates rank** — Alice: X > Y > Z; Bob: Y > Z > X; Carol: Z > X > Y
- **Companies rank** — X: Bob > Carol > Alice; Y: Carol > Alice > Bob; Z: Alice > Bob > Carol
- **Candidates propose** — round 1 ends it: Alice–X, Bob–Y, Carol–Z; every candidate gets 1st choice
- **Companies propose** — Bob–X, Carol–Y, Alice–Z; every company gets 1st, every candidate gets 3rd
- **Both stable** — in each matching, no candidate–company pair prefers each other to their matches

*Example (italic):* In the candidate-proposing run X ends up with Alice, its last choice — yet no pair blocks it: Bob ranks X last, and Carol already holds her first choice Z.

**Key point:** Same preferences, two stable endings — which side proposes decides which side walks away with first choices and which with last.

### Visualization (canvas `c1`, 720×300)

Two side-by-side panels split by a light divider at x=360: the candidate-proposing matching on the left, the company-proposing matching on the right, each side's happiness annotated.

- **Title (bold 15px, `#1a5276`, top center):** "Same Market, Two Stable Matchings (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=44 to y=262.
- **Left panel header (bold 13px blue `#2a78d6`, centered at (180, 58)):** "CANDIDATES PROPOSE".
- **Left panel nodes:** candidates at x=110, companies at x=265, y = 96/146/196 for Alice/Bob/Carol and X/Y/Z; filled circles radius 7, candidates blue `#2a78d6`, companies green `#008300`; labels bold 12px `#2c3e50`, names right-aligned at x=95 (+4 vertical offset), company letters left-aligned at x=280.
- **Left matched edges (3px blue `#2a78d6`):** Alice–X, Bob–Y, Carol–Z (three horizontal lines).
- **Left annotations (centered at x=185):** bold 12px green `#008300` at y=234 "candidates: all 1st choice"; bold 12px orange `#d95926` at y=252 "companies: all 3rd choice".
- **Right panel header (bold 13px green `#008300`, centered at (540, 58)):** "COMPANIES PROPOSE".
- **Right panel nodes:** candidates at x=470, companies at x=625, same y and styling as the left panel; names right-aligned at x=455, letters left-aligned at x=640.
- **Right matched edges (3px green `#008300`):** Bob–X (146→96), Carol–Y (196→146), Alice–Z (96→196) — crossing lines.
- **Right annotations (centered at x=545):** bold 12px green at y=234 "companies: all 1st choice"; bold 12px orange at y=252 "candidates: all 3rd choice".
- **Caption (12px `#6b7280`, centered at y=284):** "both matchings are stable — no candidate–company pair prefers each other to their assigned match".

## The Proposing Side Takes the Surplus

**Tags:** `rule of thumb` (orange), `proposer-optimal` (blue)

- **Proposer-optimal** — DA hands every proposer the best partner they have in ANY stable matching
- **Receiver-pessimal** — the very same run gives every receiver their worst stable partner
- **All at once** — no trade-off among proposers: one matching is simultaneously best for each of them
- **The middle rung** — Alice–Y, Bob–Z, Carol–X is also stable: everyone lands their 2nd choice
- **Policy, not plumbing** — the proposing side captures the surplus; choosing it is a design decision

*Example (italic):* The three stable matchings of this market form a ladder — candidates first at the top, everyone second in the middle, companies first at the bottom.

**Key point:** Among all stable matchings, deferred acceptance gives the proposing side its best and the receiving side its worst — so "who proposes" is a policy decision about who captures the surplus, not an implementation detail.

### Visualization (canvas `c2`, 720×300)

A lattice sketch: three stacked stable-matching boxes joined by a vertical spine, a better-for axis on the left, and "lands here" arrows showing where each proposing rule ends up.

- **Title (bold 15px, `#1a5276`, top center):** "The Lattice of Stable Matchings (illustrative)".
- **Boxes (x=210, width 320, height 46; y = 52, 126, 200):** fill `#fbfcfd`; header bold 12px at box top+19, sub-line 11px `#6b7280` at top+36, both centered at x=370:
  - top, 2px blue `#2a78d6` border: "CANDIDATE-OPTIMAL — Alice–X, Bob–Y, Carol–Z" / "candidates 1st, companies 3rd";
  - middle, 2px violet `#4a3aa7` border: "MIDDLE — Alice–Y, Bob–Z, Carol–X" / "everyone 2nd";
  - bottom, 2px green `#008300` border: "COMPANY-OPTIMAL — Bob–X, Carol–Y, Alice–Z" / "companies 1st, candidates 3rd".
- **Spine:** 1.5px mute `#6b7280` vertical segments at x=370 from y=98 to y=126 and from y=172 to y=200.
- **Left axis:** bold 12px blue `#2a78d6` "↑ better for candidates" centered at (105, 56); 1.5px mute vertical line at x=105 from y=66 to y=236; bold 12px green `#008300` "↓ better for companies" centered at (105, 252).
- **Right arrows:** top — bold 12px blue two lines centered at x=628, "candidates propose" at y=62, "lands here" at y=78; 2px mute line from (600, 90) to (544, 90) with a left-pointing filled arrowhead at (538, 90). Bottom — bold 12px green two lines centered at x=628, "companies propose" at y=210, "lands here" at y=226; 2px mute line from (600, 238) to (544, 238) with a left-pointing arrowhead at (538, 238).
- **Caption (bold 12px magenta `#d55181`, centered at y=284):** "who proposes picks which end of the lattice the market lands on".

## Who Can Safely Tell the Truth

**Tags:** `strategy-proof` (green), `common mistake` (red)

- **Strategy-proof** — no proposer can get a better match by ranking untruthfully (a known theorem)
- **The intuition** — proposals already sweep your true list top-down; lying can only skip options
- **The loophole** — a receiver can sometimes gain by rejecting an acceptable early proposal
- **The cascade** — the rejection displaces a proposer, the chain ripples, a better one lands later
- **The catch** — pulling it off requires knowing others' lists; a wrong guess leaves you worse off

*Example (italic):* A company that rejects a good-enough round-1 proposal is betting the rejection chain circles back with someone better — a bet, not a guarantee.

**Key point:** Only the proposing side can safely tell the truth — deferred acceptance is strategy-proof for proposers, while receivers can (riskily) game the list by strategic rejection.

### Visualization (canvas `c3`, 720×300)

A four-step vertical flow of a strategic rejection cascading through the market, with a risk note on the right.

- **Title (bold 15px, `#1a5276`, top center):** "A Strategic Rejection Ripples Back (illustrative)".
- **Boxes (x=48, width 400, height 42; y = 48, 108, 168, 228):** fill `#fbfcfd`; header bold 12px at top+17, detail 12px `#2c3e50` at top+34, both centered at x=248:
  - 2px orange `#d95926` border, orange header "REJECT" / "the schemer turns down an acceptable early proposal";
  - 2px blue `#2a78d6` border, blue header "DISPLACE" / "the rejected proposer moves on down their list";
  - 2px blue border, blue header "CHAIN" / "the new proposal bumps someone from a tentative match";
  - 2px green `#008300` border, green header "PAYOFF" / "the bumped candidate proposes to the schemer — better match".
- **Down arrows between boxes:** 2px mute `#6b7280` vertical lines at x=248 from box bottom+3 to next box top−7, each ending in a downward filled arrowhead.
- **Risk note (bold 12px orange `#d95926`, centered at x=585):** three lines — "works only if the schemer" at y=126, "knows the other lists —" at y=144, "a wrong guess ends worse" at y=162.
- **Caption (12px `#6b7280`, centered at y=288):** "one strategic rejection — a bet on the chain circling back, not a guarantee".

## Clearinghouses Rebuilt Around the Theorem

**Tags:** `where it's used` (blue), `market design` (green)

- **The first question** — every user of a matching system asks: should I game my ranked list?
- **The doctors' match** — the NRMP ran hospital-proposing for decades; applicants feared gaming
- **The redesign** — in the 1990s it moved to applicant-proposing so applicants can rank honestly
- **The data dividend** — strategy-proof systems collect true preferences, so analytics mean something
- **The design lesson** — pick who proposes deliberately: it sets both the surplus and the honesty

*Example (italic):* After the redesign, the advice to medical students became one line — rank programs in the order you truly prefer them.

**Key point:** Real clearinghouses were rebuilt around exactly this theorem — applicant-proposing lets participants rank honestly, and honest rankings are what make the match (and any analytics on it) trustworthy.

### Visualization (canvas `c4`, 720×300)

A before/after pair of design boxes for the doctors' match, joined by a redesign arrow.

- **Title (bold 15px, `#1a5276`, top center):** "The Doctors' Match: Redesigned Around Who Proposes".
- **Left box (x=40, y=64, width 290, height 160):** fill `#fbfcfd`, 2px orange `#d95926` border; header bold 13px orange "HOSPITAL-PROPOSING" centered at x=185, y=88; sub-header 11px `#6b7280` "(original design)" at y=104; three 12px `#2c3e50` lines at y=134/160/186: "hospitals get their best stable match", "truthful ranking not always safe", "applicants tempted to game the list".
- **Right box (x=390, y=64, width 290, height 160):** fill `#fbfcfd`, 2px green `#008300` border; header bold 13px green "APPLICANT-PROPOSING" centered at x=535, y=88; sub-header 11px `#6b7280` "(1990s redesign)" at y=104; three 12px lines at y=134/160/186: "applicants get their best stable match", "truthful ranking is provably safe", "advice: rank what you truly want".
- **Arrow label (bold 12px violet `#4a3aa7`, centered at x=360):** "1990s" at y=118, "redesign" at y=133.
- **Arrow:** 2px mute `#6b7280` from (334, 150) to (380, 150) with a right-pointing filled arrowhead at (386, 150).
- **Caption (bold 13px magenta `#d55181`, centered at y=258):** "same stable-matching engine — the redesign changed only which side proposes".
- **Sub-caption (12px `#6b7280`, centered at y=280):** "truthful rankings are what make match statistics worth analyzing".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize; shared `arrowHead(ctx, x, y, dir, color)` helper for filled arrowheads (`right`, `left`, `down`); shared `node(ctx, x, y, color)` filled-circle helper for the c1 graph panels.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data integrity:** hardcoded rankings only — candidates `Alice: X>Y>Z, Bob: Y>Z>X, Carol: Z>X>Y`; companies `X: Bob>Carol>Alice, Y: Carol>Alice>Bob, Z: Alice>Bob>Carol`; candidate-optimal matching `Alice–X, Bob–Y, Carol–Z`; middle matching `Alice–Y, Bob–Z, Carol–X`; company-optimal matching `Bob–X, Carol–Y, Alice–Z`; invented setup carries "(illustrative)" in chart titles. The NRMP applicant-proposing redesign is documented public history and is named factually.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
