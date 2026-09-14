# Zero-Knowledge Proofs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Zero-Knowledge Proofs

**Subtitle:** How to convince someone you know a secret — a password, an age, a solution — without revealing one bit of the secret itself

## The Cave With the Magic Door

**Tags:** `core idea` (blue), `Ali Baba cave` (orange), `prove without revealing` (green)

- **The cave** — a circular cave with one entrance that forks into a left path and a right path
- **The door** — the two paths meet at the back behind a magic door opened only by a secret word
- **The claim** — Peggy says she knows the word; Victor wants proof but Peggy won't say the word
- **One round** — Peggy walks in by a random side; Victor then shouts which side to come OUT of
- **With the word** — she opens the door if needed and always exits the side Victor demanded
- **Without it** — she can only exit the side she entered, matching his shout half the time

*Example (italic):* Peggy enters by the left path, Victor shouts "come out the right!" — she whispers the word, the door opens, and she strolls out the right side.

**Key point:** That is a zero-knowledge proof: the exchange convinces Victor that Peggy knows the word, yet the word itself never leaves her lips — he sees only her walking out of a cave.

### Visualization (canvas `c1`, 720×300)

Top-down map of the cave: an entrance at the bottom forking into two curved paths that meet at a magic door at the top, with one round's moves drawn as arrows.

- **Title (bold 15px, `#1a5276`, top center):** "One Round in the Ali Baba Cave".
- **Cave outline:** two 4px `#6b7280` arcs forming a ring centered at (360, 175), outer radius 115, inner radius 65, with a 60px gap at the bottom (the entrance) and a 12px gap at the top (the door).
- **Door:** violet `#4a3aa7` 16×34 rounded rect at (352, 48), bold 12px violet label "magic door — opens to the secret word" above it at y=38.
- **Path labels:** bold 13px `#2a78d6` "path A" at (215, 175); bold 13px `#d95926` "path B" at (505, 175).
- **Entrance:** 12px `#444` label "entrance" at (360, 292) below the gap; small stick-figure dot for Victor at (360, 268) labeled 12px `#444` "Victor waits here".
- **Peggy's walk in:** blue `#2a78d6` 3px arrow following the left arc from the entrance up to the door, 12px blue label "1. Peggy enters by a random side" at (150, 110).
- **Victor's shout:** bold 12px `#1a5276` speech text at (475, 268): "2. \"Come out the RIGHT!\"".
- **Peggy's walk out:** green `#008300` 3px arrow following the right arc from the door down to the entrance, 12px green label "3. door opens — she exits right" at (565, 110).
- **Annotation (bold 13px green `#008300`, two centered lines at x=145, y=252/268):** "without the word she exits" / "her entry side — right 50% of the time".
- **Caption (12px `#444`, bottom right):** "geometry schematic".

## Twenty Rounds Against a Coin Flip

**Tags:** `worked example` (blue), `soundness math` (green), `three properties` (orange)

- **One round** — a faker matches Victor's shout only if her random entry side happens to agree: 1/2
- **Halving** — every extra round multiplies a faker's survival chance by 1/2: (1/2)^k after k rounds
- **Ten rounds** — 2¹⁰ = 1,024, so a faker survives all ten with probability exactly 1/1,024
- **Twenty rounds** — 2²⁰ = 1,048,576: faking every round succeeds 1 time in 1,048,576 — about one in a million
- **What Victor learns** — twenty exits on demand, and nothing else; no letter of the word leaks
- **Hand-check** — replay any transcript yourself: entries are random, exits match; the word never appears

*Example (italic):* After round 20 Victor's doubt has shrunk to exactly 1/1,048,576 — he would sooner believe Peggy won a million-to-one lottery than that she guessed every shout.

**Key point:** Three properties define the game — completeness (an honest prover always convinces), soundness (a liar survives 20 rounds only 1 time in 2²⁰), and zero-knowledge (the verifier learns only that the statement is true).

### Visualization (canvas `c2`, 720×300)

Bar chart of a faker's survival probability after 1, 2, 5, 10, 15, and 20 rounds, on a log-feel scale with each bar labeled by its exact fraction.

- **Title (bold 15px, `#1a5276`, top center):** "Chance of Faking Every Round: Halved 20 Times".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = six bars labeled "1", "2", "5", "10", "15", "20" rounds (12px `#444`, axis caption "rounds survived" centered below at y=272); no numeric y axis — bar heights are hardcoded log-feel pixels.
- **Bars (55px wide, centered at x = 120, 215, 310, 405, 500, 595):** heights 172, 155, 129, 86, 43, 8 px; first five fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, the 20-round bar solid green `#008300`.
- **Bar labels (bold 12px `#1a5276` above each bar):** "1/2", "1/4", "1/32", "1/1,024", "1/32,768", "1/1,048,576".
- **Annotation (bold 13px green `#008300`, near x=430, y=70):** "20 rounds: 1 in 1,048,576 — about one in a million".
- **Caption (12px `#444`, bottom right):** "probabilities exact: (1/2)^k; bar heights log-feel schematic".

## Proving Without Showing, in the Real World

**Tags:** `where it's used` (blue), `privacy` (green)

- **Age checks** — prove "I am over 18" to a website without the site ever seeing the birthdate
- **Private payments** — privacy blockchains prove a transaction balances without revealing the amounts
- **Verified compute** — zk-SNARKs let a server prove a computation was done correctly without the checker redoing it
- **Fewer secrets held** — the verifier stores no birthdate, no balance, no password to later leak
- **No cave needed** — the interactive rounds can be replaced by a single non-interactive proof anyone can check

*Example (italic):* A bar's door scanner reads a proof from a phone and learns exactly one bit — "over 18: yes" — not the date, not the name, not the ID number.

**Key point:** Every use is the cave in disguise: convince a stranger a statement is true while handing over zero reusable information about why it is true.

### Visualization (canvas `c3`, 720×300)

Three-row comparison diagram: for each real use, a red "what you reveal today" box beside a green "what a zero-knowledge proof reveals" box.

- **Title (bold 15px, `#1a5276`, top center):** "What Gets Revealed: Today vs a Zero-Knowledge Proof".
- **Column headers (bold 13px, y=62):** red `#e74c3c` "reveals everything" centered at x=280; green `#008300` "reveals one bit" centered at x=565.
- **Rows (y = 100, 165, 230), each with a left-aligned 12px `#444` label at x=20:** "age check", "payment", "computation".
- **Left boxes (x=170, 220 wide, 44 tall, 8px radius, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, 12px `#2c3e50` text):** "full ID: name, birthdate, address" / "sender, receiver, amount" / "all inputs + rerun the work".
- **Right boxes (x=455, 220 wide, 44 tall, 8px radius, fill `rgba(0,131,0,0.12)`, 2px `#008300` border):** "over 18: yes" / "transaction valid: yes" / "result correct: yes".
- **Arrows:** 3px `#6b7280` arrow between each left and right box.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=282):** "zk-SNARKs: one short non-interactive proof replaces all twenty rounds".

## "Surely Victor Learns Something"

**Tags:** `common mistake` (red), `two balls` (green)

- **The confusion** — people assume 20 convincing rounds must leak at least a hint of the word
- **Two balls** — hand a color-blind friend a red ball and a green ball, one per hand, behind his back
- **The round** — he secretly swaps them or not, shows you, and you say whether he swapped
- **The score** — you see the colors, so you are right every time; a bluffer guesses right 1/2 per round
- **What he learns** — after 20 correct calls he knows the balls differ; which one is red, he still cannot tell
- **Why nothing leaks** — his own transcript is a fair coin's flips plus correct answers; he could fake it alone

*Example (italic):* Twenty rounds and twenty correct calls later, the friend is 1,048,575-to-1 convinced the balls differ in color — yet he still could not pick out the red one.

**Common mistake:** Believing conviction requires disclosure. The verifier gains exactly one bit — "the claim is true" — and a transcript he could have simulated himself, which is precisely why it teaches him nothing more.

### Visualization (canvas `c4`, 720×300)

Strip of the first 8 rounds of the color-blind two-ball game: each round shows the friend's secret coin (swap or keep) and your correct call, with a running bluffer-survival fraction underneath.

- **Title (bold 15px, `#1a5276`, top center):** "The Color-Blind Friend: 8 Rounds, 8 Correct Calls".
- **Round columns:** 8 columns centered at x = 90, 170, 250, 330, 410, 490, 570, 650; 12px `#444` header "r1"…"r8" at y=70.
- **Secret row (y=110, 12px `#444` row label "his coin" at x=14):** hardcoded sequence `["swap", "keep", "keep", "swap", "swap", "keep", "swap", "keep"]` in 12px `#6b7280`, each inside a 56×26 rounded rect fill `rgba(107,114,128,0.12)`.
- **Your call row (y=165, row label "your call"):** same words repeated in bold 12px green `#008300` with a green check mark, 56×26 rounded rect fill `rgba(0,131,0,0.12)` — all 8 correct.
- **Bluffer row (y=225, row label "bluffer survives"):** bold 12px `#e74c3c` fractions under each column: "1/2", "1/4", "1/8", "1/16", "1/32", "1/64", "1/128", "1/256".
- **Annotation (bold 13px `#1a5276`, centered at y=272):** "continue to round 20 and a bluffer survives 1/1,048,576 — yet 'which ball is red' never gets asked".
- **Caption (12px `#444`, bottom right):** "swap sequence illustrative; fractions exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the survival probabilities are exact powers of 1/2 (2¹⁰ = 1,024; 2²⁰ = 1,048,576) — label them exact; cave geometry, bar pixel heights, and the c4 swap sequence are schematic/illustrative and labeled so.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
