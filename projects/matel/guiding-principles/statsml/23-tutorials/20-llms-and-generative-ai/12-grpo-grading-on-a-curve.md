# GRPO: Grading on a Curve

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** GRPO: Grading on a Curve

**Subtitle:** How reasoning models practice — the model writes a group of answers to the same question, the group's average score becomes the bar, and whatever beats the bar gets reinforced; reinforcement learning without a judge model

## Grading on a Curve, Eight Attempts at a Time

**Tags:** `core idea` (blue), `no judge model` (green)

- **The setup** — after pre-training, the model practices: it writes 8 answers to the same question
- **The scores** — a checker marks each answer: 1 if the final result is right, 0 if it is wrong
- **The bar** — the group's average score becomes the bar; there is no separate judge model
- **The push** — answers above the bar get reinforced; answers below the bar get discouraged
- **The name** — Group Relative Policy Optimization: improve the policy relative to its own group

*Example (italic):* It is grading on a curve where the whole class is one student — the model competes against its own eight attempts.

**Key point:** GRPO turns "how good is this answer?" into "is it better than my other tries?" — a question a group average answers for free.

### Visualization (canvas `c1`, 720×300)

One question fanning out to eight answer cards, each marked right or wrong, with a dashed "bar" line below and per-card up/down arrows showing which answers get pushed up or down.

- **Title (bold 15px, `#1a5276`, top center):** "One Question, Eight Answers, One Bar".
- **Question box:** rounded rect x=250, y=38, 220×30, fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border; bold 12px `#1a5276` centered label "Q: 17 × 24 = ?".
- **Answer cards:** eight rounded rects 72×40 (6px radius), tops y=96, left edges x = `[37, 119, 201, 283, 365, 447, 529, 611]`; correct cards (1st, 4th, 7th) fill `rgba(0,131,0,0.12)` with 2px `#008300` border and bold 12px `#008300` centered labels "408 ✓"; wrong cards fill `rgba(107,114,128,0.10)` with 2px `#6b7280` border and 12px `#6b7280` labels "401 ✗", "398 ✗", "410 ✗", "409 ✗", "400 ✗" (in card order 2, 3, 5, 6, 8).
- **Bar line:** dashed (4/3) 1.5px `#6b7280` horizontal line at y=196 from x=37 to x=683; 12px `#6b7280` label "the bar = group average" left-aligned at (37, 246).
- **Arrows (per card, at the card's center x):** correct cards get a 2px `#008300` arrow from (cx, 228) up to (cx, 164) with arrowhead at the top; wrong cards get a 2px `#6b7280` arrow from (cx, 164) down to (cx, 228) with arrowhead at the bottom.
- **Annotation (bold 12px orange `#d95926`, centered at y=268):** "above the bar → reinforced; below the bar → discouraged — no judge model anywhere".
- **Caption (11px `#444`, bottom right, y=292):** "correct answer: 17 × 24 = 408".

## Redo the Arithmetic on One Question

**Tags:** `worked example` (blue), `advantage` (orange)

- **The question** — "17 × 24 = ?"; the model writes 8 answers; 3 say 408 (right), 5 are wrong
- **The bar** — group average = 3 ÷ 8 = 0.375
- **The push** — each correct answer: 1 − 0.375 = +0.625; each wrong one: 0 − 0.375 = −0.375
- **The update** — nudge the model toward the +0.625 answers' steps, away from the −0.375 ones
- **The detail** — real GRPO also divides by the group's spread; the sign and idea stay the same

*Example (italic):* Nothing ever judged the reasoning itself — only the final 408 was checked, and the group average did the rest.

**Key point:** Advantage = your score minus your group's average — one line of arithmetic you can redo yourself.

### Visualization (canvas `c2`, 720×300)

Bar chart of the eight advantages around a zero line: three +0.625 bars up, five −0.375 bars down, with the defining arithmetic written out at the top left.

- **Title (bold 15px, `#1a5276`, top center):** "Score − Group Average = the Push".
- **Zero line:** 1px `#999` horizontal line at y=170 from x=60 to x=690; 12px `#444` label "0" right-aligned at (54, 174).
- **Bars (50px wide, centered x = `[80, 160, 240, 320, 400, 480, 560, 640]`, scale 1.0 = 120px):** advantages in card order `[+0.625, −0.375, −0.375, +0.625, −0.375, −0.375, +0.625, −0.375]`; positive bars `#008300` rising 75px above the zero line, negative bars `#6b7280` dropping 45px below; bold 12px value label at each bar's far end in the bar's color ("+0.625" / "−0.375").
- **Marks (12px, centered under y=248 at each bar x):** "✓" in `#008300` under positive bars, "✗" in `#6b7280` under negative bars.
- **Math note (12px, left-aligned at x=76, lines y=52/68/84):** "group average = 3/8 = 0.375" in `#444`; "correct: 1 − 0.375 = +0.625" in `#008300`; "wrong: 0 − 0.375 = −0.375" in `#6b7280`.
- **Annotation (bold 12px orange `#d95926`, centered at y=272):** "real GRPO also divides by the spread — same push, standardized".
- **Caption (11px `#444`, bottom right, y=292):** "advantages you can recompute by hand".

## Why Labs Switched to It

**Tags:** `where it's used` (blue), `reasoning models` (green), `cheap` (orange)

- **The old way** — PPO-style RLHF trains a second "critic" network just to estimate the bar
- **The saving** — GRPO deletes that network: the group average is the bar, computed for free
- **Made famous** — DeepSeek-R1 trained its reasoning with GRPO; many labs followed
- **Best fit** — tasks with checkable answers: math, code that runs, formats that validate
- **The recipe** — sample a group, check answers, push the above-average ones — millions of times

*Example (italic):* The reward can be a five-line checker ("does the code compile and pass the test?") instead of a learned judge.

**Key point:** GRPO = RLHF minus the critic model — cheapest exactly where a simple checker can score the answers.

### Visualization (canvas `c3`, 720×300)

Side-by-side panels: classic PPO with two trained networks (policy + critic) on the left, GRPO with one network plus a group-average calculation on the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Expensive Judge vs the Group Average".
- **Left panel:** rounded rect x=30, y=52, 310×200, fill `rgba(74,58,167,0.05)`, 2px `#4a3aa7` border; header bold 13px `#4a3aa7` centered at (185, 74): "classic RLHF (PPO)".
  - Policy box: rounded rect x=65, y=92, 240×46, fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border; bold 12px `#1a5276` centered two lines "policy model" / "(writes the answers)".
  - Critic box: rounded rect x=65, y=156, 240×46, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border; bold 12px `#4a3aa7` centered two lines "critic model" / "(a second network to train)".
  - Note (12px `#6b7280`, centered at (185, 228)): "two big networks in memory".
- **Right panel:** rounded rect x=380, y=52, 310×200, fill `rgba(0,131,0,0.05)`, 2px `#008300` border; header bold 13px `#008300` centered at (535, 74): "GRPO".
  - Policy box: rounded rect x=415, y=92, 240×46, same blue style; bold 12px `#1a5276` centered two lines "policy model" / "(writes 8 answers)".
  - Average box: rounded rect x=465, y=156, 140×34, fill `rgba(0,131,0,0.12)`, 2px `#008300` border; bold 12px `#008300` centered label "group average".
  - Note (12px `#6b7280`, centered at (535, 228)): "one network + a calculator".
- **Annotation (bold 12px orange `#d95926`, centered at y=274):** "the critic — a whole second model — is replaced by one line of arithmetic".
- **Caption (11px `#444`, bottom right, y=294):** "training setups simplified".

## When the Curve Teaches Nothing

**Tags:** `common mistake` (red), `zero signal` (orange)

- **All wrong** — 0 of 8 correct: the average is 0, every advantage is 0 − 0 = 0; nothing is learned
- **All right** — 8 of 8 correct: same story; the group average equals every single score
- **The sweet spot** — questions the model sometimes solves; mixed groups carry all the signal
- **The mistake** — training on far-too-hard or far-too-easy questions and seeing zero progress
- **Not a new reward** — GRPO changes how the push is computed, not what counts as good

*Example (italic):* A batch of impossible olympiad problems produced zero learning — every group scored 0 for 8, so every push was zero.

**Common mistake:** Curriculum blindness — GRPO only learns from disagreement inside a group, so question difficulty must track what the model can sometimes do.

### Visualization (canvas `c4`, 720×300)

Three panels showing eight-answer groups as dot rows — a mixed group with a strong learning signal, an all-wrong group, and an all-right group both yielding zero signal.

- **Title (bold 15px, `#1a5276`, top center):** "Three Groups, One Lesson Each".
- **Panels:** three rounded rects 210×170 at x = `[30, 255, 480]`, y=52; mixed panel 2px `#008300` border, the other two 2px `#6b7280` borders, no fill.
- **Panel headers (bold 13px, centered at panel center, y=76):** "3 of 8 correct" in `#008300`; "0 of 8 correct" in `#6b7280`; "8 of 8 correct" in `#6b7280`.
- **Dots:** in each panel, 8 circles (radius 9) in two rows of four (row y = 110 and 150, centers offset 42px apart starting 51px from panel left); filled `#008300` for correct, `#6b7280` for wrong: panel 1 has 3 green (positions 1, 4, 7), panel 2 none, panel 3 all 8.
- **Signal labels (bold 13px, centered, y=196):** "strong signal — learns" in `#008300`; "zero signal — stuck" in `#6b7280`; "zero signal — too easy" in `#6b7280`.
- **Advantage lines (11px `#6b7280`, centered, y=212):** "pushes: +0.625 / −0.375", "every push: 0 − 0 = 0", "every push: 1 − 1 = 0".
- **Annotation (bold 12px orange `#d95926`, centered at y=262):** "if every answer scores the same, every push is zero — difficulty must track ability".
- **Caption (11px `#444`, bottom right, y=290):** "groups illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all card positions, dot colors, and bar values are the hardcoded arrays above (no randomness); the eight answers must show exactly 3 correct to match 3/8 = 0.375, and the advantage bars must read +0.625 / −0.375 to match the text arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
