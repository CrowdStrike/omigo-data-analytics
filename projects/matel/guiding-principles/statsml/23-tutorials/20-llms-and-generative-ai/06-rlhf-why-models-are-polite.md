# RLHF: Why Models Are Polite

**Page type:** detail page (tutorial layout: `.card-section` blocks, each a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** RLHF: Why Models Are Polite

**Subtitle:** Show the model two replies, let humans vote for the better one, and train toward the winners — the helpful tone is taught, not born

## Two Replies, One Vote

**Tags:** `core idea` (blue), `running example` (green)

- **The question** — "My flight was cancelled — what should I do?"
- **Reply A** — "Check the airline's rebooking policy." — correct, but curt
- **Reply B** — a calm three-step answer: rebook, keep receipts, ask about refunds
- **The vote** — a human reads both and clicks the better one: B
- **The scale** — repeat this over tens of thousands of questions and reply pairs

*Example:* No one writes a rule saying "be warm and give steps" — the preference is shown to the model one vote at a time.

**Key point:** RLHF — Reinforcement Learning from Human Feedback — trains the model toward whichever replies humans preferred, thousands of times over.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one prompt box fanning out to two reply cards, with vote tallies beneath.

- **Title (bold 15px `#1a5276`, top center):** "One Question, Two Candidate Replies, One Human Vote".
- **Prompt box (centered, 320×36 at y=44):** fill `rgba(74,58,167,0.10)`, violet `#4a3aa7` 2px outline, bold 12px violet text '"My flight was cancelled — what should I do?"'.
- **Reply cards (280×104 at y=116, faint `rgba(0,0,0,0.02)` fill, 2px colored outline, arrow lines from prompt):**
  - Left (x=60, mute `#6b7280`): title "Reply A — curt", lines: '"Check the airline\'s' / 'rebooking policy."' / '(correct, unhelpful in tone)'.
  - Right (x=380, green `#008300`): title "Reply B — steps", lines: "1. rebook on the app now" / "2. keep meal & hotel receipts" / "3. ask about refund rights".
- **Vote tallies (below cards, centered):** bold 13px mute "1 vote" under A; bold 14px green "9 votes ✓ preferred" under B.
- **Caption (bold 13px orange `#d95926`, bottom center, y=278):** "the click is the training signal — repeated tens of thousands of times".

## Following One Vote Through the Pipeline

**Tags:** `worked example` (green)

- **Collect votes** — 10 labelers compare A and B; 9 choose B, 1 chooses A
- **Train a judge** — a reward model learns to score replies: A gets 2.1, B gets 8.4
- **The judge generalizes** — brand-new replies it never saw also get scores
- **Tune the LLM** — the model is adjusted to write replies the judge scores high
- **The shift** — B-style answers go from 30 in 100 drafts to 90 in 100

*Example:* The reward model is the crowd's taste compressed into a number — a 9-of-10 vote becomes 8.4 versus 2.1.

**Key point:** Humans cannot vote on every future answer, so their votes train a stand-in judge (the reward model), and the LLM is tuned to please that judge.

### Visualization (canvas `c2`, 720×300)

Three-panel pipeline of small two-bar charts, separated by dashed dividers at x=250 and x=480.

- **Title (bold 15px `#1a5276`, top center):** "Votes → a Judge's Score → a Tuned Model".
- **Shared bar geometry:** baseline y=218, chart height 120, bars 60px wide, alpha 0.65 fills, bold 14px colored value labels above, 12px labels below, thin gray `#999` baselines; bold 13px ink panel titles at y=60; 12px `#444` panel footnotes below the bar labels.
- **Panel 1 "1. human votes" (scale max 10):** Reply A = 1 (mute `#6b7280`), Reply B = 9 (green `#008300`); footnote "9 of 10 labelers pick B".
- **Panel 2 "2. reward model score" (scale max 10):** Reply A = 2.1 (mute), Reply B = 8.4 (green); footnote "the votes, learned as a number".
- **Panel 3 "3. B-style drafts per 100" (scale max 100):** before = 30 (blue `#2a78d6`), after = 90 (green); footnote "tuning toward high scores".
- **Caption (bold 13px orange, bottom center, y=286):** "each stage passes the preference along: 9-of-10 → 8.4 vs 2.1 → 90 in 100".

## The Judge Grades Taste, Not Truth

**Tags:** `watch out` (orange), `where it's used` (blue)

- **Optimizes the vote** — the model learns what people pick, not what is true
- **Confidence wins** — a sure-sounding wrong answer can beat a hedged right one
- **Sycophancy** — agreeing with the user collects votes; disagreeing loses them
- **Verbosity creep** — longer, well-formatted replies tend to collect more votes
- **Labelers matter** — whoever votes defines "better"; their blind spots bake in

*Example:* In one head-to-head, the confident wrong answer took 65 of 100 votes against the hedged correct one.

**Key point:** A polished, agreeable answer is evidence that RLHF worked, not evidence that the answer is correct — verify the facts separately.

### Visualization (canvas `c3`, 720×300)

Two horizontal vote-share bars: confident-but-wrong vs hedged-but-right.

- **Title (bold 15px `#1a5276`, top center):** "100 Votes: Confident-but-Wrong vs Hedged-but-Right".
- **Bars (start x=60, max width 420 at 100 votes, 44px tall, alpha 0.60 fill + 1.5px outline):**
  - Top (y=92): 65 votes, magenta `#d55181`; heading above (bold 13px ink) "confident, WRONG", italic 12px `#555` sub '"The answer is definitely X."'; bold magenta "65 votes" after the bar; bold red `#e74c3c` "✗ incorrect" further right.
  - Bottom (y=182): 35 votes, aqua `#199e70`; heading "hedged, RIGHT", sub '"It depends, but likely Y because…"'; bold aqua "35 votes"; bold green `#008300` "✓ correct".
- **Separator:** thin `#e5e9ef` horizontal line at y=142.
- **Caption (bold 13px red, bottom center, y=262):** "the wrong answer wins the vote — and the vote is what gets trained".
- **Note (11px mute, right-aligned):** "illustrative counts".

## Taught, Not Emergent

**Tags:** `common mistake` (red), `core idea` (blue)

- **The base model** — after pre-training only, it just continues text: rude in, rude out
- **No built-in manners** — the internet it read contains every tone, not one
- **RLHF is the finishing school** — a thin final stage steers toward the voted style
- **Same knowledge** — the tuning changes tone and habits far more than facts
- **"Wants to help"** — shorthand for "was tuned toward replies people preferred"

*Example:* Ask the raw base model for advice and it may simply continue your sentence or drift into a forum thread — the assistant voice appears only after RLHF.

**Key point (labeled "Common mistake:"):** thinking helpfulness emerges from reading the internet. Pre-training supplies the ability; RLHF supplies the manner.

### Visualization (canvas `c4`, 720×300)

Before/after comparison cards for the same prompt, each with a helpfulness-rating bar.

- **Title (bold 15px `#1a5276`, top center):** "Same Question, Before and After RLHF".
- **Prompt line (bold 12px violet `#4a3aa7`, centered, y=50):** '"My flight was cancelled — what should I do?"'.
- **Cards (290×130 at y=70, faint `rgba(0,0,0,0.02)` fill, 2px colored outline, bold 13px colored title, italic 12px `#333` body lines):**
  - Left (x=50, mute `#6b7280`): title "base model — continues text", lines: '"…what should I do? This is the' / 'third time this airline has done' / 'this to me. Anyway, as I was' / 'saying in my last post…"'.
  - Right (x=390, green `#008300`): title "after RLHF — answers", lines: '"Sorry about the cancellation.' / '1. rebook on the app now' / '2. keep meal & hotel receipts' / '3. ask about refund rights"'.
- **Helpfulness rating bars (below each card):** 12px `#444` label "helpfulness rating"; a 200×16 outlined meter filled to rating/10 at alpha 0.6 in the card's color; bold 13px colored score — left card "3 / 10", right card "9 / 10".
- **Captions:** bold 13px red `#e74c3c` centered at y=280: "same knowledge in both — the assistant manner is the trained part"; 11px mute right-aligned "illustrative ratings".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`, social-graph reference skeleton). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` row of colored pill spans first, then a `<ul>` of one-line bullets each opening with `<b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, `<strong>` label).
- **Tag pill colors:** blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`. Pills: 0.72rem, weight 600, padding 2px 10px, radius 10px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with `1px solid #e0e0e0` border, radius 4px. No nav bar, no back/home links, no cross-page links.
- **Canvas:** all charts 720×300 logical, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (fixed 720×300 rect, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates). Hardcoded data arrays only — no `Math.random()`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
