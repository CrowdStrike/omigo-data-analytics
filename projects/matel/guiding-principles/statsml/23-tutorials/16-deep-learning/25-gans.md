# GANs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** GANs

**Subtitle:** A GAN pits a forger against a detective — the forger makes fake data, the detective calls real or fake, and each round of feedback sharpens both until the fakes pass for real

## The Forger and the Detective

**Tags:** `core idea` (blue), `two players` (green), `adversarial` (orange)

- **The forger** — a counterfeiter prints fake banknotes and slips them into a pile of real ones
- **The detective** — a bank inspector examines each note and calls it "real" or "fake"
- **The feedback** — every caught fake tells the forger what gave it away; every miss sharpens the detective
- **Two learners** — the forger improves by fooling, the detective by catching — same game, opposite goals
- **The name** — a GAN (generative adversarial network) is exactly this: a generator playing against a discriminator

*Example (italic):* The forger's first note is caught instantly — wrong ink; a few batches later the same detective waves half the fakes through.

**Key point:** A GAN trains two networks against each other: a generator that makes fakes and a discriminator that judges them, and the competition itself is what teaches the generator to make convincing data.

### Visualization (canvas `c1`, 720×300)

Flow diagram of one round of the game: forger and real notes feed the detective, the detective issues a verdict, and a feedback loop runs back to the forger.

- **Title (bold 15px, `#1a5276`, top center):** "The GAN Game: Forger vs Detective".
- **Forger box:** rounded rect at x=45, y=120, 150×60; fill `rgba(42,120,214,0.12)`, 2px blue `#2a78d6` border; bold 13px blue "FORGER" over 11px `#6b7280` "(generator)".
- **Real-notes box:** rounded rect at x=305, y=40, 150×50; fill `rgba(0,131,0,0.10)`, 2px green `#008300` border; bold 13px green "REAL NOTES" over 11px `#6b7280` "(from the bank)".
- **Detective box:** rounded rect at x=305, y=120, 150×60; fill `rgba(74,58,167,0.10)`, 2px violet `#4a3aa7` border; bold 13px violet "DETECTIVE" over 11px `#6b7280` "(discriminator)".
- **Verdict box:** rounded rect at x=560, y=120, 120×60; fill `#f8f9fa`, 2px ink `#1a5276` border; bold 13px ink "VERDICT" over 11px `#6b7280` "'real' or 'fake'".
- **Solid arrows (2px `#6b7280`, filled arrowheads):** forger→detective from (195,150) to (305,150) with 12px `#444` label "fake note" above; real-notes→detective from (380,90) to (380,120) with 12px `#444` label "real note" at its right; detective→verdict from (455,150) to (560,150).
- **Feedback loop:** orange `#d95926` 2px dashed (dash 6/4) path from verdict bottom (620,180) down to (620,250), across to (120,250), up to (120,180) with arrowhead into the forger box; bold 12px orange label centered near (370,242): "feedback: what gave the fake away".
- **Annotation (bold 13px magenta `#d55181`, near x=60, y=75):** "both players improve every round".

## Six Rounds of the Ink Game

**Tags:** `worked example` (blue), `training loop` (green)

- **One clue** — real notes use about 60% ink coverage; that single number is what both sides fight over
- **Forger's rule** — each round, close 40% of the gap to 60% (rounded): 20 → 36 → 46 → 52 → 55 → 57
- **Detective's rule** — catch rate is roughly 50% plus the gap in points: at 20% ink, gap 40, so 90% caught
- **Check a round** — round 3: forger at 46%, gap 14, catch rate 50 + 14 = 64% — redo any row by hand
- **Round six** — forger at 57% ink, gap only 3, the detective catches just 53% — barely beats a coin
- **Equilibrium** — when the detective hits 50% they are guessing; the fakes have become indistinguishable

*Example (italic):* Round 1: ink 20%, caught 90%; round 3: ink 46%, caught 64%; round 6: ink 57%, caught 53% — the game is nearly over.

**Key point:** Training is this loop repeated: the forger drifts toward the real data (20 → 57, chasing 60) while the detective's edge melts from 90% toward the 50% coin flip.

### Visualization (canvas `c2`, 720×300)

Two-line chart over six rounds on one 0–100% axis: the forger's ink coverage climbing toward the real notes' 60% line while the detective's catch rate slides toward the 50% coin-flip line.

- **Title (bold 15px, `#1a5276`, top center):** "Six Rounds: Forger Closes the Gap, Detective Slides to a Coin Flip".
- **Axes:** origin x=60, baseline y=245, plot width 580, plot height 185; x = rounds 1–6 evenly spaced with 12px `#444` labels "round 1" … "round 6"; y = 0 to 100% with light `#e5e9ef` gridlines every 20 and 12px `#444` labels "0%" … "100%".
- **Real-notes line:** horizontal dashed green `#008300` (dash 4/3) line at 60%; 12px green label at its right end: "real notes: 60% ink".
- **Coin-flip line:** horizontal dashed `#6b7280` (dash 4/3) line at 50%; 12px `#6b7280` label at its right end: "coin flip: 50%".
- **Forger line:** blue `#2a78d6` 3px line with 6px dots through ink coverage `[20, 36, 46, 52, 55, 57]`; bold 12px blue value labels below each dot ("20", "36", "46", "52", "55", "57"); 12px blue series label "forger's ink %" near round 2.
- **Detective line:** orange `#d95926` 3px line with 6px dots through catch rate `[90, 74, 64, 58, 55, 53]`; bold 12px orange value labels above each dot ("90", "74", "64", "58", "55", "53"); 12px orange series label "detective catch %" near round 2.
- **Annotation (bold 13px green `#008300`, near round 5, y=95):** two lines: "at 50% the detective is guessing —" / "the fakes pass for real".
- **Caption (12px `#444`, bottom right):** "illustrative — a one-number 'ink game' toy GAN".

## Why Anyone Trains a Forger

**Tags:** `where it's used` (blue), `generation` (green)

- **The prize** — after training, the detective is thrown away and the forger is kept: a machine for new data
- **New faces** — GANs generate photorealistic faces of people who do not exist, the classic demo
- **More data** — a hospital with a few hundred rare scans can generate lookalikes to augment training
- **Image translation** — sketch to photo, day to night, blurry to sharp: all forger-vs-detective setups
- **The test** — a good forger's output histogram sits on top of the real one, like the chart on the right

*Example (italic):* A factory team with 300 photos of a rare defect trains a GAN and gets thousands of plausible new defect images for their classifier.

**Key point:** The point of the game is the generator: it ends up producing new samples whose distribution matches the real data — the detective was only scaffolding.

### Visualization (canvas `c3`, 720×300)

Paired bar chart: histogram of ink coverage for 48 real notes next to 48 generated notes after training, bin by bin, showing the two distributions landing on top of each other.

- **Title (bold 15px, `#1a5276`, top center):** "After Training: Fake Notes Match the Real Distribution".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; x = six ink-coverage bins with 12px `#444` center labels "50%", "54%", "58%", "62%", "66%", "70%"; y = count 0 to 16 with light `#e5e9ef` gridlines at 4, 8, 12 and 12px `#444` labels.
- **Real bars:** fill `rgba(42,120,214,0.35)`, 1.5px blue `#2a78d6` border, counts `[3, 8, 14, 13, 7, 3]`; each pair of bars ~34px wide with a small gap inside the pair.
- **Generated bars:** fill `rgba(0,131,0,0.35)`, 1.5px green `#008300` border, counts `[2, 7, 13, 14, 8, 4]`, drawn immediately right of each real bar.
- **Legend (top right, 12px `#444`):** blue swatch "real notes (48)", green swatch "generated notes (48)".
- **Annotation (bold 13px green `#008300`, near x=380, y=80):** two lines: "same shape as the real pile —" / "the detective can't tell anymore".
- **Caption (12px `#444`, bottom right):** "illustrative — 48 real and 48 generated notes".

## When the Forger Prints Only One Note

**Tags:** `common mistake` (red), `mode collapse` (orange)

- **50% is success** — a detective stuck at coin-flip accuracy means the forger won, not that training failed
- **Mode collapse** — the forger finds one note that fools the detective and prints only that, forever
- **Two designs** — real notes come in two ink styles, near 55% and 68%; a collapsed forger makes only one
- **Looks fine per note** — each fake is individually perfect; the pile is wrong because it has no variety
- **The tell** — compare whole distributions, not single samples: a spike where the real data has two hills

*Example (italic):* Every fake note looks flawless on its own, yet the batch holds only the 55%-ink design while real cash comes in two.

**Common mistake:** Judging a GAN by individual samples. Perfect-looking fakes can hide mode collapse — always compare the full distribution of outputs against the real data.

### Visualization (canvas `c4`, 720×300)

Overlaid density curves on the ink-coverage axis: the real notes' two-hump distribution versus a collapsed forger's single tall spike sitting on one hump only.

- **Title (bold 15px, `#1a5276`, top center):** "Mode Collapse: One Perfect Note, Zero Variety".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; x = ink coverage 40% to 80% with 12px `#444` tick labels "40%", "48%", "56%", "64%", "72%", "80%"; y = unlabeled density 0 to 5 with light `#e5e9ef` gridlines at 1, 2, 3, 4.
- **X grid for the real curve:** `[40, 44, 48, 52, 56, 60, 64, 68, 72, 76, 80]`.
- **Real curve:** blue `#2a78d6` 3px line, density `[0.2, 0.8, 2.0, 3.1, 3.2, 1.6, 2.2, 3.1, 2.0, 0.7, 0.2]`; fill under `rgba(42,120,214,0.15)`; 12px blue label "real notes: two designs" above the left hump (near x=52).
- **Collapsed forger curve:** magenta `#d55181` 3px line on its own x grid `[40, 44, 48, 51, 55, 59, 64, 68, 72, 76, 80]`, density `[0, 0, 0.2, 3.8, 4.8, 0.6, 0, 0, 0, 0, 0]` (peak 4.8 at x=55, on the design A line); bold 12px magenta label "collapsed forger" above the spike peak (at x=55).
- **Design markers:** two vertical dashed `#6b7280` (dash 4/3) lines from baseline to y=70 at ink 55 and 68, with 11px `#6b7280` labels "design A (55%)" and "design B (68%)" at their tops.
- **Annotation (bold 13px red `#e74c3c`, near x=66, y=140):** two lines: "design B never learned —" / "check distributions, not single notes".
- **Caption (12px `#444`, bottom right):** "illustrative densities".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all series are the hardcoded literal arrays above (no randomness); forger ink follows "close 40% of the gap to 60, rounded" and detective catch rate follows "50 + gap", so text and chart numbers agree; histogram counts each sum to 48; density curves are invented and captioned illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
