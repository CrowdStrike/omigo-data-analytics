# Causal Thinking & Experiments

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Causal Thinking & Experiments

**Subtitle:** How to tell whether something actually caused a change — and how experiments settle the question when watching alone can't.

## Cards

Each card links to a topic page under `causal-experimentation/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | WHY EXPERIMENTS | Observational vs Experimental Data | [08-causal-thinking-and-experiments/01-observational-vs-experimental-data.md](08-causal-thinking-and-experiments/01-observational-vs-experimental-data.md) | Watching what people already do versus changing something on purpose — and why only one of these can prove cause. | watching vs doing, correlation, causation |
| 2 | WHY EXPERIMENTS | Randomized Controlled Trials | [08-causal-thinking-and-experiments/02-randomized-controlled-trials.md](08-causal-thinking-and-experiments/02-randomized-controlled-trials.md) | The gold standard: flip a coin to decide who gets the treatment, then compare the two groups fairly. | gold standard, clinical trials, fair comparison |
| 3 | WHY EXPERIMENTS | Treatment & Control Groups | [08-causal-thinking-and-experiments/03-treatment-and-control-groups.md](08-causal-thinking-and-experiments/03-treatment-and-control-groups.md) | One group gets the change, the other doesn't — the untouched group tells you what would have happened anyway. | baseline, comparison group, holdout |
| 4 | WHY EXPERIMENTS | Why Randomization Works | [08-causal-thinking-and-experiments/04-why-randomization-works.md](08-causal-thinking-and-experiments/04-why-randomization-works.md) | Random assignment balances out everything you know about people — and everything you don't — across both groups. | coin flip, hidden factors, balance |
| 5 | A/B TESTING BASICS | What an A/B Test Is | [08-causal-thinking-and-experiments/05-what-an-a-b-test-is.md](08-causal-thinking-and-experiments/05-what-an-a-b-test-is.md) | Show version A to some users and version B to others, at random, and see which one does better. | two versions, random split, online experiments |
| 6 | A/B TESTING BASICS | Choosing a Metric | [08-causal-thinking-and-experiments/06-choosing-a-metric.md](08-causal-thinking-and-experiments/06-choosing-a-metric.md) | Deciding what "better" means before you start — one number that captures what you actually care about. | success measure, north star, proxy metrics |
| 7 | A/B TESTING BASICS | Sample Size & Duration | [08-causal-thinking-and-experiments/07-sample-size-and-duration.md](08-causal-thinking-and-experiments/07-sample-size-and-duration.md) | How many users and how many days you need before the result means anything — decided up front, not on the fly. | enough data, power, run length |
| 8 | A/B TESTING BASICS | Reading the Results | [08-causal-thinking-and-experiments/08-reading-the-results.md](08-causal-thinking-and-experiments/08-reading-the-results.md) | What the lift, the uncertainty range, and "statistically significant" actually tell you — and what they don't. | lift, significance, uncertainty |
| 9 | CAUSAL REASONING | Counterfactuals | [08-causal-thinking-and-experiments/09-counterfactuals.md](08-causal-thinking-and-experiments/09-counterfactuals.md) | The "what would have happened otherwise" question at the heart of every causal claim — and why it's never directly observable. | what if, unseen outcome, potential outcomes |
| 10 | CAUSAL REASONING | Confounding | [08-causal-thinking-and-experiments/10-confounding.md](08-causal-thinking-and-experiments/10-confounding.md) | A hidden third factor that drives both things you're comparing, making an innocent pattern look like cause and effect. | hidden factor, lurking variable, spurious link |
| 11 | CAUSAL REASONING | Selection Effects | [08-causal-thinking-and-experiments/11-selection-effects.md](08-causal-thinking-and-experiments/11-selection-effects.md) | When the people who end up in your data got there in a non-random way, the comparison is rigged before you start. | who shows up, biased sample, self-selection |
| 12 | CAUSAL REASONING | Natural Experiments | [08-causal-thinking-and-experiments/12-natural-experiments.md](08-causal-thinking-and-experiments/12-natural-experiments.md) | Sometimes the world randomizes for you — a policy cutoff, a lottery, a storm — and you can borrow that luck to infer cause. | accidental randomization, policy cutoffs, quasi-experiments |
| 13 | CAUSAL REASONING | Instrumental Variables | [08-causal-thinking-and-experiments/13-instrumental-variables.md](08-causal-thinking-and-experiments/13-instrumental-variables.md) | A clever workaround: use something that nudges the treatment but touches nothing else to isolate the causal effect. | nudge variable, indirect lever, encouragement |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "WHY EXPERIMENTS" `#2980b9`, "A/B TESTING BASICS" `#27ae60`, "CAUSAL REASONING" `#8e44ad`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
