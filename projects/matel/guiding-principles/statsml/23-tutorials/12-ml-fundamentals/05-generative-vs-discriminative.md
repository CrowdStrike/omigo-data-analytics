# Generative vs Discriminative

**Page type:** detail page (tutorial: 4 card-sections; sections 1, 3, 4 two-column table.layout 45/55, section 2 three-column 38/31/31 with two canvases)
**HTML title tag:** Generative vs Discriminative

**Subtitle:** Two ways to build a classifier: learn what each class looks like and ask which is more likely, or skip that and learn only the dividing line between them

## Learn the Classes, or Learn the Border

Tags: `core idea` (blue), `running example` (green)

- **The task** — sort emails into spam vs legit using two word counts: "free" and "meeting"
- **Generative** — learns P(words | class) for each class plus class frequency, then uses Bayes rule
- **The name** — it models how each class "generates" its data: what a typical spam looks like
- **Discriminative** — learns P(class | words) or just the boundary; never models the classes
- **Analogy** — to tell two languages apart: learn both deeply, or just learn the telltale differences

*Example (italic):* A spam filter can memorize what spam looks like, or only memorize what separates spam from legit.

**Key point:** Generative models each class and asks "which is more likely to have produced this?"; discriminative models only the line between them.

### Visualization (canvas `c1`, 720×300)

Side-by-side panels: the same 10 emails, once with a fitted distribution per class, once with only a boundary line.

- **Title (bold 15px, `#1a5276`, top center):** "Two Roads to the Same Classification"
- **Divider:** vertical dashed gray line (`#bdc3c7`, dash 4/3) from y=34 to y=288 at x=360.
- **Shared data (illustrative):** 10 emails; spam S1–S5 at ("free", "meeting") counts (3,0), (2,1), (4,0), (3,1), (2,0); legit L1–L5 at (0,3), (1,2), (0,4), (1,3), (0,2).
- **Panels:** left plot area x 55–335, right x 415–695; y 58 (top) to 232 (bottom); both axes map value range −0.5..5.2; L-shaped gray `#999` axes; axis caption (11px `#6b7280`, centered under each panel at y=246): "\"free\" count (x), \"meeting\" count (y)".
- **Points:** 7px dots; spam in orange `#d95926`, legit in blue `#2a78d6` (both panels).
- **Left panel extras:** two nested dashed orange ellipses (`#d95926`, dash 5/4, width 2) around the spam cloud, centered at value (2.8, 0.4), pixel radii 52×28 and 30×16; two nested dashed blue ellipses (`#2a78d6`) around the legit cloud, centered at (0.4, 2.8), radii 30×46 and 17×27; bold 12px labels: orange "P(words | spam)" right of the spam cloud, blue "P(words | legit)" above-right of the legit cloud.
- **Right panel extras:** one dashed violet diagonal boundary (`#4a3aa7`, dash 6/4, width 2.5) along meeting=free from value (−0.5,−0.5) to (5.2,5.2); bold 12px violet label near the top of the line: "the border is all it learns".
- **Footers (bold 13px, centered at y=264, 12px `#444` sub-line at y=282):** left in aqua `#199e70`: "GENERATIVE: fit each class, compare" / "which class more likely produced this email?"; right in violet `#4a3aa7`: "DISCRIMINATIVE: fit only the border" / "which side of the line does it land on?".

## Ten Emails, Scored by Hand

Tags: `worked example` (green)

- **Count** — spam emails use "free" 14 times and "meeting" twice; legit is the exact mirror
- **Per-class rates** — P("free" | spam) = 14/16 = .875, P("meeting" | spam) = 2/16 = .125
- **Priors** — 5 spam and 5 legit out of 10 emails, so each class starts at .5
- **New email** — "free free meeting": spam .5 × .875 × .875 × .125 ≈ .048 vs legit ≈ .007
- **Verdict** — spam is about 7× more likely; that whole recipe is Naive Bayes, a generative model
- **Discriminative** — logistic regression skips the counting and fits one line: free > meeting → spam

*Example (italic):* You can redo the entire spam score on paper — three multiplications per class.

**Key point:** Generative computes "how likely would each class produce this email?"; discriminative just checks which side of the line it lands on.

### Visualization (canvas `c2a`, 420×340)

Naive Bayes computation panel: rate table, new email, two score lines, verdict.

- **Title (bold 15px, `#1a5276`, top center):** "Naive Bayes: count and multiply"
- **Rate table** (x0=60, y0=64, column widths 100/100/100, row height 24): header row bold 12px — "word" in ink `#1a5276`, "spam" in orange `#d55926`-family orange `#d95926`, "legit" in blue `#2a78d6`; rows "\"free\"" → "14/16 = .875" / "2/16 = .125" and "\"meeting\"" → "2/16 = .125" / "14/16 = .875" (cells 12px `#333`, zebra `#f4f7fa`/white, border `#c9d4de`).
- **Priors line (12px `#444`, centered, y=158):** "priors: 5 spam, 5 legit out of 10 → .5 each"
- **New email box:** dashed magenta rectangle (`#d55181`, dash 5/4) 280×34 centered at x=w/2, y=176; bold 13px magenta text centered inside: "new email: \"free free meeting\""
- **Score lines (bold 13px, centered):** orange `#d95926` at y=240: "spam: .5 × .875 × .875 × .125 ≈ .048"; blue `#2a78d6` at y=264: "legit: .5 × .125 × .125 × .875 ≈ .007"
- **Verdict (bold 13px green `#008300`, centered, y=296):** "spam ≈ 7× more likely → SPAM"
- **Caption (11px `#6b7280`, centered, y=326):** "illustrative counts from the 10 training emails"

### Visualization (canvas `c2b`, 400×340)

Scatter of the 10 emails with the logistic-regression boundary line.

- **Title (bold 15px, `#1a5276`, top center):** "Logistic regression: just the line"
- **Axes:** x = "free" count, y = "meeting" count, both mapped over −0.5..5.2; padding top 46 / bottom 58 / left 52 / right 18; L-shaped gray `#999` axis.
- **Points:** the 10 emails, 7px dots — spam orange `#d95926`, legit blue `#2a78d6`.
- **Boundary:** dashed violet diagonal (`#4a3aa7`, dash 6/4, width 2.5) along meeting=free from (−0.5,−0.5) to (5.2,5.2); bold 12px violet label lower-right of the line: "free > meeting → spam".
- **In-plot labels (bold 12px):** orange "spam" near the spam cloud, blue "legit" near the legit cloud.
- **Caption (bold 12px green `#008300`, centered below axis at y=304):** "same verdict, no class model at all"
- **Axis caption (12px `#444`, centered, y=328):** "\"free\" count (x), \"meeting\" count (y)"

## Two Families and Their Sweet Spots

Tags: `where it's used` (blue), `rule of thumb` (blue)

- **Generative family** — Naive Bayes, LDA, Gaussian mixture models, hidden Markov models
- **Also generative** — LLMs and diffusion models: they generate data, by construction
- **Discriminative family** — logistic regression, SVMs, tree ensembles, most neural classifiers
- **Prefer generative** — small training sets, missing feature values, need to simulate samples
- **Prefer discriminative** — plenty of labels and raw classification accuracy is all that matters
- **Why it wins** — every bit of capacity goes to the border, with fewer assumptions about the data

*Example (italic):* Early spam filters were Naive Bayes; with millions of labeled emails, boosted trees and neural classifiers took over.

**Key point:** With little data or missing values, model the classes; with lots of labels, model the boundary.

### Visualization (canvas `c3`, 720×300)

Two-column comparison board: family members and sweet spots per approach.

- **Title (bold 15px, `#1a5276`, top center):** "Two Families, Two Sweet Spots"
- **Left box** (300×172 at x=45, y=52, aqua `#199e70` border width 2, fill `rgba(0,0,0,0.02)`): bold 13px aqua title "GENERATIVE — learn each class" (y offset 26); two 12px `#333` member lines: "Naive Bayes · LDA · Gaussian mixtures" (y 52) and "hidden Markov models · LLMs · diffusion" (y 72); two bold 12px aqua sweet-spot lines: "shines: small data, missing values," (y 108) and "simulating samples, spotting outliers" (y 128); 11px `#6b7280` line "can also generate new samples" (y 154).
- **Right box** (300×172 at x=375, violet `#4a3aa7`, same layout): "DISCRIMINATIVE — learn the border"; members "logistic regression · SVMs · tree ensembles" and "gradient boosting · most neural classifiers"; sweet spots "shines: raw accuracy when labeled" and "data is plentiful — all effort on the line"; footnote "cannot generate; classify only".
- **Bottom caption (bold 13px orange `#d95926`, centered, y=268):** "more labeled data → the boundary specialists usually pull ahead on accuracy"

## Not That Kind of "Generative"

Tags: `common mistake` (red)

- **Not chatbots** — "generative" here means modeling P(words | class), not writing text
- **One-way street** — a generative classifier can also generate samples; a discriminative one cannot
- **Weird inputs** — generative can say "low probability under BOTH classes — looks like neither"
- **Forced choice** — discriminative always picks a side, even for garbage, often with high confidence
- **The overlap** — today's LLMs and diffusion models are generative in exactly this classic sense

*Example (italic):* An email in another language: Naive Bayes scores near zero for both classes; logistic regression still answers "spam, 97%".

**Common mistake (key-point callout):** "Generative" names how the model is built, not what product it powers — and only generative models can notice an input that looks like nothing they trained on.

### Visualization (canvas `c4`, 720×300)

Side-by-side panels: an outlier email far from both classes, judged by each approach.

- **Title (bold 15px, `#1a5276`, top center):** "One Weird Email, Two Very Different Answers"
- **Divider:** vertical dashed gray line (`#bdc3c7`, dash 4/3) from y=34 to y=288 at x=360.
- **Panels:** same geometry and axes as `c1` (left x 55–335, right x 415–695, y 58–232, value range −0.5..5.2); same 10 training points at 5px radius (spam orange, legit blue).
- **Outlier:** bold magenta `#d55181` 8px dot at value (4.8, 3.4) in BOTH panels, with a bold 11px magenta "new?" label above it.
- **Left panel extras:** one dashed orange ellipse around the spam cloud (center (2.8, 0.4), radii 52×28) and one dashed blue ellipse around the legit cloud (center (0.4, 2.8), radii 30×46), width 2, dash 5/4; bold 12px aqua `#199e70` two-line callout left of the outlier: "low prob under BOTH →" / "\"looks like neither\"".
- **Right panel extras:** dashed violet boundary along meeting=free from (−0.5,−0.5) to (5.2,5.2); bold 12px red `#e74c3c` two-line callout left of the outlier: "\"SPAM, 97% confident\"" / "no warning at all".
- **Footers (bold 13px, centered at y=264, 12px `#444` sub-line at y=282):** left in aqua `#199e70`: "GENERATIVE: can flag the outlier" / "it knows what its classes look like"; right in red `#e74c3c`: "DISCRIMINATIVE: forced to pick a side" / "the line splits the whole plane — no \"neither\"".

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (most-powerful-signals compact style). Each `.card-section` has an `<h2>` (1.3rem `#1a5276`, 2px bottom border `#2980b9`) and a `table.layout`; standard rows use `.text-col` (50%) / `.viz-col` (50%); the two-chart row uses `.text-col3` (38%) with two `.viz-col3` cells (31% each).
- **Left column per section:** `.tags` pill row first (0.72rem bold, 10px radius pills — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`), then a `<ul>` of one-line bullets each opening with `<b>` term in `#1a5276`, then an italic `.example` line (`#555`, 0.9rem), then a `.key-point` callout (background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes as given per chart (720×300, 420×340, 400×340), CSS `width:100%`, 1px border `#e0e0e0` radius 4px; scaled via `window.devicePixelRatio` in a shared `setup(id)` helper reading width/height attributes (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All charts pushed into a `__charts` array, drawn once, and redrawn on debounced window resize (150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Data:** shared literal arrays IDS `['S1'..'S5','L1'..'L5']`, FREE `[3,2,4,3,2,0,1,0,1,0]`, MEET `[0,1,0,1,0,3,2,4,3,2]`, SPAM flag `[true×5, false×5]` used by all four charts; spam word totals free=14/meeting=2 of 16, legit the mirror; no `Math.random()`; invented numbers labeled "illustrative".
- In regenerated HTML, any card links use `.html` extensions.
