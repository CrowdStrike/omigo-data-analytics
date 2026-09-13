# Maximum Entropy Models

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Maximum Entropy Models

**Subtitle:** When all you know is a few facts, the honest guess is the flattest distribution that still matches those facts — a maximum entropy model is exactly that guess, written as a formula

## Guessing the Next Order at the Counter

**Tags:** `core idea` (blue), `honest guessing` (green), `no extra assumptions` (orange)

- **The counter** — a new barista must guess what the next customer will order: latte, espresso, tea, or hot chocolate
- **Knowing nothing** — with zero facts about this shop, the only honest guess is 25% for each of the four drinks
- **The temptation** — guessing "70% latte" feels confident but invents a fact nobody gave the barista
- **The rule** — spread belief as evenly as the known facts allow; never sneak in facts you don't have
- **The name** — "entropy" measures how spread-out a guess is; maximizing it means staying maximally flat

*Example (italic):* Asked for odds on the very first order with no history at all, the barista says 25/25/25/25 — any other answer claims knowledge the barista doesn't have.

**Key point:** A maximum entropy model picks the most spread-out distribution consistent with the facts — with no facts at all, that is the flat 25% each.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart over the four drinks: the honest know-nothing guess (flat 25% bars) next to an overconfident invented guess, with a dashed line at the uniform level.

- **Title (bold 15px, `#1a5276`, top center):** "No Facts Yet: the Honest Guess Is Flat — 25% Each".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = probability 0 to 80% with light `#e5e9ef` gridlines and 12px `#444` labels at 20%, 40%, 60%; x = four drink groups centered at x = 150, 300, 450, 600 with 12px `#444` labels "latte", "espresso", "tea", "hot choc".
- **Bars (two per group, 38px wide, 6px gap):** honest guess blue `#2a78d6` at heights `[25, 25, 25, 25]`; overconfident guess mute `#6b7280` at 40% opacity at heights `[70, 10, 10, 10]`; bold 12px value labels above each bar ("25%" in blue, "70%"/"10%" in `#6b7280`).
- **Uniform line:** horizontal dashed blue (dash 4/3) line at the 25% level across the plot; 12px blue label "flat = maximum entropy" at its right end.
- **Legend (12px, top right):** blue swatch "knows nothing (honest)", grey swatch "invented confidence".
- **Annotation (bold 12px orange `#d95926`, near x=430, y=95):** two lines: "the grey guess invents a fact:" / "nobody said lattes dominate".
- **Caption (12px `#444`, bottom right):** "illustrative — a brand-new shop with no order history".

## One Fact Arrives: Coffee Is 60% of Orders

**Tags:** `worked example` (blue), `constraints` (green)

- **The fact** — the owner mentions one number: 60% of all orders are coffee drinks (latte or espresso)
- **Many guesses fit** — 30/30/20/20, 45/15/25/15, even 60/0/40/0 all satisfy "coffee = 60%"
- **Measure flatness** — their entropies are 1.97 bits, 1.84 bits, and 0.97 bits respectively
- **The winner** — MaxEnt picks 30/30/20/20: split 60% evenly inside coffee, 40% evenly outside
- **Check by hand** — 30+30 = 60% coffee, and no drink is favored beyond what the fact forces
- **Silent where facts are silent** — latte vs espresso got no fact, so they stay equal at 30% each

*Example (italic):* Given only "coffee = 60%", the barista answers latte 30%, espresso 30%, tea 20%, hot chocolate 20% — the flattest table that respects the one known fact.

**Key point:** Every distribution matching the facts is a candidate; the maximum entropy model is the one with the highest entropy — here 30/30/20/20 at 1.97 bits.

### Visualization (canvas `c2`, 720×300)

Three mini bar clusters on one canvas, one per candidate distribution that satisfies "coffee = 60%", each labeled with its entropy; the flattest (MaxEnt) cluster highlighted green.

- **Title (bold 15px, `#1a5276`, top center):** "Three Guesses All Match 'Coffee = 60%' — MaxEnt Picks the Flattest".
- **Layout:** three clusters centered at x = 160, 380, 600; shared baseline y=235, bar scale 0–70% mapped to 150px height; each cluster has four 34px bars (latte, espresso, tea, hot choc) with 11px `#444` letter labels "L", "E", "T", "H" below.
- **Cluster A (MaxEnt winner):** heights `[30, 30, 20, 20]`, green `#008300` fill; bold 13px green header above at y=58: "A: 30/30/20/20"; bold 13px green entropy label below at y=278: "1.97 bits — flattest".
- **Cluster B:** heights `[45, 15, 25, 15]`, blue `#2a78d6` at 55% opacity; 13px `#444` header "B: 45/15/25/15"; 12px `#6b7280` entropy label "1.84 bits".
- **Cluster C:** heights `[60, 0, 40, 0]`, orange `#d95926` at 55% opacity; 13px `#444` header "C: 60/0/40/0"; 12px `#6b7280` entropy label "0.97 bits".
- **Value labels:** bold 11px above each bar in the cluster's color (e.g. "30", "30", "20", "20").
- **Winner box:** 1.5px dashed green rounded rectangle around cluster A.
- **Annotation (bold 12px violet `#4a3aa7`, centered near x=490, y=45):** "all three satisfy the fact — only A adds nothing extra".
- **Caption (12px `#444`, bottom right):** "entropy in bits, computed from the shown percentages".

## From Coffee Orders to Tagging Words

**Tags:** `where it's used` (blue), `features` (green), `softmax` (orange)

- **Same recipe in NLP** — replace drinks with word tags: is "run" acting as a verb or a noun in a sentence?
- **Facts become features** — clues like "the previous word is 'to'" enter the model as weighted facts
- **The formula** — MaxEnt with feature facts works out to a softmax over weighted feature sums
- **One clue, big move** — no clues gives 50/50; a weight of 2.0 on the 'to'-clue gives e² / (e² + 1) ≈ 88% verb
- **The classic uses** — part-of-speech tagging, text classification, and ranking clicked results in search
- **Another name** — trained on data, a MaxEnt classifier is exactly multinomial logistic regression

*Example (italic):* In "I want to run", the single clue "follows 'to'" moves the verb guess from 50% to 88% — the model stays flat until a feature says otherwise.

**Key point:** MaxEnt turns each clue into a constraint, and the flattest distribution matching all clues comes out as a softmax — the workhorse behind classic NLP taggers and rankers.

### Visualization (canvas `c3`, 720×300)

Before/after paired bar chart for tagging the word "run": verb vs noun probability with no features, then with the single feature "previous word is 'to'" at weight 2.0.

- **Title (bold 15px, `#1a5276`, top center):** "Tagging 'run': One Feature Moves the Flat 50/50 to 88/12".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = probability 0 to 100% with light `#e5e9ef` gridlines and 12px `#444` labels at 25%, 50%, 75%; two panel groups centered at x = 210 ("no features") and x = 510 ("+ feature: follows 'to', weight 2.0"), 13px `#444` group labels below the baseline.
- **Left group (no features):** two 55px bars, verb blue `#2a78d6` height 50, noun mute `#6b7280` height 50; bold 12px labels "verb 50%" and "noun 50%" above the bars.
- **Right group (with feature):** verb green `#008300` height 88, noun mute `#6b7280` height 12; bold 12px labels "verb 88%" (green) and "noun 12%".
- **Arrow:** 3px blue arrow from the top of the left verb bar to the top of the right verb bar, gentle upward curve, arrowhead at the right end.
- **Dashed line:** horizontal dashed `#6b7280` (dash 4/3) line at 50% across the plot, 11px `#6b7280` label "flat start" at its left end.
- **Annotation (bold 12px green `#008300`, near x=390, y=70):** two lines: "softmax: e² / (e² + 1) ≈ 0.88" / "one weighted clue did all the work".
- **Caption (12px `#444`, bottom right):** "illustrative feature weight — real weights are learned from tagged text".

## MaxEnt Doesn't Mean Everything Is Equal

**Tags:** `common mistake` (red), `strong facts` (orange)

- **The misreading** — people hear "maximum entropy" and expect the model to always answer 25% each
- **Strong fact, spiky answer** — told "coffee is 90% of orders", MaxEnt answers 45/45/5/5, far from flat
- **Still the flattest** — 45/45/5/5 (1.47 bits) is the most spread-out table that respects the 90% fact
- **Flat only in the gaps** — equality survives only where the facts said nothing: latte = espresso = 45%
- **The mistake** — calling MaxEnt "assuming uniformity"; it is uniformity only after the facts are paid

*Example (italic):* A reviewer objected that a MaxEnt tagger "assumes all tags equally likely" — yet with its trained features it put 96% on one tag; flatness applies only to what the features leave open.

**Common mistake:** Reading "maximum entropy" as "predicts uniform". The model matches every known fact exactly, however lopsided, and stays flat only in the directions the facts leave unspecified.

### Visualization (canvas `c4`, 720×300)

Single bar chart of the MaxEnt answer under the strong fact "coffee = 90%": lopsided 45/45/5/5 bars with the uniform 25% level shown dashed for contrast.

- **Title (bold 15px, `#1a5276`, top center):** "A Strong Fact ('Coffee = 90%') Makes the MaxEnt Answer Spiky: 45/45/5/5".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = probability 0 to 60% with light `#e5e9ef` gridlines and 12px `#444` labels at 15%, 30%, 45%; four bars centered at x = 150, 300, 450, 600 with 12px `#444` labels "latte", "espresso", "tea", "hot choc".
- **Bars (70px wide):** heights `[45, 45, 5, 5]`; latte and espresso blue `#2a78d6`, tea and hot choc orange `#d95926`; bold 13px value labels above each bar ("45%", "45%", "5%", "5%") in the bar's color.
- **Uniform line:** horizontal dashed `#6b7280` (dash 4/3) line at the 25% level; 12px `#6b7280` label "uniform 25% — NOT the answer here" at its right end above the line.
- **Equality braces:** thin 1.5px `#444` bracket over the two 45% bars with 11px `#444` label "equal — facts silent here", and a matching bracket over the two 5% bars labeled "equal — facts silent here".
- **Annotation (bold 13px green `#008300`, near x=430, y=85):** two lines: "still maximum entropy: 1.47 bits," / "the flattest table that pays the 90% fact".
- **Caption (12px `#444`, bottom right):** "illustrative — entropy computed from the shown percentages".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights and probabilities are the hardcoded arrays above (no randomness); entropies are Shannon entropy in bits of the shown percentages rounded to 2 decimals (1.97, 1.84, 0.97, 1.47); the softmax number is e²/(e²+1) ≈ 0.88.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
