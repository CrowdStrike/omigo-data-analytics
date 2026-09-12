# Self-Supervised & Contrastive Learning

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Self-Supervised & Contrastive Learning

**Subtitle:** When nobody has labeled the data, manufacture labels from the data itself — two edited copies of the same photo are "same", everything else is "different", and that is enough to learn from

## Five Thousand Pet Photos, Zero Labels

**Tags:** `core idea` (blue), `free labels` (green), `pretext task` (orange)

- **The pile** — a photo app holds 5,000 pet photos and nobody has ever typed "cat" or "dog" on any of them
- **The trick** — take one photo and make two edited copies of it: a corner crop and a gray version
- **Free label** — both copies came from one photo, so the label "same" exists without anyone writing it
- **The other side** — copies made from two different photos get the label "different", also for free
- **Self-supervised** — the data manufactured its own question and its own answer key; no human labeled anything

*Example (italic):* A beagle photo becomes a cropped beagle and a gray beagle — the model is told those two are "the same", and that a copy of a cat photo is not.

**Key point:** Self-supervised learning invents a task whose answers are already inside the data — here, "did these two edited copies come from the same original photo?"

### Visualization (canvas `c1`, 720×300)

Three-column flow diagram: two source photos on the left, their edited copies in the middle, and the two manufactured labels ("same" / "different") on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Photo, Two Copies — a Label Nobody Wrote".
- **Column 1 (source photos):** two rounded rects 130×55, 2px `#1a5276` border, fill `rgba(42,120,214,0.08)`, at x=30, y=70 ("beagle photo #1742") and x=30, y=185 ("cat photo #0088"); 12px `#2c3e50` centered labels.
- **Column 2 (edited copies):** three rounded rects 140×46, 1.5px `#2a78d6` border, at x=260: "beagle crop" (y=52), "beagle in gray" (y=112), "cat crop" (y=192); 12px labels; 2px `#6b7280` arrows from beagle box to the first two and from cat box to the third, 11px `#6b7280` labels "crop corner" / "drain color" / "crop corner" along the arrows.
- **Column 3 (manufactured labels):** green `#008300` 2.5px bracket joining "beagle crop" + "beagle in gray" at x≈420, ending in a pill labeled bold 12px green "SAME — pull together" near (540, 95); orange `#d95926` 2.5px bracket joining "beagle in gray" + "cat crop", pill labeled bold 12px orange "DIFFERENT — push apart" near (540, 190).
- **Annotation (bold 12px violet `#4a3aa7`, near x=470, y=262):** "labels manufactured by editing, not by humans".
- **Caption (12px `#444`, bottom right):** "illustrative — one positive pair, one negative pair".

## Scoring the Beagle Against Three Impostors

**Tags:** `worked example` (blue), `similarity scores` (green)

- **The setup** — anchor = the beagle crop; candidates: its own gray copy, a cat, a parrot, a goldfish
- **The scores** — the model rates each pair's similarity from 0 to 1; higher means "more alike"
- **Before training** — scores 0.26, 0.28, 0.31, 0.24: the parrot beats the beagle's own copy (0.31 vs 0.26)
- **The demand** — training pulls the true partner's score up and pushes the three impostors' scores down
- **After training** — 0.85 vs 0.20, 0.15, 0.10: the gray beagle wins over the best impostor by +0.65

*Example (italic):* Before training the beagle crop matched a parrot better than its own gray copy (0.31 vs 0.26); after training its own copy wins 0.85 to 0.20.

**Key point:** Contrastive learning is one demand repeated millions of times: the true partner must outscore every impostor — the gap here goes from −0.05 (fails) to +0.65 (passes).

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: four candidates on the x axis, each with a "before training" bar and an "after training" bar of the similarity score against the beagle crop.

- **Title (bold 15px, `#1a5276`, top center):** "Who Matches the Beagle Crop? Before vs After Training".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = similarity 0 to 1.0 with light `#e5e9ef` gridlines at 0.25, 0.5, 0.75 and 12px `#444` tick labels "0", "0.25", "0.5", "0.75", "1.0"; four group labels below the baseline, 12px `#444`: "gray beagle (true partner)", "cat", "parrot", "goldfish".
- **Bars:** per group two bars 34px wide, 8px apart; before = fill `rgba(107,114,128,0.45)` with values `[0.26, 0.28, 0.31, 0.24]`; after = green `#008300` fill 0.85 for the true partner, blue `#2a78d6` fill `[0.20, 0.15, 0.10]` for the three impostors; 12px value label above every bar.
- **Impostor note (bold 12px orange `#d95926`, above the parrot's before bar):** "before: a parrot wins (0.31)".
- **Annotation (bold 12px green `#008300`, near x=180, y=70):** two lines: "after training the true partner wins" / "0.85 vs 0.20 — a +0.65 gap".
- **Legend (12px, top right):** gray swatch "before training", green/blue swatch "after training".
- **Caption (12px `#444`, bottom right):** "illustrative similarity scores".

## Why Bother: 100 Labels Instead of 5,000

**Tags:** `where it's used` (blue), `pretraining` (green), `label cost` (orange)

- **Label bills** — real labels need paid human time; labeling every photo in a big archive is a big invoice
- **Pretrain free** — contrastive pretraining eats all 5,000 unlabeled photos and learns what "alike" means
- **Fine-tune cheap** — afterwards a handful of real labels is enough to name the groups it already formed
- **The payoff** — with only 100 labeled photos: 76% accuracy pretrained vs 58% trained from scratch
- **Everywhere** — the same recipe pretrains modern image, speech, and text models before any task labels

*Example (italic):* The team labels just 100 photos over one coffee break and still reaches 76%, because the unlabeled 5,000 already taught the model what makes two pets look alike.

**Key point:** Self-supervision turns a mountain of unlabeled data into a head start, so the expensive labeled data only has to finish the job instead of doing all of it.

### Visualization (canvas `c3`, 720×300)

Two-line chart: cat-vs-dog test accuracy against the number of labeled photos used for fine-tuning, pretrained model vs from-scratch model.

- **Title (bold 15px, `#1a5276`, top center):** "Accuracy vs Labeled Photos: the Pretrained Head Start".
- **Axes:** origin x=70, baseline y=240, plot width 580, plot height 175; y = accuracy 50% to 95% with light `#e5e9ef` gridlines at 60, 70, 80, 90 and 12px `#444` tick labels "50%"–"90%"; x = six evenly spaced ticks labeled "50", "100", "200", "500", "1000", "2000" (12px `#444`); 12px `#6b7280` axis caption "labeled photos used for fine-tuning" centered below.
- **Pretrained line:** green `#008300` 3px line with 5px dots, values `[71, 76, 81, 86, 89, 91]`; 12px green label "contrastive pretrained" at the right end.
- **From-scratch line:** blue `#2a78d6` 3px line with 5px dots, values `[52, 58, 64, 72, 79, 84]`; 12px blue label "from scratch" at the right end.
- **Marker:** vertical dashed `#6b7280` (dash 4/3) line at the "100" tick from baseline to y=70.
- **Annotation (bold 12px green `#008300`, near the "100" tick, y=85):** two lines: "100 labels:" / "76% vs 58%".
- **Caption (12px `#444`, bottom right):** "illustrative — same model, only the starting point differs".

## The Collapse Trap: Why Pushing Apart Matters

**Tags:** `common mistake` (red), `collapse` (orange)

- **Tempting shortcut** — "just pull the two copies together" sounds like enough; the push feels optional
- **The cheat** — a model can score every pair a perfect 1.0 by giving every photo the exact same code
- **Collapse** — all 5,000 photos land on one point; the pull task is aced and nothing was learned
- **The fix** — pushing different photos apart makes the one-point cheat impossible to get away with
- **The reward** — with both forces, cats, dogs, and birds drift into separate groups no human named

*Example (italic):* A pull-only model gets a flawless training score while mapping a beagle, a cat, and a goldfish to the identical point — perfect grade, useless map.

**Common mistake:** Training only on "pull the pair together". The model collapses everything to one point, earns a perfect score, and learns nothing — the push against impostors is what keeps the task honest.

### Visualization (canvas `c4`, 720×300)

Two side-by-side scatter panels of the learned 2-D codes: left panel shows collapse (all points in one blob), right panel shows the spread clusters that pull + push produces.

- **Title (bold 15px, `#1a5276`, top center):** "Pull Alone Collapses; Pull + Push Spreads the Map".
- **Panels:** two rects with 1px `#e5e9ef` border — left x=50 to 330, y=55 to 255; right x=390 to 670, y=55 to 255; panel subtitles bold 13px above each: left orange `#d95926` "pull only — collapse", right green `#008300` "pull + push — real structure".
- **Left points:** twelve 5px blue `#2a78d6` dots hardcoded at `[[188,152],[192,148],[185,155],[190,150],[194,153],[187,149],[191,156],[186,151],[193,147],[189,154],[184,150],[195,152]]` — one tight blob.
- **Right points:** three clusters of four 5px dots — blue `#2a78d6` at `[[445,105],[458,98],[440,120],[462,115]]`, green `#008300` at `[[552,193],[568,188],[548,207],[572,205]]`, orange `#d95926` at `[[622,84],[638,80],[618,96],[640,98]]`; 11px `#6b7280` cluster labels "cats", "dogs", "birds" beside each cluster.
- **Annotation (bold 12px orange `#d95926`, left panel, centered near y=210):** two lines: "every photo → same point" / "perfect pull score, zero information".
- **Right note (bold 12px green `#008300`, right panel, near x=470, y=240):** "groups appear with zero human labels".
- **Caption (12px `#444`, bottom right):** "illustrative 2-D map of the learned codes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, line values, and scatter coordinates are the hardcoded arrays above (no `Math.random()`); every number quoted in the text (0.26/0.28/0.31/0.24, 0.85/0.20/0.15/0.10, −0.05/+0.65, 76% vs 58% at 100 labels) must match the chart data exactly; invented numbers keep their "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
