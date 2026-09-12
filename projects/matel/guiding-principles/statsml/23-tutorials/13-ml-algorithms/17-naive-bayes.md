# Naive Bayes

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table layout text left 50% / canvas right 50%)
**HTML title tag:** Naive Bayes

**Subtitle:** Flip prediction around: instead of asking "is this spam?", ask "how likely are these words if it were spam, and if it weren't?" — then compare

## A Spam Filter That Only Counts Words

Tags: `core idea` (blue), `Bayes rule` (green), `spam filters` (orange)

- **The training data** — 100 known spam emails and 100 known normal emails, nothing else
- **The counting** — "free" shows up in 60 of the spam emails but only 10 of the normal ones
- **More counts** — "prize": 40 spam vs 5 normal; "meeting": 5 spam vs 50 normal
- **The flip** — don't ask "is it spam?"; ask "which pile makes these words least surprising?"
- **That's the model** — a table of word counts per class is the entire thing Naive Bayes learns

*Example (italic):* An email saying "free prize" looks routine in the spam pile and bizarre in the normal pile.

**Key point:** **Bayes' flip:** we want P(spam given words), but words-given-class is what we can count. Bayes' rule lets us compute the one we want from the one we can count.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart of word frequency in spam vs normal emails.

- **Title (bold 15px `#1a5276`, center):** "How Often Each Word Appears (100 spam vs 100 normal emails)".
- **Data:** words `"free"`, `"prize"`, `"meeting"`, `"lunch"`; spam counts `[60, 40, 5, 3]` in orange `#d95926`; normal counts `[10, 5, 50, 35]` in blue `#2a78d6`. Bar width 34, pairs centered per group.
- **Axes:** gray `#999` L-axes; y scale max 70 (no tick labels); word labels bold 12px `#222` under groups. Padding: top 55, bottom 55, left 60, right 160.
- **Value labels:** 12px `#444` count above each bar.
- **X-axis caption (12px `#444`, center):** "emails containing the word (out of 100 per class)".
- **Legend (right side, x = w−148):** orange swatch "spam pile", blue swatch "normal pile" (12px `#222`).
- **Annotation (bold 13px orange, under legend):** 'each word "votes"' / 'for one pile'.

## Scoring "Free Prize" by Hand

Tags: `worked example` (green), `arithmetic` (blue)

- **Start with the prior** — half our training emails are spam, so each class starts at 0.50
- **Multiply per word** — spam score: 0.50 × 0.60 (free) × 0.40 (prize) = 0.120
- **Same for normal** — 0.50 × 0.10 (free) × 0.05 (prize) = 0.0025
- **Compare** — 0.120 vs 0.0025: the spam story is 48 times more plausible
- **Turn into a probability** — 0.120 / (0.120 + 0.0025) ≈ 0.98, so 98% spam

*Example (italic):* Three multiplications and one division — you can redo the whole prediction on paper.

**Key point:** **The "naive" part:** multiplying 0.60 × 0.40 pretends "free" and "prize" appear independently. They don't — but the ranking of the two scores usually survives the lie.

### Visualization (canvas `c2`, 720×300)

Diagram: two horizontal multiplication chains of boxes, one per class.

- **Title (bold 15px `#1a5276`, center):** 'Two Competing Stories for the Email "free prize"'.
- **Chain layout:** three boxes per row (118×46 px, fill `#f8f9fa`, 2px colored border), starting x=95, gap 52, "×" (bold 16px, chain color) between boxes, "= result" (bold 16px) at right; class label bold 13px at left of each row. Box contents: bold 13px value on top line, 12px gray `#6b7280` sublabel below.
- **Spam chain (y=60, orange `#d95926`):** label "spam"; boxes `0.50` / "prior", `0.60` / "P(free|spam)", `0.40` / "P(prize|spam)"; result "= 0.120".
- **Normal chain (y=150, blue `#2a78d6`):** label "normal"; boxes `0.50` / "prior", `0.10` / "P(free|norm)", `0.05` / "P(prize|norm)"; result "= 0.0025".
- **Conclusion (bold 14px green `#008300`, center, y=248):** "0.120 / (0.120 + 0.0025) ≈ 98% spam — the spam story is 48× more plausible".
- **Footnote (12px `#6b7280`, center, y=272):** "each word multiplies the score — that multiplication is the independence pretence".

## Why a Tiny Model Earns Its Keep

Tags: `where it's used` (blue), `rule of thumb` (green)

- **Small data** — counting words needs far fewer examples than fitting millions of weights
- **Fast baseline** — trains in one pass over the data; a standard first model for any text task
- **Real jobs** — spam filters, support-ticket routing, language detection, sentiment tagging
- **Great at ranking** — the winner is usually right even when the 98% is overconfident
- **Without it** — teams jump to heavy models and never learn word counts already solve the task

*Example (italic):* With only 50 labeled emails, the count-based filter already beats guessing by a wide margin.

**Key point:** **Rule of thumb:** run Naive Bayes first. If a big model can't clearly beat this afternoon-sized baseline, the extra complexity is not paying rent.

### Visualization (canvas `c3`, 720×300)

Two-series line chart: accuracy vs training-set size, Naive Bayes vs a heavy model.

- **Title (bold 15px `#1a5276`, center):** "Accuracy vs Labeled Emails Available (illustrative)".
- **X-axis:** 5 evenly spaced points labeled `50`, `200`, `1k`, `5k`, `20k`; caption "labeled training emails" (12px `#444`). Padding: top 55, bottom 55, left 65, right 175.
- **Y-axis:** 50% to 100%, gridlines (`#e5e9ef`) and labels at 50/60/70/80/90/100% (12px `#666`).
- **Series (line width 3, 4px dots):** Naive Bayes `[72, 84, 89, 91, 92]` in green `#008300`; heavy model `[55, 70, 85, 92, 94]` in violet `#4a3aa7`.
- **Annotation (bold 13px green, near the 200-email point):** "NB: 84% from just 200 emails".
- **Legend (right, x = w−162):** green swatch "Naive Bayes", violet swatch "heavy model" (12px `#222`).

## The Direction Trap: P(word | spam) Is Not P(spam | word)

Tags: `common mistake` (red), `base rates` (blue)

- **The two questions** — "how often does spam say free?" vs "how often is a free-email spam?"
- **They differ** — 60% of spam says "free", yet a "free" email here is only 40% likely spam
- **Why: base rates** — in an inbox of 1,000 with 10% spam, 100 are spam and 900 are normal
- **Count it out** — "free" appears in 60 spam (60% of 100) and 90 normal (10% of 900)
- **So among "free" emails** — 60 of 150 are spam: 40%, not 60%

*Example (italic):* Swapping the two directions is the same slip as "most sharks attack in shallow water, so shallow water is dangerous."

**Key point:** **The prior is not decoration:** Bayes' rule exists exactly to convert word-given-class into class-given-word — and the base rate is what does the converting.

### Visualization (canvas `c4`, 720×300)

Diagram: two stacked-rectangle blocks (whole inbox vs filtered "free" emails) connected by an arrow, showing the base-rate reversal.

- **Title (bold 15px `#1a5276`, center):** 'An Inbox of 1,000 Emails, 10% Spam (illustrative)'.
- **Left block (x=70, y=60, 250×170, `#1a5276` 1.5px outline):** top 10% strip filled `rgba(217,89,38,0.45)` (spam), bottom 90% filled `rgba(42,120,214,0.25)` (normal). Side labels bold 12px: "100 spam" (orange `#d95926`), "900 normal" (blue `#2a78d6`). Caption below (12px `#444`): "whole inbox".
- **Right block (x=470, width 150, y=70, 0.9 px per email, `#1a5276` outline):** top segment 60 units filled `rgba(217,89,38,0.45)`, bottom 90 units filled `rgba(42,120,214,0.25)`. Side labels bold 12px: '60 spam say "free"' (orange), '90 normal say "free"' (blue). Caption below: 'only the "free" emails'.
- **Arrow:** horizontal gray (`#6b7280`, width 2) arrow between blocks at y=145 with filled arrowhead; labels above/below (12px gray): "keep only" / '"free"'.
- **Conclusion (bold 14px magenta `#d55181`, center, y=282):** '60% of spam says "free", but a "free" email is only 60/150 = 40% spam'.

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, bottom border 2px solid `#2980b9`), `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: `.text-col` (50%) text, `.viz-col` (50%) canvas 720×300.
- **Text cell structure:** `.tags` row of pill spans first, then `<ul>` of one-line bullets each opening with `<b>` term (bold terms colored `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. blue = `rgba(26,82,118,0.12)`/`#1a5276`; green = `rgba(39,174,96,0.15)`/`#27ae60`; red = `rgba(231,76,60,0.12)`/`#e74c3c`; orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 logical; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
