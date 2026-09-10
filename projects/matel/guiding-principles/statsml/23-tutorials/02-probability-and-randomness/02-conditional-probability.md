# Conditional Probability

**Page type:** detail page (tutorial card-sections: one h2 per section, two-column table layout — text left 50%, canvas right 50%)
**HTML title tag:** Conditional Probability

**Subtitle:** Filtering the world down to what you already know changes the odds — 20% of email is spam, but 85% of "free money" email is

## Two Numbers for the Same Inbox

Tags: `core idea` (blue), `running example` (orange)

- **The inbox** — 20% of all incoming email is spam: P(spam) = 20%
- **New information** — this particular email contains the phrase "free money"
- **The odds shift** — among emails with that phrase, 85% are spam
- **The notation** — P(spam | contains "free money") = 85%; the bar reads "given"
- **Definition (after the example)** — a conditional probability is the odds inside a filtered-down world

*Example:* Same email, same inbox — but once you spot "free money", the spam odds jump from 20% to 85%.

**Key point:** Knowing something true about a case shrinks the world to matching cases — the probability is recomputed inside that smaller world.

### Visualization (canvas `c1`, 720×300)

Shrinking-world diagram: the full inbox box filtered down to the phrase emails.

- **Title (bold 15px, `#1a5276`, top center):** "Knowing the Phrase Shrinks the World".
- **Big box (260×170 at left, blue `#2a78d6` border, fill `rgba(42,120,214,0.08)`):** top 20% band filled `rgba(213,81,129,0.55)` labeled bold magenta "400 spam (20%)"; body labeled blue "1,600 legit"; caption below bold ink: "all 2,000 emails".
- **Arrow (orange `#d95926`) between the boxes** labeled bold two lines: "keep only" / "\"free money\"".
- **Small box (150×150 at right, blue border, same fill):** top 85% filled `rgba(213,81,129,0.55)` labeled white bold "170 spam" with magenta "(85%)" beneath; bottom sliver labeled blue "30 legit"; caption below bold ink: "200 phrase emails".
- **Side annotations (bold violet `#4a3aa7` 13px, far right):** "P(spam) = 20%", "P(spam | phrase)", "= 85%".
- **Caption (bold orange 13px, bottom center):** "Same inbox — the condition just changed which emails count".

## Counting It Out: 2,000 Emails in Four Piles

Tags: `worked example` (green), `by hand` (blue)

- **Step 1** — take 2,000 emails; 20% are spam: 400 spam, 1,600 legit
- **Step 2** — "free money" appears in 170 of the 400 spam emails
- **Step 3** — it also appears in 30 of the 1,600 legit emails
- **Step 4** — so 170 + 30 = 200 emails contain the phrase
- **Step 5** — inside that pile: 170 / 200 = 85% spam

*Example:* The condition threw away 1,800 emails — the 85% is computed only from the 200 that survived the filter.

**Key point:** "Given B" just means "divide by the size of pile B" — conditional probability is ordinary counting inside one pile.

### Visualization (canvas `c2`, 720×300)

2×2 contingency grid of counts with the conditioned column highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "2,000 Emails Sorted Into Four Piles".
- **Grid:** four cells 190×74 with 10px gaps. Column headers (bold 12px): "contains \"free money\"" in orange `#d95926`, "no phrase" in gray `#6b7280`. Row headers (right-aligned): "spam (400)" in magenta `#d55181`, "legit (1,600)" in green `#008300`.
- **Cells (bold 17px count + 11px sublabel, colored border matching the row):** 170 "spam with phrase" (fill `rgba(213,81,129,0.16)`); 230 "spam without" (fill `rgba(213,81,129,0.05)`); 30 "legit with phrase" (fill `rgba(0,131,0,0.10)`); 1,570 "legit without" (fill `rgba(0,131,0,0.04)`).
- **Highlight:** orange 3px rectangle around the whole left ("phrase") column, labeled bold orange below: "the filtered world: 200 emails".
- **Caption (bold violet `#4a3aa7` 14px, bottom center):** "P(spam | phrase) = 170 / (170 + 30) = 170 / 200 = 85%".

## Every Feature, Filter, and Segment Is a Condition

Tags: `where it's used` (blue), `rule of thumb` (green)

- **Spam filters** — every rule is a conditional: P(spam | sender, phrase, links)
- **Model features** — a classifier's whole job is moving P(label) to P(label | features)
- **Segments** — "churn among new users" is P(churn | signed up < 30 days ago)
- **Both directions** — evidence can also lower odds: P(spam | no phrase) is only 12.8%
- **Rule of thumb** — whenever you filter a table before computing a rate, you are conditioning

*Example:* Every WHERE clause in SQL turns the rate you compute next into a conditional probability.

**Key point:** Useful information is exactly the information that moves a probability away from its overall value.

### Visualization (canvas `c3`, 720×300)

Three-bar chart: how evidence moves the spam probability in both directions.

- **Title (bold 15px, `#1a5276`, top center):** "One Inbox, Three Probabilities".
- **Data:** values `[12.8, 20, 85]`% with labels `['P(spam | no phrase)', 'P(spam) — no info', 'P(spam | phrase)']`, sublabels `['230 / 1,800', '400 / 2,000', '170 / 200']`, colors green `#008300`, blue `#2a78d6`, magenta `#d55181`.
- **Axes:** y 0–100% with labels every 25% and light gridlines `#e5e9ef`; padding top 55, bottom 72, left 65, right 40; gray `#999` axis lines.
- **Bars:** 130px wide, evenly spaced; bold 14px value labels above each bar, bold 12px label and gray 12px fraction sublabel below the baseline.
- **Caption (bold orange `#d95926` 13px, bottom center):** "Evidence moves the needle both ways: the phrase raises odds, its absence lowers them".

## The Common Confusion: P(A | B) Is Not P(B | A)

Tags: `common mistake` (red)

- **Two questions** — "spam, given the phrase" and "the phrase, given spam" filter to different piles
- **P(spam | phrase)** — 170 / 200 = 85%: out of the 200 phrase emails
- **P(phrase | spam)** — 170 / 400 = 42.5%: out of the 400 spam emails
- **Same 170 emails** — the numerator is identical; only the pile you divide by changes
- **The swap error** — quoting one number when you mean the other is a classic reporting bug

*Example:* "85% of phrase emails are spam" quietly becomes "85% of spam contains the phrase" — the true figure is 42.5%.

**Key point:** Before quoting any conditional probability, say out loud which pile you divided by.

### Visualization (canvas `c4`, 720×300)

Two-pile comparison split by a dashed vertical divider at x=360: the same 170 emails over two different denominators.

- **Title (bold 15px, `#1a5276`, top center):** "Same 170 Emails, Two Different Denominators".
- **Each pile:** a 200×130 light gray box (`#f4f6f8`, gray `#6b7280` border) filled from the bottom by a colored band proportional to 170/total, with "170" labeled inside (white) or above the band; bold ink title above; bold colored fraction and gray pile description below.
  - Left pile (magenta `#d55181`): title "P(spam | phrase)", fill 170 of 200, fraction "170 / 200 = 85%", description "pile: the 200 phrase emails".
  - Right pile (violet `#4a3aa7`): title "P(phrase | spam)", fill 170 of 400, fraction "170 / 400 = 42.5%", description "pile: the 400 spam emails".
- **Caption (bold orange `#d95926` 13px, bottom center):** "Swapping the two quietly doubles the claim — 85% vs 42.5% from the same counts".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then 4 `.card-section` blocks, each an `<h2>` (1.3rem, `#1a5276`, bottom border `2px solid #2980b9`) followed by `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills + `<ul>` bullets + italic `.example` + `.key-point` callout; right `td.viz-col` (50%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `<li><b>` bold terms in `#1a5276`. No nav bar, no back/home links. The "<" in the segments bullet is HTML-escaped (`&lt;`).
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Overall doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all charts 720×300 logical; a shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); an `arrow()` helper draws arrowheads. All counts hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No cross-page links; in regenerated HTML any card links would use `.html` extensions.
