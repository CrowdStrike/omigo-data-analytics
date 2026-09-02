# Contradictory Proverb Pairs

**Page type:** detail page (single-column long doc: philosophy callout, two h2 sections each with a full-width table, orange callout, closing-note callout; no canvases)
**HTML title tag:** Contradictory Proverb Pairs — Folk Wisdom Dissected

**Subtitle:** Proverbs that advise the exact opposite with equal confidence — and what the disagreement reveals.

## Callout (philosophy box)

**The core insight:** When two proverbs contradict, the easy reading is "folk wisdom is useless." That stops the thinking. The sharper reading: **each pair brackets a hidden conditioning variable.** "Look before you leap" and "he who hesitates is lost" are not rivals — they are the two ends of one axis, *cost of delay vs cost of error*. Neither names that axis, which is why both survive. **The disagreement is what locates the variable**, so the pair carries more information than either half alone.

## The Pairs

One row each. The third column is the thing neither proverb states — and the only thing that decides which half applies to you.

Table (class `pair-table`, three columns; first column italic green `#196f3d` at 22% width, second column italic orange-brown `#a04000` at 22% width, third column begins with a bold blue `#1a5276` "hidden variable" span):

| One half says… | …the other says | The variable neither names |
|---|---|---|
| "Look before you leap" | "He who hesitates is lost" | **Cost of delay vs cost of error.** Look first when errors are expensive and options persist; leap when the opportunity decays faster than you can evaluate it. |
| "Haste makes waste" | "Time waits for no man" | **Rework cost vs time saved.** Speed is free until it generates rework exceeding the time it saved. Note this duplicates the axis above — the corpus contains redundant contradictions, so its size overstates what it says. |
| "Silence is golden" | "The squeaking wheel gets the grease" | **Is the allocator attention-limited?** Squeaking pays where resources follow noise. It also works *once* — repeated, it is discounted to zero, so the rule destroys its own precondition. |
| "Fools seldom differ" | "Great minds think alike" | **Were the judgements independent?** Same observation, opposite valence — so agreement alone carries no information. This is the ensemble-diversity condition: averaging correlated estimators buys nothing. |
| "Quit while you're ahead" | "Winners never quit" | **Is the expected value of continuing still positive?** A forward-looking quantity, but both are stated as fixed dispositions. Neither mentions incoming information, which is the only thing that should govern the choice. |
| "Better safe than sorry" | "Nothing ventured, nothing gained" | **Is the downside recoverable?** Variance is not ruin. Venture freely where you can be wrong repeatedly; be conservative only where one error ends participation. |
| "You can't teach an old dog new tricks" | "You're never too old to learn" | **Replacement or addition?** Learning new material stays feasible; *unlearning* is the expensive part. "Old dog" really means "well-trained dog" — the plasticity/stability trade-off. |
| "Best things in life are free" | "You get what you pay for" | **Is the good market-priced?** Price signals quality only in competitive markets with visible quality. And "free" is usually mismeasured — time, attention and data are real costs the price tag cannot count. |
| "Actions speak louder than words" | "The pen is mightier than the sword" | **Verifiable cost of the signal.** Not words vs deeds at all: costly words beat cheap actions. |
| "Birds of a feather flock together" | "Opposites attract" | **Which attribute, and which stage?** Similarity dominates on values, complementarity on roles. Measure at attraction vs persistence and you get opposite answers — the same phase-shifted-sampling trap seen in growth curves. |
| "He who lives by the sword dies by the sword" | "Attack is the best form of defence" | **Time horizon, and does the game repeat?** Aggression can win most rounds and still end in ruin, because ruin is absorbing and victories are not. |
| "Don't judge a book by its cover" | "Clothes make the man" | **Not a contradiction — IS vs OUGHT.** One is descriptive (observers do judge on appearance), one normative (they should not). A fact and a rule cannot contradict. Both true at once. |
| "Ignorance is bliss" | "Knowledge is power" | **Not a contradiction — different outcome variables.** Knowledge raises capability and can lower comfort. Real axis: *is it actionable?* Information that changes no available decision has zero value and non-zero cost. |

Note: the last two rows ("Don't judge a book by its cover" and "Ignorance is bliss") have a light purple row background `#faf6fd`, and their hidden-variable span is colored purple `#6c3483` instead of blue.

## Callout (undefined-terms box, orange)

**What every pair omits:** the *regime* — the conditions under which it applies, the horizon it is judged over, the attribute it ranges across. A rule with no stated domain of validity is not a compressed insight; it is an untested hypothesis with the test conditions deleted.

## How to Assess Any Claim

The pairs are practice material. This is the transferable part — an interrogation you can run against any confident statement. Most weak claims fail before step four.

Table (class `flaw-table`, two columns; first cell of each row begins bold):

| Ask | Failure looks like |
|---|---|
| **1. Define it.** What do the terms mean, in units? | Terms elastic enough to fit any outcome after the fact. |
| **2. Falsify it.** What observation would make me abandon this? | No answer exists, or every disconfirmation gets absorbed by redefinition. |
| **3. Find who's missing.** Were the failures sampled, or only survivors? | All examples are winners; the base rate among losers is unasked. |
| **4. Name the regime.** Where does this reverse? | Stated as "always" or "never" with no domain attached. |
| **5. Check direction.** Could the arrow point the other way? | Correlation presented as mechanism. |
| **6. Check the metric.** Is one scalar standing in for many dimensions? | Ranking flips under a defensible reweighting nobody documented. |
| **7. Check observability.** Are both sides equally measurable? | The countable arm wins because it is countable, not because it is better. |
| **8. Fix the horizon.** Per-round or cumulative? | Positive expectation per round quoted as though it implied survival. |
| **9. Test independence.** Did agreeing sources decide separately? | Consensus from a monoculture counted as many confirmations. |
| **10. Locate yourself.** Which regime am I in? | Advice imported from a context with different costs and reversibility. |
| **11. Discount the packaging.** Would this persuade me stated plainly? | The rhyme, the chart, the confident delivery is carrying the claim. Strip the form and little is left. |

## Callout (closing-note box, blue)

**Don't believe everything you see.** Proverbs persuade through form. "Haste makes waste" rhymes; "look before you leap" has rhythm; both feel truer than the same advice in plain words. That is the packaging working, not the evidence — and rhyme has nothing to do with correctness. The polished chart does the same job in a different medium: axis labels and a confidence band are cheap to produce and expensive to check, so the most convincing presentation is the one easiest to fake. A broken query renders as beautifully as a correct one.

The answer is not to disbelieve everything either — blanket skepticism is the same empty move as blanket trust, a fixed disposition that ignores what is actually in front of you. Strip the form, state the claim plainly, and see whether anything is left. Then believe it in proportion to what you can check.

(In the HTML, the two paragraphs above are one `.closing-note` div separated by `<br><br>`.)

## Regeneration instructions

- **Template:** claim-dissection style (`ui-templates/07-claim-dissection-cards.html` family), single-column long doc. Order: h1, `.subtitle`, `.philosophy` callout, h2 "The Pairs" + intro paragraph + `.pair-table`, `.undefined-terms` callout, h2 "How to Assess Any Claim" + intro paragraph + `.flaw-table`, `.closing-note` callout. No canvases, no nav bar, no back/home links.
- **Table structures:** `.pair-table` — full width, collapsed borders, 0.86em; th background `#f0f4f8` color `#1a5276`, cells `1px solid #e0e0e0` padding 9px 10px, even rows `#fafcfe`; `td.pA` italic weight-500 `#196f3d` width 22%; `td.pB` italic weight-500 `#a04000` width 22%; `span.hv` bold `#1a5276`. Two "not a contradiction" rows get inline `background:#faf6fd` and `.hv` inline `color:#6c3483`. `.flaw-table` — same styling at 0.88em, padding 8px 12px.
- **Callout styles:** `.philosophy` and `.closing-note` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. `.undefined-terms` — background `#fff8f0`, left border `3px solid #e67e22`, padding 8px 14px, 0.88em.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with bottom border `2px solid #2980b9`, padding-bottom 8px; p 0.95em `#333`; `.subtitle` `#666` 1.0em; `strong` `#1a5276`.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange; accents #196f3d, #a04000, #6c3483, #2980b9.
- Canvases (none on this page) would use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
