# Reference-Class Substitution: Someone Else Chose What Counts as Normal

Show someone nine real bikes out of twenty-five and they will tell you what a bike costs. They are describing the nine and they think they are describing the twenty-five.

## The one dataset behind every figure on this page

Every figure on this page comes from one list of twenty-five asking prices, given in full here so a reader can redo each of them by hand. Nothing on this page is generated.

| Group of bikes for sale | Count | Asking prices |
|---|---|---|
| Ordinary bikes around town | 14 | $70, $80, $90, $100, $110, $120, $130, $140, $150, $160, $170, $190, $200, $220 |
| Shop-restored bikes | 11 | $180, $240, $280, $300, $320, $340, $360, $380, $400, $430, $450 |
| Restored bikes the shop has room to display | 9 | $180, $280, $320, $340, $360, $380, $400, $430, $450 |

Every quantity the text quotes, and how it follows from those prices:

| Quantity | Value | How it checks out |
|---|---|---|
| Bikes for sale in town | 25 | 14 plain + 11 restored |
| Cheapest and dearest in town | $70, $450 | lowest and highest of the 25 |
| Town middle | $190 | 13th of 25 sorted prices |
| Window middle | $360 | 5th of 9 sorted prices |
| The gap | $170 | $360 − $190 |
| Town middle as a share of the window's | 53% | $190 ÷ $360 |
| Restored middle | $340 | 6th of 11 sorted prices |
| Window's miss on restored | $20 | $360 − $340 |
| How much worse the town miss is | 8.5× | $170 ÷ $20 |
| Alice's budget | $190 | set equal to the town middle |
| In reach, in town | 13 of 25 | prices ≤ $190, just over half |
| In reach, in the window | 1 of 9 | only the $180 bike |
| Cheapest bike on show | $180 | lowest of the 9 displayed |
| Bikes cheaper than anything on show | 11 | prices below $180 |
| Restored but not on show | 2 | $240 and $300 |

## 1. Twenty-Five Bikes for Sale, Nine in the Window

Tags: core idea | a slice, not the whole | every price is real

- **The town** — twenty-five bikes for sale, a $70 rusty runabout up to a $450 restoration
- **The shop window** — nine bikes on display, every one restored, none priced under $180
- **Nothing is hidden** — every price in the window is real, and not one of them is a lie
- **What is left out** — eleven bikes cost less than $180, the cheapest one on show
- **Middle of the window** — $360, and walking past for a fortnight makes that "what a bike costs"
- **Middle of the town** — $190, barely over half the window's, and the price Alice needed to know
- **The gap** — $170, put there by the choosing, not by the market

**Key point:** No false price was ever shown. The whole effect comes from which bikes the window had room for, and the sense of "normal" it installs is a fact about the window that gets stored as a fact about the town.

*Illustrative Example — 25 bikes listed as fixed prices in the page source; both middles are computed from those arrays at render time.*

## 2. A Middling Buyer Who Reads as Nearly Broke

Tags: judging yourself | the median feels poor | it feeds itself

- **Alice's budget** — $190, exactly the middle price of every bike for sale in town
- **Against the town** — 13 of the 25 are within reach, just over half, so she is a middling buyer
- **Against the window** — only the $180 bike of the nine is in reach, so she reads as nearly broke
- **What she concludes** — "I cannot afford a decent bike", which is false about her town
- **Why it lands** — she is measuring herself against a crowd that was assembled for her
- **The self-feeding part** — feeling short, she stretches $170 to the window's middle and joins it
- **Nobody quoted her a price** — there is no figure she could have argued down from

**Key point:** She has not misjudged her own budget — she has misjudged the crowd she is standing in. Swap the crowd and the identical $190 goes from 13 of 25 in reach to 1 of 9, from ordinary to inadequate, which is why the conclusion feels like self-knowledge rather than an error about the town.

*Illustrative Example — the same 25 listed prices; both counts are tallied from the arrays at render time.*

## 3. One Window, Two Questions, One Right Answer

Tags: the boundary | sometimes it is the right reference | silent substitution

- **One window, two questions** — the same nine bikes, asked to answer both of them
- **What does a restored bike cost** — the window says $360 against a true $340, so a $20 miss
- **What does a bike in town cost** — the same $360 against a true $190, out by the whole $170 gap
- **The window never changed** — one set of prices, 8.5 times further off one question than the other
- **When it is the right reference** — when the group you asked about is the group on show
- **When it quietly substitutes** — when you wanted the town and got its restored corner
- **The test** — name the group your question is about, then ask who was left out
- **Restored bikes are real** — eleven of the twenty-five really are restored, so this is no fiction

**Key point:** A curated display is not a distortion by nature — asked what a restored bike costs, a window of restored bikes is the reference you want and lands $20 from the truth. It becomes the bias only when it stands in for a group it was never drawn from, and the tell is that the substitution is silent: the window looks identical in both cases.

*Illustrative Example — the same 25 listed prices; both true middles and both misses are computed at render time.*
