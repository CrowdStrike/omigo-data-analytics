# Automation Bias: A Machine Said It, So Nobody Checked

An answer gets easier to accept once a machine produces it, and the checking that would have caught it quietly stops happening.

## 1. Same Suggestions, One Label Changed

Tags: core idea | identical answers | only the badge moved

- **The setup** — 240 suggestions reach a review queue, and a reviewer accepts or rejects each
- **The suggestions** — 190 are right and 50 wrong, the same split for both reviewers
- **The only difference** — one reviewer is told a colleague wrote them, the other a tool
- **Told a colleague wrote it** — 31 of the 50 wrong ones are caught, so 19 reach the customer
- **Told a tool produced it** — only 8 get caught, and 42 wrong suggestions go out unchallenged
- **The badge alone** — waves through 23 more wrong answers, over twice the other reviewer
- **It also lifts the good ones** — 98% of correct ones accepted against 87%, which looks like a win
- **What that costs** — 18% of everything accepted is wrong, against 10% under the sceptical reviewer

**Key point:** The suggestions were identical, so nothing about the accepting reviewer's higher throughput reflects better work arriving. The machine label bought agreement, and it bought it on the wrong answers just as readily as the right ones.

*Illustrative Example — 240 seeded suggestions reviewed twice; every count and share is tallied at render time.*

## 2. Handing It Over Helps on the Routine Work Only

Tags: the useful half | two kinds of case | pick per case

- **The work** — a hundred cases a day, of which 85 are routine and 15 are unusual in some way
- **What the tool does well** — 95 of 100 routine cases right, routine being what it trained on
- **What the tool does badly** — 40 of 100 unusual cases right, worse than a coin on hard ones
- **What a person does** — 80% on routine and 75% on unusual, steady but never brilliant at either
- **Person handles everything** — 79.3 cases right a day, which is the mark the tool has to beat
- **Tool handles everything** — 86.8 right, a real gain, and it is this gain that earns the trust
- **Tool on routine, person on unusual** — 92.0 right, and no better arrangement of these two exists
- **What blanket trust costs** — 9 wrong cases a day on unusual work against 3.8, all hard ones

**Key point:** Handing everything to the tool genuinely beats doing everything by hand, which is why it feels right. The 5.2 cases a day lost to it are all in the small hard slice, so the arrangement that looks best in aggregate is the one that fails hardest exactly where failure matters.

*Illustrative Example — constructed accuracy rates on a 100-case day; every daily total and error count is computed at render time from those rates.*

## 3. The Checking Fades Before the Tool Breaks

Tags: how it goes wrong | nobody decided this | nothing watching

- **The arrangement** — 200 outputs a week get a spot check, and whatever is spotted gets fixed
- **The first twelve weeks** — the tool is right 95 times in 100, so almost every check finds nothing
- **What that does to the checking** — it drifts from 60% of outputs in week one to 4% by week 12
- **Nobody decided to stop** — each week alone felt like time spent on things that were fine
- **Week 13** — an upstream change drops the tool to 60% right, and weekly errors jump from 12 to 81
- **What the faded checking catches** — 10 of the 651 errors made over the eight broken weeks
- **So 641 bad outputs** — 98% of them, reach customers, and no week looks unusual from the inside
- **Had checking stayed at 60%** — around 391 of those 651 would have been caught instead of 10

**Key point:** The tool breaking is the ordinary part. The costly part is that the instrument which would have shown it had already been switched off, and it was switched off by twelve weeks of correctly observing that there was nothing to find.

*Illustrative Example — 20 seeded weeks of 200 outputs; every weekly count and both totals are computed at render time.*
