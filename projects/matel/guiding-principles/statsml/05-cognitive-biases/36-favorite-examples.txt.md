# Favorite Examples: The Same Three Features, Forever

Everyone keeps a personal set of go-to checks — the queries typed into every search system, the questions asked in every interview, the examples run at every demo. Repeating a fixed set builds confidence without adding coverage, and anything repeated long enough gets fixed first.

## 1. Twenty Checks, Three Features Covered

Tags: core idea | frozen slice | no new coverage

- **The habit** — a fixed set of favourite checks, used unchanged every time something is evaluated
- **Why it forms** — the favourites once caught a real problem, so they earned a permanent place
- **What they cover** — 3 of the system's 10 features, the same 3 on every single run
- **Run them 20 times** — 20 checks performed, and still only 3 features ever touched
- **Spread the same 20** — every one of the 10 features gets checked twice over
- **What repetition adds** — confidence, since the answer keeps coming back the same
- **What repetition does not add** — coverage, because the 7 untouched features stay untouched
- **How it feels from inside** — like thorough testing: the effort is real and the count is high
- **The honest description** — one observation of 3 features, repeated 20 times over

**Key point:** Confidence grows with the number of checks while coverage grows only with the number of *distinct* checks. A frozen favourite set decouples the two, so the person with 20 runs behind them feels far better informed than the person with 10 — while actually knowing about a third as much of the system.

*Illustrative Example — a 10-feature system checked 20 times, once as a frozen 3-feature set and once spread across all 10; the coverage counts are tallied at render time.*

## 2. Whatever Gets Checked Every Time Gets Fixed First

Tags: the drift | tuned to the sample | over-rated

- **The favourites are public** — everyone knows which queries and questions get used
- **So they get attention first** — a failure on a favourite is the one guaranteed to be noticed
- **After a while** — all 3 favourite features work, because they are the ones that got the work
- **Meanwhile** — of the 7 nobody checks, 4 work and 3 do not, since nothing pushed anyone to look
- **What the favourites report** — 3 of 3, a clean pass, and it is a true report of those 3
- **What the system actually is** — 7 of 10, and no favourite check can ever show that
- **The direction is not random** — the frozen set over-rates, being the set that got repaired
- **This is the opposite of a harsh verdict** — a stale favourite set is reliably too kind
- **What fixes it** — retiring checks that always pass, since one that never fails measures nothing

**Key point:** A slice that is chosen once and reused becomes the slice most likely to work, because effort follows attention and attention follows the favourites. That makes the error one-directional: an unchanging set of checks does not merely sample the system badly, it samples the best-maintained part of it and reports that as the whole.

*Illustrative Example — the same 10-feature system after the 3 favourite features have been repaired; the two scores are computed at render time.*
