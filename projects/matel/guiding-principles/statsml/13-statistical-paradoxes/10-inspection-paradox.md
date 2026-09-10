# Inspection Paradox: Everything Looks Bigger When You Bump Into It

Spec for `10-inspection-paradox.html`. Structure, CSS, `setup()`, `P` palette, `.card-section`
and all page rules come verbatim from `/tmp/paradox-skeleton.html`. Four sections, canvases
`c1`–`c4`.

**Subtitle:** Buses come every ten minutes, so why do you always wait longer than five

---

## Section 1 — Buses Come Every Ten Minutes and You Still Wait Almost Seven

Tags: `core idea` (blue), `length bias` (green), `sampling by encounter` (orange)

Running example, one hour of service, six gaps between buses: **3, 5, 7, 10, 15, 20 minutes**.
Sum 60, so the average gap is exactly **10.0 min**. A rider arriving at a moment nobody planned
lands inside a gap in proportion to its width: 3/60 = 5% chance in the 3-min gap, 20/60 = 33.3%
in the 20-min gap. Average wait = sum(g²)/(2·sum g) = 808/120 = **6.733 min**, not 5.

Bullets:
- **The schedule** — six buses in an hour means the average gap between them is ten minutes.
- **The naive answer** — arrive at random, wait half a gap, so five minutes seems obvious.
- **The actual wait** — averaged over every arrival moment it is 6.73 minutes, a third longer.
- **Why** — you do not pick a gap, the gap picks you, and wide gaps cover more of the clock.
- **The landing odds** — the widest gap swallows a third of the hour, the narrowest a twentieth.
- **The gap you land in** — averages 13.47 minutes wide, well above the typical ten.
- **Nothing is broken** — the timetable and your stopwatch are both right about different things.
- **The tell** — your unit of observation was a moment in time, not a bus.

Example line: the 3-minute gap and the 20-minute gap are one bus each on the timetable, but the wide one catches almost seven riders for every one the narrow one catches.

Key point: sampling by encounter weights every item by its own size, so the wide gaps show up far more often than their share of the count.

Source note: Illustrative Example — gaps chosen so the mean is exactly 10 minutes.

Chart `c1` (720×320) — timeline of the six gaps:
- One horizontal band spanning 60 minutes, split into six segments with widths proportional to 3, 5, 7, 10, 15, 20.
- Segment fill by width: narrow segments in `P.mute` tint, the two widest in `P.magenta` tint, mid ones `P.blue` tint; each labelled with its length in minutes (12px).
- Under each segment, its landing chance printed as a percentage computed as `g/60` — 5%, 8%, 12%, 17%, 25%, 33%.
- Small `P.orange` arrow markers dropped at even spacing across the band, visibly clustering inside the wide segments, captioned "arrivals land in proportion to width".
- Two callout figures, computed in the draw function: `P.green` "average gap on the timetable 10.0 min" and `P.magenta` "average wait you actually experience 6.73 min" (bold 19px), plus a gray parenthetical "(gap you land in averages 13.47 min wide)".
- Bottom caption: "You never chose a gap — the widest gap chose you."

---

## Section 2 — The Small Classes on Paper Are Not the Classes Students Sit In

Tags: `second domain` (blue), `class size` (aqua), `crowding` (orange)

Eight classes, sizes **10, 10, 10, 10, 10, 20, 30, 100**. Seats total 200. Average class size =
200/8 = **25.0 students**. Ask a randomly chosen student how big their class is: each class
contributes its own size worth of answers, so the student-weighted average is
sum(c²)/sum(c) = 11800/200 = **59.0 students**. Half of all 200 students sit in the single
100-seat class.

Bullets:
- **The brochure** — eight classes, two hundred seats, so the average class holds twenty-five.
- **The student's answer** — asked at random, the average student reports a class of fifty-nine.
- **Both are right** — one averages over classes, the other averages over students.
- **The count** — five of the eight classes hold ten students but only a quarter of all students.
- **The crowd** — the single hundred-seat class holds half the student body all by itself.
- **The gym version** — the empty hours have nobody in them to report that the gym was empty.
- **The queue version** — long queues hold more people, so more people remember long queues.
- **The tell** — asking people about the group they are in samples groups by their size.

Example line: Alice picks five students at random and hears "about a hundred" from two or three of them, then checks the register and finds most classes have ten.

Key point: any survey that reaches items through their members counts each item once per member, so the big ones speak loudest.

Source note: Illustrative Example — class sizes chosen for clean arithmetic.

Chart `c2` (720×320) — bars plus two averages:
- Eight vertical bars, one per class, heights 10, 10, 10, 10, 10, 20, 30, 100, in `P.blue` tint with `P.violet` for the 100-seat bar.
- Above each bar, its share of all students printed as a percentage computed from `c/200`.
- Horizontal `P.green` dashed line at 25 labelled "average class (per class)"; horizontal `P.magenta` dashed line at 59 labelled "average class a student sits in".
- Both averages computed in the draw function from the bar array, printed to one decimal.
- Bottom caption: "Count classes and they look small. Count students and they look crowded."

---

## Section 3 — Snapshot a Waiting Room and Every Illness Looks Slow

Tags: `real case` (blue), `duration data` (green), `snapshot sampling` (magenta)

The waiting-time version of this is textbook renewal theory: W. Feller, *An Introduction to
Probability Theory and Its Applications, Vol. II* (2nd ed., Wiley, 1971), the inspection-paradox
and residual-lifetime results. The screening version is the length-bias problem for **prevalent
cases**: cases found in a one-time sweep of a population over-represent slow-progressing disease
because slow cases spend longer in the detectable state, which inflates apparent survival.
Standard reference: M. Zelen and M. Feinleib, "On the theory of screening for chronic diseases",
*Biometrika* 56 (1969), 601–614, which introduced lead time and length-biased sampling in screening.

Illustrative durations, ten support cases that ran **1, 1, 2, 2, 3, 4, 6, 8, 13, 20 days**.
Total 60, average duration **6.0 days**. Take one snapshot of the open queue: the chance a case
is open at that instant is proportional to its own length, so the average full duration of a case
caught open is sum(d²)/sum(d) = 704/60 = **11.73 days** — nearly double the truth.

Bullets:
- **The setup** — ten cases whose true average life is six days, all opened and closed on record.
- **The snapshot answer** — cases found open at one instant run 11.73 days on average.
- **The mechanism** — a twenty-day case is open on twenty days, a one-day case on exactly one.
- **The screening version** — a one-time sweep finds slow-growing disease and misses the fast.
- **Why it matters** — slow cases last longer anyway, so a sweep's survival numbers look rosy.
- **The textbook root** — Feller derives the same waiting-time result for renewal processes.
- **The safe sample** — pick cases by their start date, then follow each one to its end.
- **The tell** — "currently open", "active now" and "prevalent" all mean sampled by encounter.

Example line: two engineers audit the same queue — one lists every case opened in March, the other lists whatever was open on the 15th, and the second reports durations nearly twice as long.

Key point: a snapshot of work in progress is a length-biased sample, so it overstates typical duration unless you correct for it.

Source note: Feller (1971), Vol. II; Zelen & Feinleib, *Biometrika* 56 (1969). Duration figures are an Illustrative Example.

Chart `c3` (720×320) — durations with catch probability:
- Ten horizontal bars, one per case, lengths 1, 1, 2, 2, 3, 4, 6, 8, 13, 20 days, sorted ascending.
- Each bar shaded by snapshot-catch chance, computed as `d/60` and printed at the bar end (12px `P.mute`): 1.7% up to 33.3%.
- Short bars in `P.mute` (rarely caught), the two longest in `P.magenta` (almost always caught), middle in `P.aqua`.
- Vertical `P.green` line at 6.0 labelled "true average duration"; vertical `P.magenta` line at 11.73 labelled "average of what the snapshot catches" (bold 19px figure).
- Bottom caption: "One look at the open queue nearly doubles the measured duration."

---

## Section 4 — Make Every Gap Identical and the Puzzle Evaporates

Tags: `boundary` (blue), `spread drives it` (green), `the fix` (orange)

Five gap sets, all with **average gap exactly 10.0 minutes**, differing only in spread. Average
wait = sum(g²)/(2·sum g) in each case:

| gaps | spread (sd) | average wait |
|---|---|---|
| 10, 10, 10, 10, 10, 10 | 0.00 | 5.00 |
| 8, 9, 10, 10, 11, 12 | 1.29 | 5.08 |
| 6, 8, 9, 11, 12, 14 | 2.65 | 5.35 |
| 3, 5, 7, 10, 15, 20 | 5.89 | 6.73 |
| 1, 2, 3, 4, 10, 40 | 13.72 | 14.42 |

Bullets:
- **The clockwork case** — six gaps of exactly ten minutes gives an average wait of exactly five.
- **No spread, no paradox** — when every item is the same size, size cannot tilt the sampling.
- **The dial** — the same average gap with more spread pushes the wait from 5.00 up to 14.42.
- **The rough rule** — the extra wait grows with the square of how spread out the gaps are.
- **Fix one** — sample the items off the register, not the moments you happened to bump into.
- **Fix two** — if you must sample by encounter, weight each observation by one over its size.
- **Fix three** — ask out loud what you sampled by: a bus or a minute, a class or a student.
- **The warning sign** — a snapshot average far above a register average is length bias, not a bug.

Example line: Bob keeps the same six-bus-an-hour schedule but lets the gaps drift to 1, 2, 3, 4, 10 and 40 minutes, and the average wait triples to 14.42 minutes with no change in bus count.

Key point: the distortion is driven entirely by variation in size — equal sizes make it vanish, and wide spread makes it enormous.

Source note: Illustrative Example — five gap sets constructed with an identical 10-minute mean.

Chart `c4` (720×320) — wait versus spread:
- Five labelled columns, one per gap set ("clockwork", "nearly even", "mild drift", "the bus stop above", "one huge gap"), x ordered by spread, bar height = average wait computed in JS from each literal gap array.
- Bars coloured by distortion: the equal-gap bar `P.green`, then `P.blue`, `P.yellow`, `P.orange`, and the skewed one `P.magenta`.
- Flat `P.green` dashed reference line at 5.00 labelled "half the average gap — the naive answer".
- Under each bar its gap set and its spread printed compactly (12px `P.mute`); above each bar its wait to two decimals; the 14.42 figure in bold 19px.
- Sub-title line notes all five sets share the same 10.0-minute average gap.
- Bottom caption: "Same average gap every time — only the spread changes the wait."
