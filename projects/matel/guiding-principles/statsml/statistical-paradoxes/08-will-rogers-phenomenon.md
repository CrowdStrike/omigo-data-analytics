# Will Rogers Phenomenon: Nobody Changed, Yet Both Sides Improved

**Subtitle:** Move one borderline case across a boundary and each group's average can rise while the whole stays exactly where it was

Four `.card-section` blocks, 50/50 text/canvas layout, canvases `c1`–`c4`.

---

## 1. One member switches groups and two averages go up

**Tags:** blue `core idea` · green `moving average` · orange `relabelling`

**Bullets**
- **Two groups** — a strong group averages 75 points and a weak group averages 35 points.
- **The switch** — the weakest member of the strong group, a 60, is refiled into the weak group.
- **Strong group rises** — losing the 60 leaves 70, 80 and 90, so its average climbs to 80.
- **Weak group rises too** — a 60 beats its average of 35, so that average is pulled up to 40.
- **Nobody improved** — all eight scores are exactly what they were before the refiling.
- **The total is fixed** — the eight scores still add to 440, so the combined average stays 55.
- **Why it works** — the mover sat below one average and above the other, so it lifted both.
- **Where the name comes from** — a humorist quipped that movers raised the average in two states at once.

**Example:** Strong group 60, 70, 80, 90 becomes 70, 80, 90. Weak group 20, 30, 40, 50 becomes 20, 30, 40, 50, 60.

**Key point:** An average can rise because the group changed shape, not because anything inside it got better.

**Source note:** Illustrative Example.

**Chart c1 (720×340) — four dot strips, before block and after block**
- Shared value scale 0–100 across the top, ticks every 20, gray labels.
- Block "BEFORE": Strong group row (blue dots 60, 70, 80, 90) and Weak group row (aqua dots 20, 30, 40, 50).
- Block "AFTER the refiling": Strong group row (70, 80, 90) and Weak group row (20, 30, 40, 50, 60).
- The 60 is drawn in orange in every row it appears, with a faint dashed orange vertical guide at its value.
- Each row carries an average tick above it: gray in the before block, magenta in the after block.
- After-block average labels also print the computed rise for that group.
- All four averages come from a `mean()` helper applied to the plotted arrays.
- Big figure (19px green): the combined average, before and after, identical.
- Caption: both group averages rose because a label moved, not a score.

---

## 2. A machine gets a new label and every tier looks better

**Tags:** blue `second domain` · orange `re-tiering` · red `flat reality`

**Bullets**
- **Two service tiers** — machines are filed Healthy or Watch by hours they run before servicing.
- **Healthy tier** — four machines at 900, 1200, 1500 and 1800 hours, averaging 1,350 hours.
- **Watch tier** — four machines at 200, 300, 400 and 500 hours, averaging 350 hours.
- **One refiling** — a new inspection rule moves the 900-hour machine into the Watch tier.
- **Healthy looks better** — its average jumps to 1,500 hours because its worst member left.
- **Watch looks better** — its average jumps to 460 hours because its best member just arrived.
- **Nothing was repaired** — the same eight machines still run the same 6,800 hours in total.
- **Site average is flat** — 6,800 hours shared over eight machines is 850, before and after.

**Example:** A quarterly review shows both tiers improving while the site produced not one extra running hour.

**Key point:** Any report split by category can improve in every category at once if the category rules moved.

**Source note:** Illustrative Example.

**Chart c2 (720×340) — grouped bars, before vs after**
- Three groups on the axis: Healthy tier, Watch tier, Whole site.
- Two bars per group: gray for before, magenta for after in the two tiers, green for the whole site.
- Vertical scale 0–1900 hours with gridlines at 0, 500, 1000, 1500.
- Bar-top labels print the computed means from the literal machine arrays.
- Orange note under the title: the 900-hour machine was relabelled, not repaired.
- Big figure (19px green): the hours gained by each tier, and the zero gained overall.
- Caption: two tiers improved and the site stood still.

---

## 3. Sharper scans, better numbers in every stage, same patients

**Tags:** blue `documented case` · green `overall unchanged` · orange `regrouping`

**Bullets**
- **The original finding** — doctors named this effect while studying lung cancer survival records.
- **Better scans** — sharper imaging found spread that the older tests had missed entirely.
- **Patients were regrouped** — people once filed with local illness moved into the advanced group.
- **Local group improved** — it lost its sicker members, so its surviving share rose.
- **Advanced group improved** — its new arrivals were healthier than the patients already there.
- **Same people, same outcomes** — no treatment changed and nobody lived a day longer.
- **Overall survival flat** — the surviving share of all patients was identical before and after.
- **Plain words for it** — the label attached to a patient moved between stages; the illness did not.

**Example:** In a 1,000-patient illustration, 50 patients move from local to regional and 60 from regional to advanced.

**Key point:** Feinstein, Sosin and Wells named this the Will Rogers phenomenon in the New England Journal of Medicine in 1985, showing lung cancer survival rising within every stage while overall survival did not change.

**Source note:** Illustrative Example — the counts are constructed to show the mechanism, not taken from the paper.

**Chart c3 (720×340) — grouped bars, surviving share by stage**
- Four groups: Local, Regional, Advanced, All patients.
- Two bars per group: gray before, magenta after for the three stages, green after for All patients.
- Vertical scale 0–100 percent with gridlines every 25.
- Counts held in JS: before 250/200, 350/175, 400/80; movers 50 (30 survive) and 60 (15 survive).
- Every printed share divided from those counts at draw time, plus the group sizes below the axis.
- Orange note under the title: the migrating patient counts.
- Big figure (19px green): the overall surviving share, before and after, identical.
- Caption: every stage rose and the overall share did not budge.

---

## 4. The narrow window where both averages can rise

**Tags:** blue `boundary` · green `when it is safe` · red `detection`

**Bullets**
- **The mover must be in between** — its value has to sit above one average and below the other.
- **Too weak to help** — a 30 added to a group averaging 35 drags that group's average down.
- **Too strong to spare** — pulling an 85 out of a group averaging 81 lowers that group's average.
- **The safe window** — with these two groups, only movers between 35 and 80 lift both averages.
- **Labels must move** — if group membership is frozen between reports, the effect cannot appear.
- **Detection, the total** — print the combined figure right next to the per-group figures.
- **Detection, the sizes** — group counts that shift between periods are the warning sign.
- **The fix** — compare periods under one set of rules, or state plainly that the rules changed.

**Example:** Moving a 60 lifts both averages by 5 points; moving a 30 lifts one average and lowers the other.

**Key point:** Both averages rise only when the mover falls between the two group averages and the labels are free to change.

**Source note:** Illustrative Example.

**Chart c4 (720×340) — the safe band**
- Horizontal axis: the value of the moved member, 20 to 95.
- Blue line: change in the strong group's average as that value varies.
- Aqua line: change in the weak group's average as that value varies.
- Both changes computed from the same arrays used in section one.
- Zero line drawn dark; green shaded band where both lines are above zero.
- Band edges are the two group averages themselves, computed and printed, not hardcoded.
- Violet dashed marker at 60, labelled with both computed rises.
- Big figure (19px green): the computed band edges.
- Caption: outside the band, one group's average always falls.
