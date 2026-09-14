# Harvest Now, Decrypt Later

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Harvest Now, Decrypt Later

**Subtitle:** Recorded today, opened later — why a future machine is a present-tense problem

## One Recorded Session, Opened Half a Century Later

**Tags:** `core idea` (blue), `recorded today` (green), `secrecy lifetime` (orange)

- **The session** — a hospital sends Alice's medical records to a records system over an encrypted link today
- **The copy** — an adversary on the path cannot read it, but can save the ciphertext to disk and simply wait
- **The secrecy need** — those records must stay confidential for Alice's lifetime; call it 50 years
- **The delay is free** — storage is cheap and ciphertext does not spoil, so waiting costs the adversary almost nothing
- **The reframe** — the question is not "when does the capability arrive?" but "how long must this stay secret?"
- **Already done** — if the answer to the second outlasts the first, today's transmission is the exposure event
- **No undo** — you can change tomorrow's traffic, but nothing you deploy later un-sends the recording made today

*Example (italic):* The hospital's link is sound by today's standards; the adversary's copy of that one session sits in an archive, unreadable for now, waiting for the arithmetic below to turn against the hospital.

**Key point:** Harvest now, decrypt later means the recording starts the clock — a capability that does not exist yet still determines whether data you send this morning stays private.

### Visualization (canvas `c1`, 720×300)

Timeline of the single recorded session: recording at year 0, secrecy required through year 50, an illustrative capability arrival at year 20, with the readable-but-still-secret window shaded.

- **Title (bold 16px, `#1a5276`, top center):** "One Recorded Session: Years 0–50 Secret, Capability at an Illustrative Year 20".
- **Time scale:** helper `X(t) = 80 + t * (590/60)` maps years 0–60 onto x = 80–670. Axis line 1px `#999` at y=245 from x=80 to x=670; ticks + 12px `#444` labels at t = `[0, 10, 20, 30, 40, 50, 60]` reading "now", "+10", "+20", "+30", "+40", "+50", "+60"; 12px `#6b7280` "years from today" centered at y=283.
- **Secrecy bar:** blue `rgba(42,120,214,0.30)` rect with 2px `#2a78d6` border from X(0) to X(50), y=88, height 26; 12px `#1a5276` label "must stay secret: X = 50 yr" drawn at X(0)+8, y=105 (left aligned).
- **Exposure shading:** rect from X(20) to X(50), y=88, height 26, fill `rgba(231,76,60,0.28)`, and a 2px `#e74c3c` right/left edge; bold 13px `#e74c3c` label "30 yr readable while still required secret" centered at (X(35), y=76).
- **Capability marker:** 2px dashed (`setLineDash([6,4])`) `#4a3aa7` vertical line at X(20) from y=62 to y=238; bold 12px `#4a3aa7` label "capability exists — illustrative Z = 20 yr" at X(20)+8, y=58 (left aligned).
- **Recording marker:** 6px `#d55181` filled dot at (X(0), y=150); bold 12px `#d55181` "session recorded today" at X(0)+10, y=147; 11px `#6b7280` "ciphertext filed away" at X(0)+10, y=164.
- **Migration bar:** yellow `rgba(201,133,0,0.30)` rect with 2px `#c98500` border from X(0) to X(7), y=192, height 22; 12px `#c98500` label "migration in flight: Y = 7 yr" at X(7)+10, y=207 (left aligned).
- **Caption (12px `#444`, bottom right):** "Z = 20 yr is an illustrative assumption used to work the arithmetic, not a forecast".

## X + Y > Z on the Hospital's Traffic

**Tags:** `worked example` (blue), `rule of thumb` (green), `migration lead time` (orange)

- **The rule** — Mosca's inequality, a widely used rule of thumb: if X + Y > Z you already have a problem
- **The three terms** — X = years the data must stay secret, Y = years your migration takes, Z = years until the capability
- **Case A, the records** — X = 50, Y = 7, so X + Y = 57 against an illustrative Z = 20
- **The verdict** — 57 > 20, and the shortfall is 57 − 20 = 37 years: you needed to finish migrating 37 years ago
- **Case B, a price quote** — X = 1, Y = 7, so X + Y = 8 against the same Z = 20; 8 < 20, no problem
- **Same Z, opposite answers** — one guess about the technology, two verdicts, because only X differs
- **The real lesson** — urgency is a property of the data's secrecy lifetime, not a property of the machine

*Example (italic):* Both the medical records and the session's short-lived price quote travel the same encrypted link on the same day; one is already exposed and the other is safe, and the only thing that differs is X.

**Key point:** Mosca's inequality moves the decision away from forecasting a date: with Z held fixed, X alone flips the verdict from "already too late" to "nothing to do".

### Visualization (canvas `c2`, 720×300)

Stacked X+Y bars for Case A and Case B against one horizontal Z line, so the same Z gives visibly different verdicts.

- **Title (bold 16px, `#1a5276`, top center):** "Same Z, Two Verdicts: Stacking X + Y for Two Kinds of Data".
- **Axes:** vertical 1px `#999` axis at x=100 from y=60 to y=245; baseline 1px `#999` at y=245 from x=100 to x=660. Value scale: 1 year = 3 px, so `Yp(v) = 245 - v*3`. Gridlines `#e5e9ef` with 12px `#444` right-aligned labels at x=92 for v = `[0, 20, 40, 60]` → y = `[245, 185, 125, 65]`; rotated 12px `#6b7280` "years" at (30, 155).
- **Case A bar (x=170, width 90):** X segment blue `#2a78d6` from y=245 up 150px (top y=95) labeled inside in white 12px "X = 50"; Y segment yellow `#c98500` from y=95 up 21px (top y=74) with 11px `#c98500` label "Y = 7" at x=265, y=88 (left aligned). Bold 13px `#1a5276` "X + Y = 57" centered at (215, 66); 12px `#444` "Case A: medical records" centered at (215, 262).
- **Case B bar (x=430, width 90):** X segment blue `#2a78d6` from y=245 up 3px (top y=242) with 11px `#2a78d6` label "X = 1" at x=525, y=246; Y segment yellow `#c98500` from y=242 up 21px (top y=221) with 11px `#c98500` label "Y = 7" at x=525, y=232. Bold 13px `#1a5276` "X + Y = 8" centered at (475, 212); 12px `#444` "Case B: a price quote" centered at (475, 262).
- **Z line:** 2px dashed `#4a3aa7` horizontal at y=185 from x=100 to x=660; bold 12px `#4a3aa7` "Z = 20 yr (illustrative)" right-aligned at (656, 178).
- **Verdicts:** bold 13px `#e74c3c` "57 > 20 → shortfall 37 yr" centered at (215, 130), drawn to the right of the Case A bar at (270, 130) left-aligned so it does not overlap the blue segment; bold 13px `#008300` "8 < 20 → safe" left aligned at (530, 165).
- **Caption (12px `#444`, bottom right):** "bars to scale (1 yr = 3 px); Z illustrative, X and Y are the example's stated values".

## Ranking Data Instead of Guessing a Date

**Tags:** `sensitivity check` (blue), `where it's used` (green), `cheapest lever` (orange)

- **Z is unknown** — credible expert estimates span a wide range, and this page deliberately does not forecast one
- **So test the conclusion** — vary Z and see which verdicts move; that is a sensitivity check, not a prediction
- **Case A's threshold** — it is only safe if Z > 57 years, so it stays a problem across essentially the whole range
- **Case B's threshold** — it only becomes a problem if Z < 8 years, at the very short end of the range
- **The robust output** — a ranking of data by secrecy lifetime, which survives being wrong about Z
- **Shrink X first** — deleting data and cutting retention lowers X directly and is the cheapest lever available
- **Shrink Y next** — inventory the estate, because you cannot migrate certificates and firmware you have not found
- **Hybrid key exchange** — run a classical and a post-quantum scheme together; if either holds, the session holds

*Example (italic):* Two rows on the hospital's data inventory: records retained for a lifetime go to the top of the migration queue, while a quote cache retained for a week never enters it.

**Key point:** Because Case A fails for every Z below 57 years and Case B only fails below 8, the ranking is robust even though the date is not — rank, do not forecast.

### Visualization (canvas `c3`, 720×300)

Sensitivity sweep: Z from 0 to 80 years on the x-axis, one safe/unsafe band per case, with each case's threshold marked.

- **Title (bold 16px, `#1a5276`, top center):** "Sweeping Z from 0 to 80 Years: Which Verdict Actually Moves?".
- **Scale:** `Z(v) = 80 + v * (590/80)` maps 0–80 onto x = 80–670, so Z=8 → x=139, Z=20 → x=227.5, Z=57 → x=500.4. Axis 1px `#999` at y=245, x=80→670; ticks + 12px `#444` labels at `[0, 20, 40, 60, 80]`; 12px `#6b7280` "assumed Z (years until the capability exists)" centered at y=283.
- **Case A band (y=100, height 28):** `#e74c3c` at 0.30 alpha from x=80 to Z(57)=500.4, then `#008300` at 0.30 alpha from 500.4 to 670; 1px `#999` outline around the full band; 12px `#444` "Case A: X+Y = 57" left aligned at (80, 92)… place label above band at y=92.
- **Case B band (y=170, height 28):** `#e74c3c` at 0.30 alpha from x=80 to Z(8)=139, then `#008300` at 0.30 alpha from 139 to 670; same outline; 12px `#444` "Case B: X+Y = 8" at (80, 162).
- **Threshold ticks:** 2px `#1a5276` vertical ticks at x=500.4 (rows A) and x=139 (row B) spanning their band ±6px; bold 12px `#1a5276` "safe only if Z > 57" at (508, 118) left aligned, and "safe once Z > 8" at (147, 188) left aligned.
- **The assumed Z:** 2px dashed `#4a3aa7` vertical line at x=227.5 from y=70 to y=238; bold 12px `#4a3aa7` "Z = 20 used above" at (232, 66) left aligned.
- **Annotation (bold 13px `#e74c3c`, centered at (400, 46)):** "Case A is unsafe for every Z below 57 yr — the ranking survives the guess".
- **Legend (12px, at y=223 with 12×12 swatches at x=80 and x=250):** `rgba(231,76,60,0.30)` "X + Y > Z (act now)" and `rgba(0,131,0,0.30)` "X + Y < Z (fine)".
- **Caption (12px `#444`, bottom right):** "thresholds computed from the stated X and Y; Z axis is a sweep, not a forecast".

## Forward Secrecy Doesn't Help, and Not Everything Is Urgent

**Tags:** `common mistake` (red), `signatures run the other way` (orange), `precision` (blue)

- **Forward secrecy** — ephemeral keys stop a *stolen long-term key* from opening past recorded sessions
- **Why it misses here** — the recorded handshake's own key agreement is the thing a future capability solves
- **Be precise** — deleting the session key protects against theft, not against the underlying mathematics being broken
- **Signatures invert** — a signature only needs to resist forgery until it is verified and superseded, so breakage is a future risk
- **Encryption is retroactive** — recorded ciphertext can be opened backwards in time; a checked signature cannot be un-checked
- **The exception** — decade-long roots and firmware verification keys must resist forgery for years, so they behave like Case A
- **The dismissal** — "no machine exists, so nothing to do" times the threat from the machine instead of from the recording
- **The mirror error** — assuming everything is urgent burns the effort that the genuinely long-lived data needed

*Example (italic):* A hospital enables ephemeral key exchange and calls the archive problem solved; the archived session is still openable, because what protected it was the key agreement itself, not a stored key.

**Common mistake:** Both halves are wrong — treating the clock as starting when the machine appears, and treating every byte as urgent; the recording starts the clock, and only long-X data is on it.

### Visualization (canvas `c4`, 720×300)

Three timeline rows on one shared time axis showing which direction each risk points: recorded ciphertext breaks backwards, a short-lived signature only forwards, a long-lived signing key behaves like ciphertext.

- **Title (bold 16px, `#1a5276`, top center):** "Direction of Risk: Ciphertext Breaks Backwards, Signatures Forwards".
- **Time scale:** same helper as c1, `X(t) = 80 + t * (590/60)`, years 0–60 on x = 80–670. Axis 1px `#999` at y=250, ticks + 12px `#444` labels at `[0, 20, 40, 60]` as "now", "+20", "+40", "+60"; 12px `#6b7280` "years from today" centered at y=285.
- **Capability line:** 2px dashed `#4a3aa7` vertical at X(20)=276.7 from y=50 to y=244; bold 11px `#4a3aa7` "capability (illustrative)" at (281, 48) left aligned.
- **Row 1 (y=90):** 12px `#444` label "recorded key exchange" at (84, 76); 6px `#d55181` dot at (X(0), 90); 3px `#e74c3c` arrow drawn from X(20) leftwards to X(0)+8 at y=90; bold 12px `#e74c3c` "opens backwards — today's session" at (X(20)+10, 94) left aligned.
- **Row 2 (y=155):** 12px `#444` label "signature verified, then rotated" at (84, 141); green `rgba(0,131,0,0.30)` rect with 2px `#008300` border from X(0) to X(2), y=146, height 18; 3px `#008300` arrow from X(20) rightwards to X(52) at y=155; bold 12px `#008300` "only future signatures at risk" at (X(20)+10, 172) left aligned.
- **Row 3 (y=215):** 12px `#444` label "firmware key, 20-yr device life" at (84, 201); yellow `rgba(217,89,38,0.25)` rect with 2px `#d95926` border from X(0) to X(20), y=206, height 18; bold 12px `#d95926` "long-lived signing key behaves like Case A" at (X(20)+10, 219) left aligned.
- **Caption (12px `#444`, bottom right):** "illustrative timelines; direction of each arrow is the point, lengths are schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `arrow(ctx,x1,y1,x2,y2,color,width)` helper for the c4 arrows.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` used only for the exposure window and the failing verdict.
- **Data — all hardcoded, no `Math.random()`:** X = 50 yr (Case A) and 1 yr (Case B), Y = 7 yr, Z = 20 yr. Sums are exact: 50 + 7 = 57, 1 + 7 = 8. Shortfall 57 − 20 = 37 yr. Timeline exposure window = years 20 → 50 = 30 yr (this is a different quantity from the 37-yr shortfall and both are stated as such). Safety thresholds solve X + Y < Z: Case A needs Z > 57, Case B needs Z > 8. Every Z value is labeled illustrative; no date is forecast.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
