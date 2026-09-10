# Double Counting

**Page type:** detail page (tutorial card-sections: one `<h2>` per section, two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Double Counting

**Subtitle:** Adding two lists counts anyone on both lists twice — subtract the overlap once and the number is honest again

## The 8,000-Customer Announcement

**Tags:** `core idea` (blue), `running example` (green)

- **The claim** — 5,000 on the email list + 3,000 app users = "8,000 customers"
- **The catch** — 1,200 people are on both lists
- **Counted twice** — each of those 1,200 shows up once per list, so twice in the sum
- **The real number** — 5,000 + 3,000 − 1,200 = 6,800 distinct customers
- **Size of the miss** — the claim is 1,200 too high, an overstatement of about 18%

*Example (italic):* One customer, two rows — the sum counts rows, not people.

**Key point:** Adding list sizes counts memberships, not people. Subtracting the overlap once turns memberships back into people — that fix is called inclusion–exclusion.

### Visualization (canvas `c1`, 720×300)

Two-bar comparison: the announced total with the double-counted slab highlighted vs the real distinct count.

- **Title (bold 15px, `#1a5276`, top center):** "The Announced 8,000 vs the Real 6,800".
- **Scale:** baseline gray line at y=245 from x=120 to x=620; chart height 185px; y scale max 8,800; bars 130px wide.
- **Left bar ("announced", x=190):** stacked — bottom segment 6,800 in blue `#2a78d6` at 0.7 alpha; top segment 1,200 in `rgba(231,76,60,0.55)` with 2px red `#e74c3c` outline. Bold red 14px "8,000" above; captions "announced" (12px `#333`) and "5,000 + 3,000" (11px mute `#6b7280`).
- **Right bar ("distinct people", x=420):** 6,800 in green `#008300` at 0.7 alpha, bold green "6,800" above; captions "distinct people" and "5,000 + 3,000 − 1,200".
- **Callout beside the red slab (bold red 13px, left-aligned):** "1,200 people" / "counted twice".
- **Bottom annotation (bold orange `#d95926` 13px, center, y=288):** "the sum counts rows, not people — ~18% too high".

## Inclusion–Exclusion, Checked by Hand

**Tags:** `worked example` (green)

- **Split the Venn** — email-only 3,800, both 1,200, app-only 1,800
- **Check email** — 3,800 + 1,200 = 5,000: matches the email list
- **Check app** — 1,800 + 1,200 = 3,000: matches the app list
- **Add the pieces** — 3,800 + 1,200 + 1,800 = 6,800, each person counted once
- **Same answer** — the shortcut 5,000 + 3,000 − 1,200 lands on 6,800 too

*Example (italic):* The name says it all: include both lists, then exclude the overlap you counted twice.

**Key point:** Two roads to the truth — add the non-overlapping pieces, or add the lists and subtract the overlap. If they disagree, one of your counts is wrong.

### Visualization (canvas `c2`, 720×300)

Venn diagram with region counts on the left, verification arithmetic on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Split Into Pieces, Each Person Lands in Exactly One".
- **Venn circles:** Email circle at (235,155) radius 92 (blue `#2a78d6`), App circle at (355,155) radius 74 (magenta `#d55181`); fills at 0.22 alpha, 2px colored strokes.
- **Circle labels (bold 13px):** blue "Email 5,000" at (190,52); magenta "App 3,000" at (415,62).
- **Region counts (bold 15px at y=160) with 12px `#444` labels at y=180:** blue "3,800" / "email only" at x=190; violet `#4a3aa7` "1,200" / "both" at x=300; magenta "1,800" / "app only" at x=392.
- **Right-side checks (left-aligned at x=505):** bold blue 13px "3,800 + 1,200 = 5,000" with mute 12px "matches the email list"; bold magenta "1,800 + 1,200 = 3,000" with mute "matches the app list"; bold green `#008300` "3,800 + 1,200 + 1,800" / "= 6,800 people".
- **Bottom annotation (bold violet 13px, center, y=288):** "piece-by-piece and 5,000 + 3,000 − 1,200 both land on 6,800".

## Where Double Counting Sneaks Into Your Numbers

**Tags:** `where it's used` (blue), `watch out` (orange)

- **Summed reach** — adding campaign audiences counts multi-campaign users once per campaign
- **Join fan-out** — joining orders to a table with duplicate keys silently multiplies rows
- **Attribution** — channel credits can sum past 100% when one order is credited to several
- **The tell** — parts summing to more than the whole always means an overlap was ignored
- **The fix** — count people, not rows: `COUNT(DISTINCT customer_id)`

*Example (italic):* If your segment sizes add to 130% of the user base, users sit in more than one segment.

**Key point:** Whenever one entity can appear on two lists, a plain sum double counts it — dedupe by ID or subtract the overlap.

### Visualization (canvas `c3`, 720×300)

Bar chart of channel attribution credits plus a stacked total exceeding 100%.

- **Title (bold 15px, `#1a5276`, top center):** "Channel Credits That Sum Past the Whole (illustrative)".
- **Data (per-channel bars, 74px wide, 26px gaps, from x=90; baseline y=235, chart height 165px, y scale max 125, 0.72 alpha):** email 40% blue `#2a78d6`; search 35% aqua `#199e70`; social 28% violet `#4a3aa7`; ads 15% yellow `#c98500`. Bold 13px "%"-suffixed value in bar color above each; 12px `#333` channel name below.
- **Stacked bar (right of the four):** the four segments stacked in the same colors, totaling 118%; bold red `#e74c3c` 13px "118%" above; 12px "stacked" below.
- **100% line:** horizontal dashed red line (`#e74c3c`, dash 6/4, 1.5px) at the 100% level, labeled bold red 12px to the right, right-aligned ending at x=716 so it stays inside the canvas: "100% =" / "all revenue".
- **Bottom annotation (bold red 13px, center, y=288):** "40 + 35 + 28 + 15 = 118% — some orders were credited to two channels".
- Chart carries an "(illustrative)" label in the title since the numbers are invented.

## Three Lists: Subtract the Pairs, Add Back the Middle

**Tags:** `common mistake` (red), `worked example` (green)

- **Three lists now** — email 5,000, app 3,000, loyalty 2,000: naive sum 10,000
- **Pair overlaps** — email&app 1,200, email&loyalty 600, app&loyalty 400: total 2,200
- **Subtract pairs** — 10,000 − 2,200 = 7,800, but that over-corrects the middle
- **The middle 200** — people on all three lists: added 3 times, subtracted 3 times, now 0
- **Add back once** — 7,800 + 200 = 8,000 distinct customers, everyone counted once

*Example (italic):* The signs alternate — add singles, subtract pairs, add back triples — so each person nets out to exactly one count.

**Common mistake:** Stopping after subtracting the pair overlaps. That erases the all-three group completely — they end up counted zero times.

### Visualization (canvas `c4`, 720×300)

Waterfall chart: naive sum, pair-overlap subtraction, triple add-back, final total.

- **Title (bold 15px, `#1a5276`, top center):** "Three Lists: 10,000 − 2,200 + 200 = 8,000".
- **Scale:** baseline gray line at y=240 from x=50 to x=690; chart height 180px; y scale max 10,500; bars 110px wide, 0.72 alpha.
- **Steps (each with bold 13px value label in bar color above, 11px captions below in `#333` and mute):**
  1. x=70, full bar 0→10,000, blue `#2a78d6`, label "10,000", captions "add all three" / "5,000+3,000+2,000".
  2. x=230, floating segment 7,800→10,000, orange `#d95926`, label "− 2,200", captions "subtract pair" / "overlaps".
  3. x=390, floating segment 7,800→8,000, green `#008300`, label "+ 200", captions "add back the" / "all-three group".
  4. x=550, full bar 0→8,000, violet `#4a3aa7`, label "8,000", captions "distinct" / "customers".
- **Connectors:** dashed light-gray (`#b9c2cc`, dash 4/3) horizontal lines at the 10,000, 7,800 and 8,000 levels linking adjacent bars.
- **Warning callout (bold red `#e74c3c` 12px, right-aligned ending at x=542, in the gap left of the 8,000 bar near the 7,800 level):** "stop here and the 200 people on" / "all three lists count zero times".
- **Bottom annotation (bold orange 13px, center, y=290):** "alternate the signs: + singles, − pairs, + triples".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). h1 (no index number) + `.subtitle`, then four `.card-section` blocks, each with an `<h2>` and a `table.layout` (`td.text-col` 50%, `td.viz-col` 50%, cells padded 12px, top-aligned).
- **Left column per section:** `.tags` pill row, `<ul>` of one-line bullets opening with `<b>bold term</b>` (bold in `#1a5276`), one italic `.example` line, one `.key-point` callout. Inline `code` in bullets uses ui-monospace/Menlo 0.9em on `#f4f6f8` background, 1px 4px padding, 3px radius.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 intrinsic, scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates); data hardcoded as literals, no `Math.random()`; invented numbers labeled "(illustrative)".
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions.
