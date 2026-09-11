# Noise

**Page type:** detail page (tutorial card-sections: h2 per section, two-column layout table — text left 50% with tag pills/bullets/example/key-point, canvas right 50%)
**HTML title tag:** Noise

**Subtitle:** The day-to-day jitter left over after trend and season are removed — real money, but no meaning

## What's Left When Trend and Weekday Are Removed

**Tags:** `core idea` (blue), `running example` (green)

- **Predict a day** — trend level + weekday bump gives the expected till for any date
- **The till disagrees** — a rainy lunch, a tour bus, one big office order: every day misses a bit
- **The leftover** — actual − expected; for this shop it never strays past about ±$16
- **No pattern in it** — the leftovers don't trend and don't repeat weekly; tomorrow's is a coin flip
- **That's noise** — variation with no usable structure, the cost of measuring anything real

*Example:* 84 straight days of leftovers all landed inside a ±$16 band around zero — up one day, down the next, no story.

**Key point:** Noise is what remains after every pattern you can name has been removed. It never goes to zero — plans that assume it will are wrong on day one.

### Visualization (canvas `c1`, 720×300)

Line/dot chart: 84 days of residual leftovers inside a shaded ±$16 band around zero.

- **Title (bold 15px, `#1a5276`, top center):** "84 days of leftovers (actual − expected)"
- **Data:** 84 daily values from the deterministic noise function `10·sin(d·7.9) + 6·sin(d·3.3)` for days d = 365..448 (window starts on a Tuesday in January, year 2); all values stay within ±16
- **Axes:** y from −45 to +45 with labels at −$30, −$16, $0, +$16, +$30 (right-aligned gray `#6b7280`); solid gray zero line; L-shaped axis frame `#999`; padding l:58 r:20 t:46 b:44
- **Band:** filled rectangle `rgba(42,120,214,0.10)` between +16 and −16; dashed blue `#2a78d6` band-edge lines (dash 4/4) at +16 and −16
- **Series:** connected line `rgba(74,58,167,0.5)` width 1.2 with violet `#4a3aa7` dots radius 2.5 at every day
- **X labels (12px gray, centered):** "week 1" at day 0, "week 5" at day 28, "week 9" at day 56, "week 12" at day 83
- **Annotation (bold 13px green `#008300`, centered near top at y=+36):** "every one of the 84 days lands inside ±$16 — jitter, not signal"
- **Caption (12px `#6b7280`, bottom right):** "illustrative data"

## One Bad Tuesday, Checked by Hand

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **The bad day** — a Tuesday till shows $225 and the owner starts blaming the new barista
- **Expected** — trend says the level is $261 that week; Tuesdays run $20 below: $241
- **The gap** — 225 − 241 = −$16: that Tuesday's leftover
- **Context** — the other 7 Tuesdays left −10, −6, 0, +3, +10, −1, 0: same wobble, both signs
- **Verdict** — −$16 touches the edge of the usual band; ugly, but not news

*Example:* $225 on a Tuesday sounds grim until you compute the expected value: $241, a gap of one band-width, seen before.

**Key point:** A deviation means nothing by itself — it only means something relative to the size of the usual wobble.

### Visualization (canvas `c2`, 720×300)

Bar chart: leftovers of eight consecutive Tuesdays, the −$16 one highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Eight straight Tuesdays: the leftovers, in dollars"
- **Data (8 Tuesday leftovers, in order):** `[-10, -6, 0, 3, 10, -1, -16, 0]` — bars labeled "Tue 1".."Tue 8" (11px gray)
- **Axes:** y from −26 to +26; solid gray zero line; padding l:58 r:20 t:50 b:48
- **Band edges:** dashed blue `#2a78d6` lines (dash 4/4) at +16 and −16 with right-aligned blue 12px labels "+$16 band" and "−$16 band"
- **Bars:** 55% of a 1/8 slot; normal bars `rgba(74,58,167,0.45)`; the worst Tuesday (index 6, −16) in `rgba(217,89,38,0.75)`; bold 12px signed value labels ("+3", "-16") above positive bars / below negative bars
- **Annotations:** bold 13px orange `#d95926`, centered near bottom: "the \"bad\" Tuesday: $225 vs expected $241" and on a second line "— touches the band edge, no story"; 12px gray left-aligned near top: "both signs, no run, no drift = noise"
- **Caption (12px `#6b7280`, bottom right):** "illustrative data"

## The Noise Band vs a Real Change

**Tags:** `rule of thumb` (blue), `illustrative scenario` (orange)

- **The band** — this shop's leftovers live inside ±$16; that is its normal wobble
- **One day below** — even a −$16 day is business as usual; the band absorbs it
- **Then a real change** — the office next door closes; sales run about $28 below expected
- **The signature** — 14 straight days outside the band, all on the same side, no bounce back
- **Noise flips sign constantly** — a real change stays put; that is how you tell them apart

*Example:* One −$16 Tuesday is noise. Fourteen days in a row near −$28 is a lost customer base.

**Key point:** Don't ask "is today low?" — ask "has the line left the band and stayed out?" Duration and one-sidedness separate change from jitter.

### Visualization (canvas `c3`, 720×300)

Line chart: 84 days of residuals — 70 normal days, then a sustained −$28 shift for the last 14 days (illustrative).

- **Title (bold 15px, `#1a5276`, top center):** "Noise bounces back — a real change stays out of the band"
- **Data:** residual = same noise function `10·sin(d·7.9) + 6·sin(d·3.3)` (d = 365+i) with an added −28 shift for days i ≥ 70 of the 84-day window
- **Axes:** y from −55 to +40 with labels at −$40, −$16, $0, +$16; solid gray zero line; padding l:58 r:20 t:46 b:44
- **Band:** filled `rgba(42,120,214,0.10)` between ±16 with dashed blue edge lines (dash 4/4)
- **Series:** days 0–70 as line `rgba(74,58,167,0.55)` width 1.5 with violet dots radius 2.5; days 70–83 as red `#e74c3c` line width 2 with red dots
- **X labels (12px gray, centered):** "week 1" at day 0, "week 6" at day 35, "week 11" at day 70
- **Annotations:** bold 12px violet `#4a3aa7` left-aligned near top: "single dips inside the band: noise"; bold 13px red `#e74c3c` right-aligned near the shifted segment, two lines: "14 straight days below the band," / "same side = real change (illustrative)"

## Reacting to One Bad Day Costs Real Money

**Tags:** `common mistake` (red), `where it's used` (orange)

- **Chasing noise** — react to every day $10 below expected and this shop panics 14 times in 12 weeks
- **All false alarms** — those 12 weeks were pure jitter; nothing was wrong on any of them
- **Whipsaw cost** — each panic means menu changes, staff blame, discounts nobody needed
- **The fix** — judge a 7-day average instead: on the same data it never drifted past −$2
- **Know your band first** — measure the normal wobble, then alarm only outside it

*Example:* On identical healthy data, a day-level alert fires 14 times; a week-level alert fires zero times.

**Key point:** An alert that doesn't know the noise band is an alarm that cries wolf on schedule — and gets ignored the week a real wolf shows up.

### Visualization (canvas `c4`, 720×300)

Timeline comparison: two horizontal alarm tracks over the same 84 healthy days.

- **Title (bold 15px, `#1a5276`, top center):** "Two alarm rules, same 12 healthy weeks"
- **Tracks:** two thick (6px) horizontal lines in `#e5e9ef` at y=115 (Rule A) and y=220 (Rule B) spanning the plot width
- **Rule A ticks:** for each of the 84 days where the noise function is below −10, a red `#e74c3c` vertical tick (3px wide, 26px tall) on the top track — 14 such alarm days (count computed from the deterministic data)
- **Rule B:** no ticks — the 7-day average never leaves the ±$16 band (never even passes −$2)
- **Track labels (bold 13px `#2c3e50`, left-aligned above each track):** "Rule A — alarm on any day $10 below expected" / "Rule B — alarm when the 7-day average leaves the ±$16 band"
- **Result labels (bold 14px, right-aligned below each track):** red: "14 alarms — all false" (count rendered from data); green `#008300`: "0 alarms — correct: nothing was wrong"
- **Caption (12px gray, bottom center):** "the 7-day average of the same data never drifted past −$2"

## Regeneration instructions

- **Layout:** tutorial detail page — `<h1>` + `.subtitle`, then four `.card-section` blocks, each an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) followed by `table.layout` (full width, one row): left `td.text-col` 50% with `.tags` pill row, a `<ul>` of one-line bullets each opening with a `<b>` term (bold terms colored `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` 50% holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Charts:** shared JS palette `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; site palette #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange. Canvases declare intrinsic 720×300 and scale by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Residuals come from the deterministic function `noiseOf(d) = 10·sin(d·7.9) + 6·sin(d·3.3)` with window start `D0 = 365` — same series as the other time-series pages; no `Math.random()`.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
