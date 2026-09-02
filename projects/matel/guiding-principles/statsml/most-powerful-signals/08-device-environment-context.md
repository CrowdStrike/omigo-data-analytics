# Device & Environment Context Signals

**Page type:** detail page (compact card-sections: one h2 per section, two-column layout table with tag pills + labeled bullets left ~45%, canvas right ~55%; closing meta-point callout)
**HTML title tag:** Device & Environment Context

**Subtitle:** Time of day, device model, OS, battery, network, locale — the ambient context that predicts behavior before any behavioral signal is observed

## Time of Day & Day of Week

Tags: signal (blue), rule of thumb (blue), failure mode (red)

- **Cheapest signal** — always available, never missing, zero privacy cost
- **Content rhythm** — news peaks at commute; Netflix/YouTube 8–11pm
- **Conversion rhythm** — mobile carts midday, desktop purchases evening
- **Weekend mix** — B2B drops 60–70%; normalize before trend-reading
- **Encoding** — hours 23 and 0 are neighbors; use sin/cos. Time × device beats either alone

*Example:* Example: Spotify's clock alone reshuffles the home screen — focus playlists by day, chill at night.

**Failure mode:** timezone confusion shifts every peak-hour analysis — store UTC plus local offset, analyze in user-local time.

### Visualization (canvas `c1`, 720×300)

Two-series line chart of activity by hour. Title (bold 14px `#1a5276`, centered): "Activity by Hour of Day — Weekday vs Weekend Rhythm". Padding top 40, bottom 55, left 50, right 140; L-shaped gray (`#999`) axes.

- **Data (24 hourly values, y max 100):** weekday `[8, 5, 3, 2, 2, 4, 12, 30, 45, 42, 40, 44, 55, 48, 42, 44, 50, 62, 72, 85, 95, 88, 60, 25]` in blue `#1a5276` 3px; weekend `[15, 10, 6, 3, 2, 3, 5, 10, 22, 40, 55, 62, 65, 62, 58, 55, 55, 60, 68, 78, 88, 82, 55, 30]` in orange `#e67e22` 3px.
- **Shaded bands:** commute band hours 7-9 filled `rgba(230,126,34,0.12)` labeled bold orange 11px "commute" above; evening band hours 19-22 filled `rgba(231,76,60,0.10)` labeled bold red 11px "evening peak" above.
- **X labels:** `#555` 11px at hours 0, 6, 12, 18, 23 as "0h" ... "23h".
- **Annotation:** italic 10px blue right-aligned near bottom-right of plot: "23h and 0h are neighbors".
- **Legend (x = w-125):** blue swatch "Weekday"; orange swatch "Weekend".
- **Bottom caption (bold blue 11px, centered):** "Encode hour cyclically (sin/cos) — and always in user-local time, not server time".

## Device Model & OS as Socioeconomic Proxy

Tags: signal (blue), bias (orange), gaming (orange)

- **Orbitz 2012** — Mac users shown ~30% pricier hotels
- **iOS CPM gap** — DSPs bid 1.5–2x higher; iOS converts and subscribes more
- **Intent split** — desktop research vs mobile impulse; Android-first geographies invert assumptions
- **Device tier** — bucket models flagship/mid/budget/entry; stabler, less identifying
- **Spoofing** — fraud fakes iOS agents; verify against hardware signals

*Example:* Example: the same impression clears at multiples of the CPM on an iPhone Pro vs an entry Android.

**Failure mode:** "iOS converts more" mostly means "richer users convert more" — device pricing drifts into proxy discrimination in credit, insurance, and housing ads.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart, indexed to Android = 100. Title (bold 14px `#1a5276`): "iOS vs Android — Same Product, Different Economics (indexed, Android = 100)". Padding top 45, bottom 70, left 40, right 140.

- **Data (iOS values, Android fixed at 100; y max 190):** Conversion rate 138, Avg order value 145, Ad bid (CPM) 170, Subscription rate 160.
- **Bars:** 4 groups (group width 120, gap 18, bar width 42): iOS bar solid blue `#1a5276`, Android bar `rgba(26,82,118,0.35)`; iOS value labels bold red 12px, Android "100" labels `#555` 11px; category labels `#333` 11px below.
- **Baseline:** dashed gray (`#999`, dash 5/4) horizontal line at index 100, labeled `#777` 10px "Android = 100" on the right.
- **Legend (x = w-120):** blue swatch "iOS"; `rgba(26,82,118,0.35)` swatch "Android".
- **Bottom caption (bold orange 11px, two centered lines):** "Confounder, not cause: the gap is mostly income and geography riding on the device brand —" / "and fraud spoofs iOS user agents precisely because the bids follow".

## Battery Level & Charging State

Tags: signal (blue), privacy risk (red), trade-off (orange)

- **Uber disclosure** — <5% battery users accept surge more; not used for pricing
- **Low battery** — below ~20%, sessions shorten, video avoided, downloads deferred
- **Charging = settled** — best moment for long video, big syncs
- **API removed** — Firefox/WebKit dropped Battery Status API after fingerprinting research (Olejnik)
- **Confounder** — battery level tracks time of day; evening = drained

*Example:* Example: video platforms defer prefetch and autoplay when the OS reports low-power mode.

**Ethical line:** adapting UX to save power is good; pricing against desperation is a reputational time bomb users assume happened even though it did not.

### Visualization (canvas `c3`, 720×300)

Bar chart. Title (bold 14px `#1a5276`): "Willingness to Accept Surge Pricing by Battery Level". Padding top 45, bottom 70, left 55, right 30.

- **Data (y max 100):** battery buckets `['<10%', '10-20%', '20-40%', '40-60%', '60-80%', '>80%']`, acceptance `[88, 74, 58, 48, 44, 42]` (%).
- **Bars:** first two buckets red `#e74c3c`, remaining `rgba(26,82,118,0.35)` with matching strokes; bold 12px `#333` value labels "N%" above, 11px bucket labels below.
- **Baseline:** dashed green (`#27ae60`, 2px, dash 6/4) horizontal line at 42%, right-aligned bold green 10px label "baseline (charged users)".
- **Annotation:** red arrow pointing toward the low-battery bars with bold red 11px label "low battery = urgency (known, not used for pricing)".
- **Bottom caption (bold blue 11px, centered):** "Confound check: battery drains through the day — separate \"low battery\" from \"it is evening\" before believing the effect".

## Network Context: WiFi vs Cellular

Tags: signal (blue), bias (orange)

- **Bitrate adaptation** — Netflix/YouTube drop resolution on cellular; watch time follows
- **Downloads** — large installs and offline video cluster on WiFi
- **Session depth** — cellular is short and task-focused; WiFi explores
- **Location proxy** — WiFi settled, cellular transit; time notifications accordingly
- **Income proxy** — metered-data markets spawned Facebook Lite, YouTube Go

*Example:* Example: Facebook Lite and YouTube Go exist because connection type, not taste, defined the product.

**Analysis trap:** an engagement drop may be a cellular-mix shift — segment by connection type before reading any trend.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart, indexed to WiFi = 100. Title (bold 14px `#1a5276`): "WiFi vs Cellular — Behavior Shift (indexed, WiFi = 100)". Padding top 45, bottom 70, left 40, right 140.

- **Data (cellular values, WiFi fixed at 100; y max 115):** Video bitrate 45, Session length 62, Downloads started 18, Pages / session 70.
- **Bars:** 4 groups (group width 120, gap 18, bar width 42): WiFi bar green `#27ae60` with `#555` "100" label; cellular bar orange `#e67e22` with bold red 12px value label; category labels `#333` 11px below.
- **Highlight:** dashed red circle (radius 26) around the "Downloads started" cellular bar.
- **Legend (x = w-120):** green swatch "WiFi"; orange swatch "Cellular".
- **Bottom caption (bold blue 11px, centered):** "An engagement \"drop\" can be a network-mix shift — segment by connection type before reading any trend".

## Screen Size & Orientation

Tags: signal (blue), bias (orange), failure mode (red)

- **Format fit** — vertical video wins portrait phones; letterboxing halves completion
- **Orientation intent** — rotating to landscape signals commitment to watch
- **Viewport economics** — above-fold space differs ~3x phone vs desktop; correct position bias per screen
- **Tablet ambiguity** — couch device, evening, family-shared; not a phone
- **Display cohorts** — dark-mode/OLED cohorts contaminate creative A/B tests

*Example:* Example: TikTok's native vertical format filled portrait screens competitors letterboxed into a third.

**Failure mode:** layout A/B wins and losses across screen classes net to a false "neutral" — always segment by screen class.

### Visualization (canvas `c5`, 720×300)

Four-bar chart of video completion rate by format-screen combination. Title (bold 14px `#1a5276`): "Video Completion Rate — Format vs Screen Orientation". Padding top 45, bottom 80, left 40, right 30.

- **Data (y max 80):** Vertical video / portrait phone 72% (green `#27ae60`, MATCH); Horizontal video / portrait phone 31% (red `#e74c3c`, MISMATCH); Horizontal video / landscape / TV 68% (green, MATCH); Vertical video / landscape / TV 24% (red, MISMATCH).
- **Bars:** 70px wide at 75% alpha with 1.5px matching stroke, centered in 150px groups; bold 13px `#333` "N%" labels above; two-line 11px labels below; bold 10px colored "MATCH"/"MISMATCH" tag under each label.
- **Bottom caption (bold orange 11px, centered):** "Format-screen match roughly doubles completion — and layout A/B tests interact with screen class".

## Device Performance Tier & Age

Tags: signal (blue), rule of thumb (blue), bias (orange)

- **Latency kills** — each added load second cuts conversion; worst on old hardware
- **Segment strategy** — Android Go / lite variants exist; entry tier is huge in emerging markets
- **Wealth/tenure proxy** — device age encodes income and user tenure
- **Crash confounder** — normalize crash rates per device tier, not per release
- **Interaction feature** — same policy flips sign by render cost per tier

*Example:* Example: Android Go is a whole product line driven by the device-tier distribution.

**Rule of thumb:** test on the 10th-percentile device, not the median — team flagships hide the churn-risk experience.

### Visualization (canvas `c6`, 720×300)

Combo bar + line chart. Title (bold 14px `#1a5276`): "Device Tier — Load Time Up, Conversion Down". Padding top 45, bottom 70, left 55, right 145.

- **Data:** tiers `['Flagship', 'Upper mid', 'Mid', 'Budget', 'Entry / old']`; app-start load seconds `[1.1, 1.8, 2.9, 4.6, 7.2]` (bars, scale max 8, fill `rgba(26,82,118,0.35)` with `#1a5276` stroke, blue "Ns" labels); conversion index `[100, 91, 76, 55, 34]` (red `#e74c3c` 3px line with 4px dots and bold 10px value labels, scale max 110).
- **p10 marker:** dashed orange (`#e67e22`, 2px, dash 4/3) vertical line through the "Entry / old" tier, labeled bold orange 10px "test here (p10)" above.
- **Legend (x = w-135):** `rgba(26,82,118,0.35)` swatch "App start (s)"; red swatch "Conversion idx".
- **Bottom caption (bold blue 11px, centered):** "Crash and conversion metrics must be normalized per tier — releases reaching low-end devices \"look worse\"".

## Locale, Language & Timezone Consistency

Tags: signal (blue), defense (green), failure mode (red)

- **Content match** — device language beats IP geolocation for content language
- **Fraud signal** — "US shopper" on a UTC+8 clock deserves scrutiny
- **Consistency stack** — cross-check IP country, locale, keyboard, currency, SIM
- **Legit mismatches** — travelers, VPNs, immigrants; score, never hard-block
- **IP-only defaults** — units/currency from IP misfire on corporate VPNs

*Example:* Example: stolen-card fraudsters rarely fake the device clock; real travelers stay consistent on locale and currency.

**Key point:** joint consistency, not any single signal, is trustworthy — hard-block only on multi-signal contradiction plus behavioral evidence.

### Visualization (canvas `c7`, 720×300)

Bar chart. Title (bold 14px `#1a5276`): "Fraud Rate by Consistency-Mismatch Count"; subtitle (`#555` 11px): "signals checked: IP country · device timezone · keyboard language · currency · SIM country". Padding top 60, bottom 70, left 55, right 30.

- **Data (y max 45):** categories `['All consistent', '1 mismatch', '2 mismatches', '3 mismatches', '4+ mismatches']`, fraud rates `[0.4, 1.8, 6.5, 19, 41]` (%), minimum bar height 3px.
- **Bar colors (80% alpha):** index 0 green `#27ae60`; index 1 orange `#e67e22`; indices 2-4 red `#e74c3c`. Bold 12px `#333` "N%" labels above; 11px labels below.
- **Annotation:** rising red 2px arrow across the bars with bold red 11px label "each mismatch multiplies risk".
- **Bottom caption (bold blue 11px, centered):** "Score, don't block — travelers, VPN users, and multilingual households mismatch legitimately".

## Device Fingerprinting & Cross-Device Identity

Tags: mechanism (blue), privacy risk (red), defense (green)

- **~33 bits** — combined entropy singles out one device among 8 billion (EFF Panopticlick)
- **Persistence vs drift** — survives cookie deletion and incognito; OS updates decay it
- **Legit use** — banks flag logins from never-seen devices
- **Cross-device stitching** — deterministic (login) or probabilistic (shared IP, co-timing) person graphs
- **Pushback** — ATT gutted IDFA; browsers blur surfaces; GDPR treats fingerprints as personal data

*Example:* Example: Panopticlick showed most "anonymous" browsers were unique among hundreds of thousands tested.

**How it's exploited:** post-ATT, ad-tech traded the deletable IDFA for undeletable fingerprints — regulators treat it as the same tracking without the honesty.

### Visualization (canvas `c8`, 720×300)

Cumulative bar chart. Title (bold 14px `#1a5276`): "Fingerprint Entropy Accumulates — Cumulative Bits of Identity per Signal". Padding top 45, bottom 70, left 50, right 30.

- **Data (y max 42):** signals `['UA string', '+Timezone', '+Screen', '+Fonts', '+Plugins', '+Canvas', '+Audio']`, cumulative bits `[10, 13, 18, 25, 30, 35.5, 38]`.
- **Bars:** bars at or above 33 bits red `#e74c3c`, below `rgba(26,82,118,0.35)`, with matching strokes; bold 11px `#333` labels "N bits" above; signal labels below.
- **Threshold:** dashed red (2px, dash 6/4) horizontal line at 33 bits, labeled bold red 11px "~33 bits: unique among 8 billion devices"; bold red 10px "IDENTIFIED" centered above the "+Canvas" bar.
- **Bottom caption (bold blue 11px, centered):** "Each attribute is innocent alone (a few bits) — combined they name the device without any cookie (EFF Panopticlick)".

## Emulators, Spoofing & Device-Farm Fraud

Tags: abuse (red), defense (green), failure mode (red)

- **Physical tells** — real phones jitter sensors, drain batteries, match claimed GPUs
- **Headless leaks** — webdriver flags, missing plugins, impossible screen/window combos
- **Farm signature** — hundreds of "users" sharing one GPU string, synced battery curves
- **Economics** — fraud concentrates where device moves price: iOS CPMs, install payouts
- **Defense rule** — score consistency across independent signals, never one claim

*Example:* Example: a thousand "iPhones" reporting one Android GPU renderer string is a farm.

**Failure mode:** aggressive emulator blocking hits accessibility tools, testers, and cheap legitimate devices — keep a human-review path.

### Visualization (canvas `c9`, 720×300)

Grouped bar chart. Title (bold 14px `#1a5276`): "Environment Red Flags — Prevalence in Human vs Bot Traffic (%)". Padding top 45, bottom 80, left 40, right 145.

- **Data (y max 90):** flags (two-line labels) `['No sensor jitter', 'GPU / model mismatch', 'Webdriver flag set', 'Flat battery curve']`; human traffic `[2, 1, 0.5, 3]` (%, green `#27ae60`, min bar height 3px, green 11px labels); bot traffic `[78, 64, 52, 71]` (%, red `#e74c3c`, bold red 12px labels).
- **Layout:** 4 groups (group width 120, gap 15, bar width 42); labels `#333` 11px on two lines below.
- **Legend (x = w-135):** green swatch "Human traffic"; red swatch "Bot / farm".
- **Bottom caption (bold blue 11px, centered):** "No single flag convicts — score physical consistency across independent signals (model vs GPU vs sensors vs battery)".

## OS / App Version Cohorts & the Rollout Artifact

Tags: bias (orange), failure mode (red), best practice (green)

- **The artifact** — power users adopt first; average falls as mix broadens
- **Both directions** — the same mix effect can fake wins too
- **Fix** — compare within cohorts, use holdbacks, wait for rollout completion
- **Version drag** — newest-OS features exclude oldest-device, highest-churn-risk users
- **Compounding** — version correlates with device tier, OS, and tenure

*Example:* Example: an "engagement regression" mirrored the rollout curve; per-cohort engagement never moved.

**Rule of thumb:** any metric shift coinciding with a rollout curve is a cohort artifact until segmented per version — put rollout percentage on every release dashboard.

### Visualization (canvas `c10`, 720×300)

Three-series line chart over a 14-day rollout. Title (bold 14px `#1a5276`): "The Rollout Artifact — an \"Engagement Drop\" That Is Just Cohort Mix". Padding top 45, bottom 60, left 50, right 155; L-shaped gray axes.

- **Data (14 daily points, y max 100):** rollout % `[3, 8, 15, 25, 38, 52, 64, 74, 82, 88, 92, 95, 97, 99]` — dashed green `#27ae60` 3px (dash 6/4); topline engagement `[92, 90, 86, 80, 74, 68, 64, 61, 59, 58, 57, 57, 56, 56]` — solid red `#e74c3c` 3px; per-cohort engagement `[57, 57, 56, 57, 57, 56, 57, 57, 56, 57, 57, 56, 57, 56]` — dotted blue `#1a5276` 2px (dash 2/3).
- **Annotations:** bold orange 11px two lines "power users adopt first," / "casual users dilute the average" at ~22% width, ~30% height; bold blue 10px "within-cohort engagement: flat" near the flat blue line.
- **X labels:** `#555` 11px "Day 1" left, "Day 14" right.
- **Legend (x = w-148):** red swatch "Topline avg"; green swatch "Rollout %"; blue swatch "Per-cohort".
- **Bottom caption (bold blue 11px, centered):** "The topline mirrors the rollout curve while per-cohort engagement never moves — a mix effect, not a regression".

## Closing callout

**The meta-point:** ambient context is the strongest "free" feature set — it predicts behavior before the user does anything. But every context signal is also a proxy (device for wealth, battery for desperation, locale mismatch for fraud, version for user type), so the same features that lift a model can quietly encode discrimination, invite spoofing, or manufacture fake trends. Use context to adapt the experience; verify it before trusting it; audit it before letting it set prices or judge releases.

## Regeneration instructions

- **Layout:** most-powerful-signals compact style. One `.card-section` per topic: `<h2>` (unnumbered, 1.3rem `#1a5276`, 2px solid `#2980b9` bottom border), then a `table.layout` with one row — left `td.text-col` (45%) holding `.tags` pill row, a `<ul>` of labeled bullets (`<li><b>Label</b> — text`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) holding one `<canvas width="720" height="300">` scaled to `width: 100%` with 1px `#e0e0e0` border, 4px radius. After the last section, a standalone full-width `.key-point` div holds the closing meta-point callout.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `<strong>` lead-in label.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem with `li b` in `#1a5276`; `.example` italic `#555` 0.9rem. No nav bar, no back/home links.
- **Canvas:** shared `setup(id)` helper — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates, logical size 720×300.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#444`/`#555`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
