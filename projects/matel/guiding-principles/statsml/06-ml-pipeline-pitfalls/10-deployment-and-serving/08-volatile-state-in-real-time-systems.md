# Pitfall: Volatile State in Real-Time Systems

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Volatile State in Real-Time Systems

**Subtitle:** Data changes between API calls, model sees inconsistent state causing prediction errors.

## The Problem

Tags: `the trap` (red), `volatile state` (blue)

- **Sequential lookups** — serving fetches features one at a time while live state keeps changing
- **Race conditions** — a logout between fetches yields logged_in=true with session_id=null
- **No snapshot isolation** — each lookup reads current state, so the vector mixes moments in time
- **Cross-feature drift** — item_price and cart_total diverge when a price update lands mid-request
- **Clock skew** — a 5-minute event count and session status are computed 200ms apart and disagree
- **Impossible states** — the model receives feature combinations that never coexisted in reality

*Example:* A fraud model reads the balance, a legitimate withdrawal posts, then it reads a transaction exceeding that balance and flags fraud — a 12% false-positive rate in busy hours.

**Impact:** The model sees feature combinations never present in training, and the inconsistent state is gone before the error is even logged.

### Visualization (canvas `c1`, 720×300)

Timeline diagram: three sequential feature fetches interleaved with a state change, combined into an impossible feature vector.

- **Title (bold 14px `#1a5276`, top center):** "Volatile State: Features from Different Time Snapshots".
- **Timeline:** gray `#666` 2px horizontal line at y=60 from x=40 to x=680.
- **Three fetch points (colored dot radius 5 on the line, bold 11px time label above, and a 100×35 outlined box below with fetch name and value):** at x=100 "T=0ms" — "Fetch A" / "active_bids=3" in green `#27ae60`; at x=300 "T=50ms" — "Fetch B" / "price=$45" in blue `#2980b9`; at x=500 "T=100ms" — "Fetch C" / "inventory=12" in orange `#e67e22`.
- **State change:** "⚡ NEW BID ARRIVES" in bold 11px red at (400, 140) with a red dashed 1.5px line (dash 4/4) up to the timeline at x=400.
- **Combined feature vector box (150, 170, 420×70, red `#e74c3c` 3px border):** header "Combined Feature Vector (impossible state)" in bold 13px `#1a5276`; monospace 11px values in their source colors: `active_bids=3` (green), `price=$45` (blue), `inventory=12` (orange); below in bold 10px red centered: "(but state changed between fetches → never existed together)".
- **Bottom annotation (bold 12px red, centered):** "Model trained on coherent snapshots, serves on inconsistent mashups".

## Why It Happens

Tags: `root cause` (orange), `staleness` (blue)

- **Live reads** — serving reads from systems that keep changing underneath the request
- **No shared moment** — nothing in the read path guarantees two lookups observe the same instant
- **Read-to-serve lag** — a balance can change between the feature lookup and the served prediction
- **Stale caches** — the feature store returns precomputed values that lag behind reality
- **Concurrent writers** — one request updates the state that another request just read

*Example:* Credit scoring reads account_balance=$5000 at T=0, a $4000 withdrawal posts by T=200ms, and the model approves on the stale $5000.

**Root Cause:** Features are treated as static snapshots when they are volatile state, so the read-to-serve window lets reality diverge.

### Visualization (canvas `c2`, 720×300)

Two-line timeline chart: a flat cached value versus the real value dropping after a withdrawal, with a stale gap at serve time.

- **Title (bold 14px `#1a5276`, top center):** "Feature Staleness: Reality Diverges from Cached Value".
- **Timeline axis:** gray `#666` 2px line at y=140 from x=60 to x=680 with arrowhead and "time →" label (11px `#666`).
- **Markers (2px ticks with bold 11px label and 10px sublabel):** x=120 "T=0ms" / "Feature Read"; x=370 "T=100ms" / "State Changes"; x=600 "T=200ms" / "Prediction Served".
- **Cached line:** orange `#e67e22` dashed 2.5px line (dash 6/4) flat at y=80 from x=120 to x=600, labeled "Cached: balance = $5000" in bold 11px orange above.
- **Real line:** green `#27ae60` solid 2.5px line at y=80 from x=120 to x=360, dropping to y=220 by x=380, then flat to x=600; labeled "Real: balance = $1000 (after withdrawal)" in bold 11px green below.
- **Event marker:** "⚡ $4000 withdrawal" in bold 11px red at (370, 50) with red dashed 1.5px line (dash 3/3) down to the axis.
- **Stale gap:** red 2px vertical double-arrow line at x=610 spanning y 80–220, labeled "STALE" / "GAP" in bold 11px red and "$4000 error" in 10px red to its right.
- **Bottom annotation (bold 12px red, centered):** "Model approved loan based on stale $5000 — real balance was $1000".

## The Correct Approach

Tags: `the fix` (green), `freshness` (blue)

- **Freshness as a contract** — make feature age a first-class property of the serving path
- **Snapshot reads** — fetch all features in one transaction or batch so they share a timestamp
- **Timestamp everything** — attach a read time to each value so the model knows feature age
- **Consistency checks** — reject or recompute when features violate expected relationships
- **Freshness gates** — re-fetch key features before acting when a critical decision finds them old

*Example:* The feature vector carries balance_read_at=T0, and if the value is older than a 500ms tolerance at approval time, the serving layer re-fetches the balance.

**Fix:** Every feature value should carry a timestamp, and critical decisions should verify feature freshness before execution.

### Visualization (canvas `c3`, 720×300)

Pipeline flowchart: snapshot read → model prediction → freshness-gate diamond with three outcomes.

- **Title (bold 14px `#1a5276`, top center):** "Correct: Snapshot Read + Staleness Check Before Serving".
- **Snapshot Read box (30, 45, 180×80, fill `rgba(39,174,96,0.1)`, `#27ae60` 2px border):** header "Snapshot Read" in bold 12px green; 10px monospace `#333`: `balance = $5000`, `read_at = T0`, `All features @ T0`. Gray 2px arrow to the next box.
- **Model Prediction box (265, 45, 150×80, fill `rgba(26,82,118,0.1)`, `#1a5276` 2px border):** header "Model Prediction" in bold 12px `#1a5276`; monospace: `decision = APPROVE`, `confidence = 0.87`, `feature_age = T0`. Gray arrow to the gate.
- **Freshness gate diamond (vertices (540,50), (600,85), (540,120), (480,85), fill `rgba(230,126,34,0.1)`, `#e67e22` 2px border):** label "VERIFY" / "FRESHNESS" in bold 10px orange.
- **Three outcome boxes (120×55 each at y=170, with colored 2px arrows from the diamond):**
  - FRESH (380, 170) — fill `rgba(39,174,96,0.15)`, green border; bold 11px "FRESH", 9px `#333`: "age < 500ms", "→ Serve prediction".
  - STALE (ok) (480, 170) — fill `rgba(230,126,34,0.15)`, orange border; "STALE (ok)", "500ms < age < 2s", "→ Re-fetch, then serve".
  - STALE (reject) (580, 170) — fill `rgba(231,76,60,0.15)`, red border; "STALE (reject)", "age > 2s", "→ Reject, re-compute".
- **Legend (bottom left, bold 10px `#333` with 12px color swatches):** green — "Fresh — serve immediately"; orange — "Stale within tolerance — re-fetch then serve"; red — "Stale beyond tolerance — reject and recompute from scratch".

## Regeneration instructions

- **Layout:** three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"), each with an h2 underlined by `2px solid #2980b9` and a `table.layout` (one `<tr>`): left `<td class="text-col">` (45%) holds `.tags` pills, a `<ul>` of labeled bullets, a `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (55%) holds one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; `li b` in `#1a5276`; `ul` 0.92rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Callouts:** `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, `1px solid #e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; accent `#2980b9`; text `#2c3e50`/`#333`/`#666`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
