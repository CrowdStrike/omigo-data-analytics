# Leaking Private Data Through the Wrong Setting

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Leaking Private Data Through the Wrong Setting

**Subtitle:** Same question, same model, wrong door — the plan the account sits on, and only then the toggle nobody opened, decide whether your data stayed private

## The Same Prompt, Four Different Exposures

**Tags:** `core idea` (blue), `plan, not content` (green)

- **The moment** — one paragraph of a customer contract is pasted in with a request for a plain-English summary
- **Door 1** — a free personal plan: no contractual no-training commitment is offered on it at all
- **Door 2** — an entry-level personal paid plan: the payment buys speed and limits, not a no-training term
- **Door 3** — a team or business workspace seat: no-training can be stated contractually, plus retention and admin controls
- **Door 4** — a contracted enterprise endpoint: the same commitment in a signed agreement that can be pointed to
- **Identical text** — the words, the model behind them, and the answer that comes back can be the same
- **The screen is silent** — nothing in the chat box names the plan; all four look and feel identical
- **The consequence** — the protection is a property of the plan, so no setting on doors 1 and 2 can create it

*Example (italic):* The same 200 words travel down all four routes; only the plan the account sits on differs.

**Key point:** You cannot toggle your way into a protection the plan does not sell — check the plan first, the settings second.

### Visualization (canvas `c1`, 720×300)

One prompt fanning into four plan lanes, with the no-training commitment shown as available or not per plan.

- **Title (bold 15px, `#1a5276`, top center):** "One Prompt, Four Plans, Four Regimes".
- **Prompt box:** rounded rect x=24, y=116, 150×64, radius 6, fill `rgba(26,82,118,0.08)`, 2px `#1a5276` border; bold 12px `#1a5276` centered at (99, 140) and (99, 158): "one pasted", "paragraph".
- **Four lanes:** rounded rects 380×42 (radius 6) at x=280, tops y = `[56, 104, 152, 200]`; header bold 12px in lane color, left-aligned at (296, top+17); sub 11px `#2c3e50` left-aligned at (296, top+34); right note bold 10px in lane color, right-aligned at (652, top+27); exposure marker filled circle r=6 at (678, top+21) in the lane color.
  - **Lane 1 (magenta `#d55181`, fill `rgba(213,81,129,0.07)`):** header "free personal plan"; sub "no-training commitment: not offered"; note "not offered".
  - **Lane 2 (yellow `#c98500`, fill `rgba(201,133,0,0.07)`):** header "entry-level personal paid plan"; sub "paid for speed, not for a no-training term"; note "not offered".
  - **Lane 3 (blue `#2a78d6`, fill `rgba(42,120,214,0.07)`):** header "team / business workspace seat"; sub "no-training term, retention, admin controls"; note "offered".
  - **Lane 4 (green `#008300`, fill `rgba(0,131,0,0.07)`):** header "contracted enterprise endpoint"; sub "no-training written into the agreement"; note "in contract".
- **Arrows:** 1.5px `#6b7280` from the prompt box right edge (174, 148) fanning to each lane's left edge x=274 at top+21, arrowheads at (279, top+21).
- **Annotation line 1 (bold 12px orange `#d95926`, centered at y=262):** "the words are identical in all four — the plan decides whether a commitment exists".
- **Annotation line 2 (11px `#6b7280`, centered at y=280):** "lanes 1-2 have no protection to configure; lanes 3-4 have one, and it can still be left off".
- **Caption (11px `#444`, right-aligned at x=708, y=296):** "typical patterns, simplified".

## How Data Reaches an Unintended Route

**Tags:** `failure modes` (orange), `detection gap` (blue), `common mistake` (red)

- **Two distinct causes** — either the plan never offered the protection, or it did and the setting was left off
- **Wrong plan** — a seat that takes weeks can push users to a free plan; shared machines keep consumer sessions signed in
- **Wrong route: relay** — an extension or unofficial wrapper app forwards the prompt through its own server first
- **Wrong scope: rides along** — screenshots catch extra rows, workbooks hide sheets, connectors pull whole mailboxes
- **Wrong scope: not yours** — pasting a customer's records shares data you hold on their behalf, not your own
- **Wrong setting** — on a plan that does offer the commitment, a per-account toggle sat at its shipped default
- **No signal** — the wrong door answers instantly and just as well, and writes nothing to any company log
- **Invisible by construction** — dashboards count only the sanctioned tier; a cellular phone skips the network edge
- **Long lag** — exposure surfaces months later, via a subject-access request or an incident review

*Example (illustrative):* A dashboard showing 4,200 prompts a day looked healthy until a survey found another 3,300 on personal plans — 44.0% of all usage was never on the chart.

**Common mistake:** Treating "nothing broke" as evidence nothing leaked — the wrong plan and the right plan return the same answer and the same silence.

### Visualization (canvas `c2`, 720×300)

Two panels: the seven leak paths on the left, the admin visibility gap on the right, split by a vertical divider.

- **Title (bold 15px, `#1a5276`, top center):** "Seven Paths Out, and What the Dashboard Sees".
- **Divider:** 1px `#e5e9ef` vertical line at x=402 from y=40 to y=252.
- **Left panel header (bold 12px `#1a5276`, centered at (200, 46)):** "seven ways one question leaves".
- **Source box:** rounded rect x=16, y=118, 112×56, radius 6, fill `rgba(26,82,118,0.08)`, 2px `#1a5276` border; bold 11px `#1a5276` centered at (72, 142) and (72, 158): "one question", "to ask".
- **Seven branch rows:** rounded rects 246×22 (radius 5) at x=138, tops y = `[56, 82, 108, 134, 160, 186, 212]`; fill the row color at 0.07 alpha, 1.5px border in the row color; label bold 11px in the row color, left-aligned at (148, top+15). Row color encodes the cause; no per-row text tag (the legend carries it).
  - Row 1 — magenta `#d55181`: "free personal plan, no seat yet".
  - Row 2 — magenta `#d55181`: "consumer session on shared machine".
  - Row 3 — orange `#d95926`: "extension or wrapper relays prompt".
  - Row 4 — blue `#2a78d6`: "screenshot or file over-shares".
  - Row 5 — blue `#2a78d6`: "connector pulls a whole mailbox".
  - Row 6 — blue `#2a78d6`: "pasted customer data, not yours".
  - Row 7 — violet `#4a3aa7`, **dashed** 1.5px border: "improvement toggle left at default".
- **Arrows:** 1.5px `#6b7280` from the source box right edge (128, 146) to each row's left edge x=132 at top+11, arrowheads at (137, top+11).
- **Legend (two lines, 11px, left-aligned at x=16, y=274 and y=290):** line 1 `#6b7280` "cause by colour:" then magenta `#d55181` "wrong plan" and orange `#d95926` "wrong route" drawn as coloured runs on the same line; line 2 blue `#2a78d6` "wrong scope" and violet `#4a3aa7` "wrong setting". Implemented as sequential `fillText` calls advancing x by `measureText` so each phrase carries its own colour.
- **Right panel header (bold 12px `#1a5276`, centered at (556, 46)):** "and the admin only sees part of it".
- **Hardcoded data (the only numbers in the file):** `sanctioned = 4200`, `unsanctioned = 3300`. Computed at render time: `total = sanctioned + unsanctioned` (7,500) and `invisiblePct = unsanctioned / total * 100` (44.0). Neither the total nor the percentage is ever written as a literal.
- **Bars:** baseline 1px `#999` from (430, 246) to (694, 246); plot height 146px maps to `total`.
  - **Bar 1 (x=460, width 76):** height `sanctioned/total*146`, fill `rgba(42,120,214,0.20)`, 2px `#2a78d6` stroke; bold 12px `#2a78d6` centered 7px above the bar top printing `sanctioned.toLocaleString()`; 11px `#2c3e50` centered at (498, 262): "dashboard".
  - **Bar 2 (x=580, width 76):** lower segment `sanctioned` drawn as bar 1; upper segment `unsanctioned` fill `rgba(213,81,129,0.20)`, 2px `#d55181` stroke, so the stack top lands exactly on y=100 (`246 − 146`); bold 12px `#1a5276` centered 7px above the stack printing `total.toLocaleString()`; 11px `#2c3e50` centered at (618, 262): "actual".
- **Invisible-segment label:** bold 11px `#d55181` centered inside the upper segment of bar 2, printing `unsanctioned.toLocaleString()`.
- **Gap annotation (bold 11.5px `#d55181`, centered at (556, 282)):** built at render time as `invisiblePct.toFixed(1) + '% never on the chart'`.
- **Caption (11px `#444`, right-aligned at x=708, y=297):** "Illustrative Example".

## Practices That Actually Reduce It

**Tags:** `controls` (green), `rule of thumb` (blue)

- **Buy the commitment** — a plan that states no training is procurement, not something users can be trained into
- **Provision fast** — a business seat available in days removes the motive to reach for a personal plan
- **Enforce defaults centrally** — admin-set workspace defaults beat asking each person to find a toggle
- **Make use visible** — allow-list or block the relevant domains at the network edge so traffic shows up somewhere
- **Redact before sending** — tokenize names and identifiers on your own side rather than trusting downstream handling
- **Hard exclusions** — credentials and regulated identifiers never belong in a prompt under any plan or setting
- **Log your own side** — keep a prompt-and-response record you control, so an audit does not depend on the vendor
- **The honest limit** — a ban with no fast sanctioned plan just moves the traffic to phones and home networks

*Example (italic):* Vendor A's business tier can go live in four days with training disabled by an admin default, leaving personal-plan routes nothing to offer.

**Key point:** Match the fix to the cause — procurement closes "the plan never offered it", admin-enforced defaults close "the plan offered it and it was off".

### Visualization (canvas `c3`, 720×300)

Controls ladder: each control, the leak it closes, and which kind of fix it is.

- **Title (bold 15px, `#1a5276`, top center):** "Each Control, the Leak It Closes, and Whose Job It Is".
- **Seven rows:** rounded rects 660×26 (radius 5) at x=30, tops y = `[42, 71, 100, 129, 158, 187, 216]`, fill the row color at 0.07 alpha, 1.5px border in the row color.
  - **Step badge:** filled circle r=9 at (52, top+13) in the row color; bold 11px white centered index `1..7`.
  - **Control text:** bold 11.5px in the row color, left-aligned at (72, top+17).
  - **Closes text:** 11px `#6b7280`, left-aligned at (348, top+17), prefixed "closes: ".
  - **Fix-type tag:** bold 10px in the row color, right-aligned at (682, top+17).
  - Row 1 — green `#008300`: "buy a plan that states no training" / "closes: a commitment the plan never made" / "procurement".
  - Row 2 — blue `#2a78d6`: "provision seats in days, not weeks" / "closes: shadow use of personal plans" / "procurement".
  - Row 3 — aqua `#199e70`: "admin-enforced workspace defaults" / "closes: a toggle left off on the right plan" / "admin".
  - Row 4 — orange `#d95926`: "allow-list AI domains at the edge" / "closes: the dashboard visibility gap" / "admin".
  - Row 5 — violet `#4a3aa7`: "redact and tokenize before the prompt" / "closes: identifier over-share" / "practice".
  - Row 6 — yellow `#c98500`: "exclude secrets and regulated IDs" / "closes: worst-case blast radius" / "practice".
  - Row 7 — magenta `#d55181`: "teach the plan distinction, don't ban" / "closes: drift onto phones and home networks" / "training".
- **Divider:** 1px `#e5e9ef` vertical line at x=340 from y=38 to y=246, separating control from consequence.
- **Annotation (bold 12px orange `#d95926`, centered at y=270):** "policy without a fast sanctioned plan just moves the traffic off-network".
- **Caption (11px `#444`, right-aligned at x=708, y=292):** "typical patterns, simplified".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`), copied verbatim from `48-copyright-complications.html`. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%). Canvases are `c1`, `c2`, `c3` — one per section, in order.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Bullet budget: eight per section, with section 2 at nine because it absorbed the detection-gap material; every bullet stays one line at roughly 90-100 characters.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `roundRect` and `arrowHead` helpers as in the reference page.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data integrity:** no `Math.random()` and no seeded generator is needed — c1 and c3 are fixed diagrams, and the only quantitative inputs on the page are c2's two literals `sanctioned = 4200` and `unsanctioned = 3300`. c2's total (7,500), bar heights and its "44.0%" label are all computed from those two numbers at render time, and the prose example in section 2 quotes exactly the same three figures.
- **Primary framing (do not weaken):** the no-training guarantee is a property of the **account plan**. Team/business/enterprise plans typically commit to it contractually with retention limits and admin controls; free and entry-level personal plans generally do not offer the commitment at all, so there is nothing there to configure. The leak therefore has two causes — **wrong plan** (protection does not exist) and **wrong setting on the right plan** (protection exists, default left off) — fixed by procurement and by admin-enforced defaults respectively. The plan tier is the primary axis of c1; the toggle is one row of c2's left panel and one row of c3.
- **Content discipline:** **no vendor or product is named anywhere** — only "a free personal plan", "an entry-level personal paid plan", "a team or business workspace seat", "a contracted enterprise endpoint", and "Vendor A" as a placeholder. No named-actor scenarios. Framing is defensive and educational: risk categories and mitigations only, never evasion or exfiltration technique. No credential, key, or token strings appear at all, real or fake. Unsourced figures are labeled "Illustrative Example"; policy diagrams carry "typical patterns, simplified".
- **Scope boundary:** this page is the operational failure-mode angle (how data escapes in practice, who does it, what stops it). The terms-of-service angle — what "improve our services" decomposes into, and that opt-out is not retroactive — belongs to the sibling page `50-do-your-chats-train-the-model` and is deliberately not repeated here.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
