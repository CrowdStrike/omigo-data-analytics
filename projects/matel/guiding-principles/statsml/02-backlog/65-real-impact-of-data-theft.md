# Real Impact of Data Theft

**Page type:** detail page (backlog-style two-column layout: numbered h2 sections, text left ~45%, canvas right ~55%; DISCUSS status badge next to h1)
**HTML title tag:** Real Impact of Data Theft

**Status badge (next to h1):** DISCUSS

**Subtitle:** What actually happens when credentials leak — password reuse, identity deduplication across products, and the compounding blast radius

## Intro callout

A single stolen password is tied to one account. But people reuse passwords across services. With identity deduplication (matching email, phone, username patterns across breached datasets from different products), attackers build a credential graph — one breach unlocks many accounts.

## 1. Core Observation

The naive view: breach at service X compromises accounts at service X. The reality: a reused credential propagates through every service that shares it, and attackers actively resolve identities across leaks to find those links.

**Key point callout:** **The real damage isn't one account.** It's the combinatorial explosion when cross-product identity resolution links a reused credential to banking, email, cloud storage, and social accounts simultaneously.

### Visualization (canvas `c1`, 720×320)

Diagram: one breach box fanning out to four account boxes via arrows (blast radius of a reused password).

- **Title (bold 14px, `#1a5276`, top center):** "Blast Radius of One Reused Password".
- **Source box:** at x=60, y=130, 160×60, solid red (`#e74c3c` fill and stroke), white text, two lines: "1 breach" (bold) / "password leaked".
- **Target boxes:** four boxes at x=480, 180×44 each, fill `rgba(26,82,118,0.35)`, stroke `#1a5276`, dark text; labels top to bottom (y = 55, 115, 175, 235): "banking", "email", "cloud storage", "social".
- **Arrows:** orange (`#e67e22`, width 2) lines from source box's right edge center to each target box's left edge, each ending in a filled orange arrowhead.
- **Label (13px `#444`, centered at x=350, y=150):** "same password".
- **Caption (13px `#444`, bottom center):** "one credential, tested everywhere the same identity appears".

## 2. Discussion Points

- **Password reuse rate:** estimates suggest 60-70% of users reuse passwords across services — what does the actual distribution look like?
- **Identity dedup across breaches:** attackers perform entity resolution (email, phone, username overlap) across leaked datasets to build unified profiles
- **Credential stuffing at scale:** automated tools test leaked credentials against hundreds of services — success rate per reused password
- **Blast radius math:** if a user has N accounts and reuses the same password on K of them, a single breach exposes K accounts, not 1
- **Time-to-exploit:** delay between breach and credential use — some are immediate, some are stockpiled for months
- **Password managers vs reality:** adoption rate is low; even users who have them often have legacy reused passwords from before adoption

### Visualization (canvas `c2`, 720×320)

Flow diagram: three breach boxes feeding into an entity-resolution box, which feeds a unified-profile box.

- **Title (bold 14px, `#1a5276`, top center):** "Identity Dedup: Entity Resolution Across Leaked Datasets".
- **Left column:** three white boxes (140×52, stroke `#1a5276`) at x=30, y = 50, 130, 210, each two lines (first bold): "Breach A" / "retail site"; "Breach B" / "forum"; "Breach C" / "fitness app".
- **Middle box:** at x=260, y=118, 190×76, fill `rgba(26,82,118,0.35)`, stroke `#1a5276`, three lines: "entity resolution" (bold) / "email / phone /" / "username overlap".
- **Right box:** at x=530, y=118, 160×76, solid red `#e74c3c`, white text, three lines: "unified profile" (bold) / "credential graph" / "→ stuffing".
- **Arrows:** orange `#e67e22` (width 2) lines from each breach box's right edge converging on the middle box's left side; one horizontal arrow with filled orange arrowhead from middle box to right box.
- **Caption (13px `#444`, bottom center):** "separate leaks look small; joined on shared identifiers they become one large attack surface".

## 3. The Data Science Angle

| Question | Why It Matters |
|----------|----------------|
| How many unique identities can be resolved across N breached datasets? | Measures the true population at risk vs. naive per-breach counts |
| What's the overlap distribution of credentials across services? | Power-law vs. uniform — are some users disproportionately exposed? |
| Does breach severity compound non-linearly with reuse? | One password matching 5 accounts is worse than 5× one account |
| How do attackers prioritize targets after dedup? | High-value accounts (financial, email) get tested first — selection bias in exploit data |

### Visualization (canvas `c3`, 720×320)

Bar chart: accounts exposed as a function of reuse count K.

- **Title (bold 14px, `#1a5276`, top center):** "Blast Radius: One Breach Exposes K Accounts, Not 1".
- **Data:** K = 1..6, bar height proportional to K (linear scale, max 6). X labels below bars: "K=1" … "K=6"; bold value labels above bars: "1 account", "2 accounts", "3 accounts", "4 accounts", "5 accounts", "6 accounts".
- **Axes:** L-shaped gray (`#999`) axis, plot area x 90–660, y 55–250; bars evenly spaced with 14px inset each side of the slot.
- **Bar colors:** K=1 green `#27ae60`; K=2–4 `rgba(26,82,118,0.35)`; K=5–6 red `#e74c3c`.
- **Caption (13px `#444`, bottom center):** "K = services sharing the password. Damage compounds beyond K if one of them is email (recovery channel)."

## 4. Related Concerns

- Security questions that are the same across services (mother's maiden name, first pet)
- Phone number as universal identifier — SIM swap attacks compound this
- Email as both identity and recovery channel — single point of failure
- 2FA fatigue and bypass techniques when the identity graph is known

### Visualization (canvas `c4`, 720×320)

Hub-and-spoke diagram: two red hub circles with three spoke boxes each.

- **Title (bold 14px, `#1a5276`, top center):** "Universal Identifiers as Single Points of Failure".
- **Hubs:** red (`#e74c3c`) filled circles, radius 46, white text, two lines (label bold 13px, sub 11px): at (200,155) "email" / "identity + recovery"; at (520,155) "phone number" / "SIM swap target".
- **Spokes:** white boxes 130×30 with `#1a5276` stroke, connected to their hub by orange (`#e67e22`, width 1.5) lines. Email hub spokes (offsets from hub): "account logins" (-130,-75), "password resets" (-130,+75), "2FA codes" (0,+105). Phone hub spokes: "SMS 2FA" (+130,-75), "recovery calls" (+130,+75), "security questions" (0,+105).
- **Caption (13px `#444`, bottom center):** "compromise the hub and every spoke falls — 2FA fatigue finishes the job once the graph is known".

## Regeneration instructions

- **Layout:** backlog-style detail page. h1 with inline `.status` badge, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2; inside each, `table.layout` with a single row: left `td.text-col` (45%) holding paragraphs/bullets/`.key-point`/`table.data`, right `td.viz-col` (55%) holding the canvas.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; section h2 1.3rem `#1a5276` with the same 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; bullets 0.92rem.
- **Callouts:** `.intro` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Status badge:** `.status` inline-block, background `#fef9e7`, border `1px solid #f39c12`, color `#b7950b`, padding 2px 10px, radius 4px, 0.75em, text "DISCUSS".
- **Data table:** `table.data` full width, left-aligned cells, padding 6px 10px, `1px solid #eee` bottom borders, 0.88rem; th background `#f8f9fa`, color `#1a5276`, weight 600.
- **Canvas:** each declared 720×320 with `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled via a shared `setup(id, hgt)` helper using `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); a shared `box()` helper draws labeled rectangles (first line bold 13px system-ui, subsequent lines 12px).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`. No nav bar, no back/home links. (If this page were linked from a grid, regenerated HTML links use `.html` extensions.)
