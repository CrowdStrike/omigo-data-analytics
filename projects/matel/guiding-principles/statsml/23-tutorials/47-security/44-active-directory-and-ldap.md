# Active Directory & LDAP

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Active Directory & LDAP

**Subtitle:** A company of 10,000 people needs one authoritative list of who exists and what they may touch — LDAP is how apps ask that list, and Active Directory is the version most companies run

## Ten Thousand People, Three Hundred Apps, One List

**Tags:** `core idea` (blue), `identity` (green), `one registry` (orange)

- **The company** — Meridian Manufacturing: 10,000 employees, 300 internal apps, hires and exits every week
- **The old way** — every app keeps its own user list; one new hire means 300 separate account creations
- **The drift** — 300 lists never agree: a leaver is removed from 250 apps and forgotten in the other 50
- **The fix** — one directory holds every person, group, and machine; apps query it and store nothing
- **The name** — this shared registry is the enterprise directory: the phone book every app trusts

*Example (italic):* Priya joins Meridian on Monday; one directory entry is created, and payroll, email, wiki, and VPN all see her instantly — nobody touched 300 apps.

**Key point:** An enterprise directory is the single authoritative registry of people, groups, and machines — apps ask it "who is this and what may they touch" instead of keeping their own answer.

### Visualization (canvas `c1`, 720×300)

Two-panel flow diagram: a new hire fanning out to per-app user lists (left) vs a single directory entry that apps query (right).

- **Title (bold 15px, `#1a5276`, top center):** "One New Hire: 300 App Updates vs One Directory Entry".
- **Divider:** vertical 1px `#e5e9ef` line at x=360, from y=45 to y=280.
- **Left panel, label 12px `#444` at (30, 58):** "no directory". Blue `#2a78d6` rounded box (110×34, fill `rgba(42,120,214,0.15)`) at (40, 135) labeled "new hire: Priya" (12px `#2c3e50`); four 2px `#6b7280` arrows fanning from its right edge to orange `#d95926` boxes (130×30, fill `rgba(217,89,38,0.12)`) at x=210, y = 75 / 125 / 175 / 225, labeled "Payroll — own list", "Email — own list", "Wiki — own list", "VPN — own list"; 12px `#6b7280` text "…296 more apps" at (225, 268); bold 12px red `#e74c3c` "300 separate updates" at (40, 210).
- **Right panel, label 12px `#444` at (390, 58):** "with a directory". Blue box (110×34) at (385, 135) labeled "new hire: Priya"; one 3px `#008300` arrow to a green box (150×40, fill `rgba(0,131,0,0.12)`) at (520, 132) labeled "directory: 1 entry"; four thin 1px `#6b7280` lines from the directory box to small mute-outlined boxes (70×24) at x=630, y = 70 / 115 / 200 / 245, labeled "Payroll", "Email", "Wiki", "VPN" (11px).
- **Annotation (bold 13px green `#008300`, at (390, 272)):** "apps query the directory — they keep no user lists".
- **Caption (12px `#444`, bottom left at (30, 292)):** "4 of 300 apps shown".

## Reading the Tree: Priya's Full Address

**Tags:** `worked example` (blue), `LDAP tree` (green), `groups` (orange)

- **The shape** — the directory is a tree: company at the root, organizational units (OUs) as branches
- **The address** — a distinguished name (DN) is the path from leaf to root: cn=Priya Rao, ou=Staff, dc=meridian, dc=com
- **The attributes** — each entry carries fields any app can read: mail, title, memberOf
- **The protocol** — LDAP is the open query language for this tree; any app on any platform can ask it
- **The permission trick** — payroll never grants Priya access; it asks "is she in cn=Finance?" (42 members)
- **Hand-check** — adding accountant #43 to Finance is one group edit; zero apps change their own config

*Example (italic):* Payroll sends one LDAP query — is cn=Priya Rao a memberOf cn=Finance? — gets "yes", and lets her in; the group's 42 members were never listed inside payroll.

**Key point:** LDAP queries a tree of named entries, and groups turn access management into group management — apps check membership, so one group edit replaces hundreds of per-app grants.

### Visualization (canvas `c2`, 720×300)

Tree diagram of Meridian's directory: root, three OUs, and leaf entries, with Priya's DN spelled out and her memberOf edge to the Finance group.

- **Title (bold 15px, `#1a5276`, top center):** "The Directory Is a Tree: Priya's Distinguished Name".
- **Root box:** ink-outlined rounded box (170×30, fill `rgba(26,82,118,0.10)`) centered at x=360, y=48, labeled "dc=meridian,dc=com" (12px `#2c3e50`).
- **OU row (y=112, each 130×30, fill `rgba(42,120,214,0.15)`, 12px labels):** "ou=Staff" at x=110, "ou=Groups" at x=310, "ou=Machines" at x=520; 2px `#6b7280` connector lines from root bottom to each OU top.
- **Leaf under ou=Staff:** green `#008300` box (200×56, fill `rgba(0,131,0,0.12)`) at (60, 172), three 12px lines: "cn=Priya Rao", "mail: priya.rao@…", "title: Accountant".
- **Leaf under ou=Groups:** orange `#d95926` box (185×40, fill `rgba(217,89,38,0.12)`) at (295, 180), labeled "cn=Finance" with 12px sub-line "42 members".
- **Leaf under ou=Machines:** mute-outlined box (160×30) at (510, 185), 12px `#6b7280` label "cn=LAPTOP-4471".
- **memberOf edge:** dashed violet `#4a3aa7` (dash 5/4) 2px arrow from Priya's box right edge to the Finance box, bold 12px violet label "memberOf" above it.
- **Annotation (bold 12px green `#008300`, at (330, 246)):** "payroll asks: is Priya in Finance? — yes".
- **DN callout (bold 12px `#1a5276`, at (40, 278)):** "DN = path from leaf to root: cn=Priya Rao, ou=Staff, dc=meridian, dc=com".

## Why Attackers Aim at the Directory

**Tags:** `why it matters` (blue), `blast radius` (red), `defense` (green)

- **More than a phone book** — Active Directory bundles the directory with Kerberos logins and group policy
- **The control plane** — group policy pushes settings and software to every joined machine automatically
- **The prize** — control the directory and you control all 10,000 accounts and all 8,500 machines
- **The pattern** — enterprise breach write-ups publicly document this: attackers pivot toward domain admin
- **The fortress** — that is why domain-admin accounts get separate workstations, vaults, and short sessions
- **The cloud echo** — cloud identity directories are the same registry re-platformed, and the same prize

*Example (italic):* A stolen laptop gives an attacker 1 machine; a stolen domain-admin credential gives them the directory itself — 18,500 objects, every account and machine at Meridian.

**Key point:** The directory is the one system that owns every other login — which is why it is the crown-jewel target in enterprise breaches, and why its admin accounts get fortress treatment.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: blast radius of one compromised credential at four privilege levels, from a single laptop to domain admin.

- **Title (bold 15px, `#1a5276`, top center):** "Blast Radius: What One Stolen Credential Controls".
- **Axis:** vertical 2px `#999` baseline at x=250; bars extend right, max width 430; log-feel via hardcoded pixel widths, not a real log axis.
- **Rows (bar centers at y = 80, 130, 180, 230; 16px tall bars; left-aligned 12px `#444` labels at x=20):**
  - "one laptop login — 1 machine": blue `#2a78d6` bar width 8
  - "payroll app admin — 350 accounts": blue bar width 130
  - "helpdesk account — 2,000 resets": orange `#d95926` bar width 240
  - "domain admin — 18,500 objects": red `#e74c3c` bar width 430, bold 12px red label "every account + every machine" above the bar
- **Bar style:** fills at 0.85 alpha, 11px `#444` count labels just past each bar end (1 / 350 / 2,000 / 18,500).
- **Annotation (bold 13px magenta `#d55181`, at (250, 265)):** "the directory owns all the other accounts".
- **Caption (12px `#444`, bottom right):** "counts illustrative; bar widths schematic".

## The Slow Leak: Movers Who Never Lose Access

**Tags:** `common mistake` (red), `access creep` (orange)

- **Joiner, mover, leaver** — access should track the role: granted on join, changed on move, cut on exit
- **The creep** — moves add new groups but old ones are rarely removed; access only accumulates
- **The count** — Priya joins with 5 groups; after three moves in eight years she holds 23
- **Hand-check** — her current role needs 7 groups, so 23 − 7 = 16 grants are pure leftover risk
- **The leaver twin** — accounts of people who left long ago often stay enabled: free keys, nobody watching
- **The defense** — periodic access reviews: recertify each membership or remove it, and disable on exit day

*Example (italic):* Priya moved from accounting to procurement to sales ops; each move added groups and removed none, so she can still approve invoices two jobs later.

**Common mistake:** Treating access as grant-once. Without joiner/mover/leaver hygiene, the directory silently accumulates stale accounts and over-broad memberships — the chronic, unglamorous risk that most audits find first.

### Visualization (canvas `c4`, 720×300)

Line chart of one employee's group count over eight years and three role moves: memberships climb in steps and never come down, against a dashed line at what the current role actually needs.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Years, Three Moves: Group Count Only Goes Up".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = years 0 to 8, 12px `#444` tick labels every 2 years ("yr 0" … "yr 8"); y = groups held 0 to 25, gridlines `#e5e9ef` at 5/10/15/20 with 12px labels.
- **Creep line:** blue `#2a78d6` 3px stepped line through years `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, groups `[5, 6, 11, 12, 13, 17, 18, 22, 23]` — jumps at each move, never drops.
- **Move markers:** vertical dashed `#6b7280` (dash 4/3) lines at years 2, 5, 7, each with a 12px `#6b7280` label "move" at its top.
- **Needed line:** dashed green `#008300` (dash 6/4) 2px horizontal line at groups = 7, 12px green label "current role needs 7" at its right end.
- **Annotation (bold 13px red `#e74c3c`, near x = year 4.5, y=75):** "16 groups nobody ever removed".
- **Caption (12px `#444`, bottom right):** "group counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and counts above (no randomness); Meridian, its 10,000 employees, 300 apps, 8,500 machines, the 42-member Finance group, the blast-radius counts, and the group-creep series `[5, 6, 11, 12, 13, 17, 18, 22, 23]` are invented and labeled illustrative; text numbers must stay in sync with these chart numbers.
- **Framing:** defensive/educational only — the attacker material is limited to publicly well-documented breach patterns and motivates the defenses (fortress admin accounts, access reviews, leaver disablement); no attack procedures.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
