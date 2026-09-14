# Commercial and Source-Available Licenses

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Commercial and Source-Available Licenses

**Subtitle:** The same code can ship under terms that let you host it for free, or terms that don't — the license is where the business model is written

## From Open Source to Proprietary

**Tags:** `core idea` (blue), `license spectrum` (orange), `two questions` (green)

- **Open source** — an OSI-approved license: you may read, modify, redistribute, and host it for anyone
- **Open core** — a free open source core, with paid modules layered around it and sold by one vendor
- **Source-available** — the source is published for anyone to read, but the license restricts how you use it
- **Proprietary** — you receive a binary and a license agreement; the source never leaves the vendor
- **Two separate questions** — "can I see the source" and "may I use it however I want" are not the same question
- **Source-available answers** — yes to the first, no to the second; readable code is not permission to deploy it
- **Where the money is** — revenue comes from support, hosting, or a paid tier, never from the license text itself
- **Terms attach to versions** — a project can move along this spectrum between releases, so re-read the file each bump

*Example (italic):* A repository whose source you can read line by line may still forbid you from running it as a paid service for your own customers.

**Key point:** Visibility and permission are independent. The spectrum runs on permission, not on whether a repository is public, and a project's position on it belongs to a version rather than to the project's name.

### Visualization (canvas `c1`, 720×300)

A labelled four-region spectrum bar with three tick/cross rows beneath it, one row per question.

- **Title (bold 15px, `#1a5276`, top center):** "One Spectrum, Three Different Questions".
- **Spectrum bar (x=190 to x=690, y=62, height 32, four equal 125px regions, 1.5px `#ffffff` separators):** fills left to right — open source `rgba(0,131,0,0.18)`, open core `rgba(201,133,0,0.18)`, source-available `rgba(217,89,38,0.18)`, proprietary `rgba(74,58,167,0.16)`; region names bold 11px `#2c3e50` centered inside at y=82: "open source", "open core", "source-available", "proprietary".
- **Column centers:** 252.5, 377.5, 502.5, 627.5.
- **Row labels (12px `#444`, right-aligned at x=180, rows at y=145 / 191 / 237):** "read the source", "use it in production", "resell it as a service".
- **Marks (bold 16px, centered on each column center at the row y):** green `#008300` ✓, orange `#d95926` ~, red `#e74c3c` ✗; 11px `#6b7280` qualifier under a `~` mark at +15px.
  - read the source: ✓ / ~ ("core only") / ✓ / ✗
  - use it in production: ✓ / ✓ / ~ ("under the limit") / ~ ("as licensed")
  - resell it as a service: ✓ / ~ ("core only") / ✗ / ✗
- **Annotation (bold 12px violet `#4a3aa7`, centered at y=278):** "readable source is not the same permission as free use".
- **Caption (11px `#444`, bottom right):** "generalized license families; individual licenses vary".

## Selling a Subscription Around Free Code

**Tags:** `worked example` (blue), `subscription model` (green), `copyleft` (orange)

- **The codebase** — RHEL is built from code under the GPL and similar licenses, so it cannot be sold as a secret
- **What is actually bought** — a subscription: tested builds, security updates, certification, and support
- **What stays with the vendor** — the trademarks and the build pipeline, even when every source file is free
- **The rebuild ecosystem** — the same sources can be rebuilt by anyone, which is exactly what rebuild distros do
- **CentOS to its successors** — after CentOS Linux ended in 2021, Rocky Linux and AlmaLinux took up the rebuild role
- **The 2023 change** — Red Hat narrowed public distribution of RHEL sources to customer channels, a distribution choice
- **Not a relicense** — the license on the code did not change; where the tarballs are published did
- **The general lesson** — with copyleft code, the defensible product is the service and the brand, not the source

*Example (italic):* Two servers can run the same rebuilt kernel sources; only one of them can open a support case or claim a certified platform.

**Key point:** Copyleft removes secrecy as a business model but leaves service, certification, and trademark intact — so the invoice attaches to the subscription, never to the source files.

### Visualization (canvas `c2`, 720×300)

Two stacked boxes — the free code below, the paid subscription above — with revenue arriving only at the upper box and a community rebuild branch taking only the lower one.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Invoice Attaches".
- **Upper box (x=200, y=55, 300×76, 8px radius, fill `rgba(0,131,0,0.12)`, 2px `#008300` border):** bold 13px `#008300` "the subscription" at y=80; 12px `#2c3e50` lines "tested builds · security updates" (y=100) and "certification · support · trademark" (y=120).
- **Lower box (x=200, y=170, 300×64, 8px radius, fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border):** bold 13px `#2a78d6` "the code" at y=194; 12px `#2c3e50` "GPL and similar licenses — freely available" at y=216.
- **Connector (2.5px `#6b7280` arrow from (350,170) up to (350,131)):** 11px `#6b7280` label "built from" at x=358, y=154.
- **Revenue arrow (2.5px `#008300`, from (700,93) to (505,93)):** bold 12px `#008300` label "subscription revenue" centered at (600,78).
- **Rebuild branch (2.5px `#6b7280` arrow from (200,202) leftward to (178,202)):** box x=20, y=178, 155×48, 8px radius, fill `#f8f9fa`, 1.5px `#e5e9ef` border; 12px `#2c3e50` "community rebuilds" (y=198) and 11px `#6b7280` "Rocky Linux, AlmaLinux" (y=216).
- **Side note (11px `#6b7280`, right-aligned at x=700, y=250):** "2023: source distribution moved to customer channels".
- **Annotation (bold 13px `#008300`, centered at y=274):** "the license frees the code, not the brand or the pipeline".
- **Caption (11px `#444`, bottom right):** "schematic; licenses and dates as documented".

## Source-Available: Thresholds and Change Dates

**Tags:** `where it's used` (blue), `BSL / SSPL` (orange), `forks` (red)

- **Business Source License** — source is public, free use below a stated limit, and a paid license above it
- **The change date** — every BSL release converts to a true open source license on a date fixed at publication
- **Akka, 2022** — moved to BSL with a stated revenue threshold for production use, converting to Apache 2.0 after three years
- **Terraform, 2023** — moved to BSL, which prompted the OpenTofu fork continuing under the previous terms
- **Server Side Public License** — offering the software as a service obliges you to release your whole service stack's source
- **SSPL adoption** — MongoDB adopted it in 2018 and Elasticsearch in 2021; it is not OSI-approved
- **The forks that followed** — Elasticsearch's relicense prompted the OpenSearch fork; Elastic added an AGPL option in 2024
- **What to check** — the threshold, the change date, and whether hosting for others counts as triggering the paid tier

*Example (italic):* A team below the stated limit uses a BSL release for free today; the same release becomes fully open source on its published change date regardless of their size.

**Key point:** The pattern behind these licenses is a vendor limiting resale of its work as a hosted service. Read the threshold, the change date, and the hosting clause — those three lines decide whether you can deploy it at all.

### Visualization (canvas `c3`, 720×300)

A change-date timeline for one BSL release on top, and a threshold gauge for the same release beneath it.

- **Title (bold 15px, `#1a5276`, top center):** "One Release, Two Clauses: the Change Date and the Threshold".
- **Section A label (12px `#6b7280`, left at x=40, y=52):** "the change date".
- **Timeline band (y=88, height 18, 6px radius):** x=60 to x=600 filled `rgba(217,89,38,0.20)` with 1.5px `#d95926` border; x=600 to x=680 filled `rgba(0,131,0,0.18)` with 1.5px `#008300` border.
- **Band labels:** 12px `#2c3e50` centered at (330,101) "restricted terms — limit applies"; bold 11px `#008300` centered at (640,101) "Apache 2.0".
- **Ticks (2px `#1a5276`, 8px tall, below the band at x = 60 / 240 / 420 / 600):** 11px `#6b7280` labels centered at y=128 — "release day", "+1 year", "+2 years", and bold 12px `#008300` "change date" at x=600.
- **Section B label (12px `#6b7280`, left at x=40, y=176):** "the threshold, on that same release".
- **Threshold gauge (y=196, height 32, 6px radius):** x=60 to x=420 fill `rgba(0,131,0,0.14)` border 1.5px `#008300`, 12px `#2c3e50` centered "below the stated limit — free use"; x=420 to x=680 fill `rgba(231,76,60,0.12)` border 1.5px `#e74c3c`, 12px `#2c3e50` centered "above it — paid license".
- **Divider (dashed 1.5px `#6b7280`, dash [4,3], from (420,190) to (420,232)):** bold 11px `#6b7280` label "stated limit" centered at (420,184).
- **Annotation (bold 12px violet `#4a3aa7`, centered at y=262):** "hosting the software for others is the clause these terms are written to reach".
- **Caption (11px `#444`, bottom right):** "illustrative: split points drawn schematically; BSL mechanism as documented".

## Open Core and a Vendor-Neutral Core

**Tags:** `common mistake` (red), `open core` (green), `who owns the copyright` (blue)

- **A neutral core** — Kafka's core is Apache 2.0 under the Apache Software Foundation, so no one vendor can relicense it
- **The paid ring** — Confluent builds paid and community-licensed add-ons around that core
- **The community license** — the Confluent Community License permits use but not offering the software as a competing service
- **Cloudera's shape** — it ships Apache-licensed Hadoop-family components while selling a subscription platform
- **The 2019 change** — Cloudera moved its distribution behind customer-only access, again a distribution change
- **Why ownership matters** — donated code cannot be pulled back, while vendor-owned code can be relicensed at any release
- **The check before depending** — is the license OSI-approved, who holds the copyright, and can it be relicensed next release
- **Where to look** — the LICENSE file shipped in the artifact you actually install, not the project's website or README

*Example (italic):* A component under a foundation's copyright will still be Apache 2.0 next year; a single-vendor component can arrive under new terms with its next tag.

**Common mistake:** Reading a public GitHub repository as permission. Repository visibility says nothing about the terms — only the LICENSE file does, and for vendor-owned code it can differ from one release to the next.

### Visualization (canvas `c4`, 720×300)

Two ownership diagrams side by side: a foundation-owned core with several vendors around it, and a single-vendor-owned core with the relicensing risk marked.

- **Title (bold 15px, `#1a5276`, top center):** "Who Can Change the License?".
- **Panels (two rounded boxes, 8px radius, fill `#f8f9fa`, 1.5px `#e5e9ef` border):** left x=20, y=50, 330×200; right x=370, y=50, 330×200.
- **Panel headers (bold 13px `#1a5276`, centered at y=72):** left at x=185 "foundation-owned core"; right at x=535 "single-vendor-owned core".
- **Left core box (x=100, y=128, 170×44, 8px radius, fill `rgba(0,131,0,0.14)`, 2px `#008300` border):** 12px `#2c3e50` "core — Apache 2.0" at y=145 and 11px `#6b7280` "copyright: foundation" at y=162.
- **Left vendor boxes (86×28, 6px radius, fill `rgba(42,120,214,0.12)`, 1.5px `#2a78d6` border, 11px `#2c3e50` centered):** at (32,88) "vendor A", (232,88) "vendor B", (232,196) "vendor C"; 2px `#6b7280` arrows from each toward the core box edge.
- **Left badge (x=32, y=196, 150×28, 6px radius, fill `rgba(0,131,0,0.14)`, 1.5px `#008300` border):** bold 11px `#008300` centered "relicensing blocked".
- **Right core box (x=450, y=128, 170×44, 8px radius, fill `rgba(217,89,38,0.18)`, 2px `#d95926` border):** 12px `#2c3e50` "core — one owner" at y=145 and 11px `#6b7280` "copyright: the vendor" at y=162.
- **Right owner box (x=492, y=84, 86×28, 6px radius, fill `rgba(74,58,167,0.14)`, 1.5px `#4a3aa7` border, 11px `#2c3e50` "the vendor"):** 2px `#6b7280` arrow from (535,112) down to (535,126).
- **Right badge (x=440, y=196, 190×28, 6px radius, fill `rgba(231,76,60,0.12)`, 1.5px `#e74c3c` border):** bold 11px `#e74c3c` centered "can be relicensed next release".
- **Annotation (bold 12px `#1a5276`, centered at y=272):** "the LICENSE file in the artifact is the answer — not the repository's visibility".
- **Caption (11px `#444`, bottom right):** "ownership schematic".

## Footnote

`<p class="footnote">` (0.8rem, `#6b7280`), after the last card section and before the script:

"Many further commercial and source-available arrangements exist and are not covered here: dual licensing and sell-exceptions, the Elastic License, the Functional Source License, the PolyForm licenses, the Redis and Grafana relicensing episodes, per-seat and per-core proprietary EULAs, and OEM or redistribution agreements. These terms change between releases, so the authoritative text is always the LICENSE file shipped with the exact version you use."

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%); then the `.footnote` paragraph.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section 4's callout uses the label "Common mistake:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius; `.footnote` 0.8rem `#6b7280`, margin-top 8px. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. `roundedBox` and `lineArrow` helpers as in the sibling registry page. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all geometry is hardcoded literals; no randomness anywhere. Documented facts used: RHEL built from GPL-and-similar sources; CentOS Linux ended in 2021 with Rocky Linux and AlmaLinux taking up the rebuild role; Red Hat's 2023 narrowing of public RHEL source distribution to customer channels; Akka's 2022 move to BSL with a stated revenue threshold and a three-year conversion to Apache 2.0; Terraform's 2023 BSL move and the OpenTofu fork; MongoDB's 2018 and Elasticsearch's 2021 SSPL adoption, SSPL not being OSI-approved, the OpenSearch fork, and Elastic's 2024 addition of an AGPL option; Kafka's Apache 2.0 core under the Apache Software Foundation; the Confluent Community License's competing-service restriction; Cloudera's Apache-licensed components and its 2019 move to customer-only distribution. No revenue figures or dollar thresholds are stated anywhere — the Akka threshold is described only as "a stated revenue threshold". The c3 timeline interval and threshold split point are drawn schematically and labeled illustrative in the caption.
