# Who Else Is Scanning Your Local Network

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Who Else Is Scanning Your Local Network

**Subtitle:** Apps and gadgets quietly map every device in the house — and "inside the network" was never a credential

## The Cast Button That Mapped the House

**Tags:** `core idea` (blue), `device discovery` (orange), `flat trust` (red)

- **The moment** — Alice opens a video app on her phone and her living-room screen appears instantly as a cast target
- **How it knew** — the app shouted one discovery request to every address on the home network and waited
- **The replies** — each device that speaks the discovery protocol answered with its name, model, and capabilities
- **What the app now holds** — an inventory of the house: a media device, a printer, a storage box, a hub, a camera
- **No authentication anywhere** — nothing in the question proves who asked, and nothing in the reply checks
- **Why devices answer** — announcing yourself is how a device becomes usable; silence would break casting and printing
- **The definition** — a discovery protocol is a broadcast question that any device on the network may answer
- **Flat trust** — most home networks give every device on the inside the same standing, which is a flat trust model

*Example (italic):* One tap on the cast button produced a labeled list of every answering device in Alice's home, and the app never had to ask her permission for any of it.

**Key point:** Being on the network is a location, not an identity — discovery answers whoever asks, so any code that reaches the LAN inherits whatever the LAN is trusted with.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one broadcast question from a phone reaches a local-network bus, and five devices answer with their names and capabilities.

- **Title (bold 15px, `#1a5276`, top center):** "One Broadcast Question, Five Self-Describing Answers".
- **Asker box:** blue rounded box at x=26, y=120, 150×58, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` two-line centered text "Alice's phone" / "video app".
- **Broadcast arrow:** 3px `#2a78d6` line from (176, 149) to (246, 149) with arrowhead at (252, 149); bold 12px `#2a78d6` label "who is there?" centered at (211, 138).
- **Network bus:** 4px `#6b7280` vertical line from (256, 48) to (256, 262), with a 12px `#6b7280` label "local network" centered at (256, 282).
- **Device boxes (five, x=336, width 236, height 38, 8px radius, fill `rgba(0,131,0,0.10)`, 2px `#008300` border):** tops at y=48, 92, 136, 180, 224; each holds one line of 12px `#2c3e50` text left-aligned at x=348, baseline y=box top+24: "media device — screen mirroring", "printer — accepts print jobs", "storage box — shares files", "smart hub — takes commands", "camera — web page, no login".
- **Reply arrows:** for each box a 2px `#008300` line from the box's left edge (x=332) to the bus (x=262) at the box's vertical centre, with an arrowhead pointing left at (258, centre).
- **Annotation (bold 13px orange `#d95926`, left-aligned at x=336, y=284):** "every reply is volunteered, not requested".
- **Caption (12px `#444`, right-aligned at x=708, y=24 — the title is centered so the top-right corner is free):** "illustrative device list".

## Under Fifteen Seconds to Map a Home

**Tags:** `worked example` (blue), `enumeration cost` (orange)

- **The address space** — a common home network carries 254 usable addresses, a tiny space by any measure
- **The assumed rate** — take 200 probes per second, an unremarkable pace for an ordinary phone app
- **One probe each** — 254 / 200 = 1.27 seconds to touch every address on the network once
- **A port sweep** — checking 10 common service ports on each address is 254 × 10 = 2,540 probes
- **Sweep time** — 2,540 / 200 = 12.7 seconds to learn which service listens where, across the whole house
- **The public comparison** — sweeping all 4,294,967,296 internet addresses at 100,000/s takes 42,949.7 s, 11.9 hours
- **Why inside wins** — the interior is small, so a position inside turns hours of work into one held breath

*Example (italic):* At 200 probes per second an app finishes a full 10-port sweep of Alice's home in 12.7 seconds — less time than the video takes to buffer (rates assumed, illustrative).

**Key point:** The inside of a network is small enough to enumerate completely in seconds, so "inside" is a decisive advantage rather than a marginal one.

### Visualization (canvas `c2`, 720×300)

Bar chart on a log time scale: address sweep and port sweep of one home network against a single-port sweep of the whole public address space; every value label computed at render time.

- **Title (bold 15px, `#1a5276`, top center):** "Time to Enumerate: 254 Local Addresses vs 4.29 Billion Public Ones".
- **Data (hardcoded probe counts and rates):** `[{probes: 254, rate: 200}, {probes: 2540, rate: 200}, {probes: 4294967296, rate: 100000}]`; seconds computed in JS as `probes / rate`, giving 1.27, 12.7 and 42,949.7 seconds.
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 170; height per bar = `(log10(sec) + 0.5) / 5.5 × 170`, giving about 19 / 50 / 159 px.
- **Gridlines (`#e5e9ef`, 1px)** at 1 s, 60 s, 3,600 s and 86,400 s, placed with the same log mapping, with right-aligned 12px `#444` labels "1 s", "1 min", "1 hour", "1 day"; x-axis 2px `#999`.
- **Bars (86px wide, centered at x = 160, 330, 540):** the two home bars fill `rgba(42,120,214,0.40)` with 2px `#2a78d6` border; the public-sweep bar fills `rgba(107,114,128,0.22)` with 2px `#6b7280` border.
- **Value labels (bold 13px, `#1a5276` for the home bars, `#6b7280` for the public bar), centered 8px above each bar top:** formatted in JS — ≥3,600 s as "N.N hours", ≥60 s as "N.N min", else "N.NN s", yielding "1.27 s", "12.7 s", "11.9 hours".
- **X labels (12px `#444`, two lines below the baseline at y=+18 and y=+34):** "254 addresses" / "1 probe each, 200/s", "2,540 probes" / "10 ports each, 200/s", "4,294,967,296" / "1 port each, 100 K/s".
- **Annotation (bold 13px blue `#2a78d6`, left-aligned at x=120, y=62):** "the whole house, mapped in 12.7 s".
- **Caption (12px `#444`, bottom right):** "probe rates assumed, illustrative".

## Position Is Not Permission

**Tags:** `worked example` (blue), `flat trust` (red), `defensive` (green)

- **The inventory** — the discovery sweep of Alice's home returned 22 devices that answered something
- **Named devices** — 14 of the 22 replied with a model name, so 14 / 22 = 63.6% identify themselves precisely
- **Web interfaces** — 6 of the 22 devices serve an admin web page, which is 6 / 22 = 27.3% of the house
- **No login at all** — 3 of those 6 pages need no login, so 3 / 6 = 50% of the web pages are open
- **The exposed share** — those 3 are 3 / 22 = 13.6% of all devices, directly controllable by anything on the LAN
- **Segmentation** — a separate network for gadgets means a compromised one enumerates a near-empty segment
- **Guest network** — visitors join their own segment, so Bob's laptop never sits inside the trusted zone
- **Authenticate the caller** — put a real login on every admin page and turn off discovery and auto-port-opening where unused

*Example (italic):* Of the 22 devices found, 3 accept commands from any caller on the network with no login — 13.6% of the house is controllable by a phone app, a guest, or a compromised bulb (counts illustrative).

**Key point:** Zero trust at home means every service authenticates every caller regardless of where it sits — authenticate the request, not the position.

### Visualization (canvas `c3`, 720×300)

Grid schematic: 22 device tiles, with the model-name repliers filled, the 6 web-interface devices outlined, and the 3 unauthenticated ones highlighted, annotated with the computed shares.

- **Title (bold 15px, `#1a5276`, top center):** "22 Devices Found: 6 With Web Pages, 3 With No Login".
- **Layout (hardcoded):** 22 tiles of 44×44, 8px gaps, in two rows of 11; first tile top-left at x=70, y=70, so tile *i* sits at `x = 70 + (i % 11) * 52`, `y = 70 + Math.floor(i / 11) * 52`.
- **Tile classes (hardcoded index sets, indices 0-based):** unauthenticated web page = `[0, 1, 2]`; authenticated web page = `[3, 4, 5]`; named-only (model name, no web page) = `[6, 7, 8, 9, 10, 11, 12, 13]`; silent (no model name) = `[14, 15, 16, 17, 18, 19, 20, 21]`. So 3 + 3 = 6 with a web page, and 3 + 3 + 8 = 14 replied with a model name, matching the text.
- **Tile styling:** unauthenticated — fill `rgba(217,89,38,0.45)`, 2.5px `#d95926` border; authenticated web page — fill `rgba(42,120,214,0.35)`, 2.5px `#2a78d6` border; named-only — fill `rgba(42,120,214,0.14)`, 1.5px `#2a78d6` border; silent — fill `rgba(107,114,128,0.10)`, 1.5px `#6b7280` border. All tiles 6px corner radius.
- **Legend (four entries, 12px `#444`, left-aligned starting x=70, y=200, one per line at 20px spacing, each with a 12×12 swatch at x=70 in the class fill and border):** "3 — web page, no login (13.6% of 22)", "3 — web page, login required", "8 — model name only", "8 — answered, no model name".
- **Right-side computed callouts (right-aligned at x=690):** bold 14px `#2a78d6` at y=210 "6 / 22 = 27.3% expose a web page"; bold 14px `#d95926` at y=234 "3 / 6 = 50% of those need no login"; bold 14px `#d95926` at y=258 "3 / 22 = 13.6% openly controllable"; all three percentages computed in JS from the tile counts, not hardcoded strings.
- **Row label (12px `#6b7280`, left-aligned at x=70, y=60):** "each tile is one device that answered discovery".
- **Caption (12px `#444`, right-aligned at x=708, y=288):** "inventory illustrative".

## "It's Only on My Home Network"

**Tags:** `common mistake` (red), `the inside is not trusted` (orange)

- **The sentence** — "it's only on my home network, so it doesn't need a password" is the reasoning that loses the segment
- **Why it fails** — one weak gadget becomes a foothold, and from there every position-trusted service answers it
- **Who is already inside** — apps, the devices' own software, a guest's laptop, and a page open in a browser
- **The browser case** — a visited page can attempt connections to local addresses, so no malware is needed
- **The mirror error** — discovery traffic is not evidence of an attack; casting and printing are built on it
- **Alarming on scans** — treating every sweep as malicious buries the console in noise from ordinary app behaviour
- **The right response** — remove the network's authority rather than trying to stop devices from asking questions

*Example (italic):* Alice's hub needs no password because it is "internal"; when a cheap bulb is compromised, the bulb is internal too, and the hub cannot tell the difference.

**Common mistake:** Defending the boundary instead of the service. The fix is not blocking discovery — it is segmentation plus a real login on every device, so a scan from inside learns a lot and can do nothing.

### Visualization (canvas `c4`, 720×300)

Two-panel schematic: a flat network where one compromised device reaches 21 peers, against a segmented network where it reaches 11 and the trusted 10 are unreachable.

- **Title (bold 15px, `#1a5276`, top center):** "Flat Network vs Segmented: What One Compromised Gadget Reaches".
- **Left panel (x = 30 to 340), header bold 13px `#2c3e50` at (36, 58):** "flat network".
- **Left content:** one 40×28 rounded box (fill `rgba(217,89,38,0.45)`, 2px `#d95926`) at x=36, y=140, labelled below in bold 12px `#d95926` centered at (56, 186) "bulb"; then 21 dots of radius 6 in a 7×3 block, `x = 150 + (i % 7) * 26`, `y = 120 + Math.floor(i / 7) * 30`, fill `rgba(42,120,214,0.55)`, 1.5px `#2a78d6` stroke; three 2px `#d95926` arrows fan from (80, 154) to (140, 126), (140, 156) and (140, 186) — three arrows rather than 21 lines, which would be unreadable.
- **Left caption (bold 13px `#d95926`, at (150, 232)):** "reaches all 21 peers".
- **Divider:** 2px dashed `#6b7280` vertical line (dash 6/5) at x=360 from y=48 to y=270.
- **Right panel (x = 380 to 690), header bold 13px `#2c3e50` at (386, 58):** "segmented network".
- **Right content, gadget segment:** rounded container (2px `#c98500`, fill `rgba(201,133,0,0.07)`, 8px radius) at x=386, y=70, 296×92, with a 12px `#c98500` label "gadget segment — 12 devices" at (396, 88); the compromised 40×24 box (fill `rgba(217,89,38,0.45)`, 2px `#d95926`) at x=396, y=100; 11 dots radius 5.5 at `x = 470 + (i % 6) * 24`, `y = 106 + Math.floor(i / 6) * 26`, fill `rgba(42,120,214,0.55)`, 1.5px `#2a78d6`.
- **Right content, trusted segment:** rounded container (2px `#008300`, fill `rgba(0,131,0,0.07)`) at x=386, y=176, 296×74, with a 12px `#008300` label "trusted segment — 10 devices" at (396, 194); 10 dots radius 5.5 at `x = 400 + (i % 10) * 28`, y=222, fill `rgba(0,131,0,0.35)`, 1.5px `#008300`.
- **Right caption (bold 13px `#008300`, at (386, 268)):** "reaches 11; the other 10 stay invisible".
- **Arithmetic note (12px `#6b7280`, right-aligned at x=690, y=58):** "12 + 10 = 22 devices".
- **Caption (12px `#444`, left-aligned at x=36, y=288 — bottom-left, since the bottom-right holds the segmented panel's caption):** "topology schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data:** all values are hardcoded literal arrays, never `Math.random()`. Derived quantities are computed in JS at render time: sweep seconds from `probes / rate` (1.27 s, 12.7 s, 42,949.7 s) and the inventory shares from the tile-class counts (6/22 = 27.3%, 3/6 = 50%, 3/22 = 13.6%, 14/22 = 63.6%). The 254-usable-address figure is standard subnet arithmetic; the 22-device inventory, the 200 probes/second rate and the 12/10 segment split are invented and labeled illustrative. Text numbers must match chart numbers exactly, and the segment counts must sum (12 + 10 = 22, 11 + 10 = 21).
- **Framing:** defensive/educational throughout — the page explains the discovery mechanism and the flat trust model so a reader can segment and authenticate; no scanning procedure, tool invocation, or other operational guidance. No real vendors, apps, protocol product names, or literal address ranges. People are Alice and Bob.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
