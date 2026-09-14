# The WiFi Password Inside Your Gadgets

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The WiFi Password Inside Your Gadgets

**Subtitle:** Every smart gadget you set up keeps your network password — so the weakest one holds the key to the house

## Four Gadgets, Four Copies of the Same Key

**Tags:** `core idea` (blue), `stored network key` (orange), `weakest device` (red)

- **The house** — Alice sets up four gadgets on her home WiFi: a light bulb, a doorbell, a robot vacuum, a TV
- **The handshake** — each one had to be told the network password once, during setup, to join the network
- **It keeps it** — after a power cut nobody retypes anything, so each gadget must have stored the password
- **Four copies** — the one secret Alice thinks of as hers now exists in four places she cannot inspect
- **Not an average** — the network's safety is the *weakest* of the four, not their typical quality
- **The definition** — a stored network key is a copy of the WiFi password held on a device so it can reconnect unattended
- **The cheap one** — the bulb cost the least, gets no firmware updates, and holds exactly the same key as the TV

*Example (italic):* Alice's vacuum reconnects by itself after a two-hour outage — proof the password is sitting in its storage, not in her head.

**Key point:** Joining a device to WiFi is not borrowing the password, it is copying it — and the copy on the flimsiest gadget is as valid as the one you typed.

### Visualization (canvas `c1`, 720×300)

Hub-and-spoke diagram: the router at center holds the key; four spokes each end in a gadget box holding the same key icon; the bulb spoke is highlighted as the weakest.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "One Network Password, Copied Into Four Gadgets".
- **Router hub:** rounded box (8px radius) centered at (170, 160), drawn at x=95, y=130, 150×60, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border; centered 13px `#2c3e50` lines "home router" (y=152) and "network password" (y=170).
- **Gadget boxes:** four rounded boxes (8px radius), each 190×46, left edge x=460, at y=40 "TV", y=105 "robot vacuum", y=170 "doorbell", y=235 "light bulb"; label centered at (555, y+21) in 13px `#2c3e50`; a small key glyph drawn as a 6px filled circle plus a 10px stem at (485, y+23) in the box's border color.
- **Box colors:** TV `rgba(42,120,214,0.15)` fill / 2px `#2a78d6`; vacuum `rgba(25,158,112,0.15)` / 2px `#199e70`; doorbell `rgba(74,58,167,0.15)` / 2px `#4a3aa7`; bulb `rgba(217,89,38,0.18)` / 3px `#d95926`.
- **Spokes:** 3px lines from (245, 160) to (455, y+23) for each gadget in the box's border color; the bulb spoke drawn 4px wide and dashed `[6,4]`.
- **Spoke labels (12px `#6b7280`, centered on the first three spokes only):** "copy of the key" placed once at (350, 100) with `textAlign` center.
- **Weakest-link annotation (bold 13px `#d95926`, left-aligned at (300, 285)):** "no firmware updates — same key".
- **Caption (12px `#444`, bottom right, x = w−12, y = h−10):** "illustrative example".

## Working Out the Chance One Copy Leaks

**Tags:** `worked example` (blue), `arithmetic` (orange), `assumption` (violet)

- **The question** — if any single gadget has a chance p of being compromised, what is the chance *some* gadget is?
- **Flip it** — easier to compute the chance every gadget stays clean: (1 − p) multiplied N times
- **The formula** — chance at least one copy leaks = 1 − (1 − p)^N for N gadgets each holding the key
- **Set p = 0.02** — a 2% chance per gadget, chosen as a round illustrative figure, not a measurement
- **Hand-check N = 5** — 0.98^5 = 0.9039, so 1 − 0.9039 = 0.0961, about a 9.6% chance
- **The climb** — N = 1 gives 0.0200, N = 10 gives 0.1829, and N = 20 gives 0.3324, one in three
- **Alice's four** — 0.98^4 = 0.9224, so her house sits at 0.0776, already four times a single gadget
- **The assumption** — this treats gadgets as independent, which is a simplification: two gadgets sharing a vendor or firmware fail together, so the real risk is worse in a way this model cannot show

*Example (italic):* Twenty gadgets at 2% each is not "still 2%" — it is 33.24%, because every added gadget is another copy of the same key (illustrative p).

**Key point:** Risk compounds with the number of key holders — 1 − (1 − p)^N rises with N even when p never changes, so adding gadgets weakens a password you never altered.

### Visualization (canvas `c2`, 720×300)

Line chart of 1 − (1 − p)^N for p = 0.02, N = 1..20, with the four quoted values marked and labeled from values computed in JS at render time.

- **Data:** computed in the draw function, no hardcoded probabilities — `var p = 0.02;` and `var risk = function (n) { return 1 - Math.pow(1 - p, n); };` evaluated for N = 1..20. Marker labels are produced with `risk(n).toFixed(4)` so the printed text cannot drift from the curve.
- **Title (bold 16px, `#1a5276`, top center at y=24):** "Chance At Least One Gadget Leaks the Key: 1 − (1 − p)^N, p = 0.02".
- **Axes:** origin x=70, baseline y=250, plot width 600, plot height 190; y range 0 to 0.40; gridlines `#e5e9ef` at 0.10/0.20/0.30/0.40 with right-aligned 12px `#444` labels ("10%", "20%", "30%", "40%") at x=62; x-axis 2px `#999`.
- **X mapping:** `x(n) = 70 + (n - 1) / 19 * 600`; 12px `#444` tick labels "1", "5", "10", "15", "20" at y=272 under their x positions; 13px `#2c3e50` axis caption "number of gadgets holding the password (N)" centered at (370, 294).
- **Curve:** 3px `#2a78d6` polyline through all 20 points.
- **Markers:** filled 5px circles at N = 1, 5, 10, 20 in `#1a5276`; bold 12px labels in `#1a5276` placed above-right of each marker reading `"N=" + n + ": " + risk(n).toFixed(4)`, with the N=20 label right-aligned so it stays on canvas.
- **Annotation (bold 13px `#d95926`, left-aligned at (120, 90)):** "same 2% per gadget — the total keeps rising".
- **Caption (12px `#444`, bottom right, x = w−12, y = h−8):** "p = 0.02 illustrative; assumes independent gadgets".

## Shelves, Resales, and a Separate Network

**Tags:** `where it's used` (blue), `resale and resets` (orange), `defensive` (green)

- **It is on the chip** — the password sits in the gadget's storage, so whoever holds the hardware can try to read it
- **The resale path** — Alice sells the old vacuum; unless she wiped it first, the buyer receives a working copy of her key
- **The drawer path** — a doorbell replaced two years ago still holds the password if the password never changed
- **No updates, forever holes** — a cheap gadget with no update mechanism keeps its known flaws while the key stays valid
- **Guest network** — put gadgets on a separate network, and the key they hold opens only that, not the laptop or the files
- **Why nobody rotates** — changing the WiFi password means re-provisioning every gadget by hand, so it is put off indefinitely
- **Do wipe on exit** — a factory reset before selling or binning a gadget is the one cheap habit that closes the resale path

*Example (italic):* Bob buys a used robot vacuum at a yard sale; it powers on and tries to rejoin the seller's network, because the stored key was never cleared.

**Key point:** Segmentation beats rotation — a guest network shrinks what a leaked gadget key is worth, and it does not require you to retype the password into every gadget you own.

### Visualization (canvas `c3`, 720×300)

Two-panel before/after schematic: one flat network where the gadget key reaches everything, versus a split network where it reaches only the gadget segment.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "What a Leaked Gadget Key Opens: Flat Network vs Guest Network".
- **Panel headers (bold 13px, at y=52):** "one flat network" at x=60 (`#d95926`, left-aligned), "gadgets on a guest network" at x=390 (`#008300`, left-aligned).
- **Left panel:** a 2px `#d95926` rounded box (8px radius) at x=50, y=64, 290×180, fill `rgba(217,89,38,0.07)`; inside it five 12px `#2c3e50` labels at x=72, y = 96, 122, 148, 174, 200: "light bulb (key leaked)", "laptop", "shared files", "backup drive", "phone"; a 5px `#d95926` filled square bullet at x=58 beside each label; a 2px `#d95926` line from (64, 100) down to (64, 204) linking them.
- **Right panel:** two stacked boxes — guest box, 2px `#c98500`, x=390, y=64, 290×78, fill `rgba(201,133,0,0.09)`, containing 12px labels "light bulb (key leaked)" at (412, 96) and "other gadgets" at (412, 122); main box, 2px `#008300`, x=390, y=160, 290×84, fill `rgba(0,131,0,0.08)`, containing 12px labels "laptop, files, backup drive" at (412, 192) and "phone" at (412, 218).
- **Barrier:** a dashed 2px `#008300` horizontal line `[6,4]` at y=151 from x=390 to x=680, with a bold 12px `#008300` centered label "no route across" at (535, 146) drawn just above the line.
- **Annotation (bold 13px `#d95926`, left-aligned at (60, 268)):** "one weak gadget = the whole house".
- **Second annotation (bold 13px `#008300`, left-aligned at (390, 268)):** "one weak gadget = one segment".
- **Caption (12px `#444`, bottom right, x = w−12, y = h−8):** "illustrative example".

## You Did Not Keep That Password to Yourself

**Tags:** `common mistake` (red), `revocation` (violet)

- **The belief** — people picture the WiFi password as a secret only they know, held in their memory alone
- **The reality** — every gadget joined and every houseguest told is another holder of the identical secret
- **Not per-device** — a website can log out one device; a normal home network has one shared key for all
- **All or nothing** — revoking one holder means changing the key, which revokes every holder at once
- **The count grows** — gadgets accumulate quietly, and nobody keeps a list of which ones still hold the key
- **The consequence** — you cannot answer "who can join my network?" without auditing hardware you no longer own

*Example (italic):* Alice wants to cut off the vacuum she sold; her only lever is a new network password, which also kicks off the bulb, doorbell, TV, and every guest.

**Common mistake:** Treating the WiFi password like an account password. It has no per-device revocation on an ordinary home network — the only revoke button is "change it for everyone", which is why segmenting gadgets onto a guest network matters more than picking a longer password.

### Visualization (canvas `c4`, 720×300)

Two-column comparison table drawn on canvas: holders of the key and whether each can be revoked individually.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "Who Holds the Key, and Can You Revoke Just That One?".
- **Column headers (bold 13px `#1a5276`, at y=60):** "holder of the network password" left-aligned at x=70; "revoke this one alone?" centered at x=540.
- **Header rule:** 2px `#1a5276` line from (60, 68) to (690, 68).
- **Rows (hardcoded array of six, row i top edge y = 78 + i*32, text baseline y+21):** "light bulb", "doorbell", "robot vacuum (sold last year)", "TV", "houseguest Bob", "note on the kitchen fridge".
- **Row shading:** alternate rows filled `rgba(229,233,239,0.55)` from x=60 to x=690, height 32.
- **Left labels:** 13px `#2c3e50` at x=70; the sold-vacuum row in bold 13px `#d95926`.
- **Right cells:** bold 13px `#e74c3c` "no" centered at x=540 for every row — this is the genuine alarm state the page is about.
- **Bottom rule:** 1px `#e5e9ef` line at y=270 from x=60 to x=690.
- **Annotation (bold 13px `#4a3aa7`, centered at (375, 288)):** "the only revoke is: change it for all of them".
- **Caption:** omitted on this canvas (the rows are a schematic list, not measured data); if space allows, 12px `#444` right-aligned "schematic" at (w−12, 60) may be used instead.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`, violet `rgba(74,58,167,0.13)`/`#4a3aa7`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data:** no randomness anywhere — no `Math.random()`. The only quantitative series is `1 - Math.pow(0.98, n)` for n = 1..20, evaluated in the draw function so the curve, its markers, and their printed labels are the same arithmetic. Verified values that the prose must match to four decimals: N=1 → 0.0200, N=4 → 0.0776, N=5 → 0.0961, N=10 → 0.1829, N=20 → 0.3324; 0.98^5 = 0.9039 and 0.98^4 = 0.9224 as quoted in the text. p = 0.02 is illustrative and labeled as such.
- **Modeling caveat that must stay in the text:** the binomial-style formula assumes independent per-device compromise; shared vendors and shared firmware correlate failures, making the true risk higher than 1 − (1 − p)^N. The page states this and does not claim the model is accurate.
- **Naming and secrets rules:** no real vendors, brands, or models; devices are generic ("light bulb", "doorbell", "robot vacuum", "TV"); people are Alice and Bob; the password is always referred to as "the network password" and never written out as a literal string.
- **Framing:** defensive/educational — the page teaches why stored network keys concentrate risk and that segmentation, not rotation, is the practical control; no operational attack guidance.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
