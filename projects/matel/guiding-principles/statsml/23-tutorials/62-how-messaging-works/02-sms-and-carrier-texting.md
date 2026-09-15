# SMS & Carrier Texting

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SMS &amp; Carrier Texting

**Subtitle:** The message that rides the phone network's control channel — why texts are 160 characters, arrive after your phone was off, and cost almost nothing to carry

## A Message That Rides the Signaling Channel

**Tags:** `core idea` (blue), `SS7` (green), `7-bit alphabet` (orange)

- **Born in 1992** — the first SMS, "Merry Christmas", was sent over a GSM network in December 1992
- **Piggyback design** — SMS rides SS7, the signaling system phones already used to set up calls
- **Tiny packet** — a text is a control-channel message, so it works mid-call and on 2G-era hardware
- **140 bytes** — the payload holds 160 chars in the 7-bit GSM alphabet, or 70 chars in UCS-2
- **Concatenation** — long texts split into segments; a 6-byte header cuts each part to ~153 chars

*Example (italic):* A 400-character message is silently sent as three segments of 153, 153, and 94 characters.

**Key point:** The 160-character limit is not a product choice — it is the byte budget of a signaling packet that already existed in the phone network.

### Visualization (canvas `c1`, 720×300)

Byte-budget diagram: three horizontal bars showing the same 140-byte payload spent three ways, then a segmentation strip splitting a 400-character message into three concatenated parts.

- **Title (bold 15px, `#1a5276`, top center):** "140 Bytes, Three Ways to Spend Them".
- **Bars (x=185 to x=690 spans the full 140 bytes; 26px tall; bold 12px ink `#1a5276` row labels right-aligned at x=175):**
  - y=48 "GSM 7-bit" — full-width bar, fill `rgba(42,120,214,0.30)`, 2px blue `#2a78d6` border; centered 12px `#2c3e50` label "160 chars × 7 bits = 1,120 bits = 140 bytes".
  - y=90 "UCS-2 (non-Latin)" — full-width bar, fill `rgba(74,58,167,0.18)`, 2px violet `#4a3aa7` border; label "70 chars × 16 bits = 1,120 bits".
  - y=132 "concat segment" — a 6-byte header chunk (6/140 of the width, ≈22px) filled `rgba(217,89,38,0.45)` with 2px orange `#d95926` border, then the remainder filled `rgba(42,120,214,0.30)` with blue border; label in the blue part "153 chars × 7 bits"; bold 11px orange "UDH 6 B" above the header chunk at y=128.
- **Segmentation strip:** 12px `#444` label "one 400-char message:" right-aligned at x=175, y=215; strip from x=185 to x=690, 28px tall at y=200, split proportionally 153 / 153 / 94 chars; fills `rgba(42,120,214,0.30)` / `rgba(25,158,112,0.25)` / `rgba(217,89,38,0.25)` with 2px borders blue `#2a78d6` / aqua `#199e70` / orange `#d95926`; centered 12px `#2c3e50` labels "part 1 — 153", "part 2 — 153", "part 3 — 94".
- **Annotation (bold 13px magenta `#d55181`, centered at y=262):** "three segments billed and sent — the phones stitch them back invisibly".
- **Caption (11px `#444`, bottom right, y=290):** "byte math exact; header chunk drawn to scale".

## Store-and-Forward at the SMSC

**Tags:** `worked example` (blue), `SMSC` (green), `HLR lookup` (orange)

- **Hand-off** — the sending phone submits the text to its carrier's SMSC, and its job is done
- **HLR lookup** — the SMSC asks the HLR (Home Location Register) which switch serves the recipient
- **Delivery** — the serving MSC pages the phone over the signaling channel and pushes the text down
- **Phone off** — an unreachable phone makes the SMSC store the message and set a flag in the HLR
- **Reattach** — when the phone reappears on the network, the HLR alerts the SMSC, which retries
- **Receipts** — delivery reports ride the same path in reverse to light up the sender's "delivered"

*Example (italic):* A text sent to a phone that is off overnight arrives seconds after it powers on at 7am.

**Key point:** SMS never assumes a live connection — the SMSC is a mailbox in the middle, and that store-and-forward core is why texts survive dead batteries.

### Visualization (canvas `c2`, 720×300)

Protocol ladder: five vertical lifelines with the online delivery hops, then a shaded offline branch showing store, flag, reattach alert, and retry.

- **Title (bold 15px, `#1a5276`, top center):** "One Text Through the Network — and the Phone-Off Branch".
- **Lifelines:** header boxes 30px tall centered on y=48 at x = 80 / 235 / 375 / 515 / 655, labels bold 12px: "Asha's phone" (blue fill `rgba(42,120,214,0.15)`), "SMSC" (aqua fill `rgba(25,158,112,0.15)`), "HLR" (violet fill `rgba(74,58,167,0.12)`), "MSC" (aqua fill), "Ben's phone" (green fill `rgba(0,131,0,0.12)`); dashed `#c8ced8` vertical lifelines from y=66 to y=286.
- **Online hops (solid 2px arrows with filled arrowheads; 11px labels above each arrow):**
  - y=92 Asha → SMSC, blue `#2a78d6`, "submit text (signaling channel)".
  - y=118 SMSC → HLR, violet `#4a3aa7`, "where is Ben?".
  - y=140 HLR → SMSC, violet dashed, "serving MSC address".
  - y=164 SMSC → MSC, blue, "forward".
  - y=186 MSC → Ben, green `#008300`, "page phone + deliver".
- **Offline branch:** dashed `#c8ced8` divider line at y=204 with centered bold 12px orange `#d95926` label "if Ben's phone is off"; at y=232 a small orange-bordered box on the SMSC lifeline, "store msg", plus 11px orange text to its right "HLR flags: notify me on reattach"; y=258 HLR → SMSC, orange 2px, "Ben reattached"; y=280 SMSC → Ben, green 2px, "retry → delivered".
- **Caption (11px `#444`, bottom right):** "hop sequence real; layout simplified".

## MMS Is Secretly the Web

**Tags:** `common confusion` (red), `MMS` (green), `HTTP` (orange)

- **Not bigger SMS** — MMS (2002) does not stretch the 140-byte packet; the media never touches SS7
- **Upload first** — the sender's phone uploads the photo over the data connection to an MMSC via HTTP
- **Tiny pointer** — the recipient gets a small SMS notification that carries only a URL to fetch
- **Silent fetch** — the phone downloads the media over data, which is why MMS fails without a data path
- **Recompression** — carriers transcode media down to size caps, so photos arrive visibly degraded

*Example (italic):* A 4 MB photo arrives as a ~300 KB copy because the MMSC shrank it to the carrier's cap.

**Common confusion:** An MMS is a web download wearing a text message's clothes — the SMS part only delivers the link, and everything heavy travels over HTTP on the data connection.

### Visualization (canvas `c3`, 720×300)

Side-by-side path comparison: SMS as one signaling hop on the left, MMS as three numbered steps (upload, SMS pointer, fetch) on the right.

- **Title (bold 15px, `#1a5276`, top center):** "SMS: One Signaling Hop — MMS: Two HTTP Trips and a Pointer".
- **Divider:** dashed `#bdc3c7` vertical line at x=265 from y=35 to y=285.
- **Left panel (bold 13px blue `#2a78d6` header "SMS — pure signaling" centered at x=135, y=52):** three rounded boxes 30px tall centered on y=140 at x-centers 55 / 135 / 215, width 60px: "phone" (blue fill `rgba(42,120,214,0.15)`), "SMSC" (aqua fill `rgba(25,158,112,0.15)`), "phone" (blue fill); solid 2px blue arrows between them; one bold 11px blue label "140-byte packets over SS7" centered at x=135, y=108; 11px `#444` note centered at y=200 "no data connection needed"; bold 11px green `#008300` centered at y=220 "works on any 2G phone, mid-call".
- **Right panel (bold 13px orange `#d95926` header "MMS — a pointer plus the web" centered at x=495, y=52):** three rows of box → arrow → box, boxes 100×30 rounded, left box x=290–390, right box x=590–690, rows centered on y=105 / 170 / 235; arrow between the boxes with its bold 11px label centered above at x=490:
  - Row 1: "sender phone" (blue fill) → orange 2.5px arrow, label "1. HTTP upload (data)" (orange) → "MMSC" (orange fill `rgba(217,89,38,0.12)`, 2px orange border).
  - Row 2: "MMSC" (orange fill) → blue 2.5px dashed arrow, label "2. SMS notification: a URL" (blue) → "recipient" (green fill `rgba(0,131,0,0.12)`).
  - Row 3: "recipient" (green fill) → green 2.5px arrow, label "3. HTTP GET media (data)" (green) → "MMSC" (orange fill).
- **Annotation (bold 12px magenta `#d55181`, centered at x=495, y=280):** "MMSC recompresses to the carrier's cap — quality loss by design".
- **Caption (11px `#444`, bottom left at x=30, y=295):** "size caps vary by carrier (~300 KB–1 MB)".

## RCS, the Modern Replacement

**Tags:** `where it's used` (blue), `RCS` (green), `encryption` (orange)

- **All IP** — RCS moves texting onto the data connection: typing indicators, read receipts, big media
- **2008 spec** — the GSMA defined RCS in 2008, but each carrier ran its own incompatible deployment
- **Google Jibe** — adoption moved once Google hosted the RCS backend (Jibe) for carriers from 2019
- **Apple joins** — iPhones added RCS in iOS 18 (2024), so cross-platform chats stop falling back to MMS
- **Encryption gap** — baseline RCS is not end-to-end encrypted; Google added E2E, the spec adds MLS

*Example (italic):* A cross-platform group chat that once dropped to grainy MMS now sends full-resolution video.

**Key point:** RCS keeps the phone number as the address but replaces the SS7 packet with an internet messenger — the last piece, universal end-to-end encryption, is still arriving.

### Visualization (canvas `c4`, 720×300)

Timeline of carrier messaging milestones on top, capability comparison grid for SMS / MMS / RCS below.

- **Title (bold 15px, `#1a5276`, top center):** "From Signaling Packet to Internet Messenger".
- **Timeline:** horizontal 2px `#6b7280` line at y=78 from x=60 to x=685; year positions proportional over 1990–2026; five dots (radius 7): 1992 blue `#2a78d6` "SMS first sent" (x≈94), 2002 aqua `#199e70` "MMS launches" (x≈267), 2008 violet `#4a3aa7` "RCS spec (GSMA)" (x≈371), 2019 orange `#d95926` "Google Jibe backend" (x≈561), 2024 green `#008300` "iPhone adds RCS" (x≈648); bold 12px year labels alternating above (y=58) and below (y=100) the line in the dot color, with the event name in 11px `#444` on a second line.
- **Capability grid (y=150 to y=285):** column headers bold 12px `#1a5276` at y=160: "length" (x=225), "media" (x=355), "receipts" (x=485), "E2E encrypted" (x=625); row labels bold 13px ink at x=60: "SMS" (y=192), "MMS" (y=228), "RCS" (y=264); light `#e5e9ef` horizontal rules between rows; cell text 12px centered on the column positions:
  - SMS row: "160 chars" / "none" / "delivery only" / "no" (the "no" in bold orange `#d95926`).
  - MMS row: "size cap" / "recompressed" / "delivery only" / "no" (bold orange).
  - RCS row: "long text" / "high-res" / "typing + read" (bold green `#008300` for the first three) / "client E2E; MLS in spec" (bold 11px `#c98500`).
- **Caption (11px `#444`, bottom right, y=295):** "dates are public milestones; grid summarizes baseline standards".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). No `Math.random()`, no `Date` calls.
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Navy `#1a5276` is ink for headings, axes, and callout borders.
- **Data:** the 140-byte / 160-char / 70-char / 153-char arithmetic is exact GSM 03.38/03.40 math; the SMSC/HLR/MSC hop sequence and the MMS upload-notify-fetch flow are the standard architectures; timeline dates (1992, 2002, 2008, 2019, 2024) are public milestones; any drawn timings or sizes are labeled illustrative in captions.
- **Framing:** factual tech history — real standards bodies and public product milestones are named; no claims about any carrier's current internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
