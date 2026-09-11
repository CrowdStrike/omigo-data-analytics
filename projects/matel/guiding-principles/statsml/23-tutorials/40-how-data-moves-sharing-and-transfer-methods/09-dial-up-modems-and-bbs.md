# Dial-up Modems & BBS

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Dial-up Modems &amp; BBS

**Subtitle:** Before broadband, data traveled as audible tones on an ordinary phone call — the modem turned bits into sound and back

## A Modem Sings Bits Down a Voice Line

**Tags:** `core idea` (blue), `acoustic handshake` (orange), `sound as data` (green)

- **The running example** — Alice dials Bob's computer; her modem plays tones down an ordinary call
- **Modulate/demodulate** — the name: bits become tones going out, tones become bits coming in
- **Two tones** — in the earliest scheme a 0 is one pitch, a 1 another; the data is literally sound
- **The screech** — the opening noise is the two modems negotiating the fastest speed both share
- **One line** — the call owns the phone line for its whole duration; internet or voice, never both
- **Voice-width pipe** — a phone call passes roughly 300–3,400 Hz; everything must fit in that band

*Example (italic):* Alice's mother lifts the kitchen phone mid-download; the modems' duet breaks and the connection drops.

**Key point:** A modem is a translator between the computer's bits and the telephone's sound — the phone network never knew it was carrying data.

### Visualization (canvas `c1`, 720×300)

Two stacked panels: a square-wave bit stream on top, and below it the same eight bits as a continuous-phase two-frequency tone (FSK), each bit's tone segment colored by its value.

- **Title (bold 15px, `#1a5276`, top center):** "Bits as Tones: the Bell 103 Scheme at 300 bit/s".
- **Bit stream (hardcoded):** `[1, 0, 1, 1, 0, 0, 1, 0]`; x from 95 to 639, eight 68px segments.
- **Top panel (square wave, violet `#4a3aa7`, 2.5px):** 1 at y=70, 0 at y=105, vertical steps at segment boundaries; each bit's value drawn bold 12px `#1a5276` centered above its segment at y=58; right-aligned 12px `#444` label "bits" at x=87 near y=90.
- **Bottom panel (tone, 2.5px):** continuous-phase sine centered on y=205, amplitude 34; frequency 3 cycles/segment for a 0 (blue `#2a78d6`), 6 cycles/segment for a 1 (aqua `#199e70`); phase accumulates across segments so the wave never jumps; right-aligned 12px `#444` label "sound" at x=87 near y=205.
- **Legend (bold 12px, under the tone panel, y=262):** blue "0 = 1,070 Hz" at x≈220, aqua "1 = 1,270 Hz" at x≈470.
- **Annotation (bold 13px orange `#d95926`, centered, y=285):** "the phone network only ever hears sound".
- **Caption (11px `#6b7280`, top right, y=42):** "tone spacing exaggerated for visibility".
- **Fact status:** Bell 103 tone pair (1,070 / 1,270 Hz, 300 bit/s) and the 300–3,400 Hz voice band are documented; the drawn waveform is schematic.

## Downloading 5 MB at 56k: One Division

**Tags:** `worked example` (blue), `56k` (green), `per-minute pricing` (orange)

- **One division** — download time = file bits ÷ line bits per second; that is the whole estimate
- **The file** — 5 MB is 5,000,000 bytes = 40,000,000 bits to push through the call
- **At 56k** — 40,000,000 ÷ 56,000 ≈ 714 s ≈ 12 minutes, if nobody picks up the phone
- **The ladder** — same file: 37 h at 300, 4.6 h at 2400, 46 min at 14.4k, 23 min at 28.8k
- **Metered** — in many countries calls were billed per minute, so every megabyte had a phone-bill price
- **Fragile** — call waiting or a lifted handset mid-transfer could kill the whole download

*Example (italic):* Alice starts the 5 MB download at 9:00; at 9:07 Bob picks up the hallway phone and it dies at 58%.

**Key point:** Transfer time is size ÷ speed — the same one-line arithmetic still sizes every data transfer today.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: time to download a 5 MB file (40,000,000 bits) at each rung of the modem speed ladder, on a shared minutes axis; the 300 bit/s bar runs off the chart.

- **Title (bold 15px, `#1a5276`, top center):** "The Same 5 MB File at Every Rung of the Ladder".
- **Axis:** minutes 0–300 mapped to px x0=150 through x1=680; 1px `#999` baseline at y=250; `#e5e9ef` vertical gridlines and 12px `#444` tick labels at 0 / 60 / 120 / 180 / 240 / 300; axis caption "minutes" 12px `#444` centered at y=284.
- **Rows (bar tops y = 58 / 96 / 134 / 172 / 210, bars 26px tall, left-aligned 12px `#444` speed labels at x=20; times from 40,000,000 ÷ speed):**
  - "300 bit/s" — 2,222 min, clipped at the axis end: blue `rgba(42,120,214,0.30)` bar to x1 with 2px `#2a78d6` border, bold 13px `#2a78d6` right-aligned label inside the bar "≈ 37 hours →"
  - "2,400 bit/s" — 278 min bar, same blue fill/border, bold 13px `#2a78d6` label inside right "278 min ≈ 4.6 h"
  - "14.4 kbit/s" — 46 min bar, violet `rgba(74,58,167,0.30)` fill, 2px `#4a3aa7` border, bold 13px `#4a3aa7` label right of bar end "46 min"
  - "28.8 kbit/s" — 23 min bar, aqua `rgba(25,158,112,0.30)` fill, 2px `#199e70` border, bold 13px `#199e70` label "23 min"
  - "56 kbit/s" — 12 min bar, green `rgba(0,131,0,0.30)` fill, 2px `#008300` border, bold 13px `#008300` label "≈ 12 min"
- **Annotation (bold 13px orange `#d95926`, centered near x=450, y=165):** "the same 5 MB: 37 hours → 12 minutes".
- **Caption (11px `#6b7280`, bottom right, y=296):** "5 MB = 40,000,000 bits; time = bits ÷ rated speed — real 56k lines ran slower".

## The BBS: Dialing a Stranger's Computer Directly

**Tags:** `bulletin boards` (blue), `where it's used` (green), `one line` (orange)

- **A BBS** — a hobbyist's home computer with a modem, answering its own phone number after dinner
- **Point to point** — your call went straight to that one machine; no internet in the middle
- **One at a time** — one phone line meant one caller; the busy tone was the loading screen
- **Overnight files** — big downloads waited for night, when the line was free and calls often cheaper
- **Message boards** — leave a note, dial back tomorrow for replies: a forum at 2,400 bit/s
- **Old habits** — off-peak transfer windows and the nightly batch job are descendants of this

*Example (italic):* Carol redials Bob's BBS eleven times before the busy tone finally gives way to a connect chirp.

**Key point:** Scarce, priced-by-the-minute transfer taught habits data engineers still use — compress first, queue big jobs, move data off-peak.

### Visualization (canvas `c3`, 720×300)

Connection diagram for an evening at a single-line BBS: three callers on the left, the phone network in the middle, Bob's one-line BBS on the right; one caller connected, two hearing the busy tone.

- **Title (bold 15px, `#1a5276`, top center):** "One Phone Line, One Caller: an Evening at Bob's BBS".
- **Caller boxes (rounded 8px, 110×40, left column at x=30, tops y = 70 / 130 / 190, bold 12px `#2c3e50` labels):** "Alice" (green fill `rgba(0,131,0,0.12)`, 2px `#008300` border), "Carol" and "Dave" (grey fill `rgba(107,114,128,0.10)`, 2px `#6b7280` border).
- **Network box (rounded 8px, 160×60 at x=280, y=120, blue fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border):** label "phone network".
- **BBS box (rounded 8px, 155×70 at x=535, y=115, orange fill `rgba(217,89,38,0.15)`, 3px `#d95926` border):** three bold 12px lines "Bob's BBS" / "one modem" / "one line".
- **Lines:** Alice→network and network→BBS solid 3px `#008300`, with bold 12px `#008300` label "CONNECT 28800" above the network→BBS segment; Carol→network and Dave→network dashed 2px red `#e74c3c` (genuine failure state), each with bold 12px `#e74c3c` label "busy" near its midpoint.
- **Night note (bold 12px `#008300`, centered under the BBS box, y≈212):** "big files queued for after midnight".
- **Annotation (bold 13px orange `#d95926`, centered, y=268):** "the busy tone was the loading screen — redial and hope".
- **Caption (11px `#6b7280`, centered, y=290):** "schematic — a single-line bulletin board system".

## Why 56k Was the Ceiling

**Tags:** `common mistake` (red), `the ceiling` (orange)

- **The confusion** — "why not a 100k modem?" — the voice channel was the wall, not the modem
- **Digital container** — the network samples each call into 64 kbit/s; a modem can't beat its container
- **Noise tax** — line noise over the ~3.1 kHz band caps fully-analog modems near 35 kbit/s
- **56k's trick** — it needed a digital line on the ISP side, and US rules capped it at 53.3 kbit/s
- **Asymmetric** — "56k" was download only; uploads stayed at 33.6 kbit/s
- **DSL's move** — broadband didn't squeeze harder, it left the voice band for higher frequencies

*Example (italic):* Bob's brand-new 56k modem reports "CONNECT 44000" — his aging copper line, not the modem, picks the number.

**Common mistake:** When the channel is the bottleneck, better endpoints stop helping — broadband changed the channel, not the modem.

### Visualization (canvas `c4`, 720×300)

Line chart of modem standard speeds by year on a log scale, flattening against a dashed ceiling at the 64 kbit/s digital voice channel, with a green arrow showing DSL leaving the band entirely.

- **Title (bold 15px, `#1a5276`, top center):** "Modem Speeds Climb Toward a Hard Ceiling (log scale)".
- **Axes:** x = years 1960–2000 mapped to px 95–650; y = log10(bit/s) from 2.3 to 5.0 mapped to baseline y=245 up to y=60; 1px `#999` axis lines; year ticks 12px `#444` at 1960 / 1970 / 1980 / 1990 / 2000.
- **Gridlines (`#e5e9ef`) with right-aligned 12px `#444` left labels:** at 300 ("300"), 2,400 ("2.4k"), 14,400 ("14.4k"), 56,000 ("56k").
- **Ceiling:** dashed 2px magenta `#d55181` horizontal line at 64,000, bold 12px `#d55181` label above it "64 kbit/s — the digital voice channel is the container".
- **Data points (hardcoded, blue `#2a78d6` 3px line, 4.5px dots):** (1962, 300), (1982, 1,200), (1984, 2,400), (1991, 14,400), (1994, 28,800), (1996, 33,600), (1998, 56,000); bold 12px `#1a5276` speed labels near each dot ("300", "1.2k", "2.4k", "14.4k", "28.8k", "33.6k", "56k"), offset to avoid crowding at the right edge.
- **DSL arrow:** 3px green `#008300` arrow from (1999, 56k) up past the ceiling line, bold 12px `#008300` two-line label "DSL leaves the voice band —" / "ceiling gone".
- **Annotation (bold 13px orange `#d95926`, near x=290, y=105):** "35 years squeezing the same 3.1 kHz voice band".
- **Caption (11px `#6b7280`, bottom right, y=296):** "rated speeds of modem standards; dates approximate".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" or "Common mistake:").
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` reserved for genuine failure states (the busy-tone lines in c3).
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Documented facts: Bell 103 tones 1,070 / 1,270 Hz at 300 bit/s; voice band ~300–3,400 Hz; digital voice channel 64 kbit/s; V.90 "56k" capped at 53.3 kbit/s in the US with 33.6 kbit/s upload; modem standard speeds 300 / 1,200 / 2,400 / 14,400 / 28,800 / 33,600 / 56,000 bit/s (standard dates approximate). Illustrative figures (per-minute pricing, "CONNECT 44000", the ~35 kbit/s analog noise cap as a round number) are hedged in text or labeled in captions. Arithmetic that must stay consistent between text and charts: 5 MB = 40,000,000 bits; ÷ 56,000 ≈ 714 s ≈ 12 min; ÷ 28,800 ≈ 23 min; ÷ 14,400 ≈ 46 min; ÷ 2,400 ≈ 278 min ≈ 4.6 h; ÷ 300 ≈ 2,222 min ≈ 37 h.
- This page has no links.
