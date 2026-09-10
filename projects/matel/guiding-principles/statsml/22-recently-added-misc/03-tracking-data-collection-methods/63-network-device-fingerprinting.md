# Tracking Data: Network Device Fingerprinting

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Network Device Fingerprinting

**Subtitle:** Joining a network requires a handshake. The handshake itself carries the shape of the device's network stack, and usually a hostname the owner chose.

## Section 1: What is it?

How a device asks to join is as distinctive as what it asks for.

- **The join:** a laptop, phone or smart TV has to request an address and negotiate a connection
- **Nothing is arbitrary:** TCP option order, the DHCP request list, and the TLS cipher list are fixed by the software that built the packet
- **Read together** they narrow the device down to an operating system and version
- **Hostname** arrives the same way — free text the owner typed once, often a first name and a model

**Key point (callout):** **The hostname is a label, not a measurement:** nothing on the network checks it.

### Visualization (canvas `c1`, 720×320)

Schematic diagram: a device connecting to a router, with the handshake fields fanning out from the device, one hue per field. A shared per-signal hue map `SIG` is held constant across c1 and c2 so the same field reads as the same colour wherever it appears: TLS = blue `#2a78d6`, DHCP option 55 = green `#008300`, TCP = violet `#4a3aa7`, IP TTL/MSS = orange `#d95926`, ALPN = aqua `#199e70`, hostname = magenta `#d55181`.

- **Header (bold 14px `#1a5276`, top center):** "FIELDS THE JOIN HANDSHAKE CARRIES".
- **Router (right, an object not a signal, so neutral ink):** solid `#1a5276` rect 60×40 centered at x=540, y=118 with white 12px "ROUTER" and two ink antenna strokes (width 2) angling up from its top corners.
- **Device (left, also neutral):** laptop glyph centered at x=62, y=118 — outer `#2c3e50` rect 70×44, inner screen `#e5e9ef` rect 60×32, base `#2c3e50` bar 80×6 below.
- **Connection line:** dashed gray (`#6b7280`, dash 5/3, width 2) from the device's right edge to the router's left edge.
- **Five fanning field lines** from the device (each: a 1.5px line in its hue at angles −0.6 to 0.8 rad, a 3px-radius dot at the end, and a bold 13px left-aligned label in the same hue):
  - "DHCP option 55 request list" — green `#008300`
  - "TCP option order and TTL" — violet `#4a3aa7`
  - "DHCP hostname (owner-typed)" — magenta `#d55181`
  - "TLS cipher and extension order" — blue `#2a78d6`
  - "ALPN protocol list" — aqua `#199e70`
- **Footer band (y=238, height 60):** magenta-tinted panel (alpha 0.10) with 4px magenta left bar; three lines:
  - bold 13px `#2c3e50`: "Four of these fields are set by the software that built the packet."
  - bold 13px magenta: "The hostname (magenta) is free text the owner typed once —"
  - 13px `#2c3e50`: "a label rather than a measurement, and nothing checks it."

## Section 2: What does it collect?

- **Manufacturer** — Apple, Samsung, Dell and so on, usually read from the hardware (MAC) address, whose first bytes are assigned per maker; modern phones randomize it per network, which the other fields survive
- **Operating system** and version
- **Hostname**, often a first name and a model
- **TCP/IP stack behaviour**, distinctive per OS version
- **DHCP fingerprint** — how it asks for an address
- **Open ports** and running services

**Key point (callout):** **It resolves software, not a person:** option ordering and cipher list are properties of whatever built the packet, so the identifier is an implementation.

**Key point (callout):** **Popularity works against uniqueness:** a stock phone on a current OS shares its signature with every other such phone, so this identifies rare setups far better than common ones.

**Key point (callout):** **It survives a VPN:** the tunnel carries the handshake rather than generating it, so the fields describe a layer above the encrypted transport — which is why a record can flag a tunnel and still read the stack inside it.

### Visualization (canvas `c2`, 720×320)

Discrete-slot strength chart: what each handshake field resolves — schematic, no measured values. Each row is one signal in the same `SIG` hue used on c1; strength is shown as three discrete slots (3 = narrows to an OS build, 2 = narrows to a family, 1 = weak on its own).

- **Header (bold 13px `#1a5276`, top center):** "WHAT EACH FIELD NARROWS DOWN TO — SCHEMATIC, NOT MEASURED".
- **Rows (bar height 24, gap 8, from y=34; label right-aligned bold 14px in the signal's hue at x=140; three 72px slots starting at x=150, step 78; filled slots = hue tint at alpha 0.55, empty slots = grid gray `#e5e9ef`; qualitative note 13px `#6b7280` to the right):**
  - "TLS extension order" — blue, level 3, note "OS / library build"
  - "DHCP option 55 list" — green, level 3, note "OS family + version"
  - "TCP option order" — violet, level 2, note "OS family"
  - "IP TTL / MSS" — orange, level 1, note "weak alone; flags tunnels"
  - "ALPN list" — aqua, level 1, note "weak alone"
  - "Hostname string" — magenta, level 2, note "owner-typed, unverified"
- **Scale caption (13px `#6b7280`, under the slots):** left "weak on its own", right "narrows to a build".
- **Footer band (below the scale caption, height 42):** magenta-tinted panel (alpha 0.10) with 4px magenta left bar; bold 13px magenta: "Hostname (magenta) reaches two slots on typed text, not on stack behaviour —"; 13px `#2c3e50`: "it is the one row whose value nothing on the network verifies."

### Example payload (below canvas `c2`, right column)

Visible caption above the block (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// Field names come from the TCP, DHCP and TLS specs.
// Packaging into one record is reconstruction.
{
  // ── present in the handshakes themselves ──
  "tcp": {
    "ip_ttl":       64,
    "mss":          1460,
    "option_order": ["mss","sackOK","TS","nop","wscale"]
  },
  "dhcp_option_55": [1, 121, 3, 6, 15, 119, 252],
  "dhcp_hostname":  "…-MacBook-Pro",
  "tls_client_hello": {
    "cipher_suites":   ["0x1301","0x1302","0xc02c","…"],
    "extension_order": [0, 23, 65281, 10, 11, 35, 16, 5],
    "alpn":            ["h2","http/1.1"]
  },

  // ── inferred / plausible, matched to a signature set ──
  "os_guess":     "macOS 14.x",
  "os_conf":      0.86,
  "vpn_detected": true      // ip_ttl / mss inconsistency
}
```

## Section 3: Why is it collected?

**Label (`.lbl-purpose`):** Stated purpose

- **Inventory** — an operator needs to know what is attached to the network it runs, and to spot a device that should not be there
- **Apply the right policy** per device, and tell a stock phone apart from a bank of machines presenting as many users

**Label (`.lbl-effect`):** Additional consequence

- The same signature is a **device census** — counting distinct stacks behind one address estimates how many devices a household owns, and of what type
- Available to **anyone carrying the traffic**, without any device agreeing to be counted

**Key point (callout):** **Device count is not person count:** the signature describes software, so it splits one person across their laptop and phone and merges every household member behind one address. The error runs both ways — a household of one with six devices and a household of six sharing two are both mis-sized.

### Visualization (canvas `c3`, 720×320)

Paired bar chart: a device census read as a person count, and how far it misses. Four illustrative homes behind one address; distinct signatures is what the census counts, people is what it gets read as. Identical devices collapse into one signature, so the error runs in both directions.

- **Title (bold 14px `#1a5276`, top left at x=22):** "Distinct signatures behind one address, and the people they get read as".
- **Legend (12px, 10×10 swatches at y≈32):** aqua `#199e70` "people in the home"; blue `#2a78d6` "distinct signatures seen".
- **Data (four households, [people, distinct signatures]):**
  - "one person," / "six devices" — people 1, signatures 5
  - "a couple with" / "mixed devices" — people 2, signatures 3
  - "family of five," / "two shared devices" — people 5, signatures 2
  - "four sharing" / "one phone model" — people 4, signatures 1
- **Axes/scale:** baseline (ink `#1a5276`, 1.5px) at y=216; y scale 0–6, max bar height 116; gridlines and right-aligned 12px `#6b7280` tick labels at 0, 2, 4, 6 (gridlines `#e5e9ef`).
- **Bars:** per home, two 34px bars around the slot center (people left in aqua, signatures right in blue) — fill hue tint at alpha 0.34, hue stroke 1px, bold 13px value in hue above each bar.
- **Miscount labels (bold 13px above each pair, at y = baseline − 124):** colored orange `#d95926` when over, violet `#4a3aa7` when under: "over by 4", "over by 1", "under by 3", "under by 3".
- **Home labels:** two 12px `#2c3e50` lines centered below the baseline per home.
- **Bottom captions (centered):** italic 12px `#2c3e50`: "Identical devices collapse into one signature, so the count misses in both directions."; italic 11px `#6b7280`: "Illustrative households — the direction of the error is the point, not the counts."

## Regeneration instructions

- **Layout:** tracking detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` paragraph, bullets, `.lbl` labels and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600; `.lede` 0.95em.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `strong` in `#1a5276`.
- **Labels:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload style:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** intrinsic 720×320 each; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts use a shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`, a per-signal hue map `SIG` shared by c1 and c2, a `tint(hex, alpha)` helper for translucent fills, and a rounded-rect path helper. Red is reserved for alarm states and not used here. All chart data is hardcoded literal arrays (no Math.random); the c3 miscount labels are computed in-script from the literal people/signature counts.
- **Project palette reference:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
