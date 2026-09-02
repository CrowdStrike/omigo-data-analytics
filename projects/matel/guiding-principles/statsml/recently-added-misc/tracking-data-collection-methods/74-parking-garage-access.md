# Tracking Data: Parking and Garage Access Control

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: Parking and Garage Access Control

**Subtitle:** A barrier has to decide whether to lift, so it writes down what it decided. The row it writes is a billing record about a vehicle that later gets read as a presence record about a person.

## Section 1: What is it?

A barrier that has to decide, so it writes down what it decided.

- **Credential at the lane:** a card or fob on a post reader, a windscreen tag answering a radio query, or a camera reading the plate and treating it as the credential
- **Ticket garages** issue paper or a QR code at entry and take payment or a validation before the exit barrier lifts
- **At home:** a wi-fi opener reports door state to a phone app and accepts a remote open, making a garage door an event stream
- **Per-space sensors** — an ultrasonic or magnetic unit per bay — drive the entrance-sign count and the aisle lamps

**Both directions are recorded because duration is billable:** the exit event is part of the transaction. A flat-fee facility has no reason to keep the second half of the pair; an hourly one has to.

**Strong about a vehicle, weak about a person:** a carpool comes through on one credential, a spouse borrows the car, a valet parks it, a pool vehicle is signed out by whoever needs it. The reader authenticates the tag, not the driver — one vehicle to one person is an assumption added afterwards.

### Visualization (canvas `c1`, 720×320)

Left-to-right flow schematic: credential kinds feed a lane controller which writes one stored row; the row fans out to several possible people.

- **Title (bold 16px, centered, blue `#2a78d6`, y=22):** "The barrier must decide, so the decision is written down".
- **Left column:** four stacked boxes centered at x=96, width 132, height 26, starting y=44 with 8px gaps; fill `rgba(42,120,214,0.35)`, 1px stroke `#2a78d6`; labels (14px, `#2a78d6`): "card or fob", "windscreen tag", "plate read", "entry ticket". A blue arrow (2px line with filled arrowhead) from each box to the controller box.
- **Controller box:** outlined rect (2px `#2a78d6`) at x=262, y=62, 128×96. Text: bold 14px blue "lane controller"; 13px `#2c3e50` "lift, or refuse" / "and say why"; 13px muted `#6b7280` "entry AND exit".
- **Stored-row box:** filled rect `rgba(42,120,214,0.35)` with 1px blue stroke at x=452, y=76, 150×68. Text: bold 14px blue "one stored row"; 13px `#2c3e50` "lane, credential," / "direction, time, result". Blue arrow from controller into it.
- **Fan-out:** four thin orange (`#d95926`, 1px) lines from the stored-row box's right edge to right-aligned labels at x=664, starting y=62, 24px apart (13px): "the permit holder" (colored `#2c3e50`), then "a carpool", "a borrowed car", "a valet or fleet" (all orange).
- **Bottom text (centered, 14px `#2c3e50`, y=196):** "the row identifies a vehicle or an entitlement — the person is an added assumption".
- **Caption (centered, 13px `#6b7280`, bottom):** "Schematic".

## Section 2: What does it collect?

- **Lane or reader identity**, and which side of the barrier it sits on
- **Direction** — entry or exit
- **Credential presented** — card or fob number, tag id, ticket number, or a plate string
- **Timestamp** of the read, usually to the second
- **The decision** — lifted, refused, and the reason for a refusal
- **Loop or presence detector state**, how the barrier knows a vehicle is still under it
- **Payment**, validation code or rate plan applied at exit
- **Residential openers:** open, closing, closed and obstructed state changes, and whether the command came from a remote, a keypad or the app
- **Per-space sensors:** bay id and occupied or vacant transitions

**`duration_min` is not measured:** it is the difference between two independent reads that something had to decide belonged together, and the fields under it exist because that decision can be wrong.

**Dropping the provenance field hides the difference:** `duration_source: "assumed_open"` and `"paired"` look identical once the field is gone and the durations are averaged.

### Visualization (canvas `c2`, 720×320)

Two-timeline pairing diagram: entry reads on a top line, exit reads on a bottom line, with matched pairs connected and two unmatched events flagged.

- **Title (bold 16px, centered, blue `#2a78d6`, y=22):** "Duration exists only if an entry can be matched to an exit".
- **Timelines:** two horizontal light-gray (`#e5e9ef`, 1px) lines from x=92 to x=686, entries line at y=74, exits line at y=156. Right-aligned 14px `#2c3e50` labels left of the axis: "entries", "exits".
- **Event ticks:** vertical 2px tick (±8px) with 3.5px-radius dot at each event. Entries at fractional positions `[0.04, 0.13, 0.21, 0.30, 0.44, 0.57]`; exits at `[0.29, 0.40, 0.52, 0.68, 0.84, 0.94]` of the span. First five of each are blue `#2a78d6`; the last of each (entry 0.57, exit 0.94) is orange `#d95926`.
- **Matched pairs:** green (`#008300`, 1.5px) diagonal lines connecting entries[0..4] to exits[0..4] respectively.
- **Unmatched events:** dashed (4/4) orange 1.5px stub lines from the last entry (downward-right) and the last exit (upward-left), each ending near an orange "?" (13px).
- **Annotations (13px orange):** left-aligned at 50% width, y=44/58: "entry with no exit read —" / "barrier held open for two cars"; right-aligned at x=686, below the exits line: "exit with no entry read —" / "tailgated in behind another vehicle".
- **Legend (bold 14px green, centered at 22% width, below exits line):** "green = a pair a matcher accepted".
- **Bottom text (centered, 14px `#2c3e50`, y=208):** "drop the unpaired rows and the remaining sample is biased, not conservative".
- **Caption (centered, 13px `#6b7280`, bottom):** "Illustrative".

### Payload (right column, below canvas)

Caption above the block (italic, `.payload-note`): "Sample payload — illustrative structure, not real captured data."

```
{
  // ── documented: what a barrier controller needs in order to decide ──
  "lane_id":      "P2-EXIT-B",
  "direction":    "exit",
  "credential":   { "type": "rfid_tag", "id_hash": "a91f…" },
  "ts":           "2026-08-22T18:41:07Z",
  "result":       "granted",
  "entitlement":  "monthly_permit",

  // ── inferred / plausible ──
  "matched_entry_id": "EV-882145",
  "duration_min":     551,        // DERIVED from the paired entry
  "pair_confidence":  0.62,       // one candidate entry, different lane
  "unmatched_flag":   false,
  "duration_source":  "paired",   // paired | assumed_open | truncated
  "notes":            "entry read at P1-ENT-A; permit valid at both"
}
```

## Section 3: Why is it collected?

**Stated purpose** (label pill, blue)

- **Charging correctly** — billing an hourly stay needs the entry read matched to the exit read
- **The entrance sign** needs a running count of spaces left

**Additional consequence** (label pill, orange)

- A billing log is also a **timestamped arrival and departure series** per credential, kept long after the charge settled
- Nothing changes when it is queried for **who came in early, or which permits go unused**

**Throwing out the broken pairs is not neutral cleanup:** pairing fails when a barrier is held open for two cars, when a vehicle tailgates through without reading, when a controller restarts — all of which happen most at the crowded end of a shift. The rows that go missing are the ones carrying the occupancy being estimated, so a tidy subset is biased, not cautious.

### Visualization (canvas `c3`, 720×320)

Line chart: true occupancy vs the running gate-event count over a day, with a growing error band.

- **Title (bold 16px, centered, blue `#2a78d6`, y=22):** "Entries minus exits never corrects itself".
- **X-axis:** hours 7 through 19; plot from x=96 to x=686; muted (`#6b7280`, 1px) baseline at y=178 with 5px ticks and 13px labels at every other hour: "7:00", "9:00", "11:00", "13:00", "15:00", "17:00", "19:00".
- **Y-scale:** 0 to 500 vehicles mapped from baseline y=178 up to y=48.
- **Truth series (solid green `#008300`, 2px):** "cars actually inside" = `[40, 120, 260, 380, 430, 445, 430, 420, 400, 330, 210, 110, 45]`.
- **Counted series (dashed 5/4 blue `#2a78d6`, 2px):** "running count from gate events" = `[42, 126, 272, 398, 452, 472, 462, 458, 444, 380, 266, 172, 112]`.
- **Error band:** region between the two series filled `rgba(217,89,38,0.20)`.
- **Series labels (bold 13px):** blue "running count from gate events" above the counted line near hour index 1; green "cars actually inside" below the truth line near hour index 2.
- **End-of-day residual:** vertical orange (`#d95926`, 2px) segment at the last hour between the two series; right-aligned 13px orange annotation: "residual at close —" / "cleared by a nightly reset".
- **Bottom text (centered, 14px `#2c3e50`, baseline+40):** "the gap only grows, so the count is least trustworthy when the sign matters most".
- **Caption (centered, 13px `#6b7280`, bottom):** "Schematic".

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas and, in row 2, the `.payload-note` + `.payload` pre block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em; `li b` `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, monospace (ui-monospace/Menlo) 0.78em, `white-space: pre`; `.payload-note` 0.82em `#666` italic.
- **Canvas:** intrinsic `width="720" height="320"` per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes.
- **Chart palette (tracking pages use the CVD-checked categorical set, not the site default):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately excluded from the rotation, reserved for genuine alarm states. Site-wide accents elsewhere: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- Includes an `arrowRight` helper (2px line plus filled triangular head) and a rounded-rect path helper `rr` for canvases.
