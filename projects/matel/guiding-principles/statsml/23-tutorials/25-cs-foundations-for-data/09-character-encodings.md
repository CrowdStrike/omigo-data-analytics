# Character Encodings

**Page type:** detail page (tutorial page: 4 card-sections, each an h2 + two-column layout table, text left 50%, canvas right 50%)
**HTML title tag:** Character Encodings

**Subtitle:** "café" saved as UTF-8 but opened as Latin-1 turns into "cafÃ©" — text is just bytes plus a decoder ring, and both sides must use the same ring

## One Word, Two Readings

Tags: `core idea` (blue), `running example` (green)

- **Text is bytes** — files store numbers; an encoding says which number means which character
- **Our word** — "café" saved as UTF-8 becomes five bytes: 63 61 66 C3 A9
- **Right ring** — read as UTF-8, the pair C3 A9 decodes back to one é
- **Wrong ring** — read as Latin-1, C3 A9 decodes as two characters: Ã©
- **The tell** — garbage like Ã©, Ã¼, â€™ almost always means "UTF-8 read as Latin-1/Windows-1252"

*Example:* The file never changed — the same five bytes were decoded with the wrong ring.

**Key point:** A text file does not carry its encoding — the reader guesses, and a wrong guess garbles every non-English character.

### Visualization (canvas `c1`, 720×300)

Flow diagram: the word "café" is encoded to a five-byte strip, which then decodes two ways (fork to two outcomes).

- **Title (bold 15px, ink `#1a5276`, top center):** "Same five bytes — the reader chooses the meaning".
- **Source:** bold 18px monospace `"café"` at left (x=90), with mute 11px note "4 characters" below.
- **Encode arrow:** blue `#2a78d6` arrow labeled 11px "save as UTF-8".
- **Byte strip:** five 44×36 byte cells starting at x=210, hex labels `63`, `61`, `66`, `C3`, `A9` in bold 13px monospace; the last two cells (C3, A9) highlighted orange `#d95926` (fill rgba(217,89,38,0.12), stroke width 2); plain cells fill `#f8f9fa`, stroke `#c8ced6`. Mute 11px caption below: "5 bytes on disk (hex)".
- **Decode fork:** two arrows from the strip's right end — green `#008300` up to the good outcome, orange down to the bad one.
- **Good outcome (green):** 11px "decode as UTF-8"; bold 17px monospace `"café"`; 11px "C3 A9 → one é".
- **Bad outcome (orange):** 11px "decode as Latin-1"; bold 17px monospace `"cafÃ©"`; 11px "C3 → Ã, A9 → ©".
- **Bottom line (ink bold 13px centered):** "the bytes are identical in both rows — only the decoder ring differs".

## The Bytes by Hand

Tags: `worked example` (green)

- **c, a, f** — plain ASCII: bytes 63, 61, 66 (hex); every encoding agrees on these
- **é** — Unicode code point U+00E9; UTF-8 writes it as two bytes, C3 A9
- **Latin-1's é** — a single byte, E9; that is why the two rings disagree
- **The misread** — Latin-1 maps C3 to Ã and A9 to ©, one character per byte, no questions
- **Count check** — "café" is 4 characters but 5 UTF-8 bytes: two different questions

*Example:* len("café") is 4 in characters and 5 in bytes — never assume they match.

**Key point:** Bytes below 128 (ASCII) are safe everywhere; the disagreement starts at 128 — exactly where accents, symbols and emoji live.

### Visualization (canvas `c2`, 720×300)

Byte-grid diagram: a central row of five byte cells with the UTF-8 grouping bracketed above and the Latin-1 reading bracketed below.

- **Title (bold 15px, `#1a5276`, top center):** "Five bytes, two decodings".
- **Byte row:** five 88×44 cells at x=128, y=128 (6px gaps), hex labels `63`, `61`, `66`, `C3`, `A9`; the last two highlighted orange as in c1. Left-side mute 11px label: "bytes (hex):".
- **UTF-8 reading (above, green `#008300`):** left label bold 12px "read as UTF-8:"; green brackets group the cells 1|1|1|2 with bold 16px monospace characters above each group: "c", "a", "f", "é" (the é bracket spans the C3+A9 pair).
- **Latin-1 reading (below, orange `#d95926`):** left label bold 12px "read as Latin-1:"; an orange bracket under EACH single cell with bold 16px monospace characters: "c", "a", "f", "Ã", "©" (Ã and © in orange, the rest in `#2c3e50`).
- **Captions:** orange bold 13px centered: "Latin-1 always reads one byte per character — it splits the é pair in two"; mute 12px centered: "4 characters ≠ 5 bytes: character count and byte count are different questions".

## The Partner CSV That Broke on an Emoji

Tags: `where it's used` (blue), `common mistake` (orange)

- **The story** — a partner's CSV loads fine for months, then a comment contains 🙂
- **The bytes** — 🙂 is four UTF-8 bytes: F0 9F 99 82; Latin-1 has no such character
- **The failure** — a strict reader throws UnicodeDecodeError; a lax one renders ð-garbage
- **The fix** — read with `encoding="utf-8"`, and ask the partner to declare theirs
- **Everywhere rule** — new systems: UTF-8 in, UTF-8 out; it covers every language and emoji

*Example:* `pd.read_csv(f, encoding="utf-8")` — one argument ends a whole class of 3am pipeline failures.

**Key point:** Pipelines break on the first byte above 127, not on day one — declare encodings instead of relying on lucky ASCII-only data.

### Visualization (canvas `c3`, 720×300)

Stacked-cell column chart: UTF-8 byte count per character, one column of stacked byte cells per character.

- **Title (bold 15px, `#1a5276`, top center):** "UTF-8 spends 1 to 4 bytes per character".
- **Columns (character, hex bytes stacked bottom-up, color, note):**
  - "A" — `41` — blue `#2a78d6` — "ASCII letter"
  - "é" — `C3`, `A9` — aqua `#199e70` — "accented latin"
  - "中" — `E4`, `B8`, `AD` — violet `#4a3aa7` — "CJK character"
  - "🙂" — `F0`, `9F`, `99`, `82` — orange `#d95926` — "emoji"
- **Cells:** each byte is a box filled with its column color at alpha 0.25, solid 1.5px stroke, bold 12px monospace hex label. Character shown bold 18px below the axis; note in mute 11px beneath it.
- **Y axis:** mute 12px labels "1 byte", "2 bytes", "3 bytes", "4 bytes"; baseline axis in mute. Padding: top 56, bottom 64, left 70, right 30.
- **Annotation (orange bold 13px, top-left of plot):** "ASCII stays 1 byte — the bug hides until the first 🙂 arrives".

## The Common Confusions

Tags: `common mistake` (orange), `rule of thumb` (green)

- **Not a font issue** — fonts draw characters; the encoding decides which characters exist
- **Don't find-replace** — patching Ã© into é fixes one symptom; the next file breaks anew
- **Fix at the boundary** — decode correctly on read, encode UTF-8 on write
- **Double trouble** — misread text re-saved as UTF-8 doubles the garbage (é → 4 bytes)
- **The data is intact** — mojibake means the bytes are fine; only the reading was wrong

*Example:* A table "cleaned" by find-replace still broke — the loader kept misreading every new file.

**Key point:** Fix encodings where data enters and leaves the system, never by rewriting characters inside it.

### Visualization (canvas `c4`, 720×300)

Flowchart: the double-mojibake chain — a left column of three step boxes flowing down, a connector to a right column of two more boxes.

- **Title (bold 15px, `#1a5276`, top center):** "Each wrong round trip doubles the garbage".
- **Step boxes:** 52px tall, fill `#f8f9fa`, colored 1.5px stroke, bold 14px monospace main label + mute 11px sublabel.
- **Left chain (centered at x=200, connected by labeled down arrows):**
  1. `"é"` — "the real character" (green `#008300`)
  2. arrow "save as UTF-8" (blue `#2a78d6`)
  3. `C3 A9` — "2 bytes on disk" (blue)
  4. arrow "misread as Latin-1" (orange `#d95926`)
  5. `"Ã©"` — "now 2 wrong characters" (orange)
- **Connector:** magenta `#d55181` right-angle line from the left chain's bottom box to the right chain's top box, labeled 11px two lines: "\"fixed\" by saving" / "the garbled text".
- **Right chain (centered at x=520):**
  1. `C3 83 C2 A9` — "re-saved as UTF-8: 4 bytes" (magenta)
  2. arrow "misread again" (magenta)
  3. `"ÃÂ©"-style garbage` — "twice-cooked mojibake" (magenta)
- **Bottom line (ink bold 13px centered):** "the é grew from 2 bytes to 4 — fix the read, never patch the characters".

## Regeneration instructions

- **Template/layout:** tutorial detail page (per `tutorials/CLAUDE.md`, social-graph reference skeleton). Page = `<h1>` + `.subtitle` paragraph, then 4 `.card-section` blocks. Each `.card-section` has an `<h2>` (1.3rem, `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%), cell padding 12px, vertical-align top.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>` "Key point:" prefix.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg rgba(26,82,118,0.12) / `#1a5276`; green: bg rgba(39,174,96,0.15) / `#27ae60`; red: bg rgba(231,76,60,0.12) / `#e74c3c`; orange: bg rgba(230,126,34,0.15) / `#e67e22`.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; `<code>` in ui-monospace on `#f4f6f8`. No nav bar, no back/home links.
- **Canvases:** 720×300 intrinsic, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled via `window.devicePixelRatio` in a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). This page also defines a shared `byteCell(ctx, x, y, w, h, label, hot)` helper for the byte boxes (hot = orange highlight).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- Grid cards elsewhere linking here use `.html` extensions in regenerated HTML.
