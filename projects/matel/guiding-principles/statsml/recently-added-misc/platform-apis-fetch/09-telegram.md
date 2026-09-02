# Telegram

**Page type:** detail page (two-column obj-table: text left 45%, code sample + canvas right 55%, one row)
**HTML title tag:** Telegram — Platform APIs

**Subtitle:** Lets a bot receive and send messages in the Telegram chats and channels it has been added to.

**Verified badge:** Last verified: August 2026

## Overview (untitled — this page's obj-table has no h2 above it)

### What you can get

- New messages and channel posts, delivered as they happen
- Basic chat facts: member count and the list of admins
- Files people send to the bot (up to about 20 MB)
- Join, leave, and reaction events in chats the bot is in

**Key-point callout:** **A bot only sees what is routed to it, and only briefly.** It cannot read anything from before it joined, cannot list a group's members, and undelivered updates are discarded after about a day. Telegram is a live feed you must store yourself, not a message archive you can query later.

### Watch out for

- Bots can only be added to channels as administrators
- In groups, "privacy mode" is on by default — the bot misses most messages unless that setting is turned off
- A bot cannot message anyone who has not messaged it first, so outbound campaigns are impossible
- Tools that promise full history or member lists are using the separate user-account API, which risks getting a real account banned

### Code sample (right column)

Heading: **Asking for a group's members**

```
GET /bot{token}/getChatMemberCount?chat_id=-1001234567890

{ "ok": true, "result": 18342 }
```

Caption below the pre (0.88em `#666`): A single number. There is no method that returns the 18,342 members themselves.

### Visualization (canvas `apiBreadth`, responsive width × 380)

Paired horizontal bar chart comparing Bot API vs MTProto capability breadth, one capability per row, two bars per row (MTProto on top, Bot API below). Bar length encodes a 0/1/2 score.

- **Title (bold 13px `#1a5276`, top center):** "Bot API vs MTProto client API — capability breadth"
- **Subtitle (italic 10px `#888`):** "bar length = 0 not possible / 1 conditional / 2 available"
- **Scale:** x from 0 to 2, `#eee` vertical gridlines at 0, 1, 2; `#888` baseline axis at the bottom of the rows. Row labels right-aligned (10px `#2c3e50`) in a label column of min(232, 44% width). Margins: top 44, bottom 52, right 16.
- **Rows (label, bot score, MTProto score, optional note):**
  - Receive updates addressed to self — bot 2, mt 2
  - Read all group messages — bot 1, mt 2, note "bot: privacy mode off"
  - Read channel posts after joining — bot 2, mt 2
  - Read full channel history — bot 0, mt 2
  - Enumerate group / channel members — bot 0, mt 2
  - List administrators only — bot 2, mt 2
  - Per-post view counts — bot 0, mt 2
  - Initiate contact with a user — bot 0, mt 2
  - Backfill older than ~24h — bot 0, mt 2
  - Download large media — bot 1, mt 2, note "bot: ~20 MB cap"
  - Deletion notifications — bot 0, mt 2
- **Bar colors:** MTProto bar always `#8e44ad`; Bot API bar `#1a5276` when score 2, `#e67e22` when score 1, `#e74c3c` when score 0. Score-0 rows draw a 3px stub plus bold 9px text "not available via Bot API" beside it; score-1 rows print their note in 9px `#666` after the bar end.
- **Legend (10px, above the bottom takeaway):** swatches for "MTProto (user session)" `#8e44ad`, "Bot API" `#1a5276`, "Bot API, conditional" `#e67e22`, "Bot API, none" `#e74c3c`.
- **Takeaway (italic 10px `#e74c3c`, bottom left):** "MTProto breadth is not free: FLOOD_WAIT throttling, ToS exposure, and account-ban risk on a real number."

## Official API References

- [Telegram Bot API Reference](https://core.telegram.org/bots/api) — full method and type reference for the Bot API
- [Bots: An Introduction for Developers](https://core.telegram.org/bots) — bot platform overview, BotFather, and privacy mode settings

## Regeneration instructions

- **Layout:** single page: h1, `.subtitle`, `.verified` badge, then the one-row `.obj-table` directly (no h2 before it on this page): left `<td>` 45% with `.section-title` headings, bullet lists, and a `.key-point` callout; right `<td>` 55% with a `.section-title`, a `<pre>` code sample, a small caption paragraph (inline style `font-size:0.88em; color:#666; margin-top:-6px;`), and the canvas. Then `h2` "Official API References" with a link list.
- **Page style:** body system sans-serif, `#2c3e50` text, white background, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` inline badge — background `#eaf2f8`, border 1px `#2980b9`, color `#1a5276`, 0.8em, radius 4px, padding 2px 10px; `.section-title` bold `#1a5276` 1.05em; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `.key-point` background `#f8f9fa`, left border 3px `#e74c3c`, padding 10px 14px, 0.93em; li 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="apiBreadth" height="380">`, CSS `width: 100%`; redraws on window resize using `getBoundingClientRect()` width; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and `ctx.scale` back to logical coordinates (with `setTransform` reset before scaling).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, grays `#666`/`#888`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions.
