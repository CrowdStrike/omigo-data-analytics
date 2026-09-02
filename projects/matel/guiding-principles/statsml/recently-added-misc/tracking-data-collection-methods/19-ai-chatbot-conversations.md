# Tracking Data: AI Chatbot Conversations

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: AI Chatbot Conversations

**Subtitle:** A chatbot request contains the prompt itself, so the content is the record rather than metadata about one. Retention and training settings are account-level, while the disclosures inside a prompt often are not.

## Section 1: What is it?

**Lede:** The prompt is the record, not metadata about one.

- **No envelope/body split:** the conversation itself is the payload
- **Threaded:** a conversation is retained so follow-up turns have context
- **Retention, and whether content may be used for training, are settings** — they differ by product and by plan, with consumer and business tiers commonly differing on the training default
- **Both are account properties**, not properties of any sentence typed in

**Callout:** **Consent boundary mismatch:** the account holder sets a preference for themselves. A prompt describing a colleague, a client, or a relative discloses information about someone with no account and therefore no setting. Consent follows account ownership; the information in the text does not.

### Visualization (canvas `c1`, 720×320)

Comparison diagram: the prompt is the payload — no envelope/body split — plus a thread-to-storage flow.

- **Header band (y 0–46):** blue wash `rgba(42,120,214,0.07)` with a 2px blue `#2a78d6` rule at y=44; bold 16px blue left-aligned title: "No envelope / body split — the prompt IS the payload".
- **Left comparison (from x=30, y=72):** bold 12px mute `#6b7280` label "EVERY OTHER MECHANISM HERE"; two rounded boxes 130×40 r6 side by side: "metadata" (mute fill alpha 0.14, solid mute stroke 1.5, 12px text color) and "content (opaque)" (mute fill alpha 0.05, dashed mute stroke 5/4, 12px mute text).
- **Right comparison (from x=400):** bold 12px violet `#4a3aa7` label "THIS MECHANISM"; one undivided rounded box 270×40 r6, violet fill `rgba(74,58,167,0.16)`, violet stroke width 2, bold 13px violet centered text "the text itself is the record".
- **Thread row (y=178):** bold 12px mute label "ONE THREAD, RETAINED FOR CONTEXT" above; four rounded pills 92×40 r8 at x = 30/138/246/354, solid fills alternating blue `#2a78d6`, aqua `#199e70`, violet `#4a3aa7`, orange `#d95926`, white bold 13px labels "turn 1"–"turn 4", joined by short grid-gray `#e5e9ef` connectors.
- **Arrow to storage:** dashed mute line (5/4, width 2) with a solid mute arrowhead, into a rounded box (540, y−14) 150×68 r8: green fill `rgba(0,131,0,0.10)`, green `#008300` stroke width 2, bold 13px green centered "retained per" / "account setting", and 11px mute "a setting, not a fixed duration".
- **Footer band (bottom 44px):** violet wash alpha 0.06 with a 4px violet left edge; 13px text-color caption: "Every other mechanism records data about a message. This one records the message."

## Section 2: What does it collect?

- **Full text** of each turn, including follow-ups
- **Whatever the prompt describes**, including details about third parties
- **Pasted context** — documents, code, data, plus uploaded files and images where the product accepts them
- **Token counts** per turn and per conversation
- **Timestamps**, session length, turn cadence
- **Account and plan tier**, plus the retention and training flags in force
- **Feedback signals** — regenerations, thumbs, prompt edits

**Callout:** **Content is the record:** the `content` field is not metadata about a record — it is the record.

**Callout:** **Third party has no channel:** the diagnosis in that field belongs to someone else. The account holder consented for themselves; the person named has no account, no notice, and nothing to delete. An account-level flag therefore governs disclosures the account holder was never in a position to authorise.

### Visualization (canvas `c2`, 720×320)

Two-column capability table: what the account holder can do vs what the person named in the text can do, plus a line marking that one prompt spans both columns.

- **Header band (y 0–44):** green wash `rgba(0,131,0,0.06)` with a 2px green `#008300` rule; bold 16px green title: "The setting is account-level. The disclosures in the text are not."
- **Column headers (bold 13px, centered, y=62):** "ACCOUNT HOLDER" in green at x=300; "PERSON NAMED IN THE TEXT" in magenta `#d55181` at x=545.
- **Rows (four, row height 34 starting y=74, zebra `#f7f9fc` on even rows; row caption 13px text color at x=30):** "change retention", "opt out of training", "delete the thread", "know the record exists". Each row: a solid green pill 120×22 r11 with white bold 11px "can" (account holder), and a hollow magenta pill 150×22 r11 (magenta fill alpha 0.12, dashed magenta stroke 4/3) with magenta "cannot" (person named).
- **The crossing:** violet `#4a3aa7` line width 2.5 from x=300 to x=545 below the rows with 5px violet dots at both ends; bold 12px violet centered label above it: "a single prompt spans both columns".
- **Footer band (bottom 42px):** magenta wash alpha 0.07 with a 4px magenta left edge; 13px text-color caption: "Consent follows account ownership. The information in a sentence does not."

**Payload note (right column, below canvas):** Sample payload — illustrative structure, not real captured data.

```
// The request shape — roles, content, token counts — is
// documented across provider APIs. The retention and
// training flags are server-side state; generic names.
{
  // ── documented in public API ──
  "conversation_id":  "c-8f2a…",
  "model":            "…",
  "messages": [
    { "role": "user", "content":
      "my sister was diagnosed with … what should I ask her doctor" },
    { "role": "assistant", "content": "…" }
  ],
  "usage":            { "input_tokens": 214, "output_tokens": 903 },
  "created":          "2026-08-22T23:47:12Z",

  // ── inferred / plausible ──
  "account_id":       "u-41c…",
  "retention":        "until_deleted",
  "train_on_content": true         // account-level default
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Serving the request** — the next turn needs the earlier ones, so the thread is retained
- **Improving the model** — judging whether a change helps needs examples of what people actually ask

**Label (effect pill):** Additional consequence

- The retained text describes **people who are not parties to it**, under the account holder's settings
- The person described **cannot ask for deletion** — they do not know the record exists

**Callout:** **Typed, not true:** a conversation records what someone wrote, which may be a hypothetical, a test, a draft, or role-play. The pipeline was built to keep text, not to check it, so nothing separates those from a statement of fact — reading a prompt as a description of its author is a category error.

### Visualization (canvas `c3`, 720×320)

Sankey-style split diagram: the improvement loop is fed by a sample selected on the outcome — everyone who tries it splits into "stayed" (retained, flows into the corpus) and "left" (absent from the data).

- **Header band (y 0–44):** orange wash `rgba(217,89,38,0.06)` with a 2px orange `#d95926` rule; bold 16px orange title: "The evaluation corpus is missing the cases that went worst".
- **Source node:** rounded box (40, 86) 120×68 r8, blue fill `rgba(42,120,214,0.16)`, blue `#2a78d6` stroke width 2; bold 13px blue centered "everyone who" / "tries it".
- **Flows:** thick aqua `#199e70` quadratic band (line width 26) curving up to the "stayed" node at y=96; thin magenta `#d55181` band (line width 11) curving down to the "left" node at y=210.
- **Stayed node:** rounded box (300, 70) 150×52 r8, aqua fill alpha 0.16, solid aqua stroke 2; bold 13px aqua "stayed — prompts" / "retained".
- **Left node:** rounded box (300, 184) 150×52 r8, magenta fill alpha 0.10, dashed magenta stroke (5/4, width 2 — absent from the data); bold 13px magenta "left after a" / "bad result".
- **Corpus:** aqua connector (line width 20) from the stayed node to a rounded box (600, 66) 110×60 r8, green fill `rgba(0,131,0,0.14)`, green `#008300` stroke 2, bold 13px green "corpus used" / "to evaluate".
- **The missing edge:** sparse-dashed magenta line (dash 3/5, width 2) from the left node toward the corpus, labeled bold 12px magenta "no path into the corpus".
- **Footer band (bottom 56px):** orange wash alpha 0.07 with a 4px orange left edge; two 13px text-color lines: "Tuning on retained prompts conditions on staying — which is the outcome being measured." / "The model scores better on its own logs than on the population it is offered to."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede` paragraph, bullets with bolded lead terms (`li b` in `#1a5276`), `.lbl` purpose/effect pills, and `.key-point` callouts; right `<td>` (55%, `text-align: center`) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption plus `.payload` `<pre>` block (both left-aligned).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em. `.key-point` and `.payload`: background `#f8f9fa`, left border `3px solid #1a5276`; `.payload` ui-monospace 0.78em pre, `.payload-note` 0.82em italic `#666`. `.lbl` pills: uppercase 0.7em bold, `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart (720×320); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts share a rounded-rect path helper (`rr`).
- **Palette:** this page's charts use the tracking categorical palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Site-wide accents remain #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
