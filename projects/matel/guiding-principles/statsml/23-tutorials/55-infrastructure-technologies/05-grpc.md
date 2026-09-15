# gRPC

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** gRPC

**Subtitle:** gRPC lets one service call another like a typed local function — the contract lives in a .proto file, code generation writes the plumbing, and Protocol Buffers put the data on the wire in binary

## The Orders Service Calls Inventory Like a Function

**Tags:** `core idea` (blue), `contract first` (green), `codegen` (orange)

- **The two services** — an orders service must ask an inventory service if SKU-4471 has 3 units in stock
- **The old way** — hand-rolled JSON+HTTP: build the URL, encode, parse, hope both sides spell fields the same
- **The contract** — a .proto file declares `service Inventory { rpc CheckStock(...) }` with typed messages
- **The codegen** — `protoc` generates a client stub for orders and a server skeleton for inventory, in any language
- **The heritage** — Google open-sourced the model in 2015, based on Stubby, its internal RPC system

*Example (italic):* The orders team writes `client.CheckStock(sku, qty)` in Go; a typo in a field name fails at compile time, not at 2am in production.

**Key point:** gRPC is contract-first — the .proto file is the single source of truth, and generated stubs turn a network call into a typed function call on both sides.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one .proto contract at the top feeding codegen into both services, then a typed call flowing between them over HTTP/2.

- **Title (bold 15px, `#1a5276`, top center):** "One .proto Contract, Two Generated Stubs, Zero Hand-Written JSON".
- **Contract box (top center):** rounded box at x=215, y=45, 290×40, fill `rgba(74,58,167,0.10)`, 2px violet `#4a3aa7` border, 12px `#2c3e50` text "inventory.proto — service Inventory { rpc CheckStock }".
- **Codegen arrows:** two dashed `#6b7280` (dash 4/3) arrows from the contract box's bottom corners down to the two service boxes, each with an 11px `#6b7280` label "protoc codegen" beside it.
- **Service boxes (y=145, 52px tall, 8px radius):** left blue `#2a78d6` box at x=55, 240 wide, fill `rgba(42,120,214,0.15)`, two 12px lines "orders service" (bold) / "generated client stub (Go)"; right green `#008300` box at x=425, 240 wide, fill `rgba(0,131,0,0.12)`, two 12px lines "inventory service" (bold) / "generated server stub (Java)".
- **The call:** 3px blue `#2a78d6` arrow from the left box's right edge to the right box's left edge at y=171, bold 12px blue label `CheckStock("SKU-4471", 3)` above it, 11px `#6b7280` label "binary Protocol Buffers over HTTP/2" below it.
- **Annotation (bold 13px green `#008300`, centered near y=245):** "the compiler checks the call — no URL strings, no field-name typos".
- **Caption (12px `#444`, bottom right):** "open-sourced by Google in 2015 from its internal Stubby system".

## One Stock Check: 84 JSON Bytes vs 24 Protobuf Bytes

**Tags:** `worked example` (blue), `binary encoding` (green)

- **The request** — CheckStockRequest carries sku "SKU-4471" and quantity 3
- **As JSON** — `{"sku":"SKU-4471","quantity":3}` is 31 bytes, field names spelled out on every call
- **As protobuf** — numeric tags replace names: tag+len+8 chars (10 B) plus tag+varint (2 B) = 12 bytes
- **The response** — in_stock true, available 17, warehouse "EAST-2": 53 bytes as JSON, 12 as protobuf
- **The parse** — decoding reads tags and lengths, no string tokenizing, so it is faster as well as smaller

*Example (italic):* One stock check shrinks from 31+53 = 84 JSON bytes to 12+12 = 24 protobuf bytes — the same fields, 3.5× less on the wire (exact for these payloads).

**Key point:** Protocol Buffers replace repeated field-name strings with one-byte numeric tags, so payloads are smaller and faster to encode/decode than JSON — the byte counts here are exact encodings.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: bytes on the wire for the same request and response, encoded as JSON vs Protocol Buffers.

- **Title (bold 15px, `#1a5276`, top center):** "Same Fields on the Wire: JSON vs Protocol Buffers".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = bytes 0 to 60 (3 px per byte), gridlines `#e5e9ef` at 15/30/45 with 12px `#444` tick labels.
- **Groups:** "CheckStockRequest" centered at x=230, "CheckStockResponse" centered at x=470, 13px `#444` labels below the baseline.
- **Bars (70px wide, 16px gap within a group):** JSON bars blue fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, heights for `[31, 53]` bytes; protobuf bars solid green `#008300`, heights for `[12, 12]` bytes; bold 12px value labels "31 B" / "12 B" / "53 B" / "12 B" above each bar.
- **Legend (12px, top right of plot):** blue swatch "JSON", green swatch "protobuf".
- **Annotation (bold 13px green `#008300`, near x=280, y=55):** "round trip: 84 B → 24 B, 3.5× smaller (exact)".
- **Caption (12px `#444`, bottom right):** "byte counts are exact encodings of these two messages".

## Four Call Shapes and a Deadline That Travels

**Tags:** `where it's used` (blue), `HTTP/2` (green), `streaming` (orange)

- **One connection** — HTTP/2 multiplexes many concurrent calls over a single TCP connection
- **Unary** — one request, one response: the plain CheckStock call
- **Streams** — server-streaming (price updates out), client-streaming (batch upload in), bidirectional (both)
- **Deadlines** — orders sets a 200 ms deadline; inventory sees the time remaining and can give up early
- **Cancellation** — if the shopper abandons checkout, the cancel propagates down the whole call chain
- **The split** — gRPC is the standard between services; REST/JSON stays at the public edge

*Example (italic):* Orders calls inventory with 200 ms left; inventory's own call to the warehouse service inherits whatever remains of those 200 ms (deadline illustrative).

**Key point:** HTTP/2 gives gRPC multiplexed streams — hence four call shapes — plus metadata that carries deadlines and cancellation through every hop, which plain REST has no standard way to do.

### Visualization (canvas `c3`, 720×300)

Four-row arrow diagram: the four gRPC call shapes as messages flowing between an orders client and an inventory server.

- **Title (bold 15px, `#1a5276`, top center):** "Four Call Shapes on One HTTP/2 Connection".
- **Column headers (bold 13px `#1a5276`, y=52):** "orders (client)" centered at x=250, "inventory (server)" centered at x=560; faint 1px `#e5e9ef` vertical guide lines at x=220 and x=620 from y=60 to y=250.
- **Rows (left-aligned 12px `#444` labels at x=20, rows at y = 85, 135, 185, 235):** arrows run between x=220 and x=620, 2px, arrowheads 6px; requests point right in blue `#2a78d6`, responses point left in green `#008300`.
  - "unary": one blue arrow at row y−7, one green arrow at row y+7.
  - "server streaming": one blue arrow at row y−10, three short green arrows (each ~120px, starting x=620/470/320) staggered at row y+2/y+9/y+16.
  - "client streaming": three short blue arrows (each ~120px, starting x=220/370/520) at row y−16/y−9/y−2, one green arrow at row y+10.
  - "bidirectional": alternating short blue and green arrows (~90px each) interleaved at row y−12, y−4, y+4, y+12.
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=272):** "every call carries its deadline — 200 ms here — down the chain".
- **Caption (12px `#444`, bottom right):** "message counts and deadline illustrative".

## The Browser Can't Speak Native gRPC

**Tags:** `common mistake` (red), `gRPC-Web` (orange)

- **The plan** — a team exposes its new gRPC service directly to the web frontend
- **The wall** — browser JavaScript gets no access to HTTP/2 trailers, where gRPC returns its status
- **The symptom** — fetch() to the gRPC port hangs or errors; the client never sees a status code
- **The fix** — gRPC-Web: a browser-friendly variant plus a proxy (commonly Envoy) translating to real gRPC
- **The trade** — gRPC-Web drops client- and bidirectional streaming; unary and server-streaming survive
- **The pattern** — keep gRPC service-to-service; put REST/JSON or gRPC-Web at the public edge

*Example (italic):* The checkout page's call to the gRPC port stalls with no error code — the trailer frame carrying the gRPC status never reaches JavaScript.

**Common mistake:** Treating gRPC as a drop-in for every client. Browsers cannot speak native gRPC; without gRPC-Web and a translating proxy, the public edge still belongs to REST/JSON.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a browser calling the gRPC service directly (fails) vs through a gRPC-Web proxy (works).

- **Title (bold 15px, `#1a5276`, top center):** "From the Browser: Direct gRPC Fails, gRPC-Web Translates".
- **Row 1 (y=95), label 12px `#444` at x=20:** "direct from browser"; blue `#2a78d6` rounded box at x=160 labeled "browser fetch()" (12px), 3px arrow to a red `#e74c3c` box at x=420, 230 wide, labeled "native gRPC — status lives in HTTP/2 trailers" with bold 12px red "✗ trailers never reach JS" below it.
- **Row 2 (y=205), label:** "with gRPC-Web"; blue box at x=130, 170 wide, "browser + gRPC-Web client", 3px arrow to a green `#008300` box at x=345, 160 wide, "Envoy proxy translates", then arrow to a green box at x=545, 140 wide, "gRPC server" with bold 12px green "✓".
- **Box style:** 140–230px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "gRPC rules inside the datacenter; the public edge speaks REST/JSON or gRPC-Web".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the JSON byte counts (31, 53) and protobuf byte counts (12, 12) are exact encodings of the two literal messages shown and must match between text and chart; the 200 ms deadline, stream message counts, and stock quantities are invented and labeled illustrative; the 2015 open-sourcing from Stubby is a documented fact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
