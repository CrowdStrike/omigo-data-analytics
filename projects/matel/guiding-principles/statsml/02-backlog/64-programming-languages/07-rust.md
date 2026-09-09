# 7. Rust

**Page type:** detail page (philosophy callout + two-column attribute table: label left 32%, content right; no canvases)
**HTML title tag:** 7. Rust

**Subtitle:** Memory safety without garbage collection — ownership eliminates bugs at compile time

## Callout (philosophy box)

**Core trade-off:** If it compiles, it is memory-safe. The ownership system with borrowing and lifetimes eliminates use-after-free, double-free, and data races at compile time — no garbage collector needed. The compiler is strict so the runtime does not have to be.

## What It Is

A systems programming language (2015) with an ownership model that guarantees memory safety and thread safety at compile time. No garbage collector, no runtime overhead. Compiles to native code with performance matching C/C++.

## Unique Contribution

Ownership model for compile-time memory safety without GC. Each value has exactly one owner; when the owner goes out of scope, the value is dropped. Borrowing rules prevent data races. Zero-cost abstractions. Fearless concurrency — the compiler prevents race conditions.

## Strengths

Memory safety without GC (no pauses, no overhead), no undefined behavior, excellent compiler error messages, modern type system (enums, pattern matching, traits), cargo ecosystem (build + package manager), fearless concurrency.

## Weaknesses

Steep learning curve (the borrow checker rejects valid programs), longer compile times than Go/C, smaller ecosystem than C++, fighting the borrow checker for complex data structures (graphs, self-referential types), async ecosystem still maturing.

## Business Use Case

Systems programming (AWS Firecracker, Cloudflare Workers), WebAssembly targets, CLI tools (ripgrep, bat, fd), security-critical software, blockchain/crypto infrastructure, embedded systems, replacing C/C++ in safety-critical contexts.

## Example

Code block (`<pre><code>`):

```rust
fn main() {
    let s1 = String::from("hello");  // s1 owns the string

    let s2 = s1;  // Ownership MOVES to s2. s1 is now invalid.
    // println!("{}", s1);  // Compile error! Use-after-move.

    let s3 = s2.clone();  // Explicit deep copy

    // Borrowing: read without taking ownership
    print_len(&s3);  // Immutable borrow — s3 still valid

    println!("{}", s3);  // Works: s3 was only borrowed, not moved
}

fn print_len(s: &String) {  // Borrows, doesn't own
    println!("Length: {}", s.len());
}  // Borrow ends here. No free needed — never owned it.
```

## Regeneration instructions

- **Layout:** single page: h1, `.subtitle` paragraph, one `.philosophy` callout, then a full-width two-column table with one `<tr>` per section above (label in first `<td>`, content in second). The Example row's content cell holds a `<pre><code>` block with the code verbatim (HTML-escape `&`).
- **Table style:** `border-collapse: collapse`; cells `border: 1px solid #cfe0f0`, padding 14px 16px, `vertical-align: top`; even rows background `#f7fbff`; first column width 32%, weight 600, color `#1a5276`.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, margin 20px 0 28px 0, 0.95em, color `#222`.
- **Code block style:** `pre` — background `#f4f6f8`, border `1px solid #dde4ea`, radius 4px, padding 12px 14px, 0.9em, `overflow-x: auto`; `code` in 'SF Mono'/'Fira Code'/'Consolas' monospace.
- **Page style:** body -apple-system/BlinkMacSystemFont/'Segoe UI' sans-serif, padding 32px 28px, white background, text `#1a1a1a`, font-size 15px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvases:** none on this page; if any are added, use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
