# 2. C++

**Page type:** other (single-column reference page: h1, subtitle, philosophy callout, then a two-column attribute/value table; no canvases)
**HTML title tag:** 2. C++

**Subtitle:** OOP with C's power — templates, RAII, and zero-cost abstractions

## Callout (philosophy box)

**Core trade-off:** Add abstraction to C without losing performance. You can write high-level code that compiles to the same machine instructions as hand-tuned C. Power and complexity grow together.

## Attribute table

| Attribute | Value |
|-----------|-------|
| What It Is | A multi-paradigm language (procedural, OOP, generic, functional) that extends C with classes, templates, RAII, and operator overloading. Compiles to native code with no mandatory runtime overhead for features you don't use. |
| Unique Contribution | OOP on C's foundation. Template metaprogramming for compile-time computation. RAII for deterministic resource management — destructors run at scope exit, guaranteeing cleanup. Zero-cost abstractions that let you build complex high-performance systems. |
| Strengths | Zero-cost abstractions (you don't pay for what you don't use), deterministic destruction (no GC pauses), template generics, massive ecosystem of libraries, direct hardware access when needed, 40+ years of real-world battle testing. |
| Weaknesses | Enormous complexity (the language spec is thousands of pages), long compile times, memory bugs still possible (though mitigated by smart pointers), steep learning curve, header file hell, no standard package manager. |
| Business Use Case | Game engines (Unreal Engine), browsers (Chrome, Firefox), databases (MySQL, MongoDB), high-frequency trading systems, embedded systems, AAA games, physics simulations, compilers. |
| Example | Code block (see below) |

**Example code block (inside `<pre><code>` in the Example row):**

```cpp
#include <iostream>
#include <memory>

class FileHandle {
    FILE* f;
public:
    FileHandle(const char* path) : f(fopen(path, "r")) {
        if (!f) throw std::runtime_error("open failed");
    }
    ~FileHandle() { fclose(f); }  // RAII: auto-cleanup

    // Delete copy, allow move
    FileHandle(const FileHandle&) = delete;
    FileHandle(FileHandle&& other) : f(other.f) { other.f = nullptr; }
};

void process() {
    FileHandle fh("data.txt");  // Opens file
    // ... use file ...
}  // Destructor closes file, even if exception thrown
```

## Regeneration instructions

- **Layout:** single page: `<h1>2. C++</h1>` (numbered title matching file index), `.subtitle` paragraph, `.philosophy` callout, then one full-width two-column `<table>` — first `<td>` per row is the attribute name, second is the value; the last row's value cell contains a `<pre><code>` block.
- **Table style:** `border-collapse: collapse`; cells `border: 1px solid #cfe0f0`, padding 14px 16px, vertical-align top; even rows background `#f7fbff`; first column width 32%, weight 600, color `#1a5276`.
- **Code block style:** `pre` background `#f4f6f8`, border `1px solid #dde4ea`, radius 4px, padding 12px 14px, 0.9em, `overflow-x: auto`; `code` font `'SF Mono', 'Fira Code', 'Consolas', monospace`. HTML entities used for `<`, `>`, `&` inside the code.
- **Page style:** universal reset (`* { margin:0; padding:0; box-sizing:border-box; }`); body `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`, white background, text `#1a1a1a`, 15px, padding 32px 28px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.05em; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.95em, color `#222`. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276` (also `#2980b9` accent), green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvases:** none on this page; if any are added, use `window.devicePixelRatio` scaling.
