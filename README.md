# Forum Full Stack App

A forum/chat app proof of concept.

State management case study using `timely/differential dataflow`.

Differentiates between frontend and backend states and handles both.

Ensure Rust and `cargo-watch` are installed, then:

```
cargo watch -x 'run -- 10' 
```

## To run in browser

* `cargo install wasm-pack`
* `wasm-pack build --target web`
* `python3 -m http.server`
* Open [http://localhost:8000](http://localhost:8000)
