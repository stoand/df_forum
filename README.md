# Forum Full Stack App

A forum/chat app proof of concept.

State management case study using [timely](https://github.com/TimelyDataflow/timely-dataflow)
    \/ [differential dataflow](https://github.com/TimelyDataflow/differential-dataflow).

Differentiates between frontend and backend states and handles both.

Ensure Rust and `cargo-watch` are installed, then:

```
cargo watch -x 'test -- --nocapture' 
```

# Frontend

## To run in browser

* `cd frontend/`
* `cargo install wasm-pack`
* `nodemon -e rs -x 'wasm-pack build --target web'`
* `python3 -m http.server`
* Open [http://localhost:8000](http://localhost:8000)

## To run headless tests

* `cd frontend/`
* `nodemon -e rs -x 'wasm-pack test --chrome --headless'`
