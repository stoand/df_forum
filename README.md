# "DataFlow" Forum Full Stack App

A forum/chat app proof of concept.

State management case study using [timely](https://github.com/TimelyDataflow/timely-dataflow)
    \/ [differential dataflow](https://github.com/TimelyDataflow/differential-dataflow).

Differentiates between frontend and backend states and handles both.

Ensure Rust and `cargo-watch` (`cargo install cargo-watch`) are installed, then:

```
cargo watch -x 'test -- --nocapture' 
```

# Frontend

## To run in browser

* `cd frontend/`
* `curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh`
* `nodemon -e rs -x 'wasm-pack build --target web'`
* `python3 -m http.server`
* Open [http://localhost:8000](http://localhost:8000)

## To run headless tests

* `cd frontend/`
* `nodemon -e rs -x 'wasm-pack test --chrome --headless'`

# Backend

## Run Server

`RUST_LOG=debug cargo watch -x run`

## Run Tests

`RUST_LOG=debug cargo watch -x 'test -- --nocapture'`

The `--nocapture` is important: tests that fail inside `tokio::spawn` are just printed text

[Cranelift](https://github.com/bjorn3/rustc_codegen_cranelift#building-and-testing) is recommended for faster compilation
(though there are some bugs only on Cranelift)
