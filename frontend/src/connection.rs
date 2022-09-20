use web_sys::Event;
use web_sys::MessageEvent as WebSocketMessageEvent;
use web_sys::{Document, Element, HtmlElement, HtmlInputElement, Storage, WebSocket};

use std::cell::RefCell;
use std::rc::Rc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::log;
use crate::query_result::QueryResult;

pub struct FrontendConnection {
    buffer: Vec<QueryResult>,
    websocket: Rc<RefCell<WebSocket>>,
}

impl FrontendConnection {
    pub fn new(url: &str) -> Self {
        let websocket = Rc::new(RefCell::new(WebSocket::new(url).unwrap()));
        let websocket0 = websocket.clone();

        let onopen = Closure::<dyn FnMut(Event)>::new(move |_event: Event| {
            log(&format!("websocket opened"));
            websocket.clone().borrow().send_with_str("asdf").unwrap();
        });

        websocket0
            .borrow()
            .set_onopen(Some(onopen.as_ref().unchecked_ref()));
        onopen.forget();

        let onmessage = Closure::<dyn FnMut(WebSocketMessageEvent)>::new(
            move |message: WebSocketMessageEvent| {
                log(&format!(
                    "got websocket message: {:?}",
                    message.data().as_string().unwrap()
                ));
            },
        );
        websocket0
            .borrow()
            .set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        onmessage.forget();

        FrontendConnection {
            buffer: Vec::new(),
            websocket: websocket0,
        }
    }

    pub fn latest_results(&mut self) -> Vec<QueryResult> {
        let ret = self.buffer.clone();
        self.buffer.clear();

        vec![]
    }
}
