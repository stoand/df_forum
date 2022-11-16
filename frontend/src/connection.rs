use web_sys::Event;
use web_sys::MessageEvent as WebSocketMessageEvent;
use web_sys::WebSocket;

use std::cell::RefCell;
use std::rc::Rc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::log;
use crate::persisted::PersistedItems;
use crate::query_result::QueryResult;

pub struct FrontendConnection {
    pub websocket: Rc<RefCell<WebSocket>>,
    pub onmessage: Option<fn(Vec<QueryResult>) -> ()>,
}

impl FrontendConnection {
    pub fn new(url: &str) -> Self {
        let websocket = Rc::new(RefCell::new(WebSocket::new(url).unwrap()));
        let websocket0 = websocket.clone();

        let onclose = Closure::<dyn FnMut(Event)>::new(move |_event: Event| {
            log(&format!("websocket closed"));
        });

        websocket0
            .borrow()
            .set_onclose(Some(onclose.as_ref().unchecked_ref()));
        onclose.forget();

        FrontendConnection {
            websocket: websocket0.clone(),
            onmessage: None,
        }
    }

    pub fn set_onopen(&self, onopen: Closure<dyn FnMut(Event)>) {
        self.websocket
            .borrow()
            .set_onopen(Some(onopen.as_ref().unchecked_ref()));
        onopen.forget();       
    }

    pub fn init_on_parsed_message(&self, on_parsed_message: Box<dyn Fn(Vec<QueryResult>)>) {
        let onmessage = Closure::<dyn FnMut(WebSocketMessageEvent)>::new(
            move |message: WebSocketMessageEvent| {
                let data = message.data().as_string().unwrap();
                log(&format!("got websocket message: {:?}", data));

                let parsed_data: Vec<QueryResult> =
                    serde_json::from_str(&data).expect("could not parse QueryResults");

                on_parsed_message(parsed_data);
            },
        );
        self.websocket
            .borrow()
            .set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        onmessage.forget();
    }

    pub fn send_transaction(&self, persisted_items: PersistedItems) {
        let msg = serde_json::to_string(&persisted_items).unwrap();

        self.websocket.clone().borrow().send_with_str(&msg).unwrap();
    }
}
