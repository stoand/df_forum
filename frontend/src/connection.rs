use web_sys::Event;
use web_sys::MessageEvent as WebSocketMessageEvent;
use web_sys::{Document, Element, HtmlElement, HtmlInputElement, Storage, WebSocket};

use std::cell::RefCell;
use std::rc::Rc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::log;
use crate::query_result::QueryResult;
use crate::persisted::Persisted;

pub struct FrontendConnection {
    buffer: Rc<RefCell<Vec<QueryResult>>>,
    websocket: Rc<RefCell<WebSocket>>,
}

impl FrontendConnection {
    pub fn new(url: &str) -> Self {
        let websocket = Rc::new(RefCell::new(WebSocket::new(url).unwrap()));
        let websocket0 = websocket.clone();

        let onopen = Closure::<dyn FnMut(Event)>::new(move |_event: Event| {
            log(&format!("websocket opened"));
        });

        websocket0
            .borrow()
            .set_onopen(Some(onopen.as_ref().unchecked_ref()));
        onopen.forget();

        let buffer = Rc::new(RefCell::new(Vec::new()));
        let buffer0 = buffer.clone();

        let onmessage = Closure::<dyn FnMut(WebSocketMessageEvent)>::new(
            move |message: WebSocketMessageEvent| {
                let data = message.data().as_string().unwrap();
                log(&format!("got websocket message: {:?}", data));

                let mut parsed_data : Vec<QueryResult> = serde_json::from_str(&data).expect("could not parse QueryResults");

                buffer.borrow_mut().append(&mut parsed_data);
            },
        );
        websocket0
            .borrow()
            .set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        onmessage.forget();

        FrontendConnection {
            buffer: buffer0,
            websocket: websocket0,
        }
    }

    pub fn send_transaction(&self, persisted_items: Vec<Persisted>) {

            let fake_payload = vec![
                Persisted::Post {
                title: "Cool Stuff".into(),
                body: "Body".into(),
                user_id: 20,
                likes: 0,
            },
                Persisted::Post {
                title: "Cool Stuff".into(),
                body: "Body".into(),
                user_id: 20,
                likes: 0,
            },
            ];
            
        
            let fake_msg = serde_json::to_string(&persisted_items).unwrap();

            self.websocket.clone().borrow().send_with_str(&fake_msg).unwrap();
    }

    pub fn load_buffered(&mut self) -> Vec<QueryResult> {
        let ret = self.buffer.borrow().clone();
        self.buffer.borrow_mut().clear();

        ret
    }
}
