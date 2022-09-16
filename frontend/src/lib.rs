// #SPC-forum_minimal
extern crate abomonation;
extern crate abomonation_derive;
extern crate console_error_panic_hook;
extern crate differential_dataflow;
extern crate serde;
extern crate serde_derive;
extern crate timely;
extern crate wasm_bindgen_test;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

use std::cell::RefCell;
use std::rc::Rc;
use timely::communication::allocator::thread::Thread;
use timely::worker::Worker;
use timely::WorkerConfig;
use wasm_bindgen::prelude::*;

use differential_dataflow::operators::Count;
use differential_dataflow::operators::Reduce;

use wasm_bindgen::JsCast;
use web_sys::{Document, Element, HtmlElement, HtmlInputElement, Storage};

use differential_dataflow::input::InputSession;

pub const USERNAME_LOCAL_STORAGE_KEY: &'static str = "df_forum_username";

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    pub fn log(contents: &str);
}

pub fn get_local_storage() -> Storage {
    web_sys::window().unwrap().local_storage().unwrap().unwrap()
}

#[wasm_bindgen]
pub fn bootstrap() {
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));

    let local_storage = get_local_storage();
    if let Ok(Some(user_name)) = local_storage.get_item(USERNAME_LOCAL_STORAGE_KEY) {
        render_page_posts(user_name);
    } else {
        render_page_enter_username();
    }
}

pub fn document_and_root() -> (Document, Element) {
    let window = web_sys::window().unwrap();
    let document = window.document().unwrap();
    let root = document.query_selector("#df_forum_root").unwrap().unwrap();

    (document, root)
}

// #SPC-forum_minimal.page_enter_username
pub fn render_page_enter_username() {
    let (document, root) = document_and_root();
    root.set_inner_html("");

    let enter_chat_name = document.create_element("input").unwrap();
    root.append_child(&enter_chat_name).unwrap();

    let use_chat_name = document.create_element("button").unwrap();
    use_chat_name.set_text_content(Some("Chat with this name"));
    root.append_child(&use_chat_name).unwrap();

    let use_chat_name_click = Closure::<dyn FnMut()>::new(move || {
        let name = enter_chat_name
            .dyn_ref::<HtmlInputElement>()
            .unwrap()
            .value();

        if !name.is_empty() {
            get_local_storage()
                .set_item(USERNAME_LOCAL_STORAGE_KEY, &name)
                .unwrap();

            render_page_posts(name);
        }
    });

    let use_chat_name_el = use_chat_name.dyn_ref::<HtmlElement>().unwrap();
    use_chat_name_el.set_onclick(Some(use_chat_name_click.as_ref().unchecked_ref()));

    use_chat_name_click.forget();
}

// #SPC-forum_minimal.page_posts
pub fn render_page_posts(username: String) {
    let (document, root) = document_and_root();
    root.set_inner_html("");

    let username_label = document.create_element("div").unwrap();
    username_label.set_text_content(Some(&("Username: ".to_owned() + &username)));
    root.append_child(&username_label).unwrap();

    let use_different_name = document.create_element("button").unwrap();
    use_different_name.set_text_content(Some("Use different name"));
    root.append_child(&use_different_name).unwrap();

    let use_different_name_click = Closure::<dyn FnMut()>::new(move || {
        get_local_storage()
            .remove_item(USERNAME_LOCAL_STORAGE_KEY)
            .unwrap();

        render_page_enter_username();
    });

    let use_different_name_el = use_different_name.dyn_ref::<HtmlElement>().unwrap();
    use_different_name_el.set_onclick(Some(use_different_name_click.as_ref().unchecked_ref()));

    use_different_name_click.forget();

    let username_label = document.create_element("h2").unwrap();
    username_label.set_text_content(Some("Posts"));
    root.append_child(&username_label).unwrap();

    // on (user_id, Aggregations)

    let username_label = document.create_element("div").unwrap();
    username_label.set_text_content(Some("My Likes: ? -- My Posts: ? -- Posts Total: ?"));
    root.append_child(&username_label).unwrap();

    // on (page_num, Post & post_user_author)

    let username_label = document.create_element("h3").unwrap();
    username_label.set_text_content(Some("Post Title"));
    root.append_child(&username_label).unwrap();

    let username_label = document.create_element("h6").unwrap();
    username_label.set_text_content(Some("Post Author"));
    root.append_child(&username_label).unwrap();

    let username_label = document.create_element("p").unwrap();
    username_label.set_text_content(Some("Post Body"));
    root.append_child(&username_label).unwrap();

    let username_label = document.create_element("button").unwrap();
    username_label.set_text_content(Some("Like (like count?)"));
    root.append_child(&username_label).unwrap();

    let username_label = document.create_element("button").unwrap();
    username_label.set_text_content(Some("Delete"));
    root.append_child(&username_label).unwrap();

    let username_label = document.create_element("button").unwrap();
    username_label.set_text_content(Some("Collapse"));
    root.append_child(&username_label).unwrap();

    let page_ops = document.create_element("div").unwrap();
    root.append_child(&page_ops).unwrap();

    let username_label = document.create_element("br").unwrap();
    page_ops.append_child(&username_label).unwrap();
    
    // on (page_num) - activate or deactive

    let username_label = document.create_element("button").unwrap();
    username_label.set_text_content(Some("Prev"));
    page_ops.append_child(&username_label).unwrap();

    // on (page_num)

    let username_label = document.create_element("span").unwrap();
    username_label.set_text_content(Some("Page ?"));
    page_ops.append_child(&username_label).unwrap();
    
    // on (page_num) - activate or deactive

    let username_label = document.create_element("button").unwrap();
    username_label.set_text_content(Some("Next"));
    page_ops.append_child(&username_label).unwrap();
}
