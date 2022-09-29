// #SPC-forum_minimal
extern crate abomonation;
extern crate abomonation_derive;
extern crate console_error_panic_hook;
// extern crate differential_dataflow;
extern crate serde;
#[macro_use]
extern crate serde_derive;
// extern crate timely;
extern crate wasm_bindgen_test;
extern crate getrandom;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

pub mod connection;
pub mod persisted;
pub mod query_result;
pub mod df_tuple_items;

use persisted::Persisted;
use query_result::QueryResult;
use std::cell::RefCell;
use std::rc::Rc;

use wasm_bindgen::prelude::*;

use wasm_bindgen::JsCast;
use web_sys::{Document, Element, HtmlElement, HtmlInputElement, Storage};

pub const USERNAME_LOCAL_STORAGE_KEY: &'static str = "df_forum_username";
pub const WEBSOCKET_URL: &'static str = "ws://127.0.0.1:5050";

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    pub fn log(contents: &str);
}

pub fn get_local_storage() -> Storage {
    web_sys::window().unwrap().local_storage().unwrap().unwrap()
}

pub fn get_random_u64() -> u64 {
    let mut dst = [0u8; 8];
    getrandom::getrandom(&mut dst).unwrap();
    u64::from_be_bytes(dst)
}

#[wasm_bindgen]
pub fn bootstrap() {
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));

    let connection = Rc::new(RefCell::new(connection::FrontendConnection::new(
        &WEBSOCKET_URL,
    )));

    let local_storage = get_local_storage();
    if let Ok(Some(user_name)) = local_storage.get_item(USERNAME_LOCAL_STORAGE_KEY) {
        render_page_posts(user_name, connection);
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

            bootstrap();
        }
    });

    let use_chat_name_el = use_chat_name.dyn_ref::<HtmlElement>().unwrap();
    use_chat_name_el.set_onclick(Some(use_chat_name_click.as_ref().unchecked_ref()));

    use_chat_name_click.forget();
}

// #SPC-forum_minimal.page_posts
pub fn render_page_posts(
    username: String,
    connection: Rc<RefCell<connection::FrontendConnection>>,
) {
    let (document, root) = document_and_root();
    root.set_inner_html("");

    let connection0 = connection.clone();
    // let connection1 = connection.clone();

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

    let post_title = document.create_element("input").unwrap();
    root.append_child(&post_title).unwrap();
    post_title
        .dyn_ref::<HtmlInputElement>()
        .unwrap()
        .set_placeholder("Post Title");

    let post_body = document.create_element("input").unwrap();
    root.append_child(&post_body).unwrap();
    post_body
        .dyn_ref::<HtmlInputElement>()
        .unwrap()
        .set_placeholder("Post Body");

    let submit_post = document.create_element("button").unwrap();
    submit_post.set_text_content(Some("Create Post"));
    root.append_child(&submit_post).unwrap();

    let submit_post_click = Closure::<dyn FnMut()>::new(move || {
        let title = post_title.dyn_ref::<HtmlInputElement>().unwrap().value();

        let body = post_body.dyn_ref::<HtmlInputElement>().unwrap().value();
        if !title.is_empty() && !body.is_empty() {
            connection0.borrow().send_transaction(vec![Persisted::Post {
                // id: get_random_u64(),
                title,
                body,
                user_id: 0,
                likes: 0,
            }]);
        }
    });

    let submit_post_el = submit_post.dyn_ref::<HtmlElement>().unwrap();
    submit_post_el.set_onclick(Some(submit_post_click.as_ref().unchecked_ref()));

    submit_post_click.forget();

    // on (user_id, Aggregations)

    let aggregates = document.create_element("div").unwrap();
    aggregates.set_inner_html(
        "My Likes: ? -- My Posts: ? -- Posts Total: <span id='posts-total'>???</span>",
    );
    root.append_child(&aggregates).unwrap();

    // on (page_num, Post & post_user_author)
    //
    let posts_container = document.create_element("div").unwrap();
    posts_container.set_id("posts-container");
    root.append_child(&posts_container).unwrap();

    let post_template = document.create_element("div").unwrap();
    post_template.set_id("post-template");
    posts_container.append_child(&post_template).unwrap();
    let username_label = document.create_element("h3").unwrap();
    username_label.set_text_content(Some("Post Title"));
    username_label.set_id("post-title");
    post_template.append_child(&username_label).unwrap();

    let username_label = document.create_element("h6").unwrap();
    username_label.set_text_content(Some("Post Author"));
    username_label.set_id("post-author");
    post_template.append_child(&username_label).unwrap();

    let username_label = document.create_element("p").unwrap();
    username_label.set_text_content(Some("Post Body"));
    username_label.set_id("post-body");
    post_template.append_child(&username_label).unwrap();

    let username_label = document.create_element("button").unwrap();
    username_label.set_inner_html("Like <span id='post-likes'></span>");
    post_template.append_child(&username_label).unwrap();

    let username_label = document.create_element("button").unwrap();
    username_label.set_text_content(Some("Delete"));
    username_label.set_id("post-remove");
    post_template.append_child(&username_label).unwrap();

    let username_label = document.create_element("button").unwrap();
    username_label.set_text_content(Some("Collapse"));
    post_template.append_child(&username_label).unwrap();

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
    let on_parsed_message = move |items: Vec<QueryResult>| {
        let window = web_sys::window().unwrap();
        let document = window.document().unwrap();
        for item in items {
            match item {
                QueryResult::PostCount(count) => {
                    document
                        .query_selector("#posts-total")
                        .unwrap()
                        .unwrap()
                        .set_text_content(Some(&count.to_string()));
                }
                QueryResult::Post {
                    id,
                    title,
                    body,
                    user_id,
                    likes,
                } => {
                    let posts_container = document
                        .query_selector("#posts-container")
                        .unwrap()
                        .unwrap();
                    let post_template = document.query_selector("#post-template").unwrap().unwrap();
                    let new_post = document.create_element("div").unwrap();
                    new_post.set_inner_html(&post_template.inner_html());
                    new_post.set_id(&id.to_string());
                    posts_container.append_child(&new_post).unwrap();

                    new_post
                        .query_selector("#post-title")
                        .unwrap()
                        .unwrap()
                        .set_text_content(Some(&title));
                    new_post
                        .query_selector("#post-body")
                        .unwrap()
                        .unwrap()
                        .set_text_content(Some(&body));
                    new_post
                        .query_selector("#post-author")
                        .unwrap()
                        .unwrap()
                        .set_text_content(Some(&user_id.to_string()));
                    new_post
                        .query_selector("#post-likes")
                        .unwrap()
                        .unwrap()
                        .set_text_content(Some(&likes.to_string()));

                    let post_remove = new_post.query_selector("#post-remove").unwrap().unwrap();

                    // let connection3 = connection2.clone();

                    let post_remove_click = Closure::<dyn FnMut()>::new(move || {
                        log("todo post remove");
                        // connection3.borrow().send_transaction(vec![Persisted::PostDeleted { id }]);
                    });

                    let post_remove_el = post_remove.dyn_ref::<HtmlElement>().unwrap();
                    post_remove_el.set_onclick(Some(post_remove_click.as_ref().unchecked_ref()));

                    post_remove_click.forget();
                },
                QueryResult::PostDeleted { id } => {
                    document.get_element_by_id(&id.to_string()).unwrap().remove();
                },
            }
        }
    };

    connection
        .borrow_mut()
        .init_on_parsed_message(Box::new(on_parsed_message));
}
