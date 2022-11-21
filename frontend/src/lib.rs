// #SPC-forum_minimal
extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
extern crate console_error_panic_hook;
// extern crate differential_dataflow;
extern crate serde;
#[macro_use]
extern crate serde_derive;
// extern crate timely;
extern crate getrandom;
extern crate wasm_bindgen_test;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

pub mod connection;
pub mod df_tuple_items;
pub mod persisted;
pub mod query_result;

use persisted::Persisted;
use query_result::QueryResult;
use std::cell::RefCell;
use std::rc::Rc;

use wasm_bindgen::prelude::*;

use wasm_bindgen::JsCast;
use web_sys::{Document, Element, Event, HtmlElement, HtmlInputElement, Storage};

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
    let session_id = get_random_u64();

    let connection0 = connection.clone();

    let onopen = Closure::<dyn FnMut(Event)>::new(move |_event: Event| {
        log(&format!("websocket opened"));

        let local_storage = get_local_storage();
        if let Ok(Some(user_name)) = local_storage.get_item(USERNAME_LOCAL_STORAGE_KEY) {
            connection0.clone().borrow().send_transaction(vec![
                (session_id, Persisted::Session(user_name.clone()), 1),
                (session_id, Persisted::ViewPostsPage(0), 1),
            ]);

            render_page_posts(user_name, session_id, connection0.clone());
        } else {
            render_page_enter_username();
        }
    });

    connection.borrow().set_onopen(onopen);
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
    session_id: u64,
    connection: Rc<RefCell<connection::FrontendConnection>>,
) {
    let (document, root) = document_and_root();
    root.set_inner_html("");

    let connection0 = connection.clone();
    // let connection1 = connection.clone();
    let connection2 = connection.clone();
    let connection3 = connection.clone();
    let connection4 = connection.clone();

    let view_posts_page_id = get_random_u64();

    root.set_attribute("page", &(0.to_string())).unwrap();

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
            let id = get_random_u64();
            connection0.borrow().send_transaction(vec![
                (id, Persisted::Post, 1),
                (id, Persisted::PostTitle(title), 1),
                (id, Persisted::PostBody(body), 1),
            ]);
        }
    });

    let submit_post_el = submit_post.dyn_ref::<HtmlElement>().unwrap();
    submit_post_el.set_onclick(Some(submit_post_click.as_ref().unchecked_ref()));

    submit_post_click.forget();

    let aggregates = document.create_element("div").unwrap();
    aggregates.set_inner_html(
        "My Likes: ? -- My Posts: ? -- Posts Total: <span id='posts-total'>???</span>",
    );
    root.append_child(&aggregates).unwrap();

    // on (page_num, Post & post_user_creator)
    //
    let posts_container = document.create_element("div").unwrap();
    posts_container.set_id("posts-container");
    root.append_child(&posts_container).unwrap();

    let post_template = document.create_element("div").unwrap();
    post_template.set_id("post-template");
    posts_container.append_child(&post_template).unwrap();
    let username_label = document.create_element("h3").unwrap();
    username_label.set_text_content(Some("Post Title"));
    username_label.set_class_name("post-title");
    post_template.append_child(&username_label).unwrap();

    let username_label = document.create_element("h6").unwrap();
    username_label.set_text_content(Some("Post Author"));
    username_label.set_class_name("post-creator");
    post_template.append_child(&username_label).unwrap();

    let username_label = document.create_element("p").unwrap();
    username_label.set_text_content(Some("Post Body"));
    username_label.set_class_name("post-body");
    post_template.append_child(&username_label).unwrap();

    let username_label = document.create_element("button").unwrap();
    username_label.set_inner_html(
        "<span class='post-like-status'>Like</span><span class='post-likes'>0</span>",
    );
    username_label.set_class_name("post-like");
    post_template.append_child(&username_label).unwrap();

    let delete_button = document.create_element("button").unwrap();
    delete_button.set_text_content(Some("Delete"));
    delete_button.set_class_name("post-remove");
    post_template.append_child(&delete_button).unwrap();

    let username_label = document.create_element("button").unwrap();
    username_label.set_text_content(Some("Collapse"));
    post_template.append_child(&username_label).unwrap();

    let page_ops = document.create_element("div").unwrap();
    root.append_child(&page_ops).unwrap();

    let username_label = document.create_element("br").unwrap();
    page_ops.append_child(&username_label).unwrap();

    let page_label = document.create_element("span").unwrap();
    page_label.set_id("page_label");

    let update_page_label = || {
        let (document, root) = document_and_root();
        let page: u64 = root.get_attribute("page").unwrap().parse().unwrap();
        let page_count: u64 = root
            .get_attribute("page_count")
            .unwrap_or("1".to_string())
            .parse()
            .unwrap();
        let page_label = document.get_element_by_id("page_label").unwrap();
        page_label.set_text_content(Some(&format!("Page {} of {}", page + 1, page_count)));
    };

    let prev_page = document.create_element("button").unwrap();
    prev_page.set_text_content(Some("Prev"));
    page_ops.append_child(&prev_page).unwrap();

    let prev_page_click = Closure::<dyn FnMut()>::new(move || {
        let (_, root) = document_and_root();
        let old_page: u64 = root.get_attribute("page").unwrap().parse().unwrap();
        if old_page > 0 {
            let page = old_page - 1;
            root.set_attribute("page", &(page.to_string())).unwrap();
            update_page_label();
            connection2.borrow().send_transaction(vec![
                (view_posts_page_id, Persisted::ViewPostsPage(page), 1),
                (view_posts_page_id, Persisted::ViewPostsPage(old_page), -1),
            ]);
        }
    });

    let prev_page_el = prev_page.dyn_ref::<HtmlElement>().unwrap();
    prev_page_el.set_onclick(Some(prev_page_click.as_ref().unchecked_ref()));

    prev_page_click.forget();

    // on (page_num)

    page_ops.append_child(&page_label).unwrap();
    // on (page_num) - activate or deactive

    let next_page = document.create_element("button").unwrap();
    next_page.set_text_content(Some("Next"));
    page_ops.append_child(&next_page).unwrap();

    let next_page_click = Closure::<dyn FnMut()>::new(move || {
        let (_, root) = document_and_root();
        let old_page: u64 = root.get_attribute("page").unwrap().parse().unwrap();
        let total_pages: u64 = root
            .get_attribute("page_count")
            .unwrap_or("0".to_string())
            .parse()
            .unwrap();
        log(&total_pages.to_string());
        let page = old_page + 1;
        if page < total_pages {
            root.set_attribute("page", &(page.to_string())).unwrap();
            update_page_label();
            connection3.borrow().send_transaction(vec![
                (view_posts_page_id, Persisted::ViewPostsPage(page), 1),
                (view_posts_page_id, Persisted::ViewPostsPage(old_page), -1),
            ]);
        }
    });

    let next_page_el = next_page.dyn_ref::<HtmlElement>().unwrap();
    next_page_el.set_onclick(Some(next_page_click.as_ref().unchecked_ref()));

    next_page_click.forget();

    let on_parsed_message = move |items: Vec<QueryResult>| {
        let (document, root) = document_and_root();
        for item in items {
            match item {
                QueryResult::PagePost(post_id, page, time) => {
                    let posts_container = document
                        .query_selector("#posts-container")
                        .unwrap()
                        .unwrap();

                    let post_template = document.query_selector("#post-template").unwrap().unwrap();
                    let new_post = document.create_element("div").unwrap();
                    new_post.set_attribute("time", &time.to_string()).unwrap();
                    new_post.set_inner_html(&post_template.inner_html());
                    new_post.set_id(&post_id.to_string());

                    let mut insert_before = None;
                    let posts = posts_container.children();

                    log(&format!("inserting time: {}", time));

                    // 1 not 0, we skip the post template
                    for i in 1..posts.length() {
                        let post = posts.item(i).unwrap();
                        let other_time: u64 = post.get_attribute("time").unwrap().parse().unwrap();

                        if time >= other_time {
                            insert_before = Some(post);
                            break;
                        }

                        log(&("found post -- ".to_string() + &other_time.to_string()));
                    }

                    if let Some(insert_before) = insert_before {
                        insert_before
                            .before_with_node_1(&new_post)
                            .expect("could not insert before");
                    } else {
                        posts_container
                            .append_child(&new_post)
                            .expect("could not append");
                    }

                    let connection5 = connection4.clone();

                    let delete_button = new_post.query_selector(".post-remove").unwrap().unwrap();
                    let delete_button_click = Closure::<dyn FnMut()>::new(move || {
                        let mut persisted = vec![(post_id, Persisted::Post, -1)];

                        // the current post, the post template
                        if posts.length() == 2 && page > 0 {
                            let (_, root) = document_and_root();
                            root.set_attribute("page", &((page - 1).to_string()))
                                .unwrap();
                            update_page_label();

                            persisted.push((
                                view_posts_page_id,
                                Persisted::ViewPostsPage(page),
                                -1,
                            ));
                            persisted.push((
                                view_posts_page_id,
                                Persisted::ViewPostsPage(page - 1),
                                1,
                            ));
                        }

                        connection5.clone().borrow().send_transaction(persisted);
                    });

                    let delete_button_el = delete_button.dyn_ref::<HtmlElement>().unwrap();
                    delete_button_el
                        .set_onclick(Some(delete_button_click.as_ref().unchecked_ref()));

                    delete_button_click.forget();
                    let connection6 = connection4.clone();

                    let like_button = new_post.query_selector(".post-like").unwrap().unwrap();
                    let like_button_click = Closure::<dyn FnMut()>::new(move || {
                        // let diff =
                        //     if new_post.get_attribute("is_liked_") != Some("true".to_string()) {
                        //         1
                        //     } else {
                        //         -1
                        //     };
                        let diff = 1;

                        log(&("session: ".to_string() + &session_id.to_string()));

                        connection6.clone().borrow().send_transaction(vec![(
                            session_id,
                            Persisted::PostLike(post_id),
                            diff,
                        )]);
                    });

                    let like_button_el = like_button.dyn_ref::<HtmlElement>().unwrap();
                    like_button_el.set_onclick(Some(like_button_click.as_ref().unchecked_ref()));

                    like_button_click.forget();
                }
                QueryResult::PostTitle(post_id, title) => {
                    document
                        .get_element_by_id(&post_id.to_string())
                        .expect("could not find post by id")
                        .query_selector(".post-title")
                        .unwrap()
                        .unwrap()
                        .set_text_content(Some(&title));
                }
                QueryResult::PostBody(post_id, body) => {
                    document
                        .get_element_by_id(&post_id.to_string())
                        .expect("could not find post by id")
                        .query_selector(".post-body")
                        .unwrap()
                        .unwrap()
                        .set_text_content(Some(&body));
                }
                QueryResult::PostCreator(post_id, creator) => {
                    document
                        .get_element_by_id(&post_id.to_string())
                        .expect("could not find post by id")
                        .query_selector(".post-creator")
                        .unwrap()
                        .unwrap()
                        .set_text_content(Some(&creator));
                }
                QueryResult::DeletePost(post_id) => {
                    document
                        .get_element_by_id(&post_id.to_string())
                        .expect("could not delete post by id")
                        .remove();
                }
                QueryResult::PostLikedByUser(post_id, is_liked) => {
                    let status = if is_liked { "Unlike" } else { "Like" };
                    let post = document
                        .get_element_by_id(&post_id.to_string())
                        .expect("could not find post by id");

                    post.query_selector(".post-like-status")
                        .unwrap()
                        .unwrap()
                        .set_text_content(Some(status));

                    post.set_attribute("is_liked", if is_liked { "true" } else { "false" })
                        .unwrap();
                }
                QueryResult::PostTotalLikes(post_id, like_count) => {
                    document
                        .get_element_by_id(&post_id.to_string())
                        .expect("could not find post by id")
                        .query_selector(".post-likes")
                        .unwrap()
                        .unwrap()
                        .set_text_content(Some(&like_count.to_string()));
                }
                QueryResult::PostAggregates(post_count, page_count) => {
                    root.set_attribute("page_count", &page_count.to_string())
                        .unwrap();
                    update_page_label();
                    document
                        .query_selector("#posts-total")
                        .unwrap()
                        .unwrap()
                        .set_text_content(Some(&post_count.to_string()));
                }

                _ => {}
            }
        }
    };

    connection
        .borrow_mut()
        .init_on_parsed_message(Box::new(on_parsed_message));
}
