# SPC-events
partof: REQ-purpose
###


A case study in what a forum
has in terms of features, as well as 
how the framework most efficiently can be built around this
realistic proof of concept.

__Pages:__

### Index

SPAWNS_ON_LOAD: { session page home:index_body_id tx0 add }

### Home Page

PAGE_LOAD_ON: { session page home:index_body_id tx0 add }

[[.sign_in]] -- `Form`

The user signs in by entering any username.
No email verification is needed.

ACTION_ON_SUBMIT:

if exists { session sign_in existing_username _tx0 add } 

then { session sign_in_err_dup_username show tx0 add }

else
    { session sign_in_err_dup_username show tx0 rm }
    { session username username_val tx0 add } 
    { session page posts:index_body_id tx0 add }

[[.sign_in_err_dup_username]]

visible depending on { session sign_in_err_dup_username show tx0 add }
existing

### Posts Page

PAGE_LOAD_ON: { session page posts:index_body_id tx0 add }

[[.create_post_section_toggle]]

SEND_ON_TOGGLE { session create_post_section_toggle enable tx0 `add/rm`  } 

ACTION_ON_RECEIVE_TOGGLE - change send action from `add/rm` to `rm/add` or vice versa

[[.create_post_section]] -- `Form`

CHANGES_VISIBILITY_ON: { session create_post_section_visible visible tx0 `add/rm`  } 

show or hide depending on `add/rm`


Forum Fields: post_title post_body

update fields on form submit with: { post_id create_post_field_post_(title/body) field_value tx0 add }

validate fields on submit with: both cannot be empty; post_title with different post_id cannot exist

[[.display_posts]] -- `Generic List`

Get list of post ids to display from { post_id create_post_field_title _value tx0 add }

[[.post_item]]

Takes post_id and retrieves { post_id create_post_field_post_(title/body) field_value tx0 add }

<!-- TODO LATER

[[.update_post]] -- `Form`

[[.delete_post]] -- `Button`

-->

<!-- TODO LATER

### Comments

[[.create_comment]]

[[.update_comment]]

[[.delete_comment]]

[[.display_comments]] -- `Generic List`

[[.rate_comment]]

-->

### Generic List


Can be sorted by:

* newest to oldest or vice versa
* highest rating / controversial

Initially loads the 10 most fitting to criteria. Has option to load 10 more.
