# SPC-forum_minimal
partof: REQ-purpose
###

# TODO's

* TODO: split queries into fields ie. post title instead of entire posts
* TODO: send list of post id's to ensure order
    * suboptimal - we send the post creation time instead
* [DONE] TODO: isolate sessions

* [DONE] Make id's external instead of being inside Persisted items
* TODO: Delete items using "remove" instead of using a delete event
* [DONE] Sort list items
* [DONE] Pagination
* [DONE] TODO: Prevent flickering
    Do this by sending multiple QueryResults at once -
    concat outputs then use inspect batch
* TODO: replace unwrap and expect with error handling
* TODO: security risk
    an attacker can just connect to another port and hijack the session running there
    a security token is needed
* TODO: bootstrapping multiple times (ie. by going to the username change page and then to posts)
    causes duplicate post creation
* TODO: remove session var on websocket disconnection
* TODO: bug when you create two items, refresh, create a third, go to next page, delete third

* Cranelift -- deleting is bugged when switching sessions

# Features

Pages:

* [[.page_enter_username]] Page Form to Enter Username
* [[.page_posts]] Page Posts List

Posts Page Features:

* [[.create_post]] Create Post
    * Post titles have to be unique
    * Title or body cannot be empty
* Aggregation stats
    * [[.aggregates_global_post_count]] total posts
    * [[.aggregates_user_post_count]] posts by entered user id
    * [[.aggregates_user_likes]] total likes for user id
* List of posts
    * [[.post_info]] Post title, body, author and like count
    * [[.post_collapse]] Post can be collapsed
    * [[.post_like]] Post can be liked
    * [[.post_delete]] Post can be deleted by original user
    * [[.post_pagination]] Displays 3 newest posts, can go to previous page

## [[.post_pagination]]

While the user is on the front page (no page selected),
new posts will instantly appear when they are posted

If the user is on a certain page, the id of the newest post
at that time is saved and the entire pagination history
will be frozen.

## Dynamic Queries do not work

Dataflow results have to be global
