# SPC-forum_minimal
partof: REQ-purpose
###

# TODO's

* Maintain dataflows for every connection (prevent stale data on initial load)
* Make id's external instead of being inside Persisted items
* Delete items using "remove" instead of using a delete event

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
