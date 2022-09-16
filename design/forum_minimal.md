# SPC-forum_minimal
partof: REQ-purpose
###

# Features

* Page Form to Enter Username
* Page Posts List
    * Aggregation stats
        * total posts
        * posts by entered user id
        * total likes for user id
    * List of posts
        * Post title, body, author and like count
        * Post can be collapsed
        * Post can be liked
        * Post can be deleted by original user
        * Displays 3 newest posts, can go to previous page

## [[.pagination]]

While the user is on the front page (no page selected),
new posts will instantly appear when they are posted

If the user is on a certain page, the id of the newest post
at that time is saved and the entire pagination history
will be frozen.
