SELECT active_user.user_id , u.username,
max(active_user.date) as last_activity, 
from_unixtime(u.register_date) as register_date,
year(from_unixtime(u.register_date)) as register_year,
(2018-up.dob_year) as user_age,
date_diff('day',current_date,max(active_user.date)) as recency,
count(distinct p.post_id) as fre_cmt,
count(distinct p.thread_id) as count_thread

FROM (
SELECT user_id, CAST(from_unixtime(post_date) AS DATE) AS date
FROM xf_post

UNION 

SELECT like_user_id AS user_id, CAST(from_unixtime(like_date) AS DATE) AS date
FROM xf_liked_content

UNION

SELECT user_id, CAST(from_unixtime(message_date) AS DATE) AS date
FROM xf_conversation_message
  
UNION 

SELECT user_id, CAST(from_unixtime(post_date) AS DATE) AS date
FROM xf_thread 
) AS active_user
JOIN xf_user_profile up ON active_user.user_id = up.user_id
JOIN xf_user u ON up.user_id = u.user_id
JOIN xf_post p ON u.user_id = p.user_id
GROUP BY active_user.user_id, from_unixtime(u.register_date), (2018-up.dob_year), u.username
ORDER BY count(p.post_id) desc