-- Q2: What percent of daily content that users view on Facebook is actually Spam?

-- This alternative approach to calculating the percentage of daily content viewed by users on Facebook can actually 
-- be considered spam based on a different interpretation.

-- Assumptions:
-- A post is considered spam if its post_id exists in the reviewer_removals table. This indicates that the post has been manually reviewed and removed, marking it as spam.
-- E.g. The post may be reported as a spam in the 'user_actions' table but it will be considered spam only and only after manual review.ALTER

-- The alternate formula used to calculate the spam percentage of daily content is as follows:
-- Spam % = (Total number of spam posts in the 'user_actions' table on a given day (ds) / Total number of posts viewed on the same day (ds)) × 100


with spam_flag as (
SELECT ua.ds,
       CASE WHEN rr.post_id IS NULL THEN 0 ELSE 1 END AS spam_flag
FROM user_actions ua LEFT JOIN reviewer_removals rr ON ua.post_id = rr.post_id 
WHERE lower(action) = 'view'
)
select ds, 100* sum(spam_flag)/count(*) as spam_percentage
from spam_flag
group by ds
ORDER BY ds DESC
;