-- Q2: What percent of daily content that users view on Facebook is actually Spam?

-- Assumptions:
-- A post is considered spam if its post_id exists in the reviewer_removals table. This indicates that the post has been manually reviewed and removed, marking it as spam.
-- E.g. The post may be reported as a spam in the 'user_actions' table but it will be considered spam only and only after manual review.ALTER
-- The formula used to calculate the spam percentage of daily content is as follows:
-- Spam % = (Number of distinct posts identified as spam on a given day (ds) / Total number of distinct posts viewed on the same day (ds)) × 100
-- Note - I am counting/considering post_ids only with Action = 'view'

SELECT
    ua.ds AS date,
    COUNT(DISTINCT ua.post_id) AS total_views_count,
    COUNT(DISTINCT CASE 
                    WHEN ua.post_id IN (SELECT post_id FROM reviewer_removals) 
                    THEN ua.post_id 
                    END) AS spam_views_count,
    (COUNT(DISTINCT CASE 
                    WHEN ua.post_id IN (SELECT post_id FROM reviewer_removals) 
                    THEN ua.post_id 
                    END) / COUNT(DISTINCT ua.post_id)) * 100 AS spam_percentage
FROM
    user_actions ua
WHERE
    ua.ds < CURDATE()  -- Exclude today's data because manual review is pending
    AND ua.action = 'view'  -- only considering 'view' actions
GROUP BY
    ua.ds
ORDER BY
    ua.ds DESC; 
