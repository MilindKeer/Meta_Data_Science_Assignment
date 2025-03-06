-- Q1: How many posts were reported yesterday for each report reason?

-- Assumptions:
-- Data Format in extra:
-- The content in the extra field may contain extra spaces, case differences, and potentially NULL values.
-- If the extra field is NULL, it should be replaced with a default value 'blank_reason' while calculating stats, this is to get readable stats e.g. report action may or may not have extra string.
-- Data type of DS is given String but assumtion is it will be always in YYYY-mm-dd format which MySQL can read as a date format implicitly 

-- Key points:
-- trim(lower(extra)): I am trimming the whitespace and converting the content to lowercase to handle inconsistencies in the way users input the report reasons (e.g., handling case variations like "Nudity" vs. "nudity" and ignoring any leading or trailing spaces).
-- The COALESCE(trim(lower(extra)), 'blank_reason') part ensures that if the extra field is NULL (i.e., no report reason provided), we substitute it with a default value 'blank_reason' to make sure all reports are counted, even those without a clear reason.

SELECT 
COALESCE(NULLIF(TRIM(LOWER(extra)), ''), 'blank_reason') AS report_reason, COUNT(*) AS post_count
FROM user_actions
WHERE TRIM(LOWER(action)) = 'report' 
AND ds = CURDATE() - INTERVAL 1 DAY 
GROUP BY COALESCE(NULLIF(TRIM(LOWER(extra)), ''), 'blank_reason')
;


