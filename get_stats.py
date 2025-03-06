import mysql.connector
from mysql.connector import Error

def get_reported_posts_yesterday(mysql_connection):
    try:
        # check MySQL connection
        if mysql_connection.is_connected():
            query = '''
                SELECT 
                COALESCE(NULLIF(TRIM(LOWER(extra)), ''), 'blank_reason') AS report_reason, COUNT(*) AS post_count
                FROM user_actions
                WHERE TRIM(LOWER(action)) = 'report' 
                AND ds = CURDATE() - INTERVAL 1 DAY 
                GROUP BY COALESCE(NULLIF(TRIM(LOWER(extra)), ''), 'blank_reason')
                ;
            '''
            cursor = mysql_connection.cursor()
            cursor.execute(query)

            # Get all data
            results = cursor.fetchall()
            if results:
                print('*' * 100)
                print("Question 1: How many posts were reported yesterday for each report reason?")
                print("\nPosts reported yesterday by each reason:\n")
                for row in results:
                    print(f"Report Reason: {row[0]}, Count: {row[1]}")
                print('*' * 100)    
            else:
                print("No reports found for yesterday.")
        return True
    except Error as e:
        print(f"Error: {e}")
        return False
    finally:
        if mysql_connection.is_connected():
            cursor.close()
            # mysql_connection.close()
            # print("MySQL connection closed.")


def get_spam_percent(mysql_connection):
    try:
        if mysql_connection.is_connected():
            query = '''
                SELECT
                    ua.ds AS date,
                    COUNT(DISTINCT ua.post_id) AS total_views,
                    COUNT(DISTINCT CASE 
                                    WHEN ua.post_id IN (SELECT post_id FROM reviewer_removals) 
                                    THEN ua.post_id 
                                    END) AS spam_views,
                    (COUNT(DISTINCT CASE 
                                    WHEN ua.post_id IN (SELECT post_id FROM reviewer_removals) 
                                    THEN ua.post_id 
                                    END) / COUNT(DISTINCT ua.post_id)) * 100 AS spam_percentage
                FROM
                    user_actions ua
                WHERE
                    ua.ds < CURDATE() -- Exclude today's data
                    AND ua.action = 'view' -- Only consider 'view' actions
                GROUP BY
                    ua.ds
                ORDER BY
                    ua.ds DESC;
            '''
            cursor = mysql_connection.cursor()
            cursor.execute(query)

            results = cursor.fetchall()

            print('*' * 100)
            print("Question 2: What percent of daily content that users view on Facebook is actually Spam?")
            print("\nPercent of daily content that users view on Facebook is actually Spam:\n")    

            for result in results:
                date = result[0]
                total_views = result[1]
                spam_views = result[2]
                spam_percentage = result[3]
                print(f"Date: {date}, Total Views: {total_views}, Spam Views: {spam_views}, Spam Percentage: {spam_percentage:.2f}%")

            print('*' * 100)
        return True
    except Error as e:
        print(f"Error: {e}")

    finally:
        if mysql_connection.is_connected():
            cursor.close()
            # mysql_connection.close()
            # print("MySQL connection closed.")

def get_spam_percent_alternate(mysql_connection):
    try:
        if mysql_connection.is_connected():
            query = '''
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
            '''
            cursor = mysql_connection.cursor()
            cursor.execute(query)

            results = cursor.fetchall()

            print('*' * 100)
            print("Question 2: What percent of daily content that users view on Facebook is actually Spam?")
            print("\nAlternate approach:")
            print("\nPercent of daily content that users view on Facebook is actually Spam:\n")    

            for result in results:
                date = result[0]
                spam_percentage = result[1]
                print(f"Date: {date}, Spam Percentage: {spam_percentage:.2f}%")

            print('*' * 100)
        return True
    except Error as e:
        print(f"Error: {e}")

    finally:
        if mysql_connection.is_connected():
            cursor.close()
            # mysql_connection.close()
            # print("MySQL connection closed.")




if __name__ == "__main__":
    
    # a config file or utility function can be written for this but currently hardcoded.
    mysql_connection = mysql.connector.connect(host='localhost', port = 3306,user='milind', password='Arnav@123', database='Meta')

    result = get_reported_posts_yesterday(mysql_connection)
    if not result:
        print(f"get_reported_posts_yesterday function failed")

    result = get_spam_percent(mysql_connection)
    if not result:
        print(f"get_spam_percent function failed")

    result = get_spam_percent_alternate(mysql_connection)
    if not result:
        print(f"get_spam_percent_alternate function failed")

    mysql_connection.close()