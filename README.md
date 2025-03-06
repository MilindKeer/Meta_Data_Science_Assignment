
# Meta Data science pre-interview test 
---
### First Table: We have a data set for all user actions that a user can take on a post. Some of the actions require a bit more information (such as reaction type, comment text, or report reason), so there is an extra column to store that information.

Table: user_actions <br>
Sample data:

```
ts,ds,user_id,post_id,action,extra
1530403200,2018-07-01,1209283021,329482048384792,view,
1530403200,2018-07-01,1209283021,329482048384792,like,
1530403200,2018-07-01,1938409273,349573908750923,reaction,LOVE
1530403200,2018-07-01,1209283021,329482048384792,comment,SuchniceRaybans
1530403200,2018-07-01,1238472931,329482048384792,report,HATE
1530403200,2018-07-01,1298349287,328472938472087,report,NUDITY
1530403200,2018-07-01,1238712388,329482048384792,post,
```

Q1: How many posts were reported yesterday for each report reason?

### Second Table: reviewer_removals
The reviewer_removals table contains data about whether a manual reviewer decided to take down a post. A row exists in this table only if the post was removed.
Table: reviewer_removals <br>
Sample data:

```
ds,reviewer_id,post_id
'2018-07-01',3894729384729078,329482048384792
'2018-07-01',8477594743909585,388573002873499
```

Q2: What percent of daily content that users view on Facebook is actually Spam?

---

# Solution: 

I have provided two solutions for this:

## First Solution - (SQL Query Solution)
The sample data is already available, and the SQL queries below can be executed directly on the sample data to verify the output.

Please refer/review the following two SQL files:

## question_1_solution.sql

  Q1: How many posts were reported yesterday for each report reason?

  **Assumptions:** <br>
  -- Data Format in extra: <br>
  -- The content in the extra field may contain extra spaces, case differences, and potentially NULL values.<br>
  -- If the extra field is NULL, it should be replaced with a default value 'blank_reason' while calculating stats, this is to get readable stats e.g. report action may or may not have extra string.<br>
  -- Data type of DS is given String but assumtion is it will be always in YYYY-mm-dd format which MySQL can read as a date format implicitly 

  **Key points:** <br>
  -- trim(lower(extra)): I am trimming the whitespace and converting the content to lowercase to handle inconsistencies in the way users input the report reasons (e.g., handling case variations like "Nudity" vs. "nudity" and ignoring any leading or trailing spaces). <br>
  -- The COALESCE(trim(lower(extra)), 'blank_reason') part ensures that if the extra field is NULL (i.e., no report reason provided), we substitute it with a default value 'blank_reason' to make sure all reports are counted, even those without a clear reason. 

```
    SELECT 
    COALESCE(NULLIF(TRIM(LOWER(extra)), ''), 'blank_reason') AS report_reason, COUNT(*) AS post_count
    FROM user_actions
    WHERE TRIM(LOWER(action)) = 'report' 
    AND ds = CURDATE() - INTERVAL 1 DAY 
    GROUP BY COALESCE(NULLIF(TRIM(LOWER(extra)), ''), 'blank_reason')
    ;

```

## question_2_solution.sql 
  Q2: What percent of daily content that users view on Facebook is actually Spam? <br>

  **Assumptions:** <br>
  -- A post is considered spam if its post_id exists in the reviewer_removals table. This indicates that the post has been manually reviewed and removed, marking it as spam. <br>
  -- E.g. The post may be reported as a spam in the 'user_actions' table but it will be considered spam only and only after manual review.ALTER <br>
  -- Once a post is added to the 'reviewer_removals' table, it will be considered spam permanently, regardless of the date. <br>
  -- The formula used to calculate the spam percentage of daily content is as follows: <br>
  -- Spam % = (Number of distinct posts identified as spam on a given day (ds) / Total number of distinct posts viewed on the same day (ds)) × 100
  -- Note - I am counting/considering post_ids only with Action = 'view'

```

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

```


# Second Solution - Python Solution

## How It Works:
- **setup_database.py** sets up and populates the database in MySQL, ensuring that the necessary tables exist with the correct structure and sample data (CSV used to load the data).
- **get_stats.py** runs the analysis, querying the database to extract the required statistics for both questions. The results are then returned and can be used for further reporting or analysis.

This solution provides a Python-based implementation of the database setup and data analysis, offering flexibility and ease of use for both setting up the database and generating insights from the data.

Please note: I have used my local MySQL instance. If you want to run the solution, you'll need to update the database connection credentials in both Python files.

## **setup_database.py**:
- This Python script is responsible for setting up the database environment. It will check if a database named **'Meta'** already exists; if not, it will create one.
- Additionally, the script will create the necessary tables: **'user_actions'** and **'reviewer_removals'**, if they don’t already exist in the database.
- The script will also import sample data from the provided CSV files into the respective tables. This allows for easy population of the database with sample data for further analysis.

```
# This script is to build a sample tables in MySQL and insert a sample data from CSVs
# Note - I have used my local MySQL database
# Also, The script can be made more modular and reusable, but it has been kept straightforward 
# by hardcoding values such as DB credentials, CSV file path, and name, etc.

import mysql.connector
from mysql.connector import Error
import pandas as pd

def create_database_and_table():
    try:
        mysql_connection = mysql.connector.connect(host='localhost', port = 3306,user='milind', password='<enter password>')

        #Let's create a DB called Meta
        if mysql_connection.is_connected():
            cursor = mysql_connection.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS Meta;")
            print(f"Database 'Meta' created successfully or already exists.")

            cursor.execute("USE Meta;")
          
            # Let's create a sample table 'user_actions'
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS user_actions (
                ts BIGINT,
                ds VARCHAR(255),
                user_id BIGINT,
                post_id BIGINT,
                action VARCHAR(255),
                extra VARCHAR(255)
            );
            '''
            cursor.execute(create_table_query)
            print("Table 'user_actions' created successfully.")

            #Let's delete existing data
            delete_query = "DELETE FROM user_actions;"
            cursor.execute(delete_query)
            mysql_connection.commit()
            print("Existing data deleted successfully.")
            
            #Let's insert new data from the CSV
            #csv with sample data - this can be updated to test various scenarios
            csv_file_path = 'user_actions_sample_data.csv'
            
            #pandas to read the CSV
            df = pd.read_csv(csv_file_path)
            df = df.where(pd.notnull(df), None)

            insert_query = '''
            INSERT INTO user_actions (ts, ds, user_id, post_id, action, extra)
            VALUES (%s, %s, %s, %s, %s, %s);
            '''
            for index, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))

            mysql_connection.commit()
            print(f"data inserted successfully.")


            #Let's now create reviewer_removals table 
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS reviewer_removals (
                ds VARCHAR(255),
                reviewer_id BIGINT,
                post_id BIGINT
            );
            '''
            cursor.execute(create_table_query)
            print("Table created successfully.")

            #Let's delete existing data
            delete_query = "DELETE FROM reviewer_removals;"
            cursor.execute(delete_query)
            mysql_connection.commit()
            print("Existing data deleted successfully.")
            
            #Let's insert new data from the CSV
            #csv with sample data - this can be updated to test various scenarios
            csv_file_path = 'reviewer_removals_sample_data.csv'
            
            #pandas to read the CSV
            df = pd.read_csv(csv_file_path)
            df = df.where(pd.notnull(df), None)

            insert_query = '''
            INSERT INTO reviewer_removals (ds,reviewer_id,post_id)
            VALUES (%s, %s, %s);
            '''
            for index, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))

            mysql_connection.commit()
            print(f"data inserted successfully.")    

        return True
    except Error as e:
        print("Error:", e)
        return False
    finally:
        # closing the connection
        if mysql_connection.is_connected():
            cursor.close()
            mysql_connection.close()
            print("MySQL connection closed.")


if __name__ == "__main__":
    result = create_database_and_table()
    if not result:
        print(f"set-up script failed")

```

## **get_stats.py**:
- This Python script generates the required statistics based on the two questions:
  1. **Question 1**: It calculates how many posts were reported yesterday for each report reason (as specified in the `user_actions` data).
  2. **Question 2**: It computes the percentage of daily content viewed by users that is considered spam (based on the `reviewer_removals` table and the posts marked as removed).
- Essentially, this script replicates the SQL logic defined earlier but executes it within a Python environment, querying the database and processing the data to generate the desired results.

```
import mysql.connector
from mysql.connector import Error

def get_reported_posts_yesterday(mysql_connection):
    try:
        if mysql_connection.is_connected():
            query = '''
                SELECT 
                COALESCE(trim(lower(extra)), 'blank_reason') AS report_reason, COUNT(*) AS post_count
                FROM user_actions
                WHERE TRIM(LOWER(action)) = 'report' 
                AND ds = CURDATE() - INTERVAL 1 DAY
                GROUP BY extra;
            '''
            cursor = mysql_connection.cursor()
            cursor.execute(query)

            results = cursor.fetchall()
            if results:
                print('*' * 100)
                print("Question 1: How many posts were reported yesterday for each report reason?")
                print("
Posts reported yesterday by each reason:
")
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
            print("
Percent of daily content that users view on Facebook is actually Spam:
")    

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

 
if __name__ == "__main__":
    
    # a config file or utility function can be written for this but currently hardcoded.
    mysql_connection = mysql.connector.connect(host='localhost', port = 3306,user='milind', password='<enter password>', database='Meta')

    result = get_reported_posts_yesterday(mysql_connection)
    if not result:
        print(f"get_reported_posts_yesterday function failed")

    result = get_spam_percent(mysql_connection)
    if not result:
        print(f"get_spam_percent function failed")

    mysql_connection.close()
```


