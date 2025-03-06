# This script is to build a sample tables in MySQL and insert a sample data from CSVs
# Note - I have used my local MySQL database
# Also, The script can be made more modular and reusable, but it has been kept straightforward 
# by hardcoding values such as DB credentials, CSV file path, and name, etc.

import mysql.connector
from mysql.connector import Error
import pandas as pd

def create_database_and_table():
    try:
        mysql_connection = mysql.connector.connect(host='localhost', port = 3306,user='milind', password='Arnav@123')

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
            
            #Let's insert new data from the sample CSV
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

