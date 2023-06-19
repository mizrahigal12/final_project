import pymysql

def insert_to_log_table(timestamp, cnx):
    insert_query = "INSERT INTO process_log (insert_timestamp) VALUES (%s)"

    try:
        with cnx.cursor() as cursor:
            cursor.execute(insert_query, (timestamp,))
        cnx.commit()
        print("Data inserted successfully!")
    except pymysql.Error as e:
        print(f"Error inserting data: {e}")

# Example usage
timestamp = '2023-06-18 16:00:00'

# MySQL connection details
host = 'cnt7-naya-cdh63'
user = 'nifi'
password = 'NayaPass1!'
database = 'crypto_coins_agg'

try:
    # Establish the MySQL connection
    cnx = pymysql.connect(host=host, user=user, password=password, database=database)
    
    # Call the function to insert the timestamp into the table
    insert_to_log_table(timestamp, cnx)
    
    # Close the connection
    cnx.close()
except pymysql.Error as e:
    print(f"Error connecting to MySQL: {e}")

print('test')
