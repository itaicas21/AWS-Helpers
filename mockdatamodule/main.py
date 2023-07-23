import os
import vars  # Import vars module
import time

if vars.db_type == 'postgres':
    import psycopg2 as db_connector
elif vars.db_type == 'mysql':
    import mysql.connector as db_connector
else:
    raise ValueError("Invalid database type. Please set db_type to either 'postgres' or 'mysql'.")

def generate_blob(size):
    """
    Function to generate a BLOB of given size.
    :param size: Size of the BLOB data to generate in bytes.
    :return: BLOB data.
    """
    return os.urandom(size)

def main():
    # Desired total data size in the table in bytes
    total_size = vars.size_in_gb * 1024 * 1024 * 1024

    # Individual blob size: 1 MB
    blob_size = 1 * 1024 * 1024  # Modify this as needed

    # Calculate the number of rows needed to reach the desired total size
    num_rows = total_size // blob_size
    if total_size % blob_size > 0:
        num_rows += 1  # Add an extra row to account for the remainder

    # Establish a connection to the database
    connection = db_connector.connect(
        host=vars.db_host,
        port=vars.db_port,
        database=vars.db_name,
        user=vars.db_username,
        password=vars.db_password
    )

    # Create a cursor object
    cursor = connection.cursor()

    # SQL command to check if table exists
    if vars.db_type == 'postgres':
        check_table_sql = "SELECT * FROM information_schema.tables WHERE table_name='my_table'"
    elif vars.db_type == 'mysql':
        check_table_sql = "SELECT * FROM information_schema.tables WHERE table_schema = %s AND table_name = 'my_table'"

    cursor.execute(check_table_sql, (vars.db_name,))
    result = cursor.fetchone()

    if not result:
        # SQL command to create a table with a BLOB column
        if vars.db_type == 'postgres':
            create_table_sql = "CREATE TABLE my_table (id SERIAL PRIMARY KEY, blob_data BYTEA)"
        elif vars.db_type == 'mysql':
            create_table_sql = "CREATE TABLE my_table (id INT AUTO_INCREMENT PRIMARY KEY, blob_data LONGBLOB)"

        cursor.execute(create_table_sql)
        connection.commit()

    # Prepare the INSERT statement
    insert_sql = "INSERT INTO my_table (blob_data) VALUES (%s)"

    # Insert rows into the table with BLOB data
    for i in range(1, num_rows + 1):  
        blob_data = generate_blob(blob_size)
        while True:
            try:
                cursor.execute(insert_sql, (blob_data,))
                if i % 1000 == 0:  # Commit every 1000 rows
                    connection.commit()
                    print(f"Committed up to row {i}")
                break
            except db_connector.errors.OperationalError as e:
                print(f"Lost connection to the server. Retrying in 5 seconds. Error details: {e}")
                time.sleep(5)
                connection = db_connector.connect(
                    host=vars.db_host,
                    port=vars.db_port,
                    database=vars.db_name,
                    user=vars.db_username,
                    password=vars.db_password
                )
                cursor = connection.cursor()
                continue

    # Commit any remaining rows
    connection.commit()
    print("All rows inserted successfully.")

    # Close the cursor and the database connection
    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()