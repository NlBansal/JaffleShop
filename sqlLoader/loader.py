import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from pathlib import Path


load_dotenv()

# MySQL connection


def create_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("HOST_NAME"),
            user=os.getenv("USER_NAME"),
            password=os.getenv("USER_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        if connection.is_connected():
            print("✅ Connected to MySQL")
            return connection
    except Error as e:
        print(f"❌ Connection error: {e}")
    return None


def upload_csv_to_mysql(connection, csv_path):
    table_name = csv_path.stem  # e.g., 'customers.csv' → 'customers'
    df = pd.read_csv(csv_path)

    cursor = connection.cursor()

    # Drop table if exists
    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")

    # Create table dynamically
    columns = []
    for col in df.columns:
        col_clean = col.replace(" ", "_").lower()
        # all columns as TEXT for simplicity
        columns.append(f"`{col_clean}` TEXT")
    create_table_sql = f"CREATE TABLE `{table_name}` ({', '.join(columns)});"
    cursor.execute(create_table_sql)

    # Insert data
    for _, row in df.iterrows():
        values = ', '.join(['%s'] * len(row))
        insert_sql = f"INSERT INTO `{table_name}` VALUES ({values})"
        cursor.execute(insert_sql, tuple(row))

    connection.commit()
    print(f"✅ Loaded {csv_path.name} into `{table_name}`")

# Main script


def main():
    conn = create_connection()
    if not conn:
        return

    data_folder = Path('./data')
    csv_files = data_folder.glob('*.csv')

    for csv_file in csv_files:
        try:
            upload_csv_to_mysql(conn, csv_file)
        except Exception as e:
            print(f"❌ Failed to load {csv_file.name}: {e}")

    conn.close()
    print("✅ All done.")


if __name__ == "__main__":
    main()
