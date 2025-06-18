import os
import pandas as pd
import mysql.connector
import logging
from mysql.connector import Error
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()


def create_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("HOST_NAME"),
            user=os.getenv("USER_NAME"),
            password=os.getenv("USER_PASSWORD"),
            database=os.getenv("DB_NAME"),
            allow_local_infile=True
        )
        if conn.is_connected():
            logging.info("Successfully connected with DB")
            return conn
    except Error as e:
        logging.error(f"Error while connecting to DB: {e}")
        return None


def csv_load(conn, csv_path, chunk_size=1500):
    table_name = csv_path.stem
    cursor = conn.cursor()

    chunk_iter = pd.read_csv(csv_path, chunksize=chunk_size)
    first_chunk = next(chunk_iter)

    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
    columns = [
        f"`{col.replace(' ', '_').lower()}` TEXT"
        for col in first_chunk.columns
    ]
    create_table = f"CREATE TABLE `{table_name}` ({', '.join(columns)});"
    cursor.execute(create_table)
    logging.info(f"Created table `{table_name}`")

    def insert_chunk(chunk):
        placeholders = ', '.join(['%s'] * len(chunk.columns))
        insert_sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
        data = [tuple(row) for row in chunk.values]
        cursor.executemany(insert_sql, data)

    insert_chunk(first_chunk)
    for chunk in chunk_iter:
        insert_chunk(chunk)

    conn.commit()
    logging.info(
        f"Loaded {csv_path.name} into `{table_name}` in chunks of {chunk_size}")


def main():
    connection = create_connection()
    if not connection:
        logging.error("Failed to create database connection.")
        return

    data_folder = Path('/home/naman/Desktop/JaffleShop/data')

    for csv_file in data_folder.glob("*.csv"):
        try:
            csv_load(connection, csv_file)
        except Exception as e:
            logging.error(f"Failed to load {csv_file.name}: {e}")

    connection.close()
    logging.info("Database connection closed.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()
