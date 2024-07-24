import streamlit as st
import sqlite3
import pandas as pd
import mysql.connector
import psycopg2
import pymongo
import urllib.parse

def connect_to_database(db_type, connection_string=None, db_path=None):
    if db_type == "SQLite":
        if db_path:
            return sqlite3.connect(db_path)
        else:
            st.error("SQLite connection requires a file path.")
    elif db_type == "MySQL":
        if connection_string:
            parsed_url = urllib.parse.urlparse(connection_string)
            return mysql.connector.connect(
                host=parsed_url.hostname,
                user=parsed_url.username,
                password=parsed_url.password,
                database=parsed_url.path[1:]
            )
        elif db_path and db_path.endswith(".csv"):
            df = pd.read_csv(db_path)
            conn = mysql.connector.connect(
                host="localhost",
                user="your_mysql_user",
                password="your_mysql_password",
                database="your_mysql_database"
            )
            cursor = conn.cursor()
            table_name = "your_table_name"
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.commit()
            conn.close()
            st.success("CSV data loaded into MySQL table.")
            return conn
        else:
            st.error("MySQL connection requires a connection string or uploaded CSV file.")
    elif db_type == "PostgreSQL":
        if connection_string:
            parsed_url = urllib.parse.urlparse(connection_string)
            return psycopg2.connect(
                host=parsed_url.hostname,
                database=parsed_url.path[1:],
                user=parsed_url.username,
                password=parsed_url.password
            )
        elif db_path and db_path.endswith(".csv"):
            df = pd.read_csv(db_path)
            conn = psycopg2.connect(
                host="localhost",
                database="your_postgresql_database",
                user="your_postgresql_user",
                password="your_postgresql_password"
            )
            cursor = conn.cursor()
            table_name = "your_table_name"
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.commit()
            conn.close()
            st.success("CSV data loaded into PostgreSQL table.")
            return conn
        else:
            st.error("PostgreSQL connection requires a connection string or uploaded CSV file.")
    elif db_type == "MongoDB":
        if connection_string:
            return pymongo.MongoClient(connection_string)
        elif db_path and db_path.endswith(".csv"):
            df = pd.read_csv(db_path)
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client["your_mongodb_database"]
            table_name = "your_table_name"
            db[table_name].drop()
            db[table_name].insert_many(df.to_dict(orient="records"))
            st.success("CSV data loaded into MongoDB collection.")
            return client
        else:
            st.error("MongoDB connection requires a connection string or uploaded CSV file.")
    else:
        st.error(f"Unsupported database type: {db_type}")

def main():
    # Retrieve database details from session state
    db_type = st.session_state.get("database_type")
    connection_string = st.session_state.get("connection_string")
    db_path = st.session_state.get("file_path")

    user_db_type = st.session_state.get("user_db_type")
    user_db_connection_string = st.session_state.get("user_db_connection_string")
    user_db_path = st.session_state.get("user_db_path")

    if user_db_type:
        user_conn = connect_to_database(user_db_type, user_db_connection_string, user_db_path)
        if user_conn:
            st.success(f"Successfully connected to user database: {user_db_type}")

    if db_type:
        conn = connect_to_database(db_type, connection_string, db_path)
        if conn:
            st.success(f"Successfully connected to internal database: {db_type}")

if __name__ == "__main__":
    main()
