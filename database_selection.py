import streamlit as st
import os
import sqlite3
import pandas as pd
import mysql.connector
import psycopg2
import pymongo
import urllib.parse

# Function to connect to the database
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
    # Check if user is logged in
    if not st.session_state.get("database_selection"):
        st.info("Please log in first.")
        st.stop()  # Stop the app until user logs in

    st.set_page_config(page_title="Database Selection", layout="wide")
    st.title("Database Selection")

    database_type = st.selectbox(
        "Select Database Type",
        ("SQLite", "MySQL", "PostgreSQL", "MongoDB", "Other"),
        key="database_type"
    )

    connection_string = st.text_area(
        "Enter Database Connection String",
        placeholder="Example: postgresql://user:password@host:port/database",
        key="connection_string"
    )

    upload_file = st.file_uploader("Choose a File", type=["db", "sql", "csv"], key="upload_file")

    if st.button("Continue"):
        if database_type != "Other":
            if connection_string and upload_file:
                st.error("Please choose either a connection string or upload a file, not both.")
            elif connection_string:
                st.session_state["user_db_type"] = database_type
                st.session_state["user_db_connection_string"] = connection_string
                st.session_state["user_db_path"] = None
                st.rerun()
            elif upload_file:
                file_path = f"database_files/{upload_file.name}"
                os.makedirs("database_files", exist_ok=True)
                with open(file_path, "wb") as f:
                    f.write(upload_file.read())
                st.session_state["user_db_type"] = database_type
                st.session_state["user_db_connection_string"] = None
                st.session_state["user_db_path"] = file_path
                st.rerun()
            else:
                st.error("Please provide a connection string or upload a database file.")
        else:
            st.info("Please select a valid database type.")

if __name__ == "__main__":
    main()
