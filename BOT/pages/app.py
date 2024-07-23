import streamlit as st
import os
import sqlite3
import pandas as pd
import google.generativeai as genai
from dotenv import load_dotenv
import mysql.connector
import psycopg2
import pymongo
import urllib.parse

# Load environment variables
load_dotenv()

# Google API configuration
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# Function to get a response from the Gemini model
def get_response(question, prompt):
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content([prompt[0], question])
    return response.text

# Updated function to connect to different databases
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
                host=os.getenv("MYSQL_HOST"),
                user=os.getenv("MYSQL_USER"),
                password=os.getenv("MYSQL_PASSWORD"),
                database=os.getenv("MYSQL_DATABASE")
            )
            cursor = conn.cursor()
            table_name = "your_table_name"
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.commit()
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
                host=os.getenv("POSTGRES_HOST"),
                database=os.getenv("POSTGRES_DATABASE"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD")
            )
            cursor = conn.cursor()
            table_name = "your_table_name"
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.commit()
            st.success("CSV data loaded into PostgreSQL table.")
            return conn
        else:
            st.error("PostgreSQL connection requires a connection string or uploaded CSV file.")
    elif db_type == "MongoDB":
        if connection_string:
            return pymongo.MongoClient(connection_string)
        elif db_path and db_path.endswith(".csv"):
            df = pd.read_csv(db_path)
            client = pymongo.MongoClient(os.getenv("MONGO_URI"))
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

# Function to retrieve column names from a table
def get_column_names(table_name, conn):
    try:
        if isinstance(conn, sqlite3.Connection):
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
        elif isinstance(conn, mysql.connector.connection.MySQLConnection):
            cursor = conn.cursor()
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = [row[0] for row in cursor.fetchall()]
        elif isinstance(conn, psycopg2.extensions.connection):
            cursor = conn.cursor()
            cursor.execute(f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table_name}'")
            columns = [row[0] for row in cursor.fetchall()]
        elif isinstance(conn, pymongo.MongoClient):
            db = conn.get_default_database()
            if table_name in db.list_collection_names():
                columns = list(db[table_name].find_one().keys())
            else:
                columns = []
        else:
            columns = []
    except Exception as e:
        st.error(f"Error retrieving column names: {e}")
        columns = []
    return columns

# Function to execute SQL or MongoDB queries
def read_sql_query(sql, conn, table_name=None):
    sql = sql.replace("```", "").strip()
    try:
        if isinstance(conn, sqlite3.Connection):
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            conn.commit()
        elif isinstance(conn, mysql.connector.connection.MySQLConnection):
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
        elif isinstance(conn, psycopg2.extensions.connection):
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
        elif isinstance(conn, pymongo.MongoClient) and table_name:
            db = conn.get_default_database()
            collection = db[table_name]
            query = eval(sql)
            rows = list(collection.find(query))
        else:
            rows = []
    except Exception as e:
        st.error(f"Error executing query: {e}")
        rows = []
    return rows

# Function to read the query
def read_query(response, conn, table_name):
    try:
        if 'select' in response.lower():
            # SQL-based query
            result = read_sql_query(response, conn, table_name)
        else:
            # MongoDB-based query
            result = read_sql_query(response, conn, table_name)
        return result
    except Exception as e:
        st.error(f"Error reading query: {e}")
        return None

# Initialize the app
st.set_page_config(page_title="Data Analysis Bot!")
st.image("./logo.png", width=100)
st.header("Data Analysis Bot: App to Retrieve Data!")

# User inputs
database_type = st.selectbox("Select Database Type", ["SQLite", "MySQL", "PostgreSQL", "MongoDB"], key="database_type")
connection_string = st.text_area("Enter Database Connection String", placeholder="Example: postgresql://user:password@host:port/database", key="connection_string")
upload_file = st.file_uploader("Upload CSV File", type=["csv"])
table_name = st.text_input("Enter table name", "movies")
question = st.text_area("Input:", key="input")
submit = st.button("Ask the question!")

# Handle user input and process
if submit:
    db_path = None
    if upload_file:
        db_path = f"database_files/{upload_file.name}"
        os.makedirs("database_files", exist_ok=True)
        with open(db_path, "wb") as f:
            f.write(upload_file.read())

    conn = connect_to_database(database_type, connection_string=connection_string, db_path=db_path)

    if conn:
        columns = get_column_names(table_name, conn)
        if columns:
            prompt = [
                f"""
                You are an expert SQL Specialist. Your job is to translate natural language questions into SQL or MongoDB queries that work with the '{table_name}' table.
                The '{table_name}' table has the following columns:
                {', '.join(columns)} 

                When writing a query: 
                - Only use the columns mentioned above.
                - Do not add extra information or comments.
                - Use user-friendly aliases for clarity.
                Examples:
                    - **Question:** How many items are in the table?
                      **Answer:** SELECT COUNT(*) AS total_items FROM {table_name}; 
                    - **Question:** What are the top 5 items based on a column?
                      **Answer:** SELECT * FROM {table_name} ORDER BY some_column DESC LIMIT 5;
                    - **Question:** How many items are in the MongoDB collection?
                      **Answer:** {{"$count": "total_items"}}
                """
            ]
            response = get_response(question, prompt)
            st.subheader("The Generated SQL/MongoDB Query is:")
            st.code(response)

            # Execute SQL/MongoDB Query and display result
            result = read_query(response, conn, table_name)
            st.subheader("The Response is:")
            if result:
                st.write(pd.DataFrame(result))
            else:
                st.write("No results found.")

            # Provide explanation for SQL/MongoDB Query
            explanation_prompt = [
                f"""
                You are a SQL expert tasked with explaining the provided SQL or MongoDB query to a user who may not be familiar with SQL or MongoDB queries.

                Your explanation should be clear, concise, and easy to understand. Break down the query into its components and explain what each part does. 
                """
            ]
            explain = get_response(response, explanation_prompt)
            st.subheader("Explanation:")
            st.write(explain)

        conn.close()