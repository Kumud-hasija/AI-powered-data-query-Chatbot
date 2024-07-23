import streamlit as st
import sqlite3

# Create a database connection
def create_connection():
    conn = sqlite3.connect('database.db')
    return conn

# Function to add a new user
def add_user(username, password):
    conn = create_connection()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (username, password) VALUES (?, ?)", (username, password))
        conn.commit()
        st.success("Account created successfully!")
    except sqlite3.IntegrityError:
        st.error("Username already exists. Please choose a different username.")
    finally:
        conn.close()

# Function to verify login credentials
def login_user(username, password):
    conn = create_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE username=? AND password=?", (username, password))
    data = c.fetchone()
    conn.close()
    return data

# Streamlit app layout
st.set_page_config(page_title="User Signup/Login Page", layout="wide")

st.markdown("""
    <style>
    body {
        background-color: #000080;  /* Navy blue background */
        color: #FFFFFF;
    }
    .stTextInput>div>div>input {
        background-color: #1E1E2F;
        color: #FFFFFF;
        border: 1px solid #3C3F54;
        padding: 10px;
    }
    .stButton>button {
        background-color: #3C3F54;
        color: #FFFFFF;
        border: 1px solid #5A5C6D;
        padding: 10px;
        border-radius: 5px;
        font-weight: bold;
    }
    .stButton>button:hover {
        background-color: #5A5C6D;
        color: #FFFFFF;
    }
    .stMarkdown {
        color: #FFFFFF;
    }
    .stSidebar {
        background-color: #1E1E2F;
    }
    .stSidebar .stSelectbox>div>div {
        background-color: #3C3F54;
        color: #FFFFFF;
    }
    .stTitle {
        color: #FFD700;
    }
    </style>
""", unsafe_allow_html=True)

st.title("User Signup/Login Page")

menu = ["Signup", "Login"]
choice = st.sidebar.selectbox("Menu", menu)

# Display the appropriate page
if choice == "Signup":
    st.subheader("Create a New Account")
    new_user = st.text_input("Username", key="signup_user")
    new_password = st.text_input("Password", type="password", key="signup_pass")

    if st.button("Signup"):
        add_user(new_user, new_password)

elif choice == "Login":
    st.subheader("Login to Your Account")
    username = st.text_input("Username", key="login_user")
    password = st.text_input("Password", type="password", key="login_pass")

    if st.button("Login"):
        result = login_user(username, password)
        if result:
            st.session_state["authenticated"] = True
            st.success("Logged in successfully! Redirecting...")
            st.switch_page("pages/app.py")
        else:
            st.error("Invalid username or password")

# # Database Selection Page (only shown if authenticated)
# if st.session_state.get("authenticated") and st.session_state.get("page") == "app":
#     select_database()
