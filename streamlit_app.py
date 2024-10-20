import streamlit as st
import yaml
import bcrypt as bc


# Load the secrets.yaml file

def load_secrets():
    with open("secrets.yaml", "r") as file:
        return yaml.safe_load(file)

secrets = load_secrets()

#Password hashing

def hash_password(password):
    salt = bc.gensalt()  # Generate salt
    hashed_password = bc.hashpw(password.encode('utf-8'), salt)  # Hash password
    return hashed_password

def check_password(username, entered_password):
    if username in secrets['users']:
         stored_password_hash = secrets['users'][username]['password']
    return bc.checkpw(entered_password.encode('utf-8'), stored_password_hash.encode('utf-8'))


# Role setup

def role_lookup(username):
    if username in secrets['users']:
        return secrets['users'][username]['role']
    return None 

if "role" not in st.session_state:
    st.session_state.role = None



#Login and logout

def login():
    st.header("Log in")
    if st.session_state.role is not None:
        st.write(f"Already logged in as {st.session_state.role}")
        return
    else:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        login_button = st.button("Log in")

        
                
    if login_button:
        #if authenticate(username, password):
        if check_password(username, password):
            st.session_state.role = role_lookup(username)
            st.rerun()
        else:
            st.error("Invalid username or password")


def logout():
    st.session_state.role = None
    st.rerun()

role = st.session_state.role

logout_page = st.Page(logout, title="Log out", icon=":material/logout:")
settings = st.Page("settings.py", title="Settings", icon=":material/settings:")

request_1 = st.Page(
    "request/request_1.py",
    title="Request 1",
    icon=":material/help:",
    default=(role == "Requester"),
)
request_2 = st.Page(
    "request/request_2.py", title="Request 2", icon=":material/bug_report:"
)
respond_1 = st.Page(
    "respond/respond_1.py",
    title="Respond 1",
    icon=":material/healing:",
    default=(role == "Responder"),
)
respond_2 = st.Page(
    "respond/respond_2.py", title="Respond 2", icon=":material/handyman:"
)
admin_1 = st.Page(
    "admin/admin_1.py",
    title="Admin 1",
    icon=":material/person_add:",
    default=(role == "Admin"),
)
admin_2 = st.Page("admin/admin_2.py", title="Admin 2", icon=":material/security:")

account_pages = [logout_page, settings]
#request_pages = [request_1, request_2]
respond_pages = [respond_1, respond_2]
admin_pages = [admin_1, admin_2]

#st.title("Request manager")
st.logo("images/horizontal_blue.png", icon_image="images/new_logo.png")

page_dict = {}
#if st.session_state.role in ["Requester", "Admin"]:
#    page_dict["Request"] = request_pages
if st.session_state.role in ["Responder", "Admin"]:
    page_dict["Respond"] = respond_pages
if st.session_state.role == "Admin":
    page_dict["Admin"] = admin_pages

if len(page_dict) > 0:
    pg = st.navigation({"Account": account_pages} | page_dict)
else:
    pg = st.navigation([st.Page(login)])

pg.run()
