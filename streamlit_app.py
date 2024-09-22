import streamlit as st

if "role" not in st.session_state:
    st.session_state.role = None

ROLES = [None, "Requester", "Responder", "Admin"]

username = st.text_input("Username")
password = st.text_input("Password", type="password")
login_button = st.button("Login")



def login():

    st.header("Log in")
    role = st.selectbox("Choose your role", ROLES)

    if st.button("Log in"):
        st.session_state.role = role
        st.rerun()


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
request_pages = [request_1, request_2]
respond_pages = [respond_1, respond_2]
admin_pages = [admin_1, admin_2]

st.title("Request manager")
st.logo("images/horizontal_blue.png", icon_image="images/icon_blue.png")

page_dict = {}
if st.session_state.role in ["Requester", "Admin"]:
    page_dict["Request"] = request_pages
if st.session_state.role in ["Responder", "Admin"]:
    page_dict["Respond"] = respond_pages
if st.session_state.role == "Admin":
    page_dict["Admin"] = admin_pages

if len(page_dict) > 0:
    pg = st.navigation({"Account": account_pages} | page_dict)
else:
    pg = st.navigation([st.Page(login)])

pg.run()



# A simple in-memory user database
# users_db = {
#     "admin": sha256("admin".encode()).hexdigest(),
#     "user1": sha256("mypassword".encode()).hexdigest()
# }

# def authenticate(username, password):
#     """Function to check the username and password."""
#     password_hash = sha256(password.encode()).hexdigest()
#     if username in users_db and users_db[username] == password_hash:
#         return True
#     return False

# def main():
#     """Main function to render the Streamlit app."""
#     st.title("Please login and continue")

#     # Initialize session state for login status
#     if 'logged_in' not in st.session_state:
#         st.session_state.logged_in = False
#         st.session_state.username = ""
#         st.session_state.current_page = "Home"  # Track the current page

#     # Sidebar: If not logged in, show login form
#     if not st.session_state.logged_in:
#         st.sidebar.subheader("Login")
#         username = st.sidebar.text_input("Username")
#         password = st.sidebar.text_input("Password", type="password")
#         login_button = st.sidebar.button("Login")

#         if login_button:
#             if authenticate(username, password):
#                 st.session_state.logged_in = True
#                 st.session_state.username = username
#                 st.sidebar.success(f"Welcome, {username}!")
#             else:
#                 st.sidebar.error("Invalid credentials. Please try again.")

#     # Sidebar: If logged in, show clickable navigation buttons
#     if st.session_state.logged_in:
#         # Show Logout Button
#         if st.sidebar.button("Logout"):
#             st.session_state.logged_in = False
#             st.session_state.username = ""
#             st.session_state.current_page = "Home"
#             st.sidebar.info("You have logged out.")


#         tab1, tab2, tab3 = st.tabs(["Cat", "Dog", "Owl"])

#         with tab1:
#             st.header("A cat")
#             st.image("https://static.streamlit.io/examples/cat.jpg", width=200)
#         with tab2:
#             st.header("A dog")
#             st.image("https://static.streamlit.io/examples/dog.jpg", width=200)
#         with tab3:
#             st.header("An owl")
#             st.image("https://static.streamlit.io/examples/owl.jpg", width=200)

# if __name__ == "__main__":
#     main()