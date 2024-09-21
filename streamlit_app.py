import streamlit as st
import yaml
from yaml.loader import SafeLoader
from hashlib import sha256

# A simple in-memory user database
users_db = {
    "admin": sha256("admin".encode()).hexdigest(),
    "user1": sha256("mypassword".encode()).hexdigest()
}

def authenticate(username, password):
    """Function to check the username and password."""
    password_hash = sha256(password.encode()).hexdigest()
    if username in users_db and users_db[username] == password_hash:
        return True
    return False

def main():
    """Main function to render the Streamlit app."""
    st.title("Please login and continue")

    # Initialize session state for login status
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.username = ""
        st.session_state.current_page = "Home"  # Track the current page

    # Sidebar: If not logged in, show login form
    if not st.session_state.logged_in:
        st.sidebar.subheader("Login")
        username = st.sidebar.text_input("Username")
        password = st.sidebar.text_input("Password", type="password")
        login_button = st.sidebar.button("Login")

        if login_button:
            if authenticate(username, password):
                st.session_state.logged_in = True
                st.session_state.username = username
                st.sidebar.success(f"Welcome, {username}!")
            else:
                st.sidebar.error("Invalid credentials. Please try again.")

    # Sidebar: If logged in, show clickable navigation buttons
    if st.session_state.logged_in:
        # Show Logout Button
        if st.sidebar.button("Logout"):
            st.session_state.logged_in = False
            st.session_state.username = ""
            st.session_state.current_page = "Home"
            st.sidebar.info("You have logged out.")


        tab1, tab2, tab3 = st.tabs(["Cat", "Dog", "Owl"])

        with tab1:
            st.header("A cat")
            st.image("https://static.streamlit.io/examples/cat.jpg", width=200)
        with tab2:
            st.header("A dog")
            st.image("https://static.streamlit.io/examples/dog.jpg", width=200)
        with tab3:
            st.header("An owl")
            st.image("https://static.streamlit.io/examples/owl.jpg", width=200)

if __name__ == "__main__":
    main()