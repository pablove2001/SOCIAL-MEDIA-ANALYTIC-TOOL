from database import users_collection, posts_collection
from model import User, Post
import pymongo
import sys

# Global variables
current_username = None


def print_menu():
    mm_options = {
        1: "Create user",
        2: "Delete current user",
        3: "Login",
        4: "Logout",
        5: "Change password",
        6: "Change notifications",
        7: "Change language",
        8: "Most used language",
        9: "Create a post",
        10: "Delete a post",
        11: "See your posts",
        12: "See posts from people",
        13: "Get list of users",
        14: "Populate database",
        15: "Clean database",
        16: "Exit"
    }

    print(f'\nCurrent user: {current_username}')
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def create_user():
    try:
        username = input("Enter the username: ").strip()
        password = input("Enter the password: ").strip()

        user = User(username, password)

        result = users_collection.insert_one(user.to_dict())
        print(f"User created with ID: {result.inserted_id}")
    except pymongo.errors.DuplicateKeyError:
        print("Error: A user with this email already exists.")
    except ValueError:
        print("Error: Invalid age. Please try again.")
    except Exception as e:
        print(f"Unexpected error: {e}")
    
    global current_username
    current_username = username


def delete_user():
    global current_username

    if current_username is None:
        print("There is no current user.")
        return

    confirmation = input(f"Are you sure you want to delete the user '{current_username}'? This action cannot be undone (yes/no): ").strip().lower()

    if confirmation == "yes":
        try:
            result = users_collection.delete_one({"username": current_username})

            if result.deleted_count == 1:
                print(f"User '{current_username}' has been successfully deleted.")
                current_username = None
            else:
                print(f"Error: User '{current_username}' could not be found in the database.")
        except Exception as e:
            print(f"Unexpected error while deleting user: {e}")
    else:
        print("Deletion canceled.")


def login():
    global current_username

    if current_username is not None:
        print(f"Already logged in as '{current_username}'. Please log out first if you want to log in as a different user.")
        return

    username = input("Enter your username: ").strip()
    password = input("Enter your password: ").strip()

    # Look for the user in the database
    user = users_collection.find_one({"username": username})

    if user is None:
        print("Error: User not found.")
    elif user['password'] != password:
        print("Error: Incorrect password.")
    else:
        current_username = username  # Set the global current user to the logged-in user
        print(f"Successfully logged in as '{current_username}'.")


def logout():
    global current_username

    if current_username is None:
        print("Error: No user is currently logged in.")
    else:
        print(f"Successfully logged out from '{current_username}'.")
        current_username = None  # Reset the global variable to log out


def change_password():
    global current_username

    if current_username is None:
        print("Error: No user is currently logged in.")
        return

    old_password = input("Enter your current password: ").strip()

    # Retrieve the user from the database
    user = users_collection.find_one({"username": current_username})

    if user is None:
        print("Error: User not found.")
        return

    if user['password'] != old_password:
        print("Error: Incorrect current password.")
        return

    new_password = input("Enter your new password: ").strip()

    try:
        # Update the password in the database
        result = users_collection.update_one(
            {"username": current_username},
            {"$set": {"password": new_password}}
        )

        if result.modified_count == 1:
            print("Your password has been successfully updated.")
        else:
            print("Error: There was an issue updating your password.")
    except Exception as e:
        print(f"Unexpected error: {e}")


def change_notifications():
    global current_username

    if current_username is None:
        print("Error: No user is currently logged in.")
        return

    user = users_collection.find_one({"username": current_username})

    if user is None:
        print("Error: User not found.")
        return

    current_notifications = user['notifications']
    print(f"Current notification setting: {current_notifications}")

    # Ask the user to toggle the notifications
    new_notifications = input("Enter 'on' to turn notifications ON or 'off' to turn them OFF: ").strip().lower()

    if new_notifications not in ['on', 'off']:
        print("Invalid input. Please enter 'on' or 'off'.")
        return

    # Update the notifications setting in the database
    users_collection.update_one(
        {"username": current_username},
        {"$set": {"notifications": new_notifications}}
    )

    print(f"Your notifications have been successfully updated to '{new_notifications}'.")


def change_language():
    global current_username

    if current_username is None:
        print("Error: No user is currently logged in.")
        return

    # Retrieve the user from the database
    user = users_collection.find_one({"username": current_username})

    if user is None:
        print("Error: User not found.")
        return

    current_language = user['language']
    print(f"Current language setting: {current_language}")

    # Ask the user to change the language
    new_language = input("Enter 'eng' to switch to English or 'esp' to switch to Spanish: ").strip().lower()

    if new_language not in ['eng', 'esp']:
        print("Invalid input. Please enter 'eng' or 'esp'.")
        return

    # Update the language setting in the database
    users_collection.update_one(
        {"username": current_username},
        {"$set": {"language": new_language}}
    )

    print(f"Your language has been successfully updated to '{new_language}'.")


def most_used_language():
    # Count the number of users with language set to 'eng'
    count_eng = users_collection.count_documents({"language": "eng"})
    
    # Count the number of users with language set to 'esp'
    count_esp = users_collection.count_documents({"language": "esp"})
    
    # Determine the most used language and the second most used
    if count_eng > count_esp:
        print(f"The most used language is 'eng' with {count_eng} users.")
        print(f"The second most used language is 'esp' with {count_esp} users.")
    elif count_esp > count_eng:
        print(f"The most used language is 'esp' with {count_esp} users.")
        print(f"The second most used language is 'eng' with {count_eng} users.")
    else:
        print(f"Both languages are used equally, with {count_eng} users each.")


def create_post():
    global current_username

    if current_username is None:
        print("Error: No user is currently logged in. Please log in first.")
        return

    # Prompt for the post details
    title = input("Enter the title of the post: ").strip()
    text = input("Enter the text of the post: ").strip()

    if not title or not text:
        print("Error: Title and text cannot be empty.")
        return

    # Check if a post with the same title already exists
    existing_post = posts_collection.find_one({"title": title})

    if existing_post:
        print(f"Error: A post with the title '{title}' already exists.")
        return

    try:
        # Create the Post object
        post = Post(title=title, text=text, username=current_username)

        # Insert the post into the database
        result = posts_collection.insert_one(post.to_dict())
        print(f"Post created with ID: {result.inserted_id}")

    except pymongo.errors.DuplicateKeyError:
        # This will catch if another post was inserted with the same title due to race condition
        print(f"Error: A post with the title '{title}' already exists.")
    except Exception as e:
        print(f"Unexpected error while creating the post: {e}")


def delete_a_post():
    global current_username

    if current_username is None:
        print("Error: No user is currently logged in. Please log in first.")
        return

    # Prompt the user for the title of the post they want to delete
    title = input("Enter the title of the post you want to delete: ").strip()

    if not title:
        print("Error: Title cannot be empty.")
        return

    # Find the post in the database based on the title
    post = posts_collection.find_one({"title": title})

    if post is None:
        print(f"Error: No post found with the title '{title}'.")
        return

    # Verify that the current user is the one who created the post
    if post["username"] != current_username:
        print("Error: You can only delete posts that you created.")
        return

    try:
        # Delete the post from the database
        result = posts_collection.delete_one({"title": title})

        if result.deleted_count == 1:
            print(f"Post with title '{title}' has been deleted.")
        else:
            print("Error: Failed to delete the post.")
    except Exception as e:
        print(f"Unexpected error while deleting the post: {e}")


def see_your_posts():
    global current_username

    if current_username is None:
        print("Error: No user is currently logged in. Please log in first.")
        return

    # Ask the user for the number of posts to display
    try:
        limit = int(input("Enter the number of posts you want to see (e.g., 5): ").strip())

        # Validate the limit input
        if limit <= 0:
            print("Error: Please enter a number greater than 0.")
            return
    except ValueError:
        print("Error: Invalid input. Please enter a valid number.")
        return

    # Find the posts created by the logged-in user, with a limit and sorted by creation_date (oldest first)
    user_posts = list(posts_collection.find({"username": current_username}).sort("creation_date", 1).limit(limit))

    # Check if any posts are returned
    if len(user_posts) == 0:
        print("You have no posts.")
        return

    # Display the posts
    print(f"\nPosts created by {current_username}:")
    for post in user_posts:
        print(f"\nTitle: {post['title']}")
        print(f"Text: {post['text']}")
        print(f"Created on: {post['creation_date']}")
        print("-" * 40)  # Separator for readability


def see_posts_from_people():
    global current_username

    if current_username is None:
        print("Error: No user is currently logged in. Please log in first.")
        return

    # Ask the user for the number of posts to display
    try:
        limit = int(input("Enter the number of posts you want to see (e.g., 5): ").strip())

        # Validate the limit input
        if limit <= 0:
            print("Error: Please enter a number greater than 0.")
            return
    except ValueError:
        print("Error: Invalid input. Please enter a valid number.")
        return

    # Find all posts, sorted by creation_date (oldest first), with a limit
    posts = list(posts_collection.find().sort("creation_date", 1).limit(limit))

    # Check if any posts are returned
    if posts == 0:
        print("No posts available.")
        return

    # Display the posts
    print(f"\nAll posts from people:")
    for post in posts:
        print(f"\nTitle: {post['title']}")
        print(f"Text: {post['text']}")
        print(f"Created by: {post['username']}")
        print(f"Created on: {post['creation_date']}")
        print("-" * 40)  # Separator for readability


def get_list_of_users():
    # Query the users_collection and only project the "username" field
    users = list(users_collection.find({}, {"_id": 0, "username": 1}))  # Exclude _id, include username
    
    if len(users) == 0:
        print("No users found.")
        return

    # Print all the usernames
    print("\nList of usernames:")
    for user in users:
        print(user['username'])


def populate_database():
    print("Not implemented yet")
    pass

def clean_database():
    global current_username

    # Delete all users and posts from their respective collections
    try:
        # Remove all users
        users_collection.delete_many({})
        # Remove all posts
        posts_collection.delete_many({})
        
        # Log out the current user
        current_username = None
        
        print("Database cleaned and user logged out.")
        
    except Exception as e:
        print(f"An error occurred while cleaning the database: {e}")

def main():
    while(True):
        print_menu()
        try:
            option = int(input("Select an option: "))
            if option == 1:
                create_user()
            elif option == 2:
                delete_user()
            elif option == 3:
                login()
            elif option == 4:
                logout()
            elif option == 5:
                change_password()
            elif option == 6:
                change_notifications()
            elif option == 7:
                change_language()
            elif option == 8:
                most_used_language()
            elif option == 9:
                create_post()
            elif option == 10:
                delete_a_post()
            elif option == 11:
                see_your_posts()
            elif option == 12:
                see_posts_from_people()
            elif option == 13:
                get_list_of_users()
            elif option == 14:
                populate_database()
            elif option == 15:
                clean_database()
            elif option == 16:
                print("Exiting the program. Goodbye!")
                sys.exit(0)
            else:
                print("Invalid option. Please try again.")
        except ValueError:
            print("Error: Please enter a valid number.")
        except Exception as e:
            print(f"Unexpected error: {e}")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))