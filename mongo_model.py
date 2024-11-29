import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import pymongo

class User:
    def __init__(self, username:str, password:str):
        self.username = username
        self.password = password
        self.creation_date = datetime.now()
        self.notifications = "on" # on of of
        self.language = "ENG" # ENG or ESP

    def to_dict(self):
        return {
            "username": self.username,
            "password": self.password,
            "creation_date": self.creation_date,
            "notifications": self.notifications,
            "language": self.language,
        }

class Post:
    def __init__(self, title:str, text:str, username:str):
        self.title = title
        self.text = text
        self.username = username
        self.creation_date = datetime.now()

    def to_dict(self):
        return {
            "title": self.title,
            "text": self.text,
            "username": self.username,
            "creation_date": self.creation_date
        }

class MongoModel:
    def __init__(self):
        self.current_username = None

        # .env
        MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        DB_NAME = 'iteso'

        try:
            # Try to connect to MongoDB
            client = MongoClient(MONGODB_URI)
            
            # Check the connection
            client.admin.command('ping')  # If successful, it will respond with "ok: 1"
            
            # Successful connection, access the database and collections
            db = client[DB_NAME]
            self.users_collection = db["users"]
            self.posts_collection = db["posts"]
            
            # Create indexes
            self.users_collection.create_index("username", unique=True)
            self.posts_collection.create_index("title", unique=True)

            print("MongoDB connection successful.")
            
        except ConnectionFailure:
            print("Connection error to MongoDB. Cannot continue.")
            exit(1)  # Stop the script if it cannot connect
        except Exception as e:
            print(f"Unexpected error: {e}")
            exit(1)  # Stop the script if any other error occurs

    def create_user(self):
        try:
            username = input("Enter the username: ").strip()
            password = input("Enter the password: ").strip()

            user = User(username, password)

            result = self.users_collection.insert_one(user.to_dict())
            print(f"User created with ID: {result.inserted_id}")
        except pymongo.errors.DuplicateKeyError:
            print("Error: A user with this email already exists.")
        except ValueError:
            print("Error: Invalid age. Please try again.")
        except Exception as e:
            print(f"Unexpected error: {e}")
        
        self.current_username = username


    def delete_user(self):
        if self.current_username is None:
            print("There is no current user.")
            return

        confirmation = input(f"Are you sure you want to delete the user '{self.current_username}'? This action cannot be undone (yes/no): ").strip().lower()

        if confirmation == "yes":
            try:
                result = self.users_collection.delete_one({"username": self.current_username})

                if result.deleted_count == 1:
                    print(f"User '{self.current_username}' has been successfully deleted.")
                    self.current_username = None
                else:
                    print(f"Error: User '{self.current_username}' could not be found in the database.")
            except Exception as e:
                print(f"Unexpected error while deleting user: {e}")
        else:
            print("Deletion canceled.")


    def login(self):
        if self.current_username is not None:
            print(f"Already logged in as '{self.current_username}'. Please log out first if you want to log in as a different user.")
            return

        username = input("Enter your username: ").strip()
        password = input("Enter your password: ").strip()

        # Look for the user in the database
        user = self.users_collection.find_one({"username": username})

        if user is None:
            print("Error: User not found.")
        elif user['password'] != password:
            print("Error: Incorrect password.")
        else:
            self.current_username = username  # Set the global current user to the logged-in user
            print(f"Successfully logged in as '{self.current_username}'.")


    def logout(self):
        if self.current_username is None:
            print("Error: No user is currently logged in.")
        else:
            print(f"Successfully logged out from '{self.current_username}'.")
            self.current_username = None  # Reset the global variable to log out


    def change_password(self):
        if self.current_username is None:
            print("Error: No user is currently logged in.")
            return

        old_password = input("Enter your current password: ").strip()

        # Retrieve the user from the database
        user = self.users_collection.find_one({"username": self.current_username})

        if user is None:
            print("Error: User not found.")
            return

        if user['password'] != old_password:
            print("Error: Incorrect current password.")
            return

        new_password = input("Enter your new password: ").strip()

        try:
            # Update the password in the database
            result = self.users_collection.update_one(
                {"username": self.current_username},
                {"$set": {"password": new_password}}
            )

            if result.modified_count == 1:
                print("Your password has been successfully updated.")
            else:
                print("Error: There was an issue updating your password.")
        except Exception as e:
            print(f"Unexpected error: {e}")


    def change_notifications(self):
        if self.current_username is None:
            print("Error: No user is currently logged in.")
            return

        user = self.users_collection.find_one({"username": self.current_username})

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
        self.users_collection.update_one(
            {"username": self.current_username},
            {"$set": {"notifications": new_notifications}}
        )

        print(f"Your notifications have been successfully updated to '{new_notifications}'.")


    def change_language(self):
        if self.current_username is None:
            print("Error: No user is currently logged in.")
            return

        # Retrieve the user from the database
        user = self.users_collection.find_one({"username": self.current_username})

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
        self.users_collection.update_one(
            {"username": self.current_username},
            {"$set": {"language": new_language}}
        )

        print(f"Your language has been successfully updated to '{new_language}'.")


    def most_used_language(self):
        # Count the number of users with language set to 'eng'
        count_eng = self.users_collection.count_documents({"language": "eng"})
        
        # Count the number of users with language set to 'esp'
        count_esp = self.users_collection.count_documents({"language": "esp"})
        
        # Determine the most used language and the second most used
        if count_eng > count_esp:
            print(f"The most used language is 'eng' with {count_eng} users.")
            print(f"The second most used language is 'esp' with {count_esp} users.")
        elif count_esp > count_eng:
            print(f"The most used language is 'esp' with {count_esp} users.")
            print(f"The second most used language is 'eng' with {count_eng} users.")
        else:
            print(f"Both languages are used equally, with {count_eng} users each.")


    def create_post(self):
        if self.current_username is None:
            print("Error: No user is currently logged in. Please log in first.")
            return

        # Prompt for the post details
        title = input("Enter the title of the post: ").strip()
        text = input("Enter the text of the post: ").strip()

        if not title or not text:
            print("Error: Title and text cannot be empty.")
            return

        # Check if a post with the same title already exists
        existing_post = self.posts_collection.find_one({"title": title})

        if existing_post:
            print(f"Error: A post with the title '{title}' already exists.")
            return

        try:
            # Create the Post object
            post = Post(title=title, text=text, username=self.current_username)

            # Insert the post into the database
            result = self.posts_collection.insert_one(post.to_dict())
            print(f"Post created with ID: {result.inserted_id}")

        except pymongo.errors.DuplicateKeyError:
            # This will catch if another post was inserted with the same title due to race condition
            print(f"Error: A post with the title '{title}' already exists.")
        except Exception as e:
            print(f"Unexpected error while creating the post: {e}")


    def delete_a_post(self):
        if self.current_username is None:
            print("Error: No user is currently logged in. Please log in first.")
            return

        # Prompt the user for the title of the post they want to delete
        title = input("Enter the title of the post you want to delete: ").strip()

        if not title:
            print("Error: Title cannot be empty.")
            return

        # Find the post in the database based on the title
        post = self.posts_collection.find_one({"title": title})

        if post is None:
            print(f"Error: No post found with the title '{title}'.")
            return

        # Verify that the current user is the one who created the post
        if post["username"] != self.current_username:
            print("Error: You can only delete posts that you created.")
            return

        try:
            # Delete the post from the database
            result = self.posts_collection.delete_one({"title": title})

            if result.deleted_count == 1:
                print(f"Post with title '{title}' has been deleted.")
            else:
                print("Error: Failed to delete the post.")
        except Exception as e:
            print(f"Unexpected error while deleting the post: {e}")


    def see_your_posts(self):
        if self.current_username is None:
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
        user_posts = list(self.posts_collection.find({"username": self.current_username}).sort("creation_date", 1).limit(limit))

        # Check if any posts are returned
        if len(user_posts) == 0:
            print("You have no posts.")
            return

        # Display the posts
        print(f"\nPosts created by {self.current_username}:")
        for post in user_posts:
            print(f"\nTitle: {post['title']}")
            print(f"Text: {post['text']}")
            print(f"Created on: {post['creation_date']}")
            print("-" * 40)  # Separator for readability


    def see_posts_from_people(self):
        if self.current_username is None:
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
        posts = list(self.posts_collection.find().sort("creation_date", 1).limit(limit))

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


    def get_list_of_users(self):
        # Query the users_collection and only project the "username" field
        users = list(self.users_collection.find({}, {"_id": 0, "username": 1}))  # Exclude _id, include username
        
        if len(users) == 0:
            print("No users found.")
            return

        # Print all the usernames
        print("\nList of usernames:")
        for user in users:
            print(user['username'])


    def populate_database(self):
        users = [
            User("pablo", "1234"),
            User("pepe", "4321"),
            User("carlos", "1212"),
            User("bot234", "afsl213")
        ]
        posts = [
            Post("Today is full of possibilities", "Ready to take on anything!", users[0].username),
            Post("Coffee and good vibes", "Just what I need to start the day right.", users[3].username),
            Post("Simple things, big moments", "Sometimes the best parts of life are the smallest ones.", users[2].username),
            Post("Learning something new", "It's never too late to start.", users[3].username),
            Post("Let it flow", "When we stop forcing, everything falls into place.", users[2].username),
            Post("One for you, one for me", "Good deeds always come back around.", users[1].username),
            Post("Step by step", "Every small effort gets us closer to the goal.", users[0].username),
            Post("Eyes on the future", "The best is yet to come.", users[3].username),
            Post("A quiet moment to recharge", "Sometimes, pause is the most powerful action.", users[1].username),
            Post("Chasing dreams, not waiting for them", "The journey is what makes it worthwhile.", users[2].username)
        ]

        try:
            for user in users:
                self.users_collection.insert_one(user.to_dict())
        except pymongo.errors.DuplicateKeyError:
            print("Error: A user with this email already exists.")
        except ValueError:
            print("Error: Invalid age. Please try again.")
        except Exception as e:
            print(f"Unexpected error: {e}")
        
        try:
            for post in posts:
                self.posts_collection.insert_one(post.to_dict())
        except pymongo.errors.DuplicateKeyError:
            # This will catch if another post was inserted with the same title due to race condition
            print(f"Error: A post with the title already exists.")
        except Exception as e:
            print(f"Unexpected error while creating the post: {e}")
        
        print("Done")

        
    def clean_database(self):
        # Delete all users and posts from their respective collections
        try:
            # Remove all users
            self.users_collection.delete_many({})
            # Remove all posts
            self.posts_collection.delete_many({})
            
            # Log out the current user
            self.current_username = None
            
            print("Database cleaned and user logged out.")
            
        except Exception as e:
            print(f"An error occurred while cleaning the database: {e}")