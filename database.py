import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# .env
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb+srv://vergarapablo2001:c9rFOjS2ih4FgRSR@cluster0.5dggz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
DB_NAME = os.getenv('MONGODB_DB_NAME', 'iteso')

try:
    # Try to connect to MongoDB
    client = MongoClient(MONGODB_URI)
    
    # Check the connection
    client.admin.command('ping')  # If successful, it will respond with "ok: 1"
    
    # Successful connection, access the database and collections
    db = client[DB_NAME]
    users_collection = db["users"]
    posts_collection = db["posts"]
    
    # Create indexes
    users_collection.create_index("username", unique=True)
    posts_collection.create_index("title", unique=True)

    print("MongoDB connection successful.")
    
except ConnectionFailure:
    print("Connection error to MongoDB. Cannot continue.")
    exit(1)  # Stop the script if it cannot connect
except Exception as e:
    print(f"Unexpected error: {e}")
    exit(1)  # Stop the script if any other error occurs
