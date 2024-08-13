import requests
import hashlib
from bs4 import BeautifulSoup
from pymongo import MongoClient, ASCENDING

# Connect to MongoDB
client = MongoClient('mongodb://localhost:56789/')

# Select the database
db = client['pildong_database']

# Select the collection
collection = db['User']

# Create a unique index on the username field
collection.create_index([('username', ASCENDING)], unique=True)

# Function to insert user data
def insert_user(uid, password, username, favorite_player):
    # Check NOT NULL conditions
    if not uid or not password or not username:
        raise ValueError("uid, password, and username cannot be null")
    
    # Hash the password using SHA-512
    hashed_password = hashlib.sha512(password.encode()).hexdigest()
    
    # Create the user document
    user = {
        '_id': uid,  # MongoDB's primary key
        'password': hashed_password,
        'username': username,
        'favorite_player': favorite_player if favorite_player is not None else []
    }
    
    # Insert the user document into the collection
    try:
        collection.insert_one(user)
        print(f"User '{username}' inserted with UID '{uid}'")
    except Exception as e:
        print(f"Error inserting user '{username}': {e}")

try:
    insert_user('uid12345', 'mypassword', 'alice', ['player1', 'player2'])
    insert_user('uid67890', 'anotherpassword', 'bob', ['player3'])
    insert_user('uid00001', 'thirdpassword', 'charlie', [])
    # Attempt to insert a user with a duplicate username
    insert_user('uid00002', 'fourthpassword', 'alice', [])
except ValueError as ve:
    print(f"ValueError: {ve}")
except Exception as e:
    print(f"Exception: {e}")

# Print all documents in the collection
print("\nDocuments in the 'User' collection:")
try:
    documents = collection.find()
    for doc in documents:
        print(doc)
except Exception as e:
    print(f"Error finding documents: {e}")

# Select the collection
event_collection = db['Event']

# Create a unique index on the hash field to prevent duplicate events
event_collection.create_index([('m_code', ASCENDING)], unique=True)

# Function to insert event data
def insert_event(eid, start_time, sports_type, location, teams, league, score, record, m_code):
    # Create the event document
    event = {
        'eid': eid,
        'start_time': start_time,
        'sports_type': sports_type,
        'location': location,
        'teams': teams,
        'league': league,
        'score': score,
        'record': record,  # Ensure record is a list of dictionaries
        'm_code': m_code
    }

    # Insert the event document into the collection
    try:
        collection.insert_one(event)
        print(f"Event '{eid}' inserted with hash '{event['hash']}'")
    except Exception as e:
        print(f"Error inserting event '{eid}': {e}")
