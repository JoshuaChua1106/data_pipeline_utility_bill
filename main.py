import os
from dotenv import load_dotenv

from googleapiclient.errors import HttpError

from extract.gmail_connector import connect_gmail

# Load .env file
load_dotenv()


# Stage 1: Connect to Gmail
credentials_path = os.getenv("GMAIL_CREDENTIALS_PATH")
token_path = os.getenv("GMAIL_TOKEN_PATH")

service = connect_gmail(credentials_file=credentials_path, token_file=token_path)

# Verify if connection is successful
try:
    # Simple test: get your Gmail profile
    profile = service.users().getProfile(userId='me').execute()
    print("Connection successful!")
    print("Email address:", profile.get('emailAddress'))
    
    # Optional: list first 5 messages
    messages = service.users().messages().list(userId='me', maxResults=5).execute()
    print(f"Found {len(messages.get('messages', []))} messages")
    
except HttpError as error:
    print(f"An error occurred: {error}")