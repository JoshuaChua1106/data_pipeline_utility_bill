import os
import yaml
from dotenv import load_dotenv
from pathlib import Path

from googleapiclient.errors import HttpError

from extract.gmail_connector import connect_gmail
from extract.email_filter import search_emails


# Configuration
load_dotenv()
BASE_DIR = Path(__file__).resolve().parent
with open(BASE_DIR / "config/config.yaml") as f:
    config = yaml.safe_load(f)



# Stage 1: Connect to Gmail
# Get the path string from .env
credentials_relative = os.getenv("GMAIL_CREDENTIALS_PATH")
token_relative = os.getenv("GMAIL_TOKEN_PATH")

# Combine with BASE_DIR to get absolute paths
credentials_path = BASE_DIR / credentials_relative
token_path = BASE_DIR / token_relative

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

# Step 2: Filter for relevant emails

elec_search_query = config["gmail_queries"]["elec"]
water_search_query = config["gmail_queries"]["water"]
gas_search_query = config["gmail_queries"]["gas"]

elec_emails = search_emails(service, elec_search_query)
water_emails = search_emails(service, water_search_query)
gas_emails = search_emails(service, gas_search_query)