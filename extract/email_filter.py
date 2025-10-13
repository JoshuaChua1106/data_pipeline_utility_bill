def search_emails(service, query):
    """Search Gmail messages using a query and return list of message IDs."""
    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])
    return messages