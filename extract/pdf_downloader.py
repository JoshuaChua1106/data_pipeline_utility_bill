import os
import base64

def download_pdf_attachments(service, messages, save_folder='downloads'):
    """Download all PDF attachments from a list of messages, skipping existing files."""
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    for msg in messages:
        msg_id = msg['id']
        message = service.users().messages().get(userId='me', id=msg_id).execute()
        parts = message.get('payload', {}).get('parts', [])
        for part in parts:
            filename = part.get('filename')
            if filename and filename.lower().endswith('.pdf'):
                file_path = os.path.join(save_folder, filename)
                
                # Check if file already exists
                if os.path.exists(file_path):
                    print(f"Skipped (already exists): {filename}")
                    continue
                
                body = part.get('body', {})
                attachment_id = body.get('attachmentId')
                if attachment_id:
                    attachment = service.users().messages().attachments().get(
                        userId='me', messageId=msg_id, id=attachment_id
                    ).execute()
                    file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                    with open(file_path, 'wb') as f:
                        f.write(file_data)
                    print(f"Downloaded: {filename}")