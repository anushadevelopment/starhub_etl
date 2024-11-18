# import win32com.client
import re
import datetime
from pathlib import Path
def extract():
    # Create Output folder if it doesn't exist
    output_dir = Path.cwd() / "Data"
    output_dir.mkdir(parents=True, exist_ok=True)    

    # Connect to Outlook
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")

    # Connect to folder (make sure the account is correctly specified)
    inbox = outlook.Folders("anustarhub@gmail.com").Folders("Inbox")
    print(inbox)

    if inbox.Items.Count > 0:
        print(f"Found {inbox.Items.Count} messages in the inbox.")
        messages = inbox.Items
    else:
        print("no items in the inbox")
        
    print(messages)
    for message in messages:
        print('success')
        subject = message.Subject
        body = message.Body  
        attachments = message.Attachments

        # Create a folder for each message based on the subject and current timestamp
        current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        target_folder = output_dir / re.sub('[^0-9a-zA-Z]+', '', subject) / current_time
        target_folder.mkdir(parents=True, exist_ok=True)

        # Write body to text file
        Path(target_folder / "EMAIL_BODY.txt").write_text(str(body))

        # Save attachments and exclude special characters
        for attachment in attachments:
            # Use raw string or escape the backslash correctly to avoid syntax warnings
            filename = re.sub(r'[^0-9a-zA-Z\.]+', '', attachment.FileName)  # Using raw string (r'...')
            attachment.SaveAsFile(target_folder / filename)
    
    
        
        
        
        
        
        
        
        
  