
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import json

# Sample data
data = [
    {"request_id": "001", "status": "Completed", "type": "Backup", "source_arn": "arn:aws:source1", "target_arn": "arn:aws:target1"},
    {"request_id": "002", "status": "Processing", "type": "Restore", "source_arn": "arn:aws:source2", "target_arn": "arn:aws:target2"},
    # Add more dictionary entries as needed
]

# Prepare the HTML table
html_table = '''
<table border="1" style="border-collapse: collapse; width: 100%; table-layout: fixed;">
    <thead>
        <tr>
            <th style="padding: 8px;">Request ID</th>
            <th style="padding: 8px;">Status</th>
            <th style="padding: 8px;">Type</th>
            <th style="padding: 8px; word-wrap: break-word; width: 300px;">Source ARN</th>
            <th style="padding: 8px; word-wrap: break-word; width: 300px;">Target ARN</th>
        </tr>
    </thead>
    <tbody>
'''

for entry in data:
    html_table += f'''
    <tr>
        <td style="padding: 8px; color: red; word-wrap: break-word;">{entry["request_id"]}</td>
        <td style="padding: 8px;">{entry["status"]}</td>
        <td style="padding: 8px;">{entry["type"]}</td>
        <td style="padding: 8px; word-wrap: break-word;">{entry["source_arn"]}</td>
        <td style="padding: 8px; word-wrap: break-word;">{entry["target_arn"]}</td>
    </tr>
    '''

html_table += '''
    </tbody>
</table>
'''

# Create the email content
email_body = f'''
<html>
<body>
    <p>Hello,</p>
    <p>Please find the details below:</p>
    {html_table}
    <p>Regards,<br>Your Team</p>
</body>
</html>
'''

# More details as JSON for the attachment
more_details = [
    {"request_id": "001", "details": "Extra details about request 001"},
    {"request_id": "002", "details": "Extra details about request 002"},
    # Add more entries as needed
]

# Convert the more details to JSON string
json_data = json.dumps(more_details, indent=4)

# Email setup
from_email = "your_email@example.com"
to_email = "recipient_email@example.com"
subject = "Request Details"

# Create the email message
msg = MIMEMultipart()
msg['From'] = from_email
msg['To'] = to_email
msg['Subject'] = subject

# Attach the HTML body
msg.attach(MIMEText(email_body, 'html'))

# Attach the JSON file
attachment = MIMEBase('application', 'octet-stream')
attachment.set_payload(json_data.encode('utf-8'))
encoders.encode_base64(attachment)
attachment.add_header('Content-Disposition', 'attachment', filename="more_details.json")
msg.attach(attachment)

# Send the email
with smtplib.SMTP('your.smtp.server', 587) as server:
    server.starttls()
    server.login('your_username', 'your_password')
    server.send_message(msg)

print("Email sent successfully!")
