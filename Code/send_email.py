import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os

load_dotenv()

class EmailSender:
    def __init__(self):
        self.sender = os.getenv('sender')
        self.password = os.getenv('password')
        self.receiver = os.getenv('receiver')
        self.smtp_server = os.getenv('server')
        self.smtp_port = int(os.getenv('port'))

    def send_email(self, subject, body):
        msg = MIMEMultipart()
        msg['From'] = self.sender
        msg['To'] = self.receiver
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender, self.password)
                server.send_message(msg)
                print("Email sent successfully!")
                return True
        except Exception as e:
            print(f"Failed to send email: {e}")
            return False

# Example Usage
if __name__ == "__main__":
    email = EmailSender()
    email.send_email(
        subject="Eftaah el bab ya hassannnn",
        body="bab3tlk mn el python code yaam sahla"
    )