import logging
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import settings

logger = logging.getLogger(__name__)

SMTP_SERVER = settings.smtp_server  # e.g., smtp.mailgun.org or smtp.sendgrid.net
SMTP_PORT = settings.smtp_port                 # Use 587 for STARTTLS
SMTP_USERNAME = settings.smtp_username
SMTP_PASSWORD = settings.smtp_password  # Never use your real password; use an App Password
SENDER_EMAIL = settings.sender_email
APP_URL = settings.app_url

class EmailService:
    def __init__(self):
        self.server = SMTP_SERVER
        self.port = int(SMTP_PORT)
        self.username = SMTP_USERNAME
        self.password = SMTP_PASSWORD

    def send_pin_reset_email(self, recipient_email: str, new_pin: str):
        """
        Constructs and sends a PIN update email via SMTP.
        """
        # 1. Create the message container
        message = MIMEMultipart("alternative")
        message["Subject"] = "Your Secure PIN has been updated"
        message["From"] = SENDER_EMAIL
        message["To"] = recipient_email

        # 2. Create the body (Plain text and HTML)
        text = f"Hello,\n\nYour new security PIN is: {new_pin}\n\nIf you did not request this, please contact support."
        html = f"""
        <html>
            <body>
                <h2>Security Update</h2>
                <p>Hello,</p>
                <p>Your new security PIN is: <strong>{new_pin}</strong></p>
                <p style="color: red;">If you did not request this, please contact support immediately.</p>
            </body>
        </html>
        """

        message.attach(MIMEText(text, "plain"))
        message.attach(MIMEText(html, "html"))

        # 3. Send the email
        # We use a context manager to ensure the connection is closed
        context = ssl.create_default_context()
        
        try:
            with smtplib.SMTP(self.server, self.port) as server:
                server.starttls(context=context) # Secure the connection
                server.login(self.username, self.password)
                server.sendmail(SENDER_EMAIL, recipient_email, message.as_string())
            logger.info(f"Email sent successfully to {recipient_email}")
        except Exception as e:
            logger.error(f"Failed to send email: {e}")

    def send_password_reset_email(self, recipient_email: str, reset_token: str):
        """
        Constructs and sends a Password Reset email via SMTP.
        Note: Adapted from your PIN update template to handle reset tokens.
        """
        # 1. Create the message container
        message = MIMEMultipart("alternative")
        message["Subject"] = "Password Reset Request"
        message["From"] = SENDER_EMAIL
        message["To"] = recipient_email

        # 2. Create the body (Plain text and HTML)
        # Using a link for the token is standard for password resets
        reset_link = f"https://{APP_URL}/reset?token={reset_token}"
        
        text = f"Hello,\n\nPlease use the following link to reset your password: {reset_link}\n\nIf you did not request this, please contact support."
        html = f"""
        <html>
            <body>
                <h2 style="color: #333;">Security Update</h2>
                <p>Hello,</p>
                <p>We received a request to reset your password. Click the button below to proceed:</p>
                <p><a href="{reset_link}" style="background-color: #4CAF50; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">Reset Password</a></p>
                <p>Or use this token directly: <strong>{reset_token}</strong></p>
                <p style="color: red; font-size: 0.8em;">If you did not request this, please contact support immediately.</p>
            </body>
        </html>
        """

        message.attach(MIMEText(text, "plain"))
        message.attach(MIMEText(html, "html"))

        # 3. Send the email
        context = ssl.create_default_context()
        
        try:
            with smtplib.SMTP(self.server, self.port) as server:
                server.starttls(context=context) 
                server.login(self.username, self.password)
                server.sendmail(SENDER_EMAIL, recipient_email, message.as_string())
            logger.info(f"Reset email sent successfully to {recipient_email}")
        except Exception as e:
            logger.error(f"Failed to send email: {e}")