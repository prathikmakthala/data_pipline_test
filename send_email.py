import os
import smtplib
import ssl
from email.message import EmailMessage

def send_email():
    host = os.getenv("EMAIL_HOST", "smtp.gmail.com")
    port_str = os.getenv("EMAIL_PORT")
    port = int(port_str) if port_str and port_str.strip() else 465
    user = os.getenv("EMAIL_USER")
    password = os.getenv("EMAIL_PASSWORD")
    recipients = os.getenv("EMAIL_RECIPIENTS")
    
    if not all([user, password, recipients]):
        print("Skipping email: Missing EMAIL_USER, EMAIL_PASSWORD, or EMAIL_RECIPIENTS.")
        return

    subject = os.getenv("EMAIL_SUBJECT", "Nature Counter Pipeline Report")
    body = os.getenv("EMAIL_BODY", "The pipeline has finished running.")

    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = recipients

    context = ssl.create_default_context()

    try:
        with smtplib.SMTP_SSL(host, port, context=context) as server:
            server.login(user, password)
            server.send_message(msg)
        print(f"Email sent to {recipients}")
    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    send_email()
