import os
import smtplib
import ssl
import re
from email.message import EmailMessage
from datetime import datetime

def parse_pipeline_log(log_path="pipeline.log"):
    """Parse pipeline log to extract metrics"""
    metrics = {
        "status": "UNKNOWN",
        "new_records": 0,
        "total_records": 0,
        "watermark": None,
        "duration": None,
        "errors": []
    }
    
    if not os.path.exists(log_path):
        return metrics
    
    with open(log_path, "r") as f:
        log_content = f.read()
    
    # Check for success markers
    if "✅ Uploaded" in log_content:
        metrics["status"] = "SUCCESS"
    elif "No new data" in log_content:
        metrics["status"] = "NO_UPDATES"
    elif "ERROR" in log_content or "Traceback" in log_content:
        metrics["status"] = "FAILED"
    
    # Extract record counts
    uploaded_match = re.search(r"✅ Uploaded.*\((\d+) rows\)", log_content)
    if uploaded_match:
        metrics["total_records"] = int(uploaded_match.group(1))
    
    fetched_match = re.search(r"Fetched (\d+) new records", log_content)
    if fetched_match:
        metrics["new_records"] = int(fetched_match.group(1))
    
    # Extract watermark
    watermark_match = re.search(r"💾 Saved watermark.*: ([a-f0-9]+)", log_content)
    if watermark_match:
        metrics["watermark"] = watermark_match.group(1)
    
    # Extract duration
    duration_match = re.search(r"Pipeline completed in ([\d.]+) seconds", log_content)
    if duration_match:
        metrics["duration"] = float(duration_match.group(1))
    
    # Extract errors
    error_lines = [line for line in log_content.split("\n") if "ERROR" in line or "CRITICAL" in line]
    metrics["errors"] = error_lines[:5]  # First 5 errors
    
    return metrics

def format_email_body(metrics):
    """Format a nice email body from metrics"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # Status emoji
    status_emoji = {
        "SUCCESS": "✅",
        "NO_UPDATES": "ℹ️",
        "FAILED": "❌",
        "UNKNOWN": "⚠️"
    }
    
    emoji = status_emoji.get(metrics["status"], "⚠️")
    
    body = f"""Nature Counter Pipeline Report
{'=' * 50}

{emoji} Status: {metrics["status"]}
Run Time: {timestamp}
"""
    
    if metrics["duration"]:
        body += f"Duration: {metrics['duration']:.2f} seconds\n"
    
    body += "\n"
    
    # Records section
    if metrics["status"] == "SUCCESS":
        body += f"""Records Update:
  • New records fetched: {metrics['new_records']}
  • Total records in file: {metrics['total_records']}
"""
        if metrics["watermark"]:
            body += f"  • Last processed ID: {metrics['watermark'][:12]}...\n"
    
    elif metrics["status"] == "NO_UPDATES":
        body += """Records Update:
  • No new records found in MongoDB
  • All data is up to date
  • Next run will check for new entries
"""
    
    elif metrics["status"] == "FAILED":
        body += f"""Pipeline encountered errors!

"""
        if metrics["errors"]:
            body += "Error Details:\n"
            for error in metrics["errors"]:
                body += f"  {error}\n"
        body += "\n⚠️ Please check the full logs in GitHub Actions\n"
    
    body += f"""
{'=' * 50}
Dashboard: https://github.com/prathikmakthala/data_pipline_test/actions
"""
    
    return body

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
    
    # Parse pipeline metrics
    metrics = parse_pipeline_log("pipeline.log")
    
    # Generate subject based on status
    status_text = metrics["status"].replace("_", " ").title()
    subject = f"[{status_text}] Nature Counter Pipeline - {datetime.now().strftime('%Y-%m-%d')}"
    
    # Generate body
    body = format_email_body(metrics)

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
        print(f"✅ Email sent to {recipients}")
        print(f"   Status: {metrics['status']}")
        print(f"   New Records: {metrics['new_records']}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

if __name__ == "__main__":
    send_email()

