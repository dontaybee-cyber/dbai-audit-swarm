import os
import random
import time
import requests
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
import ui_manager as ui

load_dotenv()

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
SENDER_NAME = os.getenv("SENDER_NAME", "Scout Agent Team")
DBAI_AUDIT_LINK = "https://dbai-audit-suite.vercel.app/"
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")

"""
===========================================
HOW TO GENERATE A GMAIL APP PASSWORD:
===========================================

1. Go to your Google Account: https://myaccount.google.com/
2. Enable 2-Factor Authentication (if not already enabled)
3. Navigate to: Security > App passwords
4. Select: Mail > Windows Computer (or your device)
5. Google will generate a 16-character password
6. Copy this password and add it to your .env file as EMAIL_PASS
7. Use your full Gmail address (you@gmail.com) as EMAIL_USER

NOTE: Regular Gmail passwords DO NOT work with this script.
      You MUST use an App Password for security.
      Gmail no longer allows "Less secure app access".

===========================================
"""


def send_sniper_email(recipient_email: str, url: str, pain_point: str) -> bool:
    """
    Send a personalized 'sniper' email with pattern interrupt subject line.
    
    Args:
        recipient_email: Target email address
        url: Website URL being audited
        pain_point: The identified business pain point
    
    Returns:
        True if email sent successfully, False otherwise
    """
    
    if not EMAIL_USER or not EMAIL_PASS:
        ui.log_error("EMAIL_USER or EMAIL_PASS not configured in .env file")
        return False
    
    # Pattern Interrupt Subject Line
    subject = f"Question about {url.replace('https://', '').replace('http://', '').split('/')[0]}'s lead flow"
    
    # Email Body - Helpful, Not Salesy
    body = f"""Hi there,

I was reviewing {url} and noticed something interesting:

{pain_point}

This is costing you real money every single day. I put together a full AI roadmap to fix this and completely automate your lead capture process.

Check it out here: {DBAI_AUDIT_LINK}

You'll see exactly how to eliminate this leak and scale your business without adding manual overhead.

Looking forward to connecting,
{SENDER_NAME}

P.S. This audit is free. You'll get concrete, actionable steps to implement immediately.
"""
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        # Connect to Gmail SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        
        # Send email
        server.send_message(msg)
        server.quit()
        
        ui.log_success(f"Email sent to {recipient_email} | URL: {url}")
        return True
        
    except smtplib.SMTPAuthenticationError:
        ui.log_error(f"Authentication failed. Check EMAIL_USER and EMAIL_PASS. Did you use an App Password?")
        return False
    except smtplib.SMTPException as e:
        ui.log_error(f"SMTP error sending to {recipient_email}: {e}")
        return False
    except Exception as e:
        ui.log_error(f"Failed to send email to {recipient_email}: {e}")
        return False


def enrich_email_with_hunter(domain: str) -> Optional[str]:
    """Try to find a contact email for a domain using Hunter.io Domain Search API.

    Returns the first discovered email address or None on failure/no results.
    """
    if not HUNTER_API_KEY:
        return None
    try:
        ui.log_sniper(f"Querying Hunter.io for domain: {domain}")
        url = "https://api.hunter.io/v2/domain-search"
        params = {"domain": domain, "api_key": HUNTER_API_KEY}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        emails = data.get("data", {}).get("emails", [])
        if not emails:
            ui.log_warning(f"Hunter found no emails for {domain}")
            return None
        # Prefer corporate/verified emails if available
        for e in emails:
            if e.get("value"):
                ui.log_success(f"Hunter found email: {e.get('value')}")
                return e.get("value")
        return None
    except Exception as e:
        ui.log_warning(f"Hunter enrichment failed for {domain}: {e}")
        return None


def main():
    ui.SwarmHeader.display()
    ui.log_sniper("Sniper Agent starting...")
    
    if not os.path.exists("audits_to_send.csv"):
        ui.log_error("audits_to_send.csv not found in current directory.")
        return
    
    audits_df = pd.read_csv("audits_to_send.csv", on_bad_lines='skip')
    
    required_columns = ["Status", "URL", "Pain Point"]
    if not all(col in audits_df.columns for col in required_columns):
        ui.log_error(f"audits_to_send.csv must contain these columns: {', '.join(required_columns)}")
        return
    
    # Check if Email column exists - if not, provide instructions
    if "Email" not in audits_df.columns:
        ui.log_error("EMAIL COLUMN MISSING - ACTION REQUIRED:")
        ui.log_info("To send sniper emails, you need contact emails for each lead.")
        ui.log_info("OPTION 1: Use Hunter.io (Email Finder API)")
        ui.log_info("OPTION 2: Use RocketReach")
        ui.log_info("OPTION 3: Manual Enrichment")
        return
    
    # Filter only 'Analyzed' status rows
    pending_audits = audits_df[audits_df["Status"].str.lower() == "analyzed"]
    
    if pending_audits.empty:
        ui.log_success("No pending audits to send. All leads have been contacted.")
        return
    
    ui.log_sniper(f"Found {len(pending_audits)} audits ready to send.")
    
    # Task 1: Initialize duplicate tracking
    emailed_this_session = set()
    
    # Task 1: Load historical sent emails to prevent duplicates
    sent_history = set()
    if "Status" in audits_df.columns and "Email" in audits_df.columns:
        sent_rows = audits_df[audits_df["Status"] == "Sent"]
        for _, row in sent_rows.iterrows():
            email = str(row.get("Email", "")).strip().lower()
            if email and email != "nan":
                sent_history.add(email)

    sent_count = 0
    
    # Use UI track for progress bar
    for idx, row in ui.track(pending_audits.iterrows(), total=len(pending_audits), description="[sniper]Sending Emails...[/sniper]"):
        try:
            url = row.get("URL")
            if pd.isna(url) or not isinstance(url, str):
                ui.log_warning(f"Skipping row {idx}: Invalid URL")
                continue

            pain_point = row.get("Pain Point")
            recipient_email = row.get("Email")
            
            if not recipient_email or pd.isna(recipient_email):
                domain = url.replace('https://', '').replace('http://', '').split('/')[0]
                
                # Attempt Hunter enrichment
                if HUNTER_API_KEY:
                    ui.log_sniper(f"Attempting Hunter enrichment for {domain}")
                    found = enrich_email_with_hunter(domain)
                    time.sleep(1)
                    if found:
                        recipient_email = found
                        audits_df.at[idx, "Email"] = found
                        ui.log_success(f"Enriched email for {domain} via Hunter: {found}")
                if not recipient_email or pd.isna(recipient_email):
                    ui.log_warning(f"No contact email found for {domain}. Skipping {url}.")
                    continue
            
            # Task 1: Duplicate Checks
            current_email_lower = str(recipient_email).strip().lower()
            if current_email_lower in emailed_this_session:
                ui.log_warning(f"Skipping {recipient_email} - Already emailed this session.")
                continue
            if current_email_lower in sent_history:
                ui.log_warning(f"Skipping {recipient_email} - Already marked as Sent in history.")
                continue

            # Send the sniper email
            if send_sniper_email(recipient_email, url, pain_point):
                audits_df.at[idx, "Status"] = "Sent"
                audits_df.at[idx, "Sent Date"] = datetime.now().strftime("%Y-%m-%d")
                sent_count += 1
                emailed_this_session.add(current_email_lower)
                
                # Safety measure: avoid spam filters
                # Don't send more than 5 emails per hour
                time.sleep(random.randint(30, 60))
            else:
                ui.log_warning(f"Failed to send email for {url}")
        
        except Exception as e:
            ui.log_error(f"Unexpected error processing row {idx}: {e}")
    
    # Save updated CSV
    if sent_count > 0:
        audits_df.to_csv("audits_to_send.csv", index=False)
        ui.display_dashboard(emails_sent=sent_count)
        ui.log_success(f"Successfully sent {sent_count} sniper emails!")
        ui.log_success(f"Updated audits_to_send.csv status to 'Sent'")
    else:
        ui.log_info("No emails sent. Check configuration and contact data.")


if __name__ == "__main__":
    main()
