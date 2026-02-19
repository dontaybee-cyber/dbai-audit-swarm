import os
import random
import time
import requests
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from typing import Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
import streamlit as st
import ui_manager as ui
import swarm_config

load_dotenv()

SENDER_NAME = os.getenv("SENDER_NAME", "The DBAI Team")
DBAI_LANDING_PAGE = "https://digitaldontaybeemon.dashnexpages.net/ai-automation-consultant-custom-ai-systems-workflow-audits/"
DBAI_PHONE = "(720) 316-8360"
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

def generate_dynamic_email(url: str, pain_point_summary: str, profile: dict) -> str:
    """Generates a unique email body using Spintax and a client profile."""
    greetings = ["Hi there,", "Hello,", "Hey,", "Greetings,"]
    openers = [
        "I was just taking a look at your site",
        "I came across your website",
        "I was reviewing your online presence",
        "My team was just looking at your site"
    ]
    transitions = [
        "and I noticed a quick win.",
        "and wanted to share an observation.",
        "and spotted a massive area for optimization.",
        "and wanted to drop a quick note."
    ]
    sign_offs = ["Best,", "Cheers,", "Regards,", "Talk soon,"]

    # Select random phrases
    greeting = random.choice(greetings)
    opener = random.choice(openers)
    transition = random.choice(transitions)
    sign_off = random.choice(sign_offs)
    
    # Construct the dynamic email body
    body = f"""{greeting}

{opener} at {url} {transition}

{pain_point_summary}

I've attached a custom strategic briefing (sample_audit.pdf) showing exactly how {profile['company_name']} can plug this leak.

Let's chat: {DBAI_PHONE}
Trust link: {profile['trust_link']}

{sign_off}
{SENDER_NAME}
"""
    return body

def send_sniper_email(recipient_email: str, url: str, pain_point_summary: str, profile: dict) -> Tuple[bool, bool]:
    """
    Send a personalized 'sniper' email with a PDF attachment.
    
    Args:
        recipient_email: Target email address
        url: Website URL being audited
        pain_point_summary: The identified business pain point and ROI projection.
        profile: The client profile for personalizing the email.
    
    Returns:
        A tuple (email_sent_successfully, pdf_attached_successfully)
    """

    # Fetch directly from Streamlit Cloud Secrets, fallback to .env for local
    try:
        email_user = st.secrets.get("EMAIL_USER", os.getenv("EMAIL_USER"))
        email_pass = st.secrets.get("EMAIL_PASS", os.getenv("EMAIL_PASS"))
    except Exception:
        email_user = os.getenv("EMAIL_USER")
        email_pass = os.getenv("EMAIL_PASS")

    if not email_user or not email_pass:
        ui.log_error("CRITICAL: Email credentials missing from secure vault.")
        return False, False

    # Strip alias for Google Server login, but keep alias for the 'From' address
    login_email = email_user
    if '+' in email_user and '@' in email_user:
        user_part, domain = email_user.split('@')
        base_user = user_part.split('+')[0]
        login_email = f"{base_user}@{domain}"

    subject = f"A specific idea for {url.replace('https://', '').replace('http://', '').split('/')[0]}"
    
    # Generate the dynamic email body
    body = generate_dynamic_email(url, pain_point_summary, profile)
    
    pdf_attached = False
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = email_user
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        # Attach PDF
        pdf_path = "sample_audit.pdf"
        try:
            with open(pdf_path, "rb") as attachment:
                part = MIMEApplication(attachment.read(), _subtype="pdf")
                part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(pdf_path))
                msg.attach(part)
                pdf_attached = True
                ui.log_sniper(f"Successfully attached {pdf_path}")
        except FileNotFoundError:
            ui.log_error(f"CRITICAL: The file {pdf_path} was not found. Email will be sent without the attachment.")
            pdf_attached = False

        # Connect to Gmail SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(login_email, email_pass)
        
        # Send email
        server.send_message(msg)
        server.quit()
        
        ui.log_success(f"Email sent to {recipient_email} | PDF Attached: {pdf_attached}")
        return True, pdf_attached
        
    except smtplib.SMTPAuthenticationError as e:
        ui.log_error(f"Google SMTP Auth Rejected: {e.smtp_code} - {e.smtp_error}")
        return False, False
    except Exception as e:
        ui.log_error(f"Email dispatch failure: {e}")
        return False, False

def enrich_email_with_hunter(domain: str) -> Optional[str]:
    """Try to find a contact email for a domain using Hunter.io Domain Search API."""
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
        for e in emails:
            if e.get("value"):
                ui.log_success(f"Hunter found email: {e.get('value')}")
                return e.get("value")
        return None
    except Exception as e:
        ui.log_warning(f"Hunter enrichment failed for {domain}: {e}")
        return None

def main(client_key: str):
    ui.SwarmHeader.display()
    ui.log_sniper("Sniper Agent starting...")
    
    audits_file = f"audits_to_send_{client_key}.csv"
    
    if not os.path.exists(audits_file):
        ui.log_error(f"{audits_file} not found in current directory.")
        return
    
    audits_df = pd.read_csv(audits_file, on_bad_lines='skip')
    
    required_columns = ["Status", "URL", "Pain_Point_Summary"]
    if not all(col in audits_df.columns for col in required_columns):
        ui.log_error(f"{audits_file} must contain these columns: {', '.join(required_columns)}")
        return
    
    if "Email" not in audits_df.columns:
        ui.log_error("EMAIL COLUMN MISSING - ACTION REQUIRED:")
        ui.log_info("To send sniper emails, you need contact emails for each lead.")
        return
    
    if "Audit Attached" not in audits_df.columns:
        audits_df["Audit Attached"] = False
        
    pending_audits = audits_df[audits_df["Status"].str.lower() == "analyzed"]
    
    if pending_audits.empty:
        ui.log_success("No pending audits to send. All leads have been contacted.")
        return
    
    ui.log_sniper(f"Found {len(pending_audits)} audits ready to send in {audits_file}.")
    
    emailed_this_session = set()
    sent_history = set()
    if "Status" in audits_df.columns and "Email" in audits_df.columns:
        sent_rows = audits_df[audits_df["Status"] == "Sent"]
        for _, row in sent_rows.iterrows():
            email = str(row.get("Email", "")).strip().lower()
            if email and email != "nan":
                sent_history.add(email)

    sent_count = 0
    audits_generated = 0

    profile = swarm_config.CLIENT_PROFILES.get(client_key, swarm_config.CLIENT_PROFILES["default"])
    ui.log_sniper(f"Activating Chameleon Agent Profile: {profile['company_name']}")
    
    for idx, row in ui.track(pending_audits.iterrows(), total=len(pending_audits), description="[sniper]Generating Audits & Sending Emails...[/sniper]"):
        try:
            url = row.get("URL")
            if pd.isna(url) or not isinstance(url, str):
                ui.log_warning(f"Skipping row {idx}: Invalid URL")
                continue

            pain_point_summary = row.get("Pain_Point_Summary")
            recipient_email = row.get("Email")
            
            if not recipient_email or pd.isna(recipient_email):
                domain = url.replace('https://', '').replace('http://', '').split('/')[0]
                
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
                    audits_df.at[idx, "Status"] = "Dead End - No Email"
                    continue
            
            current_email_lower = str(recipient_email).strip().lower()
            if current_email_lower in emailed_this_session:
                ui.log_warning(f"Skipping {recipient_email} - Already emailed this session.")
                continue
            if current_email_lower in sent_history:
                ui.log_warning(f"Skipping {recipient_email} - Already marked as Sent in history.")
                audits_df.at[idx, "Status"] = "Skipped - Previously Sent"
                continue

            sent, attached = send_sniper_email(recipient_email, url, pain_point_summary, profile)
            
            if sent:
                audits_df.at[idx, "Status"] = "Sent"
                audits_df.at[idx, "Sent Date"] = datetime.now().strftime("%Y-%m-%d")
                audits_df.at[idx, "Audit Attached"] = attached
                sent_count += 1
                if attached:
                    audits_generated += 1
                emailed_this_session.add(current_email_lower)
                
                time.sleep(random.randint(30, 60))
            else:
                ui.log_warning(f"Failed to send email for {url}")
                audits_df.at[idx, "Status"] = "Send Failed"
        
        except Exception as e:
            ui.log_error(f"Unexpected error processing row {idx}: {e}")
            audits_df.at[idx, "Status"] = "Error"
    
    audits_df.to_csv(audits_file, index=False)
    
    if sent_count > 0:
        ui.display_dashboard(emails_sent=sent_count, audits_generated=audits_generated)
        ui.log_success(f"Successfully sent {sent_count} sniper emails!")
        ui.log_success(f"Generated and attached {audits_generated} audits.")
        ui.log_success(f"Updated {audits_file} with new statuses.")
    else:
        ui.log_info("No new emails sent in this session. Check logs for details.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sniper Agent - Email Outreach")
    parser.add_argument("--client_key", required=True, help="Client key for isolating data files.")
    args = parser.parse_args()
    main(args.client_key)
