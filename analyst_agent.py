import os
import random
import re
import time
from typing import Optional, Tuple, Dict
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import ui_manager as ui
import swarm_config
from serpapi import GoogleSearch
import streamlit as st
import cloud_storage

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")

try:
    import google.genai as genai
    genai_available = True
    try:
        genai.configure(api_key=API_KEY)
    except Exception:
        # some installs expose a different configure API; ignore here and rely on calls
        pass
except Exception:
    genai_available = False

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
]

def fetch_site_text(url: str, timeout: int = 15, retries: int = 1) -> Tuple[Optional[str], Dict[str, str]]:
    ui.log_analyst(f"Fetching site text for: {url}")
    socials = {"Contact_Page": None}
    
    for attempt in range(retries + 1):
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        try:
            resp = requests.get(url, timeout=timeout, headers=headers)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            
            mailtos = [a["href"].replace("mailto:", "") for a in soup.select('a[href^="mailto:"]')]
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                lower_href = href.lower()
                
                if "facebook.com" in lower_href and "sharer" not in lower_href:
                    socials["Facebook"] = href
                elif "linkedin.com" in lower_href and "share" not in lower_href:
                    socials["LinkedIn"] = href
                elif "instagram.com" in lower_href:
                    socials["Instagram"] = href
                elif "twitter.com" in lower_href or "x.com" in lower_href:
                    socials["Twitter"] = href
                
                if "contact" in lower_href and not socials.get("Contact_Page"):
                    socials["Contact_Page"] = urljoin(url, href)

            text = soup.get_text(separator=" ", strip=True)
            if not text:
                ui.log_warning(f"No text content found for {url}")
                return None, socials
            if mailtos:
                text += " " + " ".join(mailtos)
            ui.log_analyst(f"Successfully fetched {len(text)} characters")
            return text[:4000], socials
        except Exception as e:
            if attempt < retries:
                ui.log_warning(f"Attempt {attempt+1} failed for {url}: {e}. Retrying...")
                time.sleep(2)
            else:
                ui.log_warning(f"Failed to fetch {url} after {retries+1} attempts: {e}")
                return None, socials

def analyze_with_gemini(site_dna: str, profile: dict) -> Optional[str]:
    system_instruction = (
        f"You are a top-tier {profile['industry']} analyzing a local business's website from the provided text below."
        "Your task is to identify the most significant 'Revenue Leak'—a clear inefficiency where the business is losing money."
        f"Scan for these specific weaknesses: {profile['target_pain_point']}."
        "Based on the single most critical weakness you find, perform two actions:"
        "1. Calculate a realistic 'Projected ROI' figure if they were to automate this gap. Frame it as an annual projection."
        "   - Example ROI Calculation: If a business gets 100 visitors/day and a chatbot could convert 2% of them into leads valued at $50 each, the projected ROI would be (100 * 0.02 * $50 * 365) = $36,500/year."
        "2. Synthesize your finding and the ROI into a single, hard-hitting sentence for a cold email."
        "   - Format: '[Identified Weakness], potentially losing you an estimated [Projected ROI] annually.'"
        "   - Example Output: 'I noticed your site lacks an automated chat system, potentially losing you an estimated $36,500 annually from missed after-hours leads.'"
        "CRUCIAL: Output only this single sentence. Nothing else."
    )
    prompt = f"{system_instruction}\n\nWebsite Text:\n{site_dna}"
    try:
        if not genai_available:
            ui.log_warning("GenAI not available, skipping Gemini analysis.")
            return None
        try:
            ui.log_analyst("Sending prompt to Gemini...")
            model = genai.GenerativeModel('gemini-1.5-flash-latest')
            response = model.generate_content(prompt)
            text = response.text if hasattr(response, 'text') else str(response)
            ui.log_success("Gemini response received.")
            return text.strip().splitlines()[0]
        except Exception as e:
            ui.log_warning(f"Gemini generation failed: {e}")
            return None
    except Exception as e:
        ui.log_warning(f"Gemini API call failed: {e}")
        return None

def heuristic_analysis(site_dna: str) -> str:
    ui.log_analyst("Running heuristic analysis...")
    s = site_dna.lower()
    if "contact" not in s and "contact" not in s[:200]:
        ui.log_analyst("Heuristic triggered: No visible lead-capture form.")
        return "Your website has no visible lead-capture form on the homepage, potentially losing you an estimated $15,000 annually from missed conversion opportunities."
    if "book" in s and ("online" not in s and "book now" not in s):
        ui.log_analyst("Heuristic triggered: Manual booking process.")
        return "Your site appears to use a manual booking process, potentially losing you an estimated $25,000 annually from customers who expect instant online scheduling."
    if "support" in s and ("chat" not in s and "help" in s):
        ui.log_analyst("Heuristic triggered: Outdated support flow.")
        return "Your support page lacks an instant AI chat, potentially losing you an estimated $20,000 annually from unresolved customer questions."
    ui.log_analyst("Heuristic triggered: Default fallback.")
    return "Your website lacks a clear, instant lead-capture mechanism, potentially losing you an estimated $18,000 annually from missed opportunities."

def extract_email_from_text(text: str) -> Optional[str]:
    email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
    matches = re.findall(email_pattern, text)
    
    # Massive blocklist for junk, placeholders, and web builders
    ignore_terms = [
        'sentry', 'no-reply', 'noreply', 'example', 'domain', 'email', 'username', 
        'user', 'test', 'wix', 'squarespace', 'wordpress', 'name@', 'yourname', 
        'yourdomain', 'admin@example', 'john@doe', 'jane@doe', 'sitelink', 
        'theme', 'demo', 'placeholder', '12345'
    ]
    ignore_exts = ('.png', '.jpg', '.jpeg', '.gif', '.css', '.js', '.svg', '.woff', '.woff2', '.ttf', '.webp', '.mp4', '.mp3')
    valid_emails = []
    if matches:
        for email in matches:
            lower_email = email.lower().strip()
            lower_email = lower_email.rstrip('.') # Clean trailing dots
            
            if any(term in lower_email for term in ignore_terms):
                continue
            if lower_email.endswith(ignore_exts):
                continue
            if len(lower_email) < 6 or len(lower_email) > 80:
                continue
            
            if lower_email not in valid_emails:
                valid_emails.append(lower_email)
        
    if not valid_emails:
        return None
    
    # Prioritize core business inboxes over obscure developer/employee emails
    priorities = ['info@', 'contact@', 'sales@', 'hello@', 'office@', 'admin@', 'support@', 'estimate@']
    for e in valid_emails:
        if any(e.startswith(p) for p in priorities):
            return e
            
    # Fallback to the first valid email found
    return valid_emails[0]

def hunt_email_via_google(domain: str) -> Optional[str]:
    """Uses SerpAPI to search Google for the company's contact info."""
    api_key = st.secrets.get("SERP_API_KEY", os.getenv("SERP_API_KEY"))
    if not api_key: return None
    try:
        q = f'"{domain}" contact OR email OR @'
        search = GoogleSearch({"engine": "google", "q": q, "api_key": api_key, "num": 10})
        results = search.get_dict()
        snippets = " ".join([res.get("snippet", "") for res in results.get("organic_results", [])])
        return extract_email_from_text(snippets)
    except Exception:
        return None

def main(client_key: str):
    ui.SwarmHeader.display()
    ui.log_analyst("Analyst Agent starting...")

    leads_file = f"leads_queue_{client_key}.csv"
    audits_file = f"audits_to_send_{client_key}.csv"

    cloud_storage.sync_down(leads_file)
    cloud_storage.sync_down(audits_file)

    if not os.path.exists(leads_file):
        ui.log_error(f"{leads_file} not found in current directory.")
        return

    leads_df = pd.read_csv(leads_file)
    if "Status" not in leads_df.columns or "URL" not in leads_df.columns:
        ui.log_error(f"{leads_file} must contain 'URL' and 'Status' columns.")
        return

    out_rows = []
    updated = False
    ui.log_analyst(f"Found {len(leads_df)} rows in {leads_file}.")

    profile = swarm_config.CLIENT_PROFILES.get(client_key, swarm_config.CLIENT_PROFILES["default"])
    ui.log_analyst(f"Activating Chameleon Agent Profile: {profile['company_name']}")

    for idx, row in ui.track(leads_df.iterrows(), total=len(leads_df), description="[analyst]Analyzing Sites...[/analyst]"):
        try:
            if str(row.get("Status", "")).strip().lower() != "unscanned":
                continue

            url = row.get("URL")
            site_dna, socials = fetch_site_text(url)
            
            combined_dna = ""
            if site_dna:
                combined_dna = f"--- HOMEPAGE ---\n{site_dna}\n"
                
                parsed_url = urlparse(url)
                root_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
                context_paths = ["/services", "/about", "/about-us", "/faq"]
                for path in context_paths:
                    target_url = f"{root_domain}{path}"
                    ui.log_analyst(f"Deep Context: Scraping {target_url}...")
                    sub_text, _ = fetch_site_text(target_url, timeout=8, retries=0)
                    if sub_text:
                        combined_dna += f"--- {path.upper()} ---\n{sub_text}\n"
                
                combined_dna = combined_dna[:12000]

            extracted_email = None
            if not combined_dna:
                pain = "Could not fetch site content"
            else:
                pain = None
                if genai_available and API_KEY:
                    pain = analyze_with_gemini(combined_dna, profile)
                if not pain:
                    ui.log_analyst("No pain point from Gemini, falling back to heuristics.")
                    pain = heuristic_analysis(combined_dna)
                
                extracted_email = extract_email_from_text(combined_dna)
                if extracted_email:
                    ui.log_success(f"Extracted email: {extracted_email}")
                else:
                    parsed_url = urlparse(url)
                    root_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
                    sub_paths = ["/contact", "/contact-us", "/about", "/about-us", "/support", "/team", "/privacy"]
                    for path in sub_paths:
                        sub_url = f"{root_domain}{path}"
                        ui.log_analyst(f"Deep Search: Checking {sub_url} for email...")
                        sub_text, _ = fetch_site_text(sub_url, timeout=10, retries=0)
                        if sub_text:
                            extracted_email = extract_email_from_text(sub_text)
                            if extracted_email:
                                ui.log_success(f"Deep Search found email: {extracted_email}")
                                break
                if not extracted_email:
                    base_domain = urlparse(url).netloc # For SerpAPI, just the domain is fine
                    ui.log_analyst(f"Deploying SerpAPI to hunt Google for {base_domain} email...")
                    extracted_email = hunt_email_via_google(base_domain)
                    if extracted_email:
                        ui.log_success(f"SerpAPI found email via Google: {extracted_email}")
            
            status = "Dead End"
            if extracted_email:
                status = "Analyzed"
            elif socials.get("Facebook") or socials.get("Instagram") or socials.get("LinkedIn") or socials.get("Twitter"):
                status = "Requires DM"
            elif socials.get("Contact_Page"):
                status = "Use Form"

            out_rows.append({
                "URL": url, 
                "Pain_Point_Summary": pain, 
                "Status": status,
                "Email": extracted_email,
                "Facebook": socials.get("Facebook"),
                "LinkedIn": socials.get("LinkedIn"),
                "Instagram": socials.get("Instagram"),
                "Twitter": socials.get("Twitter"),
                "Contact Page": socials.get("Contact_Page")
            })
            leads_df.at[idx, "Status"] = "Processed"
            updated = True
        except Exception as e:
            ui.log_error(f"Unexpected error processing row {idx}: {e}")

    out_df = pd.DataFrame(out_rows, columns=["URL", "Pain_Point_Summary", "Status", "Email", "Facebook", "LinkedIn", "Instagram", "Twitter", "Contact Page"])
    if not out_df.empty:
        if os.path.exists(audits_file):
            try:
                existing_df = pd.read_csv(audits_file)
                # Safe merge that preserves all Sniper columns
                combined_df = pd.concat([existing_df, out_df], ignore_index=True)
                combined_df.to_csv(audits_file, index=False)
            except Exception as e:
                ui.log_warning(f"Merge failed: {e}. Overwriting.")
                out_df.to_csv(audits_file, index=False)
        else:
            out_df.to_csv(audits_file, index=False)
        ui.display_dashboard(sites_analyzed=len(out_df))
        ui.log_success(f"Wrote {len(out_df)} new rows to {audits_file}")

    if updated:
        leads_df.to_csv(leads_file, index=False)
        ui.log_info(f"Updated {leads_file} statuses to 'Processed'.")

    cloud_storage.sync_up(leads_file)
    cloud_storage.sync_up(audits_file)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Analyst Agent - Site Analysis")
    parser.add_argument("--client_key", type=str, required=True, help="Client-specific key for data isolation")
    args = parser.parse_args()
    main(args.client_key)