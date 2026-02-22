import os
import random
import re
import time
from typing import Optional, Tuple, Dict
from urllib.parse import urlparse, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import streamlit as st
import ui_manager as ui
import swarm_config

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")

try:
    HUNTER_API_KEY = st.secrets.get("HUNTER_API_KEY", os.getenv("HUNTER_API_KEY"))
except Exception:
    HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")

try:
    from duckduckgo_search import DDGS
except ImportError:
    DDGS = None

try:
    from serpapi import GoogleSearch
    SERPAPI_AVAILABLE = True
except ImportError:
    SERPAPI_AVAILABLE = False

try:
    import cloud_storage
    CLOUD_STORAGE_AVAILABLE = True
except ImportError:
    CLOUD_STORAGE_AVAILABLE = False

try:
    import google.genai as genai
    genai_available = True
    try:
        genai.configure(api_key=API_KEY)
    except Exception:
        pass
except Exception:
    genai_available = False

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
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
                return None, socials
            if mailtos:
                text += " " + " ".join(mailtos)
            ui.log_analyst(f"Successfully fetched {len(text)} characters")
            return text[:4000], socials
        except Exception as e:
            if attempt < retries:
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
        "2. Synthesize your finding and the ROI into a single, hard-hitting sentence for a cold email."
        "   - Format: '[Identified Weakness], potentially losing you an estimated [Projected ROI] annually.'"
        "CRUCIAL: Output only this single sentence. Nothing else."
    )
    prompt = f"{system_instruction}\n\nWebsite Text:\n{site_dna}"
    try:
        if not genai_available: return None
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        response = model.generate_content(prompt)
        text = response.text if hasattr(response, 'text') else str(response)
        return text.strip().splitlines()[0]
    except Exception:
        return None

def heuristic_analysis(site_dna: str) -> str:
    ui.log_analyst("Running heuristic analysis...")
    s = site_dna.lower()
    if "contact" not in s and "contact" not in s[:200]:
        return "Your website has no visible lead-capture form on the homepage, potentially losing you an estimated $15,000 annually from missed conversion opportunities."
    if "book" in s and ("online" not in s and "book now" not in s):
        return "Your site appears to use a manual booking process, potentially losing you an estimated $25,000 annually from customers who expect instant online scheduling."
    if "support" in s and ("chat" not in s and "help" in s):
        return "Your support page lacks an instant AI chat, potentially losing you an estimated $20,000 annually from unresolved customer questions."
    return "Your website lacks a clear, instant lead-capture mechanism, potentially losing you an estimated $18,000 annually from missed opportunities."

def extract_email_from_text(text: str) -> Optional[str]:
    email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
    matches = re.findall(email_pattern, text)
    
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
            lower_email = email.lower().strip().rstrip('.')
            if any(term in lower_email for term in ignore_terms): continue
            if lower_email.endswith(ignore_exts): continue
            if len(lower_email) < 6 or len(lower_email) > 80: continue
            if lower_email not in valid_emails:
                valid_emails.append(lower_email)
            
    if not valid_emails: return None
        
    priorities = ['info@', 'contact@', 'sales@', 'hello@', 'office@', 'admin@', 'support@', 'estimate@']
    for e in valid_emails:
        if any(e.startswith(p) for p in priorities):
            return e
            
    return valid_emails[0]

def hunt_email_via_ddg(domain: str) -> Optional[str]:
    if DDGS is None: return None
    try:
        ddgs = DDGS()
        q = f'"{domain}" contact OR email OR @'
        results = list(ddgs.text(q, max_results=10))
        snippets = " ".join([res.get("body", "") for res in results])
        return extract_email_from_text(snippets)
    except Exception:
        return None

def hunt_email_via_google(domain: str) -> Optional[str]:
    if not SERPAPI_AVAILABLE: return None
    api_key = st.secrets.get("SERP_API_KEY", os.getenv("SERP_API_KEY"))
    if not api_key: return None
    try:
        q = f'"{domain}" contact OR email OR @'
        search = GoogleSearch({"engine": "google", "q": q, "api_key": api_key, "num": 10})
        results = search.get_dict()
        if "error" in results: return None
        snippets = " ".join([res.get("snippet", "") for res in results.get("organic_results", [])])
        return extract_email_from_text(snippets)
    except Exception:
        return None

def enrich_email_with_hunter(domain: str) -> Optional[str]:
    if not HUNTER_API_KEY or HUNTER_API_KEY == "YOUR_HUNTER_API_KEY": return None
    try:
        ui.log_analyst(f"Querying Hunter.io for domain: {domain}")
        url = "https://api.hunter.io/v2/domain-search"
        params = {"domain": domain, "api_key": HUNTER_API_KEY}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        emails = resp.json().get("data", {}).get("emails", [])
        for e in emails:
            if e.get("value"): return e.get("value")
        return None
    except Exception as e:
        ui.log_warning(f"Hunter enrichment failed for {domain}: {e}")
        return None

def main(client_key: str):
    ui.SwarmHeader.display()
    ui.log_analyst("Analyst Agent starting...")

    leads_file = f"leads_queue_{client_key}.csv"
    audits_file = f"audits_to_send_{client_key}.csv"

    if CLOUD_STORAGE_AVAILABLE:
        cloud_storage.sync_down(leads_file)
        cloud_storage.sync_down(audits_file)

    if not os.path.exists(leads_file):
        ui.log_error(f"{leads_file} not found in current directory.")
        return

    leads_df = pd.read_csv(leads_file)
    if "Status" not in leads_df.columns or "URL" not in leads_df.columns:
        ui.log_error(f"{leads_file} must contain 'URL' and 'Status' columns.")
        return

    unscanned_mask = leads_df["Status"].astype(str).str.strip().str.lower() == "unscanned"
    unscanned_df = leads_df[unscanned_mask]
    
    MAX_BATCH = 40
    batch_df = unscanned_df.head(MAX_BATCH)
    
    if batch_df.empty:
        ui.log_success("No unscanned leads found. Run the Scout to gather more!")
        return
        
    ui.log_analyst(f"Batch Governor Active: Processing {len(batch_df)} leads this run...")
    out_rows = []
    updated = False

    profile = swarm_config.CLIENT_PROFILES.get(client_key, swarm_config.CLIENT_PROFILES["default"])
    ui.log_analyst(f"Activating Chameleon Agent Profile: {profile['company_name']}")

    for idx, row in ui.track(batch_df.iterrows(), total=len(batch_df), description="[analyst]Analyzing Sites...[/analyst]"):
        try:
            url = row.get("URL")
            parsed_url = urlparse(url)
            root_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
            base_domain_netloc = parsed_url.netloc.replace("www.", "")

            site_dna, socials = fetch_site_text(root_domain)
            
            combined_dna = ""
            if site_dna:
                combined_dna = f"--- HOMEPAGE ---\n{site_dna}\n"
                
                context_paths = ["/services", "/about", "/about-us", "/faq"]
                for path in context_paths:
                    ui.log_analyst(f"Deep Context: Scraping {root_domain}{path}...")
                    sub_text, _ = fetch_site_text(f"{root_domain}{path}", timeout=8, retries=0)
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
                    ui.log_success(f"Extracted email from homepage context: {extracted_email}")
                else:
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
                        ui.log_analyst(f"Deploying SerpAPI to hunt Google for {base_domain_netloc} email...")
                        extracted_email = hunt_email_via_google(base_domain_netloc)
                        if extracted_email: ui.log_success(f"SerpAPI found email: {extracted_email}")
                        
                    if not extracted_email:
                        ui.log_analyst(f"Deploying DuckDuckGo native search for {base_domain_netloc}...")
                        extracted_email = hunt_email_via_ddg(base_domain_netloc)
                        if extracted_email: ui.log_success(f"DDG found email: {extracted_email}")
                        
                    if not extracted_email and HUNTER_API_KEY:
                        ui.log_analyst(f"Querying Hunter.io database for {base_domain_netloc}...")
                        extracted_email = enrich_email_with_hunter(base_domain_netloc)
                        if extracted_email: ui.log_success(f"Hunter.io found email: {extracted_email}")
            
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
                combined_df = pd.concat([existing_df, out_df], ignore_index=True)
                combined_df.to_csv(audits_file, index=False)
            except Exception as e:
                ui.log_warning(f"Merge failed: {e}. Overwriting.")
                out_df.to_csv(audits_file, index=False)
        else:
            out_df.to_csv(audits_file, index=False)
        ui.display_dashboard(sites_analyzed=len(out_df))
        ui.log_success(f"Wrote {len(out_df)} new rows to {audits_file}")
        
        if CLOUD_STORAGE_AVAILABLE:
            cloud_storage.sync_up(audits_file)

    if updated:
        leads_df.to_csv(leads_file, index=False)
        ui.log_info(f"Updated {leads_file} statuses to 'Processed'.")
        if CLOUD_STORAGE_AVAILABLE:
            cloud_storage.sync_up(leads_file)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Analyst Agent - Site Analysis")
    parser.add_argument("--client_key", type=str, required=True, help="Client-specific key for data isolation")
    args = parser.parse_args()
    main(args.client_key)