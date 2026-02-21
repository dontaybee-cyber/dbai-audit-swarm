import os
import argparse
import pandas as pd
import time
import random
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv
import ui_manager as ui

load_dotenv(override=True)

# Optional SerpAPI fallback - requires `google-search-results` package and SERPAPI_API_KEY env var
try:
    from serpapi import GoogleSearch
except ImportError:
    GoogleSearch = None

def get_known_domains(client_key: str) -> set:
    """The 'Ironclad Ledger': Load all known domains for a specific client to ensure zero repeated leads."""
    if not client_key:
        return set()
    
    known_domains = set()
    files = [f"leads_queue_{client_key}.csv", f"audits_to_send_{client_key}.csv"]
    
    for f in files:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f, on_bad_lines='skip')
                if "URL" in df.columns:
                    for url in df["URL"].dropna():
                        domain = urlparse(str(url)).netloc.lower().replace('www.', '')
                        if domain:
                            known_domains.add(domain)
            except Exception:
                pass
    return known_domains

def apollo_fallback_search(niche, location, required_lead_count, master_domain_set, blacklist):
    apollo_key = os.getenv("APOLLO_API_KEY")

    if not apollo_key:
        ui.log_warning("No Apollo API key found. Fallback aborted.")
        return []

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "X-Api-Key": apollo_key,
    }

    payload = {
        "q_organization_keyword_tags": [niche],
        "organization_locations": [location],
        "per_page": 100,  # Over-fetch to bypass known duplicates
    }

    fallback_leads = []

    try:
        resp = requests.post(
            "https://api.apollo.io/v1/organizations/search",
            headers=headers,
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        for org in data.get("organizations", []):
            website_url = org.get("website_url")
            if not website_url:
                continue

            if any(bad in website_url.lower() for bad in blacklist):
                continue

            host = urlparse(website_url).netloc.lower().replace("www.", "")
            if host and host not in master_domain_set:
                fallback_leads.append(website_url)
                master_domain_set.add(host)

            if len(fallback_leads) >= required_lead_count:
                break

    except Exception as e:
        ui.log_error(f"Apollo fallback encountered an exception: {e}")
        return []

    return fallback_leads


def scout_leads(niche, location, client_key, num_results=25):
    ui.SwarmHeader.display()
    ui.display_mission_briefing(niche, location)

    blacklist = (
        "yelp.", "angi.", "bbb.", "houzz.", "thumbtack.", "expertise.", 
        "yellowpages.", "facebook.", "linkedin.", "instagram.", "twitter.", 
        "porch.", "homeadvisor.", "forbes."
    )

    master_domain_set = get_known_domains(client_key)
    if master_domain_set:
        ui.log_info(f"Loaded {len(master_domain_set)} known domains to skip for client '{client_key}'.")

    modifiers = ["", "contractors", "company", "services", "experts", "specialists", "near me", "agency", "professionals"]
    selected_modifier = random.choice(modifiers)

    # Construct the dynamic query
    if selected_modifier:
        q = f'{niche} {selected_modifier} in {location} -yelp -angi -bbb -thumbtack'
    else:
        q = f'{niche} in {location} -yelp -angi -bbb -thumbtack'

    ui.log_scout(f"Starting semantic search for: [bold white]{q}[/bold white]")

    api_key = os.getenv("SERP_API_KEY")
    if not api_key:
        ui.log_error("SERP_API_KEY is not configured. Please set it in your environment.")
        return

    if GoogleSearch is None:
        ui.log_error("The 'google-search-results' library is not installed.")
        ui.log_info("Run: pip install google-search-results")
        return

    ui.log_scout("🛰️ SCOUT: Using SerpAPI for deep-search...")

    fresh_leads = []
    search_offset = 0
    
    while True:
        if search_offset >= 300:
            ui.log_warning(f"Safety breakout: Checked 30 pages. Proceeding with {len(fresh_leads)} found leads.")
            break

        params = {
            "engine": "google",
            "q": q,
            "api_key": api_key,
            "num": num_results,
            "start": search_offset,
            "location": location,
        }
        
        ui.log_scout(f"Searching page {search_offset // 10 + 1}...")

        try:
            search = GoogleSearch(params)
            results = search.get_dict()

            # Task 1: API Error & KeyError Shielding (+ Apollo failover)
            if "error" in results:
                ui.log_error(f"SerpAPI Error: {results['error']}")
                # Always trigger Apollo if Google fails
                search_term = f"{niche} {selected_modifier}".strip()
                apollo_urls = apollo_fallback_search(search_term, location, 100, master_domain_set, blacklist)
                fresh_leads.extend([{"URL": url, "Status": "Unscanned"} for url in apollo_urls])
                ui.log_success(f"Apollo Fallback recovered {len(apollo_urls)} leads.")
                break

            # Task 2: Fix the Aggressive Pagination Break
            local_results = results.get("local_results", [])
            organic_results = results.get("organic_results", [])

            if not local_results and not organic_results:
                ui.log_warning("No more leads found on this page. Ending scrape.")
                break

            # Task 3: Safe Looping
            if local_results:
                ui.log_scout("Extracting from Google Local Pack...")
                for local in local_results:
                    website = local.get("website")
                    if website and not any(bad in website.lower() for bad in blacklist):
                        host = urlparse(website).netloc.lower().replace('www.', '')
                        if host and host not in master_domain_set:
                            ui.log_success(f"Premium Lead Found (Maps): {website}")
                            fresh_leads.append({"URL": website, "Status": "Unscanned"})
                            master_domain_set.add(host)
            
            if organic_results:
                for result in organic_results:
                    link = result.get("link")
                    if link and not any(bad in link.lower() for bad in blacklist):
                        host = urlparse(link).netloc.lower().replace('www.', '')
                        if host and host not in master_domain_set:
                            ui.log_success(f"Lead Found (Organic): {link}")
                            fresh_leads.append({"URL": link, "Status": "Unscanned"})
                            master_domain_set.add(host)
            
            search_offset += 10
            time.sleep(1)

        except Exception as e:
            ui.log_error(f"Scout agent encountered an exception: {e}")
            break
            
    # Task 4: Guarantee the Baton Pass (CSV Creation)
    leads_file = f"leads_queue_{client_key}.csv"
    new_leads_df = pd.DataFrame(fresh_leads, columns=["URL", "Status"])

    if not new_leads_df.empty:
        header = not os.path.exists(leads_file)
        new_leads_df.to_csv(leads_file, mode='a', index=False, header=header)
        ui.display_dashboard(leads_found=len(new_leads_df))
        ui.log_success(f"{len(new_leads_df)} new leads added to {leads_file}")
    else:
        ui.log_warning("No new direct business websites found in this session.")
        # Still create the file if it doesn't exist, to prevent downstream errors
        if not os.path.exists(leads_file):
            pd.DataFrame(columns=["URL", "Status"]).to_csv(leads_file, index=False)
            ui.log_info(f"Created empty leads file: {leads_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scout Agent - Lead Discovery")
    parser.add_argument("--niche", type=str, required=True, help="Target Niche (e.g., 'Roofing Contractors')")
    parser.add_argument("--location", type=str, required=True, help="Target Location (e.g., 'Denver, CO')")
    parser.add_argument("--client_key", type=str, required=True, help="Client-specific key for data isolation")
    args = parser.parse_args()
    
    scout_leads(niche=args.niche, location=args.location, client_key=args.client_key)
