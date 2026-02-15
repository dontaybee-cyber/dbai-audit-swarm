import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
import ui_manager as ui
import swarm_config as config

# Import Agents
import scout_agent
import analyst_agent
import sniper_agent
import closer_agent

# Load environment variables (Local fallback)
load_dotenv()

# --- Cloud Secrets Sync ---
try:
    for key, value in st.secrets.items():
        os.environ[key] = str(value)
except Exception:
    pass

# --- Page Config ---
st.set_page_config(
    page_title=config.APP_NAME,
    page_icon="🚀",
    layout="wide"
)

# --- Task 1: Inject Custom CSS (The SaaS Polish) ---
def inject_custom_css():
    st.markdown(f"""
    <style>
        /* Main Background */
        .stApp {{
            background-color: {config.BACKGROUND_COLOR};
        }}
        
        /* Card Styling for Containers */
        div[data-testid="stVerticalBlock"] > div[style*="flex-direction: column;"] > div[data-testid="stVerticalBlock"] {{
            background-color: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        }}

        /* Metric Styling */
        div[data-testid="stMetric"] {{
            background-color: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 15px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            text-align: center;
        }}
        
        div[data-testid="stMetricLabel"] {{
            display: flex;
            justify-content: center;
            color: #64748B;
            font-weight: 600;
        }}

        div[data-testid="stMetricValue"] {{
            color: {config.PRIMARY_COLOR};
        }}

        /* Primary Button Styling */
        div.stButton > button[kind="primary"] {{
            background-color: {config.PRIMARY_COLOR};
            color: white;
            border: none;
            border-radius: 8px;
            font-weight: 600;
            transition: all 0.3s ease;
        }}

        div.stButton > button[kind="primary"]:hover {{
            transform: scale(1.02);
            box-shadow: 0 4px 12px rgba(75, 0, 130, 0.3);
        }}
    </style>
    """, unsafe_allow_html=True)

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

def render_login():
    """Renders the login screen."""
    st.markdown(f"""
    <div style="text-align: center; padding: 3rem 0 2rem 0;">
        <h1 style="color:{config.TEXT_COLOR}; margin:0; font-size: 3rem; font-weight: 800; letter-spacing: -0.025em;">🚀 {config.APP_NAME}</h1>
    </div>
    """, unsafe_allow_html=True)
    
    with st.container(border=True):
        st.markdown("<h2 style='text-align: center; color: #4B0082;'>DBAI SaleSwarm Access</h2>", unsafe_allow_html=True)
        client_key = st.text_input("Enter License Key", type="password", key="login_key")
        
        if st.button("Login", use_container_width=True, type="primary"):
            # Check against Streamlit secrets
            valid_keys = st.secrets.get("CLIENT_KEYS", [])
            master_key = os.getenv("MASTER_KEY")

            if client_key in valid_keys or (master_key and client_key == master_key):
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Invalid License Key. Access Denied.")

# --- Helper Functions ---
def get_config(key, default=""):
    """Get configuration from st.secrets (Cloud) or os.getenv (Local)."""
    try:
        if key in st.secrets:
            return st.secrets[key]
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return os.getenv(key, default)

def render_header():
    # Task 2: The Hero Header
    st.markdown(f"""
    <div style="text-align: center; padding: 3rem 0 2rem 0;">
        <h1 style="color:{config.TEXT_COLOR}; margin:0; font-size: 3rem; font-weight: 800; letter-spacing: -0.025em;">🚀 {config.APP_NAME}</h1>
        <p style="color:{config.TEXT_COLOR}; margin-top: 0.5rem; font-size: 1.25rem; opacity: 0.8;">{config.TAGLINE}</p>
    </div>
    """, unsafe_allow_html=True)

def load_csv(filename):
    if os.path.exists(filename):
        try:
            return pd.read_csv(filename)
        except:
            return pd.DataFrame()
    return pd.DataFrame()

def save_env(key, value):
    """Save to .env file (Local only)."""
    env_path = ".env"
    lines = []
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            lines = f.readlines()
    
    key_found = False
    new_lines = []
    for line in lines:
        if line.startswith(f"{key}="):
            new_lines.append(f"{key}={value}\n")
            key_found = True
        else:
            new_lines.append(line)
    
    if not key_found:
        if new_lines and not new_lines[-1].endswith("\n"):
            new_lines.append("\n")
        new_lines.append(f"{key}={value}\n")
        
    with open(env_path, "w") as f:
        f.writelines(new_lines)

def run_full_sequence(niche, location):
    """Executes the full acquisition sequence: Scout -> Analyst -> Sniper."""
    with st.status("🚀 Engaging DBAI Swarm...", expanded=True) as status:
        
        # 1. Scout
        st.write("🔭 Scouting for leads...")
        try:
            scout_agent.scout_leads(niche, location)
            st.write("✅ Scout Mission Complete.")
        except Exception as e:
            st.error(f"Scout failed: {e}")
            status.update(label="❌ Mission Failed", state="error")
            return

        # 2. Analyst
        st.write("🧠 Analyzing business data...")
        try:
            analyst_agent.main()
            st.write("✅ Analysis Complete.")
        except Exception as e:
            st.error(f"Analyst failed: {e}")
            status.update(label="❌ Mission Failed", state="error")
            return

        # 3. Sniper
        st.write("🎯 Firing sniper emails...")
        try:
            sniper_agent.main()
            st.write("✅ Outreach Complete.")
        except Exception as e:
            st.error(f"Sniper failed: {e}")
            status.update(label="❌ Mission Failed", state="error")
            return

        status.update(label="✅ Full Swarm Sequence Complete!", state="complete")

def main():
    inject_custom_css()
    
    if not st.session_state.authenticated:
        render_login()
        st.stop()

    render_header()

    # --- Sidebar Footer ---
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Powered by {config.APP_NAME} v{config.APP_VERSION}")

    # --- Top Metrics ---
    leads_df = load_csv("leads_queue.csv")
    audits_df = load_csv("audits_to_send.csv")

    col1, col2, col3, col4, col5 = st.columns(5)

    leads_count = len(leads_df) if not leads_df.empty else 0
    analyzed_count = len(audits_df) if not audits_df.empty else 0
    sent_count = len(audits_df[audits_df["Status"] == "Sent"]) if not audits_df.empty and "Status" in audits_df.columns else 0
    replies_count = len(audits_df[audits_df["Status"] == "Replied"]) if not audits_df.empty and "Status" in audits_df.columns else 0
    follow_up_count = len(audits_df[audits_df["Status"] == "Followed Up"]) if not audits_df.empty and "Status" in audits_df.columns else 0

    with col1:
        st.metric("Leads Found", leads_count)
    with col2:
        st.metric("Sites Analyzed", analyzed_count)
    with col3:
        st.metric("Emails Sent", sent_count)
    with col4:
        st.metric("Replies", replies_count)
    with col5:
        st.metric("Follow-ups", follow_up_count)

    st.markdown("---")

    # --- Tabs Layout ---
    tab1, tab2, tab3, tab4 = st.tabs(["🚀 Launchpad", "📲 Manual DMs", "📊 Data & Logs", "⚙️ Config"])

    # --- TAB 1: LAUNCHPAD ---
    with tab1:
        # Task 3: The "Launchpad" Redesign
        st.subheader("Mission Control")
        with st.container(border=True):
            c1, c2 = st.columns(2)
            with c1:
                niche = st.text_input("Target Niche", value="Roofing", placeholder="e.g. Dentists")
            with c2:
                location = st.text_input("Target Location", value="Denver", placeholder="e.g. Chicago")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            if st.button("🚀 ACTIVATE SWARM", type="primary", use_container_width=True):
                if niche and location:
                    run_full_sequence(niche, location)
                    st.rerun()
                else:
                    st.warning("Please enter both Niche and Location.")

            st.divider()

            if st.button("🤝 Run Closer (Check Replies & Auto Follow-up)", type="secondary", use_container_width=True):
                with st.status("Syncing Inbox...", expanded=True):
                    try:
                        closer_agent.main()
                        st.success("Inbox sync complete!")
                    except Exception as e:
                        st.error(f"Closer Agent failed: {e}")
                st.rerun()

    # --- TAB 2: MANUAL DMs ---
    with tab2:
        st.subheader("Manual Attack CRM")
        
        audits_df = load_csv("audits_to_send.csv")
        if audits_df.empty:
            st.info("No leads available yet. Run the Swarm first!")
        else:
            # Filter for manual leads
            if "Status" in audits_df.columns:
                dm_leads = audits_df[audits_df["Status"].isin(["Requires DM", "Use Form"])]
                
                if dm_leads.empty:
                    st.success("Inbox Zero! No manual follow-ups required right now.")
                else:
                    st.markdown(f"**{len(dm_leads)} Targets Identified**")
                    
                    for idx, row in dm_leads.iterrows():
                        with st.container(border=True):
                            # Header
                            url = str(row.get("URL", "#"))
                            st.markdown(f"### 🔗 [{url}]({url})")
                            
                            # Pain Point
                            pain_point = str(row.get("Pain Point", "No analysis available."))
                            st.info("**AI Intel (Copy to Clipboard):**")
                            st.code(pain_point, language="text")
                            
                            # Targets (Links)
                            st.markdown("**Engagement Targets:**")
                            
                            links = []
                            if pd.notna(row.get('Instagram')): links.append(f"[📷 Instagram]({row['Instagram']})")
                            if pd.notna(row.get('Facebook')): links.append(f"[📘 Facebook]({row['Facebook']})")
                            if pd.notna(row.get('Twitter')): links.append(f"[🐦 Twitter]({row['Twitter']})")
                            if pd.notna(row.get('LinkedIn')): links.append(f"[💼 LinkedIn]({row['LinkedIn']})")
                            if pd.notna(row.get('Contact Page')): links.append(f"[📝 Contact Form]({row['Contact Page']})")
                            
                            if links:
                                cols = st.columns(len(links))
                                for i, link in enumerate(links):
                                    cols[i].markdown(link)
                            else:
                                st.caption("No direct contact links found.")
            else:
                st.warning("Status column missing in CSV.")

    # --- TAB 3: DATA & LOGS ---
    with tab3:
        # Task 4: Clean up the Data & Logs
        st.subheader("Mission Data")
        
        d1, d2 = st.columns(2)
        
        with d1:
            st.markdown("#### 🔭 Leads Queue")
            if not leads_df.empty:
                st.dataframe(leads_df, use_container_width=True, height=400)
            else:
                st.info("No leads found yet.")
                
        with d2:
            st.markdown("#### 🎯 Outreach Status")
            if not audits_df.empty:
                st.dataframe(audits_df, use_container_width=True, height=400)
            else:
                st.info("No audits generated yet.")
                
        st.divider()

        st.markdown("#### 📩 Replies Pipeline")
        if not audits_df.empty and "Status" in audits_df.columns:
            replied_df = audits_df[audits_df["Status"] == "Replied"]
            if not replied_df.empty:
                st.dataframe(replied_df, use_container_width=True, height=200)
            else:
                st.info("No replies recorded yet.")
        
        st.divider()
        
        with st.expander("📜 Terminal Output (Live Logs)", expanded=False):
            log_file = os.path.join("logs", "swarm.log")
            if st.button("Refresh Logs"):
                st.rerun()
            if os.path.exists(log_file):
                with open(log_file, "r") as f:
                    lines = f.readlines()
                    st.code("".join(lines[-50:]), language="log")
            else:
                st.warning("No logs found.")

    # --- TAB 4: CONFIG ---
    with tab4:
        st.subheader("System Configuration")
        
        with st.form("config_form"):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("#### 🧠 AI & Search")
                gemini_key = st.text_input("GEMINI_API_KEY", value=get_config("GEMINI_API_KEY"), type="password")
                serp_key = st.text_input("SERP_API_KEY", value=get_config("SERP_API_KEY"), type="password")
            
            with c2:
                st.markdown("#### 📧 Email Credentials")
                email_user = st.text_input("EMAIL_USER", value=get_config("EMAIL_USER"))
                email_pass = st.text_input("EMAIL_PASS", value=get_config("EMAIL_PASS"), type="password")

            st.markdown("#### 🔑 License")
            license_key = st.text_input("License Key", value=get_config("LICENSE_KEY"), type="password")
            
            if st.form_submit_button("Save Configuration"):
                if not hasattr(st, "secrets"):
                    save_env("GEMINI_API_KEY", gemini_key)
                    save_env("SERP_API_KEY", serp_key)
                    save_env("EMAIL_USER", email_user)
                    save_env("EMAIL_PASS", email_pass)
                    save_env("LICENSE_KEY", license_key)
                    st.success("Configuration saved to .env!")
                else:
                    st.warning("Cannot write to .env in Cloud mode. Please set secrets in Streamlit Cloud dashboard.")
        
        st.divider()
        if st.button("Log Out", type="secondary"):
            st.session_state.authenticated = False
            st.rerun()