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

# Load environment variables (Local fallback)
load_dotenv()

# --- Page Config ---
st.set_page_config(
    page_title=config.APP_NAME,
    page_icon="🚀",
    layout="wide"
)

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
    st.markdown("""
    <div style="background-color:{config.BACKGROUND_COLOR};padding:15px;border-bottom: 3px solid {config.PRIMARY_COLOR};border-radius: 5px;margin-bottom: 20px;">
        <h1 style="color:{config.TEXT_COLOR}; margin:0; font-size: 24px;">🚀 {config.APP_NAME}</h1>
        <p style="color:{config.TEXT_COLOR}; margin:0; font-size: 14px; opacity: 0.8;">{config.TAGLINE}</p>
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
    render_header()

    # --- Sidebar Footer ---
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Powered by {config.APP_NAME} v{config.APP_VERSION}")

    # --- Top Metrics ---
    leads_df = load_csv("leads_queue.csv")
    audits_df = load_csv("audits_to_send.csv")

    col1, col2, col3, col4 = st.columns(4)

    leads_count = len(leads_df) if not leads_df.empty else 0
    analyzed_count = len(audits_df) if not audits_df.empty else 0
    sent_count = len(audits_df[audits_df["Status"] == "Sent"]) if not audits_df.empty and "Status" in audits_df.columns else 0
    replies_count = len(audits_df[audits_df["Status"] == "Replied"]) if not audits_df.empty and "Status" in audits_df.columns else 0

    col1.metric("Leads Found", leads_count, delta="Scout")
    col2.metric("Sites Analyzed", analyzed_count, delta="Analyst")
    col3.metric("Emails Sent", sent_count, delta="Sniper")
    col4.metric("Replies", replies_count, delta="Closer")

    st.divider()

    # --- Tabs Layout ---
    tab1, tab2, tab3 = st.tabs(["🚀 Launchpad", "📊 Data & Logs", "⚙️ Config"])

    # --- TAB 1: LAUNCHPAD ---
    with tab1:
        st.subheader("Mission Control")
        
        c1, c2 = st.columns([1, 1])
        with c1:
            niche = st.text_input("Target Niche", value="Roofing", placeholder="e.g. Dentists")
        with c2:
            location = st.text_input("Target Location", value="Denver", placeholder="e.g. Chicago")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        if st.button("ACTIVATE SWARM 🚀", type="primary", use_container_width=True):
            if niche and location:
                run_full_sequence(niche, location)
                st.rerun()
            else:
                st.warning("Please enter both Niche and Location.")

    # --- TAB 2: DATA & LOGS ---
    with tab2:
        st.subheader("Mission Data")
        
        d1, d2 = st.columns(2)
        
        with d1:
            st.markdown("### 🔭 Leads Queue")
            if not leads_df.empty:
                st.dataframe(leads_df, use_container_width=True)
            else:
                st.info("No leads found yet.")
                
        with d2:
            st.markdown("### 🎯 Outreach Status")
            if not audits_df.empty:
                st.dataframe(audits_df, use_container_width=True)
            else:
                st.info("No audits generated yet.")
                
        st.divider()
        st.subheader("📜 Live Mission Logs")
        
        log_file = os.path.join("logs", "swarm.log")
        if st.button("Refresh Logs"):
            st.rerun()
            
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                lines = f.readlines()
                st.code("".join(lines[-50:]), language="log")
        else:
            st.warning("No logs found.")

    # --- TAB 3: CONFIG ---
    with tab3:
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

if __name__ == "__main__":
    main()