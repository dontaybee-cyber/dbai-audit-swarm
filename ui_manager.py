from datetime import datetime
import logging
import os
import sys
import swarm_config as config

try:
    from rich.console import Console
    from rich.theme import Theme
    from rich.panel import Panel
    from rich.table import Table
    from rich.progress import track as rich_track
    from rich import box
except ImportError:
    print(f"CRITICAL: 'rich' library not found. Run: \"{sys.executable}\" -m pip install rich")
    sys.exit(1)

# Streamlit Detection
IS_STREAMLIT = False
try:
    import streamlit as st
    if st.runtime.exists():
        IS_STREAMLIT = True
except Exception:
    IS_STREAMLIT = False

# Setup Production Logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=os.path.join("logs", "swarm.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Define custom theme for the agents
custom_theme = Theme({
    "scout": "bold green",
    "analyst": "bold cyan",
    "sniper": "bold red",
    "closer": "bold magenta",
    "info": "white",
    "success": "bold green",
    "warning": "yellow",
    "error": "bold red"
})

console = Console(theme=custom_theme)

class SwarmHeader:
    @staticmethod
    def display():
        if IS_STREAMLIT:
            # Header is handled by app.py layout
            return
            
        console.clear()
        console.print(Panel.fit(
            f"[bold magenta]{config.APP_NAME}[/bold magenta]\n[dim]{config.TAGLINE}[/dim]",
            box=box.DOUBLE,
            border_style="magenta",
            padding=(1, 4),
            subtitle="[dim]v2.0.0[/dim]"
        ))
        console.print("")

def display_dashboard(leads_found=0, sites_analyzed=0, emails_sent=0, followups_sent=0):
    if IS_STREAMLIT:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Leads Found", leads_found)
        col2.metric("Sites Analyzed", sites_analyzed)
        col3.metric("Emails Sent", emails_sent)
        col4.metric("Follow-ups", followups_sent)
        return

    table = Table(box=box.ROUNDED, show_header=True, header_style="bold white", expand=True)
    table.add_column("Agent", style="dim")
    table.add_column("Metric", style="white")
    table.add_column("Value", justify="right")

    table.add_row(f"[scout]{config.SCOUT_NAME}[/scout]", "Leads Found", f"[scout]{leads_found}[/scout]")
    table.add_row(f"[analyst]{config.ANALYST_NAME}[/analyst]", "Sites Analyzed", f"[analyst]{sites_analyzed}[/analyst]")
    table.add_row(f"[sniper]{config.SNIPER_NAME}[/sniper]", "Emails Sent", f"[sniper]{emails_sent}[/sniper]")
    table.add_row("[closer]Closer Agent[/closer]", "Follow-ups Sent", f"[closer]{followups_sent}[/closer]")

    console.print(Panel(table, title="[bold]Live Mission Stats[/bold]", border_style="blue"))
    console.print("")

def display_mission_briefing(niche, location):
    if IS_STREAMLIT:
        st.subheader("Mission Briefing")
        st.info(f"**Target Niche:** {niche} | **Target Location:** {location}")
        st.divider()
        return

    table = Table(box=box.ROUNDED, show_header=False, expand=False)
    table.add_column("Parameter", style="cyan")
    table.add_column("Value", style="bold yellow")
    table.add_row("Target Niche", niche)
    table.add_row("Target Location", location)
    console.print(Panel(table, title="[bold]Mission Briefing[/bold]", border_style="blue", expand=False))
    console.print("")

# Wrapper for rich.progress.track to ensure consistent console usage
def track(sequence, description="Processing...", total=None):
    if IS_STREAMLIT:
        st.write(f"*{description}*")
        pbar = st.progress(0)
        
        # Attempt to guess total length for progress bar
        if total is None:
            try:
                total = len(sequence)
            except:
                total = 0
        
        for i, item in enumerate(sequence):
            yield item
            if total > 0:
                progress = min((i + 1) / total, 1.0)
                pbar.progress(progress)
        return

    return rich_track(sequence, description=description, total=total, console=console)

def _log(style, icon, title, msg):
    if IS_STREAMLIT:
        if style == "error":
            st.error(f"{icon} {title}: {msg}")
        elif style == "warning":
            st.warning(f"{icon} {title}: {msg}")
        elif style == "success":
            st.toast(f"{icon} {title}: {msg}", icon="✅")
        else:
            st.toast(f"{icon} {title}: {msg}")
        # Continue to file logging below...

    timestamp = datetime.now().strftime("%H:%M:%S")
    console.print(f"[dim]{timestamp}[/dim] [{style}]{icon} {title}:[/{style}] {msg}")
    
    # Write to swarm.log
    log_msg = f"{title}: {msg}"
    if style == "error":
        logging.error(log_msg)
    elif style == "warning":
        logging.warning(log_msg)
    else:
        logging.info(log_msg)

def log_scout(msg):
    _log("scout", "🔭", config.SCOUT_NAME, msg)

def log_analyst(msg):
    _log("analyst", "🧠", config.ANALYST_NAME, msg)

def log_sniper(msg):
    _log("sniper", "🎯", config.SNIPER_NAME, msg)

def log_closer(msg):
    _log("closer", "🤝", "CLOSER", msg)

def log_info(msg):
    console.print(f"[info]ℹ️ {msg}[/info]")

def log_success(msg):
    console.print(f"[success]✅ {msg}[/success]")

def log_warning(msg):
    console.print(f"[warning]⚠️ {msg}[/warning]")

def log_error(msg):
    console.print(f"[error]❌ {msg}[/error]")