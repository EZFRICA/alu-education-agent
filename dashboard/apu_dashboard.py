import streamlit as st
import sys
import os
import asyncio
import time
from datetime import datetime

# Root path alignment
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apu.core.scheduler import scheduler
from apu.mmu.controller import load_dll, get_all_nodes, save_dll
from apu.mmu import cache_l1 as block_cache
from agent_os.graph import create_dll_agent_graph
from apu.core.pipeline import get_core_block_content
from langchain_core.messages import HumanMessage, AIMessage
import re

# =====================================================================
# OS / PAGE CONFIG
# =====================================================================
st.set_page_config(
    page_title="APU Control Center — System Topology",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Dark / Neon Cyberpunk CSS
st.markdown("""
<style>
    .stApp {
        background-color: #0a0a0f;
    }
    .main .block-container {
        padding-top: 0 !important;
        margin-top: -4rem !important;
        max-width: 98%;
    }
    div[data-testid="stVerticalBlock"] > div {
        background-color: transparent;
    }
    
    /* Panel Cards */
    .apu-panel {
        background: #11111a;
        border-radius: 10px;
        padding: 1.2rem;
        height: 100%;
        border: 1px solid #2d2d3d;
        color: #e2e8f0;
    }
    
    .panel-header {
        font-family: 'Courier New', monospace;
        font-size: 0.85rem;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
        border-bottom: 1px solid #2d2d3d;
        padding-bottom: 0.5rem;
    }

    /* Core Components Highlighting */
    .l1-cache   { border-top: 4px solid #4ade80; box-shadow: 0 -5px 15px -5px rgba(74,222,128,0.2); }
    .l2-ram     { border-top: 4px solid #60a5fa; box-shadow: 0 -5px 15px -5px rgba(96,165,250,0.2); }
    .l3-weaviate{ border-top: 4px solid #c084fc; box-shadow: 0 -5px 15px -5px rgba(192,132,252,0.2); }
    .terminal   { border-top: 4px solid #f472b6; box-shadow: 0 -5px 15px -5px rgba(244,114,182,0.2); }

    /* Tags & Badges */
    .badge {
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border-radius: 4px;
        font-size: 0.7rem;
        font-family: monospace;
        font-weight: bold;
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.1);
    }
    .badge-green { color: #4ade80; background: rgba(74,222,128,0.1); border-color: rgba(74,222,128,0.3); }
    .badge-blue  { color: #60a5fa; background: rgba(96,165,250,0.1); border-color: rgba(96,165,250,0.3); }
    .badge-purple{ color: #c084fc; background: rgba(192,132,252,0.1); border-color: rgba(192,132,252,0.3); }
    .badge-red   { color: #ef4444; background: rgba(239,68,68,0.1); border-color: rgba(239,68,68,0.3); }
    
    .block-item {
        background: #1a1a24;
        border: 1px solid #2d2d3d;
        border-radius: 6px;
        padding: 0.75rem;
        margin-bottom: 0.5rem;
        font-family: monospace;
        font-size: 0.85rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    
    /* Hide some default Streamlit elements aggressively */
    header[data-testid="stHeader"] {
        display: none !important;
        height: 0px !important;
    }
    #MainMenu { visibility: hidden; }

    /* Custom Metric Styling */
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 800;
        color: white;
    }
    div[data-testid="stMetricLabel"] {
        color: #a1a1aa;
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)


# =====================================================================
# BACKGROUND SCHEDULER & STATE
# =====================================================================
@st.cache_resource
def start_apu_scheduler():
    import threading
    def run_async_scheduler():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(scheduler.start())
        loop.run_forever()
    
    thread = threading.Thread(target=run_async_scheduler, daemon=True)
    thread.start()
    return thread

start_apu_scheduler()

# --- AGENT / CHAT SESSION STATE ---
if "messages" not in st.session_state:
    st.session_state.messages = []
if "langchain_history" not in st.session_state:
    st.session_state.langchain_history = []
if "last_injected_ids" not in st.session_state:
    st.session_state.last_injected_ids = []

# Load Initial State for the rest of the page (Chat, etc.)
dll_state = asyncio.run(load_dll())
agent_id = dll_state.get("agent_id", "Unknown")

# =====================================================================
# REFRESHABLE MEMORY MONITOR (FRAGMENT)
# =====================================================================
@st.fragment(run_every=2)
def memory_monitor_fragment():
    # Reload fresh state inside fragment
    try:
        f_dll_state = asyncio.run(load_dll())
        f_raw_nodes = get_all_nodes(f_dll_state)
        f_l1_metrics = block_cache.get_metrics()
        f_l1_summary = block_cache.get_summary()
        f_agent_id = f_dll_state.get("agent_id", "Unknown")
    except Exception:
        return

    # Deduplicate strictly
    seen_ids = set()
    seen_labels = set()
    nodes = []
    for n in f_raw_nodes:
        if n.get("id") not in seen_ids and n.get("label") not in seen_labels:
            seen_ids.add(n.get("id"))
            seen_labels.add(n.get("label"))
            nodes.append(n)

    # Determine active nodes from last chat turn
    active_id_list = st.session_state.get("last_injected_ids", [])
    for n in nodes:
        if n.get("id") in active_id_list:
            n["is_active"] = True

    # High-level Metrics
    cols = st.columns(4)
    with cols[0]:
        st.metric("L1 Cache Hit Rate", f"{f_l1_summary.get('global_hit_rate', 0) * 100:.1f}%")
    with cols[1]:
        active_payloads = sum(1 for data in f_l1_metrics.values() if data.get('in_cache'))
        st.metric("L1 Active Payloads", f"{active_payloads} blocks")
    with cols[2]:
        active_nodes = sum(1 for n in nodes if n.get('is_active', False))
        st.metric("L2 S-MMU Pages", f"{active_nodes} / {len(nodes)}")
    with cols[3]:
        st.metric("Tenant / Global Agent ID", f_agent_id[:12] + "..." if len(f_agent_id) > 12 else f_agent_id)

    st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)

    # ROW 1 : THE MEMORY HEIRARCHY
    col_l1, col_l2, col_l3 = st.columns(3, gap="large")

    # 1. L1 CACHE
    with col_l1:
        st.markdown("""
            <div class="apu-panel l1-cache">
                <div class="panel-header"><span style='color:#4ade80'>■</span> L1 CACHE (0ms Register)</div>
                <p style="font-size:0.75rem; color:#a1a1aa; margin-bottom:1rem;">In-Process Memory. Contains fully injected payloads bypassing RAG latency.</p>
        """, unsafe_allow_html=True)
        
        if not f_l1_metrics:
            st.markdown("<div class='block-item' style='justify-content:center; color:#52525b;'>[ L1 Register Empty ]</div>", unsafe_allow_html=True)
        else:
            for bid, data in f_l1_metrics.items():
                if data.get('in_cache'):
                    hits = data.get('l1_hits', 0)
                    st.markdown(f"""
                    <div class='block-item' style='border-color: rgba(74,222,128,0.2);'>
                        <span style='color:#f8fafc'>{bid}</span> 
                        <span class='badge badge-green'>HOT <span style='color:#fff; opacity:0.7; margin-left:4px;'>{hits} hit</span></span>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class='block-item'>
                        <span style='color:#64748b; text-decoration: line-through;'>{bid}</span> 
                        <span class='badge'>SWAPPED OUT</span>
                    </div>
                    """, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # 2. L2 S-MMU DLL
    with col_l2:
        st.markdown("""
            <div class="apu-panel l2-ram notranslate" style="height: auto; padding-bottom: 0;">
                <div class="panel-header"><span style='color:#60a5fa'>■</span> L2 DLL (Semantic Paging)</div>
                <p style="font-size:0.75rem; color:#a1a1aa; margin-bottom:1rem;">Doubly Linked List holding cognitive context. Routed by BMJ Algorithm.</p>
            </div>
        """, unsafe_allow_html=True)
        
        if "inspected_block" not in st.session_state:
            st.session_state.inspected_block = None

        for i, node in enumerate(nodes[:7]):
            is_active = node.get('is_active', False)
            label = node.get('label', 'Unknown')
            btype = node.get('type', 'temp')
            badge = "SYNC Paged" if is_active else "Evicted"
            
            if "inspected_block_id" not in st.session_state:
                st.session_state.inspected_block_id = None

            # ── NODE BUTTONS ──
            sc1, sc2 = st.columns([3, 1])
            with sc1:
                btn_key = f"btn_node_f_{node.get('id')}"
                if st.button(f"[{i}] {label}", key=btn_key, use_container_width=True):
                    if st.session_state.inspected_block_id == node.get('id'):
                        st.session_state.inspected_block_id = None
                    else:
                        st.session_state.inspected_block_id = node.get('id')
            with sc2:
                st.markdown(f"<span class='badge badge-blue'>{badge}</span>" if is_active else f"<span class='badge'>{badge}</span>", unsafe_allow_html=True)
            
            if st.session_state.inspected_block_id == node.get('id'):
                st.info(f"**Inspect: {label}**")
                st.caption(f"Type: {btype} | ID: {node.get('id')}")
                st.json({"keywords": node.get("keywords", []), "metadata": node.get("metadata", {})})
                
                # Fetch content (L1 cache or live)
                from apu.core.pipeline import get_core_block_content
                cached_content = asyncio.run(block_cache.get(node.get('id')))
                if cached_content:
                    st.success(f"**[L1 HIT] Content:**\n\n{cached_content}")
                else:
                    st.warning("Content is swapped out to L4. Click to retrieve.")
                    if st.button("Live Fetch Content", key=f"f_fetch_{node.get('id')}"):
                        content = asyncio.run(get_core_block_content(f_agent_id, node.get('id')))
                        st.success(f"**[L4 FETCHED] Content:**\n\n{content}")

    # 3. L3 VECTOR TLB
    with col_l3:
        st.markdown("""
            <div class="apu-panel l3-weaviate">
                <div class="panel-header"><span style='color:#c084fc'>■</span> L3 ARCHIVE (Vector TLB)</div>
                <p style="font-size:0.75rem; color:#a1a1aa; margin-bottom:1rem;">Weaviate Multi-Tenant storage for LLM Retrieval & Synchronization.</p>
        """, unsafe_allow_html=True)
        st.markdown(f"""
            <div class='block-item' style='margin-bottom: 1rem;'>
                <span style='color:#d8b4fe'>WCD Cluster Status</span>
                <span class='badge badge-purple'>ONLINE</span>
            </div>
        """, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

# =====================================================================
# HEADER
# =====================================================================
st.markdown("""
    <div style='text-align: center; margin-bottom: 2rem; margin-top: -40px;'>
        <h1 style='font-family: Courier New; font-size: 2rem; letter-spacing: 6px; color: #fff; margin:0; padding:0;'>AGENT PROCESSOR UNIT</h1>
        <p style='color: #64748b; font-family: monospace; letter-spacing: 2px; margin-top: 4px;'>SYSTEM TOPOLOGY & LATENCY OBSERVATORY</p>
    </div>
""", unsafe_allow_html=True)

memory_monitor_fragment()

st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)

# =====================================================================
# ROW 2 : LIVE TERMINAL LOGS & NEURAL CHAT
# =====================================================================
col_logs, col_chat = st.columns([2, 1], gap="medium")

with col_logs:
    st.markdown("<h3 style='font-family: Courier New; color:#f472b6; font-size:1rem; margin-top:2rem; letter-spacing: 1px;'>■ APU RUNTIME LOGS</h3>", unsafe_allow_html=True)
    live_feed = st.toggle("🟢 Live Refresh", value=True, help="Disable to freely scroll the logs without interruption.")

    @st.fragment(run_every=1)
    def logs_monitor():
        def render_native_logs(log_path="apu_runtime.log", lines_count=150):
            import re
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()[-lines_count:]
            except Exception:
                return "<div style='color:#f85149; font-family:monospace; padding:1rem;'>[Sys Error] Cannot read apu_runtime.log</div>"
                
            ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
            
            html = """
            <style>
                .cyber-logs-container { margin-top: 1.5rem; background-color: #0d1117; color: #c9d1d9; font-family: 'Courier New', monospace; border-radius: 8px; border: 1px solid #30363d; overflow: hidden; box-shadow: 0 4px 15px rgba(0,0,0,0.5); }
                .cyber-logs-header { background-color: #161b22; padding: 12px 15px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid #30363d; }
                .cyber-logs-title { font-size: 1rem; font-weight: 700; color: #58a6ff; display: flex; align-items: center; gap: 10px; }
                .cyber-pulse { width: 10px; height: 10px; background-color: #3fb950; border-radius: 50%; box-shadow: 0 0 10px #3fb950; animation: blink 1.5s infinite; }
                @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
                .cyber-terminal { padding: 15px; height: 410px; overflow-y: auto; background-color: #010409; box-shadow: inset 0 0 20px rgba(0, 0, 0, 0.8); }
                .cyber-log-line { margin: 0; padding: 4px 5px; font-size: 0.85rem; line-height: 1.5; white-space: pre-wrap; word-wrap: break-word; border-bottom: 1px solid rgba(255, 255, 255, 0.05); }
                .cyber-log-line:hover { background-color: #161b22; }
            </style>
            <div class="cyber-logs-container">
                <div class="cyber-logs-header">
                    <div class="cyber-logs-title"><div class="cyber-pulse"></div>APU Live Monitor</div>
                    <div style="color: #8b949e; font-size: 0.75rem;">Showing last 150 lines</div>
                </div>
                <div class="cyber-terminal" id="cyber-terminal">
                    <div>
            """
            
            for line in lines:
                clean = ansi_escape.sub('', line).strip()
                if not clean: continue
                
                parts = clean.split('|')
                if len(parts) >= 4:
                    time_part = parts[0].strip()
                    level = parts[1].strip()
                    module = parts[2].strip()
                    msg = '|'.join(parts[3:]).strip()
                    
                    level_color = "#c9d1d9"
                    if "INFO" in level: level_color = "#58a6ff"
                    elif "DEBUG" in level: level_color = "#8b949e"
                    elif "WARNING" in level: level_color = "#d29922"
                    elif "ERROR" in level: level_color = "#f85149"
                    
                    msg = re.sub(r'(HIT|Hit)', r'<span style="color:#3fb950;font-weight:bold;">\1</span>', msg)
                    msg = re.sub(r'(MISS|Miss)', r'<span style="color:#f85149;font-weight:bold;">\1</span>', msg)
                    msg = re.sub(r'(PUSHED|Pushed)', r'<span style="color:#d2a8ff;font-weight:bold;">\1</span>', msg)
                    
                    html += f'<div class="cyber-log-line"><span style="color:#8b949e;">[{time_part}]</span> <span style="color:{level_color};font-weight:bold;">{level.ljust(8)}</span> <span style="color:#d2a8ff;font-weight:bold;">[{module}]</span> <span style="color:#e2e8f0;">{msg}</span></div>'
                else:
                    html += f'<div class="cyber-log-line" style="color:#f85149;">{clean}</div>'
                    
            html += """
                    </div>
                </div>
            </div>
            <script>
                var term = window.parent.document.getElementById('cyber-terminal');
                if(!term) term = document.getElementById('cyber-terminal');
                if(term) { term.scrollTop = term.scrollHeight; }
            </script>
            """
            return html

        if live_feed:
            st.markdown(render_native_logs(), unsafe_allow_html=True)

    logs_monitor()

with col_chat:
    st.markdown("<h3 style='font-family: Courier New; color:#60a5fa; font-size:1rem; margin-top:2rem; letter-spacing: 1px;'>■ NEURAL INTERFACE</h3>", unsafe_allow_html=True)
    
    chat_box = st.container(height=450, border=True)
    with chat_box:
        for m in st.session_state.messages:
            with st.chat_message(m["role"]):
                st.markdown(m["content"])
    
    if prompt := st.chat_input("Command the APU...", key="chat_input_apu"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.session_state.langchain_history.append(HumanMessage(content=prompt))
        st.rerun()

# --- ASYNC AGENT PROCESSING ---
if st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
    with chat_box:
        with st.chat_message("assistant"):
            with st.spinner("Processing through S-MMU..."):
                agent_app = create_dll_agent_graph()
                inputs = {
                    "messages": st.session_state.langchain_history, 
                    "agent_id": agent_id,
                    "search_enabled": True
                }
                result = asyncio.run(agent_app.ainvoke(inputs))
                
                new_messages = result.get("messages", [])
                resp_text = new_messages[-1].content
                st.session_state.messages.append({"role": "assistant", "content": resp_text})
                st.session_state.langchain_history = new_messages
                
                # Highlight logic for dashboard
                match = re.search(r"Memory: (.*?) \|", resp_text)
                if match:
                    injected_str = match.group(1)
                    st.session_state.last_injected_ids = [s.strip() for s in injected_str.split("+") if s.strip() and s.strip() != "none"]
                
                st.rerun()

# Trick to auto-refresh Streamlit layout metrics purely visually without chat reset
if st.session_state.inspected_block is not None:
    st.caption("⏸️ Auto-refresh paused while inspecting block.")
elif not live_feed:
    st.caption("⏸️ Terminal monitoring paused (Scroll Mode).")
