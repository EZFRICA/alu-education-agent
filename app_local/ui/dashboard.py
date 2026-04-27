import streamlit as st
import sys
import os
import asyncio
import time
from datetime import datetime
from langchain_core.messages import HumanMessage, AIMessage
import re

# Path alignment
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app_local.core.scheduler import scheduler
from app_local.mmu import controller
from app_local.mmu.block_factory import auto_execute_block_proposal

def extract_text(content) -> str:
    """Extracts raw text from Gemini content (can be a string or a list of blocks)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(block.get("text", "") for block in content if isinstance(block, dict))
    return str(content)

from app_local.runtime.agent import create_agent_graph
from app_local.storage import lance_driver
from app_local.mmu import cache_l1
from app_local.sync.sync_manager import (
    sync_with_registry,
    download_course,
    get_remote_catalog,
    is_course_available_locally
)

# ── Database Connection (Cached for Streamlit) ──────────────────────
@st.cache_resource
def get_cached_db():
    return lance_driver.get_db()

# =====================================================================
# PAGE CONFIG
# =====================================================================
st.set_page_config(
    page_title="Akili APU Dashboard",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Cache Priming (L1) ──────────────────────────────────────────────
# Pre-fill L1 with current DLL nodes so the FIRST agent call is a HIT
try:
    _dll_init = asyncio.run(controller.load_dll())
    for _nid, _node in _dll_init.get("nodes", {}).items():
        if _node.get("content"):
            cache_l1.set(_nid, _node["content"], block_type=_node.get("type"))
except Exception:
    pass

# =====================================================================
# CSS
# =====================================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=Fira+Code:wght@400;500&display=swap');
    
    :root {
        --bg-dark: #0f172a;
        --panel-bg: rgba(30, 41, 59, 0.7);
        --accent-blue: #38bdf8;
        --accent-green: #4ade80;
        --text-main: #f1f5f9;
        --text-dim: #94a3b8;
        --border: rgba(255, 255, 255, 0.1);
    }

    .stApp { background-color: #020617; color: var(--text-main); }
    
    .apu-panel {
        background: var(--panel-bg);
        backdrop-filter: blur(12px);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 24px;
        margin-bottom: 24px;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.3);
    }

    .panel-header {
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        font-size: 1rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 20px;
        color: var(--accent-blue);
        border-bottom: 1px solid var(--border);
        padding-bottom: 10px;
    }

    .memory-block {
        padding: 16px;
        border-radius: 12px;
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.05);
        margin-bottom: 14px;
        transition: all 0.2s ease;
    }
    
    .memory-block:hover {
        background: rgba(255, 255, 255, 0.06);
        border-color: rgba(56, 189, 248, 0.3);
    }

    .block-label {
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        font-size: 1rem;
        color: var(--text-main);
    }

    .block-content {
        font-family: 'Inter', sans-serif;
        font-size: 0.95rem;
        line-height: 1.6;
        color: var(--text-dim);
        margin-top: 8px;
    }

    .badge {
        font-family: 'Fira Code', monospace;
        font-size: 0.75rem;
        padding: 3px 10px;
        border-radius: 6px;
        font-weight: 500;
    }
    .badge-blue { background: rgba(56, 189, 248, 0.2); color: #7dd3fc; border: 1px solid rgba(56, 189, 248, 0.3); }
    .badge-green { background: rgba(74, 222, 128, 0.2); color: #86efac; border: 1px solid rgba(74, 222, 128, 0.3); }
    .badge-orange { background: rgba(251, 146, 60, 0.2); color: #fdba74; border: 1px solid rgba(251, 146, 60, 0.3); }
</style>
""", unsafe_allow_html=True)

# =====================================================================
# SESSION STATE
# =====================================================================
if "messages" not in st.session_state:
    st.session_state.messages = []
if "pending_block_proposal" not in st.session_state:
    st.session_state.pending_block_proposal = None
if "remote_catalog" not in st.session_state:
    st.session_state.remote_catalog = {}

# Load DLL State
dll_state = asyncio.run(controller.load_dll())
curr_sel = dll_state.get("course_selection", {"class": "6eme", "subject": "maths"})

# =====================================================================
# SIDEBAR — COURSE SELECTION
# =====================================================================
with st.sidebar:
    st.title("🎓 Akili Local")
    st.divider()

    st.markdown("### 📚 Course Selection")

    # ── Fetch remote catalog (cached in session) ──────────────────────
    if not st.session_state.remote_catalog:
        with st.spinner("Loading catalog..."):
            catalog = asyncio.run(get_remote_catalog())
            st.session_state.remote_catalog = catalog
    else:
        catalog = st.session_state.remote_catalog

    if not catalog:
        st.warning("Registry unreachable. Using local cache only.")
        # Fallback: show current selection
        st.info(f"Active: **{curr_sel['class']}** — **{curr_sel['subject']}**")
    else:
        # ── Class selection ───────────────────────────────────────────
        classes = sorted(catalog.keys())
        current_class_idx = classes.index(curr_sel["class"]) if curr_sel["class"] in classes else 0
        selected_class = st.selectbox("Grade Level", classes, index=current_class_idx)

        # ── Subject selection ─────────────────────────────────────────
        subjects = sorted(catalog.get(selected_class, []))
        current_subject_idx = subjects.index(curr_sel["subject"]) if curr_sel["subject"] in subjects else 0
        selected_subject = st.selectbox("Subject", subjects, index=current_subject_idx)

        # ── Sidebar Setup ─────────────────────────────────────────────────────
        is_local  = is_course_available_locally(selected_class, selected_subject)
        is_active = (selected_class == curr_sel["class"] and selected_subject == curr_sel["subject"])

        # Status indicator
        if is_active:
            st.success(f"✅ **Active context** — ready to use")
        elif is_local:
            st.info(f"💾 Downloaded — not active")
        else:
            st.warning(f"☁️ Not downloaded yet")

        # ── Action buttons ────────────────────────────────────────────
        if not is_local:
            # Download + activate
            if st.button(
                f"⬇️ Download & Activate  {selected_class} — {selected_subject}",
                use_container_width=True,
                type="primary"
            ):
                with st.spinner(f"Downloading {selected_class}/{selected_subject}..."):
                    success, msg = asyncio.run(
                        download_course(selected_class, selected_subject)
                    )
                if success:
                    # 1. Flush RAM Cache
                    cache_l1.flush_all()
                    # 2. Switch active context in DLL
                    asyncio.run(controller.switch_course(selected_class, selected_subject))
                    st.session_state.messages = []  # Fresh context
                    st.rerun()
                else:
                    st.error(msg)

        elif not is_active:
            # Already downloaded — just activate
            if st.button(
                f"▶️ Activate  {selected_class} — {selected_subject}",
                use_container_width=True,
                type="primary"
            ):
                # 1. Flush RAM Cache
                cache_l1.flush_all()
                # 2. Activate
                asyncio.run(controller.switch_course(selected_class, selected_subject))
                st.session_state.messages = []  # Fresh context
                st.rerun()

    st.divider()
    st.markdown("### ⚙️ Controls")

    if st.button("🔄 Check for Updates", use_container_width=True):
        with st.spinner("Checking registry..."):
            asyncio.run(sync_with_registry())
            st.session_state.remote_catalog = {}  # Refresh catalog
            st.rerun()

    if st.button("🗑️ Reset Memory", use_container_width=True, type="secondary"):
        with st.spinner("Wiping memory (L1 + L2)..."):
            # 1. Wipe L1 (RAM)
            cache_l1.flush_all()
            # 2. Wipe L2 (Storage)
            asyncio.run(controller.force_reinit_dll())
            st.session_state.messages = []
            st.rerun()

# =====================================================================
# MAIN DASHBOARD
# =====================================================================
st.title("🤖 APU CONTROL CENTER")

# Active context badge
st.markdown(f"""
    <div style='margin-bottom: 1.5rem;'>
        <span class='badge badge-green' style='font-size: 0.9rem; padding: 0.4rem 1rem;'>
            📚 Active Context: {curr_sel['class'].upper()} — {curr_sel['subject'].title()}
        </span>
    </div>
""", unsafe_allow_html=True)

col_mem, col_chat = st.columns([1, 1])

# ── LEFT: L1 + L2 + L3 ───────────────────────────────────────────────
with col_mem:

    # ── L1 CACHE (Hot — in-process RAM, TTL-based) ────────────────────
    st.markdown('<div class="apu-panel"><div class="panel-header">⚡ L1 Cache — Hot (RAM)</div>', unsafe_allow_html=True)
    l1_data = cache_l1.get_all_cached()
    l1_summary = cache_l1.get_summary()
    if l1_data:
        for block_id, content in list(l1_data.items()):
            snippet = str(content)[:160] + ("..." if len(str(content)) > 160 else "")
            metrics = cache_l1.get_metrics().get(block_id, {})
            hit_rate = metrics.get("hit_rate", 0)
            st.markdown(f"""
            <div class="memory-block">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <span class="block-label" style="color:#a5f3fc">{block_id}</span>
                    <span class="badge badge-green">HIT {hit_rate:.0%}</span>
                </div>
                <div class="block-content">{snippet}</div>
            </div>""", unsafe_allow_html=True)
        st.caption(f"🎯 Global hit rate: {l1_summary['global_hit_rate']:.0%} — {l1_summary['cached_blocks']} block(s) in cache")
    else:
        st.caption("L1 is empty — blocks load here when the agent accesses them.")
    st.markdown('</div>', unsafe_allow_html=True)

    # ── L2 MEMORY — DLL ───────────────────────────────────────────────
    st.markdown('<div class="apu-panel"><div class="panel-header">🧠 L2 Memory — DLL</div>', unsafe_allow_html=True)
    nodes = controller.get_all_nodes(dll_state)
    for node in nodes:
        node_id   = node["id"]
        node_type = node.get("type", "temp")
        
        # Color code by type
        badge_cls = "badge-blue"
        if node_type == "fondamental": badge_cls = "badge-green"
        if node_type == "temp": badge_cls = "badge-orange"

        display_text = node.get("content")
        if not display_text:
            try:
                display_text = asyncio.run(lance_driver.get_block_content(node_id))
            except Exception:
                display_text = None

        if not display_text:
            display_text = ", ".join(node.get("keywords", []))

        if not display_text:
            display_text = "<em style='color:#64748b'>Empty — no context archived yet.</em>"

        st.markdown(f"""
        <div class="memory-block">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:5px;">
                <span class="block-label">{node['label']}</span>
                <span class="badge {badge_cls}">{node_type.upper()}</span>
            </div>
            <div class="block-content">{display_text}</div>
        </div>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # ── L3 ARCHIVE — LanceDB ──────────────────────────────────────────
    st.markdown('<div class="apu-panel"><div class="panel-header">💾 L3 Archive — LanceDB</div>', unsafe_allow_html=True)
    try:
        db = get_cached_db()
        tables = [t for t in db.list_tables() if isinstance(t, str)]
        if tables:
            for table_name in tables:
                count = db.open_table(table_name).to_pandas().shape[0]
                st.markdown(f"""
                <div style="display:flex; justify-content:space-between; padding:12px 16px; background:rgba(255,255,255,0.03); border-radius:10px; margin-bottom:8px; border:1px solid rgba(255,255,255,0.05);">
                    <span style="color:#d8b4fe; font-family:'Fira Code', monospace; font-size:0.95rem;">{table_name}</span>
                    <span class="badge badge-blue">{count} vectors</span>
                </div>""", unsafe_allow_html=True)
        else:
            st.caption("No tables yet. Download a course to populate.")
    except Exception as e:
        st.error(f"LanceDB error: {e}")
    st.markdown('</div>', unsafe_allow_html=True)

# ── RIGHT: NEURAL CHAT ────────────────────────────────────────────────
with col_chat:
    st.markdown('<div class="apu-panel"><div class="panel-header">💬 Neural Interface</div>', unsafe_allow_html=True)
    chat_container = st.container(height=420)

    for m in st.session_state.messages:
        with chat_container.chat_message(m["role"]):
            st.markdown(m["content"])

    # Block proposal
    if st.session_state.pending_block_proposal:
        with st.expander("💡 Memory Proposal", expanded=True):
            proposal = st.session_state.pending_block_proposal
            st.write(f"Create entry: **{proposal.get('label', '?')}**?")
            c1, c2, _ = st.columns([1, 1, 3])
            if c1.button("✅ Confirm"):
                asyncio.run(auto_execute_block_proposal(proposal))
                st.session_state.pending_block_proposal = None
                st.success("Entry created!")
                st.rerun()
            if c2.button("❌ Decline"):
                st.session_state.pending_block_proposal = None
                st.rerun()

    # Chat input
    if prompt := st.chat_input("Ask Akili a question..."):
        st.session_state.messages.append({"role": "user", "content": prompt})

        graph = create_agent_graph()
        state = {
            "messages": [HumanMessage(content=prompt)],
            "agent_id": dll_state.get("agent_id"),
            "class_level": curr_sel["class"],
            "subject": curr_sel["subject"],
            "memory_only_mode": False,
            "needs_new_block": "False",
            "proposed_block_config": {}
        }

        with st.spinner("Akili is thinking..."):
            result = asyncio.run(graph.ainvoke(state))
            resp = extract_text(result["messages"][-1].content)
            st.session_state.messages.append({"role": "assistant", "content": resp})

            # ── Write-back to L1 (ALU invalidation pattern) ────────────
            dll_fresh = asyncio.run(controller.load_dll())
            for node_id, node in dll_fresh.get("nodes", {}).items():
                if node.get("content"):
                    cache_l1.set(node_id, node["content"], block_type=node.get("type"))

            if result.get("needs_new_block") == "True":
                st.session_state.pending_block_proposal = result.get("proposed_block_config")

        st.rerun()
