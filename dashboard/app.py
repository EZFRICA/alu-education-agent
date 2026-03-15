import streamlit as st
import sys
import os
import asyncio
import re
from datetime import datetime

# Root path alignment
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from agent.agent_graph_dll import create_dll_agent_graph
from memory.dll_manager import load_dll, save_dll, get_all_nodes, toggle_block, update_node_keywords
from memory.block_factory import create_dynamic_block, update_block_content, delete_block_stitching
from memory.letta_cloud_client import update_block, delete_block
from memory import letta_cloud_client as letta_client
from memory import weaviate_cloud_client as wcd_client
from memory.context_compiler import get_core_block_content

st.set_page_config(page_title="Travel Agent — DLL Dashboard", page_icon="✈️", layout="wide")

# --- Initialize Session State ---
if "messages" not in st.session_state:
    st.session_state.messages = []
if "langchain_history" not in st.session_state:
    st.session_state.langchain_history = []
if "agent_app" not in st.session_state:
    st.session_state.agent_app = create_dll_agent_graph()
if "pending_proposal" not in st.session_state:
    st.session_state.pending_proposal = None
if "memory_facts" not in st.session_state:
    st.session_state.memory_facts = {}
if "last_injected_ids" not in st.session_state:
    st.session_state.last_injected_ids = []

# Load local DLL state
dll_state = load_dll()
agent_id = dll_state.get("agent_id")

if not agent_id:
    st.error("No Letta agent_id defined. Please run the agent creation script.")
    st.stop()

# --- SIDEBAR : NAVIGATION & OPTIONS ---
with st.sidebar:
    st.title("🧠 UX-Memory")
    st.info(f"Active DLL : {len(get_all_nodes(dll_state))}/12 blocks")
    st.divider()
    
    search_on = st.toggle("🔍 Search Online (Gemini Grounding)", value=False, help="Enables real-time Google search")
    st.divider()
    
    if st.button("🗑️ Reset Chat", use_container_width=True):
        st.session_state.messages = []
        st.session_state.langchain_history = []
        st.session_state.pending_proposal = None
        st.rerun()

# Initial sync of memory if empty
if not st.session_state.memory_facts:
    with st.spinner("Syncing Letta Memory..."):
        nodes = get_all_nodes(dll_state)
        for node in nodes:
            content = get_core_block_content(agent_id, node['id'])
            st.session_state.memory_facts[node['id']] = content

# --- HEADER STATISTICS ---
st.title("✈️ Travel Agent — DLL Memory Dashboard")
m1, m2, m3 = st.columns(3)
m1.metric("Dynamic Blocks", f"{dll_state['dynamic_block_count']} / {dll_state['dynamic_block_max']}")
m2.metric("Fixed Blocks", "4")
m3.metric("Injected into LLM Context", f"{len(st.session_state.last_injected_ids)}")

st.divider()

# --- MAIN LAYOUT : TWO COLUMNS ---
col_left, col_right = st.columns([1, 1])

# ── LEFT COL: DLL STRUCTURE ──
with col_left:
    st.subheader("📚 DLL Structure — HEAD → TAIL")
    
    with st.expander("➕ Create a new dynamic block (manual)", expanded=False):
        col_a, col_b = st.columns(2)
        with col_a:
            m_label = st.text_input("Label", placeholder="ex: Restaurants Porto", key="manual_label")
            m_type = st.selectbox("Type", ["temp", "projet", "fondamental"], key="manual_type")
        with col_b:
            m_keywords = st.text_input("Keywords (comma separated)", key="manual_kw")
            m_content = st.text_area("Initial content", key="manual_content")
            
        if st.button("🚀 Create Block", use_container_width=True):
            if m_label and m_content:
                m_id = m_label.lower().replace(" ", "_")
                kw_list = [k.strip() for k in m_keywords.split(",") if k.strip()]
                try:
                    dll_state = create_dynamic_block(
                        m_id, m_label, m_type, m_content, kw_list, "user_manual",
                        dll_state, letta_client, wcd_client
                    )
                    st.session_state.memory_facts[m_id] = m_content
                    st.success(f"Block '{m_label}' created!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

    st.markdown("### Current order: [HEAD] → [TAIL]")
    nodes = get_all_nodes(dll_state)
    
    for idx_pos, node in enumerate(nodes):
        b_id = node["id"]
        is_active = node.get("active", True)
        is_fixed = node.get("is_fixed", False)
        status_icon = "🟢" if is_active else "🔴"
        badge = "📌 Fixed" if is_fixed else "🔄 Dynamic"
        
        is_injected = b_id in st.session_state.last_injected_ids
        live_content = st.session_state.memory_facts.get(b_id, "")
        preview = f" — \"{live_content[:50]}...\"" if live_content else " (Empty)"
        
        with st.expander(f"{status_icon} [{idx_pos}] {node['label']} — {node['type']} | {badge}", expanded=is_injected):
            c1, c2 = st.columns([2, 1])
            with c1:
                st.markdown("**Vectorized keywords:** " + ", ".join([f"`{k}`" for k in node.get("keywords", [])]))
                new_kw_str = st.text_input("Update keywords", value=", ".join(node.get("keywords", [])), key=f"kw_in_{b_id}")
                if st.button("Re-vectorize", key=f"vec_btn_{b_id}"):
                    new_kw_list = [k.strip() for k in new_kw_str.split(",") if k.strip()]
                    dll_state = update_node_keywords(b_id, new_kw_list, dll_state)
                    save_dll(dll_state)
                    st.success("Re-vectorized!")
                    st.rerun()
                
                curr_txt = st.text_area("Block content (Letta Core Memory)", value=live_content, height=150, key=f"txt_in_{b_id}")
                if st.button("💾 Save to Letta", key=f"save_btn_{b_id}", type="primary"):
                    update_block(agent_id, b_id, curr_txt)
                    st.session_state.memory_facts[b_id] = curr_txt
                    st.success("Saved content!")
                    st.rerun()
            
            with c2:
                if is_fixed:
                    st.info("📌 Fixed Block")
                    st.caption("Always included in memory")
                else:
                    active_tog = st.checkbox("📌 Force Include (Override)", value=is_active, help="Forces the injection of this dynamic block to the AI.", key=f"act_tog_{b_id}")
                    if active_tog != is_active:
                        toggle_block(b_id, active_tog, dll_state)
                        save_dll(dll_state)
                        st.rerun()
                
                if not is_fixed:
                    if st.button("🗑️ Delete", key=f"del_btn_{b_id}", use_container_width=True):
                        dll_state = delete_block_stitching(b_id, dll_state)
                        save_dll(dll_state)
                        delete_block(agent_id, b_id)
                        st.rerun()

# ── RIGHT COL: CHAT ──
with col_right:
    st.subheader("💬 Travel Agent (LLM)")
    chat_container = st.container(height=650)
    
    with chat_container:
        # History
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
        
        # Proposals
        if st.session_state.pending_proposal:
            prop = st.session_state.pending_proposal
            with st.chat_message("assistant"):
                st.info(prop["proposal_message"])
                p1, p2 = st.columns(2)
                if p1.button(f"✅ Create '{prop['label']}'", key="create_prop"):
                    try:
                        create_dynamic_block(
                            prop["proposed_id"], prop["label"], prop["type"], 
                            prop["initial_content"], prop["keywords"], "agent_proposal",
                            dll_state, letta_client, wcd_client
                        )
                        st.session_state.memory_facts[prop["proposed_id"]] = prop["initial_content"]
                        st.session_state.pending_proposal = None
                        st.rerun()
                    except Exception as e: st.error(str(e))
                if p2.button("❌ Ignore", key="ignore_prop"):
                    st.session_state.pending_proposal = None
                    st.rerun()

# --- CHAT INPUT (Page Bottom) ---
if prompt := st.chat_input("Ask the architect your question..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.session_state.langchain_history.append(HumanMessage(content=prompt))
    
    # Rerender immediately to show user message
    st.rerun()

# --- ASYNC AGENT PROCESSING ---
if st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
    with col_right:
        with chat_container:
            with st.chat_message("assistant"):
                with st.spinner("Agent is traversing the DLL..."):
                    # Fresh graph each time to avoid loop issues
                    agent_app = create_dll_agent_graph()
                    inputs = {
                        "messages": st.session_state.langchain_history, 
                        "agent_id": agent_id,
                        "search_enabled": search_on
                    }
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(agent_app.ainvoke(inputs))
                        
                        new_messages = result.get("messages", [])
                        resp_text = new_messages[-1].content
                        
                        match = re.search(r"Memory: (.*?) \|", resp_text)
                        if match:
                            injected_str = match.group(1)
                            st.session_state.last_injected_ids = [s.strip() for s in injected_str.split("+") if s.strip() and s.strip() != "none"]
                        
                        st.session_state.messages.append({"role": "assistant", "content": resp_text})
                        st.session_state.langchain_history = new_messages
                        
                        # Set proposal if any
                        if result.get("needs_new_block") == "True":
                            st.session_state.pending_proposal = result.get("proposed_block_config")
                            
                        # Final sync of memory facts
                        dll_state = load_dll()  # Reload to get MTF changes
                        for b_id in dll_state["nodes"]:
                             content = get_core_block_content(agent_id, b_id)
                             st.session_state.memory_facts[b_id] = content
                        
                        st.rerun()
                    finally:
                        loop.close()
