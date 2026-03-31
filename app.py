"""
Polymarket vs Kalshi Arbitrage Dashboard
========================================
Usage:
    streamlit run app.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime, timezone
from scanner import run_scan, get_spread_history

st.set_page_config(page_title="Poly vs Kalshi Arb", page_icon="$", layout="wide", initial_sidebar_state="expanded")

# ═══════════════════════════════════════════════════════════════════════
# CSS
# ═══════════════════════════════════════════════════════════════════════

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
.stApp {
    background: linear-gradient(135deg, #0a1628 0%, #0d1f2d 30%, #0a1a2e 60%, #081520 100%);
    font-family: 'Inter', sans-serif;
}
section[data-testid="stSidebar"] {
    background: rgba(12, 20, 35, 0.95) !important;
    border-right: 1px solid rgba(56, 189, 189, 0.15);
}
div[data-testid="stMetric"] {
    background: rgba(15, 25, 45, 0.7);
    backdrop-filter: blur(12px);
    border: 1px solid rgba(56, 189, 189, 0.2);
    border-radius: 12px;
    padding: 16px 20px;
}
div[data-testid="stMetric"]:hover {
    border-color: rgba(56, 189, 189, 0.5);
    box-shadow: 0 0 20px rgba(56, 189, 189, 0.1);
}
div[data-testid="stMetric"] label { color: rgba(180,200,220,0.6) !important; font-size: 0.75rem !important; text-transform: uppercase; }
div[data-testid="stMetric"] div[data-testid="stMetricValue"] { color: #e8f4f8 !important; font-weight: 600 !important; }
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #1a8a7d, #2bb5a0) !important;
    border: none !important; color: #fff !important; font-weight: 600 !important;
    border-radius: 10px !important; padding: 12px 24px !important;
}
.stButton > button[kind="primary"]:hover { box-shadow: 0 0 25px rgba(43,181,160,0.4) !important; }
h1 { color: #e8f4f8 !important; }
hr { border-color: rgba(56,189,189,0.1) !important; }
div[data-testid="stExpander"] {
    background: rgba(15,25,45,0.4); border: 1px solid rgba(56,189,189,0.1); border-radius: 10px;
}
</style>
""", unsafe_allow_html=True)

# Card CSS (embedded in iframe)
CARD_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
* { margin: 0; padding: 0; box-sizing: border-box; }
body { background: transparent; font-family: 'Inter', sans-serif; color: #d0dce8; padding: 4px 0; }

.card {
    background: rgba(15,25,45,0.65); backdrop-filter: blur(16px);
    border: 1px solid rgba(56,189,189,0.15); border-radius: 14px;
    padding: 22px 26px; margin: 14px 0; transition: all 0.3s;
}
.card:hover { border-color: rgba(56,189,189,0.45); box-shadow: 0 4px 35px rgba(56,189,189,0.1); transform: translateY(-2px); }
.card-positive { border-left: 3px solid #5ce0d2; }

.header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 14px; }
.title { font-size: 1.1em; font-weight: 600; color: #e8f4f8; margin: 6px 0 2px; }
.subtitle { color: rgba(180,200,220,0.45); font-size: 0.78em; margin-bottom: 2px; }
.badge { display: inline-block; padding: 3px 10px; border-radius: 6px; font-size: 0.68em; font-weight: 600; letter-spacing: 0.4px; margin-right: 6px; text-transform: uppercase; }
.badge-cat { background: rgba(77,171,247,0.15); color: #4dabf7; border: 1px solid rgba(77,171,247,0.3); }
.badge-conf { background: rgba(92,224,210,0.12); color: #5ce0d2; border: 1px solid rgba(92,224,210,0.25); }
.badge-high { background: rgba(92,224,210,0.12); color: #5ce0d2; border: 1px solid rgba(92,224,210,0.25); }
.badge-medium { background: rgba(240,192,64,0.12); color: #f0c040; border: 1px solid rgba(240,192,64,0.25); }
.badge-low { background: rgba(224,96,96,0.12); color: #e06060; border: 1px solid rgba(224,96,96,0.25); }
.warning-bar { background: rgba(224,96,96,0.08); border: 1px solid rgba(224,96,96,0.2); border-radius: 8px; padding: 8px 14px; margin: 8px 0; font-size: 0.82em; color: #e06060; }
.card-low { border-left-color: #e06060 !important; opacity: 0.7; }
.card-medium { border-left-color: #f0c040 !important; }
.card-high { border-left-color: #5ce0d2 !important; }

.stats { text-align: right; }
.roi { font-size: 1.7em; font-weight: 700; color: #5ce0d2; line-height: 1.2; }
.profit { font-size: 1em; font-weight: 600; color: #4ade80; margin-top: 2px; }
.irr { font-size: 0.82em; color: rgba(180,200,220,0.5); margin-top: 2px; }

.legs { display: flex; gap: 14px; margin: 14px 0; }
.leg { flex: 1; background: rgba(10,18,35,0.65); border: 1px solid rgba(56,189,189,0.1); border-radius: 10px; padding: 16px 18px; }
.leg:hover { border-color: rgba(56,189,189,0.35); }
.leg-label { color: rgba(180,200,220,0.35); font-size: 0.68em; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }
.leg-action { color: #5ce0d2; font-weight: 700; font-size: 0.95em; margin-bottom: 4px; }
.leg a { color: #5ce0d2; text-decoration: none; font-weight: 600; border-bottom: 1px dashed rgba(92,224,210,0.3); }
.leg a:hover { color: #8df0e4; border-bottom-color: #8df0e4; }
.leg-detail { color: rgba(180,200,220,0.7); font-size: 0.85em; line-height: 1.7; }
.leg-detail b { color: #d0dce8; }
.leg-bottom { margin-top: 10px; padding-top: 10px; border-top: 1px solid rgba(56,189,189,0.08); display: flex; justify-content: space-between; font-size: 0.85em; }
.leg-stake { font-weight: 600; color: #e8f4f8; }
.leg-payout { font-weight: 600; color: #4ade80; }

.expiry-bar { background: rgba(10,18,35,0.5); border: 1px solid rgba(56,189,189,0.08); border-radius: 8px; padding: 10px 16px; margin: 10px 0; display: flex; justify-content: space-between; align-items: center; font-size: 0.82em; color: rgba(180,200,220,0.5); }
.expiry-bar b { color: #d0dce8; }
.expiry-bar .days { color: #f0c040; font-weight: 600; }

.explain { background: rgba(92,224,210,0.04); border: 1px solid rgba(92,224,210,0.12); border-radius: 10px; padding: 16px 20px; margin: 14px 0 0; font-size: 0.85em; line-height: 1.75; color: rgba(200,215,230,0.75); }
.explain b { color: #d0dce8; }
.explain .step { display: block; padding: 4px 0 4px 16px; border-left: 2px solid rgba(92,224,210,0.15); margin: 6px 0; }
.explain .outcome { border-left-color: rgba(74,222,128,0.25); }
.explain .result { border-left-color: #5ce0d2; color: #5ce0d2; font-weight: 500; }

.footer { margin-top: 14px; padding-top: 12px; border-top: 1px solid rgba(56,189,189,0.1); display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; }
.footer-stat { color: rgba(180,200,220,0.55); font-size: 0.82em; }
.footer-stat b { color: #d0dce8; }
.match-info { color: rgba(180,200,220,0.3); font-size: 0.72em; font-style: italic; }

.history { background: rgba(10,18,35,0.5); border: 1px solid rgba(56,189,189,0.08); border-radius: 8px; padding: 12px 16px; margin-top: 10px; }
.history-title { color: rgba(180,200,220,0.4); font-size: 0.72em; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }
.history-row { display: flex; justify-content: space-between; font-size: 0.78em; color: rgba(180,200,220,0.5); padding: 2px 0; border-bottom: 1px solid rgba(56,189,189,0.04); }
.history-row b { color: #d0dce8; }
.spread-up { color: #4ade80; }
.spread-down { color: #e06060; }

.page-footer { text-align: center; color: rgba(180,200,220,0.25); font-size: 0.78em; padding: 30px 0 10px; border-top: 1px solid rgba(56,189,189,0.08); margin-top: 20px; }
"""


# ═══════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### POLY vs KALSHI")
    st.markdown("---")

    wager = st.number_input("Wager per arb ($)", min_value=10, max_value=100000, value=500, step=100)
    min_roi = st.slider("Min ROI %", 0.0, 20.0, 0.3, 0.1)
    min_irr = st.slider("Min Annualized IRR %", 0.0, 100.0, 0.0, 1.0)

    st.markdown("---")
    st.markdown("##### Confidence")
    show_high = st.checkbox("High", value=True)
    show_medium = st.checkbox("Medium", value=True)
    show_low = st.checkbox("Low (likely mismatches)", value=False)

    st.markdown("---")
    scan_btn = st.button("SCAN NOW", type="primary", use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════════════

st.markdown('<h1>Polymarket vs <span style="color:#5ce0d2">Kalshi</span> Arbitrage</h1>', unsafe_allow_html=True)
st.markdown('<p style="color:rgba(180,200,220,0.5);margin-top:-10px">All prediction markets &middot; Strict matching &middot; Fee-adjusted &middot; IRR tracking</p>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════
# STATE + SCAN
# ═══════════════════════════════════════════════════════════════════════

if "pk_results" not in st.session_state:
    st.session_state.pk_results = None

if scan_btn:
    with st.spinner("Scanning Polymarket + Kalshi..."):
        st.session_state.pk_results = run_scan()

results = st.session_state.pk_results

if results is None:
    components.html(f"""<html><head><style>{CARD_CSS}</style></head><body>
    <div style="text-align:center;padding:60px;color:rgba(180,200,220,0.4)">
        <div style="font-size:3em;margin-bottom:16px">$</div>
        <div style="font-size:1.1em">Click <b>SCAN NOW</b> to find Polymarket vs Kalshi arbs</div>
    </div></body></html>""", height=200)
    st.stop()


# ═══════════════════════════════════════════════════════════════════════
# METRICS
# ═══════════════════════════════════════════════════════════════════════

arbs = results["arbs"]
# Confidence filter
conf_filter = set()
if show_high: conf_filter.add("HIGH")
if show_medium: conf_filter.add("MEDIUM")
if show_low: conf_filter.add("LOW")
arbs = [a for a in arbs if a.get("confidence", "MEDIUM") in conf_filter]
arbs = [a for a in arbs if a["roi"] >= min_roi]
if min_irr > 0:
    arbs = [a for a in arbs if a.get("irr") is not None and a["irr"] >= min_irr]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Arb Opportunities", len(arbs))
col2.metric("Polymarket Markets", results["n_poly"])
col3.metric("Kalshi Markets", results["n_kalshi"])
best_roi = max((a["roi"] for a in arbs), default=0)
col4.metric("Best ROI", f"{best_roi:.2f}%")

scan_dt = datetime.fromisoformat(results["scan_time"])
st.markdown(f'<p style="color:rgba(180,200,220,0.4);font-size:0.82em;margin:8px 0 16px">'
            f'Last scan: {scan_dt.strftime("%Y-%m-%d %H:%M:%S UTC")} &middot; '
            f'Showing {len(arbs)} arbs &ge; {min_roi}% ROI</p>', unsafe_allow_html=True)

with st.expander("Scan Log"):
    for line in results["log"]:
        st.text(line)


# ═══════════════════════════════════════════════════════════════════════
# ARB CARDS
# ═══════════════════════════════════════════════════════════════════════

if not arbs:
    st.info("No arbs found at current filters. Try lowering min ROI.")
    st.stop()

cards_html = ""
for arb in arbs:
    roi = arb["roi"]
    net = arb["net"]
    irr = arb.get("irr")
    days = arb.get("days")
    expiry = arb.get("expiry", "Unknown")
    category = arb.get("category", "").replace("_", " ").title()
    match_score = arb.get("match_score", 0)

    # Scale to wager
    cost_per_unit = arb["cost"] + arb["fees"]
    units = wager / cost_per_unit if cost_per_unit > 0 else 0
    profit = net * units
    stake_poly = arb["poly_price"] * units
    stake_kalshi = arb["kalshi_price"] * units
    payout = units  # $1 per unit

    poly_url = f'https://polymarket.com/event/{arb["poly_slug"]}'
    if arb.get("poly_token"):
        poly_url += f'?tid={arb["poly_token"]}'
    kalshi_url = f'https://kalshi.com/markets/{arb["kalshi_ticker"]}'

    # IRR display
    irr_html = f'<div class="irr">IRR: {irr:.0f}% annualized</div>' if irr else ''

    # Expiry bar
    days_str = f'{days:.0f} days' if days and days > 1 else f'{days*24:.0f} hours' if days else '?'
    expiry_html = f"""
    <div class="expiry-bar">
        <span>Settles: <b>{expiry}</b></span>
        <span>Time to expiry: <span class="days">{days_str}</span></span>
        {'<span>IRR: <b style="color:#5ce0d2">' + f'{irr:.0f}%' + '</b> annualized</span>' if irr else ''}
    </div>"""

    # Explanation
    explain_html = f"""
    <div class="explain">
        <b>How it works:</b> The same prediction is priced differently on Polymarket vs Kalshi.
        You buy opposite sides &mdash; one <i>must</i> pay $1 per contract.
        <span class="step"><b>Step 1:</b> {arb['poly_action']} on
        <a href="{poly_url}" target="_blank">Polymarket</a> for <b>${stake_poly:,.2f}</b>.
        If this side wins, you get <b>${payout:,.2f}</b>.</span>
        <span class="step"><b>Step 2:</b> {arb['kalshi_action']} on
        <a href="{kalshi_url}" target="_blank">Kalshi</a> for <b>${stake_kalshi:,.2f}</b>.
        If this side wins, you get <b>${payout:,.2f}</b>.</span>
        <span class="step outcome">&#10003; <b>Outcome A:</b> Polymarket pays ${payout:,.2f}.
        Total spent: ${stake_poly+stake_kalshi:,.2f}. Fees: ${arb['fees']*units:,.2f}.
        Net = <b>${payout - stake_poly - stake_kalshi - arb['fees']*units:,.2f}</b></span>
        <span class="step outcome">&#10003; <b>Outcome B:</b> Kalshi pays ${payout:,.2f}.
        Total spent: ${stake_poly+stake_kalshi:,.2f}. Fees: ${arb['fees']*units:,.2f}.
        Net = <b>${payout - stake_poly - stake_kalshi - arb['fees']*units:,.2f}</b></span>
        <span class="step result"><b>Either way you profit ${profit:,.2f} ({roi:.2f}% ROI{f', {irr:.0f}% annualized' if irr else ''})</b></span>
        <b>Why:</b> Polymarket prices one side at ${arb['poly_price']:.3f} and Kalshi prices the other at ${arb['kalshi_price']:.2f}.
        Combined cost = ${arb['cost']:.3f} + ${arb['fees']:.4f} fees = ${cost_per_unit:.3f} for a $1.00 payout.
        The ${net:.4f} gap per contract is your guaranteed profit.
    </div>"""

    # Spread history
    history = get_spread_history(arb["kalshi_ticker"], arb["direction"], limit=10)
    history_html = ""
    if len(history) > 1:
        rows = ""
        prev_spread = None
        for h in history[-8:]:
            ts = h["timestamp"][:16].replace("T", " ")
            sp = h["spread"]
            r = h["roi"]
            arrow = ""
            if prev_spread is not None:
                if sp > prev_spread:
                    arrow = '<span class="spread-up">&#9650;</span>'
                elif sp < prev_spread:
                    arrow = '<span class="spread-down">&#9660;</span>'
            prev_spread = sp
            irr_h = f'{h["irr"]:.0f}%' if h["irr"] else '-'
            rows += f'<div class="history-row"><span>{ts}</span><span>Spread: <b>{sp:.4f}</b> {arrow}</span><span>ROI: <b>{r:.2f}%</b></span><span>IRR: {irr_h}</span></div>'

        history_html = f"""
        <div class="history">
            <div class="history-title">Spread History (widening = better, narrowing = closing)</div>
            {rows}
        </div>"""

    confidence = arb.get('confidence', 'MEDIUM')
    conf_class = confidence.lower()
    warning = arb.get('warning', '')
    warning_html = f'<div class="warning-bar">&#9888; {warning}</div>' if warning else ''

    cards_html += f"""
    <div class="card card-{conf_class}">
        <div class="header">
            <div>
                <span class="badge badge-cat">{category}</span>
                <span class="badge badge-{conf_class}">{confidence}</span>
                <span class="badge badge-conf">Match: {match_score:.0%}</span>
                <div class="title">{arb['poly_question']}</div>
                <div class="subtitle">Kalshi: {arb['kalshi_title']} ({arb['kalshi_event']})</div>
            </div>
            <div class="stats">
                <div class="roi">{roi:.2f}%</div>
                <div class="profit">+${profit:,.2f}</div>
                {irr_html}
            </div>
        </div>
        {warning_html}
        {expiry_html}
        <div class="legs">
            <div class="leg">
                <div class="leg-label">Polymarket</div>
                <div class="leg-action">Buy {arb['poly_side']}</div>
                <div><a href="{poly_url}" target="_blank">Open on Polymarket &rarr;</a></div>
                <div class="leg-detail">Price: <b>${arb['poly_price']:.3f}</b> ({arb['poly_price']*100:.1f}%)</div>
                <div class="leg-bottom">
                    <span class="leg-stake">${stake_poly:,.2f}</span>
                    <span class="leg-payout">${payout:,.2f}</span>
                </div>
            </div>
            <div class="leg">
                <div class="leg-label">Kalshi</div>
                <div class="leg-action">Buy {arb['kalshi_side']}</div>
                <div><a href="{kalshi_url}" target="_blank">Open on Kalshi &rarr;</a></div>
                <div class="leg-detail">Price: <b>${arb['kalshi_price']:.2f}</b> ({arb['kalshi_price']*100:.1f}%)</div>
                <div class="leg-bottom">
                    <span class="leg-stake">${stake_kalshi:,.2f}</span>
                    <span class="leg-payout">${payout:,.2f}</span>
                </div>
            </div>
        </div>
        {explain_html}
        {history_html}
        <div class="footer">
            <div class="footer-stat">Wager: <b>${wager:,.2f}</b> &nbsp;&nbsp; Fees: <b>${arb['fees']*units:,.2f}</b> &nbsp;&nbsp; Guaranteed: <b style="color:#4ade80">${profit:,.2f}</b></div>
            <div class="match-info">Poly vol: ${arb.get('poly_volume',0):,.0f} &middot; Kalshi vol: ${arb.get('kalshi_volume',0):,.0f}</div>
        </div>
    </div>"""

full_html = f"<html><head><style>{CARD_CSS}</style></head><body>{cards_html}"
full_html += '<div class="page-footer">Prices are live snapshots. Both platforms charge fees (included in calculations).<br>'
full_html += 'Polymarket: ~2% effective fee | Kalshi: up to 7% fee | All profits shown are after fees.</div>'
full_html += "</body></html>"

card_height = max(400, len(arbs) * 550 + 100)
components.html(full_html, height=card_height, scrolling=True)
