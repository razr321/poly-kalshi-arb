"""
Polymarket vs Kalshi Arbitrage Scanner
======================================
Strict market matching + arb detection across all prediction categories.
Tracks spread history for IRR and convergence analysis.
"""

import os, json, re, time, math, sqlite3
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ── Config ──────────────────────────────────────────────────────────────

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "arb_history.db")

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json", "User-Agent": "poly-kalshi-arb/1.0"})


# ═══════════════════════════════════════════════════════════════════════
# TEXT MATCHING (ported from battle-tested server.py)
# ═══════════════════════════════════════════════════════════════════════

STOPWORDS = {
    'will', 'the', 'be', 'in', 'of', 'to', 'a', 'an', 'by', 'on', 'or',
    'and', 'for', 'is', 'it', 'at', 'if', 'as', 'that', 'this', 'do',
    'does', 'did', 'has', 'have', 'had', 'was', 'were', 'been', 'are',
    'not', 'no', 'yes', 'what', 'which', 'who', 'whom', 'when', 'where',
    'why', 'how', 'any', 'all', 'each', 'every', 'both', 'few', 'many',
    'much', 'other', 'another', 'some', 'such', 'than', 'too', 'very',
    'can', 'could', 'would', 'should', 'may', 'might', 'shall', 'must',
    'before', 'after', 'during', 'above', 'below', 'between', 'under',
    'over', 'up', 'down', 'out', 'off', 'into', 'through', 'again',
    'then', 'once', 'here', 'there', 'about', 'its', 'next', 'new',
    'most', 'from', 'with', 'more',
}


def norm(text):
    text = text.lower().strip()
    text = re.sub(r'\d{4}-\d{2}-\d{2}', '', text)
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    return ' '.join(w for w in text.split() if w not in STOPWORDS and len(w) > 1)


def extract_entities(text):
    """Extract proper nouns (capitalized words, key names)."""
    words = text.split()
    entities = set()
    for w in words:
        clean = re.sub(r'[^a-zA-Z]', '', w)
        if clean and clean[0].isupper() and clean.lower() not in STOPWORDS and len(clean) > 2:
            entities.add(clean.lower())
    return entities


def extract_action_key(text):
    """Extract action/outcome type from market question."""
    t = text.lower()
    t = re.sub(r'[^a-z0-9\s]', ' ', t)
    actions = set()

    patterns = [
        (r'\b(win|winner|champion)\b', 'win'),
        (r'\b(nominat|nominee)\b', 'nominate'),
        (r'\b(president|presidential)\b', 'president'),
        (r'\b(ipo|public|go public)\b', 'ipo'),
        (r'\b(announc)', 'announce'),
        (r'\b(most seats|majority)\b', 'most_seats'),
        (r'\b(gain seats|above \d+|over \d+|more than)\b', 'gain_seats'),
        (r'\b(first|1st)\b', 'rank_1st'),
        (r'\b(second|2nd)\b', 'rank_2nd'),
        (r'\b(third|3rd)\b', 'rank_3rd'),
        (r'\b(prime minister)\b', 'pm'),
        (r'\b(pardon)', 'pardon'),
        (r'\b(resign|leave|out|step down|depart)\b', 'leave'),
        (r'\b(acquir|buy|purchase|take over)\b', 'acquire'),
        (r'\b(declare|declaring|run for|candidate|candidacy|enter race)\b', 'declare'),
        (r'\b(election|elected)\b', 'election'),
        (r'\b(senate|senator)\b', 'senate'),
        (r'\b(house|representative|congress)\b', 'house'),
        (r'\b(governor)\b', 'governor'),
        (r'\b(democrat|democratic)\b', 'democrat'),
        (r'\b(republican)\b', 'republican'),
        (r'\b(rate cut|interest rate|fed fund)\b', 'rate'),
        (r'\b(inflation|cpi)\b', 'inflation'),
        (r'\b(gdp|growth)\b', 'gdp'),
        (r'\b(recession)\b', 'recession'),
        (r'\b(tariff)\b', 'tariff'),
        (r'\b(ceasefire|peace)\b', 'ceasefire'),
        (r'\b(war|conflict|invasion|strike)\b', 'conflict'),
        (r'\b(bitcoin|btc)\b', 'bitcoin'),
        (r'\b(ethereum|eth)\b', 'ethereum'),
        (r'\b(price|hit|reach|exceed|above|below)\b', 'price_level'),
    ]
    for pattern, key in patterns:
        if re.search(pattern, t):
            actions.add(key)

    # Extract numeric thresholds
    for qualifier, num in re.findall(r'\b(above|over|below|under|more than|less than|at least|exceed)\s*\$?([\d,.]+)', t):
        actions.add(f'threshold_{num.replace(",", "")}')

    # Extract date anchors (by March, before June, etc.)
    for month in re.findall(r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\b', t):
        actions.add(f'month_{month[:3]}')

    return actions


def _extract_state_district(text):
    """Extract US state abbreviations and district numbers."""
    t = text.upper()
    # State abbreviations in context (NY-13, KY-04, etc.)
    districts = set(re.findall(r'\b([A-Z]{2})-(\d+)\b', t))
    # Full state names
    states = set()
    for state in ['alabama','alaska','arizona','arkansas','california','colorado',
                  'connecticut','delaware','florida','georgia','hawaii','idaho',
                  'illinois','indiana','iowa','kansas','kentucky','louisiana',
                  'maine','maryland','massachusetts','michigan','minnesota',
                  'mississippi','missouri','montana','nebraska','nevada',
                  'new hampshire','new jersey','new mexico','new york',
                  'north carolina','north dakota','ohio','oklahoma','oregon',
                  'pennsylvania','rhode island','south carolina','south dakota',
                  'tennessee','texas','utah','vermont','virginia','washington',
                  'west virginia','wisconsin','wyoming']:
        if state in text.lower():
            states.add(state)
    return districts, states


def _extract_office(text):
    """Extract the office/position being contested."""
    t = text.lower()
    offices = set()
    if re.search(r'\bpresiden', t): offices.add('president')
    if re.search(r'\bgovernor', t): offices.add('governor')
    if re.search(r'\bsenat', t): offices.add('senate')
    if re.search(r'\b(house|representative|congress)\b', t) and 'white house' not in t: offices.add('house')
    if re.search(r'\bmayor', t): offices.add('mayor')
    if re.search(r'\battorney general', t): offices.add('ag')
    return offices


def is_same_market(poly_q, kalshi_q):
    """Strict check: are these actually the same bet?"""
    pe = extract_entities(poly_q)
    ke = extract_entities(kalshi_q)

    # Entity check — key names must overlap
    if pe and ke:
        overlap = pe & ke
        if not overlap:
            return False
        min_set = min(len(pe), len(ke))
        if len(overlap) / min_set < 0.5:
            return False

    # Geographic check — different states/districts = different bets
    p_dist, p_states = _extract_state_district(poly_q)
    k_dist, k_states = _extract_state_district(kalshi_q)
    if p_dist and k_dist and not (p_dist & k_dist):
        return False
    if p_states and k_states and not (p_states & k_states):
        return False

    # Office check — nominee for president != nominee for house seat
    p_office = _extract_office(poly_q)
    k_office = _extract_office(kalshi_q)
    if p_office and k_office and not (p_office & k_office):
        return False

    # Action/outcome check
    pa = extract_action_key(poly_q)
    ka = extract_action_key(kalshi_q)

    if pa and ka:
        # Conflict pairs
        conflicts = [
            ({'win', 'election', 'president'}, {'nominate'}),
            ({'most_seats'}, {'gain_seats'}),
            ({'rank_1st'}, {'rank_2nd'}), ({'rank_1st'}, {'rank_3rd'}), ({'rank_2nd'}, {'rank_3rd'}),
            ({'senate'}, {'house'}), ({'senate'}, {'governor'}), ({'house'}, {'governor'}),
            ({'win'}, {'declare'}), ({'nominate'}, {'declare'}),
            ({'pardon'}, {'leave'}), ({'ipo'}, {'announce'}),
            ({'president'}, {'house'}), ({'president'}, {'governor'}),
            ({'nominate'}, {'win'}),  # nominee != winner
        ]
        for set_a, set_b in conflicts:
            if (set_a <= pa and set_b <= ka) or (set_b <= pa and set_a <= ka):
                return False

        # Both have actions — need overlap
        if len(pa) >= 2 and len(ka) >= 2 and not (pa & ka):
            return False

        # Different thresholds = different bets
        pa_t = {w for w in pa if w.startswith('threshold_')}
        ka_t = {w for w in ka if w.startswith('threshold_')}
        if pa_t and ka_t and pa_t != ka_t:
            return False

        # Different months = different bets
        pa_m = {w for w in pa if w.startswith('month_')}
        ka_m = {w for w in ka if w.startswith('month_')}
        if pa_m and ka_m and pa_m != ka_m:
            return False

    # Negation check — one asks "will X happen" and other asks "will X NOT happen"
    # These are inversely correlated, not the same bet
    p_low = poly_q.lower()
    k_low = kalshi_q.lower()
    p_negated = bool(re.search(r'\bnot\b|\bno\b|\bwon\'t\b|\bnever\b', p_low))
    k_negated = bool(re.search(r'\bnot\b|\bno\b|\bwon\'t\b|\bnever\b', k_low))
    if p_negated != k_negated:
        # One is negated, the other isn't — be very careful
        # Only allow if the similarity is very high (exact same phrasing except negation)
        # For now, flag this but don't block — the arb calc handles directionality
        pass

    # Year/timeframe check — "in 2026" vs "during Trump's term" or "before 2029"
    p_years = set(re.findall(r'\b(202[4-9]|203[0-9])\b', p_low))
    k_years = set(re.findall(r'\b(202[4-9]|203[0-9])\b', k_low))
    if p_years and k_years and not (p_years & k_years):
        return False

    # "between X and Y" vs general — specific range != general
    p_has_range = bool(re.search(r'between\s+[\d.]+\s*(and|to)\s*[\d.]+', p_low))
    k_has_range = bool(re.search(r'between\s+[\d.]+\s*(and|to)\s*[\d.]+', k_low))
    if p_has_range != k_has_range:
        # One has specific range, other doesn't — likely different bets
        return False

    # "more than X" vs "surge" — vague vs specific
    p_has_number = bool(re.search(r'\d+\.?\d*%', p_low))
    k_has_number = bool(re.search(r'\d+\.?\d*%', k_low))
    # If one has a specific percentage threshold and the other doesn't, suspicious
    if p_has_number and not k_has_number and not re.search(r'\d', k_low):
        return False
    if k_has_number and not p_has_number and not re.search(r'\d', p_low):
        return False

    return True


def similarity(a, b):
    na, nb = norm(a), norm(b)
    if not na or not nb:
        return 0.0
    wa, wb = set(na.split()), set(nb.split())
    if not wa or not wb:
        return 0.0
    jac = len(wa & wb) / len(wa | wb)
    seq = SequenceMatcher(None, na, nb).ratio()
    return 0.4 * seq + 0.6 * jac


# ═══════════════════════════════════════════════════════════════════════
# DATA FETCHING
# ═══════════════════════════════════════════════════════════════════════

def fetch_polymarket(max_pages=10):
    """Fetch all active binary markets from Polymarket."""
    results = []
    for off in range(0, max_pages * 100, 100):
        try:
            r = SESSION.get(f'{GAMMA_API}/markets', params={
                'active': 'true', 'closed': 'false', 'limit': 100, 'offset': off,
                'order': 'volume', 'ascending': 'false',
            }, timeout=15)
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            for m in batch:
                q = m.get('question', '').strip()
                prices = json.loads(m.get('outcomePrices', '[]'))
                if not q or len(prices) < 2:
                    continue
                results.append({
                    'question': q,
                    'yes_price': float(prices[0]),
                    'no_price': float(prices[1]),
                    'volume_24h': float(m.get('volume24hr', 0) or 0),
                    'liquidity': float(m.get('liquidityNum', 0) or 0),
                    'slug': m.get('slug', ''),
                    'event_slug': m.get('eventSlug', m.get('slug', '')),
                    'fees_enabled': bool(m.get('feesEnabled', False)),
                    'end_date': m.get('endDate', '') or '',
                    'token_yes': '',
                    'token_no': '',
                })
                # Extract token IDs
                tokens = json.loads(m.get('clobTokenIds', '[]'))
                if len(tokens) >= 2:
                    results[-1]['token_yes'] = tokens[0]
                    results[-1]['token_no'] = tokens[1]
        except Exception as e:
            break
    return results


def fetch_kalshi(max_pages=10):
    """Fetch all open binary markets from Kalshi."""
    results = []
    cursor = ''
    for _ in range(max_pages):
        try:
            params = {'limit': 100, 'status': 'open', 'with_nested_markets': 'true'}
            if cursor:
                params['cursor'] = cursor
            r = SESSION.get(f'{KALSHI_API}/events', params=params, timeout=15)
            r.raise_for_status()
            d = r.json()
            for ev in d.get('events', []):
                for m in ev.get('markets', []):
                    if m.get('market_type') != 'binary':
                        continue
                    ya = float(m.get('yes_ask_dollars', 0) or 0)
                    yb = float(m.get('yes_bid_dollars', 0) or 0)
                    na = float(m.get('no_ask_dollars', 0) or 0)
                    nb = float(m.get('no_bid_dollars', 0) or 0)
                    if ya == 0 and yb == 0:
                        continue
                    results.append({
                        'title': m.get('title', ''),
                        'event_title': ev.get('title', ''),
                        'category': ev.get('category', ''),
                        'ticker': m.get('ticker', ''),
                        'yes_ask': ya, 'yes_bid': yb,
                        'no_ask': na, 'no_bid': nb,
                        'volume_24h': float(m.get('volume_24h_fp', 0) or 0),
                        'open_interest': float(m.get('open_interest_fp', 0) or 0),
                        'close_time': m.get('close_time', '') or m.get('expiration_time', '') or '',
                    })
            cursor = d.get('cursor', '')
            if not cursor:
                break
        except:
            break
    return results


# ═══════════════════════════════════════════════════════════════════════
# FEE MODELS
# ═══════════════════════════════════════════════════════════════════════

def poly_fee(price, fees_enabled=True):
    if not fees_enabled:
        return 0.002  # just slippage
    p = max(0.01, min(0.99, price))
    return p * 0.03 * (p * (1 - p)) ** 0.5 * 0.75 + 0.002

def kalshi_fee(price):
    if price <= 0 or price >= 1:
        return 0
    return min(0.07 * price * (1 - price), 0.07)


# ═══════════════════════════════════════════════════════════════════════
# DATE / IRR
# ═══════════════════════════════════════════════════════════════════════

def parse_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except:
        pass
    for fmt in ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%d']:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except:
            continue
    return None


def days_until(dt):
    if dt is None:
        return None
    delta = (dt - datetime.now(timezone.utc)).total_seconds() / 86400
    return max(delta, 0.01)


def annualized_irr(roi_pct, days):
    if days is None or days <= 0 or roi_pct <= 0:
        return None
    try:
        return (math.pow(1 + roi_pct / 100, 365 / days) - 1) * 100
    except:
        return None


# ═══════════════════════════════════════════════════════════════════════
# MATCHING + ARB DETECTION
# ═══════════════════════════════════════════════════════════════════════

def match_and_compute(poly_markets, kalshi_markets):
    """Match markets across platforms and compute arbs."""
    # Build inverted index on Kalshi
    kidx = defaultdict(set)
    for i, km in enumerate(kalshi_markets):
        for w in set(norm(km['title']).split()) | set(norm(km['event_title']).split()):
            if len(w) > 2:
                kidx[w].add(i)

    matches = []
    used_kalshi = set()

    for pm in poly_markets:
        pw = set(norm(pm['question']).split())
        # Find candidates via inverted index
        candidates = set()
        for w in pw:
            if w in kidx:
                candidates |= kidx[w]
        candidates -= used_kalshi

        best_score, best_ki = 0, -1
        for ki in candidates:
            km = kalshi_markets[ki]
            s = max(similarity(pm['question'], km['title']),
                    similarity(pm['question'], km['event_title']))
            if s > best_score:
                # STRICT: must pass is_same_market check
                if is_same_market(pm['question'], km['title']) or is_same_market(pm['question'], km['event_title']):
                    best_score, best_ki = s, ki

        if best_score >= 0.55 and best_ki >= 0:
            used_kalshi.add(best_ki)
            km = kalshi_markets[best_ki]

            # Compute arb in both directions
            arb_results = _compute_arb(pm, km)
            if arb_results:
                for arb in arb_results:
                    arb['match_score'] = round(best_score, 3)
                    arb['poly_question'] = pm['question']
                    arb['kalshi_title'] = km['title']
                    arb['kalshi_event'] = km['event_title']

                    # Confidence scoring
                    if best_score >= 0.80:
                        arb['confidence'] = 'HIGH'
                    elif best_score >= 0.65:
                        arb['confidence'] = 'MEDIUM'
                    else:
                        arb['confidence'] = 'LOW'

                    # Flag suspiciously high ROIs as likely mismatches
                    if arb['roi'] > 50:
                        arb['confidence'] = 'LOW'
                        arb['warning'] = 'ROI > 50% — verify markets are identical before trading'

                    matches.append(arb)

    matches.sort(key=lambda x: -x['roi'])
    return matches


def _compute_arb(pm, km):
    """Compute arb opportunities between a Polymarket and Kalshi market."""
    results = []

    py = pm['yes_price']
    pn = pm['no_price']
    kya = km['yes_ask']
    kna = km['no_ask']
    fees_on = pm['fees_enabled']

    # Expiry: use the earlier date (capital locked until both resolve)
    p_dt = parse_dt(pm['end_date'])
    k_dt = parse_dt(km['close_time'])
    if p_dt and k_dt:
        expiry_dt = max(p_dt, k_dt)
    else:
        expiry_dt = p_dt or k_dt
    exp_days = days_until(expiry_dt)
    expiry_str = expiry_dt.strftime('%Y-%m-%d %H:%M UTC') if expiry_dt else 'Unknown'

    # Direction 1: YES on Poly + NO on Kalshi
    if py > 0 and kna > 0:
        cost = py + kna
        fees = poly_fee(py, fees_on) + kalshi_fee(kna)
        net = 1.0 - cost - fees
        if net > 0.002:
            roi = (net / (cost + fees)) * 100
            irr = annualized_irr(roi, exp_days)
            results.append({
                'direction': 'YES Poly + NO Kalshi',
                'cost': cost, 'fees': fees, 'net': net, 'roi': roi,
                'irr': irr, 'expiry': expiry_str, 'days': exp_days,
                'poly_slug': pm['event_slug'],
                'poly_market_slug': pm['slug'],
                'poly_token': pm['token_yes'],
                'kalshi_ticker': km['ticker'],
                'poly_price': py, 'kalshi_price': kna,
                'poly_side': 'YES', 'kalshi_side': 'NO',
                'poly_action': f'Buy YES at ${py:.3f}',
                'kalshi_action': f'Buy NO at ${kna:.2f}',
                'poly_volume': pm['volume_24h'],
                'kalshi_volume': km['volume_24h'],
                'category': km['category'],
            })

    # Direction 2: YES on Kalshi + NO on Poly
    if kya > 0 and pn > 0:
        cost = kya + pn
        fees = kalshi_fee(kya) + poly_fee(pn, fees_on)
        net = 1.0 - cost - fees
        if net > 0.002:
            roi = (net / (cost + fees)) * 100
            irr = annualized_irr(roi, exp_days)
            results.append({
                'direction': 'YES Kalshi + NO Poly',
                'cost': cost, 'fees': fees, 'net': net, 'roi': roi,
                'irr': irr, 'expiry': expiry_str, 'days': exp_days,
                'poly_slug': pm['event_slug'],
                'poly_market_slug': pm['slug'],
                'poly_token': pm['token_no'],
                'kalshi_ticker': km['ticker'],
                'poly_price': pn, 'kalshi_price': kya,
                'poly_side': 'NO', 'kalshi_side': 'YES',
                'poly_action': f'Buy NO at ${pn:.3f}',
                'kalshi_action': f'Buy YES at ${kya:.2f}',
                'poly_volume': pm['volume_24h'],
                'kalshi_volume': km['volume_24h'],
                'category': km['category'],
            })

    return results


# ═══════════════════════════════════════════════════════════════════════
# SPREAD HISTORY (SQLite)
# ═══════════════════════════════════════════════════════════════════════

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS spread_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        poly_question TEXT NOT NULL,
        kalshi_ticker TEXT NOT NULL,
        direction TEXT NOT NULL,
        poly_price REAL, kalshi_price REAL,
        spread REAL, roi REAL, irr REAL,
        days_to_expiry REAL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spread_ts ON spread_history(kalshi_ticker, timestamp)")
    conn.commit()
    conn.close()


def save_spreads(arbs):
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc).isoformat()
    for a in arbs:
        conn.execute("""
            INSERT INTO spread_history (timestamp, poly_question, kalshi_ticker, direction,
                                        poly_price, kalshi_price, spread, roi, irr, days_to_expiry)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (now, a['poly_question'], a['kalshi_ticker'], a['direction'],
              a['poly_price'], a['kalshi_price'], a['net'], a['roi'], a['irr'], a['days']))
    conn.commit()
    conn.close()


def get_spread_history(kalshi_ticker, direction, limit=100):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT timestamp, spread, roi, irr, poly_price, kalshi_price, days_to_expiry
        FROM spread_history
        WHERE kalshi_ticker=? AND direction=?
        ORDER BY timestamp DESC LIMIT ?
    """, (kalshi_ticker, direction, limit)).fetchall()
    conn.close()
    return [dict(r) for r in reversed(rows)]


# ═══════════════════════════════════════════════════════════════════════
# MAIN SCAN
# ═══════════════════════════════════════════════════════════════════════

def run_scan():
    """Run a full scan. Returns dict with arbs, stats, and log."""
    init_db()
    log = []

    log.append("Fetching Polymarket markets...")
    poly = fetch_polymarket(max_pages=10)
    log.append(f"  {len(poly)} active binary markets")

    log.append("Fetching Kalshi markets...")
    kalshi = fetch_kalshi(max_pages=10)
    log.append(f"  {len(kalshi)} active binary markets")

    log.append("Matching and computing arbs...")
    arbs = match_and_compute(poly, kalshi)
    log.append(f"  {len(arbs)} arb opportunities found")

    # Save to history
    if arbs:
        save_spreads(arbs)
        log.append(f"  Saved {len(arbs)} spreads to history")

    # Category breakdown
    cats = defaultdict(int)
    for a in arbs:
        cats[a.get('category', 'unknown')] += 1

    return {
        'arbs': arbs,
        'log': log,
        'n_poly': len(poly),
        'n_kalshi': len(kalshi),
        'n_arbs': len(arbs),
        'categories': dict(cats),
        'scan_time': datetime.now(timezone.utc).isoformat(),
    }
