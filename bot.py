#!/usr/bin/env python3
"""
Midas'ta TL ile islem goren coinleri, Binance'in ucretsiz genel API'sinden
gelen 30 dakikalik mum verisiyle izler.

Kural (kullanicinin tarifine gore):
  - Son 2 KAPANMIS 30 dakikalik mum yesil (ustuste 2 yesil mum)
  - Su an olusmakta olan (henuz kapanmamis) 3. mum da su ana kadar yesil
  - Bu 3. mumun kapanmasina 5 dakika veya daha az kaldiginda
  -> Telegram'a bildirim gonderir (boylece mum kapanmadan ~5 dk once haber verir)

Midas'in herkese acik bir API'si olmadigi icin fiyat verisi Binance'in
USDT paritelerinden cekiliyor. Buyuk/likit coinlerde 30 dakikalik mumun
yon (yesil/kirmizi) bilgisi borsalar arasinda pratikte hemen hemen hep
ayni cikar; cok dusuk hacimli/yeni coinlerde arada sirada farklilik
olabilecegini goz onunde bulundurun.
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BINANCE_BASE = "https://api.binance.com"
INTERVAL = "30m"
ALERT_WINDOW_MS = 5 * 60 * 1000  # kapanisa 5 dakika kala
STATE_FILE = "state.json"
MAX_WORKERS = 10

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Midas'ta TL ile islem goren coinlerin sembolleri (USDT/TRY eki olmadan).
# Kaynak: midaskripto.com/midastaki-coinler/ (Eylul 2026 itibariyle).
# Midas zaman zaman yeni coin ekleyip cikarabilir; bu listeyi guncel
# tutmak icin sadece asagidaki listeye ekleme/cikarma yapmaniz yeterli.
MIDAS_COINS = [
    "WCT", "0G", "1INCH", "AEVO", "AAVE", "ACM", "ACX", "ACT", "AERO", "AI16Z",
    "AIXBT", "ACH", "ALGO", "ALLO", "AMP", "ANIME", "ANKR", "APE", "API3", "APT",
    "ARB", "ARIA", "ARK", "ARKM", "ARPA", "FET", "AR", "ASR", "ASTR", "ASTER",
    "ATM", "BEAT", "AUDIO", "AVAX", "AXL", "AXS", "BASED", "BEAM", "BEL", "BERA",
    "BNB", "BNX", "BIO", "BTC", "BCH", "TAO", "BTT", "BLUR", "BONK", "BOME",
    "AUCTION", "BB", "ZKC", "BRETT", "BREV", "BMT", "GALA", "ADA", "CTSI", "CSPR",
    "MEW", "CATI", "TIA", "CFG", "CETUS", "LINK", "COAI", "CHZ", "COMP", "CFX",
    "CORE", "ATOM", "COTI", "COW", "CRO", "CRV", "CYBER", "MANA", "HOME", "DEGEN",
    "DEXE", "DOGE", "DOGS", "WIF", "2Z", "DYDX", "DYM", "EIGEN", "ENJ", "ENSO",
    "EOS", "ENA", "ETHFI", "ETH", "ENS", "ROBO", "FF", "FARTCOIN", "BAR", "PORTO",
    "FIL", "FLR", "FLOKI", "FOGO", "GMT", "GNO", "GOAT", "GPS", "GRASS", "G",
    "HMSTR", "HBAR", "HOT", "ZEN", "HUMA", "H", "WET", "HYPE", "IMX", "INIT",
    "INJ", "ICP", "IO", "IOTA", "JASMY", "JTO", "JUP", "JST", "JUV", "KAIA",
    "KAITO", "KAS", "KAT", "KERNEL", "KITE", "KSM", "LAUNCHCOIN", "ZRO", "LDO", "LIT",
    "LINEA", "LTC", "LPT", "BARD", "LUMIA", "ME", "CITY", "MNT", "MANTRA", "MANTA",
    "SYRUP", "MASK", "MEME", "MERL", "MET", "METIS", "MINA", "MOG", "MOODENG", "MORPHO",
    "MOVE", "EGLD", "SHELL", "NAORIS", "NEAR", "NEIRO", "NEO", "CKB", "NEWT", "NEXO",
    "NOT", "NMR", "ROSE", "TRUMP", "OKB", "OMNI", "ONDO", "XCN", "EDU", "EDEN",
    "OPN", "OP", "ORCA", "ORDER", "ORDI", "OGN", "CAKE", "PRCL", "PSG", "PAXG",
    "PYUSD", "PNUT", "PENDLE", "PEPE", "PRL", "PHA", "PI", "PIXEL", "XPL", "PLUME",
    "DOT", "POL", "PENGU", "PUMP", "PYTH", "QANX", "QTUM", "QNT", "QUBIC", "RAD",
    "RAVE", "RAY", "RED", "RENDER", "REZ", "XRP", "RONIN", "LAZIO", "SAGA", "SAHARA",
    "SAPIEN", "SCR", "SKR", "SEI", "SENT", "SHIB", "CAT", "SIREN", "SKL", "SKY",
    "SOL", "LAYER", "SOLV", "SOMI", "S", "SXT", "SPELL", "SPX", "STX", "STRK",
    "XLM", "STEEM", "STORJ", "IP", "SUI", "SUN", "RARE", "SUPER", "SUSHI", "SNX",
    "TRB", "TNSR", "USDT", "EURT", "XAUT", "GRT", "SAND", "THETA", "TON", "TOSHI",
    "MAGIC", "TREE", "TRX", "TWT", "TURBO", "UMA", "UNI", "CHIP", "USDC", "USUAL",
    "VANA", "VANRY", "VET", "VVV", "VINE", "VIRTUAL", "VSN", "WAL", "WLFI", "WLD",
    "W", "XAI", "XDC", "ZETA", "ZEUS", "ZK", "ZORA",
]


def get_binance_usdt_symbols():
    """Binance'de aktif islem goren <TICKER>USDT paritelerinin taban varlik setini dondurur."""
    r = requests.get(f"{BINANCE_BASE}/api/v3/exchangeInfo", timeout=20)
    r.raise_for_status()
    data = r.json()
    symbols = set()
    for s in data["symbols"]:
        if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING":
            symbols.add(s["baseAsset"])
    return symbols


def fetch_klines(symbol):
    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {"symbol": f"{symbol}USDT", "interval": INTERVAL, "limit": 3}
    r = requests.get(url, params=params, timeout=10)
    if r.status_code != 200:
        return None
    return r.json()


def is_green(kline):
    open_price = float(kline[1])
    close_price = float(kline[4])
    return close_price > open_price


def check_symbol(symbol):
    try:
        kl = fetch_klines(symbol)
    except requests.RequestException:
        return None
    if not kl or len(kl) < 3:
        return None

    closed_2_ago, closed_1_ago, forming = kl[-3], kl[-2], kl[-1]

    if not (is_green(closed_2_ago) and is_green(closed_1_ago)):
        return None
    if not is_green(forming):
        return None

    close_time_ms = forming[6]
    now_ms = int(time.time() * 1000)
    remaining_ms = close_time_ms - now_ms

    if 0 < remaining_ms <= ALERT_WINDOW_MS:
        return {
            "symbol": symbol,
            "open_time": forming[0],
            "remaining_min": round(remaining_ms / 60000, 1),
        }
    return None


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID tanimli degil, bildirim gonderilemedi.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        data={"chat_id": TELEGRAM_CHAT_ID, "text": message},
        timeout=15,
    )
    if resp.status_code != 200:
        print("Telegram gonderim hatasi:", resp.status_code, resp.text)


def main():
    try:
        usdt_symbols = get_binance_usdt_symbols()
    except requests.RequestException as e:
        print("Binance exchangeInfo alinamadi:", e)
        return

    candidates = [c for c in MIDAS_COINS if c in usdt_symbols]
    print(f"{len(MIDAS_COINS)} coin tanimli, {len(candidates)} tanesi Binance USDT paritesinde bulundu.")

    state = load_state()
    hits = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(check_symbol, s): s for s in candidates}
        for fut in as_completed(futures):
            res = fut.result()
            if not res:
                continue
            sym = res["symbol"]
            if state.get(sym) == res["open_time"]:
                continue  # bu mum icin zaten bildirim gonderildi
            hits.append(res)
            state[sym] = res["open_time"]

    if hits:
        hits.sort(key=lambda x: x["symbol"])
        lines = [
            f"\U0001F7E2 {h['symbol']}USDT - ustuste 3 yesil mum (kapanisa ~{h['remaining_min']} dk kaldi)"
            for h in hits
        ]
        message = "30 dakikalik mumlarda sinyal:\n" + "\n".join(lines)
        send_telegram(message)
        print(message)
    else:
        print("Sinyal veren coin yok.")

    save_state(state)


if __name__ == "__main__":
    main()
