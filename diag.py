#!/usr/bin/env python3
import requests

CHECKS = {
    "Binance": "https://api.binance.com/api/v3/exchangeInfo",
    "Bybit": "https://api.bybit.com/v5/market/instruments-info?category=spot&limit=5",
    "OKX": "https://www.okx.com/api/v5/public/instruments?instType=SPOT",
    "KuCoin": "https://api.kucoin.com/api/v1/symbols",
    "MEXC": "https://api.mexc.com/api/v3/exchangeInfo",
    "Gate.io": "https://api.gateio.ws/api/v4/spot/currency_pairs",
    "HTX(Huobi)": "https://api.huobi.pro/v1/common/symbols",
    "Coinbase": "https://api.exchange.coinbase.com/products",
    "Kraken": "https://api.kraken.com/0/public/AssetPairs",
}

print(f"{'Borsa':<14} {'Durum':<10} Detay")
print("-" * 70)
for name, url in CHECKS.items():
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        size = len(r.content)
        ok = "OK" if r.status_code == 200 else "ENGELLI/HATA"
        print(f"{name:<14} {r.status_code:<10} {ok} (yanit boyutu: {size} byte)")
    except requests.RequestException as e:
        print(f"{name:<14} {'ERR':<10} {type(e).__name__}: {e}")
