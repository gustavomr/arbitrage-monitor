"""
Multi-Token Arbitrage Monitor
============================
Estratégia: COMPRAR USDT nas CEX (com BRL) → VENDER no DEX (USDT→TOKEN)

Fontes de COMPRA (CEX — BRL/USDT):
  Binance, Bybit, KuCoin, MEXC, Bitget, OKX

Fontes de VENDA (DEX Aggregators — USDT→TOKEN na Polygon):
  1inch, KyberSwap, ParaSwap, OpenOcean, LiFi, DeFiLlama

Saída:
  - Tabela de preços de compra e venda
  - Top 5 oportunidades de spread por token
  - Alerta Telegram quando spread > threshold

Uso:
  pip install requests rich python-dotenv
  python main.py

Variáveis de ambiente (.env):
  ONEINCH_API_KEY=xxx        # portal.1inch.dev (gratuito)
  POLL_INTERVAL=45           # segundos entre polls (padrão 45)
  TARGET_SPREAD=0.5          # % mínimo para alertar (padrão 0.5)
  CAPITAL_AMOUNT=6000       # capital de entrada (padrão 6000)
  CEX_SPREAD_COST=0.1        # custo de spread da CEX em % (padrão 0.1)
  TELEGRAM_BOT_TOKEN=xxx    # Telegram bot token
  TELEGRAM_CHAT_ID=xxx      # Telegram chat ID
  TELEGRAM_ENABLED=true     # Habilita notificações Telegram (padrão false)
  TELEGRAM_PARSE_MODE=HTML  # Formato das mensagens (HTML/Markdown)
"""

import os
import time
import asyncio
import logging
import threading
import sys
import select
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, List, Any
import requests
from dotenv import load_dotenv
from dataclasses import dataclass


# ─── Rich (opcional) ─────────────────────────────────────────────────────────
try:
    from rich.console import Console
    from rich.table import Table
    RICH = True
    console = Console()
except ImportError:
    RICH = False
    console = None

# ─── Telegram (opcional) ───────────────────────────────────────────────────────
try:
    from telegram_notifier import TelegramNotifier, TelegramConfigManager
    _tg_config = TelegramConfigManager.load_from_env()
    TELEGRAM_AVAILABLE = True
    print(f"✅ Telegram importado com sucesso - enabled: {_tg_config.enabled}")
except ImportError as e:
    print(f"❌ Erro ao importar telegram_notifier: {e}")
    TELEGRAM_AVAILABLE = False
    _tg_config = None
except Exception as e:
    print(f"❌ Erro geral no Telegram: {e}")
    TELEGRAM_AVAILABLE = False
    _tg_config = None

load_dotenv()

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
ONEINCH_API_KEY    = os.getenv("ONEINCH_API_KEY")
ZEROX_API_KEY      = os.getenv("ZEROX_API_KEY")  # Get from https://dashboard.0x.org/apps
POLL_INTERVAL      = int(os.getenv("POLL_INTERVAL"))
TARGET_SPREAD      = float(os.getenv("TARGET_SPREAD"))
CAPITAL_AMOUNT     = float(os.getenv("CAPITAL_AMOUNT"))   # capital genérico
CEX_SPREAD_COST    = float(os.getenv("CEX_SPREAD_COST"))   # custo de spread da CEX em %
TELEGRAM_MAX_OPPORTUNITIES = int(os.getenv("TELEGRAM_MAX_OPPORTUNITIES", "5"))  # máximo de oportunidades no Telegram
TIMEOUT            = 10

# Polygon network config
POLYGON_CHAIN = 137
USDT_ADDR     = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"  # USDT (PoS)
USDT_DECIMALS = 6

# ═══════════════════════════════════════════════════════════════════════════════
# TOKEN CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
@dataclass
class TokenConfig:
    symbol: str
    address: str
    decimals: int
    name: str

# Configuração dos tokens - fácil adicionar novos
TOKENS = {
    "BRLA": TokenConfig(
        symbol="BRLA",
        address="0xe6a537a407488807f0bbeb0038b79004f19dddfb",
        decimals=18,
        name="Brazilian Real Token"
    ),
    "BRZ": TokenConfig(
        symbol="BRZ", 
        address="0x4ed141110f6eeeaba9a1df36d8c26f684d2475dc",
        decimals=18,
        name="Brazilian Real Z"
    )
}

# ═══════════════════════════════════════════════════════════════════════════════
# LINKS DIRETOS
# ═══════════════════════════════════════════════════════════════════════════════
def generate_dex_links(token_symbol: str, token_address: str) -> Dict[str, str]:
    """Gera links DEX para um token específico"""
    return {
        "1inch":     f"https://app.1inch.io/#/137/simple/swap/{USDT_ADDR}/{token_address}",
        "KyberSwap": f"https://kyberswap.com/swap/polygon/{USDT_ADDR}-to-{token_address}",
        "ParaSwap":  f"https://app.paraswap.xyz/#/{USDT_ADDR}-{token_address}/1/SELL?network=polygon",
        "OpenOcean": f"https://app.openocean.finance/CLASSIC#/POLYGON/{USDT_ADDR}/{token_address}",
        "Jumper":     f"https://transferto.xyz/swap?fromChain=POL&toChain=POL&fromToken={USDT_ADDR}&toToken={token_address}",
        "DeFiLlama": f"https://swap.defillama.com/?chain=polygon&from={USDT_ADDR}&to={token_address}",
        "Matcha":    f"https://matcha.xyz/tokens/polygon/{token_address}?buyChain=137&buyAddress={token_address}&sellAmount=1000",
    }

# Links CEX (mesmos para todos os tokens)
CEX_LINKS: Dict[str, str] = {
    "Binance":  "https://www.binance.com/pt-BR/trade/USDT_BRL",
    "Bybit":    "https://www.bybit.com/pt-BR/trade/spot/USDT/BRL",
    "KuCoin":   "https://www.kucoin.com/pt_BR/trade/USDT-BRL",
    "MEXC":     "https://www.mexc.com/pt-BR/exchange/BRL_USDT",
    "Bitget":   "https://www.bitget.com/pt-BR/spot/USDTBRL",
    "OKX":      "https://www.okx.com/pt-br/trade-spot/usdt-brl",
}

# Gerar links DEX para cada token
DEX_LINKS = {symbol: generate_dex_links(symbol, token.address) for symbol, token in TOKENS.items()}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

TZ_BR = ZoneInfo("America/Sao_Paulo")

# ═══════════════════════════════════════════════════════════════════════════════
# CEX — PREÇOS DE COMPRA
# ═══════════════════════════════════════════════════════════════════════════════
def _safe(fn, name):
    try:
        return fn()
    except Exception as e:
        log.warning(f"{name} error: {e}")
        return None, None

def get_binance() -> tuple:
    r = requests.get(
        "https://api.binance.com/api/v3/ticker/bookTicker?symbol=USDTBRL",
        timeout=TIMEOUT
    )
    d = r.json()
    return float(d["bidPrice"]), float(d["askPrice"])

def get_bybit() -> tuple:
    r = requests.get(
        "https://api.bybit.com/v5/market/tickers?category=spot&symbol=USDTBRL",
        timeout=TIMEOUT
    )
    d = r.json()
    if d.get("retCode") == 0 and d["result"]["list"]:
        item = d["result"]["list"][0]
        return float(item["bid1Price"]), float(item["ask1Price"])
    return None, None

def get_kucoin() -> tuple:
    r = requests.get(
        "https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=USDT-BRL",
        timeout=TIMEOUT
    )
    d = r.json()
    if d.get("code") == "200000":
        return float(d["data"]["bestBid"]), float(d["data"]["bestAsk"])
    return None, None

def get_mexc() -> tuple:
    r = requests.get(
        "https://api.mexc.com/api/v3/ticker/bookTicker?symbol=BRLUSDT",
        timeout=TIMEOUT
    )
    d = r.json()
    bid_brlusdt = float(d["bidPrice"])
    ask_brlusdt = float(d["askPrice"])
    return 1 / ask_brlusdt, 1 / bid_brlusdt

def get_bitget() -> tuple:
    r = requests.get(
        "https://api.bitget.com/api/v2/spot/market/tickers?symbol=USDTBRL",
        timeout=TIMEOUT
    )
    d = r.json()
    if d.get("code") == "00000" and d.get("data"):
        item = d["data"][0]
        return float(item["bidPr"]), float(item["askPr"])
    return None, None

def get_okx() -> tuple:
    r = requests.get(
        "https://www.okx.com/api/v5/market/ticker?instId=USDT-BRL",
        timeout=TIMEOUT
    )
    d = r.json()
    if d.get("code") == "0" and d.get("data"):
        item = d["data"][0]
        return float(item["bidPx"]), float(item["askPx"])
    return None, None

def fetch_all_cex() -> Dict[str, Dict[str, float]]:
    """Retorna dict com preços ask de cada CEX (BRL por 1 USDT)"""
    results = {}
    fetchers = {
        "Binance": get_binance,
        "Bybit":   get_bybit,
        "KuCoin":  get_kucoin,
        "MEXC":    get_mexc,
        "Bitget":  get_bitget,
        "OKX":     get_okx,
    }
    for name, fn in fetchers.items():
        bid, ask = _safe(fn, name)
        results[name] = {"bid": bid, "ask": ask}
    return results

# ═══════════════════════════════════════════════════════════════════════════════
# DEX AGGREGATORS — FUNÇÕES GENÉRICAS
# ═══════════════════════════════════════════════════════════════════════════════

def query_1inch(usdt_amount: float, usdt_raw: int, token: TokenConfig) -> Optional[Dict[str, Any]]:
    if not ONEINCH_API_KEY:
        return {"price": None, "route": "sem API key", "error": True}
    url = f"https://api.1inch.dev/swap/v6.1/{POLYGON_CHAIN}/quote"
    headers = {"Authorization": f"Bearer {ONEINCH_API_KEY}"}
    params = {"src": USDT_ADDR, "dst": token.address, "amount": str(usdt_raw)}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        d = r.json()
        token_out = int(d["dstAmount"]) / 10 ** token.decimals
        price = token_out / usdt_amount
        protocols = d.get("protocols", [])
        names = set()
        for leg in protocols:
            for hop in leg:
                for part in hop:
                    names.add(part.get("name", "?"))
        return {"price": price, "route": " → ".join(names) or "unknown"}
    except Exception as e:
        log.warning(f"1inch {token.symbol} error: {e}")
    return None

def query_kyberswap(usdt_amount: float, usdt_raw: int, token: TokenConfig) -> Optional[Dict[str, Any]]:
    url = "https://aggregator-api.kyberswap.com/polygon/api/v1/routes"
    params = {"tokenIn": USDT_ADDR, "tokenOut": token.address, "amountIn": str(usdt_raw)}
    headers = {"X-Client-Id": f"{token.symbol.lower()}-arb-monitor", "Accept": "application/json"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        summary = r.json()["data"]["routeSummary"]
        token_out = int(summary["amountOut"]) / 10 ** token.decimals
        price = token_out / usdt_amount
        dexes = set()
        for hop_list in summary.get("route", []):
            for hop in hop_list:
                dexes.add(hop.get("exchange", "?"))
        return {"price": price, "route": " → ".join(dexes) or "unknown"}
    except Exception as e:
        log.warning(f"KyberSwap {token.symbol} error: {e}")
    return None

def query_paraswap(usdt_amount: float, usdt_raw: int, token: TokenConfig) -> Optional[Dict[str, Any]]:
    url = "https://apiv5.paraswap.io/prices"
    params = {
        "srcToken": USDT_ADDR, "destToken": token.address,
        "amount": str(usdt_raw), "srcDecimals": USDT_DECIMALS,
        "destDecimals": token.decimals, "side": "SELL",
        "network": POLYGON_CHAIN, "otherExchangePrices": "true",
    }
    try:
        r = requests.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        pr = r.json().get("priceRoute", {})
        token_out = int(pr["destAmount"]) / 10 ** token.decimals
        price = token_out / usdt_amount
        exs = set()
        for leg in pr.get("bestRoute", []):
            for swap in leg.get("swaps", []):
                for ex in swap.get("swapExchanges", []):
                    exs.add(ex.get("exchange", "?"))
        return {"price": price, "route": " → ".join(exs) or "unknown"}
    except Exception as e:
        log.warning(f"ParaSwap {token.symbol} error: {e}")
    return None

def query_openocean(usdt_amount: float, usdt_raw: int, token: TokenConfig) -> Optional[Dict[str, Any]]:
    url = "https://open-api.openocean.finance/v3/polygon/quote"
    params = {
        "inTokenAddress": USDT_ADDR, "outTokenAddress": token.address,
        "amount": str(usdt_amount), "gasPrice": "30",
    }
    try:
        r = requests.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if data.get("code") not in (200, "200", None):
            log.warning(f"OpenOcean {token.symbol} API error: {data.get('error') or data.get('message')}")
            return None
        out_data = data.get("data")
        if not isinstance(out_data, dict):
            log.warning(f"OpenOcean {token.symbol} unexpected data type ({type(out_data).__name__}): {out_data}")
            return None
        raw_out = out_data.get("outAmount") or out_data.get("toAmount") or 0
        token_out = float(raw_out) / 10 ** token.decimals
        if token_out == 0:
            return None
        price = token_out / usdt_amount
        path = out_data.get("path", [])
        dexes = set()
        for step in path:
            if isinstance(step, dict):
                for part in step.get("parts", []):
                    if isinstance(part, dict):
                        dex = part.get("dex")
                        dexes.add(dex.get("dexCode", "?") if isinstance(dex, dict) else str(dex))
            elif isinstance(step, str):
                dexes.add(step)
        return {"price": price, "route": " → ".join(sorted(dexes)) or "unknown"}
    except Exception as e:
        log.warning(f"OpenOcean {token.symbol} error: {e}")
    return None

def query_lifi(usdt_amount: float, usdt_raw: int, token: TokenConfig) -> Optional[Dict[str, Any]]:
    url = "https://li.quest/v1/quote"
    wallet = "0x0000000000000000000000000000000000000001"
    params = {
        "fromChain": POLYGON_CHAIN, "toChain": POLYGON_CHAIN,
        "fromToken": USDT_ADDR, "toToken": token.address,
        "fromAmount": str(usdt_raw),
        "fromAddress": wallet,
        "slippage": "0.005",
        "order": "CHEAPEST",
    }
    try:
        r = requests.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        d = r.json()
        token_out = int(d["estimate"]["toAmount"]) / 10 ** token.decimals
        price = token_out / usdt_amount
        tool = d.get("toolDetails", {}).get("name", "unknown")
        return {"price": price, "route": tool}
    except Exception as e:
        log.warning(f"LiFi {token.symbol} error: {e}")
    return None

def query_uniswap(usdt_amount: float, usdt_raw: int, token: TokenConfig) -> Optional[Dict[str, Any]]:
    url = "https://api.uniswap.org/v1/quote"
    params = {
        "tokenInAddress": USDT_ADDR,
        "tokenOutAddress": token.address,
        "tokenInChainId": POLYGON_CHAIN,
        "tokenOutChainId": POLYGON_CHAIN,
        "amount": str(usdt_raw),
        "type": "exactIn"
    }
    headers = {"Origin": "https://app.uniswap.org"}
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        d = r.json()
        
        token_out = int(d["quote"]) / 10 ** token.decimals
        price = token_out / usdt_amount
        
        route_info = []
        for route in d.get("route", []):
            for pool in route:
                route_info.append(f"Pool {pool.get('fee', 'unknown')}")
        
        return {"price": price, "route": " → ".join(route_info) or "Uniswap V3"}
    except Exception as e:
        log.warning(f"Uniswap {token.symbol} error: {e}")
    return None

def query_matcha(usdt_amount: float, usdt_raw: int, token: TokenConfig) -> Optional[Dict[str, Any]]:
    """
    Query Matcha (0x API) for USDT -> Token swap price on Polygon
    Uses the 0x Swap API v2 allowance holder endpoint
    """
    if not ZEROX_API_KEY:
        log.warning(f"Matcha {token.symbol} skipped: no ZEROX_API_KEY configured")
        return None
    
    url = "https://api.0x.org/swap/permit2/price"
    params = {
        "chainId": str(POLYGON_CHAIN),  # Polygon chain ID
        "sellToken": USDT_ADDR,         # USDT address on Polygon
        "buyToken": token.address,      # Target token address
        "sellAmount": str(usdt_raw),    # Amount in USDT base units (wei)
        "taker": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",  # Dummy taker address for quotes
    }
    headers = {
        "0x-api-key": ZEROX_API_KEY,
        "0x-version": "v2",
        "accept": "application/json"
    }
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        d = r.json()
        
        # Extract buy amount and calculate price
        buy_amount_raw = int(d["buyAmount"])
        token_out = buy_amount_raw / 10 ** token.decimals
        price = token_out / usdt_amount
        
        # Extract route information for better transparency
        route_sources = []
        if "route" in d and "fills" in d["route"]:
            sources_seen = set()
            for fill in d["route"]["fills"]:
                source = fill.get("source", "Unknown")
                # Avoid duplicate sources in the route display
                if source not in sources_seen:
                    route_sources.append(source)
                    sources_seen.add(source)
        
        route_description = " → ".join(route_sources) if route_sources else "0x Aggregated"
        
        return {
            "price": price, 
            "route": route_description,
            "buy_amount": buy_amount_raw,
            "gas_estimate": d.get("gas"),
            "fees": d.get("fees", {})
        }
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            log.warning(f"Matcha {token.symbol} error: Invalid API key")
        elif e.response.status_code == 429:
            log.warning(f"Matcha {token.symbol} error: Rate limited")
        else:
            log.warning(f"Matcha {token.symbol} HTTP error {e.response.status_code}: {e}")
    except Exception as e:
        log.warning(f"Matcha {token.symbol} error: {e}")
    
    return None

def query_defillama(token: TokenConfig) -> Optional[Dict[str, Any]]:
    try:
        u = requests.get(
            f"https://coins.llama.fi/prices/current/polygon:{USDT_ADDR},polygon:{token.address}",
            timeout=TIMEOUT
        )
        coins = u.json().get("coins", {})
        usdt_usd = coins.get(f"polygon:{USDT_ADDR}", {}).get("price")
        token_usd = coins.get(f"polygon:{token.address}", {}).get("price")
        if not usdt_usd or not token_usd:
            return None
        price = 1 / token_usd
        return {"price": price, "route": "spot (DeFiLlama)"}
    except Exception as e:
        log.warning(f"DeFiLlama {token.symbol} error: {e}")
    return None

def fetch_all_dex(usdt_amount: float, usdt_raw: int, token: TokenConfig) -> Dict[str, Optional[Dict[str, Any]]]:
    """Busca preços DEX para um token específico"""
    return {
        "1inch":      query_1inch(usdt_amount, usdt_raw, token),
        "KyberSwap":  query_kyberswap(usdt_amount, usdt_raw, token),
        "ParaSwap":   query_paraswap(usdt_amount, usdt_raw, token),
        "Matcha":     query_matcha(usdt_amount, usdt_raw, token),
    #    "OpenOcean":  query_openocean(usdt_amount, usdt_raw, token),
    #    "LiFi":       query_lifi(usdt_amount, usdt_raw, token),
      #  "DeFiLlama":  query_defillama(token),
    }

# ═══════════════════════════════════════════════════════════════════════════════
# CÁLCULO DE SPREAD GENÉRICO
# ═══════════════════════════════════════════════════════════════════════════════

def calcular_spreads(cex_prices: Dict[str, Dict[str, float]], dex_prices: Dict[str, Optional[Dict[str, Any]]], token: TokenConfig) -> List[Dict[str, Any]]:
    """
    Calcula spreads para um token específico.
    
    Fluxo real do capital:
      1. Você tem CAPITAL_AMOUNT em BRL
      2. Compra USDT na CEX ao preço ask (BRL por USDT)
      3. A CEX desconta CEX_SPREAD_COST% da quantidade de USDT recebida
      4. Vende USDT no DEX recebendo token_rate {token.symbol} por USDT
      5. spread = (token_recebido / CAPITAL_AMOUNT - 1) * 100
    """
    spreads = []
    usdt_qty_factor = 1 - CEX_SPREAD_COST / 100

    for cex_name, cex_data in cex_prices.items():
        ask = cex_data.get("ask")
        if not ask:
            continue

        usdt_bruto = CAPITAL_AMOUNT / ask
        usdt_comprado = usdt_bruto * usdt_qty_factor

        for dex_name, dex_data in dex_prices.items():
            if not dex_data or not dex_data.get("price"):
                continue
            
            token_rate = dex_data["price"]
            token_recebido = usdt_comprado * token_rate
            spread = (token_recebido / CAPITAL_AMOUNT - 1) * 100
            lucro_token = token_recebido - CAPITAL_AMOUNT

            spreads.append({
                "token": token.symbol,
                "compra": cex_name,
                "venda": dex_name,
                "preco_compra": ask,
                "usdt_bruto": usdt_bruto,
                "usdt_comprado": usdt_comprado,
                "preco_venda": token_rate,
                f"{token.symbol.lower()}_recebido": token_recebido,
                f"lucro_{token.symbol.lower()}": lucro_token,
                "spread": spread,
                "route": dex_data.get("route", ""),
            })
    return sorted(spreads, key=lambda x: x["spread"], reverse=True)

# ═══════════════════════════════════════════════════════════════════════════════
# DISPLAY FUNCTIONS GENÉRICAS
# ═══════════════════════════════════════════════════════════════════════════════

def print_plain(ts: str, cex: Dict[str, Dict[str, float]], dex_prices: Dict[str, Dict[str, Optional[Dict[str, Any]]]], spreads_by_token: Dict[str, List[Dict[str, Any]]], usdt_amount: float):
    """Exibe resultados em formato plain text para todos os tokens"""
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  {ts}  |  capital R$ {CAPITAL_AMOUNT:,.2f}  |  cotando {usdt_amount:.4f} USDT")
    
    # CEX prices (mesmos para todos os tokens)
    print(f"\n  COMPRA — CEX (BRL/USDT)")
    for name, d in cex.items():
        ask = d.get("ask")
        bid = d.get("bid")
        if ask:
            usdt_bruto = CAPITAL_AMOUNT / ask
            usdt_liq = usdt_bruto * (1 - CEX_SPREAD_COST / 100)
            print(f"    {name:<12}  ask R$ {ask:.4f}  |  bid R$ {bid:.4f}  "
                  f"|  USDT bruto {usdt_bruto:.4f}  USDT líquido (-{CEX_SPREAD_COST}%) {usdt_liq:.4f}")
        else:
            print(f"    {name:<12}  ❌ indisponível")
    
    # DEX table unificada para todos os tokens
    print(f"\n  VENDA — DEX (TODOS TOKENS/USDT) — cotando {usdt_amount:.4f} USDT")
    
    # Encontrar os melhores DEX para cada token
    best_dex_by_token = {}
    for token_symbol, spreads in spreads_by_token.items():
        if spreads:
            best_dex_by_token[token_symbol] = spreads[0]["venda"]
    
    # Listar todos os aggregators únicos
    all_aggregators = set()
    for dex_data in dex_prices.values():
        all_aggregators.update(dex_data.keys())
    
    # Cabeçalho da tabela unificada
    header_tokens = "  ".join(f"{token_symbol:>12}" for token_symbol in TOKENS.keys())
    print(f"    {'Aggregator':<12} {header_tokens}  {'Melhor Rota':<30}")
    print(f"    {'-'*12} {'-'* (len(TOKENS) * 13)} {'-'*30}")
    
    # Preencher tabela unificada
    for agg in sorted(all_aggregators):
        row_values = [agg]
        routes = []
        
        for token_symbol, token in TOKENS.items():
            dex = dex_prices.get(token_symbol, {})
            d = dex.get(agg)
            
            if d and d.get("price"):
                price = f"{d['price']:.6f}"
                # Adicionar badge para melhor DEX
                if best_dex_by_token.get(token_symbol) == agg:
                    price = f"⭐{price}"
                    routes.append(f"{token_symbol}: {d['route']}")
            else:
                price = "❌"
            
            row_values.append(price)
        
        # Combinar todas as rotas
        combined_route = " | ".join(routes) if routes else "-"
        row_values.append(combined_route[:30])  # Limitar tamanho
        
        # Format linha
        token_prices = "  ".join(f"{val:>12}" for val in row_values[1:-1])
        print(f"    {row_values[0]:<12} {token_prices}  {row_values[-1]:<30}")
    
    # Spreads tables para cada token
    for token_symbol, token in TOKENS.items():
        spreads = spreads_by_token.get(token_symbol, [])
        
        print(f"\n  TOP 3 OPORTUNIDADES {token_symbol}  (capital R$ {CAPITAL_AMOUNT:,.2f})")
        for i, op in enumerate(spreads[:3], 1):
            flag = "🚨 " if op["spread"] >= TARGET_SPREAD else "   "
            cex_link = CEX_LINKS.get(op["compra"], "")
            dex_link = DEX_LINKS[token_symbol].get(op["venda"], "")
            token_recebido = op[f"{token_symbol.lower()}_recebido"]
            lucro_token = op[f"lucro_{token_symbol.lower()}"]
            
            print(f"  {flag}{i}. {op['compra']:<10} → {op['venda']:<12}  spread {op['spread']:+.2f}%  lucro R$ {lucro_token:+.2f}")
            print(f"       ask R${op['preco_compra']:.4f}  USDT bruto {op['usdt_bruto']:.4f}  "
                  f"USDT líquido {op['usdt_comprado']:.4f}  {token_symbol} recebido {token_recebido:.4f}")
            if cex_link:
                print(f"       📌 Comprar USDT : {cex_link}")
            if dex_link:
                print(f"       🔄 Vender→{token_symbol}  : {dex_link}")
    
    print(f"{sep}")

def print_rich(ts: str, cex: Dict[str, Dict[str, float]], dex_prices: Dict[str, Dict[str, Optional[Dict[str, Any]]]], spreads_by_token: Dict[str, List[Dict[str, Any]]], usdt_amount: float):
    """Exibe resultados em formato rich para todos os tokens"""
    # CEX table
    t1 = Table(title=f"COMPRA — CEX (BRL/USDT)  |  {ts}  |  capital R$ {CAPITAL_AMOUNT:,.0f}", show_lines=True)
    t1.add_column("Exchange", style="cyan")
    t1.add_column("Ask", style="red", justify="right")
    t1.add_column("Bid", style="green", justify="right")
    t1.add_column("USDT bruto", style="yellow", justify="right")
    t1.add_column(f"USDT líquido (-{CEX_SPREAD_COST}%)", style="magenta", justify="right")
    
    for name, d in cex.items():
        ask, bid = d.get("ask"), d.get("bid")
        if ask:
            usdt_bruto = CAPITAL_AMOUNT / ask
            usdt_liq = usdt_bruto * (1 - CEX_SPREAD_COST / 100)
            t1.add_row(
                name,
                f"R$ {ask:.4f}",
                f"R$ {bid:.4f}" if bid else "N/A",
                f"{usdt_bruto:.4f}",
                f"{usdt_liq:.4f}",
            )
        else:
            t1.add_row(name, "[red]N/A[/red]", "[red]N/A[/red]", "", "")
    console.print(t1)
    
    # DEX table unificada para todos os tokens
    t2 = Table(title=f"VENDA — DEX (TODOS TOKENS/USDT)  |  cotando {usdt_amount:.4f} USDT", show_lines=True)
    t2.add_column("Aggregator", style="cyan")
    
    # Adicionar colunas para cada token
    for token_symbol in TOKENS.keys():
        t2.add_column(f"{token_symbol}/USDT", style="green", justify="right")
    
    t2.add_column("Melhor Rota", style="dim")
    
    # Encontrar os melhores DEX para cada token
    best_dex_by_token = {}
    for token_symbol, spreads in spreads_by_token.items():
        if spreads:
            best_dex_by_token[token_symbol] = spreads[0]["venda"]
    
    # Listar todos os aggregators únicos
    all_aggregators = set()
    for dex_data in dex_prices.values():
        all_aggregators.update(dex_data.keys())
    
    # Preencher tabela unificada
    for agg in sorted(all_aggregators):
        row_values = [agg]
        routes = []
        
        for token_symbol, token in TOKENS.items():
            dex = dex_prices.get(token_symbol, {})
            d = dex.get(agg)
            
            if d and d.get("price"):
                price = f"{d['price']:.6f}"
                # Adicionar rota apenas se for o melhor DEX para este token
                if best_dex_by_token.get(token_symbol) == agg:
                    routes.append(f"{token_symbol}: {d['route']}")
                # Adicionar badge para melhor DEX
                if best_dex_by_token.get(token_symbol) == agg:
                    price = f"⭐ {price}"
            else:
                price = "[red]N/A[/red]"
            
            row_values.append(price)
        
        # Combinar todas as rotas
        combined_route = " | ".join(routes) if routes else "-"
        row_values.append(combined_route)
        
        t2.add_row(*row_values)
    
    console.print(t2)
    
    # Spreads tables para cada token
    for token_symbol, token in TOKENS.items():
        spreads = spreads_by_token.get(token_symbol, [])
        
        # Spreads table
        t3 = Table(title=f"TOP 3 OPORTUNIDADES {token_symbol}  |  capital R$ {CAPITAL_AMOUNT:,.0f}", show_lines=True)
        t3.add_column("#", style="dim", width=3)
        t3.add_column("Compra (CEX)", style="yellow")
        t3.add_column("Venda (DEX)", style="cyan")
        t3.add_column("Ask (BRL/USDT)", justify="right")
        t3.add_column("USDT bruto", justify="right")
        t3.add_column(f"USDT −{CEX_SPREAD_COST}%", justify="right", style="magenta")
        t3.add_column(f"{token_symbol} recebido", justify="right")
        t3.add_column(f"Lucro ({token_symbol})", justify="right")
        t3.add_column("Spread", justify="right")
        t3.add_column("🔗 Comprar", style="blue")
        t3.add_column("🔗 Vender", style="blue")
        
        for i, op in enumerate(spreads[:3], 1):
            color = "bold green" if op["spread"] >= TARGET_SPREAD else "white"
            flag = "🚨 " if op["spread"] >= TARGET_SPREAD else ""
            token_recebido = op[f"{token_symbol.lower()}_recebido"]
            lucro_token = op[f"lucro_{token_symbol.lower()}"]
            lucro_color = "green" if lucro_token >= 0 else "red"
            cex_link = CEX_LINKS.get(op["compra"], "")
            dex_link = DEX_LINKS[token_symbol].get(op["venda"], "")
            
            t3.add_row(
                str(i),
                op["compra"],
                op["venda"],
                f"R$ {op['preco_compra']:.4f}",
                f"{op['usdt_bruto']:.4f}",
                f"{op['usdt_comprado']:.4f}",
                f"{token_recebido:.4f}",
                f"[{lucro_color}]{lucro_token:+.2f}[/{lucro_color}]",
                f"[{color}]{flag}{op['spread']:+.2f}%[/{color}]",
                f"[link={cex_link}]{cex_link}[/link]" if cex_link else "",
                f"[link={dex_link}]{dex_link}[/link]" if dex_link else "",
            )
        console.print(t3)

# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═════════════════════════════════════════════════════════════════════════════

async def _send_telegram(spreads_by_token: Dict[str, List[Dict[str, Any]]]):
    if not TELEGRAM_AVAILABLE or not _tg_config:
        log.warning("Telegram not available or not configured")
        return
    
    # Obter configuração de max_opportunities da variável de ambiente
    max_opportunities = TELEGRAM_MAX_OPPORTUNITIES
    log.info(f"Enviando até {max_opportunities} oportunidades por alerta Telegram")
    
    # Enviar alertas para todos os tokens que tiverem spreads acima do threshold
    alerts_sent = 0
    for token_symbol, spreads in spreads_by_token.items():
        if spreads:
            best_spread = spreads[0]["spread"]
            log.info(f"{token_symbol} best spread: {best_spread:.2f}% (threshold: {TARGET_SPREAD:.2f}%)")
            
            if best_spread >= TARGET_SPREAD:
                log.info(f"Sending Telegram alert for {token_symbol}: {best_spread:.2f}% >= {TARGET_SPREAD:.2f}%")
                try:
                    async with TelegramNotifier(_tg_config) as notifier:
                        result = await notifier.send_arbitrage_alert(spreads, min_profit_threshold=TARGET_SPREAD, max_opportunities=max_opportunities)
                        if result:
                            alerts_sent += 1
                            log.info(f"Telegram alert sent successfully for {token_symbol}")
                        else:
                            log.warning(f"Failed to send Telegram alert for {token_symbol}")
                except Exception as e:
                    log.error(f"Error sending Telegram alert for {token_symbol}: {e}")
            else:
                log.info(f"No alert for {token_symbol}: {best_spread:.2f}% < {TARGET_SPREAD:.2f}%")
        else:
            log.info(f"No spreads available for {token_symbol}")
    
    if alerts_sent > 0:
        log.info(f"Total Telegram alerts sent: {alerts_sent}")
    else:
        log.info("No Telegram alerts sent (no spreads above threshold)")

def notify_telegram(spreads_by_token: Dict[str, List[Dict[str, Any]]]):
    try:
        asyncio.run(_send_telegram(spreads_by_token))
    except Exception as e:
        log.warning(f"Telegram error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# COUNTDOWN TIMER
# ═══════════════════════════════════════════════════════════════════════════════

def countdown_timer(seconds: int, stop_event: threading.Event, manual_trigger: threading.Event):
    """Exibe countdown regressivo na mesma linha com opção de busca manual"""
    while seconds > 0 and not stop_event.is_set():
        mins, secs = divmod(seconds, 60)
        time_str = f"{mins:02d}:{secs:02d}"
        
        # Verificar se há entrada do usuário (Enter para busca manual)
        if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
            sys.stdin.readline()
            manual_trigger.set()
            print("\r" + " " * 50 + "\r", end="", flush=True)
            print("\n🔍 Busca manual disparada!")
            return
        
        # Sempre usar texto plano para evitar problemas com Rich em threads
        print(f"\rPróxima busca em: {time_str} (Pressione Enter para buscar agora)", end="", flush=True)
        
        time.sleep(1)
        seconds -= 1
    
    # Limpar a linha do countdown quando terminar
    print("\r" + " " * 60 + "\r", end="", flush=True)

# ═══════════════════════════════════════════════════════════════════════════════
# POLL FUNCTION GENÉRICA
# ═══════════════════════════════════════════════════════════════════════════════

def poll():
    now = datetime.now(TZ_BR)
    ts = now.strftime("%d/%m/%Y %H:%M:%S")

    # Mensagem de busca
    if RICH:
        print("🔍 Buscando dados...", flush=True)
    else:
        print("🔍 Buscando dados...", flush=True)

    cex = fetch_all_cex()

    # Calcula USDT_AMOUNT a partir do melhor ask disponível nas CEX
    asks_validos = [d["ask"] for d in cex.values() if d.get("ask")]
    if asks_validos:
        melhor_ask = min(asks_validos)
        usdt_amount = CAPITAL_AMOUNT / (melhor_ask * (1 + CEX_SPREAD_COST / 100))
    else:
        usdt_amount = CAPITAL_AMOUNT / 5.0  # fallback
    usdt_raw = int(usdt_amount * 10 ** USDT_DECIMALS)

    # Buscar dados para todos os tokens
    dex_prices = {}
    spreads_by_token = {}
    
    for token_symbol, token in TOKENS.items():
        # Buscar preços DEX para este token
        dex = fetch_all_dex(usdt_amount, usdt_raw, token)
        dex_prices[token_symbol] = dex
        
        # Calcular spreads para este token
        spreads = calcular_spreads(cex, dex, token)
        spreads_by_token[token_symbol] = spreads

    # Exibir resultados
    if RICH:
        print_rich(ts, cex, dex_prices, spreads_by_token, usdt_amount)
    else:
        print_plain(ts, cex, dex_prices, spreads_by_token, usdt_amount)

    # Alertas Telegram para tokens com spreads acima do threshold
    notify_telegram(spreads_by_token)

def main():
    token_names = ", ".join([token.symbol for token in TOKENS.values()])
    header = (f"{token_names} Arbitrage Monitor  |  capital R$ {CAPITAL_AMOUNT:,.0f}  |  "
              f"custo CEX {CEX_SPREAD_COST}%  |  alerta >{TARGET_SPREAD}%  |  intervalo {POLL_INTERVAL}s")
    if RICH:
        console.rule(f"[bold cyan]{header}[/bold cyan]")
    else:
        print(header)
        print("-" * len(header))

    try:
        while True:
            # Executar busca
            poll()
            
            # Criar eventos para controlar o countdown e busca manual
            stop_event = threading.Event()
            manual_trigger = threading.Event()
            
            # Iniciar countdown em thread separada
            countdown_thread = threading.Thread(
                target=countdown_timer, 
                args=(POLL_INTERVAL, stop_event, manual_trigger),
                daemon=True
            )
            countdown_thread.start()
            
            # Esperar o tempo ou interrupção ou busca manual
            try:
                for i in range(POLL_INTERVAL):
                    if stop_event.is_set():
                        break
                    if manual_trigger.is_set():
                        break
                    time.sleep(1)
            except KeyboardInterrupt:
                stop_event.set()
                manual_trigger.set()
                countdown_thread.join(timeout=1)
                raise
                
    except KeyboardInterrupt:
        msg = "Monitor encerrado."
        print(f"\n{msg}") if not RICH else console.print(f"\n[bold red]{msg}[/bold red]")

if __name__ == "__main__":
    main()
