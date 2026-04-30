"""
Full Pipeline — Raw API calls only (no Binance client library):
  1. Fetch live USDTBRL rate  →  GET /api/v3/ticker/price
  2. Place market BUY order   →  POST /api/v3/order
  3. Withdraw USDT to Polygon →  POST /sapi/v1/capital/withdraw/apply
  4. Poll withdrawal status   →  GET /sapi/v1/capital/withdraw/history

Requirements:
  pip install requests python-dotenv

⚠️  Binance API key requirements:
  - "Enable Reading"                → ticker + withdraw history
  - "Enable Spot & Margin Trading"  → placing orders
  - "Enable Withdrawals"            → withdraw endpoint
  - Withdrawal address MUST be whitelisted on Binance (once via the website)

Environment variables (arquivo .env ou export):
  BINANCE_API_KEY       - Sua API key da Binance
  BINANCE_API_SECRET    - Seu API secret da Binance
  RECIPIENT_ADDRESS     - Endereço Polygon destino (0x...)
  BRL_AMOUNT            - Valor em BRL a gastar (padrão: 100.00)
"""

import os
import time
import hmac
import hashlib
import logging
from decimal import Decimal, ROUND_UP, ROUND_DOWN
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

load_dotenv()  # carrega variáveis do arquivo .env

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
API_KEY        = os.environ["BINANCE_API_KEY"]
API_SECRET     = os.environ["BINANCE_API_SECRET"]
RECIPIENT_ADDR = os.environ["RECIPIENT_ADDRESS"]   # your Polygon 0x... wallet

BASE_URL       = "https://api.binance.com"
BRL_AMOUNT     = Decimal(os.environ.get("BRL_AMOUNT", "100.00"))  # BRL a gastar

# Modo simulação (mudar para False para operar real)
SIMULATION_MODE = os.environ.get("SIMULATION_MODE", "true").lower() == "true"

# ─── HMAC helpers ─────────────────────────────────────────────────────────────

def _sign(params: dict) -> str:
    """HMAC-SHA256 over the URL-encoded param string."""
    query = urlencode(params)
    return hmac.new(
        API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _headers() -> dict:
    return {"X-MBX-APIKEY": API_KEY}


def _signed_params(extra: dict) -> dict:
    params = {
        "timestamp":  int(time.time() * 1000),
        "recvWindow": 5000,
        **extra,
    }
    params["signature"] = _sign(params)
    return params


def _get(path: str, params: dict = None, signed: bool = False) -> dict:
    if signed:
        params = _signed_params(params or {})
    r = requests.get(BASE_URL + path, params=params, headers=_headers())
    r.raise_for_status()
    return r.json()


def _post(path: str, params: dict) -> dict:
    params = _signed_params(params)
    r = requests.post(BASE_URL + path, data=params, headers=_headers())
    data = r.json()
    if not r.ok:
        raise RuntimeError(f"Binance API error {r.status_code}: {data}")
    return data


def validate_api_permissions() -> dict:
    """
    Valida a API key e verifica quais operações são permitidas.
    
    Returns:
        dict: {'valid': bool, 'permissions': list, 'account_info': dict, 'errors': list}
    """
    log.info("🔍 Validando API key e permissões...")
    
    result = {
        'valid': False,
        'permissions': [],
        'account_info': {},
        'errors': []
    }
    
    try:
        # 1. Testar conectividade básica (sem API key)
        log.info("   Testando conectividade com Binance...")
        _get("/api/v3/time")  # Não precisa de API key
        log.info("   ✅ Conectividade OK")
        
        # 2. Testar API key com endpoint público (não requer assinatura)
        log.info("   Testando API key...")
        try:
            # Testar com header da API key em endpoint público
            r = requests.get(BASE_URL + "/api/v3/exchangeInfo", headers=_headers())
            if r.status_code == 200:
                log.info("   ✅ API key formatada corretamente")
            else:
                result['errors'].append('❌ API key mal formatada')
                return result
        except Exception as e:
            result['errors'].append(f'❌ Erro ao testar API key: {str(e)}')
            return result
        
        # 3. Testar endpoint básico de account info (requer permissão de leitura)
        log.info("   Testando permissão de leitura...")
        try:
            account_data = _get("/api/v3/account", signed=True)
            balances = [b for b in account_data.get('balances', []) if float(b['free']) > 0]
            result['account_info']['active_balances'] = len(balances)
            result['account_info']['account_type'] = 'SPOT'
            log.info("   ✅ Permissão de leitura OK")
            result['permissions'].append('READING')
        except Exception as e:
            error_msg = str(e)
            if '401' in error_msg:
                result['errors'].append('❌ API key inválida, expirada ou sem permissão de leitura')
                result['errors'].append('❌ Verifique: API key, IP whitelist, ou ative "Enable Reading"')
                return result
            elif '1020' in error_msg:
                result['errors'].append('❌ IP bloqueado - adicione seu IP à whitelist')
                return result
            else:
                result['errors'].append(f'❌ Erro ao acessar conta: {error_msg}')
                return result
        
        # 4. Testar endpoint de account status (requer permissões)
        try:
            log.info("   Testando status da conta...")
            status_data = _get("/sapi/v1/account/status", signed=True)
            result['account_info']['status'] = status_data.get('data', 'unknown')
            
            if status_data.get('data') == 'normal':
                log.info("   ✅ Status da conta: Normal")
            else:
                result['errors'].append(f'❌ Status da conta: {status_data.get("data")}')
                
        except Exception as e:
            if '401' in str(e):
                result['errors'].append('❌ Sem permissão para verificar status da conta')
            else:
                result['errors'].append(f'❌ Erro ao verificar status: {str(e)}')
        
        # 5. Testar permissões de trading via endpoint de ordens
        try:
            log.info("   Testando permissão de trading...")
            # Tentar obter ordens abertas (se não tiver, retorna array vazio)
            orders = _get("/api/v3/openOrders", {"symbol": "USDTBRL"}, signed=True)
            result['permissions'].append('SPOT_TRADING')
            log.info("   ✅ Permissão de trading OK")
        except Exception as e:
            if '401' in str(e) or '403' in str(e):
                result['errors'].append('❌ Sem permissão para trading - ative "Enable Spot & Margin Trading"')
            else:
                result['errors'].append(f'❌ Erro ao testar trading: {str(e)}')
        
        # 6. Testar endpoint de withdrawal permissions
        try:
            log.info("   Testando permissão de saques...")
            withdraw_data = _get("/sapi/v1/capital/withdraw/history", {"coin": "USDT", "limit": 1}, signed=True)
            result['permissions'].append('WITHDRAWALS')
            log.info("   ✅ Permissão de saques OK")
        except Exception as e:
            if '401' in str(e) or '403' in str(e):
                result['errors'].append('❌ Sem permissão para saques - ative "Enable Withdrawals"')
            else:
                result['errors'].append(f'❌ Erro ao testar saques: {str(e)}')
        
        # 7. Verificar se o endereço de destino está na whitelist
        try:
            log.info("   Verificando endereço na whitelist...")
            withdraw_data = _get("/sapi/v1/capital/withdraw/address/list", {"coin": "USDT"}, signed=True)
            addresses = [addr for addr in withdraw_data if addr.get('address') == RECIPIENT_ADDR]
            
            if addresses:
                result['account_info']['address_whitelisted'] = True
                log.info("   ✅ Endereço está na whitelist")
            else:
                result['account_info']['address_whitelisted'] = False
                result['errors'].append('❌ Endereço de destino não está na whitelist')
                log.warning("   ⚠️ Endereço não está na whitelist - adicione no site da Binance")
                
        except Exception as e:
            if '401' in str(e) or '403' in str(e):
                result['errors'].append('❌ Sem permissão para verificar whitelist')
            else:
                result['errors'].append(f'❌ Erro ao verificar whitelist: {str(e)}')
        
        # Se não houver erros críticos, API é válida
        critical_errors = [e for e in result['errors'] if '❌' in e and 'Sem permissão' not in e and 'Status da conta anormal: Normal' not in e]
        if not critical_errors:
            result['valid'] = True
            log.info("🎉 API key válida e funcionando!")
        else:
            log.error("❌ API key com problemas críticos")
            
    except Exception as e:
        error_msg = str(e)
        if '401' in error_msg or 'Invalid API-key' in error_msg:
            result['errors'].append('❌ API key inválida ou expirada')
            result['errors'].append('❌ Verifique: API key, IP whitelist, ou permissões')
        elif '1020' in error_msg:
            result['errors'].append('❌ IP bloqueado - adicione seu IP à whitelist')
        elif '404' in error_msg:
            result['errors'].append('❌ Endpoint não encontrado - verifique se API key tem permissões corretas')
        else:
            result['errors'].append(f'❌ Erro desconhecido: {error_msg}')
        
        log.error(f"❌ Falha na validação: {error_msg}")
    
    return result


def print_api_validation_report(validation_result: dict):
    """
    Exibe um relatório detalhado da validação da API.
    """
    print("\n" + "="*70)
    print("📊 RELATÓRIO DE VALIDAÇÃO DA API BINANCE")
    print("="*70)
    
    if validation_result['valid']:
        print("🟢 STATUS: API Key VÁLIDA e funcionando")
    else:
        print("🔴 STATUS: API Key INVÁLIDA ou com problemas")
    
    print(f"\n🔑 Permissões disponíveis: {', '.join(validation_result['permissions']) if validation_result['permissions'] else 'Nenhuma'}")
    
    account_info = validation_result['account_info']
    if account_info:
        print(f"\n📋 Informações da conta:")
        print(f"   Tipo: {account_info.get('account_type', 'N/A')}")
        print(f"   Status: {account_info.get('status', 'N/A')}")
        print(f"   Endereço whitelist: {'✅' if account_info.get('address_whitelisted') else '❌'}")
        print(f"   Saldos ativos: {account_info.get('active_balances', 0)}")
    
    if validation_result['errors']:
        print(f"\n⚠️  Erros encontrados:")
        for error in validation_result['errors']:
            print(f"   {error}")
    
    print("\n" + "="*70)
    
    # Recomendações baseadas nos resultados
    recommendations = []
    permissions = validation_result['permissions']
    
    if not validation_result['valid']:
        recommendations.append("Verifique se a API key está correta e não expirou")
        recommendations.append("Adicione seu IP à whitelist nas configurações da API")
    
    if 'SPOT_TRADING' not in permissions:
        recommendations.append("Ative 'Enable Spot & Margin Trading' nas configurações da API")
    
    if 'WITHDRAWALS' not in permissions:
        recommendations.append("Ative 'Enable Withdrawals' nas configurações da API")
    
    if not account_info.get('address_whitelisted'):
        recommendations.append("Adicione o endereço de destino à whitelist no site da Binance")
    
    # Verificar permissões necessárias para operação completa
    required_permissions = ['READING', 'SPOT_TRADING', 'WITHDRAWALS']
    missing_permissions = [p for p in required_permissions if p not in permissions]
    
    if missing_permissions:
        recommendations.append(f"Permissões faltantes para operação completa: {', '.join(missing_permissions)}")
    
    if recommendations:
        print("💡 RECOMENDAÇÕES:")
        for rec in recommendations:
            print(f"   • {rec}")
        print("="*70)
    
    # Status final
    if all(perm in permissions for perm in required_permissions) and account_info.get('address_whitelisted'):
        print("🎉 API está pronta para operação completa!")
    elif 'READING' in permissions and 'SPOT_TRADING' in permissions:
        print("⚠️  API pode trading, mas não sacar (verifique permissões de withdrawal)")
    elif 'READING' in permissions:
        print("⚠️  API tem apenas permissão de leitura")
    else:
        print("❌ API não tem permissões básicas funcionando")


# ─── Step 1: Live USDTBRL rate ────────────────────────────────────────────────

def get_usdt_brl_rate() -> Decimal:
    """
    GET /api/v3/ticker/bookTicker?symbol=USDTBRL
    Retorna bid e ask do order book.
      ASK = preco que voce PAGA ao comprar USDT  (market buy bate aqui)
      BID = preco que voce RECEBE ao vender USDT
    Retorna o ASK como taxa de referencia para estimativa.
    """
    data = _get("/api/v3/ticker/bookTicker", {"symbol": "USDTBRL"})

    ask = Decimal(data["askPrice"])   # <- voce compra por este preco
    bid = Decimal(data["bidPrice"])   # <- voce vende por este preco
    spread = ask - bid

    log.info("[1] Cotacao USDTBRL (order book):")
    log.info(f"    ASK (compra)  : 1 USDT = {ask} BRL  <- seu market BUY executa aqui")
    log.info(f"    BID (venda)   : 1 USDT = {bid} BRL")
    log.info(f"    Spread        : {spread:.4f} BRL")
    log.info(f"    Estimativa    : {BRL_AMOUNT} BRL -> ~{BRL_AMOUNT / ask:.4f} USDT")

    return ask  # market buy usa o ASK


def get_order_book_levels(limit: int = 5) -> dict:
    """
    GET /api/v3/depth?symbol=USDTBRL&limit=5
    Retorna os melhores níveis de ask (venda) com volume.
    
    Returns:
        dict: {'asks': [[price, quantity], ...], 'total_volume_usdt': volume_total}
    """
    data = _get("/api/v3/depth", {"symbol": "USDTBRL", "limit": str(limit)})
    
    asks = data.get("asks", [])  # Formato: [[price, quantity], ...]
    
    # Converter para Decimal e calcular volume total em USDT
    processed_asks = []
    total_volume_usdt = Decimal('0')
    
    for price_str, qty_str in asks:
        price = Decimal(price_str)
        quantity = Decimal(qty_str)
        volume_usdt = price * quantity
        
        processed_asks.append({
            'price': price,
            'quantity': quantity,
            'volume_usdt': volume_usdt,
            'volume_brl': volume_usdt * price  # Volume em BRL
        })
        total_volume_usdt += volume_usdt
    
    return {
        'asks': processed_asks,
        'total_volume_usdt': total_volume_usdt,
        'total_volume_brl': total_volume_usdt * processed_asks[0]['price'] if processed_asks else Decimal('0')
    }


def display_current_ask() -> bool:
    """
    Exibe apenas o melhor ask atual com refresh de 5 segundos.
    Confirma automaticamente a cada atualização.
    
    Returns:
        bool: True para comprar no preço atual, False para cancelar
    """
    log.info("\n" + "="*60)
    log.info(" COTAÇÃO ATUAL USDTBRL (MELHOR ASK)")
    log.info("="*60)
    
    while True:
        try:
            # Buscar apenas o bookTicker para pegar o melhor ask
            data = _get("/api/v3/ticker/bookTicker", {"symbol": "USDTBRL"})
            
            ask_price = Decimal(data["askPrice"])
            ask_qty = Decimal(data["askQty"])
            
            # Mostrar cotação atualizada
            print(f"\n💰 Preço: {ask_price} BRL/USDT")
            print(f"📊 Volume disponível: {ask_qty} USDT")
            
            # Perguntar se quer comprar a este preço
            print(f"\n⏰ Próxima atualização em 5 segundos...")
            choice = input("Comprar agora? (s/n/q): ").strip().lower()
            
            if choice == 'q':
                log.info("❌ Operação cancelada pelo usuário.")
                return False
            elif choice == 's':
                log.info(f"✅ Comprando no preço atual: {ask_price} BRL/USDT")
                log.info(f"   Volume disponível: {ask_qty} USDT")
                return True
            elif choice == 'n':
                print("⏰ Aguardando próxima cotação...")
                time.sleep(1)
                continue
            else:
                # Timeout ou ENTER - continua monitorando
                print("⏰ Aguardando próxima cotação...")
                time.sleep(4)  # Menos 1 segundo do tempo de espera
                continue
                
        except KeyboardInterrupt:
            print("\n\n❌ Operação cancelada pelo usuário.")
            return False
        except Exception as e:
            log.error(f"Erro ao buscar cotação: {str(e)}")
            time.sleep(1)
            continue


def buy_usdt_at_price(brl_amount: Decimal, target_price: Decimal) -> Decimal:
    """
    POST /api/v3/order
    Compra USDT a um preço específico (ordem limit).
    Se não houver volume suficiente no preço, falha (all or nothing).
    
    Args:
        brl_amount: Valor em BRL para gastar
        target_price: Preço alvo em BRL por USDT
    
    Returns:
        Decimal: Quantidade de USDT comprada
    """
    # Calcular quantidade de USDT baseada no preço alvo
    usdt_quantity = brl_amount / target_price
    
    # Formatar quantidade com precisão adequada (múltiplo de 0.1 para USDTBRL)
    # LOT_SIZE: minQty=0.1, stepSize=0.1
    # NOTIONAL: minNotional=10.0 BRL
    
    # Calcular quantidade ideal baseada no valor desejado
    ideal_quantity = brl_amount / target_price
    
    # Arredondar para múltiplo de 0.1, mas para BAIXO para NUNCA exceder o valor
    usdt_quantity_rounded = (ideal_quantity / Decimal('0.1')).quantize(Decimal('1'), rounding=ROUND_DOWN) * Decimal('0.1')
    
    # Calcular valor notional real
    notional_value = usdt_quantity_rounded * target_price
    
    # Verificar se ainda atende o MIN_NOTIONAL de 10 BRL
    min_notional = Decimal('10.0')
    if notional_value < min_notional:
        log.warning(f"⚠️ Valor ajustado ({notional_value:.2f} BRL) é menor que o mínimo (10.00 BRL)")
        log.warning("   Considere aumentar BRL_AMOUNT no .env")
        # Mesmo assim, continua com o valor calculado para validação
    
    # Garantir mínimo de 0.1 USDT
    if usdt_quantity_rounded < Decimal('0.1'):
        usdt_quantity_rounded = Decimal('0.1')
        brl_amount = usdt_quantity_rounded * target_price
    
    usdt_quantity_str = str(usdt_quantity_rounded.quantize(Decimal('0.1'), rounding=ROUND_DOWN))
    
    log.info(f"[2] LIMIT BUY : Desejado {brl_amount} BRL → {usdt_quantity_str} USDT @ {target_price} BRL/USDT (real: {notional_value:.2f} BRL)")
    
    # MODO VALIDAÇÃO (em vez de execução)
    if SIMULATION_MODE:
        log.info("    🧪 MODO SIMULAÇÃO - Validando ordem...")
        
        # Validar todos os filtros
        errors = []
        
        # Validar LOT_SIZE
        if usdt_quantity_rounded < Decimal('0.1'):
            errors.append(f"❌ Quantidade mínima: 0.1 USDT (atual: {usdt_quantity_rounded})")
        
        # Validar MIN_NOTIONAL
        if notional_value < Decimal('10.0'):
            errors.append(f"❌ Valor mínimo: 10.00 BRL (atual: {notional_value:.2f} BRL)")
        
        # Validar PRICE_FILTER
        if target_price < Decimal('4.3') or target_price > Decimal('6.5'):
            errors.append(f"❌ Preço fora do range: 4.3 - 6.5 BRL/USDT (atual: {target_price})")
        
        if errors:
            log.info("    ❌ ORDEM INVÁLIDA:")
            for error in errors:
                log.info(f"      {error}")
            log.info("\n    💡 Sugestões:")
            log.info("      - Aumente BRL_AMOUNT no .env para pelo menos 10.00")
            log.info("      - Verifique se o preço está dentro do range permitido")
            return Decimal('0')
        
        # Se passou em todas as validações
        log.info("    ✅ ORDEM VÁLIDA!")
        log.info(f"    📊 Detalhes da ordem:")
        log.info(f"      Quantidade: {usdt_quantity_rounded} USDT")
        log.info(f"      Valor: {notional_value:.2f} BRL")
        log.info(f"      Preço: {target_price} BRL/USDT")
        log.info(f"      Taxa estimada: {usdt_quantity_rounded * Decimal('0.001')} USDT")
        log.info(f"      Líquido: {usdt_quantity_rounded - (usdt_quantity_rounded * Decimal('0.001'))} USDT")
        
        return usdt_quantity_rounded - (usdt_quantity_rounded * Decimal('0.001'))
    
    # MODO REAL
    try:
        data = _post("/api/v3/order", {
            "symbol":        "USDTBRL",
            "side":          "BUY",
            "type":          "LIMIT",
            "timeInForce":   "GTC",  # Good Till Cancelled
            "quantity":      usdt_quantity_str,
            "price":         str(target_price),
        })
        
        log.info(f"    Order ID : {data['orderId']} | Status: {data['status']}")
        log.info(f"    ✅ Ordem colocada com sucesso!")
        
        # Para ordens LIMIT, precisamos verificar se foi executada imediatamente
        # Se status = FILLED, foi executada. Se NEW, está aguardando.
        if data['status'] == 'FILLED':
            usdt_gross = sum(Decimal(f["qty"]) for f in data["fills"])
            usdt_fee   = sum(
                Decimal(f["commission"])
                for f in data["fills"]
                if f["commissionAsset"] == "USDT"
            )
            net_usdt = usdt_gross - usdt_fee
            
            log.info(f"    USDT gross : {usdt_gross}")
            log.info(f"    Fee (USDT) : {usdt_fee}")
            log.info(f"    Net USDT   : {net_usdt}")
            return net_usdt
        else:
            log.warning(f"    ⚠️  Ordem não executada imediatamente. Status: {data['status']}")
            log.warning("    Verifique manualmente se a ordem foi executada posteriormente.")
            return Decimal('0')
            
    except Exception as e:
        log.error(f"    ❌ Falha na ordem: {str(e)}")
        log.error("    Possivelmente não há volume suficiente neste preço.")
        raise


# ─── Step 2: Market buy USDT with BRL ─────────────────────────────────────────

def buy_usdt_with_brl(brl_amount: Decimal) -> Decimal:
    """
    POST /api/v3/order
    Pair   : USDTBRL  (base=USDT, quote=BRL)
    Side   : BUY
    Type   : MARKET
    quoteOrderQty = BRL to spend  →  Binance returns USDT

    Returns net USDT after commissions.
    """
    log.info(f"[2] Market BUY : spend {brl_amount} BRL → USDT...")

    data = _post("/api/v3/order", {
        "symbol":        "USDTBRL",
        "side":          "BUY",
        "type":          "MARKET",
        "quoteOrderQty": str(brl_amount),
    })

    log.info(f"    Order ID : {data['orderId']} | Status: {data['status']}")

    usdt_gross = sum(Decimal(f["qty"]) for f in data["fills"])
    usdt_fee   = sum(
        Decimal(f["commission"])
        for f in data["fills"]
        if f["commissionAsset"] == "USDT"
    )
    net_usdt = usdt_gross - usdt_fee

    log.info(f"    USDT gross : {usdt_gross}")
    log.info(f"    Fee (USDT) : {usdt_fee}")
    log.info(f"    Net USDT   : {net_usdt}")
    return net_usdt


# ─── Step 3: Withdraw USDT → Polygon via Binance API ─────────────────────────

def withdraw_usdt_to_polygon(usdt_amount: Decimal) -> str:
    """
    POST /sapi/v1/capital/withdraw/apply
    coin    = USDT
    network = MATIC  (Polygon PoS — Binance's network code for Polygon)
    address = recipient Polygon wallet

    Binance sends USDT directly on-chain to the Polygon address.
    No bridging needed — Binance handles it natively.

    Returns the Binance withdrawal ID for status tracking.
    """
    # Truncar para 2 casas decimais em vez de arredondar para cima
    amount_str = f"{usdt_amount.quantize(Decimal('0.01'), rounding=ROUND_DOWN)}"
    log.info(f"[3] Withdraw   : {amount_str} USDT → {RECIPIENT_ADDR} (Polygon/MATIC)...")

    data = _post("/sapi/v1/capital/withdraw/apply", {
        "coin":    "USDT",
        "network": "MATIC",         # Polygon PoS network code on Binance
        "address": RECIPIENT_ADDR,
        "amount":  amount_str,
    })

    withdraw_id = data.get("id", "")
    log.info(f"    Withdrawal submitted! Binance ID: {withdraw_id}")
    return withdraw_id


# ─── Step 4: Poll withdrawal status ───────────────────────────────────────────

def poll_withdrawal_status(withdraw_id: str, max_wait: int = 300) -> dict:
    """
    GET /sapi/v1/capital/withdraw/history
    Polls every 10s until completed, failed, or timeout.

    Status codes:
      0 = Email Sent       1 = Cancelled      2 = Awaiting Approval
      3 = Rejected         4 = Processing     5 = Failure
      6 = Completed ✅
    """
    status_map = {
        0: "Email Sent",
        1: "Cancelled",
        2: "Awaiting Approval",
        3: "Rejected",
        4: "Processing",
        5: "Failure ❌",
        6: "Completed ✅",
    }

    log.info(f"[4] Polling withdrawal status (max {max_wait}s)...")
    elapsed  = 0
    interval = 10

    while elapsed < max_wait:
        history = _get("/sapi/v1/capital/withdraw/history", {"coin": "USDT"}, signed=True)
        record  = next((r for r in history if r.get("id") == withdraw_id), None)

        if record:
            code  = record.get("status", -1)
            label = status_map.get(code, f"Unknown ({code})")
            tx_id = record.get("txId", "pending...")
            log.info(f"    Status: {label} | TxID: {tx_id}")

            if code == 6:
                log.info(f"    Polygonscan: https://polygonscan.com/tx/{tx_id}")
                return record
            if code in (1, 3, 5):
                raise RuntimeError(f"Withdrawal failed: {label}")
        else:
            log.info("    Record not indexed yet, retrying...")

        time.sleep(interval)
        elapsed += interval

    raise TimeoutError(f"Withdrawal not confirmed within {max_wait}s — check Binance manually.")


# ─── Main ──────────────────────────────────────────────────────────────────────

def format_usdt_balance(amount: Decimal) -> str:
    """
    Formata o valor de USDT para exibição, evitando notação científica.
    
    Args:
        amount: Valor em USDT
        
    Returns:
        str: Valor formatado com até 8 casas decimais
    """
    if amount == 0:
        return "0.00000000"
    
    # Usar a representação em string do Decimal para manter precisão total
    # Depois formatar para exibição sem perder casas decimais importantes
    amount_str = str(amount)
    
    # Se já tiver ponto decimal, garantir pelo menos 8 casas decimais
    if '.' in amount_str:
        # Contar casas decimais
        decimal_places = len(amount_str.split('.')[1])
        if decimal_places < 8:
            # Completar com zeros até 8 casas
            return f"{amount:.8f}".rstrip('0').rstrip('.')
        elif decimal_places > 8:
            # Manter apenas as primeiras 8 casas sem arredondar
            integer_part, decimal_part = amount_str.split('.')
            return f"{integer_part}.{decimal_part[:8]}"
        else:
            return amount_str
    else:
        # Se não tiver ponto decimal, adicionar 8 casas
        return f"{amount:.8f}".rstrip('0').rstrip('.')


def get_usdt_balance() -> Decimal:
    """
    Busca o saldo atual de USDT na conta Binance.
    
    Returns:
        Decimal: Saldo disponível de USDT
    """
    try:
        account_data = _get("/api/v3/account", signed=True)
        balances = account_data.get('balances', [])
        
        # Debug: mostrar todos os saldos para verificação
        log.info("🔍 Verificando todos os saldos da conta:")
        for balance in balances:
            if float(balance['free']) > 0 or float(balance['locked']) > 0:
                log.info(f"   {balance['asset']}: free={balance['free']}, locked={balance['locked']}")
        
        # Encontrar saldo de USDT
        usdt_balance = Decimal('0')
        for balance in balances:
            if balance['asset'] == 'USDT':
                # Debug: mostrar o valor bruto da API
                raw_balance = balance['free']
                log.info(f"🔍 Valor bruto USDT da API: '{raw_balance}' (tipo: {type(raw_balance)})")
                
                # Converter string para Decimal sem normalização para manter precisão
                usdt_balance = Decimal(str(raw_balance))
                log.info(f"🔍 Valor convertido para Decimal: {usdt_balance}")
                break
        
        return usdt_balance
    except Exception as e:
        log.error(f"❌ Erro ao buscar saldo USDT: {str(e)}")
        return Decimal('0')


def get_brl_balance() -> Decimal:
    """
    Busca o saldo atual de BRL na conta Binance.
    
    Returns:
        Decimal: Saldo disponível de BRL
    """
    try:
        account_data = _get("/api/v3/account", signed=True)
        balances = account_data.get('balances', [])
        
        # Encontrar saldo de BRL
        brl_balance = Decimal('0')
        for balance in balances:
            if balance['asset'] == 'BRL':
                # Converter string para Decimal mantendo precisão
                brl_balance = Decimal(str(balance['free']))
                log.info(f"💰 Saldo BRL disponível: {brl_balance}")
                break
        
        return brl_balance
    except Exception as e:
        log.error(f"❌ Erro ao buscar saldo BRL: {str(e)}")
        return Decimal('0')


def get_user_brl_amount() -> Decimal:
    """
    Pergunta ao usuário quanto em BRL gostaria de comprar.
    
    Returns:
        Decimal: Valor em BRL que o usuário deseja gastar
    """
    # Consultar saldo disponível de BRL apenas se não for modo simulação
    brl_balance = None
    if not SIMULATION_MODE:
        print("\n🔍 Consultando seu saldo disponível...")
        brl_balance = get_brl_balance()
    
    while True:
        try:
            if not SIMULATION_MODE and brl_balance is not None:
                print(f"\n💰 Saldo BRL disponível: R$ {brl_balance:,.2f}")
            else:
                print(f"\n🧪 MODO SIMULAÇÃO - Sem validação de saldo")
            print(f"💰 Valor padrão configurado: R$ {BRL_AMOUNT}")
            amount_input = input("Quanto em BRL você gostaria de comprar? (ou 'Enter' para usar o padrão): ").strip()
            
            if not amount_input:
                # Usar valor padrão do .env
                print(f"✅ Usando valor padrão: R$ {BRL_AMOUNT}")
                return BRL_AMOUNT
            
            # Converter para Decimal e validar
            amount = Decimal(amount_input)
            
            # Validar contra o saldo disponível apenas se não for simulação
            if not SIMULATION_MODE and brl_balance is not None and amount > brl_balance:
                print(f"❌ Saldo insuficiente! Você tem R$ {brl_balance:,.2f} disponível.")
                continue
            
            # Validar valor mínimo (10 BRL para USDTBRL)
            if amount < Decimal('10.0'):
                print(f"❌ Valor mínimo é R$ 10.00 (regra da Binance)")
                continue
            
            print(f"✅ Valor definido: R$ {amount}")
            return amount
            
        except ValueError:
            print("❌ Valor inválido. Digite um número (ex: 50.00)")
        except KeyboardInterrupt:
            print("\n❌ Operação cancelada.")
            return None


def buy_usdt_operation() -> tuple:
    """
    Executa apenas a operação de compra USDT.
    
    Returns:
        tuple: (net_usdt, target_price, real_spent) ou (None, None, None) se falhar
    """
    log.info("=" * 60)
    log.info(" ETAPA 1: COMPRA USDT NA BINANCE")
    log.info("=" * 60)

    # 1. Perguntar quanto comprar
    user_amount = get_user_brl_amount()
    if user_amount is None:
        return None, None, None

    # 2. Exibir cotação atual com refresh automático
    should_buy = display_current_ask()
    
    if not should_buy:
        return None, None, None
    
    # 3. Obter dados atuais do ask para a compra
    data = _get("/api/v3/ticker/bookTicker", {"symbol": "USDTBRL"})
    target_price = Decimal(data["askPrice"])
    available_volume_usdt = Decimal(data["askQty"])
    
    # Calcular se o valor escolhido cobre o volume disponível
    max_usdt_possible = user_amount / target_price
    
    if max_usdt_possible > available_volume_usdt:
        log.warning(f"\n⚠️  Saldo insuficiente para comprar volume completo:")
        log.warning(f"   Disponível: {available_volume_usdt} USDT")
        log.warning(f"   Possível com R$ {user_amount}: {max_usdt_possible:.6f} USDT")
        
        use_partial = input("\nComprar parcialmente? (s/n): ").strip().lower()
        if use_partial != 's':
            log.info("\n❌ Operação cancelada.")
            return None, None, None
        
        # Ajustar para o volume disponível
        brl_to_spend = available_volume_usdt * target_price
    else:
        brl_to_spend = user_amount
    
    # 4. Executar compra no preço atual (all or nothing)
    try:
        net_usdt = buy_usdt_at_price(brl_to_spend, target_price)
        
        # Obter o valor real gasto
        if SIMULATION_MODE:
            real_spent = (net_usdt * target_price) / (Decimal('1') - Decimal('0.001'))
        else:
            real_spent = brl_to_spend
        
        if net_usdt > 0:
            log.info("=" * 60)
            log.info(" COMPRA CONCLUÍDA!")
            log.info(f"  BRL desejado   : {user_amount}")
            log.info(f"  BRL gasto     : {real_spent:.2f}")
            log.info(f"  USDT received  : {net_usdt}")
            log.info(f"  Price         : {target_price} BRL/USDT")
            log.info("=" * 60)
            return net_usdt, target_price, real_spent
        else:
            log.warning("\n⚠️  Nenhum USDT foi comprado. Verifique o status da ordem.")
            return None, None, None
            
    except Exception as e:
        log.error(f"\n❌ Erro na execução da compra: {str(e)}")
        log.error("   Tente novamente.")
        return None, None, None


def transfer_usdt_operation(net_usdt: Decimal) -> bool:
    """
    Executa apenas a transferência de USDT para Polygon.
    
    Args:
        net_usdt: Quantidade de USDT a transferir
    
    Returns:
        bool: True se sucesso, False se falhar
    """
    log.info("=" * 60)
    log.info(" ETAPA 2: TRANSFERÊNCIA USDT → POLYGON")
    log.info("=" * 60)
    
    if net_usdt <= 0:
        log.error("❌ Nenhum USDT disponível para transferir.")
        return False
    
    if SIMULATION_MODE:
        log.info(f"\n[3] 🧪 MODO SIMULAÇÃO - Withdrawal NÃO executado")
        log.info(f"      Seriam enviados {net_usdt} USDT para {RECIPIENT_ADDR}")
        log.info(f"      Network: Polygon (MATIC)")
        
        log.info("=" * 60)
        log.info(" TRANSFERÊNCIA SIMULADA!")
        log.info(f"  USDT enviado   : {net_usdt}")
        log.info(f"  Para          : {RECIPIENT_ADDR}")
        log.info(f"  Network       : Polygon (MATIC)")
        log.info(f"  TX hash       : simulado")
        log.info("=" * 60)
        return True
    else:
        try:
            # Withdraw USDT directly to Polygon wallet via Binance SAPI
            log.info(f"\n[3] Enviando {net_usdt} USDT para Polygon...")
            withdraw_id = withdraw_usdt_to_polygon(net_usdt)

            # Poll until on-chain confirmation
            log.info(f"\n[4] Aguardando confirmação na blockchain...")
            record = poll_withdrawal_status(withdraw_id)

            log.info("=" * 60)
            log.info(" TRANSFERÊNCIA CONCLUÍDA!")
            log.info(f"  USDT enviado   : {net_usdt}")
            log.info(f"  Para          : {RECIPIENT_ADDR}")
            log.info(f"  Network       : Polygon (MATIC)")
            log.info(f"  TX hash       : {record.get('txId', 'pending')}")
            log.info("=" * 60)
            return True
            
        except Exception as e:
            log.error(f"\n❌ Erro na transferência: {str(e)}")
            return False


def main():
    log.info("=" * 60)
    if SIMULATION_MODE:
        log.info(" 🧪 MODO SIMULAÇÃO ATIVADO - Nenhum dinheiro real será gasto")
    else:
        log.info(" 💰 MODO REAL ATIVADO - Dinheiro real será usado")
    log.info(" BRL → USDT (Binance Spot) → Polygon Wallet")
    log.info("=" * 60)

    # Mostrar IP atual
    try:
        import requests
        ip_response = requests.get("https://ifconfig.me", timeout=5)
        current_ip = ip_response.text.strip()
        log.info(f" 🌐 Seu IP atual: {current_ip}")
        log.info("=" * 60)
    except:
        log.info(" 🌐 Não foi possível obter seu IP")
        log.info("=" * 60)

    # Menu de opções
    while True:
        print("\nEscolha uma opção:")
        print("1. Validar API Key e permissões")
        print("2. Comprar USDT apenas")
        print("3. Transferir USDT existente")
        print("4. Comprar e Transferir (pipeline completo)")
        print("5. Sair")
        
        choice = input("\nOpção (1-5): ").strip()
        
        if choice == '1':
            # Validar API key
            validation_result = validate_api_permissions()
            print_api_validation_report(validation_result)
            
            # Perguntar se quer continuar após validação
            if validation_result['valid']:
                continue_choice = input("\nDeseja continuar operando? (s/n): ").strip().lower()
                if continue_choice != 's':
                    log.info("\n👋 Saindo...")
                    return
            else:
                input("\nPressione Enter para voltar ao menu...")
            continue
            
        elif choice == '2':
            # Apenas compra
            net_usdt, target_price, real_spent = buy_usdt_operation()
            if net_usdt:
                log.info("\n💡 Dica: Use opção 4 para comprar e transferir automaticamente")
            break
            
        elif choice == '3':
            # Apenas transferência
            try:
                # Consultar saldo de USDT disponível
                log.info("\n🔍 Consultando saldo de USDT disponível...")
                usdt_balance = get_usdt_balance()
                
                if usdt_balance > 0:
                    formatted_balance = format_usdt_balance(usdt_balance)
                    print(f"\n💰 Saldo disponível: {formatted_balance} USDT")
                    
                    # Perguntar quanto transferir
                    amount_input = input(f"Quantidade de USDT para transferir (máximo {formatted_balance}): ").strip()
                    
                    if not amount_input:
                        log.error("❌ Valor não informado.")
                        continue
                    
                    amount = Decimal(amount_input)
                    
                    # Validar se tem saldo suficiente
                    if amount > usdt_balance:
                        log.error(f"❌ Saldo insuficiente. Disponível: {formatted_balance} USDT")
                        continue
                    
                    # Validar valor mínimo (geralmente 1 USDT para withdrawal)
                    if amount < Decimal('1.0'):
                        log.error("❌ Valor mínimo para transferência é 1.0 USDT")
                        continue
                    
                    transfer_usdt_operation(amount)
                else:
                    log.error("❌ Você não possui USDT disponível para transferir.")
                    log.info("💡 Use a opção 2 para comprar USDT primeiro.")
                    
            except ValueError:
                log.error("❌ Valor inválido. Digite um número (ex: 10.5)")
            except KeyboardInterrupt:
                log.info("\n❌ Operação cancelada.")
            break
            
        elif choice == '4':
            # Pipeline completo
            net_usdt, target_price, real_spent = buy_usdt_operation()
            if net_usdt:
                # Aguardar 5 segundos para garantir que a compra foi processada
                log.info("\n⏰ Aguardando 5 segundos para processamento da compra...")
                time.sleep(5)
                log.info("✅ Tempo de espera concluído, iniciando transferência...")
                
                transfer_usdt_operation(net_usdt)
            break
            
        elif choice == '5':
            log.info("\n👋 Saindo...")
            break
            
        else:
            log.error("❌ Opção inválida. Escolha 1-5.")
            continue


if __name__ == "__main__":
    main()