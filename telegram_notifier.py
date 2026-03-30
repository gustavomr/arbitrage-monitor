#!/usr/bin/env python3
"""
Telegram Notifier Class for Arbitrage Monitor
Sends messages and alerts to Telegram bot
"""

import asyncio
import aiohttp
import json
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

@dataclass
class TelegramConfig:
    """Configuration for Telegram bot"""
    bot_token: str
    chat_id: str
    enabled: bool = True
    parse_mode: str = "HTML"  # HTML or Markdown

class TelegramNotifier:
    """Telegram notification class"""
    
    def __init__(self, config: TelegramConfig):
        self.config = config
        self.api_url = f"https://api.telegram.org/bot{config.bot_token}"
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def send_message(self, message: str, parse_mode: Optional[str] = None) -> bool:
        """Send a message to Telegram"""
        if not self.config.enabled:
            logger.debug("Telegram notifications disabled")
            return True
        
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        try:
            url = f"{self.api_url}/sendMessage"
            data = {
                "chat_id": self.config.chat_id,
                "text": message,
                "parse_mode": parse_mode or self.config.parse_mode,
                "disable_web_page_preview": True
            }
            
            async with self.session.post(url, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    if result.get("ok"):
                        logger.info("Telegram message sent successfully")
                        return True
                    else:
                        logger.error(f"Telegram API error: {result.get('description', 'Unknown error')}")
                        return False
                else:
                    logger.error(f"HTTP error sending Telegram message: {response.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False
    
    async def send_arbitrage_alert(self, opportunities: List[Dict[str, Any]], min_profit_threshold: float = 0.5) -> bool:
        """Send arbitrage opportunity alert"""
        if not opportunities:
            return True
        
        # Filter opportunities above threshold
        good_opportunities = [opp for opp in opportunities if opp.get('spread', 0) >= min_profit_threshold]
        
        if not good_opportunities:
            return True
        
        now = datetime.now(ZoneInfo("America/Sao_Paulo"))
        message = f"🚨 <b>ARBITRAGE OPPORTUNITY ALERT</b> 🚨\n"
        message += f"📅 {now.strftime('%d/%m/%Y %H:%M:%S')}\n\n"
        
        for i, opp in enumerate(good_opportunities[:5], 1):  # Top 5 opportunities
            spread = opp.get('spread', 0)
            buy_exchange = opp.get('compra', 'Unknown')
            sell_exchange = opp.get('venda', 'Unknown')
            buy_price = opp.get('preco_compra', 0)
            sell_price = opp.get('preco_venda', 0)
            
            emoji = "🔥" if spread >= 1.0 else "⚡" if spread >= 0.5 else "💰"
            message += f"{emoji} <b>Opportunity {i}</b>\n"
            message += f"📈 Spread: <b>+{spread:.2f}%</b>\n"
            message += f"💰 Buy: {buy_exchange} @ R$ {buy_price:.4f}\n"
            message += f"💸 Sell: {sell_exchange} @ R$ {sell_price:.4f}\n"
            message += f"━━━━━━━━━━━━━━━━━━━━\n"
        
        message += f"\n<i>Monitor your arbitrage opportunities!</i>"
        
        return await self.send_message(message)
    
    async def send_price_alert(self, exchange: str, symbol: str, price: float, threshold: float) -> bool:
        """Send price alert when threshold is reached"""
        now = datetime.now(ZoneInfo("America/Sao_Paulo"))
        message = f"📊 <b>PRICE ALERT</b>\n"
        message += f"📅 {now.strftime('%d/%m/%Y %H:%M:%S')}\n\n"
        message += f"🏢 <b>{exchange}</b>\n"
        message += f"💱 <b>{symbol}</b>\n"
        message += f"💰 Price: <b>R$ {price:.4f}</b>\n"
        message += f"🎯 Threshold: R$ {threshold:.4f}\n"
        
        return await self.send_message(message)
    
    async def send_error_alert(self, error_message: str, exchange: Optional[str] = None) -> bool:
        """Send error alert"""
        now = datetime.now(ZoneInfo("America/Sao_Paulo"))
        message = f"❌ <b>ERROR ALERT</b>\n"
        message += f"📅 {now.strftime('%d/%m/%Y %H:%M:%S')}\n\n"
        
        if exchange:
            message += f"🏢 Exchange: <b>{exchange}</b>\n"
        
        message += f"🚨 Error: <code>{error_message}</code>\n"
        
        return await self.send_message(message)
    
    async def send_summary_report(self, stats: Dict[str, Any]) -> bool:
        """Send daily/periodic summary report"""
        now = datetime.now(ZoneInfo("America/Sao_Paulo"))
        message = f"📈 <b>ARBITRAGE MONITOR SUMMARY</b>\n"
        message += f"📅 {now.strftime('%d/%m/%Y %H:%M:%S')}\n\n"
        
        message += f"🔍 Total Scans: <b>{stats.get('total_scans', 0)}</b>\n"
        message += f"💰 Opportunities Found: <b>{stats.get('opportunities_found', 0)}</b>\n"
        message += f"🚨 Alerts Sent: <b>{stats.get('alerts_sent', 0)}</b>\n"
        message += f"📊 Best Spread: <b>+{stats.get('best_spread', 0):.2f}%</b>\n"
        message += f"⏱️ Uptime: <b>{stats.get('uptime', 'Unknown')}</b>\n"
        
        if stats.get('top_opportunities'):
            message += f"\n<b>🏆 Top Opportunities Today:</b>\n"
            for i, opp in enumerate(stats['top_opportunities'][:3], 1):
                message += f"{i}. {opp.get('spread', 0):.2f}% ({opp.get('compra', '?')} → {opp.get('venda', '?')})\n"
        
        return await self.send_message(message)
    
    async def test_connection(self) -> bool:
        """Test Telegram bot connection"""
        if not self.config.enabled:
            return True
        
        test_message = f"✅ <b>Telegram Bot Test</b>\n🤖 Arbitrage Monitor is online!\n📅 {datetime.now(ZoneInfo('America/Sao_Paulo')).strftime('%d/%m/%Y %H:%M:%S')}"
        return await self.send_message(test_message)

class TelegramConfigManager:
    """Manages Telegram configuration"""
    
    @staticmethod
    def load_from_file(config_file: str = "telegram_config.json") -> TelegramConfig:
        """Load Telegram configuration from file"""
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            
            return TelegramConfig(
                bot_token=config_data['bot_token'],
                chat_id=config_data['chat_id'],
                enabled=config_data.get('enabled', True),
                parse_mode=config_data.get('parse_mode', 'HTML')
            )
        except FileNotFoundError:
            logger.warning(f"Telegram config file {config_file} not found, creating default")
            TelegramConfigManager.create_default_config(config_file)
            return TelegramConfigManager.load_from_file(config_file)
        except Exception as e:
            logger.error(f"Error loading Telegram config: {e}")
            raise
    
    @staticmethod
    def create_default_config(config_file: str = "telegram_config.json"):
        """Create default Telegram configuration file"""
        default_config = {
            "bot_token": "YOUR_BOT_TOKEN_HERE",
            "chat_id": "YOUR_CHAT_ID_HERE",
            "enabled": False,
            "parse_mode": "HTML"
        }
        
        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        logger.info(f"Created default Telegram config: {config_file}")
        logger.info("Please edit the file with your bot token and chat ID, then set enabled to true")

# Example usage
async def example_usage():
    """Example of how to use the Telegram notifier"""
    try:
        config = TelegramConfigManager.load_from_file()
        
        async with TelegramNotifier(config) as notifier:
            # Test connection
            if await notifier.test_connection():
                print("Telegram connection test successful!")
            
            # Send arbitrage alert
            opportunities = [
                {'spread': 0.75, 'compra': 'Binance', 'venda': 'Kucoin', 'preco_compra': 5.2934, 'preco_venda': 5.3333},
                {'spread': 0.45, 'compra': 'Bybit', 'venda': 'Mexc', 'preco_compra': 5.2910, 'preco_venda': 5.3149}
            ]
            await notifier.send_arbitrage_alert(opportunities, min_profit_threshold=0.3)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(example_usage())
