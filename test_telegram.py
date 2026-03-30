#!/usr/bin/env python3
"""
Test Telegram bot connection
"""

import asyncio
from telegram_notifier import TelegramNotifier, TelegramConfigManager

async def test_telegram():
    try:
        print("Loading Telegram config from environment variables...")
        config = TelegramConfigManager.load_from_env()
        print(f"Bot token: {config.bot_token[:20]}..." if config.bot_token else "Bot token: NOT SET")
        print(f"Chat ID: {config.chat_id}" if config.chat_id else "Chat ID: NOT SET")
        print(f"Enabled: {config.enabled}")
        
        if not config.enabled:
            print("⚠️  Telegram notifications are disabled in environment variables")
            return
        
        if not config.bot_token or not config.chat_id:
            print("❌ Telegram configuration incomplete. Please check your .env file.")
            return
        
        print("\nTesting Telegram connection...")
        async with TelegramNotifier(config) as notifier:
            result = await notifier.test_connection()
            if result:
                print("✅ Telegram connection successful!")
            else:
                print("❌ Telegram connection failed!")
                
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_telegram())
