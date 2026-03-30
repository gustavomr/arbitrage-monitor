#!/usr/bin/env python3
"""
Test Telegram bot connection
"""

import asyncio
from telegram_notifier import TelegramNotifier, TelegramConfigManager

async def test_telegram():
    try:
        print("Loading Telegram config...")
        config = TelegramConfigManager.load_from_file()
        print(f"Bot token: {config.bot_token[:20]}...")
        print(f"Chat ID: {config.chat_id}")
        print(f"Enabled: {config.enabled}")
        
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
