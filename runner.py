# -*- coding: utf-8 -*-
"""Runner for Koyeb/containers.

- Keeps bot.py logic intact.
- Creates Bot with AiohttpSession(trust_env=True) so HTTPS_PROXY/HTTP_PROXY are honored (common on PaaS).
- Tries to prefer IPv4 to avoid IPv6-only resolution issues.

Usage:
  python runner.py
"""

import asyncio
import os
import socket

import bot as app  # your original single-file bot


async def _build_bot():
    """Create a Bot instance compatible with the original module, but with better networking defaults."""
    from aiogram import Bot
    from aiogram.client.default import DefaultBotProperties
    from aiogram.enums import ParseMode

    # aiogram v3 uses AiohttpSession
    from aiogram.client.session.aiohttp import AiohttpSession

    token = os.getenv("BOT_TOKEN") or getattr(app, "API_TOKEN", None)
    if not token:
        raise RuntimeError("BOT_TOKEN (or API_TOKEN in bot.py) topilmadi")

    # Prefer IPv4 (some hosts have flaky IPv6 to Telegram)
    connector = None
    try:
        import aiohttp

        connector = aiohttp.TCPConnector(family=socket.AF_INET)
    except Exception:
        connector = None

    try:
        session = AiohttpSession(trust_env=True, connector=connector) if connector else AiohttpSession(trust_env=True)
    except TypeError:
        # Older aiogram versions might not accept 'connector'
        session = AiohttpSession(trust_env=True)

    return Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=session,
    )


async def main():
    # Build patched bot (networking)
    bot = await _build_bot()

    # Patch the module-level bot so handlers that reference `bot` keep working.
    try:
        app.bot = bot
    except Exception:
        pass

    # Ensure router is included once
    try:
        # dp may already include router in bot.py; include_router duplicates handlers, so guard it.
        included = getattr(app, "_KOYEB_ROUTER_INCLUDED", False)
        if not included:
            try:
                app.dp.include_router(app.router)
            except Exception:
                pass
            app._KOYEB_ROUTER_INCLUDED = True
    except Exception:
        pass

    # Startup hooks (if defined)
    try:
        if hasattr(app, "on_startup"):
            try:
                app.dp.startup.register(app.on_startup)
            except Exception:
                pass
    except Exception:
        pass

    # Start polling
    await app.dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
