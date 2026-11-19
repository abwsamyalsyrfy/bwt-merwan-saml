import asyncio
import logging
import os
import threading
from firebase_functions import https_fn, options
from firebase_admin import initialize_app, _apps
from telegram import Update

# Initialize Firebase Admin FIRST, and only once.
if not _apps:
    initialize_app()

# Import the bot creation helper
from bot import create_app

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()

# Background event loop dedicated thread to safely process async telegram updates
_bg_loop = asyncio.new_event_loop()
_bg_thread = threading.Thread(target=_bg_loop.run_forever, daemon=True)
_bg_thread.start()

# Global application instance cache
global_ptb_app = None

async def get_or_init_app():
    """Initialize bot application once and reuse it across requests."""
    global global_ptb_app
    if global_ptb_app is None:
        logger.info("Initializing global bot application (Run Once)...")
        global_ptb_app = create_app()
        await global_ptb_app.initialize()
        await global_ptb_app.start()
    return global_ptb_app

def run_async(coro, timeout=55):
    """Run coroutine safely in dedicated background event loop."""
    future = asyncio.run_coroutine_threadsafe(coro, _bg_loop)
    return future.result(timeout=timeout)

@https_fn.on_request(
    memory=options.MemoryOption.GB_1,
    timeout_sec=60,
    region="us-central1"
)
def telegram_webhook(req: https_fn.Request) -> https_fn.Response:
    """Main HTTPS entry point for Telegram Webhook."""
    # Security check if secret token is configured
    if WEBHOOK_SECRET:
        header_secret = req.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if header_secret != WEBHOOK_SECRET:
            logger.warning("Unauthorized access attempt blocked. Secret token mismatch.")
            return https_fn.Response("Forbidden", status=403)

    async def handle_request():
        try:
            ptb_app = await get_or_init_app()

            request_json = req.get_json(silent=True)
            if not request_json:
                logger.warning("Received an empty or invalid JSON request.")
                return https_fn.Response("OK", status=200)

            update = Update.de_json(request_json, ptb_app.bot)
            await ptb_app.process_update(update)
            return https_fn.Response("OK", status=200)

        except Exception as e:
            logger.error(f"An error occurred during update processing: {e}", exc_info=True)
            return https_fn.Response("Error processing update", status=200)

    try:
        return run_async(handle_request())
    except Exception as e:
        logger.error(f"Error executing async handler: {e}", exc_info=True)
        return https_fn.Response("Internal Error", status=200)
