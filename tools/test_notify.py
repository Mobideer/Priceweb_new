import os
import notify
import config

print("DEBUG: Checking config...")
print(f"TG_BOT_TOKEN in os.environ: {'Yes' if os.environ.get('TG_BOT_TOKEN') else 'No'}")
print(f"TG_CHAT_ID in os.environ: {'Yes' if os.environ.get('TG_CHAT_ID') else 'No'}")
print(f"notify.TG_BOT_TOKEN: {'Yes' if notify.TG_BOT_TOKEN else 'No'}")

test_msg = "🔧 <b>PriceWeb Notification Test</b>\nThis is a manual test message from the server debugging process."
print(f"Sending test message: {test_msg}")
notify.send(test_msg)
print("Done.")
