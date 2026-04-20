"""
Webhook system for enterprise customers.
Sends HTTP POST notifications when invalid numbers are detected.
"""

import json
import httpx
from typing import Dict, List, Optional
from datetime import datetime
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Storage for registered webhooks
WEBHOOK_FILE = Path("webhooks.json")

class WebhookManager:
    def __init__(self):
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        if not WEBHOOK_FILE.exists():
            WEBHOOK_FILE.write_text(json.dumps({}, indent=2))
    
    def _load_data(self) -> Dict:
        try:
            return json.loads(WEBHOOK_FILE.read_text())
        except:
            return {}
    
    def _save_data(self, data: Dict):
        WEBHOOK_FILE.write_text(json.dumps(data, indent=2))
    
    def register(self, api_key: str, url: str, events: List[str] = None):
        """Register a webhook URL for an API key"""
        if events is None:
            events = ["invalid_number", "rate_limit_exceeded"]
        
        data = self._load_data()
        if api_key not in data:
            data[api_key] = []
        
        webhook_id = f"wh_{datetime.utcnow().timestamp()}"
        data[api_key].append({
            "id": webhook_id,
            "url": url,
            "events": events,
            "created_at": datetime.utcnow().isoformat()
        })
        
        self._save_data(data)
        return {"webhook_id": webhook_id, "status": "registered"}
    
    def get_webhooks(self, api_key: str) -> List[Dict]:
        """Get all webhooks for an API key"""
        data = self._load_data()
        return data.get(api_key, [])
    
    def delete(self, api_key: str, webhook_id: str):
        """Delete a webhook"""
        data = self._load_data()
        if api_key in data:
            data[api_key] = [w for w in data[api_key] if w["id"] != webhook_id]
            self._save_data(data)
        return {"status": "deleted"}
    
    async def trigger(self, api_key: str, event: str, payload: Dict):
        """Send webhook notification for an event"""
        data = self._load_data()
        webhooks = data.get(api_key, [])
        
        for webhook in webhooks:
            if event in webhook["events"]:
                try:
                    async with httpx.AsyncClient() as client:
                        await client.post(
                            webhook["url"],
                            json={
                                "event": event,
                                "timestamp": datetime.utcnow().isoformat(),
                                "data": payload,
                                "api_key": api_key[:8] + "..."
                            },
                            timeout=5.0
                        )
                    logger.info(f"Webhook sent to {webhook['url']} for event {event}")
                except Exception as e:
                    logger.error(f"Webhook failed for {webhook['url']}: {str(e)}")

webhook_manager = WebhookManager()