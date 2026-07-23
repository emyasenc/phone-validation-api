"""
Webhook system for enterprise customers.
Stores webhooks in SQLite for persistence across deploys.
"""

import json
import sqlite3
from typing import List, Dict, Optional
from datetime import datetime
import logging
from pathlib import Path
import httpx
import asyncio

logger = logging.getLogger(__name__)

class WebhookManager:
    def __init__(self, db_path="webhooks.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize the database with webhooks table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS webhooks (
                id TEXT PRIMARY KEY,
                api_key TEXT,
                url TEXT,
                events TEXT,
                created_at TIMESTAMP
            )
        """)
        # Also create logs table for delivery tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS webhook_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                webhook_id TEXT,
                event TEXT,
                status TEXT,
                response_code INTEGER,
                error TEXT,
                created_at TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()
        logger.info("✅ Webhook database initialized")
    
    def register(self, api_key: str, url: str, events: List[str] = None) -> Dict:
        """Register a webhook URL for an API key"""
        if events is None:
            events = ["invalid_number", "rate_limit_exceeded"]
        
        import secrets
        webhook_id = f"wh_{datetime.utcnow().timestamp()}"
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO webhooks (id, api_key, url, events, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (webhook_id, api_key, url, json.dumps(events), datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Webhook registered: {webhook_id} for {api_key}")
        
        return {"webhook_id": webhook_id, "status": "registered"}
    
    def get_webhooks(self, api_key: str) -> List[Dict]:
        """Get all webhooks for an API key"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, url, events, created_at
            FROM webhooks WHERE api_key = ?
        """, (api_key,))
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def delete(self, api_key: str, webhook_id: str) -> Dict:
        """Delete a webhook"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM webhooks
            WHERE id = ? AND api_key = ?
        """, (webhook_id, api_key))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        
        if deleted:
            logger.info(f"✅ Webhook deleted: {webhook_id}")
            return {"status": "deleted"}
        return {"status": "not_found"}
    
    async def trigger(self, api_key: str, event: str, payload: Dict, max_retries: int = 3):
        """Send webhook notification with retry logic."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, url FROM webhooks WHERE api_key = ?
        """, (api_key,))
        webhooks = cursor.fetchall()
        conn.close()
        
        for webhook_id, url in webhooks:
            success = await self._send_with_retry(url, event, payload, max_retries)
            self._log_delivery(webhook_id, event, success)
    
    async def _send_with_retry(self, url: str, event: str, payload: Dict, max_retries: int = 3) -> bool:
        """Send webhook with exponential backoff retry."""
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        url,
                        json={
                            "event": event,
                            "timestamp": datetime.utcnow().isoformat(),
                            "data": payload
                        },
                        timeout=5.0
                    )
                    if response.status_code in [200, 201, 202, 204]:
                        logger.info(f"✅ Webhook sent to {url} for event {event}")
                        return True
                    else:
                        logger.warning(f"⚠️ Webhook returned {response.status_code} for {url}")
            except Exception as e:
                logger.warning(f"⚠️ Webhook attempt {attempt+1} failed: {e}")
            
            # Exponential backoff: 1s, 2s, 4s
            await asyncio.sleep(2 ** attempt)
        
        logger.error(f"❌ Webhook failed after {max_retries} attempts for {url}")
        return False
    
    def _log_delivery(self, webhook_id: str, event: str, success: bool):
        """Log webhook delivery attempt."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO webhook_logs (webhook_id, event, status, created_at)
            VALUES (?, ?, ?, ?)
        """, (webhook_id, event, "success" if success else "failed", datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()

# Create singleton instance
webhook_manager = WebhookManager()