"""
Usage tracking for enterprise customers.
Stores API call counts per API key in a simple JSON file.
For production scale, replace with PostgreSQL or Redis.
"""

import json
import os
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

# Storage file for usage data
USAGE_FILE = Path(os.environ.get("USAGE_FILE", "usage_data.json"))

class UsageTracker:
    def __init__(self):
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        if not USAGE_FILE.exists():
            USAGE_FILE.write_text(json.dumps({}, indent=2))
    
    def _load_data(self) -> Dict:
        try:
            return json.loads(USAGE_FILE.read_text())
        except:
            return {}
    
    def _save_data(self, data: Dict):
        USAGE_FILE.write_text(json.dumps(data, indent=2))
    
    def increment(self, api_key: str, tier: str = "free", count: int = 1):
        today = date.today().isoformat()
        data = self._load_data()
    
        if api_key not in data:
            data[api_key] = {"tier": tier, "usage": {}}
    
        if today not in data[api_key]["usage"]:
            data[api_key]["usage"][today] = 0
    
        data[api_key]["usage"][today] += count
        data[api_key]["tier"] = tier
    
        self._save_data(data)
    
    def get_usage(self, api_key: str) -> Dict:
        """Get usage statistics for an API key"""
        data = self._load_data()
        today = date.today().isoformat()
        
        if api_key not in data:
            return {
                "api_key": api_key[:8] + "...",
                "today": 0,
                "this_month": 0,
                "limit_today": None,
                "limit_month": None,
                "remaining_today": None,
                "remaining_month": None
            }
        
        user_data = data[api_key]
        tier = user_data.get("tier", "free")
        
        # Get monthly limit based on tier
        from main import MONTHLY_LIMITS, RATE_LIMITS
        monthly_limit = MONTHLY_LIMITS.get(tier, 1500)
        daily_limit = RATE_LIMITS.get(tier, 10) * 60 * 24  # approximate
        
        today_usage = user_data["usage"].get(today, 0)
        
        # Sum current month
        current_month = today[:7]  # YYYY-MM
        month_usage = sum(
            count for day, count in user_data["usage"].items()
            if day.startswith(current_month)
        )
        
        return {
            "api_key": api_key[:8] + "...",
            "tier": tier,
            "today": today_usage,
            "this_month": month_usage,
            "daily_limit": daily_limit,
            "monthly_limit": monthly_limit,
            "remaining_today": daily_limit - today_usage if daily_limit else None,
            "remaining_month": monthly_limit - month_usage if monthly_limit else None,
            "upgrade_url": "https://rapidapi.com/.../pricing"
        }

# Global instance
tracker = UsageTracker()