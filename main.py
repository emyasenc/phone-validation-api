"""
Phone Validation API - Enterprise Edition (Optimized for Speed)
Copyright (c) YASEN-ALPHA
"""

import os
import re
import csv
import io
import logging
from typing import Optional, List, Dict
from datetime import datetime
from functools import lru_cache
import asyncio
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException, Query, Request, UploadFile, File, Form, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import phonenumbers
from phonenumbers import carrier, timezone, geocoder
from phonenumbers.phonenumberutil import NumberParseException
import uvicorn

from core.usage_tracker import tracker
from core.webhooks import webhook_manager

# ============================================
# CONFIGURATION
# ============================================

ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")

# Thread pool for parallel processing
executor = ThreadPoolExecutor(max_workers=4)

# ============================================
# LOGGING SETUP
# ============================================

logging.basicConfig(
    level=logging.INFO if ENVIRONMENT == "production" else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# FASTAPI APP SETUP
# ============================================

app = FastAPI(
    title="Phone Validation API",
    description="Enterprise-grade phone number validation. Validate global numbers, detect carrier, location, and timezone.",
    version="2.0.0",
    docs_url="/docs" if ENVIRONMENT == "development" else None,
    redoc_url=None,
    openapi_url="/openapi.json" if ENVIRONMENT == "development" else None
)

# Warm up the phonenumbers library on startup (eliminates first-request delay)
@app.on_event("startup")
async def warmup():
    """Pre-load phonenumbers library to avoid cold start delay."""
    logger.info("Warming up phonenumbers library...")
    try:
        # Trigger library load with a test number
        test_parsed = phonenumbers.parse("+14155552671", None)
        phonenumbers.is_valid_number(test_parsed)
        logger.info("Phonenumbers library warmed up successfully")
    except Exception as e:
        logger.warning(f"Warmup failed: {e}")

if ENVIRONMENT == "production":
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["*.rapidapi.com", "*.onrender.com", "*.render.com"]
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://rapidapi.com",
        "https://*.rapidapi.com",
        "https://*.onrender.com"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)

# ============================================
# HELPER FUNCTIONS WITH CACHING
# ============================================

def clean_phone_number(phone: str) -> str:
    """Remove all non-digit characters except +"""
    return re.sub(r'[^\d+]', '', phone)

# LRU Cache for validation results (most frequent numbers will be cached)
@lru_cache(maxsize=1000)
def cached_validate_phone(phone: str, include_carrier: bool, include_timezone: bool, include_location: bool) -> str:
    """Cached version of validation logic - returns JSON string for speed."""
    import json
    try:
        cleaned = clean_phone_number(phone)
        parsed = phonenumbers.parse(cleaned, None)
        is_valid = phonenumbers.is_valid_number(parsed)
        is_possible = phonenumbers.is_possible_number(parsed)
        
        response = {
            "phone": phone,
            "valid": is_valid,
            "possible": is_possible,
            "formatted_number": phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164),
            "country_code": parsed.country_code,
            "national_number": str(parsed.national_number),
            "source": "YASEN-ALPHA"
        }
        
        if include_carrier and is_valid:
            try:
                response["carrier"] = carrier.name_for_number(parsed, "en") or "Unknown"
            except:
                response["carrier"] = None
                
        if include_timezone and is_valid:
            try:
                tz_set = timezone.time_zones_for_number(parsed)
                response["timezone"] = list(tz_set) if tz_set else None
            except:
                response["timezone"] = None
                
        if include_location and is_valid:
            try:
                response["location"] = geocoder.description_for_number(parsed, "en") or "Unknown"
            except:
                response["location"] = None
        
        return json.dumps(response)
    except NumberParseException as e:
        # Return error as JSON instead of raising
        error_response = {
            "phone": phone,
            "valid": False,
            "error": str(e),
            "example": "+14155552671"
        }
        return json.dumps(error_response)

def validate_phone_logic(phone: str, include_carrier: bool, include_timezone: bool, include_location: bool) -> dict:
    """Core validation logic with caching support."""
    import json
    cached_result = cached_validate_phone(phone, include_carrier, include_timezone, include_location)
    result = json.loads(cached_result)
    # If there's an error, raise it so the endpoint can handle it
    if "error" in result and not result.get("valid"):
        raise NumberParseException(0, result["error"])
    return result

async def process_bulk_csv(file: UploadFile, validate_func) -> Dict:
    """Process CSV file without pandas - optimized with async."""
    content = await file.read()
    text = content.decode('utf-8')
    csv_reader = csv.DictReader(io.StringIO(text))
    
    fieldnames = csv_reader.fieldnames or []
    phone_column = None
    for col in fieldnames:
        if 'phone' in col.lower():
            phone_column = col
            break
    
    if not phone_column and fieldnames:
        phone_column = fieldnames[0]
    
    results = []
    valid_count = 0
    
    async def process_row(row):
        nonlocal valid_count
        phone = row.get(phone_column, "")
        if not phone:
            return None
        try:
            result = validate_func(phone, True, True, True)
            if result.get("valid"):
                valid_count += 1
            return result
        except Exception as e:
            return {"phone": phone, "valid": False, "error": str(e)}
    
    # Process rows concurrently
    tasks = [process_row(row) for row in csv_reader]
    batch_results = await asyncio.gather(*tasks)
    results = [r for r in batch_results if r is not None]
    
    return {
        "total": len(results),
        "valid_count": valid_count,
        "invalid_count": len(results) - valid_count,
        "valid_percentage": round(valid_count / len(results) * 100, 2) if results else 0,
        "results": results[:10],
        "batch_id": datetime.utcnow().strftime("%Y%m%d%H%M%S")
    }

# ============================================
# PUBLIC ENDPOINTS
# ============================================

@app.get("/")
def root():
    return {
        "service": "YASEN-ALPHA Phone Validation API",
        "version": "2.0.0",
        "status": "operational",
        "pricing": {
            "free": "50 requests/day",
            "pro": "$19/month for 1,000 requests",
            "business": "$49/month for 5,000 requests",
            "enterprise": "Contact for custom pricing"
        },
        "features": [
            "Global number validation (200+ countries)",
            "Carrier detection",
            "Timezone detection", 
            "Geographic location",
            "Batch validation (up to 100 numbers)",
            "Bulk CSV upload",
            "Webhook notifications",
            "Usage dashboard",
            "99.9% uptime SLA"
        ]
    }

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "service": "phone-validation",
        "timestamp": datetime.utcnow().isoformat(),
        "environment": ENVIRONMENT
    }

# ============================================
# PROTECTED ENDPOINTS (Optimized)
# ============================================

@app.get("/validate")
async def validate_phone(
    phone: str = Query(..., description="Phone number with country code (e.g., +14155552671)"),
    include_carrier: bool = Query(True),
    include_timezone: bool = Query(True),
    include_location: bool = Query(True)
):
    """Validate a single phone number - Optimized with caching."""
    api_key = "rapidapi_user"
    tier = "free"
    
    # Basic validation before trying to parse
    if not phone or len(phone) < 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "INVALID_PHONE_FORMAT", "message": "Phone number is too short", "example": "+14155552671"}
        )
    
    try:
        # Run validation in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            executor, 
            validate_phone_logic, 
            phone, include_carrier, include_timezone, include_location
        )
        tracker.increment(api_key, tier)
        logger.info(f"Validation successful for {phone[:10]}...")
        return JSONResponse(content=result)
        
    except NumberParseException as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "INVALID_PHONE_FORMAT", "message": str(e), "example": "+14155552671"}
        )
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "INTERNAL_ERROR", "message": "An internal error occurred."}
        )

@app.post("/validate/batch")
async def validate_batch(
    phones: List[str],
    include_carrier: bool = True,
    include_timezone: bool = True,
    include_location: bool = True
):
    """Validate up to 100 phone numbers in one request - Parallel processing."""
    api_key = "rapidapi_user"
    
    if len(phones) > 100:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Maximum 100 phone numbers per batch request"
        )
    
    async def validate_single(phone):
        try:
            cleaned = clean_phone_number(phone)
            parsed = phonenumbers.parse(cleaned, None)
            is_valid = phonenumbers.is_valid_number(parsed)
            is_possible = phonenumbers.is_possible_number(parsed)
            
            result = {
                "phone": phone,
                "valid": is_valid,
                "possible": is_possible,
                "formatted_number": phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164),
                "country_code": parsed.country_code,
                "national_number": str(parsed.national_number),
            }
            
            if include_carrier and is_valid:
                try:
                    result["carrier"] = carrier.name_for_number(parsed, "en") or "Unknown"
                except:
                    result["carrier"] = None
            
            if include_timezone and is_valid:
                try:
                    tz_set = timezone.time_zones_for_number(parsed)
                    result["timezone"] = list(tz_set) if tz_set else None
                except:
                    result["timezone"] = None
            
            if include_location and is_valid:
                try:
                    result["location"] = geocoder.description_for_number(parsed, "en") or "Unknown"
                except:
                    result["location"] = None
            
            return result, is_valid
                
        except NumberParseException as e:
            return {"phone": phone, "valid": False, "error": f"Invalid format: {str(e)}"}, False
        except Exception as e:
            return {"phone": phone, "valid": False, "error": f"Validation error: {str(e)}"}, False
    
    # Process all phones concurrently
    tasks = [validate_single(phone) for phone in phones]
    results_data = await asyncio.gather(*tasks)
    
    results = []
    valid_count = 0
    for result, is_valid in results_data:
        results.append(result)
        if is_valid:
            valid_count += 1
    
    tracker.increment(api_key, "business", len(phones))
    logger.info(f"Batch validation: {valid_count}/{len(phones)} valid")
    
    return {
        "total": len(results),
        "valid_count": valid_count,
        "results": results,
        "batch_id": datetime.utcnow().strftime("%Y%m%d%H%M%S")
    }

@app.post("/bulk/upload")
async def bulk_upload(
    file: UploadFile = File(...),
    include_carrier: bool = Form(True),
    include_timezone: bool = Form(True),
    include_location: bool = Form(True)
):
    """Upload CSV file for bulk phone validation."""
    api_key = "rapidapi_user"
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    
    result = await process_bulk_csv(file, lambda p, c, t, l: validate_phone_logic(p, c, t, l))
    tracker.increment(api_key, "enterprise", count=result.get("total", 0))
    logger.info(f"Bulk upload processed {result.get('total', 0)} numbers")
    
    return result

@app.post("/webhooks/register")
async def register_webhook(request: Request):
    """Register a webhook URL for notifications."""
    try:
        body = await request.json()
        url = body.get("url")
        events = body.get("events", ["invalid_number"])
        
        if not url:
            raise HTTPException(status_code=400, detail="url is required")
        
        api_key = "rapidapi_user"
        return webhook_manager.register(api_key, url, events)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/webhooks")
def get_webhooks():
    api_key = "rapidapi_user"
    return {"webhooks": webhook_manager.get_webhooks(api_key)}

@app.delete("/webhooks/{webhook_id}")
def delete_webhook(webhook_id: str):
    api_key = "rapidapi_user"
    return webhook_manager.delete(api_key, webhook_id)

@app.get("/stats")
def stats():
    return {
        "service": "YASEN-ALPHA Phone Validation API",
        "version": "2.0.0",
        "countries_supported": "200+",
        "carrier_coverage": "Varies by country",
        "last_updated": "2026-04-26"
    }

@app.get("/legal/gdpr")
def gdpr_compliance():
    return {
        "service": "YASEN-ALPHA Phone Validation API",
        "gdpr_compliant": True,
        "data_processing": {
            "purpose": "Phone number validation only. No data stored permanently.",
            "data_retention": "Usage logs anonymized after 30 days."
        },
        "data_controller": "YASEN-ALPHA"
    }

@app.post("/webhook/rapidapi")
async def rapidapi_webhook(request: Request):
    try:
        payload = await request.json()
        logger.info(f"RapidAPI webhook: {payload}")
        return {"status": "received"}
    except Exception as e:
        logger.error(f"Webhook error: {str(e)}")
        return {"status": "error", "message": str(e)}

# ============================================
# ENTRY POINT
# ============================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8005))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")