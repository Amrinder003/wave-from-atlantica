"""
Wave API  v4.0 — Supabase Edition (Stable & Fixed Chat)
"""

from fastapi import FastAPI, Query, Header, HTTPException, UploadFile, File, Request, Response, Form
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.datastructures import UploadFile as StarletteUploadFile
from pydantic import BaseModel, Field
import os, re, json, time, uuid, shutil, math, base64, hashlib, hmac, secrets
import csv
from contextvars import ContextVar
from datetime import datetime, timezone
from difflib import SequenceMatcher
import requests
import smtplib
from typing import Any, Dict, List, Optional, Tuple
from io import StringIO
from email.message import EmailMessage
from supabase import create_client, Client
try:
    from supabase.client import ClientOptions  # type: ignore
except Exception:
    try:
        from supabase import ClientOptions  # type: ignore
    except Exception:
        try:
            from supabase.lib.client_options import ClientOptions  # type: ignore
        except Exception:
            ClientOptions = None  # type: ignore
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from retrieval_chat import retrieve as _retrieve
    from build_kb import build_kb as _build_kb
    HAS_RAG = True
except ImportError:
    HAS_RAG = False
    def _retrieve(*a, **k): return {"matches": []}
    def _build_kb(*a, **k): pass

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG & SUPABASE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SERVER_DIR        = os.path.dirname(os.path.abspath(__file__))
SHOPS_DIR         = os.path.join(SERVER_DIR, "shops")
VENDOR_DIR        = os.path.join(SERVER_DIR, "vendor")
os.makedirs(SHOPS_DIR, exist_ok=True)

PAGE_SIZE         = 24                   
ALLOWED_IMG_EXTS  = {".png", ".jpg", ".jpeg", ".webp", ".jfif", ".jif"}
ALLOWED_IMG_CONTENT_TYPES = {"image/png", "image/jpeg", "image/webp"}
MAX_UPLOAD_BYTES  = int(os.environ.get("MAX_UPLOAD_BYTES", "8388608") or 8388608)
IMAGE_BUCKET      = "product-images"
BUSINESS_PROFILE_IMAGE_PREFIX = "business-profiles"
AVATAR_IMAGE_PREFIX = "avatars"
RATE_LIMIT_STATE: Dict[str, List[float]] = {}
CURRENT_REQUEST: ContextVar[Optional[Request]] = ContextVar("current_request", default=None)
PUBLIC_SHOP_FIELDS = (
    "shop_id", "shop_slug", "name", "profile_image_url", "address", "formatted_address", "overview", "phone", "hours",
    "hours_structured", "category", "business_type", "location_mode", "service_area", "whatsapp", "country_code", "country_name", "timezone_name",
    "region", "city", "postal_code", "street_line1", "street_line2", "currency_code", "latitude",
    "longitude", "supports_pickup", "supports_delivery", "supports_walk_in", "delivery_radius_km",
    "delivery_fee", "pickup_notes",
)

OPENROUTER_KEY    = os.environ.get("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL  = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4.1-mini").strip()
OPENROUTER_FALLBACK_MODELS = [m.strip() for m in os.environ.get("OPENROUTER_FALLBACK_MODELS", "").split(",") if m.strip()]
OPENROUTER_TEMPERATURE = float(os.environ.get("OPENROUTER_TEMPERATURE", "0.2") or 0.2)
OPENROUTER_MAX_TOKENS = int(os.environ.get("OPENROUTER_MAX_TOKENS", "700") or 700)
OPENROUTER_URL    = "https://openrouter.ai/api/v1/chat/completions"
MAPBOX_TOKEN      = os.environ.get("MAPBOX_TOKEN", "").strip()
FX_API_BASE       = os.environ.get("FX_API_BASE", "https://api.frankfurter.dev/v2").strip().rstrip("/")
FX_CACHE_SECONDS  = int(os.environ.get("FX_CACHE_SECONDS", "21600") or 21600)

SUPABASE_URL      = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY      = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
APP_BASE_URL      = os.environ.get("APP_BASE_URL", "http://localhost:8001").strip()
CORS_ORIGINS      = [o.strip() for o in os.environ.get("CORS_ORIGINS", APP_BASE_URL).split(",") if o.strip()]
SMTP_HOST         = os.environ.get("SMTP_HOST", "").strip()
SMTP_PORT         = int(os.environ.get("SMTP_PORT", "587") or 587)
SMTP_USERNAME     = os.environ.get("SMTP_USERNAME", "").strip()
SMTP_PASSWORD     = os.environ.get("SMTP_PASSWORD", "").strip()
SMTP_FROM_EMAIL   = os.environ.get("SMTP_FROM_EMAIL", "").strip()
SMTP_FROM_NAME    = os.environ.get("SMTP_FROM_NAME", "Wave from Atlantica").strip()
SMTP_USE_TLS      = os.environ.get("SMTP_USE_TLS", "true").strip().lower() not in {"0", "false", "no"}
AUTH_ACCESS_COOKIE = os.environ.get("AUTH_ACCESS_COOKIE", "wave_at").strip() or "wave_at"
AUTH_REFRESH_COOKIE = os.environ.get("AUTH_REFRESH_COOKIE", "wave_rt").strip() or "wave_rt"
AUTH_CSRF_COOKIE = os.environ.get("AUTH_CSRF_COOKIE", "wave_csrf").strip() or "wave_csrf"
AUTH_CSRF_HEADER = os.environ.get("AUTH_CSRF_HEADER", "X-CSRF-Token").strip() or "X-CSRF-Token"
AUTH_COOKIE_DOMAIN = os.environ.get("AUTH_COOKIE_DOMAIN", "").strip() or None
AUTH_COOKIE_SAMESITE = os.environ.get("AUTH_COOKIE_SAMESITE", "lax").strip().lower()
AUTH_COOKIE_SECURE_RAW = os.environ.get("AUTH_COOKIE_SECURE", "auto").strip().lower()
AUTH_ACCESS_COOKIE_MAX_AGE = int(os.environ.get("AUTH_ACCESS_COOKIE_MAX_AGE", "3600") or 3600)
AUTH_REFRESH_COOKIE_MAX_AGE = int(os.environ.get("AUTH_REFRESH_COOKIE_MAX_AGE", "2592000") or 2592000)
AUTH_CSRF_COOKIE_MAX_AGE = int(os.environ.get("AUTH_CSRF_COOKIE_MAX_AGE", str(AUTH_REFRESH_COOKIE_MAX_AGE)) or AUTH_REFRESH_COOKIE_MAX_AGE)

if AUTH_COOKIE_SAMESITE not in {"lax", "strict", "none"}:
    AUTH_COOKIE_SAMESITE = "lax"
AUTH_COOKIE_SECURE = APP_BASE_URL.lower().startswith("https://") if AUTH_COOKIE_SECURE_RAW == "auto" else AUTH_COOKIE_SECURE_RAW not in {"0", "false", "no"}
if AUTH_COOKIE_SAMESITE == "none" and not AUTH_COOKIE_SECURE:
    AUTH_COOKIE_SAMESITE = "lax"
TRACK_TOKEN_SECRET = (os.environ.get("TRACK_TOKEN_SECRET", "").strip() or SUPABASE_KEY or OPENROUTER_KEY or APP_BASE_URL or "wave-track-secret").encode("utf-8")
CSRF_PROTECTED_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
CSRF_EXEMPT_PATHS = {
    "/auth/login",
    "/auth/register",
    "/auth/session",
    "/auth/refresh",
    "/auth/forgot-password",
    "/auth/update-password",
}
SECURITY_CSP = "; ".join([
    "default-src 'self'",
    "script-src 'self' 'unsafe-inline'",
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
    "font-src 'self' https://fonts.gstatic.com data:",
    "img-src 'self' data: blob: https:",
    "connect-src 'self' https://ipapi.co",
    "object-src 'none'",
    "base-uri 'self'",
    "frame-ancestors 'none'",
    "form-action 'self'",
])

if not SUPABASE_URL or not SUPABASE_KEY:
    print("⚠️ WARNING: Missing SUPABASE_URL or SUPABASE_SERVICE_KEY.")

def make_supabase_client_options() -> Optional[Any]:
    if ClientOptions is None:
        return None
    try:
        return ClientOptions(auto_refresh_token=False, persist_session=False)
    except TypeError:
        return None

def is_supabase_option_error(exc: Exception) -> bool:
    message = str(exc)
    if isinstance(exc, TypeError):
        return "unexpected keyword" in message or "required positional argument" in message
    if isinstance(exc, AttributeError):
        return any(token in message for token in ("storage", "persist_session", "auto_refresh_token"))
    return False

def make_supabase_client(key: str) -> Optional[Client]:
    if not SUPABASE_URL or not key:
        return None
    options = make_supabase_client_options()
    if options is not None:
        try:
            return create_client(SUPABASE_URL, key, options=options)
        except Exception as exc:
            if not is_supabase_option_error(exc):
                raise
            print(f"WARNING: Supabase ClientOptions fallback triggered: {exc}")
    return create_client(SUPABASE_URL, key)

supabase: Optional[Client] = make_supabase_client(SUPABASE_KEY)
FX_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}

COUNTRY_META: Dict[str, Dict[str, Any]] = {
    "AU": {"name": "Australia", "currency": "AUD", "postal_regex": r"^\d{4}$", "timezone": "Australia/Sydney"},
    "BD": {"name": "Bangladesh", "currency": "BDT", "postal_regex": r"^\d{4}$", "timezone": "Asia/Dhaka"},
    "CA": {"name": "Canada", "currency": "CAD", "postal_regex": r"^[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d$", "timezone": "America/Halifax"},
    "DE": {"name": "Germany", "currency": "EUR", "postal_regex": r"^\d{5}$", "timezone": "Europe/Berlin"},
    "FR": {"name": "France", "currency": "EUR", "postal_regex": r"^\d{5}$", "timezone": "Europe/Paris"},
    "GB": {"name": "United Kingdom", "currency": "GBP", "postal_regex": r"^[A-Za-z]{1,2}\d[A-Za-z\d]?\s?\d[A-Za-z]{2}$", "timezone": "Europe/London"},
    "IE": {"name": "Ireland", "currency": "EUR", "postal_regex": r"^[A-Za-z]\d{2}\s?[A-Za-z0-9]{4}$", "timezone": "Europe/Dublin"},
    "IN": {"name": "India", "currency": "INR", "postal_regex": r"^\d{6}$", "timezone": "Asia/Kolkata"},
    "NZ": {"name": "New Zealand", "currency": "NZD", "postal_regex": r"^\d{4}$", "timezone": "Pacific/Auckland"},
    "PK": {"name": "Pakistan", "currency": "PKR", "postal_regex": r"^\d{5}$", "timezone": "Asia/Karachi"},
    "QA": {"name": "Qatar", "currency": "QAR", "postal_regex": r"^\d{3,4}$", "timezone": "Asia/Qatar"},
    "SA": {"name": "Saudi Arabia", "currency": "SAR", "postal_regex": r"^\d{5}$", "timezone": "Asia/Riyadh"},
    "SG": {"name": "Singapore", "currency": "SGD", "postal_regex": r"^\d{6}$", "timezone": "Asia/Singapore"},
    "US": {"name": "United States", "currency": "USD", "postal_regex": r"^\d{5}(?:-\d{4})?$", "timezone": "America/New_York"},
    "AE": {"name": "United Arab Emirates", "currency": "AED", "postal_regex": r"^[A-Za-z0-9 -]{3,10}$", "timezone": "Asia/Dubai"},
}
SUPPORTED_TIMEZONE_NAMES = sorted({
    "UTC",
    "America/Halifax",
    "America/St_Johns",
    "America/Toronto",
    "America/Vancouver",
    "America/Edmonton",
    "America/Winnipeg",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
    "Europe/London",
    "Europe/Dublin",
    "Europe/Paris",
    "Europe/Berlin",
    "Asia/Dubai",
    "Asia/Kolkata",
    "Asia/Karachi",
    "Asia/Dhaka",
    "Asia/Riyadh",
    "Asia/Qatar",
    "Asia/Singapore",
    "Australia/Sydney",
    "Pacific/Auckland",
    *[str(meta.get("timezone", "")).strip() for meta in COUNTRY_META.values() if str(meta.get("timezone", "")).strip()],
})
DAY_ORDER = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
DAY_LABELS = {"mon": "Mon", "tue": "Tue", "wed": "Wed", "thu": "Thu", "fri": "Fri", "sat": "Sat", "sun": "Sun"}
BUSINESS_TYPE_META: Dict[str, Dict[str, str]] = {
    "retail": {"label": "Retail business", "offering_type": "product", "singular": "product", "plural": "products", "business_label": "store"},
    "service": {"label": "Service business", "offering_type": "service", "singular": "service", "plural": "services", "business_label": "business"},
    "professional": {"label": "Professional practice", "offering_type": "service", "singular": "service", "plural": "services", "business_label": "business"},
    "education": {"label": "Education business", "offering_type": "service", "singular": "service", "plural": "services", "business_label": "business"},
    "creator": {"label": "Creator studio", "offering_type": "offering", "singular": "offering", "plural": "offerings", "business_label": "studio"},
    "other": {"label": "Local business", "offering_type": "offering", "singular": "offering", "plural": "offerings", "business_label": "business"},
}
OFFERING_TYPE_META: Dict[str, Dict[str, str]] = {
    "product": {"label": "Product", "singular": "product", "plural": "products"},
    "service": {"label": "Service", "singular": "service", "plural": "services"},
    "class": {"label": "Class", "singular": "class", "plural": "classes"},
    "event": {"label": "Event", "singular": "event", "plural": "events"},
    "portfolio": {"label": "Portfolio item", "singular": "portfolio item", "plural": "portfolio items"},
    "offering": {"label": "Offering", "singular": "offering", "plural": "offerings"},
}
LEGACY_CATEGORY_BUSINESS_TYPE: Dict[str, str] = {
    "Food": "retail",
    "Clothing": "retail",
    "Electronics": "retail",
    "Beauty": "retail",
    "Books": "retail",
    "Home": "retail",
    "Sports": "retail",
}
LOCATION_MODE_VALUES = {"storefront", "service_area", "hybrid", "online"}
PRICE_MODE_VALUES = {"fixed", "starting_at", "inquiry", "custom", "free"}
AVAILABILITY_MODE_VALUES = {"in_stock", "available", "scheduled", "limited", "on_request", "unavailable"}
PRODUCT_ATTRIBUTE_SCHEMA: Dict[str, List[Tuple[str, str]]] = {
    "Food": [("ingredients", "Ingredients"), ("allergens", "Allergens"), ("serving_size", "Serving size"), ("origin", "Origin")],
    "Clothing": [("brand", "Brand"), ("material", "Material"), ("sizes", "Sizes"), ("colors", "Colors")],
    "Electronics": [("brand", "Brand"), ("model", "Model"), ("warranty", "Warranty"), ("compatibility", "Compatibility")],
    "Beauty": [("brand", "Brand"), ("skin_type", "Skin type"), ("size", "Size"), ("ingredients", "Key ingredients")],
    "Books": [("author", "Author"), ("publisher", "Publisher"), ("format", "Format"), ("language", "Language")],
    "Home": [("brand", "Brand"), ("material", "Material"), ("dimensions", "Dimensions"), ("color", "Color")],
    "Sports": [("brand", "Brand"), ("size", "Size"), ("material", "Material"), ("skill_level", "Skill level")],
}
NON_PRODUCT_ATTRIBUTE_SCHEMA: Dict[str, List[Tuple[str, str]]] = {
    "service": [("service_mode", "Service mode"), ("booking_notes", "Booking notes"), ("service_area_note", "Service area"), ("provider", "Provider")],
    "class": [("level", "Level"), ("audience", "Audience"), ("materials", "Materials"), ("schedule_notes", "Schedule notes")],
    "event": [("event_date", "Event date"), ("venue", "Venue"), ("ticket_notes", "Ticket notes"), ("audience", "Audience")],
    "portfolio": [("medium", "Medium"), ("style", "Style"), ("commission_type", "Commission type"), ("turnaround", "Turnaround")],
    "offering": [("details", "Details"), ("availability_notes", "Availability notes"), ("service_area_note", "Area served"), ("pricing_notes", "Pricing notes")],
}
ATTRIBUTE_QUERY_SYNONYMS: Dict[str, List[str]] = {
    "ingredients": ["ingredient", "ingredients", "made of", "contains", "contain", "inside"],
    "allergens": ["allergen", "allergens", "gluten", "gluten free", "dairy", "nut", "nuts", "peanut", "peanuts", "egg", "soy", "sesame"],
    "serving_size": ["serving size", "portion", "serves", "size"],
    "origin": ["origin", "from where", "made where", "country of origin"],
    "brand": ["brand", "maker", "company"],
    "material": ["material", "fabric", "made of", "cotton", "wool", "leather"],
    "sizes": ["size", "sizes", "sizing"],
    "size": ["size", "sizes", "sizing"],
    "colors": ["color", "colors", "colour", "colours"],
    "color": ["color", "colors", "colour", "colours"],
    "model": ["model", "version"],
    "warranty": ["warranty", "guarantee"],
    "compatibility": ["compatible", "compatibility", "works with", "fit", "fits"],
    "skin_type": ["skin type", "for skin", "sensitive skin", "oily skin", "dry skin"],
    "author": ["author", "writer", "written by"],
    "publisher": ["publisher", "published by"],
    "format": ["format", "paperback", "hardcover", "ebook"],
    "language": ["language"],
    "dimensions": ["dimensions", "dimension", "size", "measurements"],
    "skill_level": ["skill level", "beginner", "intermediate", "advanced"],
    "service_mode": ["service mode", "online", "remote", "in person", "virtual", "onsite", "on site"],
    "booking_notes": ["booking", "appointment", "appointments", "booking notes"],
    "service_area_note": ["service area", "area served", "travel area", "coverage area"],
    "provider": ["provider", "teacher", "agent", "broker", "artist"],
    "level": ["level", "beginner", "intermediate", "advanced"],
    "audience": ["audience", "for whom", "who is this for"],
    "materials": ["materials", "supplies", "what to bring"],
    "schedule_notes": ["schedule", "when", "timing"],
    "event_date": ["date", "when is it", "event date"],
    "venue": ["venue", "location", "where is it"],
    "ticket_notes": ["ticket", "tickets", "entry"],
    "medium": ["medium", "made with"],
    "style": ["style"],
    "commission_type": ["commission", "commissions", "custom work"],
    "turnaround": ["turnaround", "delivery time", "lead time"],
    "details": ["details", "more info"],
    "availability_notes": ["availability notes", "availability"],
    "pricing_notes": ["pricing notes", "pricing"],
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# APP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
app = FastAPI(title="Wave API", version="4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS or ["http://localhost:8001"],
    allow_credentials="*" not in CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def set_security_headers(request: Request, call_next):
    ctx_token = CURRENT_REQUEST.set(request)
    response: Optional[Response] = None
    try:
        if csrf_required(request) and not csrf_valid(request):
            response = JSONResponse({"detail": "Session verification failed. Refresh and try again."}, status_code=403)
        else:
            response = await call_next(request)
    finally:
        CURRENT_REQUEST.reset(ctx_token)
    if response is None:
        response = JSONResponse({"detail": "Request failed."}, status_code=500)
    if session_cookie_present(request) and not cookie_token(AUTH_CSRF_COOKIE, request):
        set_csrf_cookie(response, request=request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    response.headers.setdefault("Content-Security-Policy", SECURITY_CSP)
    if request.url.scheme == "https":
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    if request.url.path.startswith("/auth/"):
        response.headers.setdefault("Cache-Control", "no-store")
    return response

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PYDANTIC MODELS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def normalize_business_type(value: str = "", category: str = "") -> str:
    raw = re.sub(r"[^a-z]+", "", str(value or "").strip().lower())
    aliases = {
        "retail": "retail",
        "shop": "retail",
        "store": "retail",
        "service": "service",
        "services": "service",
        "professional": "professional",
        "education": "education",
        "teacher": "education",
        "school": "education",
        "creator": "creator",
        "artist": "creator",
        "portfolio": "creator",
        "other": "other",
    }
    normalized = aliases.get(raw, "")
    if normalized:
        return normalized
    if category in LEGACY_CATEGORY_BUSINESS_TYPE:
        return LEGACY_CATEGORY_BUSINESS_TYPE[category]
    return "retail"

def business_meta(value: str = "", category: str = "") -> Dict[str, str]:
    normalized = normalize_business_type(value, category)
    return BUSINESS_TYPE_META.get(normalized, BUSINESS_TYPE_META["other"])

def default_offering_type_for_business(value: str = "", category: str = "") -> str:
    return business_meta(value, category).get("offering_type", "offering")

def normalize_location_mode(value: str = "", business_type: str = "", category: str = "") -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if raw in LOCATION_MODE_VALUES:
        return raw
    business_type = normalize_business_type(business_type, category)
    if business_type in {"service", "professional"}:
        return "service_area"
    if business_type == "creator":
        return "online"
    return "storefront"

def normalize_offering_type(value: str = "", business_type: str = "", category: str = "") -> str:
    raw = re.sub(r"[^a-z]+", "", str(value or "").strip().lower())
    aliases = {
        "product": "product",
        "products": "product",
        "service": "service",
        "services": "service",
        "class": "class",
        "classes": "class",
        "course": "class",
        "courses": "class",
        "event": "event",
        "events": "event",
        "portfolio": "portfolio",
        "portfolioitem": "portfolio",
        "offering": "offering",
        "offerings": "offering",
    }
    normalized = aliases.get(raw, "")
    if normalized:
        return normalized
    return default_offering_type_for_business(business_type, category)

def offering_meta(value: str = "", business_type: str = "", category: str = "") -> Dict[str, str]:
    normalized = normalize_offering_type(value, business_type, category)
    return OFFERING_TYPE_META.get(normalized, OFFERING_TYPE_META["offering"])

def normalize_price_mode(value: str = "") -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "fixed": "fixed",
        "startingat": "starting_at",
        "starting_at": "starting_at",
        "from": "starting_at",
        "inquiry": "inquiry",
        "contact": "inquiry",
        "contactforprice": "inquiry",
        "custom": "custom",
        "quote": "custom",
        "free": "free",
    }
    return aliases.get(raw, "fixed")

def normalize_availability_mode(value: str = "", offering_type: str = "", business_type: str = "", category: str = "") -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "in_stock": "in_stock",
        "instock": "in_stock",
        "available": "available",
        "scheduled": "scheduled",
        "limited": "limited",
        "on_request": "on_request",
        "onrequest": "on_request",
        "unavailable": "unavailable",
    }
    normalized = aliases.get(raw, "")
    if normalized in AVAILABILITY_MODE_VALUES:
        return normalized
    offering_type = normalize_offering_type(offering_type, business_type, category)
    return "in_stock" if offering_type == "product" else "available"

def tracks_inventory(offering_type: str = "", business_type: str = "", category: str = "") -> bool:
    return normalize_offering_type(offering_type, business_type, category) == "product"

def uses_variants(offering_type: str = "", business_type: str = "", category: str = "") -> bool:
    return tracks_inventory(offering_type, business_type, category)

def supports_capacity(offering_type: str = "", business_type: str = "", category: str = "") -> bool:
    return normalize_offering_type(offering_type, business_type, category) in {"class", "event"}

def supports_duration(offering_type: str = "", business_type: str = "", category: str = "") -> bool:
    return normalize_offering_type(offering_type, business_type, category) in {"service", "class", "event"}

def offering_attribute_schema(business_type: str = "", category: str = "", offering_type: str = "") -> List[Tuple[str, str]]:
    normalized_offering_type = normalize_offering_type(offering_type, business_type, category)
    if normalized_offering_type == "product":
        return PRODUCT_ATTRIBUTE_SCHEMA.get(str(category or "").strip(), [])
    return NON_PRODUCT_ATTRIBUTE_SCHEMA.get(normalized_offering_type, NON_PRODUCT_ATTRIBUTE_SCHEMA.get("offering", []))

def offering_nouns(shop: Optional[Dict[str, Any]] = None, rows: Optional[List[Dict[str, Any]]] = None, offering_type: str = "") -> Dict[str, str]:
    shop = shop or {}
    business_type = normalize_business_type(shop.get("business_type", ""), shop.get("category", ""))
    explicit = normalize_offering_type(offering_type, business_type, shop.get("category", ""))
    if explicit != "offering":
        meta = offering_meta(explicit, business_type, shop.get("category", ""))
        return {"singular": meta["singular"], "plural": meta["plural"]}
    types = {
        normalize_offering_type(row.get("offering_type", ""), business_type, shop.get("category", ""))
        for row in (rows or [])
        if isinstance(row, dict)
    }
    types.discard("")
    if len(types) == 1:
        only = next(iter(types))
        meta = offering_meta(only, business_type, shop.get("category", ""))
        return {"singular": meta["singular"], "plural": meta["plural"]}
    meta = business_meta(business_type, shop.get("category", ""))
    fallback = offering_meta(meta.get("offering_type", "offering"), business_type, shop.get("category", ""))
    if len(types) > 1:
        fallback = OFFERING_TYPE_META["offering"]
    return {"singular": fallback["singular"], "plural": fallback["plural"]}

def business_display_name(shop: Optional[Dict[str, Any]] = None) -> str:
    shop = shop or {}
    return business_meta(shop.get("business_type", ""), shop.get("category", "")).get("business_label", "business")

def offering_status_label(row: Dict[str, Any], shop: Optional[Dict[str, Any]] = None) -> str:
    shop = shop or {}
    business_type = normalize_business_type(shop.get("business_type", ""), shop.get("category", ""))
    offering_type = normalize_offering_type(row.get("offering_type", ""), business_type, shop.get("category", ""))
    stock = str(row.get("stock", "in") or "in").strip().lower()
    availability_mode = normalize_availability_mode(row.get("availability_mode", ""), offering_type, business_type, shop.get("category", ""))
    if offering_type == "product":
        return {"in": "In Stock", "low": "Low Stock", "out": "Out of Stock"}.get(stock, "Available")
    if stock == "out" or availability_mode == "unavailable":
        return "Unavailable"
    if stock == "low" or availability_mode == "limited":
        return "Limited availability"
    if availability_mode == "scheduled":
        return "Scheduled"
    if availability_mode == "on_request":
        return "On request"
    if offering_type == "service":
        return "Available for booking"
    if offering_type == "class":
        return "Open for enrollment"
    if offering_type == "event":
        return "Available"
    if offering_type == "portfolio":
        return "Available on request"
    return "Available"

def offering_summary_bits(row: Dict[str, Any], shop: Optional[Dict[str, Any]] = None) -> List[str]:
    shop = shop or {}
    business_type = normalize_business_type(shop.get("business_type", ""), shop.get("category", ""))
    offering_type = normalize_offering_type(row.get("offering_type", ""), business_type, shop.get("category", ""))
    bits: List[str] = []
    price = str(row.get("price") or "").strip()
    if price:
        bits.append(price)
    status = offering_status_label(row, shop)
    if status:
        bits.append(status)
    if supports_duration(offering_type, business_type, shop.get("category", "")) and row.get("duration_minutes") not in (None, ""):
        bits.append(f"{int(row['duration_minutes'])} min")
    if supports_capacity(offering_type, business_type, shop.get("category", "")) and row.get("capacity") not in (None, ""):
        bits.append(f"Capacity {int(row['capacity'])}")
    return bits

class RegisterReq(BaseModel):
    email: str
    password: str
    display_name: str = ""

class LoginReq(BaseModel):
    email: str
    password: str

class ForgotPasswordReq(BaseModel):
    email: str

class ResetPasswordReq(BaseModel):
    password: str

class UpdateProfileReq(BaseModel):
    display_name: str = ""

class BrowserSessionReq(BaseModel):
    access_token: str
    refresh_token: str = ""

class ShopInfo(BaseModel):
    name: str
    address: str = ""
    profile_image_url: str = ""
    overview: str = ""
    phone: str = ""
    hours: str = ""
    hours_structured: List[Dict[str, Any]] = []
    category: str = ""
    business_type: str = "retail"
    location_mode: str = ""
    service_area: str = ""
    whatsapp: str = ""
    country_code: str = ""
    country_name: str = ""
    timezone_name: str = ""
    region: str = ""
    city: str = ""
    postal_code: str = ""
    street_line1: str = ""
    street_line2: str = ""
    formatted_address: str = ""
    currency_code: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    supports_pickup: bool = True
    supports_delivery: bool = False
    supports_walk_in: bool = True
    delivery_radius_km: Optional[float] = None
    delivery_fee: Optional[float] = None
    pickup_notes: str = ""

class OrderRequestReq(BaseModel):
    shop_id: str = ""
    business_id: str = ""
    fulfillment_type: str
    customer_name: str
    phone: str
    customer_email: str = ""
    note: str = ""
    preferred_time: str = ""
    delivery_address: str = ""
    items: List[Dict[str, Any]] = []

class OrderStatusUpdateReq(BaseModel):
    status: str

class Product(BaseModel):
    product_id: str = ""
    offering_id: str = ""
    name: str
    overview: str = ""
    price: str = ""
    price_amount: Optional[float] = Field(default=None, ge=0)
    currency_code: str = ""
    offering_type: str = ""
    price_mode: str = "fixed"
    availability_mode: str = ""
    stock: str = "in"
    stock_quantity: Optional[int] = Field(default=None, ge=0)
    duration_minutes: Optional[int] = Field(default=None, ge=0)
    capacity: Optional[int] = Field(default=None, ge=0)
    variants: str = ""
    variant_data: List[Dict[str, Any]] = []
    variant_matrix: List[Dict[str, Any]] = []
    attribute_data: Dict[str, str] = {}
    images: List[str] = []

class CreateShopReq(BaseModel):
    shop_id: Optional[str] = None
    business_id: Optional[str] = None
    shop: Optional[ShopInfo] = None
    business: Optional[ShopInfo] = None

class ReviewReq(BaseModel):
    rating: int = Field(..., ge=1, le=5)
    body: str

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AUTH HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def bearer(auth: Optional[str], request: Optional[Request] = None) -> str:
    if auth:
        m = re.match(r"Bearer\s+(.+)", auth.strip(), re.I)
        if not m:
            raise HTTPException(401, "Use: Bearer <token>")
        return m.group(1).strip()
    token = cookie_token(AUTH_ACCESS_COOKIE, request)
    if token:
        return token
    raise HTTPException(401, "Not signed in")

def get_user(auth: Optional[str], request: Optional[Request] = None):
    token = bearer(auth, request)
    try:
        res = supabase.auth.get_user(token)
        user = res.user
        prof = load_profile(user.id)
        return user, prof
    except Exception:
        raise HTTPException(401, "Session expired or invalid")

def require_verified(user):
    if not user.email_confirmed_at:
        raise HTTPException(403, "Email not verified. Check your inbox.")

def require_supabase() -> Client:
    if supabase is None:
        raise HTTPException(503, "Server is missing Supabase configuration.")
    return supabase

def require_supabase_auth() -> Client:
    auth_client = make_supabase_client(SUPABASE_KEY)
    if auth_client is None:
        raise HTTPException(503, "Server is missing Supabase configuration.")
    return auth_client

def ui_redirect_url() -> str:
    base = (APP_BASE_URL or "http://localhost:8001").strip().rstrip("/")
    return base if base.endswith("/ui") else f"{base}/ui"

def active_request(request: Optional[Request] = None) -> Optional[Request]:
    return request or CURRENT_REQUEST.get()

def cookie_token(name: str, request: Optional[Request] = None) -> str:
    req = active_request(request)
    if req is None:
        return ""
    return str(req.cookies.get(name) or "").strip()

def session_cookie_present(request: Optional[Request] = None) -> bool:
    return bool(cookie_token(AUTH_ACCESS_COOKIE, request) or cookie_token(AUTH_REFRESH_COOKIE, request))

def csrf_cookie_args(httponly: bool) -> Dict[str, Any]:
    common: Dict[str, Any] = {
        "httponly": httponly,
        "secure": AUTH_COOKIE_SECURE,
        "samesite": AUTH_COOKIE_SAMESITE,
        "path": "/",
    }
    if AUTH_COOKIE_DOMAIN:
        common["domain"] = AUTH_COOKIE_DOMAIN
    return common

def csrf_value(request: Optional[Request] = None) -> str:
    existing = cookie_token(AUTH_CSRF_COOKIE, request)
    return existing or secrets.token_urlsafe(32)

def set_csrf_cookie(response: Response, request: Optional[Request] = None, token: str = "") -> str:
    value = str(token or csrf_value(request)).strip()
    response.set_cookie(AUTH_CSRF_COOKIE, value, max_age=max(300, AUTH_CSRF_COOKIE_MAX_AGE), **csrf_cookie_args(httponly=False))
    return value

def csrf_required(request: Request) -> bool:
    if request.method.upper() not in CSRF_PROTECTED_METHODS:
        return False
    if request.url.path in CSRF_EXEMPT_PATHS:
        return False
    if str(request.headers.get("authorization") or "").strip():
        return False
    return session_cookie_present(request)

def csrf_valid(request: Request) -> bool:
    cookie = cookie_token(AUTH_CSRF_COOKIE, request)
    header = str(request.headers.get(AUTH_CSRF_HEADER) or "").strip()
    return bool(cookie and header and secrets.compare_digest(cookie, header))

def set_auth_cookies(response: Response, access_token: str, refresh_token: str = "", access_max_age: Optional[int] = None) -> None:
    common = csrf_cookie_args(httponly=True)
    response.set_cookie(AUTH_ACCESS_COOKIE, access_token, max_age=max(60, int(access_max_age or AUTH_ACCESS_COOKIE_MAX_AGE)), **common)
    if refresh_token:
        response.set_cookie(AUTH_REFRESH_COOKIE, refresh_token, max_age=max(300, AUTH_REFRESH_COOKIE_MAX_AGE), **common)
    set_csrf_cookie(response)

def clear_auth_cookies(response: Response) -> None:
    common = {"path": "/"}
    if AUTH_COOKIE_DOMAIN:
        common["domain"] = AUTH_COOKIE_DOMAIN
    response.delete_cookie(AUTH_ACCESS_COOKIE, **common)
    response.delete_cookie(AUTH_REFRESH_COOKIE, **common)
    response.delete_cookie(AUTH_CSRF_COOKIE, **common)

def client_ip(request: Request) -> str:
    forwarded = str(request.headers.get("x-forwarded-for", "")).split(",")[0].strip()
    if forwarded:
        return forwarded
    return getattr(request.client, "host", "") or "unknown"

def enforce_rate_limit(request: Request, scope: str, limit: int, window_seconds: int, key_suffix: str = "") -> None:
    now = time.time()
    key = f"{scope}:{client_ip(request)}:{key_suffix}"
    bucket = [ts for ts in RATE_LIMIT_STATE.get(key, []) if now - ts < window_seconds]
    if len(bucket) >= limit:
        raise HTTPException(429, "Too many requests. Please wait a moment and try again.")
    bucket.append(now)
    RATE_LIMIT_STATE[key] = bucket

def load_profile(user_id: str) -> Dict[str, Any]:
    prof_res = supabase.table("profiles").select("*").eq("id", user_id).execute()
    return prof_res.data[0] if prof_res.data else {}

def auth_user_payload(user: Any, prof: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    profile = prof or load_profile(user.id)
    shops = supabase.table("shops").select("shop_id, name").eq("owner_user_id", user.id).execute().data
    favs = supabase.table("favourites").select("shop_id").eq("user_id", user.id).execute().data
    revs = supabase.table("reviews").select("id").eq("user_id", user.id).execute().data
    return {
        "ok": True,
        "user_id": user.id,
        "email": user.email,
        "display_name": profile.get("display_name", ""),
        "avatar_url": profile.get("avatar_url", ""),
        "email_verified": user.email_confirmed_at is not None,
        "role": profile.get("role", "customer"),
        "my_shops": shops,
        "fav_count": len(favs),
        "review_count": len(revs),
    }

def supabase_auth_headers(token: Optional[str] = None) -> Dict[str, str]:
    api_key = SUPABASE_KEY or ""
    auth_token = token or api_key
    return {
        "apikey": api_key,
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
    }

def supabase_auth_post(path: str, payload: Dict[str, Any], token: Optional[str] = None) -> None:
    url = f"{SUPABASE_URL.rstrip('/')}/auth/v1/{path.lstrip('/')}"
    res = requests.post(url, headers=supabase_auth_headers(token), json=payload, timeout=20)
    if res.status_code >= 400:
        try:
            detail = res.json().get("msg") or res.json().get("error_description") or res.json().get("message")
        except Exception:
            detail = res.text or "Supabase auth request failed"
        raise HTTPException(400, detail)

def supabase_refresh_session(refresh_token: str) -> Dict[str, Any]:
    refresh = str(refresh_token or "").strip()
    if not refresh:
        raise HTTPException(401, "Session expired. Sign in again.")
    url = f"{SUPABASE_URL.rstrip('/')}/auth/v1/token?grant_type=refresh_token"
    res = requests.post(url, headers=supabase_auth_headers(), json={"refresh_token": refresh}, timeout=20)
    if res.status_code >= 400:
        try:
            detail = res.json().get("msg") or res.json().get("error_description") or res.json().get("message")
        except Exception:
            detail = res.text or "Could not refresh the session."
        raise HTTPException(401, detail or "Could not refresh the session.")
    data = res.json() or {}
    access_token = str(data.get("access_token") or "").strip()
    if not access_token:
        raise HTTPException(401, "Could not refresh the session.")
    return data

def notifications_enabled() -> bool:
    return bool(SMTP_HOST and SMTP_FROM_EMAIL)

def clean_email(value: str) -> str:
    return re.sub(r"\s+", "", str(value or "").strip()).lower()

def send_email_message(to_email: str, subject: str, text_body: str) -> bool:
    recipient = clean_email(to_email)
    if not recipient or not notifications_enabled():
        return False
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = f"{SMTP_FROM_NAME} <{SMTP_FROM_EMAIL}>" if SMTP_FROM_NAME else SMTP_FROM_EMAIL
        msg["To"] = recipient
        msg.set_content(text_body)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
            if SMTP_USE_TLS:
                smtp.starttls()
            if SMTP_USERNAME:
                smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.send_message(msg)
        return True
    except Exception as e:
        print(f"[Notification Warning] Email send failed for {recipient}: {e}")
        return False

def request_track_payload(request_id: str, phone: str) -> str:
    rid = str(request_id or "").strip()
    phone_digits = re.sub(r"\D+", "", str(phone or ""))
    if not rid or not phone_digits:
        return ""
    return f"{rid}:{phone_digits}"

def issue_request_track_token(request_id: str, phone: str) -> str:
    payload = request_track_payload(request_id, phone)
    if not payload:
        return ""
    sig = hmac.new(TRACK_TOKEN_SECRET, payload.encode("utf-8"), hashlib.sha256).digest()
    return (
        base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")
        + "."
        + base64.urlsafe_b64encode(sig).decode("ascii").rstrip("=")
    )

def verify_request_track_token(token: str, request_id: str, phone: str) -> bool:
    token = str(token or "").strip()
    if "." not in token:
        return False
    payload_part, sig_part = token.split(".", 1)
    try:
        payload = base64.urlsafe_b64decode(payload_part + "=" * (-len(payload_part) % 4)).decode("utf-8")
        given_sig = base64.urlsafe_b64decode(sig_part + "=" * (-len(sig_part) % 4))
    except Exception:
        return False
    expected_payload = request_track_payload(request_id, phone)
    if not expected_payload or payload != expected_payload:
        return False
    expected_sig = hmac.new(TRACK_TOKEN_SECRET, payload.encode("utf-8"), hashlib.sha256).digest()
    return hmac.compare_digest(given_sig, expected_sig)

def format_request_items_text(items: List[Dict[str, Any]]) -> str:
    lines = []
    for item in items or []:
        name = str(item.get("name") or "Product").strip()
        qty = int(item.get("qty") or 1)
        variant = str(item.get("variant") or "").strip()
        price = str(item.get("price") or "").strip()
        desc = f"- {qty} x {name}"
        if variant:
            desc += f" ({variant})"
        if price:
            desc += f" — {price}"
        lines.append(desc)
    return "\n".join(lines) or "- Order items not available"

def request_status_human(status: str) -> str:
    mapping = {"new": "New", "accepted": "Accepted", "ready": "Ready for pickup", "completed": "Completed", "cancelled": "Cancelled"}
    return mapping.get(str(status or "").strip().lower(), "New")

def send_request_confirmation_email(shop: Dict[str, Any], payload: Dict[str, Any]) -> None:
    email = clean_email(payload.get("customer_email", ""))
    if not email:
        return
    subject = f"Your {shop.get('name', 'business')} request is confirmed"
    fulfillment = str(payload.get("fulfillment_type") or "pickup").replace("_", " ").title()
    track_token = issue_request_track_token(payload.get("request_id", ""), payload.get("phone", ""))
    tracker_url = f"{ui_redirect_url()}?request_id={payload.get('request_id','')}{f'&track_token={track_token}' if track_token else ''}"
    body = "\n".join([
        f"Hi {payload.get('customer_name') or 'there'},",
        "",
        f"Your request with {shop.get('name', 'the business')} has been received.",
        f"Request ID: {payload.get('request_id', '')}",
        f"Fulfillment: {fulfillment}",
        f"Status: {request_status_human(payload.get('status', 'new'))}",
        "",
        "Items:",
        format_request_items_text(payload.get("items") or []),
        "",
        f"Track in Wave: {tracker_url}",
        "",
        "Thank you for shopping local.",
        "Wave from Atlantica",
    ])
    send_email_message(email, subject, body)

def send_request_status_email(shop: Dict[str, Any], row: Dict[str, Any], status: str) -> None:
    email = clean_email(row.get("customer_email", ""))
    if not email:
        return
    subject = f"Your {shop.get('name', 'business')} request is now {request_status_human(status)}"
    track_token = issue_request_track_token(row.get("request_id", ""), row.get("phone", ""))
    tracker_url = f"{ui_redirect_url()}?request_id={row.get('request_id','')}{f'&track_token={track_token}' if track_token else ''}"
    body = "\n".join([
        f"Hi {row.get('customer_name') or 'there'},",
        "",
        f"Your request with {shop.get('name', 'the business')} was updated.",
        f"Request ID: {row.get('request_id', '')}",
        f"New status: {request_status_human(status)}",
        f"Fulfillment: {str(row.get('fulfillment_type') or 'pickup').replace('_', ' ').title()}",
        "",
        "Items:",
        format_request_items_text(row.get('items') or []),
        "",
        f"Track in Wave: {tracker_url}",
        "",
        "Wave from Atlantica",
    ])
    send_email_message(email, subject, body)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LLM 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def llm_chat(system: str, user: str, max_tokens: Optional[int] = None) -> Dict[str, Any]:
    if not OPENROUTER_KEY:
        raise ValueError("Missing OPENROUTER_API_KEY in environment variables.")

    if max_tokens is None:
        max_tokens = OPENROUTER_MAX_TOKENS

    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    payload: Dict[str, Any] = {
        "model": OPENROUTER_MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": OPENROUTER_TEMPERATURE,
    }
    if OPENROUTER_FALLBACK_MODELS:
        payload["models"] = [OPENROUTER_MODEL, *OPENROUTER_FALLBACK_MODELS]

    r = requests.post(
        OPENROUTER_URL,
        headers={
            "Authorization": f"Bearer {OPENROUTER_KEY}", 
            "Content-Type": "application/json",
            "HTTP-Referer": APP_BASE_URL,
            "X-Title": "Wave from Atlantica"
        },
        json=payload,
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    choice = ((data.get("choices") or [{}])[0]) or {}
    finish_reason = str(choice.get("finish_reason") or "").strip().lower()
    return {
        "content": (choice.get("message", {}) or {}).get("content", "").strip(),
        "model": data.get("model") or OPENROUTER_MODEL,
        "finish_reason": finish_reason,
        "truncated": finish_reason == "length",
    }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def slug(s: str, max_len: int = 40) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9\-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    if not s: raise HTTPException(400, "Invalid ID")
    return s[:max_len]

def gen_shop_id(name: str) -> str:
    base = slug(name)[:22]
    return f"{base}-{uuid.uuid4().hex[:4]}" if base else f"shop-{uuid.uuid4().hex[:6]}"

def gen_product_id(name: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", norm_text(name)).strip("-")[:24]
    return f"prd-{base}-{uuid.uuid4().hex[:5]}" if base else f"prd-{uuid.uuid4().hex[:8]}"

def slug_base(value: str, fallback: str = "item", max_len: int = 60) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", norm_text(value)).strip("-")
    return (base or fallback)[:max_len]

def unique_shop_slug(name: str, ignore_shop_id: str = "") -> str:
    base = slug_base(name, "shop", 50)
    candidate = base
    for i in range(20):
        q = supabase.table("shops").select("shop_id").eq("shop_slug", candidate)
        rows = q.execute().data or []
        if not rows or all(str(r.get("shop_id", "")) == str(ignore_shop_id or "") for r in rows):
            return candidate
        suffix = f"-{i+2}"
        candidate = f"{base[:max(1, 50-len(suffix))]}{suffix}"
    return f"{base[:40]}-{uuid.uuid4().hex[:6]}"

def unique_product_slug(shop_id: str, name: str, ignore_product_id: str = "") -> str:
    base = slug_base(name, "product", 50)
    candidate = base
    for i in range(20):
        try:
            q = supabase.table("products").select("product_id").eq("shop_id", shop_id).eq("product_slug", candidate)
            rows = q.execute().data or []
        except Exception:
            return candidate
        if not rows or all(str(r.get("product_id", "")) == str(ignore_product_id or "") for r in rows):
            return candidate
        suffix = f"-{i+2}"
        candidate = f"{base[:max(1, 50-len(suffix))]}{suffix}"
    return f"{base[:40]}-{uuid.uuid4().hex[:6]}"

SHOP_OPTIONAL_WRITE_COLUMNS = ["business_type", "location_mode", "service_area", "profile_image_url"]
PRODUCT_OPTIONAL_WRITE_COLUMNS = [
    "price_amount", "stock_quantity", "product_slug", "variant_data", "variant_matrix",
    "attribute_data", "currency_code", "offering_type", "price_mode", "availability_mode",
    "duration_minutes", "capacity",
]

def shop_write_payload_with_fallback(shop_id: str, data: Dict[str, Any], existing: bool, strict_cols: Optional[List[str]] = None) -> Tuple[Dict[str, Any], List[str]]:
    payload = dict(data or {})
    unsupported: List[str] = []
    strict = set(strict_cols or [])
    for _ in range(len(SHOP_OPTIONAL_WRITE_COLUMNS) + 1):
        try:
            if existing:
                supabase.table("shops").update(payload).eq("shop_id", shop_id).execute()
            else:
                supabase.table("shops").insert({"shop_id": shop_id, **payload}).execute()
            return payload, unsupported
        except Exception as e:
            msg = str(e or "")
            missing = [col for col in SHOP_OPTIONAL_WRITE_COLUMNS if col not in unsupported and col in msg]
            if not missing:
                raise
            if any(col in strict for col in missing):
                raise HTTPException(500, "The database is missing the latest business template fields. Run the latest Supabase migration before saving this business setup.")
            for col in missing:
                payload.pop(col, None)
                unsupported.append(col)
    raise HTTPException(500, "Could not save the business because the database schema is missing required fields.")

def product_write_payload_with_fallback(shop_id: str, product_id: str, data: Dict[str, Any], existing: bool, strict_cols: Optional[List[str]] = None) -> Tuple[Dict[str, Any], List[str]]:
    payload = dict(data or {})
    unsupported: List[str] = []
    strict = set(strict_cols or [])
    for _ in range(len(PRODUCT_OPTIONAL_WRITE_COLUMNS) + 1):
        try:
            if existing:
                supabase.table("products").update(payload).eq("shop_id", shop_id).eq("product_id", product_id).execute()
            else:
                supabase.table("products").insert({**payload, "shop_id": shop_id, "product_id": product_id}).execute()
            return payload, unsupported
        except Exception as e:
            msg = str(e or "")
            missing = [col for col in PRODUCT_OPTIONAL_WRITE_COLUMNS if col not in unsupported and col in msg]
            if not missing:
                raise
            if any(col in strict for col in missing):
                raise HTTPException(500, "The database is missing the latest offering fields. Run the latest Supabase migration before saving this offering.")
            for col in missing:
                payload.pop(col, None)
                unsupported.append(col)
    raise HTTPException(500, "Could not save the offering because the database schema is missing required fields.")

def norm_ext(ext: str) -> str:
    return ".jpg" if ext.lower() in {".jfif", ".jif"} else ext.lower()

def image_content_type_for_ext(ext: str) -> str:
    ext = norm_ext(ext)
    if ext == ".png":
        return "image/png"
    if ext == ".webp":
        return "image/webp"
    return "image/jpeg"

def read_validated_image(upload: UploadFile, label: str = "image") -> Tuple[str, bytes, str]:
    if not upload or not getattr(upload, "filename", None):
        raise HTTPException(400, f"No {label} provided")
    ext = norm_ext(os.path.splitext(str(upload.filename).lower())[1])
    if ext not in ALLOWED_IMG_EXTS:
        raise HTTPException(400, f"Unsupported {label} type: {ext}")
    content_type = str(upload.content_type or "").split(";")[0].strip().lower()
    if content_type and content_type not in ALLOWED_IMG_CONTENT_TYPES:
        raise HTTPException(400, f"Unsupported {label} content type")
    data = upload.file.read(MAX_UPLOAD_BYTES + 1)
    if not data:
        raise HTTPException(400, f"{label.capitalize()} is empty")
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(400, f"{label.capitalize()} exceeds the upload limit")
    return ext, data, content_type or image_content_type_for_ext(ext)

def upload_public_image(bucket: str, path: str, data: bytes, content_type: str, error_detail: str) -> str:
    sb = require_supabase()
    try:
        sb.storage.from_(bucket).upload(path, data, {"content-type": content_type})
        return sb.storage.from_(bucket).get_public_url(path)
    except Exception as e:
        print(f"[Supabase Storage Error] {e}")
        raise HTTPException(500, error_detail)

def storage_public_path(bucket: str, value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    marker = f"/object/public/{bucket}/"
    if re.match(r"^https?://", raw, re.I):
        idx = raw.lower().find(marker.lower())
        if idx >= 0:
            return raw[idx + len(marker):].split("?", 1)[0]
        token = f"/{bucket}/"
        idx = raw.lower().find(token.lower())
        return raw[idx + len(token):].split("?", 1)[0] if idx >= 0 else ""
    if raw.startswith("/"):
        token = f"/{bucket}/"
        idx = raw.lower().find(token.lower())
        return raw[idx + len(token):].split("?", 1)[0] if idx >= 0 else raw.lstrip("/").split("?", 1)[0]
    return raw.split("?", 1)[0].lstrip("/")

def remove_public_image(bucket: str, value: str) -> bool:
    path = storage_public_path(bucket, value)
    if not path:
        return False
    try:
        require_supabase().storage.from_(bucket).remove([path])
        return True
    except Exception as e:
        print(f"[Supabase Storage Warning] Could not remove {path}: {e}")
        return False

def dedup(items: List[str]) -> List[str]:
    out, seen = [], set()
    for x in items or []:
        v = str(x or "").strip()
        if v and v not in seen: seen.add(v); out.append(v)
    return out

def normalize_image_ref(shop_id: str, value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if re.match(r"^https?://", raw, re.I):
        return raw
    if raw.startswith("/shops/"):
        parts = [p for p in raw.split("/") if p]
        if len(parts) >= 4 and parts[2] == "images":
            return f"/shops/{parts[1]}/images/{os.path.basename(parts[-1])}"
    if raw.startswith("/"):
        return raw
    return f"/shops/{shop_id}/images/{os.path.basename(raw)}"

def normalize_image_list(shop_id: str, images: Any) -> List[str]:
    if isinstance(images, str):
        try:
            images = json.loads(images)
        except Exception:
            images = [images]
    if not isinstance(images, list):
        images = []
    return dedup([normalize_image_ref(shop_id, img) for img in images if str(img or "").strip()])

def save_images(shop_id: str, files: List[UploadFile]) -> List[str]:
    out = []
    for f in files or []:
        if not f or not getattr(f, "filename", None): continue
        ext, data, content_type = read_validated_image(f, "image")
        name = f"{uuid.uuid4().hex}{norm_ext(ext)}"
        path = f"{shop_id}/{name}"
        out.append(upload_public_image(
            IMAGE_BUCKET,
            path,
            data,
            content_type,
            "Failed to upload image. Ensure the 'product-images' bucket is created and public in Supabase.",
        ))
    return out

def save_business_profile_image(shop_id: str, upload: UploadFile) -> str:
    ext, data, content_type = read_validated_image(upload, "business profile image")
    path = f"{BUSINESS_PROFILE_IMAGE_PREFIX}/{shop_id}/{uuid.uuid4().hex}{norm_ext(ext)}"
    return upload_public_image(
        IMAGE_BUCKET,
        path,
        data,
        content_type,
        "Failed to upload the business profile image. Ensure the 'product-images' bucket is created and public in Supabase.",
    )

def serialize_product(row: dict, user_id: str = None, shop: Optional[Dict[str, Any]] = None, viewer_currency: str = "") -> Dict[str, Any]:
    shop_id = row.get("shop_id", "")
    prod_id = row.get("product_id", "")
    imgs = normalize_image_list(shop_id, row.get("images", []))
    shop_row = normalize_shop_record(shop or {})
    price_fields = get_display_price_fields(row, shop_row, viewer_currency)
    
    try:
        rv = supabase.table("reviews").select("rating").eq("shop_id", shop_id).eq("product_id", prod_id).execute().data
    except:
        rv = []
    try:
        product_views = supabase.table("analytics").select("id").eq("shop_id", shop_id).eq("product_id", prod_id).eq("event", "view").execute().data
    except:
        product_views = []
        
    is_fav = False
    if user_id:
        try:
            favs = supabase.table("favourites").select("shop_id").eq("user_id", user_id).eq("shop_id", shop_id).eq("product_id", prod_id).execute().data
            is_fav = len(favs) > 0
        except: pass
    offering_type = normalize_offering_type(row.get("offering_type", ""), shop_row.get("business_type", ""), shop_row.get("category", ""))
    normalized_attributes = normalize_attribute_data(row.get("attribute_data"), shop_row.get("category", ""), offering_type, shop_row.get("business_type", ""))
    variant_data = normalize_variant_data(row.get("variant_data") or row.get("variants", ""), shop_id)
    variant_matrix = normalize_variant_matrix(row.get("variant_matrix", []), variant_data, shop_id)
    return offering_record_with_aliases({
        "product_id": prod_id, "product_slug": row.get("product_slug") or slug_base(row.get("name") or prod_id or "product", "product", 50), "shop_id": shop_id, "shop_slug": shop_row.get("shop_slug", ""), "name": row.get("name", ""),
        "offering_type": offering_type,
        "price_mode": normalize_price_mode(row.get("price_mode", "")),
        "availability_mode": normalize_availability_mode(row.get("availability_mode", ""), offering_type, shop_row.get("business_type", ""), shop_row.get("category", "")),
        "overview": row.get("overview", ""), **price_fields,
        "stock": row.get("stock", "in"), "stock_quantity": row.get("stock_quantity"),
        "duration_minutes": row.get("duration_minutes"),
        "capacity": row.get("capacity"),
        "availability_label": offering_status_label(row, shop_row),
        "variants": row.get("variants", "") or summarize_variant_data(variant_data),
        "variant_data": variant_data,
        "variant_matrix": variant_matrix,
        "attribute_data": normalized_attributes,
        "attribute_lines": format_attribute_lines(normalized_attributes, shop_row.get("category", ""), offering_type, shop_row.get("business_type", "")),
        "images": imgs, "image_count": len(imgs),
        "product_views": len(product_views),
        "avg_rating": round(sum(r["rating"] for r in rv)/len(rv) if rv else 0, 1),
        "review_count": len(rv), "is_favourite": is_fav,
        "quality_flags": product_completeness_flags({**row, "images": imgs, "shop_id": shop_id, "attribute_data": normalized_attributes, "offering_type": offering_type}, shop_row.get("category", ""), shop_row.get("business_type", "")),
    })

def serialize_products_bulk(rows: List[dict], user_id: str = None, shop_map: Optional[Dict[str, Dict[str, Any]]] = None, viewer_currency: str = "") -> List[Dict[str, Any]]:
    if not rows:
        return []

    pairs = {(str(r.get("shop_id", "")), str(r.get("product_id", ""))) for r in rows if r.get("shop_id") and r.get("product_id")}
    shop_ids = sorted({shop_id for shop_id, _ in pairs})
    product_ids = sorted({product_id for _, product_id in pairs})

    review_map: Dict[tuple, List[int]] = {}
    view_map: Dict[tuple, int] = {}
    if shop_ids and product_ids and supabase is not None:
        try:
            review_rows = supabase.table("reviews").select("shop_id, product_id, rating").in_("shop_id", shop_ids).in_("product_id", product_ids).execute().data
            for review in review_rows:
                key = (str(review.get("shop_id", "")), str(review.get("product_id", "")))
                if key in pairs:
                    review_map.setdefault(key, []).append(int(review.get("rating", 0)))
        except Exception:
            review_map = {}
        try:
            view_rows = supabase.table("analytics").select("shop_id, product_id").eq("event", "view").in_("shop_id", shop_ids).in_("product_id", product_ids).execute().data
            for view in view_rows:
                key = (str(view.get("shop_id", "")), str(view.get("product_id", "")))
                if key in pairs:
                    view_map[key] = view_map.get(key, 0) + 1
        except Exception:
            view_map = {}

    fav_set = set()
    if user_id and shop_ids and product_ids and supabase is not None:
        try:
            fav_rows = supabase.table("favourites").select("shop_id, product_id").eq("user_id", user_id).in_("shop_id", shop_ids).in_("product_id", product_ids).execute().data
            fav_set = {
                (str(fav.get("shop_id", "")), str(fav.get("product_id", "")))
                for fav in fav_rows
                if (str(fav.get("shop_id", "")), str(fav.get("product_id", ""))) in pairs
            }
        except Exception:
            fav_set = set()

    out: List[Dict[str, Any]] = []
    for row in rows:
        shop_id = str(row.get("shop_id", ""))
        product_id = str(row.get("product_id", ""))
        key = (shop_id, product_id)
        ratings = review_map.get(key, [])
        imgs = normalize_image_list(shop_id, row.get("images", []))
        shop_row = normalize_shop_record((shop_map or {}).get(shop_id, {}))
        offering_type = normalize_offering_type(row.get("offering_type", ""), shop_row.get("business_type", ""), shop_row.get("category", ""))
        normalized_attributes = normalize_attribute_data(row.get("attribute_data"), shop_row.get("category", ""), offering_type, shop_row.get("business_type", ""))
        variant_data = normalize_variant_data(row.get("variant_data") or row.get("variants", ""), shop_id)
        variant_matrix = normalize_variant_matrix(row.get("variant_matrix", []), variant_data, shop_id)
        out.append(offering_record_with_aliases({
            "product_id": product_id,
            "product_slug": row.get("product_slug") or slug_base(row.get("name") or product_id or "product", "product", 50),
            "shop_id": shop_id,
            "shop_slug": shop_row.get("shop_slug", ""),
            "name": row.get("name", ""),
            "offering_type": offering_type,
            "price_mode": normalize_price_mode(row.get("price_mode", "")),
            "availability_mode": normalize_availability_mode(row.get("availability_mode", ""), offering_type, shop_row.get("business_type", ""), shop_row.get("category", "")),
            "overview": row.get("overview", ""),
            **get_display_price_fields(row, shop_row, viewer_currency),
            "stock": row.get("stock", "in"),
            "stock_quantity": row.get("stock_quantity"),
            "duration_minutes": row.get("duration_minutes"),
            "capacity": row.get("capacity"),
            "availability_label": offering_status_label(row, shop_row),
            "variants": row.get("variants", "") or summarize_variant_data(variant_data),
            "variant_data": variant_data,
            "variant_matrix": variant_matrix,
            "attribute_data": normalized_attributes,
            "attribute_lines": format_attribute_lines(normalized_attributes, shop_row.get("category", ""), offering_type, shop_row.get("business_type", "")),
            "images": imgs,
            "image_count": len(imgs),
            "product_views": view_map.get(key, 0),
            "avg_rating": round(sum(ratings) / len(ratings), 1) if ratings else 0,
            "review_count": len(ratings),
            "is_favourite": key in fav_set,
            "quality_flags": product_completeness_flags({**row, "images": imgs, "shop_id": shop_id, "attribute_data": normalized_attributes, "offering_type": offering_type}, shop_row.get("category", ""), shop_row.get("business_type", "")),
        }))
    return out

def shop_stats(shop_id: str) -> Dict:
    sb = require_supabase()
    prods = sb.table("products").select("images").eq("shop_id", shop_id).execute().data
    normalized_images = [normalize_image_list(shop_id, p.get("images", [])) for p in prods]
    with_imgs = sum(1 for images in normalized_images if images)
    imgs = sum(len(images) for images in normalized_images)
    
    since = int(time.time()) - 86400 * 30
    since_iso = datetime.fromtimestamp(since, tz=timezone.utc).isoformat()
    
    chats = sb.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "chat").gte("created_at", since_iso).execute().data
    shop_views = sb.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "shop_view").gte("created_at", since_iso).execute().data
    product_views = sb.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "view").gte("created_at", since_iso).execute().data
    rvs = sb.table("reviews").select("rating").eq("shop_id", shop_id).execute().data
    
    avg_r = sum(r["rating"] for r in rvs)/len(rvs) if rvs else 0
    
    return {
        "product_count": len(prods),
        "offering_count": len(prods),
        "image_count": imgs,
        "products_with_images": with_imgs,
        "offerings_with_images": with_imgs,
        "chat_hits_30d": len(chats), 
        "shop_views_30d": len(shop_views),
        "product_views_30d": len(product_views),
        "offering_views_30d": len(product_views),
        "avg_rating": round(avg_r, 1)
    }

def track(shop_id: str, event: str, product_id: Optional[str] = None):
    if supabase is None:
        return
    try:
        supabase.table("analytics").insert({
            "shop_id": shop_id,
            "product_id": product_id,
            "event": event,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        print(f"[Analytics Track Warning] shop={shop_id} product={product_id} event={event}: {e}")

def paginate_list(items: List[Any], page: int, page_size: int = PAGE_SIZE) -> Dict[str, Any]:
    total = len(items)
    page_size = max(1, page_size)
    pages = max((total + page_size - 1) // page_size, 1)
    page = max(1, min(page, pages))
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "items": items[start:end],
        "pagination": {
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "total": total,
            "has_prev": page > 1,
            "has_next": page < pages,
        },
    }

def rebuild_kb(shop_id: str):
    shop = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data
    if not shop: return
    shop_row = normalize_shop_record(shop[0])
    
    prods = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute().data
    
    p_serialized = []
    for p in prods:
        imgs = p.get("images", [])
        if isinstance(imgs, str):
            try: imgs = json.loads(imgs)
            except: imgs = []
        p_serialized.append({
            "product_id": p["product_id"], "name": p["name"], "overview": p.get("overview", ""),
            "price": get_display_price_fields(p, shop_row).get("price_native") or p.get("price", ""),
            "stock": p.get("stock", "in"), "variants": p.get("variants", ""),
            "offering_type": normalize_offering_type(p.get("offering_type", ""), shop_row.get("business_type", ""), shop_row.get("category", "")),
            "attribute_data": normalize_attribute_data(p.get("attribute_data"), shop_row.get("category", ""), p.get("offering_type", ""), shop_row.get("business_type", "")),
            "images": imgs
        })
    
    obj = {"shop": {k: shop_row.get(k, "") for k in ("name","profile_image_url","address","overview","phone","hours","hours_structured","category","business_type","location_mode","service_area","country_code","country_name","timezone_name","region","city","postal_code","street_line1","street_line2","currency_code")},
           "products": p_serialized}
           
    d = os.path.join(SHOPS_DIR, shop_id)
    os.makedirs(d, exist_ok=True)
    
    with open(os.path.join(d, "shop.json"), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        
    if HAS_RAG:
        try: _build_kb(os.path.join(SHOPS_DIR, shop_id))
        except Exception: pass

# ── Chat helpers ──
def is_greeting(q: str) -> bool:
    return norm_text(q) in {"hi","hello","hey","hii","hlo","good morning","good evening","good afternoon"}

def is_list_intent(q: str) -> bool:
    qn = norm_text(q)
    return any(
        t in qn
        for t in [
            "what do you sell",
            "what products",
            "what items",
            "what do you have",
            "what do you offer",
            "what services",
            "what classes",
            "show products",
            "show services",
            "show classes",
            "show offerings",
            "product list",
            "service list",
            "catalog",
            "menu",
            "inventory",
            "show all products",
            "show all services",
            "show all classes",
            "show all offerings",
            "list products",
            "list services",
            "list offerings",
            "all products",
            "all services",
            "all offerings",
        ]
    )

def wants_all_images(q: str) -> bool:
    qn = norm_text(q)
    if "gallery" in qn: return True
    if "all" not in qn and "every" not in qn: return False
    return any(v in qn for v in ["image","images","photo","photos","picture","pictures"])

def wants_product_image(q: str) -> bool:
    qn = norm_text(q)
    return any(v in qn for v in ["image", "images", "photo", "photos", "picture", "pictures", "pic"]) and not wants_all_images(q)

def parse_price_value(price: str) -> Optional[float]:
    raw = str(price or "").strip().lower()
    if not raw:
        return None
    cleaned = raw.replace(",", "")
    m = re.search(r"(\d+(?:\.\d+)?)", cleaned)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None

def clean_code(value: str) -> str:
    return re.sub(r"[^A-Za-z]", "", str(value or "").upper())[:2]

def clean_currency(value: str) -> str:
    return re.sub(r"[^A-Za-z]", "", str(value or "").upper())[:3]

def country_meta(country_code: str) -> Dict[str, Any]:
    return COUNTRY_META.get(clean_code(country_code), {})

def currency_for_country(country_code: str) -> str:
    meta = country_meta(country_code)
    return meta.get("currency", "USD")

def timezone_for_country(country_code: str) -> str:
    meta = country_meta(country_code)
    return meta.get("timezone", "UTC")

def is_supported_timezone_name(value: str) -> bool:
    tz_name = str(value or "").strip() or "UTC"
    try:
        ZoneInfo(tz_name)
        return True
    except Exception:
        return tz_name in SUPPORTED_TIMEZONE_NAMES

def build_formatted_address(data: Dict[str, Any]) -> str:
    parts = [
        (data.get("street_line1") or "").strip(),
        (data.get("street_line2") or "").strip(),
        ", ".join([v for v in [(data.get("city") or "").strip(), (data.get("region") or "").strip()] if v]),
        (data.get("postal_code") or "").strip(),
        (data.get("country_name") or "").strip() or country_meta(data.get("country_code", "")).get("name", ""),
    ]
    return ", ".join([p for p in parts if p])

def geocode_structured_address(data: Dict[str, Any]) -> Dict[str, Optional[float]]:
    if not MAPBOX_TOKEN:
        return {"latitude": None, "longitude": None}
    query = (data.get("formatted_address") or build_formatted_address(data) or data.get("address") or "").strip()
    if not query:
        return {"latitude": None, "longitude": None}
    params = {
        "access_token": MAPBOX_TOKEN,
        "limit": 1,
        "autocomplete": "false",
        "types": "address,place,postcode",
    }
    country_code = clean_code(data.get("country_code", ""))
    if country_code:
        params["country"] = country_code.lower()
    try:
        res = requests.get(
            f"https://api.mapbox.com/geocoding/v5/mapbox.places/{requests.utils.quote(query)}.json",
            params=params,
            timeout=8,
        )
        res.raise_for_status()
        feature = ((res.json() or {}).get("features") or [None])[0]
        center = (feature or {}).get("center") or [None, None]
        return {"latitude": center[1], "longitude": center[0]}
    except Exception as e:
        print(f"[Map Geocode Warning] {e}")
        return {"latitude": None, "longitude": None}

def ensure_shop_coordinates(row: Dict[str, Any]) -> Dict[str, Any]:
    out = normalize_shop_record(row or {})
    if not shop_has_mappable_address(out):
        out["latitude"] = None
        out["longitude"] = None
        return out
    try:
        lat = float(out.get("latitude"))
        lng = float(out.get("longitude"))
        if math.isfinite(lat) and math.isfinite(lng):
            return out
    except Exception:
        pass
    geo = geocode_structured_address(out)
    if geo.get("latitude") is None or geo.get("longitude") is None:
        return out
    out["latitude"] = geo["latitude"]
    out["longitude"] = geo["longitude"]
    shop_id = out.get("shop_id")
    if shop_id:
        try:
            supabase.table("shops").update({
                "latitude": geo["latitude"],
                "longitude": geo["longitude"],
            }).eq("shop_id", shop_id).execute()
        except Exception as e:
            print(f"[Map Backfill Warning] {shop_id}: {e}")
    return out

def normalize_shop_record(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row or {})
    out["shop_slug"] = (out.get("shop_slug") or slug_base(out.get("name") or out.get("shop_id") or "shop", "shop", 50)).strip()
    out["business_id"] = out.get("business_id") or out.get("shop_id", "")
    out["business_slug"] = out.get("business_slug") or out.get("shop_slug", "")
    out["profile_image_url"] = str(out.get("profile_image_url") or out.get("business_profile_image_url") or "").strip()
    out["business_profile_image_url"] = out["profile_image_url"]
    out["business_type"] = normalize_business_type(out.get("business_type", ""), out.get("category", ""))
    out["location_mode"] = normalize_location_mode(out.get("location_mode", ""), out.get("business_type", ""), out.get("category", ""))
    out["service_area"] = re.sub(r"\s+", " ", str(out.get("service_area") or "")).strip()
    out["country_code"] = clean_code(out.get("country_code", ""))
    out["country_name"] = (out.get("country_name") or country_meta(out.get("country_code", "")).get("name", "")).strip()
    out["currency_code"] = clean_currency(out.get("currency_code") or currency_for_country(out.get("country_code", "")))
    out["timezone_name"] = (out.get("timezone_name") or timezone_for_country(out.get("country_code", ""))).strip()
    out["hours_structured"] = parse_hours_structured(out.get("hours_structured"))
    out["hours"] = (out.get("hours") or "").strip() or format_hours_structured(out["hours_structured"])
    formatted_address = (out.get("formatted_address") or "").strip()
    if out["location_mode"] in {"storefront", "hybrid"}:
        formatted_address = formatted_address or build_formatted_address(out)
    elif out["location_mode"] == "service_area":
        formatted_address = formatted_address or out["service_area"] or ", ".join([part for part in [out.get("city", ""), out.get("region", ""), out.get("country_name", "")] if part])
    elif out["location_mode"] == "online":
        formatted_address = formatted_address or "Online"
    out["formatted_address"] = formatted_address
    out["address"] = out["formatted_address"] or (out.get("address") or "").strip()
    out["supports_pickup"] = bool(out.get("supports_pickup", True))
    out["supports_delivery"] = bool(out.get("supports_delivery", False))
    out["supports_walk_in"] = bool(out.get("supports_walk_in", True))
    try:
        out["delivery_radius_km"] = round(float(out.get("delivery_radius_km")), 2) if out.get("delivery_radius_km") not in (None, "") else None
    except Exception:
        out["delivery_radius_km"] = None
    try:
        out["delivery_fee"] = round(float(out.get("delivery_fee")), 2) if out.get("delivery_fee") not in (None, "") else None
    except Exception:
        out["delivery_fee"] = None
    out["pickup_notes"] = (out.get("pickup_notes") or "").strip()
    return out

NON_MAPPABLE_ADDRESS_VALUES = {
    "online",
    "online only",
    "remote",
    "virtual",
    "service area",
    "service-area",
    "n/a",
    "na",
    "none",
    "unknown",
    "tbd",
}

def normalize_map_address_label(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip().strip(",.-")
    return text.lower()

def shop_has_mappable_address(row: Dict[str, Any]) -> bool:
    shop = normalize_shop_record(row or {})
    if shop.get("location_mode") not in {"storefront", "hybrid"}:
        return False
    for candidate in (shop.get("street_line1"), shop.get("formatted_address"), shop.get("address")):
        normalized = normalize_map_address_label(candidate)
        if not normalized or normalized in NON_MAPPABLE_ADDRESS_VALUES:
            continue
        return True
    return False

def map_safe_shop_record(row: Dict[str, Any]) -> Dict[str, Any]:
    shop = normalize_shop_record(row or {})
    if not shop_has_mappable_address(shop):
        shop["latitude"] = None
        shop["longitude"] = None
    return shop

def public_shop_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    safe = map_safe_shop_record({key: row.get(key) for key in PUBLIC_SHOP_FIELDS})
    safe["is_open_now"] = bool(row.get("is_open_now", shop_is_open_now(safe)))
    if "stats" in row:
        safe["stats"] = row.get("stats") or {}
    return safe

def offering_record_with_aliases(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row or {})
    out["offering_id"] = out.get("offering_id") or out.get("product_id", "")
    out["offering_slug"] = out.get("offering_slug") or out.get("product_slug", "")
    out["business_id"] = out.get("business_id") or out.get("shop_id", "")
    out["business_slug"] = out.get("business_slug") or out.get("shop_slug", "")
    if out.get("product_views") is not None and out.get("offering_views") is None:
        out["offering_views"] = out.get("product_views")
    if out.get("shop_name") and not out.get("business_name"):
        out["business_name"] = out.get("shop_name")
    if out.get("shop_address") and not out.get("business_address"):
        out["business_address"] = out.get("shop_address")
    if out.get("shop_category") and not out.get("business_category"):
        out["business_category"] = out.get("shop_category")
    return out

def alias_catalog_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload or {})
    if "shop_id" in out and "business_id" not in out:
        out["business_id"] = out.get("shop_id", "")
    if "shop_slug" in out and "business_slug" not in out:
        out["business_slug"] = out.get("shop_slug", "")
    if isinstance(out.get("shop"), dict) and "business" not in out:
        out["business"] = out["shop"]
    if isinstance(out.get("business"), dict) and "shop" not in out:
        out["shop"] = out["business"]
    if isinstance(out.get("shops"), list) and "businesses" not in out:
        out["businesses"] = out["shops"]
    if isinstance(out.get("businesses"), list) and "shops" not in out:
        out["shops"] = out["businesses"]
    if isinstance(out.get("products"), list):
        out["products"] = [offering_record_with_aliases(item) for item in out.get("products") or []]
        out.setdefault("offerings", out["products"])
    elif isinstance(out.get("offerings"), list):
        out["offerings"] = [offering_record_with_aliases(item) for item in out.get("offerings") or []]
        out.setdefault("products", out["offerings"])
    if isinstance(out.get("results"), list):
        out["results"] = [offering_record_with_aliases(item) for item in out.get("results") or []]
        out.setdefault("offerings", out["results"])
    return out

def resolve_shop_by_ref(shop_ref: str) -> Dict[str, Any]:
    ref = str(shop_ref or "").strip()
    if not ref:
        raise HTTPException(404, "Business not found")
    rows = supabase.table("shops").select("*").eq("shop_id", ref).limit(1).execute().data or []
    if not rows:
        rows = supabase.table("shops").select("*").eq("shop_slug", ref).limit(1).execute().data or []
    if not rows:
        raise HTTPException(404, "Business not found")
    return normalize_shop_record(rows[0])

def normalize_order_variant_selections(raw: Any) -> Dict[str, str]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for key, value in raw.items():
        group = re.sub(r"\s+", " ", str(key or "")).strip()[:40]
        label = re.sub(r"\s+", " ", str(value or "")).strip()[:60]
        if group and label:
            out[group] = label
    return out

def selection_signature(selections: Dict[str, str]) -> Tuple[Tuple[str, str], ...]:
    return tuple(sorted((norm_text(group), norm_text(label)) for group, label in (selections or {}).items() if group and label))

def resolve_order_item(product_row: Dict[str, Any], shop_row: Dict[str, Any], raw_item: Dict[str, Any], qty: int) -> Dict[str, Any]:
    variant_data = normalize_variant_data(product_row.get("variant_data") or product_row.get("variants", ""), product_row.get("shop_id", ""))
    variant_matrix = normalize_variant_matrix(product_row.get("variant_matrix", []), variant_data, product_row.get("shop_id", ""))
    requested_selections = normalize_order_variant_selections(raw_item.get("variant_selections") or {})
    matched_selections: Dict[str, str] = {}
    option_delta = 0.0
    if requested_selections:
        for group, label in requested_selections.items():
            match = next((
                item for item in variant_data
                if norm_text(item.get("group", "")) == norm_text(group) and norm_text(item.get("label", "")) == norm_text(label)
            ), None)
            if match is None:
                raise HTTPException(400, f"Invalid option selection for {product_row.get('name') or product_row.get('product_id')}")
            matched_selections[str(match.get("group") or group)] = str(match.get("label") or label)
            option_delta += float(match.get("price_delta") or 0)
    combo = None
    requested_sig = selection_signature(matched_selections)
    if requested_sig:
        combo = next((item for item in variant_matrix if selection_signature(item.get("selections") or {}) == requested_sig), None)
    combo_delta = float((combo or {}).get("price_delta") or 0)
    base_amount = get_row_price_amount(product_row)
    currency_code = get_row_currency_code(product_row, shop_row)
    if base_amount is not None:
        final_amount = round(base_amount + option_delta + combo_delta, 2)
        price_label = format_money(final_amount, currency_code)
    else:
        final_amount = None
        price_label = get_display_price_fields(product_row, shop_row).get("price") or str(product_row.get("price", "")).strip()
    variant_label = ""
    if combo and combo.get("selection_key"):
        variant_label = str(combo.get("selection_key"))
    elif matched_selections:
        variant_label = " / ".join(f"{group}: {label}" for group, label in sorted(matched_selections.items()))
    return {
        "product_id": str(product_row.get("product_id") or "").strip(),
        "name": str(product_row.get("name") or "").strip(),
        "qty": qty,
        "price": price_label,
        "price_amount": final_amount,
        "currency_code": currency_code,
        "variant": variant_label,
        "variant_selections": matched_selections,
        "combo_key": str((combo or {}).get("selection_key") or variant_label).strip(),
    }

def validate_postal_code(country_code: str, postal_code: str) -> bool:
    value = (postal_code or "").strip()
    if not value:
        return False
    pattern = country_meta(country_code).get("postal_regex")
    if not pattern:
        return True
    return re.fullmatch(pattern, value, flags=re.IGNORECASE) is not None

def parse_hours_structured(value: Any) -> List[Dict[str, Any]]:
    raw = value
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = []
    if not isinstance(raw, list):
        return []
    out = []
    seen = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        day = str(item.get("day", "")).strip().lower()[:3]
        if day not in DAY_ORDER or day in seen:
            continue
        closed = bool(item.get("closed", False))
        start = str(item.get("start", "")).strip()
        end = str(item.get("end", "")).strip()
        if not closed:
            if not re.fullmatch(r"^\d{2}:\d{2}$", start) or not re.fullmatch(r"^\d{2}:\d{2}$", end) or start >= end:
                continue
        out.append({"day": day, "closed": closed, "start": start if not closed else "", "end": end if not closed else ""})
        seen.add(day)
    out.sort(key=lambda item: DAY_ORDER.index(item["day"]))
    return out

def format_hours_structured(hours_structured: List[Dict[str, Any]]) -> str:
    if not hours_structured:
        return ""
    grouped = []
    current = None
    for item in hours_structured:
        slot = "Closed" if item.get("closed") else f"{item.get('start')} - {item.get('end')}"
        if current and current["slot"] == slot and DAY_ORDER.index(item["day"]) == DAY_ORDER.index(current["days"][-1]) + 1:
            current["days"].append(item["day"])
        else:
            current = {"slot": slot, "days": [item["day"]]}
            grouped.append(current)
    parts = []
    for group in grouped:
        days = group["days"]
        label = DAY_LABELS[days[0]] if len(days) == 1 else f"{DAY_LABELS[days[0]]}-{DAY_LABELS[days[-1]]}"
        parts.append(f"{label} {group['slot']}")
    return ", ".join(parts)

def shop_matches_schedule(shop: Dict[str, Any], day: str = "", at_time: str = "") -> bool:
    hours_structured = parse_hours_structured(shop.get("hours_structured"))
    if not hours_structured:
        return True
    day_code = str(day or "").strip().lower()[:3]
    if day_code and day_code not in DAY_ORDER:
        return True
    for slot in hours_structured:
        if day_code and slot["day"] != day_code:
            continue
        if slot.get("closed"):
            if day_code:
                return False
            continue
        if not at_time:
            return True
        if re.fullmatch(r"^\d{2}:\d{2}$", at_time) and slot["start"] <= at_time <= slot["end"]:
            return True
    return not day_code and not at_time

def shop_is_open_now(shop: Dict[str, Any]) -> bool:
    hours_structured = parse_hours_structured(shop.get("hours_structured"))
    if not hours_structured:
        return True
    tz_name = (shop.get("timezone_name") or "UTC").strip()
    try:
        now_local = datetime.now(ZoneInfo(tz_name))
    except Exception:
        now_local = datetime.now(timezone.utc)
    day_code = DAY_ORDER[now_local.weekday()]
    time_str = now_local.strftime("%H:%M")
    return shop_matches_schedule(shop, day_code, time_str)

def shop_completeness_flags(shop: Dict[str, Any], stats: Optional[Dict[str, Any]] = None) -> List[str]:
    flags = []
    location_mode = normalize_location_mode(shop.get("location_mode", ""), shop.get("business_type", ""), shop.get("category", ""))
    if location_mode in {"storefront", "hybrid"} and not (shop.get("formatted_address") or shop.get("address")):
        flags.append("Missing address")
    if location_mode == "service_area" and not (shop.get("service_area") or "").strip():
        flags.append("Missing service area")
    if not shop.get("country_code"):
        flags.append("Missing country")
    if not shop.get("timezone_name"):
        flags.append("Missing timezone")
    if not parse_hours_structured(shop.get("hours_structured")):
        flags.append("Missing weekly hours")
    if not (shop.get("phone") or shop.get("whatsapp")):
        flags.append("Missing contact")
    if not (shop.get("overview") or "").strip():
        flags.append("Missing overview")
    if stats is not None:
        if not stats.get("offering_count", stats.get("product_count", 0)):
            flags.append("No offerings")
        elif (stats.get("offerings_with_images", stats.get("products_with_images", 0)) or 0) < (stats.get("offering_count", stats.get("product_count", 0)) or 0):
            flags.append("Offerings missing images")
    return flags

def parse_price_amount(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        try:
            return round(float(value), 2)
        except Exception:
            return None
    return parse_price_value(str(value))

def normalize_variants_text(value: str) -> str:
    parts = []
    seen = set()
    for item in str(value or "").split(","):
        cleaned = re.sub(r"\s+", " ", item).strip()
        key = cleaned.lower()
        if not cleaned or key in seen:
            continue
        seen.add(key)
        parts.append(cleaned)
    return ", ".join(parts)

def normalize_variant_data(raw: Any, shop_id: str = "") -> List[Dict[str, Any]]:
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            raw = []
        else:
            try:
                raw = json.loads(text)
            except Exception:
                # Legacy comma-separated variants become generic options with no price delta.
                raw = [{"group": "Option", "label": item.strip(), "price_delta": 0, "images": []} for item in text.split(",") if item.strip()]
    if not isinstance(raw, list):
        raw = []
    out: List[Dict[str, Any]] = []
    seen = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        group = re.sub(r"\s+", " ", str(item.get("group") or item.get("type") or "Option")).strip()[:40]
        label = re.sub(r"\s+", " ", str(item.get("label") or item.get("name") or "")).strip()[:60]
        if not label:
            continue
        try:
            price_delta = round(float(item.get("price_delta") or item.get("priceDelta") or 0), 2)
        except Exception:
            price_delta = 0.0
        stock_quantity_raw = item.get("stock_quantity", item.get("stockQuantity"))
        try:
            stock_quantity = int(stock_quantity_raw) if stock_quantity_raw not in (None, "") else None
        except Exception:
            stock_quantity = None
        if stock_quantity is not None and stock_quantity < 0:
            stock_quantity = None
        images = normalize_image_list(shop_id, item.get("images", []))
        key = (group.lower(), label.lower(), price_delta)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "group": group or "Option",
            "label": label,
            "price_delta": price_delta,
            "stock_quantity": stock_quantity,
            "stock": derive_stock_from_quantity(stock_quantity, "") if stock_quantity is not None else "",
            "images": images,
        })
    return out

def summarize_variant_data(variant_data: List[Dict[str, Any]]) -> str:
    grouped: Dict[str, List[str]] = {}
    for item in variant_data:
        group = str(item.get("group") or "Option").strip() or "Option"
        label = str(item.get("label") or "").strip()
        if not label:
            continue
        price_delta = float(item.get("price_delta") or 0)
        text = label
        if price_delta:
            sign = "+" if price_delta > 0 else "-"
            text += f" ({sign}{abs(price_delta):.2f})"
        grouped.setdefault(group, []).append(text)
    parts = []
    for group, labels in grouped.items():
        parts.append(f"{group}: {', '.join(labels)}")
    return " | ".join(parts)

def normalize_variant_matrix(raw: Any, variant_data: List[Dict[str, Any]], shop_id: str = "") -> List[Dict[str, Any]]:
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            raw = []
        else:
            try:
                raw = json.loads(text)
            except Exception:
                raw = []
    if not isinstance(raw, list):
        raw = []
    valid_options: Dict[str, set] = {}
    for item in variant_data or []:
        group = str(item.get("group") or "Option").strip() or "Option"
        label = str(item.get("label") or "").strip()
        if not label:
            continue
        valid_options.setdefault(group, set()).add(label)
    out: List[Dict[str, Any]] = []
    seen = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        selections_raw = item.get("selections", {})
        if isinstance(selections_raw, str):
            try:
                selections_raw = json.loads(selections_raw)
            except Exception:
                selections_raw = {}
        if not isinstance(selections_raw, dict):
            continue
        clean_selections: Dict[str, str] = {}
        for key, value in selections_raw.items():
            group = re.sub(r"\s+", " ", str(key or "")).strip()[:40]
            label = re.sub(r"\s+", " ", str(value or "")).strip()[:60]
            if not group or not label:
                continue
            if valid_options and label not in valid_options.get(group, set()):
                continue
            clean_selections[group] = label
        if len(clean_selections) < 2:
            continue
        key = tuple(sorted((k.lower(), v.lower()) for k, v in clean_selections.items()))
        if key in seen:
            continue
        seen.add(key)
        try:
            price_delta = round(float(item.get("price_delta") or item.get("priceDelta") or 0), 2)
        except Exception:
            price_delta = 0.0
        stock_quantity_raw = item.get("stock_quantity", item.get("stockQuantity"))
        try:
            stock_quantity = int(stock_quantity_raw) if stock_quantity_raw not in (None, "") else None
        except Exception:
            stock_quantity = None
        if stock_quantity is not None and stock_quantity < 0:
            stock_quantity = None
        images = normalize_image_list(shop_id, item.get("images", []))
        out.append({
            "selections": clean_selections,
            "selection_key": " / ".join(f"{group}: {label}" for group, label in sorted(clean_selections.items())),
            "price_delta": price_delta,
            "stock_quantity": stock_quantity,
            "stock": derive_stock_from_quantity(stock_quantity, "") if stock_quantity is not None else "",
            "images": images,
        })
    return out

def summarize_variant_matrix(variant_matrix: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for item in variant_matrix or []:
        label = item.get("selection_key") or " / ".join(f"{k}: {v}" for k, v in sorted((item.get("selections") or {}).items()))
        if not label:
            continue
        price_delta = float(item.get("price_delta") or 0)
        stock_quantity = item.get("stock_quantity")
        summary = str(label)
        if price_delta:
            summary += f" ({'+' if price_delta > 0 else '-'}{abs(price_delta):.2f})"
        if stock_quantity not in (None, ""):
            summary += f" [qty {stock_quantity}]"
        parts.append(summary)
    return " | ".join(parts)

def normalize_attribute_data(raw: Any, category: str = "", offering_type: str = "", business_type: str = "") -> Dict[str, str]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        raw = {}
    allowed = {key for key, _ in offering_attribute_schema(business_type, category, offering_type)}
    out: Dict[str, str] = {}
    for key, value in raw.items():
        clean_key = re.sub(r"[^a-z0-9_]", "", str(key or "").lower())
        clean_value = re.sub(r"\s+", " ", str(value or "")).strip()
        if not clean_key or not clean_value:
            continue
        if allowed and clean_key not in allowed:
            continue
        out[clean_key] = clean_value
    return out

def parse_attribute_data_strict(raw: Any, category: str = "", offering_type: str = "", business_type: str = "") -> Dict[str, str]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raise HTTPException(400, "attribute_data_json must be valid JSON")
    if raw in (None, ""):
        raw = {}
    if not isinstance(raw, dict):
        raise HTTPException(400, "attribute_data must be a JSON object")
    allowed = {key for key, _ in offering_attribute_schema(business_type, category, offering_type)}
    invalid_keys = []
    out: Dict[str, str] = {}
    for key, value in raw.items():
        clean_key = re.sub(r"[^a-z0-9_]", "", str(key or "").lower())
        clean_value = re.sub(r"\s+", " ", str(value or "")).strip()
        if not clean_key or not clean_value:
            continue
        if allowed and clean_key not in allowed:
            invalid_keys.append(clean_key)
            continue
        out[clean_key] = clean_value
    if invalid_keys:
        raise HTTPException(400, f"Unsupported offering detail fields: {', '.join(sorted(set(invalid_keys)))}")
    return out

def format_attribute_lines(attribute_data: Dict[str, str], category: str = "", offering_type: str = "", business_type: str = "") -> List[str]:
    label_map = {key: label for key, label in offering_attribute_schema(business_type, category, offering_type)}
    lines = []
    for key, value in attribute_data.items():
        label = label_map.get(key, key.replace("_", " ").title())
        lines.append(f"{label}: {value}")
    return lines

def product_attribute_label_map(category: str = "", offering_type: str = "", business_type: str = "") -> Dict[str, str]:
    return {key: label for key, label in offering_attribute_schema(business_type, category, offering_type)}

def attribute_query_keys(q: str, category: str = "", offering_type: str = "", business_type: str = "") -> List[str]:
    qn = norm_text(q)
    keys = []
    label_map = product_attribute_label_map(category, offering_type, business_type)
    for key, label in label_map.items():
        aliases = [key.replace("_", " "), label] + ATTRIBUTE_QUERY_SYNONYMS.get(key, [])
        if any(norm_text(alias) in qn for alias in aliases if alias):
            keys.append(key)
    return dedup(keys)

def product_attribute_text(row: Dict[str, Any], category: str = "", offering_type: str = "", business_type: str = "") -> str:
    resolved_offering_type = normalize_offering_type(offering_type or row.get("offering_type", ""), business_type, category)
    attribute_data = normalize_attribute_data(row.get("attribute_data"), category, resolved_offering_type, business_type)
    label_map = product_attribute_label_map(category, resolved_offering_type, business_type)
    parts: List[str] = []
    for key, value in attribute_data.items():
        label = label_map.get(key, key.replace("_", " "))
        parts.extend([key.replace("_", " "), label, str(value)])
    return " ".join(parts)

def product_search_blob(row: Dict[str, Any], category: str = "", business_type: str = "") -> str:
    offering_type = normalize_offering_type(row.get("offering_type", ""), business_type, category)
    return " ".join([
        str(row.get("product_id", "")),
        str(row.get("name", "")),
        str(row.get("overview", "")),
        str(row.get("offering_type", "")),
        str(row.get("price_mode", "")),
        str(row.get("availability_mode", "")),
        str(row.get("duration_minutes", "")),
        str(row.get("capacity", "")),
        str(row.get("variants", "")),
        summarize_variant_data(normalize_variant_data(row.get("variant_data") or row.get("variants", ""), row.get("shop_id", ""))),
        summarize_variant_matrix(
            normalize_variant_matrix(
                row.get("variant_matrix", []),
                normalize_variant_data(row.get("variant_data") or row.get("variants", ""), row.get("shop_id", "")),
                row.get("shop_id", ""),
            )
        ),
        product_attribute_text(row, category, offering_type, business_type),
    ]).strip()

def matches_attribute_filter(row: Dict[str, Any], category: str = "", attr_key: str = "", attr_value: str = "", business_type: str = "") -> bool:
    attr_key = re.sub(r"[^a-z0-9_]", "", str(attr_key or "").lower())
    attr_value_n = norm_text(attr_value)
    offering_type = normalize_offering_type(row.get("offering_type", ""), business_type, category)
    attribute_data = normalize_attribute_data(row.get("attribute_data"), category, offering_type, business_type)
    if attr_key and attr_key not in attribute_data:
        return False
    if attr_value_n:
        hay = product_attribute_text(row, category, offering_type, business_type) if not attr_key else str(attribute_data.get(attr_key, ""))
        if attr_value_n not in norm_text(hay):
            return False
    return True

def product_matches_query(row: Dict[str, Any], q: str, category: str = "", business_type: str = "") -> bool:
    qn = norm_text(q)
    if not qn:
        return True
    return qn in norm_text(product_search_blob(row, category, business_type))

def derive_stock_from_quantity(quantity: Optional[int], fallback_stock: str) -> str:
    if quantity is None:
        return str(fallback_stock or "in")
    if quantity <= 0:
        return "out"
    if quantity <= 5:
        return "low"
    return "in"

def product_completeness_flags(product: Dict[str, Any], shop_category: str = "", shop_business_type: str = "") -> List[str]:
    flags = []
    business_type = normalize_business_type(shop_business_type, shop_category)
    offering_type = normalize_offering_type(product.get("offering_type", ""), business_type, shop_category)
    price_mode = normalize_price_mode(product.get("price_mode", ""))
    if price_mode not in {"inquiry", "custom", "free"} and parse_price_amount(product.get("price_amount")) is None and parse_price_amount(product.get("price")) is None:
        flags.append("Missing price")
    if not (product.get("overview") or "").strip():
        flags.append("Missing description")
    if not normalize_image_list(str(product.get("shop_id", "")), product.get("images", [])):
        flags.append("Missing images")
    if tracks_inventory(offering_type, business_type, shop_category) and product.get("stock_quantity") in (None, ""):
        flags.append("Missing quantity")
    if supports_duration(offering_type, business_type, shop_category) and product.get("duration_minutes") in (None, ""):
        flags.append("Missing duration")
    if supports_capacity(offering_type, business_type, shop_category) and product.get("capacity") in (None, ""):
        flags.append("Missing capacity")
    if offering_attribute_schema(business_type, shop_category, offering_type) and not normalize_attribute_data(product.get("attribute_data"), shop_category, offering_type, business_type):
        flags.append("Missing offering details")
    return flags

def format_money(amount: Optional[float], currency_code: str) -> str:
    if amount is None:
        return ""
    code = clean_currency(currency_code) or "USD"
    return f"{code} {amount:,.2f}"

def get_row_price_amount(row: Dict[str, Any]) -> Optional[float]:
    return parse_price_amount(row.get("price_amount")) if row.get("price_amount") not in (None, "") else parse_price_amount(row.get("price"))

def get_row_currency_code(row: Dict[str, Any], shop: Optional[Dict[str, Any]] = None) -> str:
    return clean_currency(row.get("currency_code") or (shop or {}).get("currency_code") or currency_for_country((shop or {}).get("country_code", "")) or "USD")

def get_fx_rate(base_currency: str, target_currency: str) -> Optional[float]:
    base = clean_currency(base_currency)
    target = clean_currency(target_currency)
    if not base or not target or base == target:
        return 1.0
    key = (base, target)
    now = time.time()
    cached = FX_CACHE.get(key)
    if cached and now - cached.get("ts", 0) < FX_CACHE_SECONDS:
        return cached.get("rate")
    try:
        res = requests.get(f"{FX_API_BASE}/rate/{base}/{target}", timeout=8)
        res.raise_for_status()
        data = res.json() or {}
        rate = float(data.get("rate"))
        FX_CACHE[key] = {"rate": rate, "ts": now}
        return rate
    except Exception:
        return None

def get_display_price_fields(row: Dict[str, Any], shop: Optional[Dict[str, Any]] = None, viewer_currency: str = "") -> Dict[str, Any]:
    amount = get_row_price_amount(row)
    source_currency = get_row_currency_code(row, shop)
    price_mode = normalize_price_mode(row.get("price_mode", ""))
    if price_mode == "free":
        original = "Free"
        amount = 0.0
    elif price_mode == "inquiry":
        original = "Price on request"
        amount = None
    elif price_mode == "custom":
        original = "Contact for pricing"
        amount = None
    elif price_mode == "starting_at":
        original = f"From {format_money(amount, source_currency)}" if amount is not None else "Starting price on request"
    else:
        original = format_money(amount, source_currency) if amount is not None else (str(row.get("price", "")) if row.get("price") else "")
    viewer = clean_currency(viewer_currency)
    converted_amount = None
    converted_code = source_currency
    price_text = original
    converted = False
    if price_mode in {"fixed", "starting_at"} and amount is not None and viewer and viewer != source_currency:
        rate = get_fx_rate(source_currency, viewer)
        if rate:
            converted_amount = round(amount * rate, 2)
            converted_code = viewer
            price_text = format_money(converted_amount, converted_code)
            if price_mode == "starting_at":
                price_text = f"From {price_text}"
            converted = True
    return {
        "price": price_text,
        "price_native": original,
        "price_amount": converted_amount if converted_amount is not None else amount,
        "price_amount_native": amount,
        "currency_code": converted_code,
        "currency_code_native": source_currency,
        "price_converted": converted,
    }

def validate_shop_payload(shop: ShopInfo) -> Dict[str, Any]:
    raw = shop.model_dump() if hasattr(shop, "model_dump") else shop.dict()
    data = normalize_shop_record(raw)
    if not data.get("name", "").strip():
        raise HTTPException(400, "Business name is required")
    data["business_type"] = normalize_business_type(data.get("business_type", ""), data.get("category", ""))
    data["location_mode"] = normalize_location_mode(data.get("location_mode", ""), data.get("business_type", ""), data.get("category", ""))
    if not data.get("country_code"):
        raise HTTPException(400, "Select the business country")
    location_mode = data["location_mode"]
    if location_mode in {"storefront", "hybrid"}:
        if not data.get("region", "").strip():
            raise HTTPException(400, "Select or enter the business province/state")
        if not data.get("city", "").strip():
            raise HTTPException(400, "Enter the business city")
        if not data.get("street_line1", "").strip():
            raise HTTPException(400, "Enter the street address")
        if not data.get("postal_code", "").strip():
            raise HTTPException(400, "Enter the postal code")
        if not validate_postal_code(data["country_code"], data["postal_code"]):
            raise HTTPException(400, f"Postal code format is not valid for {data.get('country_name') or data['country_code']}")
    elif location_mode == "service_area":
        if not data.get("service_area", "").strip():
            raise HTTPException(400, "Describe the service area")
    if not data.get("hours_structured"):
        raise HTTPException(400, "Set the business working days and hours")
    if not is_supported_timezone_name(data.get("timezone_name") or "UTC"):
        raise HTTPException(400, "Select a valid business timezone")
    data["currency_code"] = clean_currency(data.get("currency_code") or currency_for_country(data["country_code"]))
    if not any([data.get("supports_pickup"), data.get("supports_delivery"), data.get("supports_walk_in")]):
        raise HTTPException(400, "Enable at least one customer access option")
    if data.get("supports_delivery"):
        if data.get("delivery_radius_km") in (None, ""):
            raise HTTPException(400, "Set a travel or delivery radius")
        if float(data["delivery_radius_km"]) <= 0:
            raise HTTPException(400, "Travel or delivery radius must be greater than zero")
        if data.get("delivery_fee") not in (None, "") and float(data["delivery_fee"]) < 0:
            raise HTTPException(400, "Travel or delivery fee cannot be negative")
    if location_mode in {"storefront", "hybrid"}:
        data["formatted_address"] = data.get("formatted_address") or build_formatted_address(data)
    elif location_mode == "service_area":
        data["formatted_address"] = data.get("formatted_address") or data.get("service_area", "").strip() or ", ".join([part for part in [data.get("city", ""), data.get("region", ""), data.get("country_name", "")] if part])
        data["latitude"] = None
        data["longitude"] = None
    else:
        data["formatted_address"] = data.get("formatted_address") or "Online"
        data["address"] = data["formatted_address"]
        data["latitude"] = None
        data["longitude"] = None
    data["address"] = data["formatted_address"]
    data["hours"] = format_hours_structured(data["hours_structured"])
    if location_mode in {"storefront", "hybrid"} and (data.get("latitude") in (None, "") or data.get("longitude") in (None, "")):
        geo = geocode_structured_address(data)
        if geo.get("latitude") is not None and geo.get("longitude") is not None:
            data["latitude"] = geo["latitude"]
            data["longitude"] = geo["longitude"]
    return data

def normalize_product_payload(product: Product, shop: Dict[str, Any]) -> Dict[str, Any]:
    name = (product.name or "").strip()
    if not name:
        raise HTTPException(400, "Offering name required")
    business_type = normalize_business_type(shop.get("business_type", ""), shop.get("category", ""))
    offering_type = normalize_offering_type(product.offering_type or "", business_type, shop.get("category", ""))
    price_mode = normalize_price_mode(product.price_mode or "")
    amount = parse_price_amount(product.price_amount if product.price_amount not in (None, "") else product.price)
    if price_mode in {"fixed", "starting_at"}:
        if amount is None:
            raise HTTPException(400, "Enter a valid numeric price")
        if amount < 0:
            raise HTTPException(400, "Price cannot be negative")
    elif price_mode == "free":
        amount = 0.0
    else:
        amount = None
    stock_quantity = None
    if tracks_inventory(offering_type, business_type, shop.get("category", "")):
        if product.stock_quantity in (None, ""):
            raise HTTPException(400, "Enter an inventory quantity")
        stock_quantity = int(product.stock_quantity)
        if stock_quantity < 0:
            raise HTTPException(400, "Inventory quantity cannot be negative")
    elif product.stock_quantity not in (None, ""):
        stock_quantity = int(product.stock_quantity)
        if stock_quantity < 0:
            raise HTTPException(400, "Quantity cannot be negative")
    duration_minutes = int(product.duration_minutes) if product.duration_minutes not in (None, "") else None
    if duration_minutes is not None and duration_minutes < 0:
        raise HTTPException(400, "Duration cannot be negative")
    if supports_duration(offering_type, business_type, shop.get("category", "")) and duration_minutes in (None, 0):
        raise HTTPException(400, "Enter a duration")
    capacity = int(product.capacity) if product.capacity not in (None, "") else None
    if capacity is not None and capacity < 0:
        raise HTTPException(400, "Capacity cannot be negative")
    stock_raw = str(product.stock or "in").strip().lower()
    if stock_raw not in {"in", "low", "out"}:
        raise HTTPException(400, "Availability status must be in, low, or out")
    currency_code = clean_currency(product.currency_code or shop.get("currency_code") or currency_for_country(shop.get("country_code", "")))
    variant_data = normalize_variant_data(product.variant_data or product.variants, shop.get("shop_id", "")) if uses_variants(offering_type, business_type, shop.get("category", "")) else []
    variant_matrix = normalize_variant_matrix(product.variant_matrix, variant_data, shop.get("shop_id", "")) if uses_variants(offering_type, business_type, shop.get("category", "")) else []
    normalized_variants = summarize_variant_data(variant_data) or normalize_variants_text(product.variants) if uses_variants(offering_type, business_type, shop.get("category", "")) else ""
    stock_value = derive_stock_from_quantity(stock_quantity, stock_raw) if tracks_inventory(offering_type, business_type, shop.get("category", "")) else stock_raw
    attribute_data = parse_attribute_data_strict(product.attribute_data, shop.get("category", ""), offering_type, business_type)
    price_text = get_display_price_fields({
        "price_amount": round(amount, 2) if amount is not None else None,
        "price": product.price,
        "currency_code": currency_code,
        "price_mode": price_mode,
    }, shop).get("price", "")
    return {
        "name": name,
        "overview": product.overview,
        "offering_type": offering_type,
        "price_mode": price_mode,
        "availability_mode": normalize_availability_mode(product.availability_mode or "", offering_type, business_type, shop.get("category", "")),
        "price_amount": round(amount, 2) if amount is not None else None,
        "currency_code": currency_code,
        "price": price_text,
        "stock": stock_value,
        "stock_quantity": stock_quantity,
        "duration_minutes": duration_minutes,
        "capacity": capacity,
        "variants": normalized_variants,
        "variant_data": variant_data,
        "variant_matrix": variant_matrix,
        "attribute_data": attribute_data,
        "images": dedup(product.images or []),
        "updated_at": "now()",
    }

def extract_budget_limit(q: str) -> Optional[float]:
    qn = norm_text(q)
    m = re.search(r"(?:below|under|less than|cheaper than|upto|up to|within)\s+\$?\s*(\d+(?:\.\d+)?)", qn)
    if m:
        return float(m.group(1))
    m = re.search(r"\$+\s*(\d+(?:\.\d+)?)\s*(?:or less|or below|or under)?", qn)
    if m and any(t in qn for t in ["below", "under", "less", "cheap", "budget", "dollar", "$"]):
        return float(m.group(1))
    return None

def is_budget_query(q: str) -> bool:
    qn = norm_text(q)
    return extract_budget_limit(q) is not None and any(t in qn for t in ["below", "under", "less", "cheap", "budget", "dollar", "$", "within", "upto", "up to"])

def is_location_query(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in ["where", "location", "located", "address", "find you"])

def is_hours_query(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in ["hours", "open", "opening", "closing", "close time", "when are you open"])

def is_contact_query(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in ["phone", "contact", "call", "whatsapp", "number"])

def is_stock_query(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in ["in stock", "available", "availability", "what's in stock", "what is in stock", "instock", "available now", "what is available"])

def is_cheapest_query(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in ["cheapest", "lowest price", "least expensive", "most affordable", "budget friendly"])

def is_recommendation_query(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in ["recommend", "suggest", "best", "popular", "top pick", "top picks", "good option", "good options"])

def is_price_lookup_query(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in ["how much", "price of", "price for", "cost of", "cost for"])

def default_catalog_question(shop: dict, rows: Optional[List[Dict[str, Any]]] = None) -> str:
    nouns = offering_nouns(shop, rows or [])
    plural = nouns["plural"]
    return f"Show all {plural}" if plural != "offerings" else "Show all offerings"

def default_availability_question(shop: dict, rows: Optional[List[Dict[str, Any]]] = None) -> str:
    business_type = normalize_business_type(shop.get("business_type", ""), shop.get("category", ""))
    offering_type = normalize_offering_type("", business_type, shop.get("category", ""))
    if offering_type == "product":
        return "What's in stock?"
    return "What is available right now?"

def row_is_available_for_chat(row: Dict[str, Any], shop: Optional[Dict[str, Any]] = None) -> bool:
    shop = shop or {}
    business_type = normalize_business_type(shop.get("business_type", ""), shop.get("category", ""))
    offering_type = normalize_offering_type(row.get("offering_type", ""), business_type, shop.get("category", ""))
    stock = str(row.get("stock", "in") or "in").strip().lower()
    availability_mode = normalize_availability_mode(row.get("availability_mode", ""), offering_type, business_type, shop.get("category", ""))
    if offering_type == "product":
        return stock != "out"
    return stock != "out" and availability_mode != "unavailable"

def enrich_chat_row(row: Dict[str, Any], shop: Optional[Dict[str, Any]] = None, category: str = "", business_type: str = "") -> Dict[str, Any]:
    shop = shop or {}
    category = category or shop.get("category", "")
    business_type = normalize_business_type(business_type or shop.get("business_type", ""), category)
    offering_type = normalize_offering_type(row.get("offering_type", ""), business_type, category)
    attr_data = normalize_attribute_data(row.get("attribute_data"), category, offering_type, business_type)
    return {
        "shop_id": row.get("shop_id", ""),
        "product_id": row.get("product_id", ""),
        "name": row.get("name", ""),
        "overview": row.get("overview", ""),
        "price": row.get("price", ""),
        "price_amount": row.get("price_amount"),
        "price_amount_native": row.get("price_amount_native"),
        "currency_code": row.get("currency_code"),
        "currency_code_native": row.get("currency_code_native"),
        "price_native": row.get("price_native"),
        "price_converted": row.get("price_converted"),
        "stock": row.get("stock", "in"),
        "stock_quantity": row.get("stock_quantity"),
        "offering_type": offering_type,
        "price_mode": row.get("price_mode", ""),
        "availability_mode": row.get("availability_mode", ""),
        "availability_label": offering_status_label(row, shop or {"business_type": business_type, "category": category}),
        "duration_minutes": row.get("duration_minutes"),
        "capacity": row.get("capacity"),
        "variant_data": row.get("variant_data"),
        "variant_matrix": row.get("variant_matrix"),
        "variants": row.get("variants", ""),
        "images": normalize_image_list(row.get("shop_id", ""), row.get("images", [])),
        "attribute_data": attr_data,
        "attribute_lines": format_attribute_lines(attr_data, category, offering_type, business_type),
    }

def chat_item_line(item: Dict[str, Any], shop: Optional[Dict[str, Any]] = None) -> str:
    summary = " | ".join(offering_summary_bits(item, shop)) or "Details available on request"
    return f"- **{item.get('name', 'Offering')}** - {summary}"

def answer_budget_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    limit = extract_budget_limit(q)
    if limit is None:
        return None
    nouns = offering_nouns(shop, prod_rows)
    plural = nouns["plural"]

    matches = []
    for row in prod_rows:
        value = parse_price_value(row.get("price", ""))
        if value is None or value > limit:
            continue
        enriched = enrich_chat_row(row, shop)
        enriched["_price_value"] = value
        matches.append(enriched)

    matches.sort(key=lambda item: (item["_price_value"], item["name"].lower()))
    suggestions = dedup([default_catalog_question(shop, prod_rows), default_availability_question(shop, prod_rows), "Do you have anything cheaper?"])

    if not matches:
        return {
            "answer": f"I couldn't find any {plural} at {shop_label(shop)} priced below **{limit:g}**.",
            "products": [],
            "meta": {"llm_used": False, "reason": "budget_filter", "suggestions": suggestions},
        }

    top = matches[:6]
    lines = [f"Here {'is' if len(top)==1 else 'are'} the {plural} I found at {shop_label(shop)} under **{limit:g}**:"]
    for item in top:
        lines.append(chat_item_line(item, shop))
        continue
        lines.append(f"• **{item['name']}** — {item.get('price','Price not listed')} *({item.get('stock','in')})*")
    if len(matches) > len(top):
        lines.append(f"\nThere are **{len(matches)}** matching {plural} in total.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top),
        "meta": {"llm_used": False, "reason": "budget_filter", "suggestions": suggestions},
    }

def answer_shop_info_query(shop: dict, q: str) -> Optional[Dict[str, Any]]:
    suggestions = dedup([default_catalog_question(shop), default_availability_question(shop), "What are your prices?"])
    if is_location_query(q):
        location_mode = normalize_location_mode(shop.get("location_mode", ""), shop.get("business_type", ""), shop.get("category", ""))
        service_area = (shop.get("service_area") or "").strip()
        address = (shop.get("address") or "").strip()
        if location_mode == "online":
            return {
                "answer": f"{shop_label(shop)} operates online.",
                "products": [],
                "meta": {"llm_used": False, "reason": "shop_location", "suggestions": suggestions},
            }
        if location_mode == "service_area" and service_area:
            return {
                "answer": f"{shop_label(shop)} serves **{service_area}**.",
                "products": [],
                "meta": {"llm_used": False, "reason": "shop_location", "suggestions": suggestions},
            }
        if address:
            return {
                "answer": f"You can find {shop_label(shop)} at **{address}**." if location_mode != "hybrid" else f"{shop_label(shop)} has a location at **{address}** and may also serve nearby areas.",
                "products": [],
                "meta": {"llm_used": False, "reason": "shop_location", "suggestions": suggestions},
            }
    if is_hours_query(q):
        hours = (shop.get("hours") or "").strip()
        if hours:
            return {
                "answer": f"{shop_label(shop)} is open **{hours}**.",
                "products": [],
                "meta": {"llm_used": False, "reason": "shop_hours", "suggestions": suggestions},
            }
    if is_contact_query(q):
        parts = []
        phone = (shop.get("phone") or "").strip()
        whatsapp = (shop.get("whatsapp") or "").strip()
        if phone:
            parts.append(f"Phone: **{phone}**")
        if whatsapp:
            parts.append(f"WhatsApp: **{whatsapp}**")
        if parts:
            return {
                "answer": f"Here is the best way to reach {shop_label(shop)}:\n" + "\n".join(f"- {part}" for part in parts),
                "products": [],
                "meta": {"llm_used": False, "reason": "shop_contact", "suggestions": suggestions},
            }
    return None

def answer_stock_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    if not is_stock_query(q):
        return None
    nouns = offering_nouns(shop, prod_rows)
    plural = nouns["plural"]
    business_type = normalize_business_type(shop.get("business_type", ""), shop.get("category", ""))
    default_type = normalize_offering_type("", business_type, shop.get("category", ""))
    inventory_mode = any(tracks_inventory(row.get("offering_type", ""), business_type, shop.get("category", "")) for row in prod_rows) or default_type == "product"

    matches = []
    for row in prod_rows:
        if not row_is_available_for_chat(row, shop):
            continue
        matches.append(enrich_chat_row(row, shop))

    if not matches:
        empty_message = f"I don't see any in-stock {plural} at {shop_label(shop)} right now." if inventory_mode else f"I don't see any available {plural} at {shop_label(shop)} right now."
        return {
            "answer": empty_message,
            "products": [],
            "meta": {"llm_used": False, "reason": "stock_filter", "suggestions": dedup([default_catalog_question(shop, prod_rows), "Do you have anything else available?"])},
        }

    top = matches[:6]
    intro = f"Here {'is' if len(top)==1 else 'are'} what {shop_label(shop)} currently has in stock:" if inventory_mode else f"Here {'is' if len(top)==1 else 'are'} the {plural} currently available from {shop_label(shop)}:"
    lines = [intro]
    for item in top:
        lines.append(chat_item_line(item, shop))
    if len(matches) > len(top):
        lines.append(f"\nThere are **{len(matches)}** matching {plural} in total.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top),
        "meta": {"llm_used": False, "reason": "stock_filter", "suggestions": dedup([default_catalog_question(shop, prod_rows), "What are your prices?", "Do you have anything cheaper?"])},
    }

def answer_cheapest_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    if not is_cheapest_query(q):
        return None

    nouns = offering_nouns(shop, prod_rows)
    plural = nouns["plural"]
    ranked = []
    for row in prod_rows:
        value = parse_price_value(row.get("price", ""))
        if value is None:
            continue
        enriched = enrich_chat_row(row, shop)
        enriched["_price_value"] = value
        ranked.append(enriched)

    if not ranked:
        return None

    ranked.sort(key=lambda item: (item["_price_value"], item["name"].lower()))
    top = ranked[:6]
    lines = [f"These are the lowest-priced {plural} I found at {shop_label(shop)}:"]
    for item in top:
        lines.append(chat_item_line(item, shop))
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top),
        "meta": {"llm_used": False, "reason": "cheapest_filter", "suggestions": [f"Do you have anything below 5 dollars?", default_availability_question(shop, prod_rows), default_catalog_question(shop, prod_rows)]},
    }

def fuzzy_match_score(a: str, b: str) -> float:
    a = norm_text(a)
    b = norm_text(b)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()

def extract_candidate_phrases(q: str) -> List[str]:
    qn = norm_text(q)
    phrases = [qn]
    patterns = [
        r"(?:photo|picture|pic|image|images) of ([a-z0-9][a-z0-9 \-]{1,80})",
        r"(?:show|see|find) (?:me )?(?:the )?(?:photo|picture|pic|image|images) (?:of )?([a-z0-9][a-z0-9 \-]{1,80})",
        r"(?:price|cost) of ([a-z0-9][a-z0-9 \-]{1,80})",
        r"(?:how much is|do you have) ([a-z0-9][a-z0-9 \-]{1,80})",
    ]
    for pat in patterns:
        for m in re.finditer(pat, qn):
            phrase = (m.group(1) or "").strip(" ?!.")
            if phrase:
                phrases.append(phrase)
    cleaned = []
    seen = set()
    for phrase in phrases:
        phrase = re.sub(r"\b(please|show|photo|picture|pic|image|images|of|the|a|an)\b", " ", phrase)
        phrase = re.sub(r"\s+", " ", phrase).strip()
        if phrase and phrase not in seen:
            seen.add(phrase)
            cleaned.append(phrase)
    return cleaned[:6]

def choose_chat_products(prod_rows: List[Dict], q: str, answer_text: str = "", prefer_images: bool = False, limit: int = 4, category: str = "", business_type: str = "") -> List[Dict]:
    qn = norm_text(q)
    an = norm_text(answer_text)
    q_tokens = set(re.findall(r"[a-z0-9]+", qn))
    phrases = extract_candidate_phrases(q)
    ranked = []
    for row in prod_rows:
        name = (row.get("name") or "").strip()
        if not name:
            continue
        imgs = normalize_image_list(row.get("shop_id", ""), row.get("images", []))
        if prefer_images and not imgs:
            continue
        name_n = norm_text(name)
        score = 0.0
        if name_n and name_n in qn:
            score += 12
        if name_n and name_n in an:
            score += 10
        best_phrase = max((fuzzy_match_score(phrase, name_n) for phrase in phrases), default=0.0)
        if best_phrase >= 0.92:
            score += 11
        elif best_phrase >= 0.82:
            score += 7
        elif best_phrase >= 0.72:
            score += 4
        row_type = normalize_offering_type(row.get("offering_type", ""), business_type, category)
        attr_text = product_attribute_text(row, category, row_type, business_type)
        row_tokens = set(re.findall(r"[a-z0-9]+", norm_text(f"{name} {row.get('overview','')} {row.get('product_id','')} {row.get('variants','')} {attr_text}")))
        score += len(q_tokens & row_tokens) * 0.9
        if imgs:
            score += 0.5
        if score > 0:
            enriched = enrich_chat_row(row, {"category": category, "business_type": business_type}, category, business_type)
            enriched["_score"] = score
            ranked.append(enriched)
    ranked.sort(key=lambda item: (item["_score"], len(item.get("images", []))), reverse=True)
    return ranked[:limit]

def answer_product_image_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    if not wants_product_image(q):
        return None
    nouns = offering_nouns(shop, prod_rows)
    singular = nouns["singular"]
    matches = choose_chat_products(prod_rows, q, prefer_images=True, limit=4, category=shop.get("category", ""), business_type=shop.get("business_type", ""))
    if not matches:
        return {
            "answer": f"I couldn't find a matching {singular} photo at {shop_label(shop)} yet. Try asking with the {singular} name or say **{default_catalog_question(shop, prod_rows).lower()}**.",
            "products": [],
            "meta": {"llm_used": False, "reason": "product_image_missing", "suggestions": [default_catalog_question(shop, prod_rows), "Show all images", default_availability_question(shop, prod_rows)]},
        }
    top = matches[0]
    lines = [f"Here {'is' if len(matches)==1 else 'are'} the photo{'s' if len(matches)>1 else ''} I found from {shop_label(shop)}:"]
    lines.append(chat_item_line(top, shop))
    if len(matches) > 1:
        lines.append(f"\nI also found **{len(matches)-1}** more matching listing card{'s' if len(matches)-1 != 1 else ''} below.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(matches),
        "meta": {"llm_used": False, "reason": "product_image", "suggestions": [f"What is the price of {top['name']}?", "Show all images", "Do you have more like this?"]},
    }

def answer_price_lookup_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    if not is_price_lookup_query(q):
        return None
    picked = rank_products(prod_rows, q, shop.get("category", ""), shop.get("business_type", ""))
    if not picked:
        return None
    top = picked[0]
    price = top.get("price") or "Price not listed"
    status = top.get("availability_label") or offering_status_label(top, shop)
    answer = f"{shop_label(shop)} has **{top['name']}** listed at **{price}**"
    if status:
        answer += f" and it is currently **{status.lower()}**."
    else:
        answer += "."
    return {
        "answer": answer,
        "products": serialize_products_bulk([top]),
        "meta": {"llm_used": False, "reason": "price_lookup", "suggestions": [f"Show me photos of {top['name']}", f"Do you have more like {top['name']}?", default_availability_question(shop, prod_rows)]},
    }

def answer_attribute_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    category = shop.get("category", "")
    business_type = shop.get("business_type", "")
    keys = attribute_query_keys(q, category, "", business_type)
    if not keys:
        return None
    ranked = rank_products(prod_rows, q, category, business_type)
    matches = []
    for row in ranked or prod_rows:
        row_type = normalize_offering_type(row.get("offering_type", ""), business_type, category)
        attr_data = normalize_attribute_data(row.get("attribute_data"), category, row_type, business_type)
        if not any(attr_data.get(key) for key in keys):
            continue
        matches.append(enrich_chat_row(row, shop, category, business_type))
        if len(matches) >= 5:
            break

    if not matches:
        default_type = normalize_offering_type("", business_type, category)
        label_map = product_attribute_label_map(category, default_type, business_type)
        asked = ", ".join(label_map.get(key, key.replace("_", " ")) for key in keys)
        return {
            "answer": f"I could not find any saved {asked} details for {offering_nouns(shop, prod_rows)['plural']} at {shop_label(shop)} yet.",
            "products": [],
            "meta": {"llm_used": False, "reason": "attribute_missing", "suggestions": [default_catalog_question(shop, prod_rows), default_availability_question(shop, prod_rows), "What do you recommend?"]},
        }

    asked_key = keys[0]
    top_type = normalize_offering_type(matches[0].get("offering_type", ""), business_type, category)
    asked_label = product_attribute_label_map(category, top_type, business_type).get(asked_key, asked_key.replace("_", " ").title())
    top = matches[0]
    top_value = top["attribute_data"].get(asked_key)
    if top_value and len(matches) == 1:
        return {
            "answer": f"For **{top['name']}**, the **{asked_label.lower()}** is **{top_value}**.",
            "products": serialize_products_bulk(matches[:1]),
            "meta": {"llm_used": False, "reason": "attribute_lookup", "suggestions": [f"Show me photos of {top['name']}", f"What is the price of {top['name']}?", default_catalog_question(shop, prod_rows)]},
        }

    lines = [f"Here are the {asked_label.lower()} details I found at {shop_label(shop)}:"]
    for item in matches:
        picked_lines = []
        for key in keys:
            value = item["attribute_data"].get(key)
            if value:
                item_type = normalize_offering_type(item.get("offering_type", ""), business_type, category)
                label = product_attribute_label_map(category, item_type, business_type).get(key, key.replace("_", " ").title())
                picked_lines.append(f"{label}: {value}")
        if not picked_lines:
            continue
        lines.append(f"- **{item['name']}** - {' | '.join(picked_lines)}")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(matches),
        "meta": {"llm_used": False, "reason": "attribute_lookup", "suggestions": [default_catalog_question(shop, prod_rows), default_availability_question(shop, prod_rows), "What do you recommend?"]},
    }

def answer_recommendation_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    if not is_recommendation_query(q):
        return None

    nouns = offering_nouns(shop, prod_rows)
    plural = nouns["plural"]
    budget = extract_budget_limit(q)
    candidates = []
    for row in prod_rows:
        if not row_is_available_for_chat(row, shop):
            continue
        price_value = parse_price_value(row.get("price", ""))
        if budget is not None and price_value is not None and price_value > budget:
            continue
        candidates.append(row)

    if not candidates:
        return {
            "answer": f"I could not find a good recommendation from the available {plural} at {shop_label(shop)} for that request.",
            "products": [],
            "meta": {"llm_used": False, "reason": "recommendation_empty", "suggestions": [default_catalog_question(shop, prod_rows), default_availability_question(shop, prod_rows), "Do you have anything cheaper?"]},
        }

    ranked = rank_products(candidates, q, shop.get("category", ""), shop.get("business_type", ""))
    if not ranked:
        ranked = rank_products(candidates, "", shop.get("category", ""), shop.get("business_type", "")) or [enrich_chat_row(row, shop) for row in candidates]

    top = ranked[:4]
    opener = f"Here are a few good {plural} from {shop_label(shop)}:"
    if budget is not None:
        opener = f"Here are a few good {plural} from {shop_label(shop)} under **{budget:g}**:"
    lines = [opener]
    for item in top:
        line = chat_item_line(item, shop)
        overview = (item.get("overview") or "").strip()
        if overview:
            line += f"\n  {overview[:120]}"
        lines.append(line)
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top),
        "meta": {"llm_used": False, "reason": "recommendation", "suggestions": build_chat_suggestions(q, shop, top)},
    }

def rank_products(products: List[Dict], q: str, category: str = "", business_type: str = "") -> List[Dict]:
    qn = norm_text(q); qt = set(re.findall(r"[a-z0-9]+", qn))
    scored = []
    for r in products:
        name = (r.get("name") or "").strip(); ov = (r.get("overview") or "").strip()
        row_type = normalize_offering_type(r.get("offering_type", ""), business_type, category)
        attr_text = product_attribute_text(r, category, row_type, business_type)
        hay = norm_text(f"{name} {ov} {r.get('variants','')} {attr_text}"); ht = set(re.findall(r"[a-z0-9]+", hay))
        s = 0.0
        if qn:
            if norm_text(name) and norm_text(name) in qn:
                s += 8
            if qt & ht:
                s += len(qt & ht) * 1.4
            if qn in hay:
                s += 5
        else:
            s = 1.0
            if row_is_available_for_chat(r, {"category": category, "business_type": business_type}):
                s += 1.0
            if normalize_image_list(r.get("shop_id", ""), r.get("images", [])):
                s += 0.4
            if (r.get("overview") or "").strip():
                s += 0.3
            if parse_price_amount(r.get("price_amount")) is not None or parse_price_amount(r.get("price")) is not None:
                s += 0.2
            try:
                s += min(float(r.get("avg_rating") or 0), 5.0) * 0.1
            except Exception:
                pass
            try:
                s += min(float(r.get("product_views") or 0), 100.0) * 0.01
            except Exception:
                pass
        if s > 0:
            scored.append((s, enrich_chat_row(r, {"category": category, "business_type": business_type}, category, business_type)))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored]

def build_context(shop: dict, picked: List[Dict], all_rows: List, rag_chunks: Optional[List[str]] = None) -> str:
    nouns = offering_nouns(shop, all_rows)
    business_name = business_display_name(shop)
    lines = [
        f"Business: {shop['name']}",
        f"Business type: {normalize_business_type(shop.get('business_type', ''), shop.get('category', ''))}",
        f"Business label: {business_name}",
        f"Shop ID: {shop.get('shop_id','')}",
        f"Shop Slug: {shop.get('shop_slug','')}",
    ]
    if shop.get("address"): lines.append(f"Address: {shop.get('address','')}")
    if shop.get("service_area"): lines.append(f"Service area: {shop.get('service_area','')}")
    if shop.get("phone"): lines.append(f"Phone: {shop['phone']}")
    if shop.get("hours"): lines.append(f"Hours: {shop['hours']}")
    if shop.get("category"): lines.append(f"Category: {shop['category']}")
    if shop.get("location_mode"): lines.append(f"Location mode: {shop['location_mode']}")
    if shop.get("whatsapp"): lines.append(f"WhatsApp: {shop['whatsapp']}")
    if shop.get("overview"): lines.append(f"About: {shop['overview']}")
    lines.append(f"Total {nouns['plural']}: {len(all_rows)}")
    if picked:
        lines.append(f"\nRelevant {nouns['plural']}:")
        for p in picked[:6]:
            img_part = f' | Photo: ![{p["name"]}]({p["images"][0]})' if p.get("images") else ""
            row_type = normalize_offering_type(p.get("offering_type", ""), shop.get("business_type", ""), shop.get("category", ""))
            attr_lines = format_attribute_lines(normalize_attribute_data(p.get("attribute_data"), shop.get("category", ""), row_type, shop.get("business_type", "")), shop.get("category", ""), row_type, shop.get("business_type", ""))
            attr_part = f" | Details: {'; '.join(attr_lines[:3])}" if attr_lines else ""
            summary = " | ".join(offering_summary_bits(p, shop)) or "Details available on request"
            lines.append(f'- {p["name"]} | {summary}{img_part}{attr_part}')
    elif all_rows:
        lines.append(f"\nSample {nouns['plural']} from this business:")
        for row in all_rows[:8]:
            imgs = normalize_image_list(row.get("shop_id", ""), row.get("images", []))
            row_type = normalize_offering_type(row.get("offering_type", ""), shop.get("business_type", ""), shop.get("category", ""))
            img_part = f' | Photo: ![{row.get("name","Offering")}]({imgs[0]})' if imgs else ""
            attr_lines = format_attribute_lines(normalize_attribute_data(row.get("attribute_data"), shop.get("category", ""), row_type, shop.get("business_type", "")), shop.get("category", ""), row_type, shop.get("business_type", ""))
            attr_part = f" | Details: {'; '.join(attr_lines[:3])}" if attr_lines else ""
            summary = " | ".join(offering_summary_bits(row, shop)) or "Details available on request"
            lines.append(f'- {row.get("name","Offering")} | {summary}{img_part}{attr_part}')
    if rag_chunks:
        lines.append("\nKnowledge base notes:")
        for chunk in rag_chunks[:4]:
            lines.append(chunk.strip())
    return "\n".join(lines)

CHAT_GENERIC_BOLD_TERMS = {
    "price", "prices", "stock", "in stock", "out of stock", "low stock", "hours", "opening hours",
    "address", "phone", "whatsapp", "pickup", "delivery", "buy in shop", "open", "closed",
    "available", "not available", "today", "shop", "shops", "business", "businesses", "product", "products",
    "service", "services", "class", "classes", "event", "events", "offering", "offerings", "catalog"
}

def extract_markdown_bold_terms(text: str) -> List[str]:
    return [m.group(1).strip() for m in re.finditer(r"\*\*([^*\n]{1,120})\*\*", text or "")]

def find_out_of_scope_bold_terms_legacy(answer: str, shop: dict, prod_rows: List[Dict]) -> List[str]:
    allowed = {norm_text(shop.get("name", "")), norm_text(shop.get("shop_id", "")), norm_text(shop.get("shop_slug", ""))}
    allowed.update(norm_text(row.get("name", "")) for row in prod_rows if row.get("name"))
    flagged: List[str] = []
    seen = set()
    for term in extract_markdown_bold_terms(answer):
        normalized = norm_text(term)
        if not normalized or normalized in allowed or normalized in CHAT_GENERIC_BOLD_TERMS:
            continue
        if re.fullmatch(r"[\d\s.,:$€£cadusdinr-]+", normalized):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        flagged.append(term)
    return flagged[:4]

# Override with an ASCII-safe version so scope-guard checks do not rely on legacy encoded currency symbols.
def find_out_of_scope_bold_terms(answer: str, shop: dict, prod_rows: List[Dict]) -> List[str]:
    allowed = {norm_text(shop.get("name", "")), norm_text(shop.get("shop_id", "")), norm_text(shop.get("shop_slug", ""))}
    allowed.update(norm_text(row.get("name", "")) for row in prod_rows if row.get("name"))
    flagged: List[str] = []
    seen = set()
    for term in extract_markdown_bold_terms(answer):
        normalized = norm_text(term)
        if not normalized or normalized in allowed or normalized in CHAT_GENERIC_BOLD_TERMS:
            continue
        price_like = normalized.replace("cad", "").replace("usd", "").replace("inr", "")
        if re.fullmatch(r"[\d\s.,:$-]+", price_like):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        flagged.append(term)
    return flagged[:4]

SHOP_ASSISTANT_SYSTEM = """You are the live assistant for a local marketplace business.
Answer using ONLY the supplied business context.
Never mention another business, owner workspace, or any product, service, class, event, or portfolio item that is not explicitly present in the supplied context.
Never invent offerings, prices, availability, hours, or policies.
If a requested item or fact is not in the current business context, say you do not see it listed right now.
Be concise, warm, and professional.
Mention price and availability when available.
If the user asks what the business offers, summarize first and then list examples.
If information is missing, say so clearly and suggest a useful follow-up question.
Use simple markdown.
Prefer short sections or bullets over long paragraphs.
If you mention offerings, format them as clean bullets with name, price, and availability when available.
Sound natural and conversational, not robotic or overly salesy.
Do not say things like "based on the provided context" or "I found a likely match" unless necessary.
When answering simple customer questions, speak like a real local business assistant would.
Do not mention being an AI, model, chatbot, or system unless the user directly asks.
Prefer clear direct answers over meta explanations.
If the customer asks for a recommendation, suggest a few suitable offerings from the business and briefly say why.
If the customer asks a broad question, answer naturally as a helpful business representative would, but stay grounded in the supplied context.
If the customer asks something unrelated to the business, reply briefly and politely in a human tone without pretending the business has facts you do not have.
"""

def shop_voice_instructions(shop: dict) -> str:
    category = norm_text(shop.get("category", ""))
    if "food" in category or "bakery" in category or "cafe" in category or "restaurant" in category:
        return "Tone: friendly and appetite-oriented. Keep it warm, casual, and simple."
    if "electronics" in category:
        return "Tone: clear, helpful, and confident. Explain practical differences without sounding technical unless asked."
    if "clothing" in category or "fashion" in category:
        return "Tone: friendly and style-aware. Keep it relaxed and helpful."
    if "beauty" in category:
        return "Tone: warm, reassuring, and polished."
    if "books" in category:
        return "Tone: thoughtful, calm, and helpful."
    if "sports" in category:
        return "Tone: energetic, clear, and practical."
    return "Tone: warm, natural, and helpful."

def shop_persona_instructions(shop: dict) -> str:
    name = (shop.get("name") or "This shop").strip()
    category = norm_text(shop.get("category", ""))
    overview = (shop.get("overview") or "").strip()
    lines = [f"Shop identity: {name}."]
    if overview:
        lines.append(f"Brand notes: {overview[:220]}")
    if "food" in category or "bakery" in category or "cafe" in category or "restaurant" in category:
        lines.append("Voice style: welcoming, fresh, and easygoing. Make food suggestions sound appetizing without overdoing it.")
    elif "electronics" in category:
        lines.append("Voice style: practical, reliable, and clear. Help customers choose with simple explanations.")
    elif "clothing" in category or "fashion" in category:
        lines.append("Voice style: stylish, relaxed, and helpful. Guide customers without sounding pushy.")
    elif "beauty" in category:
        lines.append("Voice style: polished, reassuring, and friendly.")
    elif "books" in category:
        lines.append("Voice style: thoughtful, calm, and inviting.")
    elif "sports" in category:
        lines.append("Voice style: upbeat, practical, and motivating.")
    else:
        lines.append("Voice style: local, warm, and personable.")
    lines.append("Keep the personality subtle. Sound like a real shopkeeper, not a character.")
    return "\n".join(lines)

def response_style_instructions(q: str) -> str:
    qn = norm_text(q)
    if is_recommendation_query(qn) or any(t in qn for t in ["affordable", "budget", "gift", "best", "popular"]):
        return (
            "Response format: start with one short natural sentence, then list up to 4 offering bullets. "
            "Each bullet should include name, price, availability, and a short reason it fits."
        )
    if is_list_intent(qn):
        return (
            "Response format: start with one short summary sentence, then list offerings as bullets with name, "
            "price, and availability. Keep it tidy and easy to scan."
        )
    if is_price_lookup_query(qn) or is_budget_query(qn) or is_cheapest_query(qn):
        return "Response format: answer directly in 1 to 4 lines. Put the exact price or budget result first."
    if is_location_query(qn) or is_hours_query(qn) or is_contact_query(qn):
        return "Response format: answer directly and briefly in 1 to 3 lines."
    return "Response format: give a short direct answer first. Use bullets only if they clearly help."

def chat_max_tokens_for_query(q: str, picked: Optional[List[Dict[str, Any]]] = None) -> int:
    qn = norm_text(q)
    picked_count = len(picked or [])
    if is_list_intent(qn) or is_recommendation_query(qn):
        return max(OPENROUTER_MAX_TOKENS, 950)
    if any(token in qn for token in ["compare", "difference", "details", "options", "which one", "which is best", "help me choose"]):
        return max(OPENROUTER_MAX_TOKENS, 850)
    if picked_count >= 4:
        return max(OPENROUTER_MAX_TOKENS, 800)
    return OPENROUTER_MAX_TOKENS

def should_retry_truncated_chat(llm_res: Dict[str, Any]) -> bool:
    return bool((llm_res or {}).get("truncated")) and bool((llm_res or {}).get("content", "").strip())

def build_chat_suggestions(q: str, shop: dict, picked: List[Dict]) -> List[str]:
    suggestions: List[str] = []
    qn = norm_text(q)
    category = (shop.get("category") or "").strip()
    nouns = offering_nouns(shop, picked)
    plural = nouns["plural"]
    if picked:
        top = picked[0]
        suggestions.extend([
            f"What is the price of {top['name']}?",
            f"Show me photos of {top['name']}",
            f"Do you have more like {top['name']}?",
        ])
    else:
        suggestions.extend(default_catalog_prompts(shop, picked))
    if category:
        suggestions.append(f"What are your best {category.lower()} {plural}?")
        suggestions.append(f"Show me your most popular {category.lower()} {plural}")
        suggestions.extend(category_prompt_suggestions(category, shop.get("business_type", ""), picked))
    if "hour" not in qn and shop.get("hours"):
        suggestions.append("What are your opening hours?")
    if "address" not in qn and shop.get("address") and normalize_location_mode(shop.get("location_mode", ""), shop.get("business_type", ""), category) != "online":
        suggestions.append("Where is this business located?")
    return dedup(suggestions)[:4]

def default_catalog_prompts(shop: dict, rows: Optional[List[Dict[str, Any]]] = None) -> List[str]:
    nouns = offering_nouns(shop, rows or [])
    business_type = normalize_business_type(shop.get("business_type", ""), shop.get("category", ""))
    offering_type = normalize_offering_type("", business_type, shop.get("category", ""))
    plural = nouns["plural"]
    singular = nouns["singular"]
    if offering_type == "product":
        return [f"What {plural} do you have?", f"Show all {plural}", "What's in stock?", "What are your prices?"]
    if offering_type == "service":
        return [f"What {plural} do you offer?", f"Show all {plural}", "What is available right now?", f"Which {singular} do you recommend?"]
    if offering_type == "class":
        return [f"What {plural} do you offer?", f"Show all {plural}", "Which class is best for beginners?", "What are your prices?"]
    if offering_type == "event":
        return [f"What {plural} are coming up?", f"Show all {plural}", "What is available right now?", "What are your prices?"]
    return ["What do you offer?", "Show all offerings", "What is available right now?", "What are your prices?"]

def category_prompt_suggestions(category: str = "", business_type: str = "", rows: Optional[List[Dict[str, Any]]] = None) -> List[str]:
    category = str(category or "").strip()
    if category == "Food":
        return ["Which items are gluten free?", "What ingredients are in this product?"]
    if category == "Clothing":
        return ["What sizes do you have?", "What colors are available?"]
    if category == "Electronics":
        return ["What brand is this?", "What does this work with?"]
    if category == "Beauty":
        return ["What skin type is this for?", "What ingredients does it use?"]
    if category == "Books":
        return ["Who is the author?", "What format is this book?"]
    if category == "Home":
        return ["What material is this made from?", "What are the dimensions?"]
    if category == "Sports":
        return ["What size do you have?", "Is this for beginners or advanced users?"]
    offering_type = normalize_offering_type("", business_type, category)
    if offering_type == "service":
        return ["How long does this take?", "Do I need an appointment?"]
    if offering_type == "class":
        return ["What level is this for?", "What should I bring?"]
    if offering_type == "portfolio":
        return ["Do you take commissions?", "What is the turnaround time?"]
    return []

def attribute_analytics_for_products(products: List[Dict[str, Any]], category: str = "", business_type: str = "") -> Dict[str, Any]:
    inferred_type = ""
    if products:
        inferred_type = normalize_offering_type(products[0].get("offering_type", ""), business_type, category)
    schema = offering_attribute_schema(business_type, category, inferred_type)
    if not schema:
        return {"category": category, "fields": [], "complete_products": 0, "total_products": len(products)}
    fields = []
    complete_products = 0
    for row in products:
        row_type = normalize_offering_type(row.get("offering_type", ""), business_type, category)
        if normalize_attribute_data(row.get("attribute_data"), category, row_type, business_type):
            complete_products += 1
    for key, label in schema:
        value_counts: Dict[str, int] = {}
        filled_count = 0
        for row in products:
            row_type = normalize_offering_type(row.get("offering_type", ""), business_type, category)
            attr = normalize_attribute_data(row.get("attribute_data"), category, row_type, business_type)
            value = str(attr.get(key, "")).strip()
            if not value:
                continue
            filled_count += 1
            for part in [re.sub(r"\s+", " ", item).strip() for item in re.split(r"[,/|;]", value) if item.strip()]:
                value_counts[part] = value_counts.get(part, 0) + 1
        top_values = [{"label": k, "count": v} for k, v in sorted(value_counts.items(), key=lambda item: (-item[1], item[0].lower()))[:6]]
        fields.append({
            "key": key,
            "label": label,
            "filled_count": filled_count,
            "missing_count": max(0, len(products) - filled_count),
            "top_values": top_values,
        })
    return {
        "category": category,
        "fields": fields,
        "complete_products": complete_products,
        "total_products": len(products),
    }

def shop_label(shop: dict) -> str:
    return f"**{shop['name']}**"

def fallback_answer_v2(shop: dict, picked: List[Dict], q: str) -> str:
    if is_greeting(q):
        category = (shop.get("category") or "").strip().lower()
        nouns = offering_nouns(shop, picked)
        opener = "Hi"
        if "food" in category or "bakery" in category or "cafe" in category:
            opener = "Hi there"
        elif "electronics" in category:
            opener = "Hello"
        elif "clothing" in category or "fashion" in category:
            opener = "Hey"
        return f"{opener}! Welcome to {shop_label(shop)}. Ask me about {nouns['plural']}, prices, availability, hours, or just say **show all {nouns['plural']}**."
    if picked:
        top = picked[0]
        summary = " | ".join(offering_summary_bits(top, shop)) or "Details available on request"
        lines = [
            f"This looks like a good match from {shop_label(shop)}:",
            "",
            f"**{top['name']}** - {summary}",
        ]
        if top.get("overview"):
            lines.append(str(top["overview"])[:160])
        if top.get("images"):
            lines.append(f"![{top['name']}]({top['images'][0]})")
        return "\n".join(lines)
    nouns = offering_nouns(shop, picked)
    return f"I couldn't find a clear match at {shop_label(shop)}. Try **show all {nouns['plural']}**, **what is available right now**, or **do you have anything below 10 dollars**."

def fallback_answer(shop: dict, picked: List[Dict], q: str) -> str:
    nouns = offering_nouns(shop, picked)
    if is_greeting(q): return f"Hi! Welcome to **{shop['name']}**! Ask me about our {nouns['plural']}, or say '{default_catalog_question(shop, picked).lower()}'."
    if picked:
        top = picked[0]
        s = f"**{top['name']}** — {top.get('price','')} *({top.get('stock','in')})*"
        if top.get("images"): s += f"\n![{top['name']}]({top['images'][0]})"
        return s
    return f"I couldn't find a direct match. Try asking '{default_catalog_question(shop, picked).lower()}' to see everything available."

def answer_catalog_query(shop: dict, prod_rows: List[Dict]) -> Dict[str, Any]:
    nouns = offering_nouns(shop, prod_rows)
    plural = nouns["plural"]
    suggestions = build_chat_suggestions(default_catalog_question(shop, prod_rows), shop, prod_rows)
    if not prod_rows:
        return {
            "answer": f"{shop_label(shop)} has not added any {plural} yet.",
            "products": [],
            "meta": {"llm_used": False, "reason": "catalog_empty", "suggestions": suggestions},
        }

    top = prod_rows[:10]
    category = (shop.get("category") or "").strip()
    intro = f"Here is what you can browse at {shop_label(shop)}:"
    if category:
        intro = f"Here is a quick look at the {category.lower()} {plural} available at {shop_label(shop)}:"
    lines = [intro]
    for item in top:
        overview = (item.get("overview") or "").strip()
        detail = chat_item_line(enrich_chat_row(item, shop), shop)
        if overview:
            detail += f"\n  {overview[:120]}"
        lines.append(detail)
    if len(prod_rows) > len(top):
        lines.append(f"\nThere are **{len(prod_rows)}** {plural} in total. Ask if you want something specific.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top[:8]),
        "meta": {"llm_used": False, "reason": "catalog_list", "suggestions": suggestions},
    }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — System
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/ui", response_class=HTMLResponse)
def serve_ui():
    with open(os.path.join(SERVER_DIR, "ui.html"), encoding="utf-8") as f: return f.read()

@app.get("/business/{shop_ref}", response_class=HTMLResponse)
@app.get("/shop/{shop_ref}", response_class=HTMLResponse)
def serve_shop_ui(shop_ref: str):
    with open(os.path.join(SERVER_DIR, "ui.html"), encoding="utf-8") as f: return f.read()

@app.get("/offering/{shop_ref}/{product_ref}", response_class=HTMLResponse)
@app.get("/product/{shop_ref}/{product_ref}", response_class=HTMLResponse)
def serve_product_ui(shop_ref: str, product_ref: str):
    with open(os.path.join(SERVER_DIR, "ui.html"), encoding="utf-8") as f: return f.read()

@app.get("/favicon.ico")
def favicon():
    raise HTTPException(204)

@app.get("/vendor/leaflet/{asset_path:path}")
def serve_leaflet_vendor(asset_path: str):
    rel = str(asset_path or "").replace("\\", "/").lstrip("/")
    if not rel:
        raise HTTPException(404, "Asset not found")
    base_dir = os.path.realpath(os.path.join(VENDOR_DIR, "leaflet"))
    target = os.path.realpath(os.path.join(base_dir, rel))
    if not target.startswith(base_dir + os.sep) and target != base_dir:
        raise HTTPException(404, "Asset not found")
    if not os.path.isfile(target):
        raise HTTPException(404, "Asset not found")
    return FileResponse(target)

@app.get("/")
def root(): return {"status": "running", "version": "4.1 Supabase"}

@app.get("/health")
def health():
    return {
        "ok": True,
        "model": OPENROUTER_MODEL if OPENROUTER_KEY else None,
        "fallback_models": OPENROUTER_FALLBACK_MODELS,
        "temperature": OPENROUTER_TEMPERATURE if OPENROUTER_KEY else None,
        "rag_enabled": HAS_RAG,
        "supabase_configured": supabase is not None,
        "notifications_enabled": notifications_enabled(),
    }

@app.get("/shops/{shop_id}/images/{filename}")
def serve_shop_image(shop_id: str, filename: str):
    ext = norm_ext(os.path.splitext(filename)[1])
    if ext not in ALLOWED_IMG_EXTS:
        raise HTTPException(404, "File not found")
    safe_shop_id = slug(shop_id, 60)
    image_path = os.path.join(SHOPS_DIR, safe_shop_id, "images", os.path.basename(filename))
    if not os.path.isfile(image_path):
        raise HTTPException(404, "Image not found")
    return FileResponse(image_path)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Auth
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.post("/auth/register")
def register(body: RegisterReq, request: Request):
    enforce_rate_limit(request, "auth_register", limit=8, window_seconds=900, key_suffix=clean_email(body.email or "").lower())
    try:
        auth_client = require_supabase_auth()
        auth_client.auth.sign_up({
            "email": body.email.strip(),
            "password": body.password,
            "options": {
                "data": {"display_name": body.display_name.strip()},
                "email_redirect_to": ui_redirect_url(),
            },
        })
        return {"ok": True, "message": "Account created. Check your email to verify your address."}
    except Exception as e:
        raise HTTPException(400, str(e))

@app.post("/auth/login")
def login(body: LoginReq, request: Request, response: Response):
    enforce_rate_limit(request, "auth_login", limit=12, window_seconds=900, key_suffix=clean_email(body.email or "").lower())
    try:
        auth_client = require_supabase_auth()
        res = auth_client.auth.sign_in_with_password({"email": body.email, "password": body.password})
        user = res.user
        prof = load_profile(user.id)
        access_token = str(getattr(res.session, "access_token", "") or "").strip()
        refresh_token = str(getattr(res.session, "refresh_token", "") or "").strip()
        expires_in = int(getattr(res.session, "expires_in", AUTH_ACCESS_COOKIE_MAX_AGE) or AUTH_ACCESS_COOKIE_MAX_AGE)
        set_auth_cookies(response, access_token, refresh_token, expires_in)
        return auth_user_payload(user, prof)
    except Exception:
        raise HTTPException(401, "Invalid email or password")

@app.post("/auth/session")
def auth_session(body: BrowserSessionReq, request: Request, response: Response):
    enforce_rate_limit(request, "auth_session", limit=12, window_seconds=900)
    access_token = str(body.access_token or "").strip()
    if not access_token:
        raise HTTPException(400, "Missing access token")
    user, prof = get_user(f"Bearer {access_token}")
    set_auth_cookies(response, access_token, str(body.refresh_token or "").strip())
    return auth_user_payload(user, prof)

@app.post("/auth/refresh")
def auth_refresh(request: Request, response: Response):
    data = supabase_refresh_session(cookie_token(AUTH_REFRESH_COOKIE, request))
    access_token = str(data.get("access_token") or "").strip()
    refresh_token = str(data.get("refresh_token") or "").strip()
    expires_in = int(data.get("expires_in") or AUTH_ACCESS_COOKIE_MAX_AGE)
    set_auth_cookies(response, access_token, refresh_token, expires_in)
    user, prof = get_user(f"Bearer {access_token}")
    return auth_user_payload(user, prof)

@app.get("/auth/me")
def auth_me(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    return auth_user_payload(user, prof)

@app.post("/auth/logout")
def logout(response: Response, authorization: Optional[str] = Header(None)):
    try:
        token = bearer(authorization)
    except HTTPException:
        token = ""
    if token:
        try:
            requests.post(f"{SUPABASE_URL.rstrip('/')}/auth/v1/logout", headers=supabase_auth_headers(token), timeout=20)
        except Exception:
            pass
    clear_auth_cookies(response)
    return {"ok": True}

@app.post("/auth/resend-verification")
def resend_verification(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    if user.email_confirmed_at: return {"ok": True, "message": "Already verified"}
    try:
        supabase_auth_post("resend", {
            "type": "signup",
            "email": user.email,
            "options": {"email_redirect_to": ui_redirect_url()},
        })
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(400, "Could not resend the verification email right now.")
    return {"ok": True, "message": "Verification email sent"}

@app.post("/auth/forgot-password")
def forgot_password(body: ForgotPasswordReq, request: Request):
    enforce_rate_limit(request, "auth_forgot_password", limit=6, window_seconds=900, key_suffix=clean_email(body.email or "").lower())
    try:
        supabase_auth_post("recover", {
            "email": body.email.strip(),
            "redirect_to": ui_redirect_url(),
        })
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(400, "Could not send the reset link right now.")
    return {"ok": True, "message": "If that email exists, a reset link has been sent."}

@app.post("/auth/update-password")
def update_password(body: ResetPasswordReq, authorization: Optional[str] = Header(None)):
    token = bearer(authorization)
    password = (body.password or "").strip()
    if len(password) < 8:
        raise HTTPException(400, "Use at least 8 characters for the new password.")
    url = f"{SUPABASE_URL.rstrip('/')}/auth/v1/user"
    res = requests.put(url, headers=supabase_auth_headers(token), json={"password": password}, timeout=20)
    if res.status_code >= 400:
        try:
            detail = res.json().get("msg") or res.json().get("error_description") or res.json().get("message")
        except Exception:
            detail = res.text or "Could not update password."
        raise HTTPException(400, detail)
    return {"ok": True, "message": "Password updated successfully."}

@app.put("/auth/profile")
def update_profile(body: UpdateProfileReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    supabase.table("profiles").update({"display_name": body.display_name.strip()}).eq("id", user.id).execute()
    return {"ok": True}

@app.post("/auth/profile/avatar")
def upload_avatar(authorization: Optional[str] = Header(None), avatar: UploadFile = File(...)):
    user, prof = get_user(authorization)
    sb = require_supabase()
    if not avatar.filename: raise HTTPException(400, "No file provided")
    try:
        ext, data, content_type = read_validated_image(avatar, "avatar")
        filename = f"user_{user.id}_{uuid.uuid4().hex[:8]}{ext}"
        path = f"{AVATAR_IMAGE_PREFIX}/{filename}"
        url = upload_public_image(
            IMAGE_BUCKET,
            path,
            data,
            content_type,
            "Failed to upload avatar. Ensure the 'product-images' bucket is created and public in Supabase.",
        )
        sb.table("profiles").update({"avatar_url": url}).eq("id", user.id).execute()
        return {"ok": True, "avatar_url": url}
    except Exception as e:
        raise HTTPException(500, f"Avatar upload failed: {str(e)}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Customer Interactions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.post("/customer/favourite/{business_id}/{offering_id}")
@app.post("/customer/favourite/{shop_id}/{product_id}")
def toggle_favourite(shop_id: str = "", product_id: str = "", business_id: str = "", offering_id: str = "", authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    shop_id = business_id or shop_id
    product_id = offering_id or product_id
    favs = supabase.table("favourites").select("shop_id").eq("user_id", user.id).eq("shop_id", shop_id).eq("product_id", product_id).execute().data
    exists = len(favs) > 0
    if exists: supabase.table("favourites").delete().eq("user_id", user.id).eq("shop_id", shop_id).eq("product_id", product_id).execute()
    else: supabase.table("favourites").insert({"user_id": user.id, "shop_id": shop_id, "product_id": product_id}).execute()
    return {"ok": True, "saved": not exists, "business_id": shop_id, "offering_id": product_id}

@app.get("/customer/favourites")
def get_favourites(request: Request, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    favs = supabase.table("favourites").select("shop_id, product_id").eq("user_id", user.id).order("created_at", desc=True).execute().data
    product_rows = []
    for f in favs:
        p = supabase.table("products").select("*").eq("shop_id", f["shop_id"]).eq("product_id", f["product_id"]).execute().data
        if p: product_rows.append(p[0])
    out = serialize_products_bulk(product_rows, user.id)
    return {"ok": True, "favourites": out, "offerings": out}

@app.get("/public/reviews/{business_id}/{offering_id}")
@app.get("/public/reviews/{shop_id}/{product_id}")
def get_reviews(shop_id: str = "", product_id: str = "", business_id: str = "", offering_id: str = ""):
    shop_id = business_id or shop_id
    product_id = offering_id or product_id
    try:
        rows = supabase.table("reviews").select("*").eq("shop_id", shop_id).eq("product_id", product_id).order("created_at", desc=True).execute().data
        out = []
        for r in rows:
            author_name = "User"
            if r.get("user_id"):
                try:
                    prof = supabase.table("profiles").select("display_name").eq("id", r["user_id"]).execute().data
                    if prof and prof[0].get("display_name"):
                        author_name = prof[0]["display_name"]
                except: pass
            out.append({
                "id": r["id"], "rating": r["rating"], "body": r["body"], 
                "author": author_name, "created_at": r.get("created_at", "")
            })
        return {"ok": True, "reviews": out, "business_id": shop_id, "offering_id": product_id}
    except Exception as e:
        print(f"Reviews Error: {e}")
        return {"ok": True, "reviews": [], "business_id": shop_id, "offering_id": product_id}

@app.post("/public/track-view/{business_id}/{offering_id}")
@app.post("/public/track-view/{shop_id}/{product_id}")
def track_product_view(shop_id: str = "", product_id: str = "", business_id: str = "", offering_id: str = ""):
    shop_id = business_id or shop_id
    product_id = offering_id or product_id
    track(shop_id, "view", product_id)
    return {"ok": True, "business_id": shop_id, "offering_id": product_id}

@app.post("/customer/review/{business_id}/{offering_id}")
@app.post("/customer/review/{shop_id}/{product_id}")
def post_review(shop_id: str = "", product_id: str = "", business_id: str = "", offering_id: str = "", body: ReviewReq = ..., authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    shop_id = business_id or shop_id
    product_id = offering_id or product_id
    if not body.body.strip(): raise HTTPException(400, "Review cannot be empty")
    
    prod_check = supabase.table("products").select("product_id").eq("shop_id", shop_id).eq("product_id", product_id).execute().data
    if not prod_check: raise HTTPException(404, "Offering not found")
    
    existing = supabase.table("reviews").select("id").eq("shop_id", shop_id).eq("product_id", product_id).eq("user_id", user.id).execute().data
    if existing:
        supabase.table("reviews").update({"rating": body.rating, "body": body.body.strip(), "created_at": "now()"}).eq("id", existing[0]["id"]).execute()
    else:
        supabase.table("reviews").insert({"shop_id": shop_id, "product_id": product_id, "user_id": user.id, "rating": body.rating, "body": body.body.strip()}).execute()
    return {"ok": True, "message": "Review saved", "business_id": shop_id, "offering_id": product_id}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Public Browsing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/public/businesses")
@app.get("/public/shops")
def public_shops(
    category: Optional[str] = None,
    country_code: str = Query(""),
    region: str = Query(""),
    city: str = Query(""),
    postal_code: str = Query(""),
    open_day: str = Query(""),
    open_time: str = Query(""),
    open_now: bool = Query(False),
):
    q = supabase.table("shops").select("*").order("created_at", desc=True)
    if category: q = q.ilike("category", f"%{category}%")
    rows = q.execute().data
    filtered = []
    for r in rows:
        nr = ensure_shop_coordinates(r)
        r.update(nr)
        if country_code and r.get("country_code", "") != clean_code(country_code):
            continue
        if region and region.lower() not in (r.get("region", "") or "").lower():
            continue
        if city and city.lower() not in (r.get("city", "") or "").lower():
            continue
        if postal_code and postal_code.lower() not in (r.get("postal_code", "") or "").lower():
            continue
        if (open_day or open_time) and not shop_matches_schedule(r, open_day, open_time):
            continue
        if open_now and not shop_is_open_now(r):
            continue
        try:
            r["stats"] = shop_stats(r["shop_id"])
        except Exception as e:
            print(f"[Shop Stats Warning] {r.get('shop_id')}: {e}")
            r["stats"] = {"product_count": 0, "image_count": 0, "products_with_images": 0, "chat_hits_30d": 0, "shop_views_30d": 0, "product_views_30d": 0, "avg_rating": 0}
        r["is_open_now"] = shop_is_open_now(r)
        filtered.append(public_shop_payload(r))
    return alias_catalog_response({"ok": True, "shops": filtered})

@app.get("/public/location-support")
def public_location_support():
    countries = []
    for code, meta in sorted(COUNTRY_META.items(), key=lambda item: item[1]["name"]):
        countries.append({"code": code, "name": meta["name"], "currency_code": meta["currency"]})
    return {"ok": True, "countries": countries, "address_autocomplete": bool(MAPBOX_TOKEN)}

@app.get("/public/map-support")
def public_map_support():
    token = MAPBOX_TOKEN if MAPBOX_TOKEN.startswith("pk.") else ""
    return {"ok": True, "enabled": bool(token), "provider": "mapbox", "public_token": token}

@app.get("/public/timezone-support")
def public_timezone_support():
    return {"ok": True, "timezones": SUPPORTED_TIMEZONE_NAMES}

@app.get("/public/address/search")
def public_address_search(request: Request, q: str = Query(..., min_length=3), country: str = Query("")):
    enforce_rate_limit(request, "public_address_search", limit=30, window_seconds=300)
    if not MAPBOX_TOKEN:
        return {"ok": True, "suggestions": [], "provider": "disabled"}
    params = {
        "access_token": MAPBOX_TOKEN,
        "autocomplete": "true",
        "limit": 5,
        "types": "address",
    }
    country_code = clean_code(country)
    if country_code:
        params["country"] = country_code.lower()
    try:
        res = requests.get(
            f"https://api.mapbox.com/geocoding/v5/mapbox.places/{requests.utils.quote(q)}.json",
            params=params,
            timeout=8,
        )
        res.raise_for_status()
        data = res.json() or {}
        suggestions = []
        for feature in data.get("features", []):
            context = feature.get("context") or []
            country_name = ""
            region = ""
            city = ""
            postal_code = ""
            country_code_val = country_code
            for item in context:
                item_id = str(item.get("id", ""))
                if item_id.startswith("country."):
                    country_name = item.get("text", "")
                    country_code_val = clean_code(item.get("short_code", "") or country_code_val)
                elif item_id.startswith("region."):
                    region = item.get("text", "")
                elif item_id.startswith("place."):
                    city = item.get("text", "")
                elif item_id.startswith("postcode."):
                    postal_code = item.get("text", "")
            street_number = str(feature.get("address", "")).strip()
            street_name = str(feature.get("text", "")).strip()
            street_line1 = " ".join([v for v in [street_number, street_name] if v]).strip() or str(feature.get("place_name", "")).split(",")[0].strip()
            suggestions.append({
                "label": feature.get("place_name", ""),
                "formatted_address": feature.get("place_name", ""),
                "street_line1": street_line1,
                "street_line2": "",
                "city": city,
                "region": region,
                "postal_code": postal_code,
                "country_code": country_code_val,
                "country_name": country_name or country_meta(country_code_val).get("name", ""),
                "latitude": (feature.get("center") or [None, None])[1],
                "longitude": (feature.get("center") or [None, None])[0],
            })
        return {"ok": True, "suggestions": suggestions, "provider": "mapbox"}
    except Exception as e:
        print(f"[Address Search Warning] {e}")
        return {"ok": True, "suggestions": [], "provider": "error"}

@app.get("/public/business/{shop_ref}")
@app.get("/public/shop/{shop_ref}")
def public_shop(shop_ref: str, request: Request, sort: str = Query("default"), stock: str = Query(""), attr_key: str = Query(""), attr_value: str = Query(""), currency: str = Query(""), page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=100), authorization: Optional[str] = Header(None)):
    shop_row = ensure_shop_coordinates(resolve_shop_by_ref(shop_ref))
    shop_id = shop_row["shop_id"]
    
    user_id = None
    if authorization:
        try: user_id = get_user(authorization)[0].id
        except: pass
        
    q = supabase.table("products").select("*").eq("shop_id", shop_id)
    if stock in ("in", "low", "out"): q = q.eq("stock", stock)
    all_prods = q.execute().data
    if attr_key or attr_value:
        all_prods = [row for row in all_prods if matches_attribute_filter(row, shop_row.get("category", ""), attr_key, attr_value, shop_row.get("business_type", ""))]
    
    if sort == "price-asc": all_prods.sort(key=lambda x: get_row_price_amount(x) or 0)
    elif sort == "price-desc": all_prods.sort(key=lambda x: get_row_price_amount(x) or 0, reverse=True)
    else: all_prods.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    
    track(shop_id, "shop_view")
    
    paged = paginate_list(all_prods, page, limit)
    ser_prods = serialize_products_bulk(paged["items"], user_id, {shop_id: shop_row}, currency)
    
    return alias_catalog_response({
        "ok": True, "shop_id": shop_id, "shop_slug": shop_row.get("shop_slug", ""), "shop": public_shop_payload(shop_row),
        "products": ser_prods,
        "pagination": paged["pagination"],
        "stats": shop_stats(shop_id),
        "suggested_questions": dedup([*default_catalog_prompts(shop_row, all_prods), "Show all images", *category_prompt_suggestions(shop_row.get("category", ""), shop_row.get("business_type", ""), all_prods)])[:6]
    })

@app.get("/public/business-search")
@app.get("/public/search")
def search_shop(request: Request, shop_id: str = Query(""), business_id: str = Query(""), q: str = Query(...), attr_key: str = Query(""), attr_value: str = Query(""), currency: str = Query(""), page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=100)):
    qn = (q or "").strip()
    resolved_shop = resolve_shop_by_ref(business_id or shop_id)
    real_shop_id = resolved_shop["shop_id"]
    if not qn:
        return alias_catalog_response({"ok": True, "shop_id": real_shop_id, "shop_slug": resolved_shop.get("shop_slug", ""), "q": q, "results": [], "total": 0, "pagination": paginate_list([], 1, limit)["pagination"]})
    shop_map = {real_shop_id: resolved_shop}
    shop_row = shop_map.get(real_shop_id, {})
    rows = supabase.table("products").select("*").eq("shop_id", real_shop_id).order("updated_at", desc=True).execute().data
    rows = [row for row in rows if product_matches_query(row, qn, shop_row.get("category", ""), shop_row.get("business_type", ""))]
    if attr_key or attr_value:
        rows = [row for row in rows if matches_attribute_filter(row, shop_row.get("category", ""), attr_key, attr_value, shop_row.get("business_type", ""))]
    paged = paginate_list(rows, page, limit)
    return alias_catalog_response({"ok": True, "shop_id": real_shop_id, "shop_slug": resolved_shop.get("shop_slug", ""), "q": q, "results": serialize_products_bulk(paged["items"], None, shop_map, currency), "total": len(rows), "pagination": paged["pagination"]})

@app.get("/public/offering-search")
@app.get("/public/search/global")
def search_global(request: Request, q: str = Query(...), attr_key: str = Query(""), attr_value: str = Query(""), currency: str = Query(""), page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=60)):
    qn = (q or "").strip()
    if not qn:
        return alias_catalog_response({"ok": True, "q": q, "results": [], "total": 0, "pagination": paginate_list([], 1, limit)["pagination"]})
    rows = supabase.table("products").select("*, shops(name, shop_slug, category, business_type, location_mode, service_area, address, formatted_address, whatsapp, country_code, country_name, currency_code, region, city, postal_code, street_line1, street_line2)").order("updated_at", desc=True).execute().data
    rows = [row for row in rows if product_matches_query(row, qn, normalize_shop_record(row.get("shops", {}) or {}).get("category", ""), normalize_shop_record(row.get("shops", {}) or {}).get("business_type", ""))]
    if attr_key or attr_value:
        rows = [row for row in rows if matches_attribute_filter(row, normalize_shop_record(row.get("shops", {}) or {}).get("category", ""), attr_key, attr_value, normalize_shop_record(row.get("shops", {}) or {}).get("business_type", ""))]
    
    paged = paginate_list(rows, page, limit)
    shop_map = {}
    for row in paged["items"]:
        if row.get("shop_id"):
            shop_map[str(row.get("shop_id"))] = normalize_shop_record(row.get("shops", {}) or {})
    results = serialize_products_bulk(paged["items"], None, shop_map, currency)
    for r, prod in zip(paged["items"], results):
        prod["shop_name"] = r.get("shops", {}).get("name", "")
        prod["shop_address"] = normalize_shop_record(r.get("shops", {}) or {}).get("address", "")
        prod["shop_category"] = normalize_shop_record(r.get("shops", {}) or {}).get("category", "")
        prod["business_name"] = prod.get("shop_name", "")
        prod["business_address"] = prod.get("shop_address", "")
        prod["business_category"] = prod.get("shop_category", "")
    return alias_catalog_response({"ok": True, "q": q, "results": results, "total": len(rows), "pagination": paged["pagination"]})

@app.get("/public/top-offerings")
@app.get("/public/top-products")
def top_products(request: Request, page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=60), category: str = Query(""), attr_key: str = Query(""), attr_value: str = Query(""), currency: str = Query("")):
    q = supabase.table("products").select("*, shops!inner(name, shop_slug, category, business_type, location_mode, service_area, address, formatted_address, country_code, country_name, currency_code, region, city, postal_code, street_line1, street_line2)").neq("stock", "out")
    if category: q = q.ilike("shops.category", f"%{category}%")
    rows = q.execute().data
    if attr_key or attr_value:
        rows = [row for row in rows if matches_attribute_filter(row, normalize_shop_record(row.get("shops", {}) or {}).get("category", ""), attr_key, attr_value, normalize_shop_record(row.get("shops", {}) or {}).get("business_type", ""))]
    
    paged = paginate_list(rows, page, limit)
    shop_map = {}
    for row in paged["items"]:
        if row.get("shop_id"):
            shop_map[str(row.get("shop_id"))] = normalize_shop_record(row.get("shops", {}) or {})
    results = serialize_products_bulk(paged["items"], None, shop_map, currency)
    for r, prod in zip(paged["items"], results):
        prod["shop_name"] = r.get("shops", {}).get("name", "")
        prod["shop_category"] = normalize_shop_record(r.get("shops", {}) or {}).get("category", "")
        prod["business_name"] = prod.get("shop_name", "")
        prod["business_category"] = prod.get("shop_category", "")
    return alias_catalog_response({"ok": True, "products": results, "pagination": paged["pagination"]})

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Chat 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/chat")
def chat_endpoint(request: Request, shop_id: str = Query(""), business_id: str = Query(""), q: str = Query(...), currency: str = Query("")):
    shop_ref = business_id or shop_id
    enforce_rate_limit(request, "chat", limit=40, window_seconds=300, key_suffix=str(shop_ref or "").strip().lower())
    q = (q or "").strip()
    if not q: raise HTTPException(400, "Missing q")
    shop = resolve_shop_by_ref(shop_ref)
    shop_id = shop["shop_id"]
    def respond(payload: Dict[str, Any]) -> Dict[str, Any]:
        return alias_catalog_response({
            **(payload or {}),
            "business_id": shop_id,
            "business_slug": shop.get("shop_slug", ""),
        })
    track(shop_id, "chat")
    
    raw_prod_rows = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute().data
    prod_rows = []
    for row in raw_prod_rows:
        prod_rows.append({**row, **get_display_price_fields(row, shop, currency)})

    info_answer = answer_shop_info_query(shop, q)
    if info_answer is not None:
        return respond(info_answer)

    budget_answer = answer_budget_query(shop, prod_rows, q)
    if budget_answer is not None:
        return respond(budget_answer)

    stock_answer = answer_stock_query(shop, prod_rows, q)
    if stock_answer is not None:
        return respond(stock_answer)

    cheapest_answer = answer_cheapest_query(shop, prod_rows, q)
    if cheapest_answer is not None:
        return respond(cheapest_answer)

    product_image_answer = answer_product_image_query(shop, prod_rows, q)
    if product_image_answer is not None:
        return respond(product_image_answer)

    attribute_answer = answer_attribute_query(shop, prod_rows, q)
    if attribute_answer is not None:
        return respond(attribute_answer)

    picked = rank_products(prod_rows, q, shop.get("category", ""), shop.get("business_type", ""))
    abs_picked = serialize_products_bulk(picked[:4], None, {shop_id: shop}, currency)
    rag = {"chunks": [], "matches": []}
    if HAS_RAG and not is_greeting(q):
        try:
            rag = _retrieve(shop_id, q, top_k=4) or {"chunks": [], "matches": []}
        except Exception as rag_err:
            print(f"[Chat RAG Warning] {rag_err}")
    suggestions = build_chat_suggestions(q, shop, picked)

    # 1. Shortcut: Show Images
    if wants_all_images(q):
        gallery = []
        for r in prod_rows:
            gallery.extend(normalize_image_list(shop_id, r.get("images", [])))
        if gallery:
            ans = f"Here are all photos from **{shop['name']}**:\n" + "\n".join(f"![Image]({url})" for url in gallery[:40])
        else:
            ans = "This business hasn't uploaded any listing photos yet."
        return respond({"answer": ans, "products": abs_picked, "meta": {"llm_used": False, "suggestions": suggestions}})

    # 2. Shortcut: Greeting
    if is_greeting(q):
        nouns = offering_nouns(shop, prod_rows)
        return respond({"answer": f"Hi! Welcome to **{shop['name']}**! Ask me about {nouns['plural']}, prices, availability, opening hours, or say '{default_catalog_question(shop, prod_rows).lower()}'.", "products": abs_picked, "meta": {"llm_used": False, "suggestions": suggestions}})

    # 3. Handle Full Catalog Requests safely
    if is_list_intent(q):
        return respond(answer_catalog_query(shop, prod_rows))

    try:
        system_prompt = (
            SHOP_ASSISTANT_SYSTEM
            + "\n"
            + f"Current live business: {shop.get('name','Business')} (business_id: {shop.get('shop_id','')}, business_slug: {shop.get('shop_slug','')})."
            + "\n"
            + shop_voice_instructions(shop)
            + "\n"
            + shop_persona_instructions(shop)
            + "\n"
            + response_style_instructions(q)
        )
        context_blob = f"CONTEXT:\n{build_context(shop, picked, prod_rows, rag.get('chunks', []))}\n\nCUSTOMER: {q}"
        llm_budget = chat_max_tokens_for_query(q, picked)
        llm_res = llm_chat(system_prompt, context_blob, max_tokens=llm_budget)
        if should_retry_truncated_chat(llm_res):
            retry_prompt = system_prompt + "\nThe previous draft was cut off. Retry with a complete answer. Keep it concise, but finish every sentence and list cleanly."
            llm_res = llm_chat(retry_prompt, context_blob, max_tokens=max(llm_budget + 300, 1100))
        flagged_terms = find_out_of_scope_bold_terms(llm_res["content"], shop, prod_rows)
        if flagged_terms:
            print(f"[Chat Scope Guard] shop={shop_id} blocked out-of-scope terms: {flagged_terms}")
            return respond({
                "answer": fallback_answer_v2(shop, picked, q),
                "products": abs_picked,
                "meta": {
                    "llm_used": False,
                    "reason": "scope_guard",
                    "suggestions": suggestions,
                    "rag_matches": len(rag.get('matches', [])),
                }
            })
        attached = choose_chat_products(prod_rows, q, llm_res["content"], prefer_images=wants_product_image(q), limit=4, category=shop.get("category", ""), business_type=shop.get("business_type", ""))
        if attached:
            abs_picked = serialize_products_bulk(attached, None, {shop_id: shop}, currency)
        elif wants_product_image(q):
            with_images = [p for p in picked if p.get("images")]
            if with_images:
                abs_picked = serialize_products_bulk(with_images[:4], None, {shop_id: shop}, currency)
        return respond({"answer": llm_res["content"], "products": abs_picked, "meta": {"llm_used": True, "model": llm_res.get("model") or OPENROUTER_MODEL, "suggestions": suggestions, "rag_matches": len(rag.get('matches', [])), "finish_reason": llm_res.get("finish_reason") or "stop"}})
    except Exception as e:
        err_msg = str(e)
        if hasattr(e, "response") and getattr(e, "response") is not None:
            err_msg += f" | {e.response.text}"
            
        print(f"[Chat LLM Exception] Fallback triggered: {err_msg}")
        
        ans = fallback_answer_v2(shop, picked, q)

        return respond({
            "answer": ans,
            "products": abs_picked,
            "meta": {"llm_used": False, "reason": "fallback_after_llm_error", "suggestions": suggestions, "rag_matches": len(rag.get('matches', []))}
        })

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Admin 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_shop_owner(user_id: str, shop_id: str):
    res = supabase.table("shops").select("shop_id").eq("shop_id", shop_id).eq("owner_user_id", user_id).execute()
    if not res.data: raise HTTPException(404, "Not found or not yours")

@app.post("/admin/create-business")
@app.post("/admin/create-shop")
def create_shop(body: CreateShopReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    raw_business = body.business or body.shop
    if raw_business is None:
        raise HTTPException(400, "Business payload is required")
    shop = validate_shop_payload(raw_business)
    sid = gen_shop_id(shop["name"])
    shop_slug = unique_shop_slug(shop["name"])
    for _ in range(5):
        exists_res = supabase.table("shops").select("shop_id").eq("shop_id", sid).execute()
        if not exists_res.data:
            break
        sid = gen_shop_id(shop["name"])
    else:
        raise HTTPException(500, "Could not generate a unique shop ID")

    payload = {
        "shop_slug": shop_slug,
        "owner_user_id": user.id,
        "name": shop["name"].strip(),
        "profile_image_url": shop.get("profile_image_url", ""),
        "address": shop["address"],
        "formatted_address": shop["formatted_address"],
        "business_type": shop["business_type"],
        "location_mode": shop["location_mode"],
        "service_area": shop.get("service_area", ""),
        "country_code": shop["country_code"],
        "country_name": shop["country_name"],
        "timezone_name": shop["timezone_name"],
        "region": shop["region"],
        "city": shop["city"],
        "postal_code": shop["postal_code"],
        "street_line1": shop["street_line1"],
        "street_line2": shop["street_line2"],
        "currency_code": shop["currency_code"],
        "latitude": shop.get("latitude"),
        "longitude": shop.get("longitude"),
        "overview": shop["overview"],
        "phone": shop["phone"],
        "hours": shop["hours"],
        "hours_structured": shop["hours_structured"],
        "category": shop["category"],
        "whatsapp": shop["whatsapp"],
        "supports_pickup": shop["supports_pickup"],
        "supports_delivery": shop["supports_delivery"],
        "supports_walk_in": shop["supports_walk_in"],
        "delivery_radius_km": shop.get("delivery_radius_km"),
        "delivery_fee": shop.get("delivery_fee"),
        "pickup_notes": shop.get("pickup_notes", ""),
    }
    strict_cols = []
    if shop.get("business_type") != "retail":
        strict_cols.append("business_type")
    if shop.get("location_mode") != "storefront":
        strict_cols.append("location_mode")
    if shop.get("service_area"):
        strict_cols.append("service_area")
    payload, unsupported_cols = shop_write_payload_with_fallback(sid, payload, False, strict_cols)
    if unsupported_cols:
        print(f"[Shop Schema Warning] {sid}: saved without optional columns {unsupported_cols}")
    supabase.table("profiles").update({"role": "shopkeeper"}).eq("id", user.id).execute()
    
    rebuild_kb(sid)
    return {"ok": True, "shop_id": sid, "business_id": sid, "shop_slug": shop_slug, "business_slug": shop_slug}

@app.get("/admin/my-businesses")
@app.get("/admin/my-shops")
def my_shops(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    shops_res = supabase.table("shops").select("*").eq("owner_user_id", user.id).order("created_at", desc=True).execute()
    rows = shops_res.data
    
    for r in rows:
        r.update(normalize_shop_record(r))
        stats = shop_stats(r["shop_id"])
        r["stats"] = stats
        r["quality_flags"] = shop_completeness_flags(r, stats)
    return alias_catalog_response({"ok": True, "shops": rows})

@app.post("/admin/businesses/geocode-missing")
@app.post("/admin/shops/geocode-missing")
def admin_geocode_missing_shop_coords(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    shops_res = supabase.table("shops").select("*").eq("owner_user_id", user.id).execute()
    updated = 0
    checked = 0
    for row in shops_res.data or []:
        checked += 1
        current = normalize_shop_record(row)
        if not shop_has_mappable_address(current):
            continue
        try:
            lat = float(current.get("latitude"))
            lng = float(current.get("longitude"))
            if math.isfinite(lat) and math.isfinite(lng):
                continue
        except Exception:
            pass
        geo = geocode_structured_address(current)
        if geo.get("latitude") is None or geo.get("longitude") is None:
            continue
        try:
            supabase.table("shops").update({
                "latitude": geo["latitude"],
                "longitude": geo["longitude"],
            }).eq("shop_id", current["shop_id"]).execute()
            updated += 1
        except Exception as e:
            print(f"[Admin Geocode Warning] {current.get('shop_id')}: {e}")
    return {"ok": True, "checked": checked, "updated": updated}

@app.get("/admin/export/businesses.csv")
@app.get("/admin/export/shops.csv")
def admin_export_shops_csv(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    shops_res = supabase.table("shops").select("*").eq("owner_user_id", user.id).order("created_at", desc=True).execute()
    rows = [normalize_shop_record(r) for r in (shops_res.data or [])]
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "shop_id", "name", "business_type", "location_mode", "service_area", "category", "country_code", "country_name", "region", "city", "postal_code",
        "formatted_address", "profile_image_url", "timezone_name", "currency_code", "hours", "phone", "whatsapp",
        "supports_pickup", "supports_delivery", "supports_walk_in", "delivery_radius_km", "delivery_fee", "pickup_notes", "overview"
    ])
    for row in rows:
        writer.writerow([
            row.get("shop_id", ""), row.get("name", ""), row.get("business_type", ""), row.get("location_mode", ""), row.get("service_area", ""), row.get("category", ""), row.get("country_code", ""),
            row.get("country_name", ""), row.get("region", ""), row.get("city", ""), row.get("postal_code", ""),
            row.get("formatted_address", "") or row.get("address", ""), row.get("profile_image_url", ""), row.get("timezone_name", ""),
            row.get("currency_code", ""), row.get("hours", ""), row.get("phone", ""), row.get("whatsapp", ""),
            row.get("supports_pickup", True), row.get("supports_delivery", False), row.get("supports_walk_in", True),
            row.get("delivery_radius_km", ""), row.get("delivery_fee", ""), row.get("pickup_notes", ""), row.get("overview", ""),
        ])
    return PlainTextResponse(
        content=buffer.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="businesses_export.csv"'},
    )

@app.get("/admin/export/offerings.csv")
@app.get("/admin/export/products.csv")
def admin_export_products_csv(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    shop_ids = [row.get("shop_id") for row in (supabase.table("shops").select("shop_id").eq("owner_user_id", user.id).execute().data or []) if row.get("shop_id")]
    rows = supabase.table("products").select("*").in_("shop_id", shop_ids).order("updated_at", desc=True).execute().data if shop_ids else []
    buffer = StringIO()
    writer = csv.writer(buffer)
    shop_meta = {
        row.get("shop_id"): normalize_shop_record(row)
        for row in (supabase.table("shops").select("*").in_("shop_id", shop_ids).execute().data or [])
    } if shop_ids else {}
    writer.writerow(["shop_id", "product_id", "offering_type", "price_mode", "availability_mode", "name", "price_amount", "currency_code", "price", "stock", "stock_quantity", "duration_minutes", "capacity", "variants", "variant_data_json", "variant_matrix_json", "attribute_data_json", "overview"])
    for row in rows or []:
        shop_row = shop_meta.get(row.get("shop_id"), {})
        business_type = shop_row.get("business_type", "")
        category = shop_row.get("category", "")
        offering_type = normalize_offering_type(row.get("offering_type", ""), business_type, category)
        writer.writerow([
            row.get("shop_id", ""), row.get("product_id", ""), offering_type, normalize_price_mode(row.get("price_mode", "")), normalize_availability_mode(row.get("availability_mode", ""), offering_type, business_type, category), row.get("name", ""), row.get("price_amount", ""),
            row.get("currency_code", ""), row.get("price", ""), row.get("stock", ""), row.get("stock_quantity", ""), row.get("duration_minutes", ""), row.get("capacity", ""), row.get("variants", ""),
            json.dumps(normalize_variant_data(row.get("variant_data") or row.get("variants", ""), row.get("shop_id", "")), ensure_ascii=False),
            json.dumps(normalize_variant_matrix(row.get("variant_matrix", []), normalize_variant_data(row.get("variant_data") or row.get("variants", ""), row.get("shop_id", "")), row.get("shop_id", "")), ensure_ascii=False),
            json.dumps(normalize_attribute_data(row.get("attribute_data"), category, offering_type, business_type), ensure_ascii=False),
            row.get("overview", ""),
        ])
    return PlainTextResponse(
        content=buffer.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="offerings_export.csv"'},
    )

@app.get("/admin/business/{shop_id}")
@app.get("/admin/shop/{shop_id}")
def admin_get_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    
    shop = normalize_shop_record(supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data[0])
    prods = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute().data
    stats = shop_stats(shop_id)
    
    ser_prods = serialize_products_bulk(prods, None, {shop_id: shop})
    return {
        "ok": True,
        "shop_id": shop_id,
        "business_id": shop_id,
        "data": {
            "shop": shop,
            "business": shop,
            "products": ser_prods,
            "offerings": ser_prods,
            "stats": stats,
        },
    }

@app.get("/admin/business/{shop_id}/requests")
@app.get("/admin/shop/{shop_id}/requests")
def admin_shop_requests(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    try:
        rows = supabase.table("order_requests").select("*").eq("shop_id", shop_id).order("created_at", desc=True).limit(100).execute().data or []
    except Exception:
        rows = []
    return {"ok": True, "requests": rows, "business_id": shop_id}

@app.put("/admin/business/{shop_id}/request/{request_id}/status")
@app.put("/admin/shop/{shop_id}/request/{request_id}/status")
def admin_update_request_status(shop_id: str, request_id: str, body: OrderStatusUpdateReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    status = str(body.status or "").strip().lower()
    allowed = {"new", "accepted", "ready", "completed", "cancelled"}
    if status not in allowed:
        raise HTTPException(400, "Status must be new, accepted, ready, completed, or cancelled")
    rows = supabase.table("order_requests").select("*").eq("shop_id", shop_id).eq("request_id", request_id).limit(1).execute().data or []
    if not rows:
        raise HTTPException(404, "Request not found")
    row = rows[0]
    shop_rows = supabase.table("shops").select("*").eq("shop_id", shop_id).limit(1).execute().data or []
    shop = normalize_shop_record(shop_rows[0]) if shop_rows else {"name": "the business"}
    history = row.get("status_history") or []
    if isinstance(history, str):
        try:
            history = json.loads(history)
        except Exception:
            history = []
    if not isinstance(history, list):
        history = []
    history.append({"status": status, "at": datetime.now(timezone.utc).isoformat()})
    supabase.table("order_requests").update({
        "status": status,
        "status_history": history,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).eq("shop_id", shop_id).eq("request_id", request_id).execute()
    updated_row = {**row, "status": status, "status_history": history}
    send_request_status_email(shop, updated_row, status)
    return {"ok": True, "status": status, "business_id": shop_id}

@app.put("/admin/business/{shop_id}")
@app.put("/admin/shop/{shop_id}")
def admin_update_shop(shop_id: str, body: ShopInfo, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    shop = validate_shop_payload(body)
    shop_slug = unique_shop_slug(shop["name"], shop_id)
    payload = {
        "name": shop["name"].strip(),
        "shop_slug": shop_slug,
        "profile_image_url": shop.get("profile_image_url", ""),
        "address": shop["address"],
        "formatted_address": shop["formatted_address"],
        "business_type": shop["business_type"],
        "location_mode": shop["location_mode"],
        "service_area": shop.get("service_area", ""),
        "country_code": shop["country_code"],
        "country_name": shop["country_name"],
        "timezone_name": shop["timezone_name"],
        "region": shop["region"],
        "city": shop["city"],
        "postal_code": shop["postal_code"],
        "street_line1": shop["street_line1"],
        "street_line2": shop["street_line2"],
        "currency_code": shop["currency_code"],
        "latitude": shop.get("latitude"),
        "longitude": shop.get("longitude"),
        "overview": shop["overview"],
        "phone": shop["phone"],
        "hours": shop["hours"],
        "hours_structured": shop["hours_structured"],
        "category": shop["category"],
        "whatsapp": shop["whatsapp"],
        "supports_pickup": shop["supports_pickup"],
        "supports_delivery": shop["supports_delivery"],
        "supports_walk_in": shop["supports_walk_in"],
        "delivery_radius_km": shop.get("delivery_radius_km"),
        "delivery_fee": shop.get("delivery_fee"),
        "pickup_notes": shop.get("pickup_notes", ""),
    }
    strict_cols = []
    if shop.get("business_type") != "retail":
        strict_cols.append("business_type")
    if shop.get("location_mode") != "storefront":
        strict_cols.append("location_mode")
    if shop.get("service_area"):
        strict_cols.append("service_area")
    payload, unsupported_cols = shop_write_payload_with_fallback(shop_id, payload, True, strict_cols)
    if unsupported_cols:
        print(f"[Shop Schema Warning] {shop_id}: saved without optional columns {unsupported_cols}")
    rebuild_kb(shop_id)
    return {"ok": True, "business_id": shop_id}

@app.post("/admin/business/{shop_id}/profile-image")
@app.post("/admin/shop/{shop_id}/profile-image")
def admin_upload_shop_profile_image(shop_id: str, authorization: Optional[str] = Header(None), profile_image: UploadFile = File(...)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    rows = supabase.table("shops").select("profile_image_url").eq("shop_id", shop_id).limit(1).execute().data or []
    if not rows:
        raise HTTPException(404, "Business not found")
    current_url = normalize_shop_record(rows[0]).get("profile_image_url", "")
    new_url = save_business_profile_image(shop_id, profile_image)
    try:
        payload, unsupported_cols = shop_write_payload_with_fallback(
            shop_id,
            {"profile_image_url": new_url},
            True,
            ["profile_image_url"],
        )
    except Exception:
        remove_public_image(IMAGE_BUCKET, new_url)
        raise
    if unsupported_cols:
        print(f"[Shop Schema Warning] {shop_id}: saved without optional columns {unsupported_cols}")
    if current_url and current_url != new_url:
        remove_public_image(IMAGE_BUCKET, current_url)
    rebuild_kb(shop_id)
    return {
        "ok": True,
        "business_id": shop_id,
        "profile_image_url": payload.get("profile_image_url", new_url),
        "business_profile_image_url": payload.get("profile_image_url", new_url),
    }

@app.delete("/admin/business/{shop_id}/profile-image")
@app.delete("/admin/shop/{shop_id}/profile-image")
def admin_delete_shop_profile_image(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    rows = supabase.table("shops").select("profile_image_url").eq("shop_id", shop_id).limit(1).execute().data or []
    if not rows:
        raise HTTPException(404, "Business not found")
    current_url = normalize_shop_record(rows[0]).get("profile_image_url", "")
    payload, unsupported_cols = shop_write_payload_with_fallback(
        shop_id,
        {"profile_image_url": ""},
        True,
        ["profile_image_url"],
    )
    if unsupported_cols:
        print(f"[Shop Schema Warning] {shop_id}: saved without optional columns {unsupported_cols}")
    if current_url:
        remove_public_image(IMAGE_BUCKET, current_url)
    rebuild_kb(shop_id)
    return {"ok": True, "business_id": shop_id, "profile_image_url": "", "business_profile_image_url": ""}

@app.post("/public/fulfillment-request")
@app.post("/public/order-request")
def public_order_request(body: OrderRequestReq, request: Request):
    enforce_rate_limit(request, "public_order_request", limit=12, window_seconds=600)
    raw_shop_id = str(body.business_id or body.shop_id or "").strip()
    if not raw_shop_id:
        raise HTTPException(400, "Business is required")
    shop_id = slug(raw_shop_id, 80)
    shop_rows = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data
    if not shop_rows:
        raise HTTPException(404, "Business not found")
    shop = normalize_shop_record(shop_rows[0])
    fulfillment_type = str(body.fulfillment_type or "").strip().lower()
    if fulfillment_type not in {"pickup", "delivery", "walk_in"}:
        raise HTTPException(400, "Select pickup, delivery, or walk_in")
    if fulfillment_type == "pickup" and not shop.get("supports_pickup"):
        raise HTTPException(400, "This business does not accept pickup requests")
    if fulfillment_type == "delivery" and not shop.get("supports_delivery"):
        raise HTTPException(400, "This business does not offer local delivery")
    if fulfillment_type == "walk_in" and not shop.get("supports_walk_in"):
        raise HTTPException(400, "This business does not accept walk-in orders through the app")
    customer_name = re.sub(r"\s+", " ", str(body.customer_name or "")).strip()
    phone = re.sub(r"\s+", " ", str(body.phone or "")).strip()
    customer_email = clean_email(body.customer_email or "")
    if len(customer_name) < 2:
        raise HTTPException(400, "Enter your name")
    if len(phone) < 6:
        raise HTTPException(400, "Enter a valid phone number")
    if customer_email and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", customer_email):
        raise HTTPException(400, "Enter a valid email address")
    if fulfillment_type == "delivery" and not str(body.delivery_address or "").strip():
        raise HTTPException(400, "Enter the delivery address")
    items = body.items or []
    if not items:
        raise HTTPException(400, "Cart is empty")
    if len({str(item.get("business_id") or item.get("shop_id") or shop_id) for item in items}) > 1:
        raise HTTPException(400, "Use one business per request")
    product_ids = list({str(item.get("offering_id") or item.get("product_id") or "").strip() for item in items if isinstance(item, dict) and str(item.get("offering_id") or item.get("product_id") or "").strip()})
    product_rows = supabase.table("products").select("*").eq("shop_id", shop_id).in_("product_id", product_ids).execute().data if product_ids else []
    product_map = {str(row.get("product_id")): row for row in (product_rows or [])}
    clean_items: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        qty = int(item.get("qty") or 1)
        if qty < 1:
            continue
        product_id = str(item.get("offering_id") or item.get("product_id") or "").strip()
        product_row = product_map.get(product_id)
        if not product_row:
            raise HTTPException(400, "One or more cart items are no longer available.")
        clean_items.append(resolve_order_item(product_row, shop, item, qty))
    if not clean_items:
        raise HTTPException(400, "Cart is empty")
    total_amount = 0.0
    known_amounts = True
    for item in clean_items:
        try:
            total_amount += float(item.get("price_amount")) * int(item.get("qty") or 1)
        except Exception:
            known_amounts = False
            break
    request_id = f"req_{uuid.uuid4().hex[:10]}"
    payload = {
        "request_id": request_id,
        "shop_id": shop_id,
        "fulfillment_type": fulfillment_type,
        "customer_name": customer_name,
        "phone": phone,
        "customer_email": customer_email,
        "note": str(body.note or "").strip(),
        "preferred_time": str(body.preferred_time or "").strip(),
        "delivery_address": str(body.delivery_address or "").strip(),
        "items": clean_items,
        "total_amount": round(total_amount, 2) if known_amounts else None,
        "currency_code": clean_items[0].get("currency_code") or shop.get("currency_code") or "",
        "status": "new",
        "status_history": [{"status": "new", "at": datetime.now(timezone.utc).isoformat()}],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    supabase.table("order_requests").insert(payload).execute()
    send_request_confirmation_email(shop, payload)
    return {"ok": True, "request_id": request_id, "track_token": issue_request_track_token(request_id, phone), "business_id": shop_id}

@app.get("/public/fulfillment-request-status")
@app.get("/public/order-request-status")
def public_order_request_status(request: Request, request_id: str = Query(...), phone: str = Query(""), track_token: str = Query("")):
    enforce_rate_limit(request, "public_order_request_status", limit=20, window_seconds=300)
    rid = str(request_id or "").strip()
    phone_clean = re.sub(r"\D+", "", str(phone or ""))
    token_clean = str(track_token or "").strip()
    if not rid or (not phone_clean and not token_clean):
        raise HTTPException(400, "Request ID and a secure tracking link or phone number are required")
    rows = supabase.table("order_requests").select("*").eq("request_id", rid).limit(1).execute().data or []
    if not rows:
        raise HTTPException(404, "Request not found")
    row = rows[0]
    row_phone = re.sub(r"\D+", "", str(row.get("phone") or ""))
    token_ok = verify_request_track_token(token_clean, rid, row_phone) if token_clean else False
    phone_ok = bool(phone_clean and row_phone == phone_clean)
    if not token_ok and not phone_ok:
        raise HTTPException(404, "Request not found")
    return {
        "ok": True,
        "track_token": issue_request_track_token(rid, row_phone),
        "request": {
            "request_id": row.get("request_id"),
            "shop_id": row.get("shop_id"),
            "business_id": row.get("shop_id"),
            "fulfillment_type": row.get("fulfillment_type"),
            "status": row.get("status", "new"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "items": [
                {**item, "business_id": item.get("business_id") or item.get("shop_id") or row.get("shop_id"), "offering_id": item.get("offering_id") or item.get("product_id", "")}
                for item in (row.get("items") or [])
                if isinstance(item, dict)
            ],
            "status_history": row.get("status_history") or [],
        }
    }

@app.delete("/admin/business/{shop_id}")
@app.delete("/admin/shop/{shop_id}")
def admin_delete_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    image_names: List[str] = []
    profile_image_url = ""
    try:
        shop_rows = supabase.table("shops").select("profile_image_url").eq("shop_id", shop_id).limit(1).execute().data or []
        profile_image_url = normalize_shop_record(shop_rows[0]).get("profile_image_url", "") if shop_rows else ""
    except Exception as e:
        print(f"[Business Delete Warning] could not load business profile image for {shop_id}: {e}")
    try:
        product_rows = supabase.table("products").select("images").eq("shop_id", shop_id).execute().data or []
        for row in product_rows:
            for image_url in normalize_image_list(shop_id, row.get("images", [])):
                img_name = os.path.basename(str(image_url or "").rstrip("/"))
                if img_name and img_name not in image_names:
                    image_names.append(img_name)
    except Exception as e:
        print(f"[Business Delete Warning] could not enumerate product images for {shop_id}: {e}")
    for table_name in ("favourites", "reviews", "analytics", "order_requests", "products"):
        try:
            supabase.table(table_name).delete().eq("shop_id", shop_id).execute()
        except Exception as e:
            print(f"[Business Delete Warning] could not delete {table_name} rows for {shop_id}: {e}")
    if image_names:
        try:
            supabase.storage.from_(IMAGE_BUCKET).remove([f"{shop_id}/{img_name}" for img_name in image_names])
        except Exception as e:
            print(f"[Business Delete Warning] could not remove storage images for {shop_id}: {e}")
    if profile_image_url:
        remove_public_image(IMAGE_BUCKET, profile_image_url)
    supabase.table("shops").delete().eq("shop_id", shop_id).execute()
    shutil.rmtree(os.path.join(SHOPS_DIR, shop_id), ignore_errors=True)
    return {"ok": True, "business_id": shop_id}

@app.post("/admin/business/{shop_id}/offering")
@app.post("/admin/shop/{shop_id}/product")
def admin_upsert_product(shop_id: str, product: Product, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    raw_pid = str(product.offering_id or product.product_id or "").strip()
    pid = slug(raw_pid, 60) if raw_pid else gen_product_id(product.name)
    shop_rows = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data
    if not shop_rows: raise HTTPException(404, "Business not found")
    shop = normalize_shop_record(shop_rows[0])
    
    existing = supabase.table("products").select("images").eq("shop_id", shop_id).eq("product_id", pid).execute().data
    if not existing and not raw_pid:
        for _ in range(5):
            dupe = supabase.table("products").select("product_id").eq("shop_id", shop_id).eq("product_id", pid).execute().data
            if not dupe:
                break
            pid = gen_product_id(product.name)
        else:
            raise HTTPException(500, "Could not generate a unique product ID")
    data = normalize_product_payload(product, shop)
    data["product_slug"] = unique_product_slug(shop_id, product.name, pid if existing else "")
    imgs = data["images"]
    if existing and not imgs: imgs = existing[0].get("images", [])
    data["images"] = imgs
    strict_cols = ["variant_data", "variant_matrix"] if data.get("variant_data") or data.get("variant_matrix") else []
    if data.get("offering_type") != "product":
        strict_cols.append("offering_type")
    if data.get("price_mode") != "fixed":
        strict_cols.append("price_mode")
    if data.get("availability_mode") not in {"", "in_stock"}:
        strict_cols.append("availability_mode")
    if data.get("duration_minutes") not in (None, ""):
        strict_cols.append("duration_minutes")
    if data.get("capacity") not in (None, ""):
        strict_cols.append("capacity")
    data, unsupported_cols = product_write_payload_with_fallback(shop_id, pid, data, bool(existing), strict_cols)
    if unsupported_cols:
        print(f"[Product Schema Warning] {shop_id}/{pid}: saved without optional columns {unsupported_cols}")
    
    rebuild_kb(shop_id)
    return {"ok": True, "product_id": pid, "offering_id": pid, "business_id": shop_id}

@app.delete("/admin/business/{shop_id}/offering/{product_id}")
@app.delete("/admin/shop/{shop_id}/product/{product_id}")
def admin_delete_product(shop_id: str, product_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    supabase.table("products").delete().eq("shop_id", shop_id).eq("product_id", product_id).execute()
    rebuild_kb(shop_id)
    return {"ok": True, "business_id": shop_id, "offering_id": product_id}

@app.post("/admin/business/{shop_id}/offering-with-images")
@app.post("/admin/shop/{shop_id}/product-with-images")
async def admin_product_with_images(
    request: Request,
    shop_id: str, authorization: Optional[str] = Header(None),
    product_id: str = Form(""), offering_id: str = Form(""), name: str = Form(...), overview: str = Form(""), price: str = Form(""),
    price_amount: str = Form(""), currency_code: str = Form(""),
    offering_type: str = Form(""), price_mode: str = Form("fixed"), availability_mode: str = Form(""),
    stock: str = Form("in"), stock_quantity: str = Form(""), duration_minutes: str = Form(""), capacity: str = Form(""),
    variants: str = Form(""), variant_data_json: str = Form(""), variant_matrix_json: str = Form(""), attribute_data_json: str = Form(""), images: List[UploadFile] = File(default=[])
):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    if not name.strip(): raise HTTPException(400, "Name required")
    shop_rows = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data
    if not shop_rows: raise HTTPException(404, "Business not found")
    shop = normalize_shop_record(shop_rows[0])
    raw_pid = str(offering_id or product_id or "").strip()
    pid = slug(raw_pid, 60) if raw_pid else gen_product_id(name)
    
    existing = supabase.table("products").select("images").eq("shop_id", shop_id).eq("product_id", pid).execute().data
    if not existing and not raw_pid:
        for _ in range(5):
            dupe = supabase.table("products").select("product_id").eq("shop_id", shop_id).eq("product_id", pid).execute().data
            if not dupe:
                break
            pid = gen_product_id(name)
        else:
            raise HTTPException(500, "Could not generate a unique product ID")
    current_imgs = []
    if existing:
        current_imgs = existing[0].get("images", [])
        if isinstance(current_imgs, str):
            try: current_imgs = json.loads(current_imgs)
            except: current_imgs = []

    variant_form_data = await request.form()
    variant_upload_map: Dict[int, List[UploadFile]] = {}
    combo_upload_map: Dict[int, List[UploadFile]] = {}
    for key, value in variant_form_data.multi_items():
        if not isinstance(value, (UploadFile, StarletteUploadFile)):
            continue
        m = re.match(r"^variant_images_(\d+)$", str(key))
        if m:
            variant_upload_map.setdefault(int(m.group(1)), []).append(value)
            continue
        cm = re.match(r"^variant_combo_images_(\d+)$", str(key))
        if cm:
            combo_upload_map.setdefault(int(cm.group(1)), []).append(value)

    new_urls = save_images(shop_id, images)
    merged = dedup(current_imgs + new_urls)
    parsed_price_amount = parse_price_amount(price_amount)
    if str(price_amount).strip() and parsed_price_amount is None:
        raise HTTPException(400, "Enter a valid numeric price")
    try:
        parsed_stock_quantity = int(stock_quantity) if str(stock_quantity).strip() else None
    except Exception:
        raise HTTPException(400, "Stock quantity must be a whole number")
    try:
        parsed_duration_minutes = int(duration_minutes) if str(duration_minutes).strip() else None
    except Exception:
        raise HTTPException(400, "Duration must be a whole number")
    try:
        parsed_capacity = int(capacity) if str(capacity).strip() else None
    except Exception:
        raise HTTPException(400, "Capacity must be a whole number")
    parsed_attribute_data = parse_attribute_data_strict(attribute_data_json, shop.get("category", ""), offering_type, shop.get("business_type", ""))
    
    parsed_variant_data = normalize_variant_data(variant_data_json or variants, shop_id)
    for idx, files in variant_upload_map.items():
        if idx < 0 or idx >= len(parsed_variant_data):
            continue
        extra_urls = save_images(shop_id, files)
        parsed_variant_data[idx]["images"] = dedup(parsed_variant_data[idx].get("images", []) + extra_urls)
    parsed_variant_matrix = normalize_variant_matrix(variant_matrix_json, parsed_variant_data, shop_id)
    for idx, files in combo_upload_map.items():
        if idx < 0 or idx >= len(parsed_variant_matrix):
            continue
        extra_urls = save_images(shop_id, files)
        parsed_variant_matrix[idx]["images"] = dedup(parsed_variant_matrix[idx].get("images", []) + extra_urls)

    data = normalize_product_payload(Product(
        product_id=pid,
        name=name.strip(),
        overview=overview,
        price=price,
        price_amount=parsed_price_amount,
        currency_code=currency_code,
        offering_type=offering_type,
        price_mode=price_mode,
        availability_mode=availability_mode,
        stock=stock,
        stock_quantity=parsed_stock_quantity,
        duration_minutes=parsed_duration_minutes,
        capacity=parsed_capacity,
        variants=variants,
        variant_data=parsed_variant_data,
        variant_matrix=parsed_variant_matrix,
        attribute_data=parsed_attribute_data,
        images=merged,
    ), shop)
    data["product_slug"] = unique_product_slug(shop_id, name.strip(), pid if existing else "")
    strict_cols = ["variant_data", "variant_matrix"] if data.get("variant_data") or data.get("variant_matrix") else []
    if data.get("offering_type") != "product":
        strict_cols.append("offering_type")
    if data.get("price_mode") != "fixed":
        strict_cols.append("price_mode")
    if data.get("availability_mode") not in {"", "in_stock"}:
        strict_cols.append("availability_mode")
    if data.get("duration_minutes") not in (None, ""):
        strict_cols.append("duration_minutes")
    if data.get("capacity") not in (None, ""):
        strict_cols.append("capacity")
    data, unsupported_cols = product_write_payload_with_fallback(shop_id, pid, data, bool(existing), strict_cols)
    if unsupported_cols:
        print(f"[Product Schema Warning] {shop_id}/{pid}: saved without optional columns {unsupported_cols}")
    
    rebuild_kb(shop_id)
    return {"ok": True, "product_id": pid, "offering_id": pid, "business_id": shop_id, "images": merged}

@app.delete("/admin/business/{shop_id}/offering/{product_id}/image")
@app.delete("/admin/shop/{shop_id}/product/{product_id}/image")
def admin_delete_image(shop_id: str, product_id: str, image_path: str = Query(...), authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    
    row = supabase.table("products").select("images").eq("shop_id", shop_id).eq("product_id", product_id).execute().data
    if not row: raise HTTPException(404, "Offering not found")
    
    img_name = os.path.basename(image_path.rstrip("/"))
    try: supabase.storage.from_(IMAGE_BUCKET).remove([f"{shop_id}/{img_name}"])
    except Exception: pass
    
    current_imgs = row[0].get("images", [])
    if isinstance(current_imgs, str):
        try: current_imgs = json.loads(current_imgs)
        except: current_imgs = []
        
    new_imgs = [u for u in current_imgs if os.path.basename(u) != img_name]
    supabase.table("products").update({"images": new_imgs, "updated_at": "now()"}).eq("shop_id", shop_id).eq("product_id", product_id).execute()
    rebuild_kb(shop_id)
    return {"ok": True, "remaining": len(new_imgs), "business_id": shop_id, "offering_id": product_id}

@app.post("/admin/business/{shop_id}/rebuild-kb")
@app.post("/admin/shop/{shop_id}/rebuild-kb")
def admin_rebuild_kb(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    rebuild_kb(shop_id)
    return {"ok": True, "business_id": shop_id}

@app.get("/admin/business/{shop_id}/analytics")
@app.get("/admin/shop/{shop_id}/analytics")
def admin_analytics(shop_id: str, days: int = 30, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    shop_rows = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data
    shop = normalize_shop_record(shop_rows[0]) if shop_rows else {"shop_id": shop_id}
    
    since = int(time.time()) - 86400 * max(1, min(days, 365))
    since_iso = datetime.fromtimestamp(since, tz=timezone.utc).isoformat()
    
    evs = supabase.table("analytics").select("event, product_id, created_at").eq("shop_id", shop_id).gte("created_at", since_iso).execute().data
    
    totals = {"chat": 0, "shop_view": 0, "view": 0}
    prod_counts = {}
    for e in evs:
        evt = e["event"]
        if evt in totals: totals[evt] += 1
        if evt == "view" and e["product_id"]:
            pid = e["product_id"]
            prod_counts[pid] = prod_counts.get(pid, 0) + 1
            
    top = [{"product_id": pid, "views": c} for pid, c in sorted(prod_counts.items(), key=lambda x: x[1], reverse=True)[:10]]
    for t in top:
        p_res = supabase.table("products").select("name").eq("shop_id", shop_id).eq("product_id", t["product_id"]).execute()
        t["name"] = p_res.data[0]["name"] if p_res.data else t["product_id"]

    now = int(time.time())
    daily = []
    for d in range(13, -1, -1):
        s = now - 86400 * (d + 1); e = now - 86400 * d
        s_iso = datetime.fromtimestamp(s, tz=timezone.utc).isoformat()
        e_iso = datetime.fromtimestamp(e, tz=timezone.utc).isoformat()
        
        c = len(supabase.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "chat").gte("created_at", s_iso).lt("created_at", e_iso).execute().data)
        daily.append({"date": datetime.fromtimestamp(e, tz=timezone.utc).strftime("%b %d"), "chats": c})

    recent_raw = supabase.table("analytics").select("event, product_id, created_at").eq("shop_id", shop_id).order("created_at", desc=True).limit(20).execute().data
    recent_events = []
    for row in recent_raw or []:
        event = row.get("event", "")
        product_id = row.get("product_id")
        product_name = None
        if product_id:
            try:
                p_res = supabase.table("products").select("name").eq("shop_id", shop_id).eq("product_id", product_id).execute()
                if p_res.data:
                    product_name = p_res.data[0].get("name")
            except Exception:
                product_name = None
        recent_events.append({
            "event": event,
            "product_id": product_id,
            "product_name": product_name,
            "created_at": row.get("created_at", ""),
        })

    shop_products = supabase.table("products").select("*").eq("shop_id", shop_id).execute().data or []
    attribute_insights = attribute_analytics_for_products(shop_products, shop.get("category", ""), shop.get("business_type", ""))

    all_shops = [normalize_shop_record(r) for r in (supabase.table("shops").select("*").execute().data or [])]
    country_counts: Dict[str, int] = {}
    region_counts: Dict[str, int] = {}
    city_counts: Dict[str, int] = {}
    open_now_count = 0
    for row in all_shops:
        country = row.get("country_name") or row.get("country_code") or "Unknown"
        region = row.get("region") or "Unknown"
        city = row.get("city") or "Unknown"
        country_counts[country] = country_counts.get(country, 0) + 1
        region_counts[region] = region_counts.get(region, 0) + 1
        city_counts[city] = city_counts.get(city, 0) + 1
        if shop_is_open_now(row):
            open_now_count += 1

    shop_profile = {
        "country": shop.get("country_name") or shop.get("country_code") or "Not set",
        "region": shop.get("region") or "Not set",
        "city": shop.get("city") or "Not set",
        "postal_code": shop.get("postal_code") or "Not set",
        "timezone_name": shop.get("timezone_name") or "UTC",
        "hours": shop.get("hours") or "Not set",
        "is_open_now": shop_is_open_now(shop),
    }
    marketplace_breakdown = {
        "shop_count": len(all_shops),
        "open_now_count": open_now_count,
        "country_counts": [{"label": k, "count": v} for k, v in sorted(country_counts.items(), key=lambda item: (-item[1], item[0]))[:6]],
        "region_counts": [{"label": k, "count": v} for k, v in sorted(region_counts.items(), key=lambda item: (-item[1], item[0]))[:6]],
        "city_counts": [{"label": k, "count": v} for k, v in sorted(city_counts.items(), key=lambda item: (-item[1], item[0]))[:6]],
    }

    return {
        "ok": True,
        "totals": totals,
        "top_products": top,
        "top_offerings": [{**row, "offering_id": row.get("product_id", "")} for row in top],
        "daily_chats": daily,
        "recent_events": recent_events,
        "shop_profile": shop_profile,
        "business_profile": shop_profile,
        "attribute_insights": attribute_insights,
        "marketplace_breakdown": marketplace_breakdown,
        "business_id": shop_id,
    }
