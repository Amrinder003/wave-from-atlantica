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
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
import requests
import smtplib
from typing import Any, Dict, List, Optional, Tuple
from io import BytesIO, StringIO
from email.message import EmailMessage
from urllib.parse import urlparse
import zipfile
import xml.etree.ElementTree as ET
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
MAX_CATALOG_PACKAGE_BYTES = int(os.environ.get("MAX_CATALOG_PACKAGE_BYTES", "262144000") or 262144000)
IMAGE_BUCKET      = "product-images"
BUSINESS_PROFILE_IMAGE_PREFIX = "business-profiles"
AVATAR_IMAGE_PREFIX = "avatars"
RATE_LIMIT_STATE: Dict[str, List[float]] = {}
CURRENT_REQUEST: ContextVar[Optional[Request]] = ContextVar("current_request", default=None)
PUBLIC_SHOP_FIELDS = (
    "shop_id", "shop_slug", "name", "profile_image_url", "address", "formatted_address", "overview", "phone", "phone_public", "hours",
    "hours_structured", "category", "business_type", "location_mode", "service_area", "website", "whatsapp", "country_code", "country_name", "timezone_name",
    "region", "city", "postal_code", "street_line1", "street_line2", "currency_code", "latitude",
    "longitude", "supports_pickup", "supports_delivery", "supports_walk_in", "delivery_radius_km",
    "delivery_fee", "pickup_notes",
)
LISTING_STATUS_DRAFT = "draft"
LISTING_STATUS_PENDING_REVIEW = "pending_review"
LISTING_STATUS_VERIFIED = "verified"
LISTING_STATUS_REJECTED = "rejected"
ALLOWED_LISTING_STATUSES = {
    LISTING_STATUS_DRAFT,
    LISTING_STATUS_PENDING_REVIEW,
    LISTING_STATUS_VERIFIED,
    LISTING_STATUS_REJECTED,
}
CLAIM_STATUS_PENDING = "pending"
CLAIM_STATUS_APPROVED = "approved"
CLAIM_STATUS_REJECTED = "rejected"
ALLOWED_CLAIM_STATUSES = {
    CLAIM_STATUS_PENDING,
    CLAIM_STATUS_APPROVED,
    CLAIM_STATUS_REJECTED,
}
LISTING_SOURCE_OWNER_CREATED = "owner_created"
LISTING_SOURCE_PLATFORM_IMPORT = "platform_import"
ALLOWED_LISTING_SOURCES = {
    LISTING_SOURCE_OWNER_CREATED,
    LISTING_SOURCE_PLATFORM_IMPORT,
}
OWNERSHIP_STATUS_CLAIMED = "claimed"
OWNERSHIP_STATUS_PLATFORM_MANAGED = "platform_managed"
ALLOWED_OWNERSHIP_STATUSES = {
    OWNERSHIP_STATUS_CLAIMED,
    OWNERSHIP_STATUS_PLATFORM_MANAGED,
}
PLATFORM_MANAGED_OWNER_CONTACT = "Platform imported listing"
PUBLIC_LISTING_STATUSES = {LISTING_STATUS_VERIFIED}
ALLOWED_VERIFICATION_METHODS = {
    "manual",
    "registry",
    "license",
    "website",
    "domain_email",
    "phone",
}
SHOP_REVIEW_WRITE_COLUMNS = [
    "listing_status",
    "owner_contact_name",
    "verification_method",
    "verification_evidence",
    "verification_submitted_at",
    "verified_at",
    "verification_rejection_reason",
]
SHOP_TRUST_WRITE_COLUMNS = [
    "trust_flags",
    "risk_score",
    "risk_level",
]
UNPUBLISHED_LISTING_STATUSES = {
    LISTING_STATUS_DRAFT,
    LISTING_STATUS_PENDING_REVIEW,
    LISTING_STATUS_REJECTED,
}
NEW_CREATOR_ACCOUNT_AGE_DAYS = int(os.environ.get("NEW_CREATOR_ACCOUNT_AGE_DAYS", "14") or 14)
NEW_CREATOR_DRAFT_BURST_HOURS = int(os.environ.get("NEW_CREATOR_DRAFT_BURST_HOURS", "24") or 24)
NEW_CREATOR_DRAFT_BURST_LIMIT = int(os.environ.get("NEW_CREATOR_DRAFT_BURST_LIMIT", "2") or 2)
NEW_CREATOR_TOTAL_UNPUBLISHED_LIMIT = int(os.environ.get("NEW_CREATOR_TOTAL_UNPUBLISHED_LIMIT", "4") or 4)
BUSINESS_CLAIM_SEARCH_LIMIT = int(os.environ.get("BUSINESS_CLAIM_SEARCH_LIMIT", "8") or 8)
BUSINESS_CLAIM_NOTE_MIN_LEN = int(os.environ.get("BUSINESS_CLAIM_NOTE_MIN_LEN", "8") or 8)
BUSINESS_CLAIM_NOTE_MAX_LEN = int(os.environ.get("BUSINESS_CLAIM_NOTE_MAX_LEN", "800") or 800)
CLAIMABLE_SHOP_FIELDS = ",".join([
    "shop_id",
    "shop_slug",
    "owner_user_id",
    "listing_status",
    "name",
    "profile_image_url",
    "formatted_address",
    "address",
    "service_area",
    "location_mode",
    "category",
    "phone",
    "website",
    "whatsapp",
    "country_code",
    "country_name",
    "region",
    "city",
    "postal_code",
    "street_line1",
    "street_line2",
    "owner_contact_name",
    "verified_at",
    "created_at",
])

OPENROUTER_KEY    = os.environ.get("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL  = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4.1-mini").strip()
OPENROUTER_FALLBACK_MODELS = [m.strip() for m in os.environ.get("OPENROUTER_FALLBACK_MODELS", "").split(",") if m.strip()]
OPENROUTER_TEMPERATURE = float(os.environ.get("OPENROUTER_TEMPERATURE", "0.2") or 0.2)
OPENROUTER_MAX_TOKENS = int(os.environ.get("OPENROUTER_MAX_TOKENS", "700") or 700)
OPENROUTER_URL    = "https://openrouter.ai/api/v1/chat/completions"
MAPBOX_TOKEN      = os.environ.get("MAPBOX_TOKEN", "").strip()
MAPBOX_GEOCODING_URL = "https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json"
LEAFLET_REQUIRED_ASSETS = ("leaflet.js", "leaflet.css")
FX_API_BASE       = os.environ.get("FX_API_BASE", "https://api.frankfurter.dev/v2").strip().rstrip("/")
FX_CACHE_SECONDS  = int(os.environ.get("FX_CACHE_SECONDS", "21600") or 21600)
PRODUCT_SEARCH_INDEX_CACHE_SECONDS = int(os.environ.get("PRODUCT_SEARCH_INDEX_CACHE_SECONDS", "45") or 45)

SUPABASE_URL      = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY      = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
APP_BASE_URL      = os.environ.get("APP_BASE_URL", "http://localhost:8001").strip()
CORS_ORIGINS      = [o.strip() for o in os.environ.get("CORS_ORIGINS", APP_BASE_URL).split(",") if o.strip()]
SMTP_HOST         = os.environ.get("SMTP_HOST", "").strip()
SMTP_PORT         = int(os.environ.get("SMTP_PORT", "587") or 587)
SMTP_USERNAME     = os.environ.get("SMTP_USERNAME", "").strip()
SMTP_PASSWORD     = os.environ.get("SMTP_PASSWORD", "").strip()
SMTP_FROM_EMAIL   = os.environ.get("SMTP_FROM_EMAIL", "").strip()
SMTP_FROM_NAME    = os.environ.get("SMTP_FROM_NAME", "Atlantic Ordinate").strip()
SMTP_USE_TLS      = os.environ.get("SMTP_USE_TLS", "true").strip().lower() not in {"0", "false", "no"}
REVIEW_NOTIFICATION_EMAILS = [
    re.sub(r"\s+", "", item).strip().lower()
    for item in os.environ.get("REVIEW_NOTIFICATION_EMAILS", "").split(",")
    if re.sub(r"\s+", "", item).strip()
]
LAST_REVIEW_NOTIFICATION_RESULT: Dict[str, Any] = {
    "attempted_at": None,
    "subject": "",
    "sent_count": 0,
    "recipient_count": 0,
    "delivered_to": [],
    "errors": [],
}
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
PRODUCT_SEARCH_INDEX_CACHE: Dict[str, Dict[str, Any]] = {}
PRODUCT_SEARCH_INDEX_COLUMNS = ",".join([
    "product_id",
    "shop_id",
    "name",
    "overview",
    "offering_type",
    "price_mode",
    "availability_mode",
    "duration_minutes",
    "capacity",
    "variants",
    "variant_data",
    "variant_matrix",
    "attribute_data",
    "external_links",
    "stock",
    "updated_at",
    "price",
    "price_amount",
])
PRODUCT_SEARCH_INDEX_COLUMNS_LEGACY = ",".join(
    col for col in PRODUCT_SEARCH_INDEX_COLUMNS.split(",") if col != "external_links"
)

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
BUSINESS_COUNTRY_LOCK_ENABLED = os.environ.get("BUSINESS_COUNTRY_LOCK_ENABLED", "true").strip().lower() not in {"0", "false", "no"}
BUSINESS_COUNTRY_LOCK_CODE = re.sub(r"[^A-Za-z]", "", os.environ.get("BUSINESS_COUNTRY_LOCK_CODE", "CA").upper())[:2] or "CA"
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
    "creator": {"label": "Creator studio", "offering_type": "product", "singular": "product", "plural": "products", "business_label": "studio"},
    "other": {"label": "Business", "offering_type": "product", "singular": "product", "plural": "products", "business_label": "business"},
}
OFFERING_TYPE_META: Dict[str, Dict[str, str]] = {
    "product": {"label": "Product", "singular": "product", "plural": "products"},
    "service": {"label": "Service", "singular": "service", "plural": "services"},
    "class": {"label": "Class", "singular": "class", "plural": "classes"},
    "event": {"label": "Event", "singular": "event", "plural": "events"},
    "portfolio": {"label": "Portfolio item", "singular": "portfolio item", "plural": "portfolio items"},
    "offering": {"label": "Product", "singular": "product", "plural": "products"},
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
app = FastAPI(title="Atlantic Ordinate API", version="4.0")

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
        "booked_out": "unavailable",
        "bookedout": "unavailable",
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

def offering_external_links_text(row: Dict[str, Any], limit: int = 3) -> str:
    links = normalize_offering_external_links((row or {}).get("external_links") or (row or {}).get("offering_links") or (row or {}).get("action_links"))
    if not links:
        return ""
    return "; ".join(f"{item['label']}: {item['url']}" for item in links[:limit])

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

class AccountDeletionRequestReq(BaseModel):
    confirm_text: str = ""
    reason: str = ""

class AdminAccountDeletionProcessReq(BaseModel):
    confirm_email: str = ""
    confirm_text: str = ""
    delete_owned_businesses: bool = False
    dry_run: bool = True

class UpdateProfileReq(BaseModel):
    display_name: str = ""

class BrowserSessionReq(BaseModel):
    access_token: str = ""
    refresh_token: str = ""
    auth_code: str = ""
    code_verifier: str = ""
    token_hash: str = ""
    type: str = ""
    redirect_to: str = ""

class ShopInfo(BaseModel):
    name: str
    address: str = ""
    profile_image_url: str = ""
    overview: str = ""
    phone: str = ""
    phone_public: bool = False
    website: str = ""
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
    owner_contact_name: str = ""
    verification_method: str = ""
    verification_evidence: str = ""

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
    external_links: List[Dict[str, Any]] = []
    images: List[str] = []

class CreateShopReq(BaseModel):
    shop_id: Optional[str] = None
    business_id: Optional[str] = None
    shop: Optional[ShopInfo] = None
    business: Optional[ShopInfo] = None

class CreateManagedShopReq(BaseModel):
    shop: Optional[ShopInfo] = None
    business: Optional[ShopInfo] = None
    publish: bool = True

class ReviewReq(BaseModel):
    rating: int = Field(..., ge=1, le=5)
    body: str

class ReviewDecisionReq(BaseModel):
    reason: str = ""

class BusinessClaimReq(BaseModel):
    note: str = ""

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

def optional_user_id(auth: Optional[str] = None, request: Optional[Request] = None) -> Optional[str]:
    try:
        if auth or session_cookie_present(request):
            user, _ = get_user(auth, request)
            return user.id
    except HTTPException:
        return None
    except Exception:
        return None
    return None

def require_verified(user):
    if not user.email_confirmed_at:
        raise HTTPException(403, "Email not verified. Check your inbox.")

def require_admin_role(prof: Optional[Dict[str, Any]] = None):
    role = str((prof or {}).get("role", "") or "").strip().lower()
    if role not in {"admin", "superadmin"}:
        raise HTTPException(403, "Admin access required")

def is_admin_profile(prof: Optional[Dict[str, Any]] = None) -> bool:
    role = str((prof or {}).get("role", "") or "").strip().lower()
    return role in {"admin", "superadmin"}

def normalize_listing_status(value: Any, *, default: str = LISTING_STATUS_DRAFT) -> str:
    status = str(value or "").strip().lower()
    return status if status in ALLOWED_LISTING_STATUSES else default

def normalize_listing_source(value: Any) -> str:
    source = str(value or "").strip().lower()
    return source if source in ALLOWED_LISTING_SOURCES else ""

def normalize_ownership_status(value: Any, *, listing_source: str = "", owner_contact_name: str = "") -> str:
    status = str(value or "").strip().lower()
    if status in ALLOWED_OWNERSHIP_STATUSES:
        return status
    source = normalize_listing_source(listing_source)
    contact = re.sub(r"\s+", " ", str(owner_contact_name or "")).strip().lower()
    if source == LISTING_SOURCE_PLATFORM_IMPORT or contact == PLATFORM_MANAGED_OWNER_CONTACT.lower():
        return OWNERSHIP_STATUS_PLATFORM_MANAGED
    return OWNERSHIP_STATUS_CLAIMED

def shop_is_platform_managed(row: Dict[str, Any]) -> bool:
    return normalize_ownership_status(
        (row or {}).get("ownership_status"),
        listing_source=(row or {}).get("listing_source", ""),
        owner_contact_name=(row or {}).get("owner_contact_name", ""),
    ) == OWNERSHIP_STATUS_PLATFORM_MANAGED

def normalize_verification_method(value: Any) -> str:
    method = str(value or "").strip().lower()
    return method if method in ALLOWED_VERIFICATION_METHODS else ""

def normalize_bool_flag(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return default
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}

def normalize_owner_contact_name(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:120]

MAX_PUBLIC_URL_LENGTH = 800
MAX_OFFERING_EXTERNAL_LINKS = 4
MAX_OFFERING_LINK_LABEL_LENGTH = 48

def normalize_public_url(value: Any) -> str:
    text = re.sub(r"\s+", "", str(value or "")).strip()[:MAX_PUBLIC_URL_LENGTH]
    if not text:
        return ""
    if text.startswith("//"):
        text = f"https:{text}"
    if not re.match(r"^https?://", text, re.I):
        text = f"https://{text}"
    try:
        parsed = urlparse(text)
    except Exception:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or "." not in parsed.netloc:
        return ""
    return text

def normalize_offering_external_links(raw: Any, strict: bool = False) -> List[Dict[str, str]]:
    if raw in (None, ""):
        return []
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            raw = json.loads(text)
        except Exception:
            if strict:
                raise HTTPException(400, "external_links_json must be valid JSON")
            return []
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list):
        if strict:
            raise HTTPException(400, "external_links must be a JSON array")
        return []

    out: List[Dict[str, str]] = []
    seen = set()
    for idx, item in enumerate(raw[:MAX_OFFERING_EXTERNAL_LINKS]):
        if isinstance(item, str):
            label_raw = "Website"
            url_raw = item
        elif isinstance(item, dict):
            label_raw = item.get("label") or item.get("name") or item.get("title") or item.get("button_label") or item.get("text") or ""
            url_raw = item.get("url") or item.get("href") or item.get("website") or item.get("link") or ""
        else:
            if strict:
                raise HTTPException(400, f"External link {idx + 1} must be an object")
            continue

        label = re.sub(r"\s+", " ", str(label_raw or "")).strip()[:MAX_OFFERING_LINK_LABEL_LENGTH].strip()
        url = normalize_public_url(url_raw)
        if not label and not url:
            continue
        if not url:
            if strict:
                raise HTTPException(400, f"External link {idx + 1} needs a valid website URL")
            continue
        if not label:
            label = "Website"
        signature = (label.lower(), url.lower())
        if signature in seen:
            continue
        seen.add(signature)
        out.append({"label": label, "url": url})
    return out

def normalize_verification_evidence(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:500]

def parse_datetime_value(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def normalize_phone_fingerprint(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits

def shop_contact_fingerprints(row: Dict[str, Any]) -> List[str]:
    shop = normalize_shop_record(row or {})
    values = [
        normalize_phone_fingerprint(shop.get("phone", "")),
        normalize_phone_fingerprint(shop.get("whatsapp", "")),
    ]
    seen: set = set()
    ordered: List[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered

def normalize_name_fingerprint(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", norm_text(str(value or "")))).strip()

def normalize_address_fingerprint(row: Dict[str, Any]) -> str:
    shop = normalize_shop_record(row or {})
    if shop.get("location_mode") not in {"storefront", "hybrid"}:
        return ""
    parts = [
        shop.get("street_line1", ""),
        shop.get("city", ""),
        shop.get("region", ""),
        shop.get("postal_code", ""),
        shop.get("country_code", ""),
    ]
    fallback = shop.get("formatted_address", "") or shop.get("address", "")
    raw = " ".join([part for part in parts if str(part or "").strip()]) or fallback
    return normalize_name_fingerprint(raw)

def normalize_market_name_fingerprint(row: Dict[str, Any]) -> str:
    shop = normalize_shop_record(row or {})
    name = normalize_name_fingerprint(shop.get("name", ""))
    city = normalize_name_fingerprint(shop.get("city", ""))
    region = normalize_name_fingerprint(shop.get("region", ""))
    if not name or not (city or region):
        return ""
    return " | ".join([part for part in [name, city, region] if part])

def risk_level_for_score(score: Any) -> str:
    text = str(score or "").strip().lower()
    if text in {"low", "medium", "high"}:
        return text
    try:
        value = int(score or 0)
    except Exception:
        value = 0
    if value >= 60:
        return "high"
    if value >= 25:
        return "medium"
    return "low"

def is_new_creator_account(user: Any) -> bool:
    created_at = parse_datetime_value(getattr(user, "created_at", None))
    if not created_at:
        return False
    age = datetime.now(timezone.utc) - created_at
    return age <= timedelta(days=max(1, NEW_CREATOR_ACCOUNT_AGE_DAYS))

def summarize_creator_draft_counts(owner_rows: List[Dict[str, Any]]) -> Dict[str, int]:
    now = datetime.now(timezone.utc)
    unpublished = [
        row for row in (owner_rows or [])
        if normalize_listing_status(row.get("listing_status"), default=LISTING_STATUS_VERIFIED) in UNPUBLISHED_LISTING_STATUSES
    ]
    recent_unpublished = 0
    for row in unpublished:
        created_at = parse_datetime_value(row.get("created_at"))
        if created_at and created_at >= now - timedelta(hours=max(1, NEW_CREATOR_DRAFT_BURST_HOURS)):
            recent_unpublished += 1
    return {
        "recent_unpublished": recent_unpublished,
        "total_unpublished": len(unpublished),
        "total_all": len(owner_rows or []),
    }

def enforce_creator_draft_limits(user: Any, owner_rows: List[Dict[str, Any]]) -> None:
    if not is_new_creator_account(user):
        return
    counts = summarize_creator_draft_counts(owner_rows or [])
    if counts["recent_unpublished"] >= max(1, NEW_CREATOR_DRAFT_BURST_LIMIT):
        raise HTTPException(429, f"New accounts can only create {NEW_CREATOR_DRAFT_BURST_LIMIT} fresh business drafts in {NEW_CREATOR_DRAFT_BURST_HOURS} hours. Finish or review the current drafts first.")
    if counts["total_unpublished"] >= max(1, NEW_CREATOR_TOTAL_UNPUBLISHED_LIMIT):
        raise HTTPException(429, f"New accounts can only keep {NEW_CREATOR_TOTAL_UNPUBLISHED_LIMIT} unpublished business drafts at a time. Submit or clean up the current drafts first.")

def build_shop_trust_snapshot(
    shop_row: Dict[str, Any],
    user: Any,
    owner_rows: Optional[List[Dict[str, Any]]] = None,
    all_rows: Optional[List[Dict[str, Any]]] = None,
    *,
    ignore_shop_id: str = "",
    projected_new_unpublished: bool = False,
) -> Dict[str, Any]:
    shop = normalize_shop_record(shop_row or {})
    owner_rows = [normalize_shop_record(row) for row in (owner_rows or [])]
    all_rows = [normalize_shop_record(row) for row in (all_rows or [])]
    flags: List[str] = []
    score = 0
    if is_new_creator_account(user):
        counts = summarize_creator_draft_counts(owner_rows)
        projected_recent = counts["recent_unpublished"] + (1 if projected_new_unpublished else 0)
        projected_total = counts["total_unpublished"] + (1 if projected_new_unpublished else 0)
        if projected_recent >= max(1, NEW_CREATOR_DRAFT_BURST_LIMIT):
            flags.append("Review: new account is creating several drafts quickly")
            score += 25
        if projected_total >= max(2, NEW_CREATOR_TOTAL_UNPUBLISHED_LIMIT):
            flags.append("Review: new account already has several unpublished businesses")
            score += 20
    phone_fingerprints = shop_contact_fingerprints(shop)
    if phone_fingerprints:
        other_owner_phone_matches = [
            row for row in all_rows
            if str(row.get("shop_id", "")) != str(ignore_shop_id or "")
            and str(row.get("owner_user_id", "")) != str(getattr(user, "id", "") or "")
            and any(match in shop_contact_fingerprints(row) for match in phone_fingerprints)
        ]
        if other_owner_phone_matches:
            flags.append("Review: phone number matches another business listing")
            score += 45
        same_owner_phone_matches = [
            row for row in owner_rows
            if str(row.get("shop_id", "")) != str(ignore_shop_id or "")
            and any(match in shop_contact_fingerprints(row) for match in phone_fingerprints)
        ]
        if len(same_owner_phone_matches) >= 2:
            flags.append("Review: same phone number is reused across several businesses")
            score += 10
    address_fingerprint = normalize_address_fingerprint(shop)
    if address_fingerprint:
        other_owner_address_matches = [
            row for row in all_rows
            if str(row.get("shop_id", "")) != str(ignore_shop_id or "")
            and str(row.get("owner_user_id", "")) != str(getattr(user, "id", "") or "")
            and normalize_address_fingerprint(row) == address_fingerprint
        ]
        if other_owner_address_matches:
            flags.append("Review: address matches another owner's business listing")
            score += 35
    market_name_fingerprint = normalize_market_name_fingerprint(shop)
    if market_name_fingerprint:
        similar_name_matches = [
            row for row in all_rows
            if str(row.get("shop_id", "")) != str(ignore_shop_id or "")
            and str(row.get("owner_user_id", "")) != str(getattr(user, "id", "") or "")
            and normalize_market_name_fingerprint(row) == market_name_fingerprint
        ]
        if similar_name_matches:
            flags.append("Review: very similar business name already exists in the same area")
            score += 18
    deduped: List[str] = []
    seen: set = set()
    for item in flags:
        key = str(item or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(str(item).strip()[:160])
    return {
        "trust_flags": deduped,
        "risk_score": score,
        "risk_level": risk_level_for_score(score),
    }

def load_shop_trust_rows(owner_user_id: str = "") -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rows = supabase.table("shops").select("*").order("created_at", desc=True).execute().data or []
    owner_rows = [row for row in rows if str(row.get("owner_user_id", "")) == str(owner_user_id or "")]
    return owner_rows, rows

def normalize_claim_status(value: Any, *, default: str = CLAIM_STATUS_PENDING) -> str:
    status = str(value or "").strip().lower()
    if status in ALLOWED_CLAIM_STATUSES:
        return status
    return default

def normalize_business_claim_note(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:BUSINESS_CLAIM_NOTE_MAX_LEN]

def gen_business_claim_id(shop_id: str = "") -> str:
    base = slug_base(shop_id or "claim", "claim", 28)
    return f"clm-{base}-{uuid.uuid4().hex[:6]}"

def same_business_locality(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    shop_a = normalize_shop_record(a or {})
    shop_b = normalize_shop_record(b or {})
    city_a = normalize_name_fingerprint(shop_a.get("city", ""))
    city_b = normalize_name_fingerprint(shop_b.get("city", ""))
    region_a = normalize_name_fingerprint(shop_a.get("region", ""))
    region_b = normalize_name_fingerprint(shop_b.get("region", ""))
    postal_a = normalize_name_fingerprint(shop_a.get("postal_code", ""))
    postal_b = normalize_name_fingerprint(shop_b.get("postal_code", ""))
    if city_a and city_b and region_a and region_b and city_a == city_b and region_a == region_b:
        return True
    if postal_a and postal_b and postal_a == postal_b:
        return True
    if city_a and city_b and city_a == city_b:
        return True
    return False

def same_storefront_address(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    fp_a = normalize_address_fingerprint(a)
    fp_b = normalize_address_fingerprint(b)
    if not fp_a or not fp_b or fp_a != fp_b:
        return False
    unit_a = normalize_name_fingerprint(normalize_shop_record(a or {}).get("street_line2", ""))
    unit_b = normalize_name_fingerprint(normalize_shop_record(b or {}).get("street_line2", ""))
    if unit_a and unit_b and unit_a != unit_b:
        return False
    return True

def load_claimable_shop_rows(*, public_only: bool = True) -> List[Dict[str, Any]]:
    q = supabase.table("shops").select(CLAIMABLE_SHOP_FIELDS)
    if public_only:
        q = q.eq("listing_status", LISTING_STATUS_VERIFIED)
    rows = q.order("verified_at", desc=True).order("created_at", desc=True).execute().data or []
    return [normalize_shop_record(row) for row in rows]

def duplicate_business_matches(
    shop_row: Dict[str, Any],
    *,
    viewer_user_id: str = "",
    ignore_shop_id: str = "",
    rows: Optional[List[Dict[str, Any]]] = None,
    public_only: bool = True,
    blocking_only: bool = False,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    source = normalize_shop_record(shop_row or {})
    source_name = str(source.get("name", "") or "").strip()
    if not source_name:
        return []
    name_fp = normalize_market_name_fingerprint(source)
    address_matchable = same_storefront_address(source, source)
    source_contacts = set(shop_contact_fingerprints(source))
    base_rows = rows if rows is not None else load_claimable_shop_rows(public_only=public_only)
    matches: List[Dict[str, Any]] = []
    for raw in base_rows or []:
        candidate = normalize_shop_record(raw or {})
        if public_only and candidate.get("listing_status") not in PUBLIC_LISTING_STATUSES:
            continue
        if ignore_shop_id and str(candidate.get("shop_id", "")) == str(ignore_shop_id):
            continue
        if viewer_user_id and str(candidate.get("owner_user_id", "")) == str(viewer_user_id):
            continue
        candidate_name = str(candidate.get("name", "") or "").strip()
        if not candidate_name:
            continue
        name_similarity = fuzzy_match_score(source_name, candidate_name)
        candidate_name_fp = normalize_market_name_fingerprint(candidate)
        exact_name_area = bool(name_fp and candidate_name_fp and name_fp == candidate_name_fp)
        same_locality = same_business_locality(source, candidate)
        same_address = address_matchable and same_storefront_address(source, candidate)
        shared_contacts = sorted(source_contacts.intersection(shop_contact_fingerprints(candidate)))
        score = 0
        reasons: List[str] = []
        if same_address:
            score += 75
            reasons.append("Same storefront address")
        if shared_contacts:
            score += 78
            reasons.append("Same business contact number")
        if exact_name_area:
            score += 56
            reasons.append("Same business name in the same area")
        elif name_similarity >= 0.93:
            score += 42
            reasons.append("Nearly identical business name")
        elif name_similarity >= 0.84 and same_locality:
            score += 24
            reasons.append("Very similar business name nearby")
        if same_locality:
            score += 10
        blocked = (
            (same_address and name_similarity >= 0.58)
            or (shared_contacts and name_similarity >= 0.58)
            or (exact_name_area and (same_address or shared_contacts))
        )
        suggested = blocked or (
            score >= 55 and (
                same_address
                or shared_contacts
                or exact_name_area
                or (name_similarity >= 0.88 and same_locality)
            )
        )
        if blocking_only and not blocked:
            continue
        if not blocking_only and not suggested:
            continue
        candidate["duplicate_score"] = score
        candidate["duplicate_reasons"] = reasons[:3]
        candidate["duplicate_blocking_match"] = blocked
        matches.append(candidate)
    matches.sort(
        key=lambda row: (
            1 if row.get("duplicate_blocking_match") else 0,
            int(row.get("duplicate_score") or 0),
            str(row.get("verified_at") or row.get("created_at") or ""),
        ),
        reverse=True,
    )
    return matches[:max(1, limit)]

def search_claimable_businesses(query: str, *, viewer_user_id: str = "", limit: int = BUSINESS_CLAIM_SEARCH_LIMIT) -> List[Dict[str, Any]]:
    q = norm_text(query)
    if len(q) < 2:
        return []
    tokens = [tok for tok in re.findall(r"[a-z0-9]+", q) if len(tok) >= 2]
    if not tokens:
        return []
    rows = load_claimable_shop_rows(public_only=True)
    results: List[Dict[str, Any]] = []
    for row in rows:
        if viewer_user_id and str(row.get("owner_user_id", "")) == str(viewer_user_id):
            continue
        hay = " ".join([
            str(row.get("name", "") or ""),
            str(row.get("formatted_address", "") or row.get("address", "") or row.get("service_area", "") or ""),
            str(row.get("city", "") or ""),
            str(row.get("region", "") or ""),
            str(row.get("postal_code", "") or ""),
            str(row.get("phone", "") or row.get("whatsapp", "") or ""),
            str(row.get("category", "") or ""),
        ])
        hay_norm = norm_text(hay)
        score = 0
        if q in norm_text(row.get("name", "")):
            score += 80
        if q in hay_norm:
            score += 34
        score += min(45, int(fuzzy_match_score(q, row.get("name", "")) * 45))
        token_hits = sum(1 for tok in tokens if tok in hay_norm)
        if not token_hits and q not in hay_norm and fuzzy_match_score(q, row.get("name", "")) < 0.55:
            continue
        score += token_hits * 8
        row["duplicate_score"] = score
        results.append(row)
    results.sort(
        key=lambda row: (
            int(row.get("duplicate_score") or 0),
            str(row.get("verified_at") or row.get("created_at") or ""),
        ),
        reverse=True,
    )
    return results[:max(1, limit)]

def normalize_business_claim_record(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row or {})
    out["claim_id"] = str(out.get("claim_id", "") or "").strip()
    out["shop_id"] = str(out.get("shop_id", "") or out.get("business_id", "") or "").strip()
    out["business_id"] = out["shop_id"]
    out["status"] = normalize_claim_status(out.get("status"), default=CLAIM_STATUS_PENDING)
    out["note"] = normalize_business_claim_note(out.get("note", ""))
    out["review_note"] = normalize_business_claim_note(out.get("review_note", ""))
    out["claimant_user_id"] = str(out.get("claimant_user_id", "") or "").strip()
    out["claimant_display_name"] = re.sub(r"\s+", " ", str(out.get("claimant_display_name", "") or "")).strip()[:120]
    out["claimant_email"] = re.sub(r"\s+", " ", str(out.get("claimant_email", "") or "")).strip()[:200]
    out["created_at"] = out.get("created_at") or None
    out["updated_at"] = out.get("updated_at") or None
    shop = out.get("shop") or out.get("business") or {}
    if isinstance(shop, dict) and shop:
        normalized_shop = normalize_shop_record(shop)
        out["shop"] = normalized_shop
        out["business"] = normalized_shop
        out["shop_id"] = out["shop_id"] or normalized_shop.get("shop_id", "")
        out["business_id"] = out["shop_id"]
    else:
        out["shop"] = {}
        out["business"] = {}
    return out

def attach_business_claim_shops(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    claims = [normalize_business_claim_record(row) for row in (rows or [])]
    shop_ids = [claim.get("shop_id") for claim in claims if claim.get("shop_id")]
    shop_map: Dict[str, Dict[str, Any]] = {}
    if shop_ids:
        shop_rows = supabase.table("shops").select(CLAIMABLE_SHOP_FIELDS).in_("shop_id", shop_ids).execute().data or []
        shop_map = {
            str(row.get("shop_id", "")): normalize_shop_record(row)
            for row in shop_rows
            if row.get("shop_id")
        }
    for claim in claims:
        shop = shop_map.get(str(claim.get("shop_id", "")), claim.get("shop") or {})
        claim["shop"] = shop or {}
        claim["business"] = shop or {}
        claim["shop_name"] = (shop or {}).get("name", "")
        claim["business_name"] = claim["shop_name"]
    return claims

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
    return app_route_url("/ui")

def app_base_url() -> str:
    base = (APP_BASE_URL or "http://localhost:8001").strip().rstrip("/")
    if base.endswith("/ui"):
        base = base[:-3].rstrip("/")
    return base or "http://localhost:8001"

def app_route_url(path: str = "/ui") -> str:
    return f"{app_base_url()}/{str(path or '/ui').lstrip('/')}"

def password_reset_redirect_url() -> str:
    return f"{app_route_url('/reset-password')}?auth=recovery"

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

def normalize_display_name(value: str = "", email: str = "") -> str:
    name = re.sub(r"\s+", " ", str(value or "").strip())
    if name:
        return name[:120]
    clean = clean_email(email or "")
    if clean and "@" in clean:
        return clean.split("@", 1)[0][:120]
    return ""

def profile_defaults_for_user(user: Any, prof: Optional[Dict[str, Any]] = None, preferred_display_name: str = "") -> Dict[str, Any]:
    profile = prof or {}
    metadata = getattr(user, "user_metadata", {}) or {}
    email = str(getattr(user, "email", "") or "").strip()
    return {
        "display_name": normalize_display_name(
            preferred_display_name or profile.get("display_name", "") or metadata.get("display_name", ""),
            email,
        ),
        "avatar_url": str(profile.get("avatar_url", "") or metadata.get("avatar_url", "") or "").strip(),
        "role": str(profile.get("role", "") or metadata.get("role", "") or "customer").strip() or "customer",
    }

def ensure_profile_row(user: Any, prof: Optional[Dict[str, Any]] = None, preferred_display_name: str = "") -> Dict[str, Any]:
    profile = dict(prof or load_profile(user.id) or {})
    defaults = profile_defaults_for_user(user, profile, preferred_display_name)
    if profile:
        updates: Dict[str, Any] = {}
        current_name = str(profile.get("display_name", "") or "").strip()
        current_avatar = str(profile.get("avatar_url", "") or "").strip()
        current_role = str(profile.get("role", "") or "").strip()
        if preferred_display_name and defaults["display_name"] and defaults["display_name"] != current_name:
            updates["display_name"] = defaults["display_name"]
        elif not current_name and defaults["display_name"]:
            updates["display_name"] = defaults["display_name"]
        if not current_avatar and defaults["avatar_url"]:
            updates["avatar_url"] = defaults["avatar_url"]
        if not current_role:
            updates["role"] = defaults["role"]
        if updates:
            supabase.table("profiles").update(updates).eq("id", user.id).execute()
            profile.update(updates)
        return {**defaults, **profile}
    payload = {"id": user.id, **defaults}
    supabase.table("profiles").insert(payload).execute()
    return payload

def safe_ensure_profile_row(user: Any, prof: Optional[Dict[str, Any]] = None, preferred_display_name: str = "") -> Dict[str, Any]:
    try:
        return ensure_profile_row(user, prof, preferred_display_name)
    except Exception as e:
        print(f"[Profile Sync Warning] user={getattr(user, 'id', '')}: {e}")
        return profile_defaults_for_user(user, prof, preferred_display_name)

PASSWORD_MIN_LENGTH = 10
PASSWORD_SYMBOL_RE = re.compile(r"[^A-Za-z0-9]")
COMMON_WEAK_PASSWORDS = {
    "password", "password1", "password12", "password123", "password123!",
    "qwerty123", "qwerty123!", "admin123", "admin123!", "welcome123",
    "letmein123", "changeme123", "atlantica123", "wave12345",
}

def password_policy_errors(password: str, email: str = "", display_name: str = "") -> List[str]:
    value = str(password or "")
    errors: List[str] = []
    if len(value) < PASSWORD_MIN_LENGTH:
        errors.append(f"at least {PASSWORD_MIN_LENGTH} characters")
    if re.search(r"\s", value):
        errors.append("no spaces")
    if not re.search(r"[A-Z]", value):
        errors.append("one uppercase letter")
    if not re.search(r"[a-z]", value):
        errors.append("one lowercase letter")
    if not re.search(r"\d", value):
        errors.append("one number")
    if not PASSWORD_SYMBOL_RE.search(value):
        errors.append("one symbol")
    lowered = value.lower()
    compact = re.sub(r"[^a-z0-9]", "", lowered)
    if lowered in COMMON_WEAK_PASSWORDS or compact in COMMON_WEAK_PASSWORDS:
        errors.append("not a common password")
    email_name = re.sub(r"[^a-z0-9]", "", clean_email(email).split("@", 1)[0].lower()) if email else ""
    display_bits = [re.sub(r"[^a-z0-9]", "", part.lower()) for part in re.findall(r"[A-Za-z0-9]{3,}", display_name or "")]
    blocked_parts = [part for part in [email_name, *display_bits] if len(part) >= 3]
    if any(part and part in compact for part in blocked_parts):
        errors.append("does not include your email or name")
    if re.search(r"(.)\1{4,}", value):
        errors.append("no long repeated character runs")
    return dedup(errors)

def require_strong_password(password: str, context: str = "password", email: str = "", display_name: str = "") -> str:
    value = str(password or "")
    errors = password_policy_errors(value, email, display_name)
    if errors:
        raise HTTPException(400, f"Your {context} must include: {', '.join(errors)}.")
    return value

def auth_user_payload(user: Any, prof: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    profile = profile_defaults_for_user(user, prof or load_profile(user.id))
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
    return bool(SMTP_HOST and SMTP_FROM_EMAIL and (not SMTP_USERNAME or SMTP_PASSWORD))

def clean_email(value: str) -> str:
    return re.sub(r"\s+", "", str(value or "").strip()).lower()

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def require_clean_email(value: str) -> str:
    email = clean_email(value)
    if not email or len(email) > 254 or not EMAIL_RE.match(email):
        raise HTTPException(400, "Enter a valid email address.")
    return email

def load_auth_email_map(user_ids: List[str]) -> Dict[str, str]:
    admin_api = getattr(getattr(supabase, "auth", None), "admin", None)
    if not admin_api or not hasattr(admin_api, "get_user_by_id"):
        return {}
    out: Dict[str, str] = {}
    for user_id in {str(uid or "").strip() for uid in (user_ids or []) if str(uid or "").strip()}:
        try:
            res = admin_api.get_user_by_id(user_id)
            user_obj = getattr(res, "user", None)
            if user_obj is None and isinstance(res, dict):
                user_obj = res.get("user") or res
            email = ""
            if isinstance(user_obj, dict):
                email = clean_email(user_obj.get("email", ""))
            elif user_obj is not None:
                email = clean_email(getattr(user_obj, "email", ""))
            elif hasattr(res, "email"):
                email = clean_email(getattr(res, "email", ""))
            if email:
                out[user_id] = email
        except Exception as e:
            print(f"[Auth Lookup Warning] {user_id}: {e}")
    return out

def auth_admin_api():
    auth_client = require_supabase_auth()
    return getattr(getattr(auth_client, "auth", None), "admin", None)

def auth_user_email(user_obj: Any) -> str:
    if isinstance(user_obj, dict):
        return clean_email(user_obj.get("email", ""))
    return clean_email(getattr(user_obj, "email", "") if user_obj is not None else "")

def auth_user_payload_dict(user_obj: Any) -> Dict[str, Any]:
    if isinstance(user_obj, dict):
        return dict(user_obj)
    return {
        "id": str(getattr(user_obj, "id", "") or ""),
        "email": auth_user_email(user_obj),
        "email_confirmed_at": getattr(user_obj, "email_confirmed_at", None),
    }

def load_auth_user_by_id(user_id: str) -> Dict[str, Any]:
    uid = str(user_id or "").strip()
    if not uid:
        raise HTTPException(400, "User ID is required.")
    admin_api = auth_admin_api()
    if admin_api and hasattr(admin_api, "get_user_by_id"):
        try:
            res = admin_api.get_user_by_id(uid)
            user_obj = getattr(res, "user", None)
            if user_obj is None and isinstance(res, dict):
                user_obj = res.get("user") or res
            payload = auth_user_payload_dict(user_obj)
            if payload.get("id") or payload.get("email"):
                payload["id"] = payload.get("id") or uid
                return payload
        except Exception as e:
            print(f"[Auth Lookup Warning] admin.get_user_by_id failed for {uid}: {e}")
    res = requests.get(f"{SUPABASE_URL.rstrip('/')}/auth/v1/admin/users/{uid}", headers=supabase_auth_headers(SUPABASE_KEY), timeout=20)
    if res.status_code >= 400:
        raise HTTPException(404, "Auth user not found.")
    data = res.json() or {}
    payload = auth_user_payload_dict(data.get("user") or data)
    payload["id"] = payload.get("id") or uid
    return payload

def delete_supabase_auth_user(user_id: str) -> None:
    uid = str(user_id or "").strip()
    if not uid:
        raise HTTPException(400, "User ID is required.")
    admin_api = auth_admin_api()
    if admin_api and hasattr(admin_api, "delete_user"):
        try:
            admin_api.delete_user(uid)
            return
        except Exception as e:
            print(f"[Auth Delete Warning] admin.delete_user failed for {uid}: {e}")
    res = requests.delete(f"{SUPABASE_URL.rstrip('/')}/auth/v1/admin/users/{uid}", headers=supabase_auth_headers(SUPABASE_KEY), timeout=20)
    if res.status_code >= 400 and res.status_code != 404:
        try:
            detail = res.json().get("msg") or res.json().get("error_description") or res.json().get("message")
        except Exception:
            detail = res.text or "Could not delete auth user."
        raise HTTPException(400, detail)

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

def send_email_message_detailed(to_email: str, subject: str, text_body: str) -> Tuple[bool, str]:
    recipient = clean_email(to_email)
    if not recipient:
        return False, "Recipient email is empty."
    if not SMTP_HOST:
        return False, "SMTP_HOST is not configured."
    if not SMTP_FROM_EMAIL:
        return False, "SMTP_FROM_EMAIL is not configured."
    if SMTP_USERNAME and not SMTP_PASSWORD:
        return False, "SMTP_PASSWORD is missing for the configured SMTP username."
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
        return True, ""
    except Exception as e:
        message = str(e or "").strip() or "Unknown SMTP error."
        print(f"[Notification Warning] Email send failed for {recipient}: {message}")
        return False, message

def review_notification_recipients() -> List[str]:
    seen: set = set()
    recipients: List[str] = []
    for email in REVIEW_NOTIFICATION_EMAILS:
        cleaned = clean_email(email)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        recipients.append(cleaned)
    return recipients

def send_review_notification(subject: str, lines: List[str]) -> int:
    return send_review_notification_report(subject, lines).get("sent_count", 0)

def send_review_notification_report(subject: str, lines: List[str]) -> Dict[str, Any]:
    global LAST_REVIEW_NOTIFICATION_RESULT
    sent = 0
    body = "\n".join([str(line or "") for line in lines if str(line or "").strip()])
    delivered_to: List[str] = []
    errors: List[Dict[str, str]] = []
    recipients = review_notification_recipients()
    for email in recipients:
        ok, error = send_email_message_detailed(email, subject, body)
        if ok:
            sent += 1
            delivered_to.append(email)
        else:
            errors.append({"email": email, "error": error})
    LAST_REVIEW_NOTIFICATION_RESULT = {
        "attempted_at": datetime.now(timezone.utc).isoformat(),
        "subject": subject,
        "sent_count": sent,
        "recipient_count": len(recipients),
        "delivered_to": delivered_to,
        "errors": errors,
    }
    return dict(LAST_REVIEW_NOTIFICATION_RESULT)

def review_notification_status_payload() -> Dict[str, Any]:
    recipients = review_notification_recipients()
    smtp_ready = notifications_enabled()
    return {
        "ok": True,
        "smtp_ready": smtp_ready,
        "smtp_host_set": bool(SMTP_HOST),
        "smtp_port": SMTP_PORT if SMTP_HOST else None,
        "smtp_username_set": bool(SMTP_USERNAME),
        "smtp_password_set": bool(SMTP_PASSWORD),
        "from_email": SMTP_FROM_EMAIL,
        "from_name": SMTP_FROM_NAME,
        "tls_enabled": SMTP_USE_TLS,
        "recipient_count": len(recipients),
        "recipients": recipients,
        "ready": smtp_ready and bool(recipients),
        "last_attempted_at": LAST_REVIEW_NOTIFICATION_RESULT.get("attempted_at"),
        "last_sent_count": LAST_REVIEW_NOTIFICATION_RESULT.get("sent_count", 0),
        "last_errors": LAST_REVIEW_NOTIFICATION_RESULT.get("errors", []),
    }

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
        f"Track in Atlantic Ordinate: {tracker_url}",
        "",
        "Thank you for shopping with us.",
        "Atlantic Ordinate",
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
        f"Track in Atlantic Ordinate: {tracker_url}",
        "",
        "Atlantic Ordinate",
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
            "X-Title": "Atlantic Ordinate"
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

SHOP_OWNERSHIP_WRITE_COLUMNS = ["listing_source", "ownership_status", "claimed_at"]
SHOP_OPTIONAL_WRITE_COLUMNS = [
    "business_type", "location_mode", "service_area", "profile_image_url", "phone_public", "website",
    *SHOP_REVIEW_WRITE_COLUMNS, *SHOP_TRUST_WRITE_COLUMNS, *SHOP_OWNERSHIP_WRITE_COLUMNS,
]
PRODUCT_OPTIONAL_WRITE_COLUMNS = [
    "price_amount", "stock_quantity", "product_slug", "variant_data", "variant_matrix",
    "attribute_data", "currency_code", "offering_type", "price_mode", "availability_mode",
    "duration_minutes", "capacity", "external_links",
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
            invalidate_shop_product_search_cache(shop_id)
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

def parse_image_ref_field(shop_id: str, raw: Any) -> List[str]:
    if raw in (None, ""):
        return []
    if isinstance(raw, list):
        return normalize_image_list(shop_id, raw)
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        return normalize_image_list(shop_id, parsed)
    except Exception:
        pass
    return normalize_image_list(shop_id, [part.strip() for part in re.split(r"[|\n;]+", text) if part.strip()])

def split_catalog_refs(raw: Any) -> List[str]:
    if raw in (None, ""):
        return []
    if isinstance(raw, list):
        out: List[str] = []
        for item in raw:
            out.extend(split_catalog_refs(item))
        return out
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return split_catalog_refs(parsed)
    except Exception:
        pass
    return [part.strip() for part in re.split(r"[|\n;]+", text) if part.strip()]

def normalize_catalog_asset_ref(value: str) -> str:
    ref = str(value or "").strip().strip("\"'").replace("\\", "/")
    ref = re.sub(r"^/+", "", ref)
    while ref.startswith("./"):
        ref = ref[2:]
    return ref.lower()

def save_image_bytes(shop_id: str, filename: str, data: bytes) -> str:
    ext = norm_ext(os.path.splitext(str(filename or "").lower())[1])
    if ext not in ALLOWED_IMG_EXTS:
        raise HTTPException(400, f"Unsupported image type: {ext}")
    if not data:
        raise HTTPException(400, "Image is empty")
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(400, f"Image {filename} exceeds the upload limit")
    name = f"{uuid.uuid4().hex}{norm_ext(ext)}"
    return upload_public_image(
        IMAGE_BUCKET,
        f"{shop_id}/{name}",
        data,
        image_content_type_for_ext(ext),
        "Failed to upload image. Ensure the 'product-images' bucket is created and public in Supabase.",
    )

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
    external_links = normalize_offering_external_links(row.get("external_links") or row.get("offering_links") or row.get("action_links"))
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
        "external_links": external_links,
        "offering_links": external_links,
        "action_links": external_links,
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
        external_links = normalize_offering_external_links(row.get("external_links") or row.get("offering_links") or row.get("action_links"))
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
            "external_links": external_links,
            "offering_links": external_links,
            "action_links": external_links,
            "images": imgs,
            "image_count": len(imgs),
            "product_views": view_map.get(key, 0),
            "avg_rating": round(sum(ratings) / len(ratings), 1) if ratings else 0,
            "review_count": len(ratings),
            "is_favourite": key in fav_set,
            "quality_flags": product_completeness_flags({**row, "images": imgs, "shop_id": shop_id, "attribute_data": normalized_attributes, "offering_type": offering_type}, shop_row.get("category", ""), shop_row.get("business_type", "")),
        }))
    return out

def load_product_metric_maps(rows: List[Dict[str, Any]]) -> Tuple[Dict[Tuple[str, str], List[int]], Dict[Tuple[str, str], int]]:
    pairs = {(str(r.get("shop_id", "")), str(r.get("product_id", ""))) for r in rows if r.get("shop_id") and r.get("product_id")}
    if not pairs or supabase is None:
        return {}, {}

    shop_ids = sorted({shop_id for shop_id, _ in pairs})
    product_ids = sorted({product_id for _, product_id in pairs})
    review_map: Dict[Tuple[str, str], List[int]] = {}
    view_map: Dict[Tuple[str, str], int] = {}

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

    return review_map, view_map

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
    pagination = pagination_meta(total, page, page_size)
    start = (pagination["page"] - 1) * pagination["page_size"]
    end = start + pagination["page_size"]
    return {
        "items": items[start:end],
        "pagination": pagination,
    }

def pagination_meta(total: int, page: int, page_size: int = PAGE_SIZE) -> Dict[str, Any]:
    total = max(int(total or 0), 0)
    page_size = max(1, page_size)
    pages = max((total + page_size - 1) // page_size, 1)
    page = max(1, min(page, pages))
    return {
        "page": page,
        "page_size": page_size,
        "pages": pages,
        "total": total,
        "has_prev": page > 1,
        "has_next": page < pages,
    }

def empty_public_page(page: int = 1, page_size: int = PAGE_SIZE) -> Dict[str, Any]:
    return paginate_list([], page, page_size)["pagination"]

def public_browse_error_payload(kind: str, exc: Exception, page: int = 1, page_size: int = PAGE_SIZE) -> Dict[str, Any]:
    print(f"[Public Browse Warning] {kind}: {exc}")
    message = "Public browsing data is temporarily unavailable. Please try again shortly."
    payload = {
        "ok": False,
        "message": message,
        "detail": message,
        "pagination": empty_public_page(page, page_size),
    }
    if kind == "businesses":
        payload["shops"] = []
        payload["businesses"] = []
    elif kind == "shop":
        payload["shop"] = {}
        payload["business"] = {}
        payload["products"] = []
        payload["offerings"] = []
        payload["stats"] = {}
        payload["suggested_questions"] = []
    elif kind == "search":
        payload["results"] = []
        payload["total"] = 0
    else:
        payload["products"] = []
        payload["offerings"] = []
    return alias_catalog_response(payload)

def sort_catalog_rows(rows: List[Dict[str, Any]], sort: str = "default") -> List[Dict[str, Any]]:
    items = list(rows or [])
    sort_key = str(sort or "default").strip().lower()
    if sort_key == "price-asc":
        items.sort(key=lambda row: get_row_price_amount(row) or 0)
    elif sort_key == "price-desc":
        items.sort(key=lambda row: get_row_price_amount(row) or 0, reverse=True)
    else:
        items.sort(key=lambda row: row.get("updated_at", ""), reverse=True)
    return items

def filter_catalog_rows_by_stock(rows: List[Dict[str, Any]], stock: str = "") -> List[Dict[str, Any]]:
    stock_key = str(stock or "").strip().lower()
    if stock_key not in {"in", "low", "out"}:
        return list(rows or [])
    return [row for row in (rows or []) if str(row.get("stock", "") or "").strip().lower() == stock_key]

def invalidate_shop_product_search_cache(shop_id: str) -> None:
    PRODUCT_SEARCH_INDEX_CACHE.pop(str(shop_id or "").strip(), None)

def get_shop_product_search_rows(shop_id: str) -> List[Dict[str, Any]]:
    shop_key = str(shop_id or "").strip()
    if not shop_key:
        return []
    now = time.time()
    cached = PRODUCT_SEARCH_INDEX_CACHE.get(shop_key)
    if cached and now - float(cached.get("ts", 0) or 0) < PRODUCT_SEARCH_INDEX_CACHE_SECONDS:
        return list(cached.get("rows") or [])
    try:
        rows = supabase.table("products").select(PRODUCT_SEARCH_INDEX_COLUMNS).eq("shop_id", shop_key).order("updated_at", desc=True).execute().data or []
    except Exception as e:
        if "external_links" not in str(e or ""):
            raise
        rows = supabase.table("products").select(PRODUCT_SEARCH_INDEX_COLUMNS_LEGACY).eq("shop_id", shop_key).order("updated_at", desc=True).execute().data or []
    PRODUCT_SEARCH_INDEX_CACHE[shop_key] = {"ts": now, "rows": rows}
    return list(rows)

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
            "external_links": normalize_offering_external_links(p.get("external_links")),
            "images": imgs
        })
    
    obj = {"shop": {k: shop_row.get(k, "") for k in ("name","profile_image_url","address","overview","website","hours","hours_structured","category","business_type","location_mode","service_area","country_code","country_name","timezone_name","region","city","postal_code","street_line1","street_line2","currency_code")},
           "products": p_serialized}
    obj["shop"]["phone"] = public_shop_phone_value(shop_row)
           
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

CHAT_CARD_FULL_THRESHOLD = 5
CHAT_CARD_MAX_RESULTS = 4

def chat_card_limit(total: int, max_cards: int = CHAT_CARD_MAX_RESULTS) -> int:
    total = max(0, int(total or 0))
    if total <= CHAT_CARD_FULL_THRESHOLD:
        return total
    return min(total, max_cards)

def take_chat_card_rows(rows: List[Dict[str, Any]], max_cards: int = CHAT_CARD_MAX_RESULTS) -> List[Dict[str, Any]]:
    return list(rows or [])[:chat_card_limit(len(rows or []), max_cards)]

def product_display_score(row: Dict[str, Any], shop: Optional[Dict[str, Any]] = None) -> Tuple[float, str, str]:
    shop = shop or {}
    score = 0.0
    if row_is_available_for_chat(row, shop):
        score += 2.0
    try:
        score += min(float(row.get("avg_rating") or 0), 5.0) * 0.45
    except Exception:
        pass
    try:
        score += min(float(row.get("review_count") or 0), 25.0) * 0.12
    except Exception:
        pass
    try:
        score += min(float(row.get("product_views") or 0), 200.0) * 0.025
    except Exception:
        pass
    if normalize_image_list(row.get("shop_id", shop.get("shop_id", "")), row.get("images", [])):
        score += 0.35
    if str(row.get("overview", "") or "").strip():
        score += 0.25
    if parse_price_value(row.get("price", "")) is not None or parse_price_amount(row.get("price_amount")) is not None:
        score += 0.2
    return (score, str(row.get("updated_at", "") or ""), str(row.get("name", "") or "").lower())

def ranked_display_products(rows: List[Dict[str, Any]], shop: Optional[Dict[str, Any]] = None, prefer_images: bool = False) -> List[Dict[str, Any]]:
    pool = list(rows or [])
    if prefer_images:
        with_images = [row for row in pool if normalize_image_list(row.get("shop_id", (shop or {}).get("shop_id", "")), row.get("images", []))]
        if with_images:
            pool = with_images
    return sorted(pool, key=lambda row: product_display_score(row, shop), reverse=True)

def select_chat_product_cards(rows: List[Dict[str, Any]], shop: Optional[Dict[str, Any]] = None, prefer_images: bool = False, max_cards: int = CHAT_CARD_MAX_RESULTS) -> List[Dict[str, Any]]:
    ranked = ranked_display_products(rows, shop, prefer_images=prefer_images)
    return ranked[:chat_card_limit(len(ranked), max_cards)]

def is_shop_profile_query(q: str, shop: Optional[Dict[str, Any]] = None) -> bool:
    if is_location_query(q) or is_hours_query(q) or is_contact_query(q):
        return False
    qn = norm_text(q)
    if not qn:
        return False
    shop_name = norm_text((shop or {}).get("name", ""))
    has_shop_name = bool(shop_name and shop_name in qn)
    triggers = [
        "tell me about this shop",
        "tell me about this business",
        "tell me about your shop",
        "tell me about your business",
        "tell me about the shop",
        "tell me about the business",
        "tell me about you",
        "about this shop",
        "about this business",
        "about your shop",
        "about your business",
        "what is this shop",
        "what is this business",
        "what kind of shop",
        "what kind of business",
        "who are you",
        "what do you do",
    ]
    if any(trigger in qn for trigger in triggers):
        return True
    return has_shop_name and any(trigger in qn for trigger in ["tell me about", "about", "who is", "what is"])

def product_card_query_intent(q: str, answer_text: str = "", matches: Optional[List[Dict[str, Any]]] = None, shop: Optional[Dict[str, Any]] = None) -> bool:
    if is_greeting(q) or is_location_query(q) or is_hours_query(q) or is_contact_query(q):
        return False
    if is_shop_profile_query(q, shop):
        return True
    if any(predicate(q) for predicate in [wants_all_images, wants_product_image, is_list_intent, is_stock_query, is_cheapest_query, is_budget_query, is_price_lookup_query, is_recommendation_query, is_rating_query]):
        return True
    qn = norm_text(q)
    product_terms = [
        "product",
        "products",
        "item",
        "items",
        "offering",
        "offerings",
        "service",
        "services",
        "buy",
        "sell",
        "available",
        "availability",
        "in stock",
        "do you have",
        "do they have",
        "looking for",
        "show me",
        "recommend",
    ]
    if any(term in qn for term in product_terms):
        return True
    if matches:
        answer_n = norm_text(answer_text or "")
        for row in matches[:4]:
            name_n = norm_text(row.get("name", ""))
            if name_n and (name_n in qn or name_n in answer_n):
                return True
    return False

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

def active_market_country_code() -> str:
    return BUSINESS_COUNTRY_LOCK_CODE if BUSINESS_COUNTRY_LOCK_ENABLED else ""

def active_market_country_meta() -> Dict[str, Any]:
    return country_meta(active_market_country_code())

def active_market_country_name() -> str:
    code = active_market_country_code()
    meta = active_market_country_meta()
    return meta.get("name", "Canada" if code == "CA" else code)

def enforce_public_country_code(country_code: str) -> str:
    return active_market_country_code() or clean_code(country_code)

def infer_country_code(country_code: str = "", country_name: str = "") -> str:
    code = clean_code(country_code)
    if code:
        return code
    name = re.sub(r"\s+", " ", str(country_name or "")).strip().lower()
    if name:
        for meta_code, meta in COUNTRY_META.items():
            if name == str(meta.get("name", "")).strip().lower():
                return meta_code
    if BUSINESS_COUNTRY_LOCK_ENABLED:
        return BUSINESS_COUNTRY_LOCK_CODE
    return ""

def shop_is_publicly_listable(row: Dict[str, Any]) -> bool:
    shop = normalize_shop_record(row or {})
    return shop.get("listing_status", LISTING_STATUS_DRAFT) in PUBLIC_LISTING_STATUSES

def shop_review_requirements(row: Dict[str, Any], stats: Optional[Dict[str, Any]] = None) -> List[str]:
    shop = normalize_shop_record(row or {})
    issues: List[str] = []
    quality_flags = shop_completeness_flags(shop, stats or {})
    if quality_flags:
        issues.extend([f"Complete: {flag}" for flag in quality_flags])
    if not shop.get("phone", "").strip():
        issues.append("Add a business phone number for private review use.")
    if not shop.get("owner_contact_name", "").strip():
        issues.append("Add the owner or primary contact name.")
    if not shop.get("verification_method", "").strip():
        issues.append("Choose how the business should be verified.")
    if len(shop.get("verification_evidence", "").strip()) < 8:
        issues.append("Add a short verification note with a website, registry, licence, or other proof reference.")
    deduped: List[str] = []
    seen: set = set()
    for item in issues:
        key = str(item or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(str(item).strip())
    return deduped

def shop_matches_market_country(row: Dict[str, Any]) -> bool:
    locked_country = active_market_country_code()
    if not locked_country:
        return True
    return normalize_shop_record(row or {}).get("country_code", "") == locked_country

def ensure_market_shop_access(row: Dict[str, Any]) -> Dict[str, Any]:
    shop = normalize_shop_record(row or {})
    locked_country = active_market_country_code()
    if locked_country and shop.get("country_code", "") != locked_country:
        raise HTTPException(404, "Business not found")
    if not shop_is_publicly_listable(shop):
        raise HTTPException(404, "Business not found")
    return shop

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

def leaflet_vendor_available() -> bool:
    base_dir = os.path.join(VENDOR_DIR, "leaflet")
    return all(os.path.isfile(os.path.join(base_dir, asset)) for asset in LEAFLET_REQUIRED_ASSETS)

def mapbox_geocoding_request(query: str, *, country_code: str = "", limit: int = 1, autocomplete: bool = False, types: str = "address,place,postcode") -> Tuple[str, Dict[str, Any]]:
    params = {
        "access_token": MAPBOX_TOKEN,
        "limit": max(1, int(limit or 1)),
        "autocomplete": "true" if autocomplete else "false",
        "types": types,
    }
    cleaned_country = clean_code(country_code)
    if cleaned_country:
        params["country"] = cleaned_country.lower()
    return MAPBOX_GEOCODING_URL.format(query=requests.utils.quote(str(query or "").strip())), params

def has_valid_coordinates(row: Dict[str, Any]) -> bool:
    try:
        lat = float(row.get("latitude"))
        lng = float(row.get("longitude"))
        return math.isfinite(lat) and math.isfinite(lng)
    except Exception:
        return False

def persist_shop_coordinates(shop_id: str, geo: Dict[str, Optional[float]], warning_label: str = "Map Backfill") -> bool:
    if not shop_id or geo.get("latitude") is None or geo.get("longitude") is None:
        return False
    try:
        supabase.table("shops").update({
            "latitude": geo["latitude"],
            "longitude": geo["longitude"],
        }).eq("shop_id", shop_id).execute()
        return True
    except Exception as e:
        print(f"[{warning_label} Warning] {shop_id}: {e}")
        return False

def geocode_structured_address(data: Dict[str, Any]) -> Dict[str, Optional[float]]:
    if not MAPBOX_TOKEN:
        return {"latitude": None, "longitude": None}
    query = (data.get("formatted_address") or build_formatted_address(data) or data.get("address") or "").strip()
    if not query:
        return {"latitude": None, "longitude": None}
    url, params = mapbox_geocoding_request(query, country_code=data.get("country_code", ""))
    try:
        res = requests.get(url, params=params, timeout=8)
        res.raise_for_status()
        feature = ((res.json() or {}).get("features") or [None])[0]
        center = (feature or {}).get("center") or [None, None]
        return {"latitude": center[1], "longitude": center[0]}
    except Exception as e:
        print(f"[Map Geocode Warning] {e}")
        return {"latitude": None, "longitude": None}

def fill_missing_shop_coordinates(row: Dict[str, Any], *, persist: bool = False, warning_label: str = "Map Backfill") -> Tuple[Dict[str, Any], bool]:
    out = normalize_shop_record(row or {})
    if not shop_has_mappable_address(out):
        out["latitude"] = None
        out["longitude"] = None
        return out, False
    if has_valid_coordinates(out):
        return out, False
    geo = geocode_structured_address(out)
    if geo.get("latitude") is None or geo.get("longitude") is None:
        return out, False
    out["latitude"] = geo["latitude"]
    out["longitude"] = geo["longitude"]
    persisted = persist_shop_coordinates(out.get("shop_id", ""), geo, warning_label) if persist else False
    return out, persisted

def ensure_shop_coordinates(row: Dict[str, Any]) -> Dict[str, Any]:
    out, _ = fill_missing_shop_coordinates(row, persist=True, warning_label="Map Backfill")
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
    out["country_code"] = infer_country_code(out.get("country_code", ""), out.get("country_name", ""))
    out["country_name"] = (out.get("country_name") or country_meta(out.get("country_code", "")).get("name", "")).strip()
    out["currency_code"] = clean_currency(out.get("currency_code") or currency_for_country(out.get("country_code", "")))
    out["timezone_name"] = (out.get("timezone_name") or timezone_for_country(out.get("country_code", ""))).strip()
    out["hours_structured"] = parse_hours_structured(out.get("hours_structured"))
    if not out["hours_structured"]:
        out["hours_structured"] = parse_hours_summary_text(out.get("hours"))
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
    out["phone"] = re.sub(r"\s+", " ", str(out.get("phone") or "")).strip()[:40]
    out["phone_public"] = normalize_bool_flag(out.get("phone_public"), default=False)
    out["website"] = normalize_public_url(out.get("website", ""))
    out["whatsapp"] = re.sub(r"\s+", " ", str(out.get("whatsapp") or "")).strip()[:40]
    out["supports_pickup"] = bool(out.get("supports_pickup", True))
    out["supports_delivery"] = bool(out.get("supports_delivery", False))
    out["supports_walk_in"] = bool(out.get("supports_walk_in", True))
    status_default = LISTING_STATUS_VERIFIED if "listing_status" not in out else LISTING_STATUS_DRAFT
    out["listing_status"] = normalize_listing_status(out.get("listing_status"), default=status_default)
    out["owner_contact_name"] = normalize_owner_contact_name(out.get("owner_contact_name", ""))
    out["listing_source"] = normalize_listing_source(out.get("listing_source", ""))
    out["ownership_status"] = normalize_ownership_status(
        out.get("ownership_status", ""),
        listing_source=out.get("listing_source", ""),
        owner_contact_name=out.get("owner_contact_name", ""),
    )
    out["claimed_at"] = out.get("claimed_at") or None
    out["is_platform_managed"] = out["ownership_status"] == OWNERSHIP_STATUS_PLATFORM_MANAGED
    out["is_claimed"] = out["ownership_status"] == OWNERSHIP_STATUS_CLAIMED
    out["verification_method"] = normalize_verification_method(out.get("verification_method", ""))
    out["verification_evidence"] = normalize_verification_evidence(out.get("verification_evidence", ""))
    out["verification_submitted_at"] = out.get("verification_submitted_at") or None
    out["verified_at"] = out.get("verified_at") or None
    out["verification_rejection_reason"] = re.sub(r"\s+", " ", str(out.get("verification_rejection_reason") or "")).strip()[:500]
    out["is_publicly_listed"] = out["listing_status"] in PUBLIC_LISTING_STATUSES
    trust_flags = out.get("trust_flags", [])
    if isinstance(trust_flags, str):
        try:
            trust_flags = json.loads(trust_flags)
        except Exception:
            trust_flags = [trust_flags]
    if not isinstance(trust_flags, list):
        trust_flags = []
    out["trust_flags"] = [re.sub(r"\s+", " ", str(item or "")).strip()[:160] for item in trust_flags if str(item or "").strip()]
    try:
        out["risk_score"] = int(out.get("risk_score") or 0)
    except Exception:
        out["risk_score"] = 0
    out["risk_level"] = risk_level_for_score(out.get("risk_level") or out["risk_score"])
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

def public_shop_phone_value(row: Dict[str, Any]) -> str:
    shop = normalize_shop_record(row or {})
    return shop.get("phone", "") if shop.get("phone_public") else ""

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
    shop["phone"] = public_shop_phone_value(shop)
    if not shop_has_mappable_address(shop):
        shop["latitude"] = None
        shop["longitude"] = None
    return shop

def public_shop_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    safe = map_safe_shop_record({key: row.get(key) for key in PUBLIC_SHOP_FIELDS})
    for internal_key in (
        "listing_status",
        "listing_source",
        "ownership_status",
        "claimed_at",
        "is_platform_managed",
        "is_claimed",
        "owner_contact_name",
        "verification_method",
        "verification_evidence",
        "verification_submitted_at",
        "verified_at",
        "verification_rejection_reason",
        "trust_flags",
        "risk_score",
        "risk_level",
        "is_publicly_listed",
    ):
        safe.pop(internal_key, None)
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
    external_links = normalize_offering_external_links(out.get("external_links") or out.get("offering_links") or out.get("action_links"))
    out["external_links"] = external_links
    out["offering_links"] = external_links
    out["action_links"] = external_links
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

def resolve_product_by_ref(shop_id: str, product_ref: str) -> Dict[str, Any]:
    ref = str(product_ref or "").strip()
    if not ref:
        raise HTTPException(404, "Offering not found")
    rows = supabase.table("products").select("*").eq("shop_id", shop_id).eq("product_id", ref).limit(1).execute().data or []
    if not rows:
        rows = supabase.table("products").select("*").eq("shop_id", shop_id).eq("product_slug", ref).limit(1).execute().data or []
    if not rows:
        raise HTTPException(404, "Offering not found")
    return rows[0]

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

def parse_hours_summary_text(value: Any) -> List[Dict[str, Any]]:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        return []
    parsed: Dict[str, Dict[str, Any]] = {}
    day_token = "(Mon|Tue|Wed|Thu|Fri|Sat|Sun)"
    pattern = re.compile(
        rf"^{day_token}(?:-({day_token[1:-1]}))?\s+(Closed|\d{{2}}:\d{{2}}\s*-\s*\d{{2}}:\d{{2}})$",
        re.IGNORECASE,
    )
    for chunk in [part.strip() for part in text.split(",") if part.strip()]:
        match = pattern.fullmatch(chunk)
        if not match:
            return []
        start_day = match.group(1).lower()[:3]
        end_day = (match.group(2) or match.group(1)).lower()[:3]
        if start_day not in DAY_ORDER or end_day not in DAY_ORDER:
            return []
        start_idx = DAY_ORDER.index(start_day)
        end_idx = DAY_ORDER.index(end_day)
        if end_idx < start_idx:
            return []
        slot = match.group(3)
        closed = slot.lower() == "closed"
        start = ""
        end = ""
        if not closed:
            times = re.findall(r"\d{2}:\d{2}", slot)
            if len(times) != 2 or times[0] >= times[1]:
                return []
            start, end = times
        for day in DAY_ORDER[start_idx:end_idx + 1]:
            if day in parsed:
                return []
            parsed[day] = {
                "day": day,
                "closed": closed,
                "start": "" if closed else start,
                "end": "" if closed else end,
            }
    return [parsed[day] for day in DAY_ORDER if day in parsed]

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
            flags.append("Products missing images")
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
    if not data.get("phone", "").strip():
        raise HTTPException(400, "Enter a business phone number for private review use")
    data["business_type"] = normalize_business_type(data.get("business_type", ""), data.get("category", ""))
    data["location_mode"] = normalize_location_mode(data.get("location_mode", ""), data.get("business_type", ""), data.get("category", ""))
    if BUSINESS_COUNTRY_LOCK_ENABLED:
        data["country_code"] = BUSINESS_COUNTRY_LOCK_CODE
        data["country_name"] = country_meta(BUSINESS_COUNTRY_LOCK_CODE).get("name", "Canada")
        data["currency_code"] = currency_for_country(BUSINESS_COUNTRY_LOCK_CODE)
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
    data["hours_structured"] = parse_hours_structured(data.get("hours_structured")) or parse_hours_summary_text(data.get("hours"))
    if not data.get("hours_structured"):
        raise HTTPException(400, "Set the business working days and hours")
    if not any(not slot.get("closed") for slot in data["hours_structured"]):
        raise HTTPException(400, "Mark at least one working day as open")
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
    data["owner_contact_name"] = normalize_owner_contact_name(data.get("owner_contact_name", ""))
    data["verification_method"] = normalize_verification_method(data.get("verification_method", ""))
    data["verification_evidence"] = normalize_verification_evidence(data.get("verification_evidence", ""))
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
    external_links = normalize_offering_external_links(product.external_links, strict=True)
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
        "external_links": external_links,
        "images": dedup(product.images or []),
        "updated_at": "now()",
    }

def product_strict_columns_for_data(data: Dict[str, Any]) -> List[str]:
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
    if data.get("external_links"):
        strict_cols.append("external_links")
    return strict_cols

def normalize_catalog_header(value: str) -> str:
    text = str(value or "").strip().lstrip("\ufeff").lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")

def catalog_value(row: Dict[str, str], *keys: str) -> str:
    for key in keys:
        normalized = normalize_catalog_header(key)
        if normalized in row and str(row.get(normalized) or "").strip():
            return str(row.get(normalized) or "").strip()
    return ""

def parse_catalog_int(value: Any) -> Optional[int]:
    text = str(value if value is not None else "").strip()
    if not text:
        return None
    try:
        number = int(float(text))
    except Exception:
        raise ValueError("must be a whole number")
    if number < 0:
        raise ValueError("cannot be negative")
    return number

def parse_catalog_json(value: str, fallback: Any) -> Any:
    text = str(value or "").strip()
    if not text:
        return fallback
    return json.loads(text)

def split_catalog_assignments(value: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for entry in split_catalog_refs(value):
        if ">" not in entry:
            continue
        target, ref = entry.split(">", 1)
        target = target.strip()
        ref = ref.strip()
        if target and ref:
            out.append((target, ref))
    return out

def catalog_key(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())

def catalog_pair_key(value: str) -> str:
    text = str(value or "").strip()
    m = re.match(r"^(.+?)(?:=|:)(.+)$", text)
    if not m:
        return catalog_key(text)
    return f"{catalog_key(m.group(1))}={catalog_key(m.group(2))}"

def catalog_combo_signature(selections: Dict[str, str]) -> str:
    return "|".join(sorted(f"{catalog_key(group)}={catalog_key(label)}" for group, label in (selections or {}).items() if group and label))

def catalog_combo_target_signature(target: str) -> str:
    parts = [catalog_pair_key(part) for part in str(target or "").split("+") if "=" in catalog_pair_key(part)]
    return "|".join(sorted(parts)) if parts else catalog_key(target)

def parse_catalog_combo_target(target: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in str(target or "").split("+"):
        m = re.match(r"^(.+?)(?:=|:)(.+)$", part.strip())
        if m:
            group = re.sub(r"\s+", " ", m.group(1)).strip()[:40]
            label = re.sub(r"\s+", " ", m.group(2)).strip()[:60]
            if group and label:
                out[group] = label
    return out

def parse_simple_variant_options(raw: str) -> List[Dict[str, Any]]:
    text = str(raw or "").strip()
    if not text:
        return []
    parts = [part.strip() for part in re.split(r"\s*;\s*", text) if part.strip()]
    if len(parts) == 1:
        parts = [part.strip() for part in re.split(r"\s+\|\s+(?=[^:=|]{1,40}\s*[:=])", text) if part.strip()]
    out: List[Dict[str, Any]] = []
    for part in parts:
        m = re.match(r"^(.+?)(?:=|:)(.+)$", part)
        if m:
            group = re.sub(r"\s+", " ", m.group(1)).strip()[:40] or "Option"
            values = [v.strip() for v in re.split(r"\s*(?:,|\|)\s*", m.group(2)) if v.strip()]
        else:
            group = "Option"
            values = [v.strip() for v in re.split(r"\s*,\s*", part) if v.strip()]
        for raw_label in values:
            price_delta = 0.0
            label = raw_label
            dm = re.search(r"\(([+-]\s*\d+(?:\.\d+)?)\)\s*$", raw_label)
            if dm:
                try:
                    price_delta = round(float(dm.group(1).replace(" ", "")), 2)
                    label = raw_label[:dm.start()].strip()
                except Exception:
                    pass
            if label:
                out.append({"group": group, "label": label[:60], "price_delta": price_delta, "images": []})
    return out

def xlsx_col_index(cell_ref: str) -> int:
    letters = "".join(ch for ch in str(cell_ref or "") if ch.isalpha()).upper()
    if not letters:
        return 0
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - 64)
    return max(0, idx - 1)

def xlsx_node_text(node: Optional[ET.Element]) -> str:
    if node is None:
        return ""
    return "".join(node.itertext())

def xlsx_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    try:
        root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    except Exception:
        raise HTTPException(400, "Could not read shared strings from the Excel file.")
    out: List[str] = []
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    for item in root.findall(f".//{ns}si"):
        out.append(xlsx_node_text(item))
    return out

def xlsx_first_sheet_path(zf: zipfile.ZipFile) -> str:
    names = set(zf.namelist())
    if "xl/worksheets/sheet1.xml" in names:
        return "xl/worksheets/sheet1.xml"
    sheets = sorted(name for name in names if name.startswith("xl/worksheets/") and name.endswith(".xml"))
    if not sheets:
        raise HTTPException(400, "The Excel file does not contain a worksheet.")
    return sheets[0]

def xlsx_bytes_to_csv_text(data: bytes) -> str:
    try:
        zf = zipfile.ZipFile(BytesIO(data))
    except zipfile.BadZipFile:
        raise HTTPException(400, "The Excel file is not a valid .xlsx workbook.")
    with zf:
        shared = xlsx_shared_strings(zf)
        sheet_path = xlsx_first_sheet_path(zf)
        try:
            root = ET.fromstring(zf.read(sheet_path))
        except Exception:
            raise HTTPException(400, "Could not read the first worksheet from the Excel file.")
        ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
        rows: List[List[str]] = []
        max_width = 0
        for row_node in root.findall(f".//{ns}sheetData/{ns}row"):
            values: Dict[int, str] = {}
            next_idx = 0
            for cell in row_node.findall(f"{ns}c"):
                idx = xlsx_col_index(cell.attrib.get("r", "")) if cell.attrib.get("r") else next_idx
                next_idx = idx + 1
                cell_type = cell.attrib.get("t", "")
                value = ""
                if cell_type == "s":
                    raw = xlsx_node_text(cell.find(f"{ns}v")).strip()
                    if raw:
                        try:
                            value = shared[int(raw)]
                        except Exception:
                            value = raw
                elif cell_type == "inlineStr":
                    value = xlsx_node_text(cell.find(f"{ns}is"))
                else:
                    value = xlsx_node_text(cell.find(f"{ns}v"))
                values[idx] = value
            if values:
                width = max(values.keys()) + 1
                max_width = max(max_width, width)
                rows.append([values.get(i, "") for i in range(width)])
        if not rows:
            raise HTTPException(400, "The Excel worksheet has no rows.")
        buffer = StringIO()
        writer = csv.writer(buffer)
        for row in rows:
            writer.writerow(row + [""] * max(0, max_width - len(row)))
        return buffer.getvalue()

def build_catalog_asset_map(data: bytes, filename: str) -> Tuple[str, Dict[str, Tuple[str, bytes]]]:
    name = str(filename or "").lower()
    if name.endswith(".csv"):
        return data.decode("utf-8-sig"), {}
    if name.endswith(".xlsx"):
        return xlsx_bytes_to_csv_text(data), {}
    if not name.endswith(".zip"):
        raise HTTPException(400, "Upload a .zip catalog package, .csv file, or .xlsx file.")
    try:
        zf = zipfile.ZipFile(BytesIO(data))
    except zipfile.BadZipFile:
        raise HTTPException(400, "The catalog package is not a valid ZIP file.")
    with zf:
        files = [info for info in zf.infolist() if not info.is_dir()]
        total_size = sum(max(0, info.file_size) for info in files)
        if total_size > MAX_CATALOG_PACKAGE_BYTES * 3:
            raise HTTPException(400, "The catalog package is too large after extraction.")
        tabular_candidates = [
            info for info in files
            if os.path.basename(info.filename).lower() in {"products.csv", "offerings.csv", "catalog.csv", "products.xlsx", "offerings.xlsx", "catalog.xlsx"}
        ]
        if not tabular_candidates:
            tabular_candidates = [info for info in files if info.filename.lower().endswith((".csv", ".xlsx"))]
        if not tabular_candidates:
            raise HTTPException(400, "Add a products.csv or products.xlsx file to the ZIP package.")
        tabular_info = tabular_candidates[0]
        tabular_blob = zf.read(tabular_info)
        csv_text = xlsx_bytes_to_csv_text(tabular_blob) if tabular_info.filename.lower().endswith(".xlsx") else tabular_blob.decode("utf-8-sig")
        assets: Dict[str, Tuple[str, bytes]] = {}
        for info in files:
            ext = norm_ext(os.path.splitext(info.filename.lower())[1])
            if ext not in ALLOWED_IMG_EXTS:
                continue
            if info.file_size > MAX_UPLOAD_BYTES:
                continue
            blob = zf.read(info)
            parts = [part for part in normalize_catalog_asset_ref(info.filename).split("/") if part]
            keys = {"/".join(parts[i:]) for i in range(len(parts))}
            keys.add(os.path.basename(info.filename).lower())
            for key in keys:
                if key and key not in assets:
                    assets[key] = (info.filename, blob)
        return csv_text, assets

def catalog_upload_image_refs(shop_id: str, refs: Any, assets: Dict[str, Tuple[str, bytes]], uploaded: Dict[str, str], row_number: int, errors: List[str], context: str, save_assets: bool = True) -> List[str]:
    out: List[str] = []
    for ref in split_catalog_refs(refs):
        if re.match(r"^https?://", ref, re.I) or str(ref).startswith("/shops/"):
            out.append(normalize_image_ref(shop_id, ref))
            continue
        key = normalize_catalog_asset_ref(ref)
        asset = assets.get(key) or assets.get(os.path.basename(key))
        if not asset:
            errors.append(f"Row {row_number}: {context} image '{ref}' was not found in the ZIP images folder.")
            continue
        asset_name, blob = asset
        cache_key = normalize_catalog_asset_ref(asset_name)
        if cache_key not in uploaded:
            uploaded[cache_key] = save_image_bytes(shop_id, asset_name, blob) if save_assets else f"local:{asset_name}"
        out.append(uploaded[cache_key])
    return dedup(out)

def catalog_apply_variant_images(shop_id: str, row: Dict[str, str], row_number: int, variant_data: List[Dict[str, Any]], assets: Dict[str, Tuple[str, bytes]], uploaded: Dict[str, str], errors: List[str], save_assets: bool = True) -> List[Dict[str, Any]]:
    out = [{**item, "images": catalog_upload_image_refs(shop_id, item.get("images", []), assets, uploaded, row_number, errors, f"variant {item.get('label') or idx + 1}", save_assets)} for idx, item in enumerate(variant_data or [])]
    option_index: Dict[str, int] = {}
    for idx, item in enumerate(out):
        group = catalog_key(item.get("group") or "Option")
        label = catalog_key(item.get("label") or "")
        if label:
            option_index[f"{group}={label}"] = idx
            option_index[label] = idx
    assignments = []
    for key in ("variant_images", "variant_image_files", "variant_option_images", "variant_option_image_files", "option_images", "option_image_files"):
        assignments.extend(split_catalog_assignments(catalog_value(row, key)))
    for target, ref in assignments:
        idx = option_index.get(catalog_pair_key(target))
        if idx is None:
            errors.append(f"Row {row_number}: variant image target '{target}' does not match the variant options.")
            continue
        out[idx]["images"] = dedup(list(out[idx].get("images") or []) + catalog_upload_image_refs(shop_id, [ref], assets, uploaded, row_number, errors, f"variant {target}", save_assets))
    return out

def catalog_apply_combo_images(shop_id: str, row: Dict[str, str], row_number: int, matrix: List[Dict[str, Any]], assets: Dict[str, Tuple[str, bytes]], uploaded: Dict[str, str], errors: List[str], save_assets: bool = True) -> List[Dict[str, Any]]:
    out = [{**item, "images": catalog_upload_image_refs(shop_id, item.get("images", []), assets, uploaded, row_number, errors, f"combination {idx + 1}", save_assets)} for idx, item in enumerate(matrix or [])]
    combo_index: Dict[str, int] = {}
    for idx, item in enumerate(out):
        sig = catalog_combo_signature(item.get("selections") or {})
        if sig:
            combo_index[sig] = idx
        if item.get("selection_key"):
            combo_index[catalog_key(item.get("selection_key"))] = idx
    assignments = []
    for key in ("combination_images", "combination_image_files", "variant_combo_images", "variant_combo_image_files", "combo_images", "combo_image_files"):
        assignments.extend(split_catalog_assignments(catalog_value(row, key)))
    for target, ref in assignments:
        sig = catalog_combo_target_signature(target)
        idx = combo_index.get(sig)
        if idx is None:
            selections = parse_catalog_combo_target(target)
            if len(selections) < 2:
                errors.append(f"Row {row_number}: combination image target '{target}' must look like Size=Large+Color=Red.")
                continue
            out.append({"selections": selections, "price_delta": 0, "images": []})
            idx = len(out) - 1
            combo_index[catalog_combo_signature(selections)] = idx
        out[idx]["images"] = dedup(list(out[idx].get("images") or []) + catalog_upload_image_refs(shop_id, [ref], assets, uploaded, row_number, errors, f"combination {target}", save_assets))
    return out

def build_product_from_catalog_row(shop_id: str, shop: Dict[str, Any], row: Dict[str, str], row_number: int, assets: Dict[str, Tuple[str, bytes]], uploaded: Dict[str, str], errors: List[str], save_assets: bool = True) -> Tuple[str, Product]:
    name = catalog_value(row, "name", "product_name", "offering_name", "title")
    if not name:
        raise ValueError("name is required")
    business_type = normalize_business_type(shop.get("business_type", ""), shop.get("category", ""))
    offering_type = normalize_offering_type(catalog_value(row, "offering_type", "type"), business_type, shop.get("category", ""))
    raw_pid = catalog_value(row, "product_id", "offering_id", "sku", "id")
    pid = slug(raw_pid, 60) if raw_pid else gen_product_id(name)
    price_raw = catalog_value(row, "price", "price_amount", "amount")
    stock_qty = parse_catalog_int(catalog_value(row, "stock_quantity", "quantity", "qty", "inventory")) if catalog_value(row, "stock_quantity", "quantity", "qty", "inventory") else None
    duration = parse_catalog_int(catalog_value(row, "duration_minutes", "duration")) if catalog_value(row, "duration_minutes", "duration") else None
    capacity = parse_catalog_int(catalog_value(row, "capacity", "seats")) if catalog_value(row, "capacity", "seats") else None
    attr_raw = parse_catalog_json(catalog_value(row, "attribute_data_json", "category_details_json"), {})
    if not isinstance(attr_raw, dict):
        raise ValueError("attribute_data_json must be a JSON object")
    external_links_raw = parse_catalog_json(catalog_value(row, "external_links_json", "offering_links_json", "action_links_json"), [])
    if not isinstance(external_links_raw, list):
        raise ValueError("external_links_json must be a JSON array")
    simple_link_url = catalog_value(row, "external_url", "external_link", "website_url", "booking_url", "product_url")
    if simple_link_url:
        external_links_raw.append({
            "label": catalog_value(row, "external_label", "external_link_label", "website_label", "booking_label", "product_link_label") or "Website",
            "url": simple_link_url,
        })
    schema = offering_attribute_schema(business_type, shop.get("category", ""), offering_type)
    for key, label in schema:
        value = catalog_value(row, key, label)
        if value:
            attr_raw[key] = value
    variant_data_raw = parse_catalog_json(catalog_value(row, "variant_data_json"), None)
    if variant_data_raw is None:
        variant_data_raw = parse_simple_variant_options(catalog_value(row, "variants", "variant_options", "options"))
    if not isinstance(variant_data_raw, list):
        raise ValueError("variant_data_json must be a JSON array")
    variant_data = catalog_apply_variant_images(shop_id, row, row_number, variant_data_raw, assets, uploaded, errors, save_assets)
    matrix_raw = parse_catalog_json(catalog_value(row, "variant_matrix_json", "combination_data_json"), [])
    if not isinstance(matrix_raw, list):
        raise ValueError("variant_matrix_json must be a JSON array")
    variant_matrix = catalog_apply_combo_images(shop_id, row, row_number, matrix_raw, assets, uploaded, errors, save_assets)
    product_images = []
    for key in ("image_urls", "images", "image_files", "photos", "photo_files"):
        product_images.extend(catalog_upload_image_refs(shop_id, catalog_value(row, key), assets, uploaded, row_number, errors, "product", save_assets))
    product = Product(
        product_id=pid,
        offering_id=pid,
        name=name,
        overview=catalog_value(row, "overview", "description", "details"),
        price=price_raw,
        price_amount=parse_price_amount(price_raw),
        currency_code=catalog_value(row, "currency_code", "currency") or shop.get("currency_code", ""),
        offering_type=offering_type,
        price_mode=catalog_value(row, "price_mode", "pricing") or ("inquiry" if not price_raw else "fixed"),
        availability_mode=catalog_value(row, "availability_mode", "availability"),
        stock=catalog_value(row, "stock", "stock_status") or "in",
        stock_quantity=stock_qty,
        duration_minutes=duration,
        capacity=capacity,
        variants=catalog_value(row, "variants", "variant_options", "options"),
        variant_data=variant_data,
        variant_matrix=variant_matrix,
        attribute_data=attr_raw,
        external_links=normalize_offering_external_links(external_links_raw, strict=True),
        images=dedup(product_images),
    )
    return pid, product

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

RATING_WORD_VALUES = {
    "one": 1.0,
    "two": 2.0,
    "three": 3.0,
    "four": 4.0,
    "five": 5.0,
}

def is_rating_query(q: str) -> bool:
    qn = norm_text(q)
    return any(
        token in qn
        for token in [
            "rating",
            "ratings",
            "review",
            "reviews",
            "star",
            "stars",
            "top rated",
            "highest rated",
            "best rated",
        ]
    )

def extract_rating_value(q: str) -> Optional[float]:
    qn = norm_text(q)
    patterns = [
        r"\b([1-5](?:\.\d)?)\s*[- ]?stars?\b",
        r"\brated\s*([1-5](?:\.\d)?)\b",
        r"\brating(?:s)?\s*(?:of|at)?\s*([1-5](?:\.\d)?)\b",
        r"\b(one|two|three|four|five)\s*[- ]?stars?\b",
        r"\brated\s*(one|two|three|four|five)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, qn)
        if not match:
            continue
        raw = str(match.group(1) or "").strip().lower()
        if raw in RATING_WORD_VALUES:
            return RATING_WORD_VALUES[raw]
        try:
            value = float(raw)
        except Exception:
            continue
        if 1 <= value <= 5:
            return value
    return None

def rating_query_mode(q: str) -> str:
    qn = norm_text(q)
    if any(token in qn for token in ["top rated", "highest rated", "best rated"]):
        return "top"
    if any(token in qn for token in ["at least", "or higher", "and above", "or above", "minimum", "min rating"]):
        return "min"
    if any(token in qn for token in ["above", "more than"]):
        return "above"
    if any(token in qn for token in ["below", "less than", "under"]):
        return "max"
    if extract_rating_value(q) is not None:
        return "exact"
    return "top"

def query_prefers_product_offerings(q: str) -> bool:
    qn = norm_text(q)
    return any(token in qn for token in ["product", "products", "item", "items"])

def query_prefers_service_offerings(q: str) -> bool:
    qn = norm_text(q)
    return any(token in qn for token in ["service", "services", "appointment", "appointments"])

def rating_query_subject(q: str) -> str:
    qn = norm_text(q)
    qn = re.sub(r"\b(top rated|highest rated|best rated)\b", " ", qn)
    qn = re.sub(r"\b([1-5](?:\.\d+)?|one|two|three|four|five)\s*[- ]?stars?\b", " ", qn)
    qn = re.sub(r"\brated\s*([1-5](?:\.\d+)?|one|two|three|four|five)\b", " ", qn)
    qn = re.sub(r"\brating(?:s)?\b", " ", qn)
    qn = re.sub(r"\breview(?:s)?\b", " ", qn)
    qn = re.sub(r"\b(show|find|me|with|that|have|has|all|any|only|products?|items?|offerings?|services?|please)\b", " ", qn)
    qn = re.sub(r"\s+", " ", qn).strip()
    return qn

def product_rating_label(row: Dict[str, Any]) -> str:
    avg = float(row.get("avg_rating", 0) or 0)
    count = int(row.get("review_count", 0) or 0)
    if count <= 0 or avg <= 0:
        return "No public rating yet"
    review_word = "review" if count == 1 else "reviews"
    return f"{avg:.1f} stars from {count} {review_word}"

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
        "external_links": normalize_offering_external_links(row.get("external_links") or row.get("offering_links") or row.get("action_links")),
        "avg_rating": row.get("avg_rating", 0),
        "review_count": row.get("review_count", 0),
        "product_views": row.get("product_views", 0),
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

    top = take_chat_card_rows(matches)
    lines = [f"Here {'is' if len(top)==1 else 'are'} the {plural} I found at {shop_label(shop)} under **{limit:g}**:"]
    for item in top:
        lines.append(chat_item_line(item, shop))
        continue
        lines.append(f"• **{item['name']}** — {item.get('price','Price not listed')} *({item.get('stock','in')})*")
    if len(matches) > len(top):
        lines.append(f"\nThere are **{len(matches)}** matching {plural} in total.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top, None, {shop.get("shop_id", ""): shop}),
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
        phone = public_shop_phone_value(shop).strip()
        website = normalize_public_url(shop.get("website", ""))
        whatsapp = (shop.get("whatsapp") or "").strip()
        if phone:
            parts.append(f"Phone: **{phone}**")
        if website:
            parts.append(f"Website: **{website}**")
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

    top = take_chat_card_rows(matches)
    intro = f"Here {'is' if len(top)==1 else 'are'} what {shop_label(shop)} currently has in stock:" if inventory_mode else f"Here {'is' if len(top)==1 else 'are'} the {plural} currently available from {shop_label(shop)}:"
    lines = [intro]
    for item in top:
        lines.append(chat_item_line(item, shop))
    if len(matches) > len(top):
        lines.append(f"\nThere are **{len(matches)}** matching {plural} in total.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top, None, {shop.get("shop_id", ""): shop}),
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
    top = take_chat_card_rows(ranked)
    lines = [f"These are the lowest-priced {plural} I found at {shop_label(shop)}:"]
    for item in top:
        lines.append(chat_item_line(item, shop))
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top, None, {shop.get("shop_id", ""): shop}),
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
        "products": serialize_products_bulk(take_chat_card_rows(matches), None, {shop.get("shop_id", ""): shop}),
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
            "products": serialize_products_bulk(matches[:1], None, {shop.get("shop_id", ""): shop}),
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
        "products": serialize_products_bulk(take_chat_card_rows(matches), None, {shop.get("shop_id", ""): shop}),
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

    top = take_chat_card_rows(ranked)
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
        "products": serialize_products_bulk(top, None, {shop.get("shop_id", ""): shop}),
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
    public_phone = public_shop_phone_value(shop)
    if public_phone: lines.append(f"Phone: {public_phone}")
    if shop.get("website"): lines.append(f"Website: {shop.get('website')}")
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
            link_text = offering_external_links_text(p)
            link_part = f" | Links: {link_text}" if link_text else ""
            summary = " | ".join(offering_summary_bits(p, shop)) or "Details available on request"
            lines.append(f'- {p["name"]} | {summary}{img_part}{attr_part}{link_part}')
    elif all_rows:
        lines.append(f"\nSample {nouns['plural']} from this business:")
        for row in all_rows[:8]:
            imgs = normalize_image_list(row.get("shop_id", ""), row.get("images", []))
            row_type = normalize_offering_type(row.get("offering_type", ""), shop.get("business_type", ""), shop.get("category", ""))
            img_part = f' | Photo: ![{row.get("name","Offering")}]({imgs[0]})' if imgs else ""
            attr_lines = format_attribute_lines(normalize_attribute_data(row.get("attribute_data"), shop.get("category", ""), row_type, shop.get("business_type", "")), shop.get("category", ""), row_type, shop.get("business_type", ""))
            attr_part = f" | Details: {'; '.join(attr_lines[:3])}" if attr_lines else ""
            link_text = offering_external_links_text(row)
            link_part = f" | Links: {link_text}" if link_text else ""
            summary = " | ".join(offering_summary_bits(row, shop)) or "Details available on request"
            lines.append(f'- {row.get("name","Offering")} | {summary}{img_part}{attr_part}{link_part}')
    if rag_chunks:
        lines.append("\nKnowledge base notes:")
        for chunk in rag_chunks[:4]:
            chunk_text = chunk.strip()
            if not public_phone:
                chunk_text = re.sub(r"(?im)^phone:\s.*(?:\n|$)", "", chunk_text).strip()
            if chunk_text:
                lines.append(chunk_text)
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

SHOP_ASSISTANT_SYSTEM = """You are the live assistant for a business on Atlantic Ordinate.
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
When answering simple customer questions, speak like a real business assistant would.
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
        lines.append("Voice style: warm and personable.")
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

def answer_business_shop_profile_query(shop: dict, prod_rows: List[Dict], q: str, currency: str = "") -> Optional[Dict[str, Any]]:
    if not is_shop_profile_query(q, shop):
        return None
    nouns = offering_nouns(shop, prod_rows)
    plural = nouns["plural"]
    location = (shop.get("address") or shop.get("service_area") or "").strip()
    overview = str(shop.get("overview", "") or "").strip()
    card_rows = select_chat_product_cards(prod_rows, shop)
    lines = [f"{shop_label(shop)} is listed here with **{len(prod_rows)}** public {plural}."]
    if overview:
        lines.append(overview)
    if location:
        lines.append(f"Location or service area: **{location}**.")
    if shop_is_open_now(shop):
        lines.append("It is marked **open now**.")
    elif shop.get("hours"):
        lines.append(f"Public hours: **{shop.get('hours')}**.")
    if card_rows:
        if len(prod_rows) <= CHAT_CARD_FULL_THRESHOLD:
            lines.append(f"I attached the available {plural} below.")
        else:
            lines.append(f"I attached **{len(card_rows)}** popular {plural} to start with, so the chat stays easy to scan.")
    return {
        "answer": "\n\n".join(lines),
        "products": serialize_products_bulk(card_rows, None, {shop.get("shop_id", ""): shop}, currency),
        "meta": {
            "llm_used": False,
            "reason": "shop_profile",
            "suggestions": build_chat_suggestions(q, shop, card_rows or prod_rows),
        },
    }

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

    top = select_chat_product_cards(prod_rows, shop)
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
        lines.append(f"\nThere are **{len(prod_rows)}** {plural} in total. I attached the strongest **{len(top)}** cards so you can narrow from there.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top, None, {shop.get("shop_id", ""): shop}),
        "meta": {"llm_used": False, "reason": "catalog_list", "suggestions": suggestions},
    }

MARKETPLACE_CHAT_STOPWORDS = {
    "a", "an", "and", "are", "at", "best", "buy", "can", "compare", "cost", "do", "does",
    "dollar", "dollars", "find", "for", "from", "have", "help", "i", "in", "is", "it", "like",
    "list", "looking", "match", "matches", "me", "my", "need", "of", "on", "option", "options",
    "or", "please", "price", "prices", "recommend", "search", "show", "something", "suggest",
    "than", "that", "the", "their", "these", "this", "to", "under", "want", "what", "which",
    "with", "you", "your",
}

MARKETPLACE_BOLD_TERMS = CHAT_GENERIC_BOLD_TERMS | {
    "atlantica", "atlantic ordinate", "app", "map", "marketplace", "assistant", "guide",
    "home page", "business page", "compare", "comparison", "recommendation", "recommendations",
}

ATLANTICA_SYSTEM = """You are Atlantica, the marketplace assistant for Atlantic Ordinate.
Answer using ONLY the supplied marketplace and app context.
You may compare public offerings across businesses and explain how the app works.
Never invent businesses, offerings, prices, availability, hours, contact details, locations, policies, or app features.
If the user asks for the cheapest option, compare only real prices from the supplied context.
If the user asks what is available, rely only on offerings that are currently available in the supplied context.
Keep the answer warm, concise, practical, and human.
Sound like a helpful marketplace guide, not a script.
Do not overwhelm the user.
Use simple markdown.
Prefer a short direct answer first, then clean bullets only when they help.
When listing offerings, include the business name plus price and availability when available.
Do not say you are an AI, model, chatbot, or system unless the user directly asks.
If information is missing, say that clearly and suggest a useful next question.
"""

def marketplace_app_notes() -> List[str]:
    return [
        "Atlantic Ordinate helps people browse registered businesses, compare public offerings, open the map, and visit each business page.",
        "Each business page includes a focused assistant for that business only.",
        "Atlantica is the marketplace-wide assistant on the home page and can compare public businesses and offerings across the app.",
    ]

def marketplace_summary(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    categories = sorted({str(shop.get("category", "")).strip() for shop in shops if str(shop.get("category", "")).strip()})
    return {
        "business_count": len(shops),
        "offering_count": len(prod_rows),
        "category_count": len(categories),
        "open_now_count": sum(1 for shop in shops if shop_is_open_now(shop)),
        "categories": categories[:12],
    }

def public_marketplace_snapshot(currency: str = "") -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    if supabase is None:
        return [], {}, []
    shop_rows = supabase.table("shops").select("*").order("created_at", desc=True).execute().data or []
    shops: List[Dict[str, Any]] = []
    shop_map: Dict[str, Dict[str, Any]] = {}
    for row in shop_rows:
        shop = ensure_shop_coordinates(normalize_shop_record(row or {}))
        if not shop_matches_market_country(shop) or not shop_is_publicly_listable(shop):
            continue
        shops.append(shop)
        shop_map[str(shop.get("shop_id", ""))] = shop

    product_rows = supabase.table("products").select("*").order("updated_at", desc=True).execute().data or []
    review_map, view_map = load_product_metric_maps(product_rows)
    prod_rows: List[Dict[str, Any]] = []
    for row in product_rows:
        shop = shop_map.get(str(row.get("shop_id", "")))
        if not shop:
            continue
        key = (str(row.get("shop_id", "")), str(row.get("product_id", "")))
        ratings = review_map.get(key, [])
        prod_rows.append({
            **row,
            **get_display_price_fields(row, shop, currency),
            "avg_rating": round(sum(ratings) / len(ratings), 1) if ratings else 0,
            "review_count": len(ratings),
            "product_views": view_map.get(key, 0),
        })
    return shops, shop_map, prod_rows

def marketplace_focus_tokens(q: str) -> List[str]:
    return [
        token
        for token in re.findall(r"[a-z0-9]+", norm_text(q))
        if token and token not in MARKETPLACE_CHAT_STOPWORDS
    ]

def marketplace_shop_offering_counts(prod_rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in prod_rows:
        shop_id = str(row.get("shop_id", ""))
        if not shop_id:
            continue
        counts[shop_id] = counts.get(shop_id, 0) + 1
    return counts

def marketplace_rows_for_shop(prod_rows: List[Dict[str, Any]], shop_id: str) -> List[Dict[str, Any]]:
    target = str(shop_id or "").strip()
    if not target:
        return []
    return [row for row in prod_rows if str(row.get("shop_id", "")) == target]

def marketplace_shop_card(shop: Dict[str, Any], prod_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = marketplace_rows_for_shop(prod_rows, str(shop.get("shop_id", "")))
    stats = {
        "offering_count": len(rows),
        "product_count": len(rows),
        "offerings_count": len(rows),
        "products_count": len(rows),
        "products_with_images": sum(1 for row in rows if normalize_image_list(row.get("shop_id", ""), row.get("images", []))),
        "offerings_with_images": sum(1 for row in rows if normalize_image_list(row.get("shop_id", ""), row.get("images", []))),
        "avg_rating": float(shop.get("stats", {}).get("avg_rating", 0) or 0),
    }
    return public_shop_payload({**shop, "stats": stats, "is_open_now": shop_is_open_now(shop)})

def serialize_marketplace_businesses(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [marketplace_shop_card(normalize_shop_record(shop or {}), prod_rows) for shop in shops if str((shop or {}).get("shop_id", "")).strip()]

def marketplace_query_mentions_shop(shop: Dict[str, Any], q: str) -> bool:
    qn = norm_text(q)
    if not qn:
        return False
    name_n = norm_text(shop.get("name", ""))
    slug_n = norm_text(re.sub(r"[-_]+", " ", str(shop.get("shop_slug", "") or "")))
    shop_id_n = norm_text(re.sub(r"[-_]+", " ", str(shop.get("shop_id", "") or "")))
    return any(label and label in qn for label in [name_n, slug_n, shop_id_n])

def marketplace_named_shop_matches(shops: List[Dict[str, Any]], q: str) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    seen = set()
    for shop in shops:
        shop_id = str(shop.get("shop_id", "")).strip()
        if not shop_id or shop_id in seen:
            continue
        if marketplace_query_mentions_shop(shop, q):
            matches.append(shop)
            seen.add(shop_id)
    return matches

def is_marketplace_compare_query(q: str) -> bool:
    qn = norm_text(q)
    return any(token in qn for token in ["compare", "comparison", "versus", "vs", "difference", "between"])

def marketplace_shop_focus_intent(q: str) -> bool:
    qn = norm_text(q)
    return any(
        token in qn
        for token in [
            "about",
            "business",
            "shop",
            "store",
            "what does",
            "what do",
            "tell me",
            "who is",
            "where is",
            "hours",
            "open",
            "contact",
            "phone",
            "whatsapp",
        ]
    )

def detect_marketplace_focus_shop(shops: List[Dict[str, Any]], q: str) -> Optional[Dict[str, Any]]:
    qn = norm_text(q)
    if not qn:
        return None
    named_matches = marketplace_named_shop_matches(shops, q)
    if len(named_matches) == 1:
        return normalize_shop_record(named_matches[0] or {})
    if len(named_matches) > 1:
        return None
    if is_marketplace_compare_query(q) or not marketplace_shop_focus_intent(q):
        return None
    ranked = rank_marketplace_shops(shops, q)
    if not ranked:
        return None
    top = normalize_shop_record(ranked[0] or {})
    top_score = float(ranked[0].get("_market_score", 0) or 0)
    second_score = float(ranked[1].get("_market_score", 0) or 0) if len(ranked) > 1 else 0.0
    if top_score < 2.55:
        return None
    if second_score and top_score < second_score + 1.1:
        return None
    return top

def score_marketplace_shop_highlight(row: Dict[str, Any], shop: Dict[str, Any], q: str) -> float:
    score = score_marketplace_product(row, shop, q)
    score += float(row.get("product_views", 0) or 0) * 0.04
    score += float(row.get("review_count", 0) or 0) * 0.2
    score += float(row.get("avg_rating", 0) or 0) * 0.18
    if row_is_available_for_chat(row, shop):
        score += 0.8
    if parse_price_value(row.get("price", "")) is not None:
        score += 0.14
    if row.get("overview"):
        score += 0.12
    if normalize_image_list(row.get("shop_id", ""), row.get("images", [])):
        score += 0.12
    return score

def pick_marketplace_shop_highlights(prod_rows: List[Dict[str, Any]], shop: Dict[str, Any], q: str, limit: int = 1) -> List[Dict[str, Any]]:
    ranked = sorted(
        prod_rows,
        key=lambda row: (
            score_marketplace_shop_highlight(row, shop, q),
            str(row.get("updated_at", "") or ""),
            str(row.get("name", "")).lower(),
        ),
        reverse=True,
    )
    return ranked[: max(1, limit)]

def is_marketplace_shop_profile_query(q: str, focus_shop: Optional[Dict[str, Any]]) -> bool:
    if not focus_shop:
        return False
    if any(
        predicate(q)
        for predicate in [is_price_lookup_query, is_stock_query, is_cheapest_query, is_budget_query, is_hours_query, is_contact_query, is_location_query, wants_product_image]
    ):
        return False
    qn = norm_text(q)
    if is_list_intent(qn) or is_marketplace_business_list_query(q) or is_marketplace_compare_query(q):
        return False
    aliases = {
        norm_text(focus_shop.get("name", "")),
        norm_text(re.sub(r"[-_]+", " ", str(focus_shop.get("shop_slug", "") or ""))),
        norm_text(re.sub(r"[-_]+", " ", str(focus_shop.get("shop_id", "") or ""))),
    }
    if qn in {alias for alias in aliases if alias}:
        return True
    triggers = [
        "tell me about",
        "about",
        "what does",
        "what do",
        "who is",
        "what is",
        "do they sell",
        "do they have",
        "what can i get from",
    ]
    return any(trigger in qn for trigger in triggers)

def score_marketplace_shop(shop: Dict[str, Any], q: str) -> float:
    qn = norm_text(q)
    tokens = set(marketplace_focus_tokens(q))
    hay = norm_text(" ".join([
        str(shop.get("name", "")),
        str(shop.get("category", "")),
        str(shop.get("overview", "")),
        str(shop.get("address", "")),
        str(shop.get("service_area", "")),
        str(shop.get("hours", "")),
    ]))
    hay_tokens = set(re.findall(r"[a-z0-9]+", hay))
    score = 0.0
    if not qn:
        score = 1.0
    else:
        name_n = norm_text(shop.get("name", ""))
        category_n = norm_text(shop.get("category", ""))
        if name_n and name_n in qn:
            score += 10
        if qn and qn in hay:
            score += 5
        if category_n and category_n in qn:
            score += 1.2
        score += len(tokens & hay_tokens) * 1.55
        if tokens and name_n:
            score += sum(0.6 for token in tokens if token in name_n)
        if shop_is_open_now(shop):
            score += 0.2
        if shop.get("overview"):
            score += 0.15
    return score

def rank_marketplace_shops(shops: List[Dict[str, Any]], q: str) -> List[Dict[str, Any]]:
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for shop in shops:
        score = score_marketplace_shop(shop, q)
        if score <= 0 and norm_text(q):
            continue
        scored.append((score, {**shop, "_market_score": score}))
    scored.sort(key=lambda item: (item[0], item[1].get("name", "").lower()), reverse=True)
    return [shop for _, shop in scored]

def score_marketplace_product(row: Dict[str, Any], shop: Dict[str, Any], q: str) -> float:
    qn = norm_text(q)
    tokens = set(marketplace_focus_tokens(q))
    business_type = shop.get("business_type", "")
    category = shop.get("category", "")
    offering_type = normalize_offering_type(row.get("offering_type", ""), business_type, category)
    attr_text = product_attribute_text(row, category, offering_type, business_type)
    rating_text = f"{row.get('avg_rating', 0)} star {row.get('review_count', 0)} reviews"
    hay = norm_text(" ".join([
        str(row.get("name", "")),
        str(row.get("overview", "")),
        str(row.get("variants", "")),
        attr_text,
        rating_text,
        str(shop.get("name", "")),
        str(shop.get("category", "")),
        str(shop.get("overview", "")),
        str(shop.get("address", "")),
        str(shop.get("service_area", "")),
    ]))
    hay_tokens = set(re.findall(r"[a-z0-9]+", hay))
    score = 0.0
    if not qn:
        score = 1.0
    else:
        name_n = norm_text(row.get("name", ""))
        shop_n = norm_text(shop.get("name", ""))
        category_n = norm_text(shop.get("category", ""))
        if name_n and name_n in qn:
            score += 10
        if shop_n and shop_n in qn:
            score += 5.5
        if qn and qn in hay:
            score += 5
        if category_n and category_n in qn:
            score += 1.1
        overlap = tokens & hay_tokens
        score += len(overlap) * 1.6
        if tokens and name_n:
            score += sum(0.7 for token in tokens if token in name_n)
        if qn and not tokens and row_is_available_for_chat(row, shop):
            score += 1.0
        if is_rating_query(q) and float(row.get("avg_rating", 0) or 0) > 0:
            score += float(row.get("avg_rating", 0) or 0) * 0.35
            score += min(float(row.get("review_count", 0) or 0), 8.0) * 0.08
    if row_is_available_for_chat(row, shop):
        score += 0.35
    if normalize_image_list(row.get("shop_id", ""), row.get("images", [])):
        score += 0.2
    if row.get("overview"):
        score += 0.15
    return score

def rank_marketplace_products(prod_rows: List[Dict[str, Any]], q: str, shop_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for row in prod_rows:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        score = score_marketplace_product(row, shop, q)
        if score <= 0 and norm_text(q):
            continue
        scored.append((score, {**row, "_market_score": score}))
    scored.sort(
        key=lambda item: (
            item[0],
            1 if row_is_available_for_chat(item[1], normalize_shop_record(shop_map.get(str(item[1].get("shop_id", "")), {}) or {})) else 0,
            len(normalize_image_list(item[1].get("shop_id", ""), item[1].get("images", []))),
            str(item[1].get("name", "")).lower(),
        ),
        reverse=True,
    )
    return [row for _, row in scored]

def match_marketplace_products(prod_rows: List[Dict[str, Any]], q: str, shop_map: Dict[str, Dict[str, Any]], min_score: float = 1.05) -> List[Dict[str, Any]]:
    ranked = rank_marketplace_products(prod_rows, q, shop_map)
    if not norm_text(q):
        return ranked
    matched = [row for row in ranked if float(row.get("_market_score", 0) or 0) >= min_score]
    if matched:
        return matched
    if not marketplace_focus_tokens(q):
        return ranked[:20]
    return []

def serialize_marketplace_products(rows: List[Dict[str, Any]], shop_map: Dict[str, Dict[str, Any]], currency: str = "") -> List[Dict[str, Any]]:
    items = serialize_products_bulk(rows, None, shop_map, currency)
    for item in items:
        shop = normalize_shop_record(shop_map.get(str(item.get("shop_id", "")), {}) or {})
        item["shop_name"] = shop.get("name", "")
        item["shop_address"] = shop.get("address", "") or shop.get("service_area", "")
        item["shop_category"] = shop.get("category", "")
        item["business_name"] = item.get("shop_name", "")
        item["business_address"] = item.get("shop_address", "")
        item["business_category"] = item.get("shop_category", "")
    return items

def marketplace_chat_item_line(item: Dict[str, Any], shop: Dict[str, Any]) -> str:
    summary = " | ".join(offering_summary_bits(item, shop)) or "Details available on request"
    return f"- **{item.get('name', 'Offering')}** from **{shop.get('name', 'Business')}** - {summary}"

def marketplace_response_style_instructions(q: str, focus_shop: Optional[Dict[str, Any]] = None) -> str:
    qn = norm_text(q)
    if focus_shop and focus_shop.get("name"):
        return (
            f"Stay focused on **{focus_shop.get('name')}** unless the user explicitly asks to compare other businesses. "
            "Keep the tone natural and conversational. "
            "For general business questions, give a short answer in 2 to 4 sentences and mention at most one standout offering unless the user asks for more."
        )
    if is_recommendation_query(qn) or any(token in qn for token in ["compare", "difference", "best", "popular"]):
        return (
            "Response format: start with one short direct sentence, then list up to 4 offering bullets. "
            "Each bullet should include offering name, business name, price, availability, and a short reason."
        )
    if is_list_intent(qn) or any(token in qn for token in ["buy", "find", "looking", "need", "want"]):
        return (
            "Response format: start with one short summary sentence, then list the clearest matching offerings. "
            "Each bullet should include offering name, business name, price, and availability."
        )
    if is_price_lookup_query(qn) or is_budget_query(qn) or is_cheapest_query(qn):
        return "Response format: answer directly in 1 to 4 lines. Put the actual price result first."
    if is_location_query(qn) or is_hours_query(qn) or is_contact_query(qn):
        return "Response format: answer directly and briefly in 1 to 3 lines."
    return "Response format: give a short direct answer first. Use bullets only if they clearly help."

def marketplace_chat_max_tokens(q: str, picked: Optional[List[Dict[str, Any]]] = None) -> int:
    base = chat_max_tokens_for_query(q, picked)
    qn = norm_text(q)
    if any(token in qn for token in ["compare", "difference", "which one", "help me choose"]):
        return max(base, 1000)
    if is_list_intent(qn) or is_recommendation_query(qn):
        return max(base, 950)
    return max(base, 780)

def build_marketplace_chat_suggestions(q: str, picked: List[Dict[str, Any]], shops: List[Dict[str, Any]], shop_map: Dict[str, Dict[str, Any]]) -> List[str]:
    suggestions: List[str] = []
    focus_shop = shops[0] if len(shops) == 1 else None
    if picked:
        top = picked[0]
        top_name = str(top.get("name", "")).strip()
        top_shop = normalize_shop_record(shop_map.get(str(top.get("shop_id", "")), {}) or {})
        if top_name:
            suggestions.extend([
                f"What is the price of {top_name}?",
                f"Show me more like {top_name}",
                f"Which business has the cheapest {top_name}?",
            ])
        if top_shop.get("name"):
            suggestions.append(f"What else does {top_shop['name']} have?")
    elif focus_shop and focus_shop.get("name"):
        suggestions.extend([
            f"What does {focus_shop['name']} have?",
            f"What are {focus_shop['name']} hours?",
            f"Where is {focus_shop['name']} located?",
        ])
    else:
        suggestions.extend(["Show me shoes", "Find the cheapest option", "Which businesses are open now?", "What can I buy on this app?"])
    return dedup(suggestions)[:4]

def is_marketplace_app_query(q: str) -> bool:
    qn = norm_text(q)
    triggers = [
        "who are you",
        "what is atlantica",
        "what is this app",
        "tell me about this app",
        "how does this app work",
        "what can i do here",
        "what can you do",
        "what can i buy here",
    ]
    return any(trigger in qn for trigger in triggers)

def answer_marketplace_app_query(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    summary = marketplace_summary(shops, prod_rows)
    categories = ", ".join(summary["categories"][:6]) if summary["categories"] else "multiple categories"
    return {
        "answer": (
            f"I'm **Atlantica**, the marketplace guide for Atlantic Ordinate.\n\n"
            f"Right now I can help you browse **{summary['business_count']}** public businesses and **{summary['offering_count']}** public offerings across **{summary['category_count']}** categories.\n\n"
            f"You can ask me to compare prices, find the cheapest option, show matching offerings, explain how the app works, or point you to a business page for details. Current categories include **{categories}**."
        ),
        "products": [],
        "meta": {
            "llm_used": False,
            "reason": "app_info",
            "suggestions": ["Show me shoes", "Find the cheapest option", "Which businesses are open now?", "How does the business chat work?"],
        },
    }

def is_marketplace_business_list_query(q: str) -> bool:
    qn = norm_text(q)
    return (
        any(token in qn for token in ["business", "businesses", "shop", "shops", "store", "stores"])
        and (is_list_intent(qn) or any(trigger in qn for trigger in ["what businesses", "which businesses", "what shops", "which shops", "what stores", "which stores"]))
    )

def answer_marketplace_business_list_query(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]], q: str) -> Optional[Dict[str, Any]]:
    if not is_marketplace_business_list_query(q):
        return None
    counts = marketplace_shop_offering_counts(prod_rows)
    ranked = rank_marketplace_shops(shops, q)
    focus = marketplace_focus_tokens(q)
    if focus:
        ranked = [shop for shop in ranked if float(shop.get("_market_score", 0) or 0) >= 1.05]
    if not ranked:
        return {
            "answer": "I could not find a business match for that yet. Try a business name, category, or product type.",
            "products": [],
            "meta": {"llm_used": False, "reason": "business_list_empty", "suggestions": ["Show me shoes", "Which businesses are open now?", "What can I buy on this app?"]},
        }
    top = ranked[:8]
    lines = [f"I found **{len(ranked)}** business{'es' if len(ranked) != 1 else ''} that fit that request:"]
    for shop in top:
        location = shop.get("address") or shop.get("service_area") or "Location coming soon"
        offering_count = counts.get(str(shop.get("shop_id", "")), 0)
        bits = [shop.get("category") or "Business", location, f"{offering_count} offerings"]
        if shop_is_open_now(shop):
            bits.append("Open now")
        lines.append(f"- **{shop.get('name', 'Business')}** - {' | '.join(str(bit) for bit in bits if bit)}")
    if len(ranked) > len(top):
        lines.append(f"\nThere are **{len(ranked)}** matching businesses in total. Ask me to narrow it by product, price, or location.")
    return {
        "answer": "\n".join(lines),
        "products": [],
        "meta": {"llm_used": False, "reason": "business_list", "suggestions": ["Which businesses are open now?", "Show me shoes", "Find the cheapest option"]},
    }

def answer_marketplace_shop_profile_query(shop: Dict[str, Any], prod_rows: List[Dict[str, Any]], q: str, shop_map: Dict[str, Dict[str, Any]], currency: str = "") -> Dict[str, Any]:
    shop_rows = marketplace_rows_for_shop(prod_rows, str(shop.get("shop_id", "")))
    highlight_rows = take_chat_card_rows(pick_marketplace_shop_highlights(shop_rows, shop, q, limit=len(shop_rows) or CHAT_CARD_MAX_RESULTS))
    name = shop.get("name", "This business")
    category = (shop.get("category") or "business").strip()
    location = (shop.get("address") or shop.get("service_area") or "").strip()
    overview = str(shop.get("overview", "") or "").strip()
    parts = []
    opener = f"**{name}** is a {category.lower()} business"
    if location:
        opener += f" in **{location}**"
    parts.append(opener + ".")
    if overview:
        parts.append(overview)
    if shop.get("website"):
        parts.append(f"Official website: {shop.get('website')}")
    detail_bits = []
    if shop_rows:
        detail_bits.append(f"**{len(shop_rows)}** public offering{'s' if len(shop_rows) != 1 else ''} on Atlantica")
    if shop_is_open_now(shop):
        detail_bits.append("currently **open now**")
    elif shop.get("hours"):
        detail_bits.append(f"public hours are **{shop.get('hours')}**")
    if detail_bits:
        parts.append("Right now it has " + " and ".join(detail_bits) + ".")
    if highlight_rows:
        top = highlight_rows[0]
        price = top.get("price") or "price on request"
        if len(shop_rows) <= CHAT_CARD_FULL_THRESHOLD:
            parts.append("I attached the available offerings below.")
        else:
            parts.append(f"A good place to start is **{top.get('name', 'this offering')}** at **{price}**. I attached **{len(highlight_rows)}** popular cards so the chat stays easy to scan.")
    return {
        "answer": "\n\n".join(parts),
        "businesses": serialize_marketplace_businesses([shop], prod_rows),
        "products": serialize_marketplace_products(highlight_rows, shop_map, currency) if highlight_rows else [],
        "meta": {
            "llm_used": False,
            "reason": "shop_profile",
            "suggestions": dedup([
                f"What does {name} have?",
                f"What are {name} hours?",
                f"Where is {name} located?",
                f"Show me more from {name}",
            ]),
        },
    }

def answer_marketplace_shop_info_query(
    shops: List[Dict[str, Any]],
    prod_rows: List[Dict[str, Any]],
    q: str,
    shop_map: Dict[str, Dict[str, Any]],
    currency: str = "",
    focus_shop: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not (is_location_query(q) or is_hours_query(q) or is_contact_query(q)):
        return None
    ranked = rank_marketplace_shops(shops, q) if not focus_shop else []
    top = focus_shop or (ranked[0] if ranked else None)
    if not top or (not focus_shop and float(top.get("_market_score", 0) or 0) < 2.1):
        return {
            "answer": "Tell me the business name and I can share its location, hours, or public contact details.",
            "products": [],
            "meta": {"llm_used": False, "reason": "shop_info_missing_name", "suggestions": ["Which businesses are open now?", "Show me businesses", "What can I buy on this app?"]},
        }
    businesses = serialize_marketplace_businesses([top], prod_rows)
    if is_location_query(q):
        location_mode = normalize_location_mode(top.get("location_mode", ""), top.get("business_type", ""), top.get("category", ""))
        if location_mode == "online":
            answer = f"**{top.get('name', 'This business')}** operates online."
        elif top.get("service_area"):
            answer = f"**{top.get('name', 'This business')}** serves **{top.get('service_area')}**."
        else:
            answer = f"**{top.get('name', 'This business')}** is listed at **{top.get('address') or 'location not listed'}**."
        return {
            "answer": answer,
            "businesses": businesses,
            "products": [],
            "meta": {"llm_used": False, "reason": "shop_location", "suggestions": [f"What does {top.get('name', 'this business')} offer?", f"What are {top.get('name', 'this business')} opening hours?", "Show me more businesses"]},
        }
    if is_hours_query(q):
        hours = top.get("hours") or "Hours are not listed yet."
        return {
            "answer": f"The public hours I found for **{top.get('name', 'this business')}** are **{hours}**.",
            "businesses": businesses,
            "products": [],
            "meta": {"llm_used": False, "reason": "shop_hours", "suggestions": [f"Where is {top.get('name', 'this business')} located?", f"What does {top.get('name', 'this business')} offer?", "Which businesses are open now?"]},
        }
    phone = public_shop_phone_value(top).strip()
    website = normalize_public_url(top.get("website", ""))
    whatsapp = str(top.get("whatsapp") or "").strip()
    if phone or website or whatsapp:
        parts = []
        if phone:
            parts.append(f"Phone: **{phone}**")
        if website:
            parts.append(f"Website: **{website}**")
        if whatsapp:
            parts.append(f"WhatsApp: **{whatsapp}**")
        return {
            "answer": f"Here is the public contact info I found for **{top.get('name', 'this business')}**:\n" + "\n".join(f"- {part}" for part in parts),
            "businesses": businesses,
            "products": [],
            "meta": {"llm_used": False, "reason": "shop_contact", "suggestions": [f"What does {top.get('name', 'this business')} offer?", f"Where is {top.get('name', 'this business')} located?", "Show me more businesses"]},
        }
    return {
        "answer": f"I do not see a public phone or WhatsApp listing for **{top.get('name', 'this business')}** right now.",
        "businesses": businesses,
        "products": [],
        "meta": {"llm_used": False, "reason": "shop_contact_missing", "suggestions": [f"What does {top.get('name', 'this business')} offer?", f"Where is {top.get('name', 'this business')} located?", "Which businesses are open now?"]},
    }

def answer_marketplace_open_now_query(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]], q: str) -> Optional[Dict[str, Any]]:
    qn = norm_text(q)
    if not is_hours_query(q) or not any(token in qn for token in ["business", "businesses", "shop", "shops", "store", "stores"]):
        return None
    counts = marketplace_shop_offering_counts(prod_rows)
    open_shops = [shop for shop in shops if shop_is_open_now(shop)]
    if not open_shops:
        return {
            "answer": "I do not see any businesses marked open right now.",
            "products": [],
            "meta": {"llm_used": False, "reason": "open_now_empty", "suggestions": ["Show me businesses", "Show me shoes", "What can I buy on this app?"]},
        }
    top = open_shops[:8]
    lines = [f"These businesses are marked **open now**:"]
    for shop in top:
        location = shop.get("address") or shop.get("service_area") or "Location coming soon"
        lines.append(f"- **{shop.get('name', 'Business')}** - {shop.get('category') or 'Business'} | {location} | {counts.get(str(shop.get('shop_id', '')), 0)} offerings")
    if len(open_shops) > len(top):
        lines.append(f"\nThere are **{len(open_shops)}** open businesses in total.")
    return {
        "answer": "\n".join(lines),
        "products": [],
        "meta": {"llm_used": False, "reason": "open_now_businesses", "suggestions": ["Show me shoes", "Find the cheapest option", "What can I buy on this app?"]},
    }

def answer_marketplace_rating_query(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]], q: str, shop_map: Dict[str, Dict[str, Any]], currency: str = "") -> Optional[Dict[str, Any]]:
    if not is_rating_query(q):
        return None

    qn = norm_text(q)
    subject_q = rating_query_subject(q)
    if subject_q:
        ranked = match_marketplace_products(prod_rows, subject_q, shop_map)
        pool = ranked or rank_marketplace_products(prod_rows, subject_q, shop_map)
    else:
        pool = rank_marketplace_products(prod_rows, "", shop_map)

    prefers_products = query_prefers_product_offerings(q)
    prefers_services = query_prefers_service_offerings(q)
    mode = rating_query_mode(q)
    target_rating = extract_rating_value(q)
    matches: List[Dict[str, Any]] = []

    for row in pool:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        business_type = shop.get("business_type", "")
        category = shop.get("category", "")
        offering_type = normalize_offering_type(row.get("offering_type", ""), business_type, category)
        if prefers_products and offering_type != "product":
            continue
        if prefers_services and offering_type == "product":
            continue
        avg_rating = round(float(row.get("avg_rating", 0) or 0), 1)
        review_count = int(row.get("review_count", 0) or 0)
        if review_count <= 0 or avg_rating <= 0:
            continue
        if target_rating is not None:
            if mode == "min" and avg_rating < target_rating:
                continue
            if mode == "above" and avg_rating <= target_rating:
                continue
            if mode == "max" and avg_rating > target_rating:
                continue
            if mode == "exact":
                if round(target_rating, 1) == 5.0:
                    if avg_rating < 5.0:
                        continue
                elif avg_rating != round(target_rating, 1):
                    continue
        matches.append({**row, "_rating_value": avg_rating, "_review_total": review_count})

    if not matches:
        offering_label = "products" if prefers_products else "services" if prefers_services else "offerings"
        if target_rating is not None and mode == "exact" and round(target_rating, 1) == 5.0:
            detail = "**5-star**"
        elif target_rating is not None and mode == "min":
            detail = f"rated **{target_rating:g} stars or higher**"
        elif target_rating is not None and mode == "above":
            detail = f"rated **above {target_rating:g} stars**"
        elif target_rating is not None and mode == "max":
            detail = f"rated **{target_rating:g} stars or lower**"
        elif target_rating is not None:
            detail = f"rated **{target_rating:g} stars**"
        else:
            detail = "with public ratings"
        scope = f" at **{shops[0].get('name')}**" if len(shops) == 1 and shops[0].get("name") else ""
        return {
            "answer": f"I could not find any public {offering_label} {detail}{scope} right now.",
            "products": [],
            "meta": {"llm_used": False, "reason": "market_rating_empty", "suggestions": ["Show me top rated products", "Show me shoes", "Find the cheapest option"]},
        }

    if is_cheapest_query(q):
        matches.sort(
            key=lambda row: (
                parse_price_value(row.get("price", "")) is None,
                parse_price_value(row.get("price", "")) if parse_price_value(row.get("price", "")) is not None else float("inf"),
                -float(row.get("_rating_value", 0) or 0),
                -int(row.get("_review_total", 0) or 0),
                -float(row.get("_market_score", 0) or 0),
            )
        )
    else:
        matches.sort(
            key=lambda row: (
                float(row.get("_rating_value", 0) or 0),
                int(row.get("_review_total", 0) or 0),
                float(row.get("product_views", 0) or 0),
                float(row.get("_market_score", 0) or 0),
                str(row.get("name", "")).lower(),
            ),
            reverse=True,
        )

    top = take_chat_card_rows(matches)
    if target_rating is not None and mode == "exact" and round(target_rating, 1) == 5.0:
        rating_phrase = "**5-star**"
    elif target_rating is not None and mode == "min":
        rating_phrase = f"rated **{target_rating:g} stars or higher**"
    elif target_rating is not None and mode == "above":
        rating_phrase = f"rated **above {target_rating:g} stars**"
    elif target_rating is not None and mode == "max":
        rating_phrase = f"rated **{target_rating:g} stars or lower**"
    elif target_rating is not None:
        rating_phrase = f"rated **{target_rating:g} stars**"
    else:
        rating_phrase = "**top-rated**"

    offering_label = "product" if prefers_products else "service" if prefers_services else "offering"
    offering_plural = f"{offering_label}s" if offering_label != "service" else "services"
    scope = f" at **{shops[0].get('name')}**" if len(shops) == 1 and shops[0].get("name") else " across the marketplace"
    opener = f"I found **{len(matches)}** {offering_plural} {rating_phrase}{scope}:"
    if mode == "top" and target_rating is None:
        opener = f"Here are the strongest rated {offering_plural}{scope}:"

    lines = [opener]
    for row in top:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        summary_bits = offering_summary_bits(row, shop)
        summary_bits.insert(0, product_rating_label(row))
        line = f"- **{row.get('name', 'Offering')}** from **{shop.get('name', 'Business')}** - {' | '.join(bit for bit in summary_bits if bit)}"
        lines.append(line)
    if len(matches) > len(top):
        lines.append(f"\nI attached **{len(top)}** cards below. Ask me to narrow it by product type, shop, or budget.")

    return {
        "answer": "\n".join(lines),
        "products": serialize_marketplace_products(top, shop_map, currency),
        "meta": {
            "llm_used": False,
            "reason": "market_rating",
            "suggestions": build_marketplace_chat_suggestions(q, top, shops, shop_map),
        },
    }

def answer_marketplace_price_query(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]], q: str, shop_map: Dict[str, Dict[str, Any]], currency: str = "") -> Optional[Dict[str, Any]]:
    if not is_price_lookup_query(q):
        return None
    matches = match_marketplace_products(prod_rows, q, shop_map)
    if not matches:
        return None
    top = matches[:4]
    focus_shop = shops[0] if len(shops) == 1 else None
    lines = []
    for row in top:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        price = row.get("price") or "Price not listed"
        status = offering_status_label(row, shop)
        line = f"- **{row.get('name', 'Offering')}** from **{shop.get('name', 'Business')}** is listed at **{price}**"
        if status:
            line += f" and is currently **{status.lower()}**."
        else:
            line += "."
        lines.append(line)
    if focus_shop and focus_shop.get("name"):
        opener = f"Here {'is' if len(lines) == 1 else 'are'} the closest price match{'es' if len(lines) != 1 else ''} I found at **{focus_shop.get('name')}**:"
    else:
        opener = "Here is the closest price match I found:" if len(lines) == 1 else "Here are the closest price matches I found:"
    return {
        "answer": "\n".join([opener, *lines]),
        "products": serialize_marketplace_products(top, shop_map, currency),
        "meta": {"llm_used": False, "reason": "market_price_lookup", "suggestions": build_marketplace_chat_suggestions(q, top, shops, shop_map)},
    }

def answer_marketplace_budget_query(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]], q: str, shop_map: Dict[str, Dict[str, Any]], currency: str = "") -> Optional[Dict[str, Any]]:
    limit = extract_budget_limit(q)
    if limit is None:
        return None
    ranked = match_marketplace_products(prod_rows, q, shop_map)
    pool = ranked or rank_marketplace_products(prod_rows, "", shop_map)
    matches: List[Dict[str, Any]] = []
    for row in pool:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        value = parse_price_value(row.get("price", ""))
        if value is None or value > limit:
            continue
        if not row_is_available_for_chat(row, shop):
            continue
        matches.append({**row, "_price_value": value})
    matches.sort(key=lambda row: (row.get("_price_value", 0), str(row.get("name", "")).lower()))
    if not matches:
        return {
            "answer": f"I could not find a public offering priced below **{limit:g}** right now.",
            "products": [],
            "meta": {"llm_used": False, "reason": "market_budget_empty", "suggestions": ["Find the cheapest option", "Show me businesses", "What can I buy on this app?"]},
        }
    top = take_chat_card_rows(matches)
    focus_shop = shops[0] if len(shops) == 1 else None
    business_count = len({str(row.get("shop_id", "")) for row in matches})
    if focus_shop and focus_shop.get("name"):
        lines = [f"I found **{len(matches)}** matching offering{'s' if len(matches) != 1 else ''} under **{limit:g}** at **{focus_shop.get('name')}**:"]
    else:
        lines = [f"I found **{len(matches)}** matching offering{'s' if len(matches) != 1 else ''} under **{limit:g}** across **{business_count}** business{'es' if business_count != 1 else ''}:"]
    for row in top:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        lines.append(marketplace_chat_item_line(enrich_chat_row(row, shop), shop))
    if len(matches) > len(top):
        lines.append(f"\nI attached **{len(top)}** cards below. Ask me to narrow the list by type, brand, or business.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_marketplace_products(top, shop_map, currency),
        "meta": {"llm_used": False, "reason": "market_budget", "suggestions": build_marketplace_chat_suggestions(q, top, shops, shop_map)},
    }

def answer_marketplace_cheapest_query(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]], q: str, shop_map: Dict[str, Dict[str, Any]], currency: str = "") -> Optional[Dict[str, Any]]:
    if not is_cheapest_query(q):
        return None
    ranked = match_marketplace_products(prod_rows, q, shop_map)
    pool = ranked or rank_marketplace_products(prod_rows, "", shop_map)
    priced: List[Dict[str, Any]] = []
    for row in pool:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        value = parse_price_value(row.get("price", ""))
        if value is None:
            continue
        if not row_is_available_for_chat(row, shop):
            continue
        priced.append({**row, "_price_value": value})
    priced.sort(key=lambda row: (row.get("_price_value", 0), str(row.get("name", "")).lower()))
    if not priced:
        return None
    top = take_chat_card_rows(priced)
    focus_shop = shops[0] if len(shops) == 1 else None
    lines = [f"These are the lowest-priced matches I found at **{focus_shop.get('name')}**:" if focus_shop and focus_shop.get("name") else "These are the lowest-priced matches I found right now:"]
    for row in top:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        lines.append(marketplace_chat_item_line(enrich_chat_row(row, shop), shop))
    return {
        "answer": "\n".join(lines),
        "products": serialize_marketplace_products(top, shop_map, currency),
        "meta": {"llm_used": False, "reason": "market_cheapest", "suggestions": build_marketplace_chat_suggestions(q, top, shops, shop_map)},
    }

def answer_marketplace_stock_query(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]], q: str, shop_map: Dict[str, Dict[str, Any]], currency: str = "") -> Optional[Dict[str, Any]]:
    if not is_stock_query(q):
        return None
    ranked = match_marketplace_products(prod_rows, q, shop_map)
    pool = ranked or rank_marketplace_products(prod_rows, "", shop_map)
    matches: List[Dict[str, Any]] = []
    for row in pool:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        if row_is_available_for_chat(row, shop):
            matches.append(row)
    if not matches:
        return {
            "answer": "I do not see any public offerings available for that request right now.",
            "products": [],
            "meta": {"llm_used": False, "reason": "market_stock_empty", "suggestions": ["Show me businesses", "Find the cheapest option", "What can I buy on this app?"]},
        }
    top = take_chat_card_rows(matches)
    focus_shop = shops[0] if len(shops) == 1 else None
    business_count = len({str(row.get("shop_id", "")) for row in matches})
    if focus_shop and focus_shop.get("name"):
        lines = [f"Here {'is' if len(matches) == 1 else 'are'} what **{focus_shop.get('name')}** currently has available:"]
    else:
        lines = [f"I found **{len(matches)}** available match{'es' if len(matches) != 1 else ''} across **{business_count}** business{'es' if business_count != 1 else ''}:"]
    for row in top:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        lines.append(marketplace_chat_item_line(enrich_chat_row(row, shop), shop))
    if len(matches) > len(top):
        lines.append(f"\nI attached **{len(top)}** cards below.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_marketplace_products(top, shop_map, currency),
        "meta": {"llm_used": False, "reason": "market_stock", "suggestions": build_marketplace_chat_suggestions(q, top, shops, shop_map)},
    }

def answer_marketplace_catalog_query(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]], q: str, shop_map: Dict[str, Dict[str, Any]], currency: str = "") -> Dict[str, Any]:
    ranked = match_marketplace_products(prod_rows, q, shop_map)
    if norm_text(q) and not ranked:
        return {
            "answer": "I could not find a direct marketplace match for that yet. Try a product type, business name, price range, or category.",
            "products": [],
            "meta": {"llm_used": False, "reason": "market_catalog_empty", "suggestions": ["Show me businesses", "Find the cheapest option", "What can I buy on this app?"]},
        }
    pool = ranked or rank_marketplace_products(prod_rows, "", shop_map)
    top = take_chat_card_rows(pool)
    focus_shop = shops[0] if len(shops) == 1 else None
    business_count = len({str(row.get("shop_id", "")) for row in pool})
    if focus_shop and focus_shop.get("name"):
        lines = [f"Here {'is' if len(pool) == 1 else 'are'} the best match{'es' if len(pool) != 1 else ''} I found at **{focus_shop.get('name')}**:"]
    else:
        lines = [f"I found **{len(pool)}** matching offering{'s' if len(pool) != 1 else ''} across **{business_count}** business{'es' if business_count != 1 else ''}:"]
    for row in top:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        lines.append(marketplace_chat_item_line(enrich_chat_row(row, shop), shop))
    if len(pool) > len(top):
        lines.append(f"\nI attached **{len(top)}** cards below. Ask me to narrow the list by budget, business, or product details.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_marketplace_products(top, shop_map, currency),
        "meta": {"llm_used": False, "reason": "market_catalog", "suggestions": build_marketplace_chat_suggestions(q, top, shops, shop_map)},
    }

def answer_marketplace_recommendation_query(shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]], q: str, shop_map: Dict[str, Dict[str, Any]], currency: str = "") -> Optional[Dict[str, Any]]:
    if not is_recommendation_query(q):
        return None
    budget = extract_budget_limit(q)
    ranked = match_marketplace_products(prod_rows, q, shop_map)
    pool = ranked or rank_marketplace_products(prod_rows, "", shop_map)
    candidates: List[Dict[str, Any]] = []
    for row in pool:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        if not row_is_available_for_chat(row, shop):
            continue
        value = parse_price_value(row.get("price", ""))
        if budget is not None and value is not None and value > budget:
            continue
        candidates.append(row)
    if not candidates:
        return {
            "answer": "I could not find a strong public recommendation for that request right now.",
            "products": [],
            "meta": {"llm_used": False, "reason": "market_recommendation_empty", "suggestions": ["Find the cheapest option", "Show me businesses", "What can I buy on this app?"]},
        }
    top = take_chat_card_rows(candidates)
    focus_shop = shops[0] if len(shops) == 1 else None
    opener = f"Here are a few strong picks from **{focus_shop.get('name')}**:" if focus_shop and focus_shop.get("name") else "Here are a few strong marketplace options:"
    if budget is not None:
        opener = f"Here are a few strong picks from **{focus_shop.get('name')}** under **{budget:g}**:" if focus_shop and focus_shop.get("name") else f"Here are a few strong marketplace options under **{budget:g}**:"
    lines = [opener]
    for row in top:
        shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
        detail = marketplace_chat_item_line(enrich_chat_row(row, shop), shop)
        overview = str(row.get("overview", "") or "").strip()
        if overview:
            detail += f"\n  {overview[:120]}"
        lines.append(detail)
    return {
        "answer": "\n".join(lines),
        "products": serialize_marketplace_products(top, shop_map, currency),
        "meta": {"llm_used": False, "reason": "market_recommendation", "suggestions": build_marketplace_chat_suggestions(q, top, shops, shop_map)},
    }

def build_marketplace_context(shops: List[Dict[str, Any]], picked: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]], shop_map: Dict[str, Dict[str, Any]], focus_shop: Optional[Dict[str, Any]] = None) -> str:
    summary = marketplace_summary(shops, prod_rows)
    lines = [
        "Assistant: Atlantica",
        "App: Atlantic Ordinate",
        *[f"App note: {note}" for note in marketplace_app_notes()],
        f"Public businesses: {summary['business_count']}",
        f"Public offerings: {summary['offering_count']}",
        f"Categories: {', '.join(summary['categories']) if summary['categories'] else 'Not listed'}",
        f"Open now: {summary['open_now_count']}",
    ]
    if focus_shop:
        location = focus_shop.get("address") or focus_shop.get("service_area") or "Location not listed"
        lines.append(
            f"Focused business: {focus_shop.get('name', 'Business')} | Category: {focus_shop.get('category') or 'Business'} | Location: {location} | Hours: {focus_shop.get('hours') or 'Not listed'}"
        )
        if focus_shop.get("website"):
            lines.append(f"Focused business website: {focus_shop.get('website')}")
        if focus_shop.get("overview"):
            lines.append(f"Focused business overview: {focus_shop.get('overview')}")
    if picked:
        lines.append("\nRelevant marketplace offerings:")
        for row in picked[:8]:
            shop = normalize_shop_record(shop_map.get(str(row.get("shop_id", "")), {}) or {})
            summary_bits = " | ".join(offering_summary_bits(row, shop)) or "Details available on request"
            location = shop.get("address") or shop.get("service_area") or ""
            row_type = normalize_offering_type(row.get("offering_type", ""), shop.get("business_type", ""), shop.get("category", ""))
            attr_lines = format_attribute_lines(normalize_attribute_data(row.get("attribute_data"), shop.get("category", ""), row_type, shop.get("business_type", "")), shop.get("category", ""), row_type, shop.get("business_type", ""))
            attr_part = f" | Details: {'; '.join(attr_lines[:3])}" if attr_lines else ""
            link_text = offering_external_links_text(row)
            link_part = f" | Links: {link_text}" if link_text else ""
            location_part = f" | Location: {location}" if location else ""
            rating_part = ""
            if float(row.get("avg_rating", 0) or 0) > 0 and int(row.get("review_count", 0) or 0) > 0:
                rating_part = f" | Rating: {product_rating_label(row)}"
            lines.append(f"- {row.get('name', 'Offering')} | Business: {shop.get('name', 'Business')} | Category: {shop.get('category', 'Business')} | {summary_bits}{rating_part}{location_part}{attr_part}{link_part}")
    else:
        lines.append("\nSample businesses:")
        counts = marketplace_shop_offering_counts(prod_rows)
        for shop in shops[:8]:
            location = shop.get("address") or shop.get("service_area") or "Location coming soon"
            website_part = f" | Website: {shop.get('website')}" if shop.get("website") else ""
            lines.append(f"- {shop.get('name', 'Business')} | Category: {shop.get('category', 'Business')} | Location: {location} | Offerings: {counts.get(str(shop.get('shop_id', '')), 0)}{website_part}")
    return "\n".join(lines)

def find_marketplace_out_of_scope_bold_terms(answer: str, shops: List[Dict[str, Any]], prod_rows: List[Dict[str, Any]]) -> List[str]:
    allowed = {norm_text("Atlantic Ordinate"), norm_text("Atlantica")}
    allowed.update(norm_text(shop.get("name", "")) for shop in shops if shop.get("name"))
    allowed.update(norm_text(shop.get("shop_id", "")) for shop in shops if shop.get("shop_id"))
    allowed.update(norm_text(shop.get("shop_slug", "")) for shop in shops if shop.get("shop_slug"))
    allowed.update(norm_text(row.get("name", "")) for row in prod_rows if row.get("name"))
    flagged: List[str] = []
    seen = set()
    for term in extract_markdown_bold_terms(answer):
        normalized = norm_text(term)
        if not normalized or normalized in allowed or normalized in MARKETPLACE_BOLD_TERMS:
            continue
        price_like = normalized.replace("cad", "").replace("usd", "").replace("inr", "")
        if re.fullmatch(r"[\d\s.,:$-]+", price_like):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        flagged.append(term)
    return flagged[:6]

def fallback_marketplace_answer(shops: List[Dict[str, Any]], picked: List[Dict[str, Any]], q: str, shop_map: Dict[str, Dict[str, Any]], focus_shop: Optional[Dict[str, Any]] = None) -> str:
    if is_greeting(q):
        return (
            f"Hi, I'm **Atlantica**. I can help you search across **{len(shops)}** businesses, compare prices, and point you to the right business page."
        )
    if focus_shop:
        opener = f"**{focus_shop.get('name', 'This business')}** is listed on Atlantica as a {str(focus_shop.get('category') or 'business').lower()} business."
        overview = str(focus_shop.get("overview", "") or "").strip()
        if picked:
            top = picked[0]
            shop = normalize_shop_record(shop_map.get(str(top.get("shop_id", "")), {}) or focus_shop or {})
            summary_bits = " | ".join(offering_summary_bits(top, shop)) or "Details available on request"
            return f"{opener} {overview} A good place to start is **{top.get('name', 'this offering')}** - {summary_bits}".strip()
        if overview:
            return f"{opener} {overview}".strip()
        return opener
    if picked:
        top = picked[0]
        shop = normalize_shop_record(shop_map.get(str(top.get("shop_id", "")), {}) or {})
        summary_bits = " | ".join(offering_summary_bits(top, shop)) or "Details available on request"
        return f"A strong marketplace match is **{top.get('name', 'Offering')}** from **{shop.get('name', 'Business')}** - {summary_bits}"
    return "I could not find a clear marketplace match yet. Try a product type, business name, category, or budget."

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — System
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def read_ui_html() -> str:
    with open(os.path.join(SERVER_DIR, "ui.html"), encoding="utf-8") as f:
        return f.read()

@app.get("/ui", response_class=HTMLResponse)
def serve_ui():
    return read_ui_html()

@app.get("/", response_class=HTMLResponse)
@app.get("/internal/review", response_class=HTMLResponse)
@app.get("/reset-password", response_class=HTMLResponse)
@app.get("/auth/callback", response_class=HTMLResponse)
def serve_app_shell():
    return read_ui_html()

@app.get("/business/{shop_ref}", response_class=HTMLResponse)
@app.get("/shop/{shop_ref}", response_class=HTMLResponse)
def serve_shop_ui(shop_ref: str):
    return read_ui_html()

@app.get("/offering/{shop_ref}/{product_ref}", response_class=HTMLResponse)
@app.get("/product/{shop_ref}/{product_ref}", response_class=HTMLResponse)
def serve_product_ui(shop_ref: str, product_ref: str):
    return read_ui_html()

@app.get("/favicon.ico")
def favicon():
    icon_path = os.path.join(SERVER_DIR, "atlantic-ordinate-favicon.png")
    if os.path.isfile(icon_path):
        return FileResponse(icon_path)
    raise HTTPException(204)

@app.get("/brand/{filename}")
def serve_brand_asset(filename: str):
    allowed = {"atlantic-ordinate-logo-mark.png", "atlantic-ordinate-favicon.png"}
    safe_name = os.path.basename(filename or "")
    if safe_name not in allowed:
        raise HTTPException(404, "Asset not found")
    target = os.path.join(SERVER_DIR, safe_name)
    if not os.path.isfile(target):
        raise HTTPException(404, "Asset not found")
    return FileResponse(target)

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
    email = require_clean_email(body.email or "")
    display_name = normalize_display_name(body.display_name, email)
    password = require_strong_password(body.password, "password", email=email, display_name=display_name)
    try:
        auth_client = require_supabase_auth()
        res = auth_client.auth.sign_up({
            "email": email,
            "password": password,
            "options": {
                "data": {"display_name": display_name},
                "email_redirect_to": ui_redirect_url(),
            },
        })
        if getattr(res, "user", None):
            safe_ensure_profile_row(res.user, preferred_display_name=display_name)
        return {"ok": True, "message": "Account created. Check your inbox for the verification link. If it is not there, check Spam, Junk, or Promotions."}
    except Exception as e:
        raise HTTPException(400, str(e))

@app.post("/auth/login")
def login(body: LoginReq, request: Request, response: Response):
    enforce_rate_limit(request, "auth_login", limit=12, window_seconds=900, key_suffix=clean_email(body.email or "").lower())
    email = require_clean_email(body.email or "")
    password = str(body.password or "")
    if not password:
        raise HTTPException(400, "Enter your password.")
    try:
        auth_client = require_supabase_auth()
        res = auth_client.auth.sign_in_with_password({"email": email, "password": password})
        user = res.user
        prof = safe_ensure_profile_row(user)
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
    refresh_token = str(body.refresh_token or "").strip()
    redirect_to = str(body.redirect_to or ui_redirect_url()).strip() or ui_redirect_url()
    if access_token:
        user, prof = get_user(f"Bearer {access_token}")
        prof = safe_ensure_profile_row(user, prof)
        set_auth_cookies(response, access_token, refresh_token)
        return {**auth_user_payload(user, prof), "access_token": access_token, "refresh_token": refresh_token}
    auth_client = require_supabase_auth()
    auth_code = str(body.auth_code or "").strip()
    token_hash = str(body.token_hash or "").strip()
    flow_type = str(body.type or "").strip().lower()
    try:
        if token_hash and flow_type:
            res = auth_client.auth.verify_otp({
                "token_hash": token_hash,
                "type": flow_type,
                "options": {"redirect_to": redirect_to},
            })
        elif auth_code:
            exchange_params = {
                "auth_code": auth_code,
                "redirect_to": redirect_to,
            }
            code_verifier = str(body.code_verifier or "").strip()
            if code_verifier:
                exchange_params["code_verifier"] = code_verifier
            res = auth_client.auth.exchange_code_for_session(exchange_params)
        else:
            raise HTTPException(400, "Missing access token or auth link parameters")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, str(e))
    access_token = str(getattr(res.session, "access_token", "") or "").strip()
    refresh_token = str(getattr(res.session, "refresh_token", "") or refresh_token).strip()
    if not access_token:
        raise HTTPException(400, "This email link did not create a usable session. Request a new link and try again.")
    user = res.user
    prof = safe_ensure_profile_row(user)
    expires_in = int(getattr(res.session, "expires_in", AUTH_ACCESS_COOKIE_MAX_AGE) or AUTH_ACCESS_COOKIE_MAX_AGE)
    set_auth_cookies(response, access_token, refresh_token, expires_in)
    return {**auth_user_payload(user, prof), "access_token": access_token, "refresh_token": refresh_token}

@app.post("/auth/refresh")
def auth_refresh(request: Request, response: Response):
    data = supabase_refresh_session(cookie_token(AUTH_REFRESH_COOKIE, request))
    access_token = str(data.get("access_token") or "").strip()
    refresh_token = str(data.get("refresh_token") or "").strip()
    expires_in = int(data.get("expires_in") or AUTH_ACCESS_COOKIE_MAX_AGE)
    set_auth_cookies(response, access_token, refresh_token, expires_in)
    user, prof = get_user(f"Bearer {access_token}")
    prof = safe_ensure_profile_row(user, prof)
    return auth_user_payload(user, prof)

@app.get("/auth/me")
def auth_me(request: Request, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization, request)
    return auth_user_payload(user, prof)

@app.post("/auth/logout")
def logout(request: Request, response: Response, authorization: Optional[str] = Header(None)):
    try:
        token = bearer(authorization, request)
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
    return {"ok": True, "message": "Verification email sent. Check your inbox, Spam, Junk, or Promotions."}

@app.post("/auth/forgot-password")
def forgot_password(body: ForgotPasswordReq, request: Request):
    enforce_rate_limit(request, "auth_forgot_password", limit=6, window_seconds=900, key_suffix=clean_email(body.email or "").lower())
    email = require_clean_email(body.email or "")
    try:
        supabase_auth_post("recover", {
            "email": email,
            "redirect_to": password_reset_redirect_url(),
        })
    except Exception as e:
        print(f"[Auth Warning] password reset request failed for {email}: {e}")
        raise HTTPException(400, "Could not send the reset link right now.")
    return {"ok": True, "message": "If that email exists, a reset link has been sent. Check your inbox, Spam, Junk, or Promotions."}

@app.post("/auth/update-password")
def update_password(body: ResetPasswordReq, request: Request, authorization: Optional[str] = Header(None)):
    enforce_rate_limit(request, "auth_update_password", limit=8, window_seconds=900)
    token = bearer(authorization, request)
    user, _ = get_user(f"Bearer {token}")
    password = require_strong_password(body.password, "new password", email=str(getattr(user, "email", "") or "")).strip()
    url = f"{SUPABASE_URL.rstrip('/')}/auth/v1/user"
    res = requests.put(url, headers=supabase_auth_headers(token), json={"password": password}, timeout=20)
    if res.status_code >= 400:
        try:
            detail = res.json().get("msg") or res.json().get("error_description") or res.json().get("message")
        except Exception:
            detail = res.text or "Could not update password."
        raise HTTPException(400, detail)
    return {"ok": True, "message": "Password updated successfully."}

@app.post("/auth/account-deletion-request")
def request_account_deletion(body: AccountDeletionRequestReq, request: Request, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization, request)
    enforce_rate_limit(request, "auth_delete_account_request", limit=3, window_seconds=86400, key_suffix=str(user.id))
    email = clean_email(getattr(user, "email", "") or "")
    confirm = str(body.confirm_text or "").strip()
    if confirm.upper() != "DELETE" and clean_email(confirm) != email:
        raise HTTPException(400, "Type DELETE or your account email to confirm this request.")
    reason = re.sub(r"\s+", " ", str(body.reason or "").strip())[:1200]
    sb = require_supabase()
    owned_businesses = sb.table("shops").select("shop_id, name").eq("owner_user_id", user.id).limit(20).execute().data or []
    requested_at = datetime.now(timezone.utc).isoformat()
    business_lines = [
        f"- {row.get('name') or 'Untitled Business'} ({row.get('shop_id') or 'no id'})"
        for row in owned_businesses
    ]
    report = send_review_notification_report(
        "Account deletion request",
        [
            "A signed-in user requested account deletion.",
            f"Requested at: {requested_at}",
            f"User ID: {user.id}",
            f"Email: {email}",
            f"Display name: {prof.get('display_name', '') or getattr(user, 'email', '')}",
            f"Email verified: {bool(getattr(user, 'email_confirmed_at', None))}",
            f"Owned businesses: {len(owned_businesses)}",
            *(business_lines or ["- None"]),
            f"Reason: {reason or 'Not provided'}",
            "",
            "Do not delete this user until business ownership, reviews, orders, favourites, and uploaded images have been reviewed.",
        ],
    )
    if int(report.get("sent_count") or 0) <= 0:
        raise HTTPException(503, "Account deletion requests are not configured yet. Contact support directly.")
    return {
        "ok": True,
        "message": "Account deletion request sent. We will review it before making irreversible changes.",
        "owned_business_count": len(owned_businesses),
    }

@app.put("/auth/profile")
def update_profile(body: UpdateProfileReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    profile = ensure_profile_row(user, prof, body.display_name)
    return {"ok": True, "display_name": profile.get("display_name", "")}

@app.post("/auth/profile/avatar")
def upload_avatar(authorization: Optional[str] = Header(None), avatar: UploadFile = File(...)):
    user, prof = get_user(authorization)
    sb = require_supabase()
    if not avatar.filename: raise HTTPException(400, "No file provided")
    try:
        ext, data, content_type = read_validated_image(avatar, "avatar")
        filename = f"user_{user.id}_{uuid.uuid4().hex[:8]}{ext}"
        path = f"{AVATAR_IMAGE_PREFIX}/{filename}"
        metadata = getattr(user, "user_metadata", {}) or {}
        url = upload_public_image(
            IMAGE_BUCKET,
            path,
            data,
            content_type,
            "Failed to upload avatar. Ensure the 'product-images' bucket is created and public in Supabase.",
        )
        existing = sb.table("profiles").select("id").eq("id", user.id).limit(1).execute().data or []
        if existing:
            sb.table("profiles").update({"avatar_url": url}).eq("id", user.id).execute()
        else:
            sb.table("profiles").insert({
                "id": user.id,
                "display_name": str(metadata.get("display_name", "") or ""),
                "avatar_url": url,
            }).execute()
        return {"ok": True, "avatar_url": url}
    except HTTPException:
        raise
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
        author_map: Dict[str, str] = {}
        user_ids = sorted({str(r.get("user_id", "")).strip() for r in rows if r.get("user_id")})
        if user_ids:
            try:
                profiles = supabase.table("profiles").select("id, display_name").in_("id", user_ids).execute().data or []
                author_map = {
                    str(profile.get("id", "")).strip(): str(profile.get("display_name", "") or "").strip()
                    for profile in profiles
                    if profile.get("id")
                }
            except Exception:
                author_map = {}
        out = []
        for r in rows:
            author_name = "User"
            if r.get("user_id"):
                author_name = author_map.get(str(r.get("user_id", "")).strip()) or "User"
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
    try:
        target_country = enforce_public_country_code(country_code)
        q = supabase.table("shops").select("*").order("created_at", desc=True)
        if category: q = q.ilike("category", f"%{category}%")
        rows = q.execute().data
        filtered = []
        for r in rows:
            nr = ensure_shop_coordinates(r)
            r.update(nr)
            if not shop_is_publicly_listable(r):
                continue
            if target_country and clean_code(r.get("country_code", "")) != target_country:
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
    except Exception as e:
        return public_browse_error_payload("businesses", e)

@app.get("/public/location-support")
def public_location_support():
    countries = []
    locked_country = active_market_country_code()
    items = [(locked_country, active_market_country_meta())] if locked_country else sorted(COUNTRY_META.items(), key=lambda item: item[1]["name"])
    for code, meta in items:
        if not code or not meta:
            continue
        countries.append({"code": code, "name": meta["name"], "currency_code": meta["currency"]})
    return {"ok": True, "countries": countries, "address_autocomplete": bool(MAPBOX_TOKEN)}

@app.get("/public/map-support")
def public_map_support():
    return {
        "ok": True,
        "enabled": leaflet_vendor_available(),
        "provider": "leaflet",
        "tile_provider": "openstreetmap",
        "geocoding_enabled": bool(MAPBOX_TOKEN),
        "public_token": "",
    }

@app.get("/public/timezone-support")
def public_timezone_support():
    return {"ok": True, "timezones": SUPPORTED_TIMEZONE_NAMES}

@app.get("/public/address/search")
def public_address_search(request: Request, q: str = Query(..., min_length=3), country: str = Query("")):
    enforce_rate_limit(request, "public_address_search", limit=30, window_seconds=300)
    if not MAPBOX_TOKEN:
        return {"ok": True, "suggestions": [], "provider": "disabled"}
    country_code = enforce_public_country_code(country)
    url, params = mapbox_geocoding_request(q, country_code=country_code, limit=5, autocomplete=True, types="address")
    try:
        res = requests.get(url, params=params, timeout=8)
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
    try:
        shop_row = ensure_shop_coordinates(ensure_market_shop_access(resolve_shop_by_ref(shop_ref)))
        shop_id = shop_row["shop_id"]
        user_id = optional_user_id(authorization, request)

        sort_key = str(sort or "default").strip().lower()
        stock_key = str(stock or "").strip().lower()
        attr_key = str(attr_key or "").strip()
        attr_value = str(attr_value or "").strip()
        can_use_db_paging = not attr_key and not attr_value and sort_key in {"", "default"}

        if can_use_db_paging:
            def build_shop_products_query():
                query = supabase.table("products").select("*", count="exact").eq("shop_id", shop_id)
                if stock_key in {"in", "low", "out"}:
                    query = query.eq("stock", stock_key)
                return query.order("updated_at", desc=True)

            def count_shop_products() -> int:
                query = supabase.table("products").select("product_id", count="exact", head=True).eq("shop_id", shop_id)
                if stock_key in {"in", "low", "out"}:
                    query = query.eq("stock", stock_key)
                try:
                    return int(query.execute().count or 0)
                except Exception:
                    return 0

            request_start = (page - 1) * limit
            request_end = request_start + limit - 1
            page_res = build_shop_products_query().range(request_start, request_end).execute()
            total = page_res.count if page_res.count is not None else count_shop_products()
            if not total and page_res.data:
                total = len(page_res.data or [])
            pagination = pagination_meta(total, page, limit)
            page_items = page_res.data or []
            if pagination["page"] != page and total:
                page_start = (pagination["page"] - 1) * limit
                page_end = page_start + limit - 1
                page_items = build_shop_products_query().range(page_start, page_end).execute().data or []
            all_prods = page_items
            paged = {"items": page_items, "pagination": pagination}
        else:
            q = supabase.table("products").select("*").eq("shop_id", shop_id)
            if stock_key in ("in", "low", "out"):
                q = q.eq("stock", stock_key)
            all_prods = q.execute().data
            if attr_key or attr_value:
                all_prods = [row for row in all_prods if matches_attribute_filter(row, shop_row.get("category", ""), attr_key, attr_value, shop_row.get("business_type", ""))]
            all_prods = sort_catalog_rows(all_prods, sort)
            paged = paginate_list(all_prods, page, limit)

        if page == 1 and sort_key in {"", "default"} and not str(stock or "").strip() and not str(attr_key or "").strip() and not str(attr_value or "").strip():
            track(shop_id, "shop_view")

        ser_prods = serialize_products_bulk(paged["items"], user_id, {shop_id: shop_row}, currency)

        return alias_catalog_response({
            "ok": True, "shop_id": shop_id, "shop_slug": shop_row.get("shop_slug", ""), "shop": public_shop_payload(shop_row),
            "products": ser_prods,
            "pagination": paged["pagination"],
            "stats": shop_stats(shop_id),
            "suggested_questions": dedup([*default_catalog_prompts(shop_row, all_prods), "Show all images", *category_prompt_suggestions(shop_row.get("category", ""), shop_row.get("business_type", ""), all_prods)])[:6]
        })
    except HTTPException:
        raise
    except Exception as exc:
        return public_browse_error_payload("shop", exc, page, limit)

@app.get("/public/business/{shop_ref}/offering/{product_ref}")
@app.get("/public/shop/{shop_ref}/offering/{product_ref}")
@app.get("/public/offering/{shop_ref}/{product_ref}")
@app.get("/public/product/{shop_ref}/{product_ref}")
def public_product(shop_ref: str, product_ref: str, request: Request, currency: str = Query(""), authorization: Optional[str] = Header(None)):
    try:
        shop_row = ensure_shop_coordinates(ensure_market_shop_access(resolve_shop_by_ref(shop_ref)))
        product_row = resolve_product_by_ref(shop_row["shop_id"], product_ref)
        user_id = optional_user_id(authorization, request)
        product = serialize_product(product_row, user_id, shop_row, currency)
        return alias_catalog_response({
            "ok": True,
            "shop_id": shop_row["shop_id"],
            "shop_slug": shop_row.get("shop_slug", ""),
            "shop": public_shop_payload(shop_row),
            "product": product,
            "offering": product,
            "products": [product],
            "offerings": [product],
            "pagination": paginate_list([product], 1, 1)["pagination"],
        })
    except HTTPException:
        raise
    except Exception as exc:
        payload = public_browse_error_payload("shop", exc, 1, 1)
        payload["product"] = {}
        payload["offering"] = {}
        return payload

@app.get("/public/business-search")
@app.get("/public/search")
def search_shop(request: Request, shop_id: str = Query(""), business_id: str = Query(""), q: str = Query(...), sort: str = Query("default"), stock: str = Query(""), attr_key: str = Query(""), attr_value: str = Query(""), currency: str = Query(""), page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=100), authorization: Optional[str] = Header(None)):
    try:
        qn = (q or "").strip()
        resolved_shop = ensure_market_shop_access(resolve_shop_by_ref(business_id or shop_id))
        real_shop_id = resolved_shop["shop_id"]
        if not qn:
            return alias_catalog_response({"ok": True, "shop_id": real_shop_id, "shop_slug": resolved_shop.get("shop_slug", ""), "q": q, "results": [], "total": 0, "pagination": paginate_list([], 1, limit)["pagination"]})
        shop_map = {real_shop_id: resolved_shop}
        shop_row = shop_map.get(real_shop_id, {})
        rows = get_shop_product_search_rows(real_shop_id)
        rows = filter_catalog_rows_by_stock(rows, stock)
        rows = [row for row in rows if product_matches_query(row, qn, shop_row.get("category", ""), shop_row.get("business_type", ""))]
        if attr_key or attr_value:
            rows = [row for row in rows if matches_attribute_filter(row, shop_row.get("category", ""), attr_key, attr_value, shop_row.get("business_type", ""))]
        rows = sort_catalog_rows(rows, sort)
        paged = paginate_list(rows, page, limit)
        user_id = optional_user_id(authorization, request)
        page_ids = [str(row.get("product_id", "")) for row in paged["items"] if row.get("product_id")]
        if page_ids:
            full_rows = supabase.table("products").select("*").eq("shop_id", real_shop_id).in_("product_id", page_ids).execute().data
            full_map = {str(row.get("product_id", "")): row for row in full_rows}
            page_rows = [full_map[product_id] for product_id in page_ids if product_id in full_map]
        else:
            page_rows = []
        return alias_catalog_response({"ok": True, "shop_id": real_shop_id, "shop_slug": resolved_shop.get("shop_slug", ""), "q": q, "results": serialize_products_bulk(page_rows, user_id, shop_map, currency), "total": len(rows), "pagination": paged["pagination"]})
    except HTTPException:
        raise
    except Exception as exc:
        return public_browse_error_payload("search", exc, page, limit)

@app.get("/public/offering-search")
@app.get("/public/search/global")
def search_global(request: Request, q: str = Query(...), attr_key: str = Query(""), attr_value: str = Query(""), currency: str = Query(""), page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=60)):
    qn = (q or "").strip()
    if not qn:
        return alias_catalog_response({"ok": True, "q": q, "results": [], "total": 0, "pagination": paginate_list([], 1, limit)["pagination"]})
    try:
        rows = supabase.table("products").select("*, shops(name, shop_slug, category, business_type, location_mode, service_area, address, formatted_address, whatsapp, country_code, country_name, currency_code, region, city, postal_code, street_line1, street_line2, listing_status)").order("updated_at", desc=True).execute().data
        normalized_rows = []
        for row in rows:
            shop = normalize_shop_record(row.get("shops", {}) or {})
            if not shop_matches_market_country(shop) or not shop_is_publicly_listable(shop):
                continue
            normalized_rows.append({**row, "shops": shop})
        rows = [row for row in normalized_rows if product_matches_query(row, qn, row.get("shops", {}).get("category", ""), row.get("shops", {}).get("business_type", ""))]
        if attr_key or attr_value:
            rows = [row for row in rows if matches_attribute_filter(row, row.get("shops", {}).get("category", ""), attr_key, attr_value, row.get("shops", {}).get("business_type", ""))]
        
        paged = paginate_list(rows, page, limit)
        shop_map = {}
        for row in paged["items"]:
            if row.get("shop_id"):
                shop_map[str(row.get("shop_id"))] = row.get("shops", {}) or {}
        results = serialize_products_bulk(paged["items"], None, shop_map, currency)
        for r, prod in zip(paged["items"], results):
            prod["shop_name"] = r.get("shops", {}).get("name", "")
            prod["shop_address"] = r.get("shops", {}).get("address", "")
            prod["shop_category"] = r.get("shops", {}).get("category", "")
            prod["business_name"] = prod.get("shop_name", "")
            prod["business_address"] = prod.get("shop_address", "")
            prod["business_category"] = prod.get("shop_category", "")
        return alias_catalog_response({"ok": True, "q": q, "results": results, "total": len(rows), "pagination": paged["pagination"]})
    except Exception as e:
        payload = public_browse_error_payload("search", e, page, limit)
        payload["q"] = q
        return payload

@app.get("/public/top-offerings")
@app.get("/public/top-products")
def top_products(request: Request, page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=60), category: str = Query(""), attr_key: str = Query(""), attr_value: str = Query(""), currency: str = Query("")):
    try:
        q = supabase.table("products").select("*, shops!inner(name, shop_slug, category, business_type, location_mode, service_area, address, formatted_address, country_code, country_name, currency_code, region, city, postal_code, street_line1, street_line2, listing_status)").neq("stock", "out")
        if category: q = q.ilike("shops.category", f"%{category}%")
        rows = q.execute().data
        normalized_rows = []
        for row in rows:
            shop = normalize_shop_record(row.get("shops", {}) or {})
            if not shop_matches_market_country(shop) or not shop_is_publicly_listable(shop):
                continue
            normalized_rows.append({**row, "shops": shop})
        rows = normalized_rows
        if attr_key or attr_value:
            rows = [row for row in rows if matches_attribute_filter(row, row.get("shops", {}).get("category", ""), attr_key, attr_value, row.get("shops", {}).get("business_type", ""))]
        
        paged = paginate_list(rows, page, limit)
        shop_map = {}
        for row in paged["items"]:
            if row.get("shop_id"):
                shop_map[str(row.get("shop_id"))] = row.get("shops", {}) or {}
        results = serialize_products_bulk(paged["items"], None, shop_map, currency)
        for r, prod in zip(paged["items"], results):
            prod["shop_name"] = r.get("shops", {}).get("name", "")
            prod["shop_category"] = r.get("shops", {}).get("category", "")
            prod["business_name"] = prod.get("shop_name", "")
            prod["business_category"] = prod.get("shop_category", "")
        return alias_catalog_response({"ok": True, "products": results, "pagination": paged["pagination"]})
    except Exception as e:
        return public_browse_error_payload("top_offerings", e, page, limit)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Chat 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/chat/global")
@app.get("/atlantica/chat")
def global_chat_endpoint(request: Request, q: str = Query(...), currency: str = Query("")):
    enforce_rate_limit(request, "global_chat", limit=40, window_seconds=300, key_suffix="marketplace")
    q = (q or "").strip()
    if not q:
        raise HTTPException(400, "Missing q")

    shops, shop_map, prod_rows = public_marketplace_snapshot(currency)
    summary = marketplace_summary(shops, prod_rows)
    focus_shop = detect_marketplace_focus_shop(shops, q)
    scope_shops = [focus_shop] if focus_shop else shops
    scope_shop_map = {str(focus_shop.get("shop_id", "")): focus_shop} if focus_shop else shop_map
    scope_prod_rows = marketplace_rows_for_shop(prod_rows, str(focus_shop.get("shop_id", ""))) if focus_shop else prod_rows
    focus_businesses = serialize_marketplace_businesses([focus_shop], prod_rows) if focus_shop else []
    focus_highlight_rows = []

    def respond(payload: Dict[str, Any], force_focus_highlight: bool = False) -> Dict[str, Any]:
        body = dict(payload or {})
        if focus_shop:
            body.setdefault("businesses", focus_businesses)
            if force_focus_highlight:
                body["products"] = serialize_marketplace_products(focus_highlight_rows, scope_shop_map, currency) if focus_highlight_rows else []
        return alias_catalog_response({
            **body,
            "assistant": "Atlantica",
            "mode": "global",
            "context": {"summary": summary},
        })

    if is_marketplace_app_query(q):
        return respond(answer_marketplace_app_query(shops, prod_rows))

    if is_marketplace_shop_profile_query(q, focus_shop):
        return respond(answer_marketplace_shop_profile_query(focus_shop or {}, scope_prod_rows, q, scope_shop_map, currency))

    shop_info_answer = answer_marketplace_shop_info_query(shops, prod_rows, q, shop_map, currency, focus_shop=focus_shop)
    if shop_info_answer is not None:
        return respond(shop_info_answer)

    open_now_answer = answer_marketplace_open_now_query(shops, prod_rows, q)
    if open_now_answer is not None:
        return respond(open_now_answer)

    business_list_answer = answer_marketplace_business_list_query(shops, prod_rows, q)
    if business_list_answer is not None:
        return respond(business_list_answer)

    rating_answer = answer_marketplace_rating_query(scope_shops, scope_prod_rows, q, scope_shop_map, currency)
    if rating_answer is not None:
        return respond(rating_answer)

    budget_answer = answer_marketplace_budget_query(scope_shops, scope_prod_rows, q, scope_shop_map, currency)
    if budget_answer is not None:
        return respond(budget_answer)

    cheapest_answer = answer_marketplace_cheapest_query(scope_shops, scope_prod_rows, q, scope_shop_map, currency)
    if cheapest_answer is not None:
        return respond(cheapest_answer)

    stock_answer = answer_marketplace_stock_query(scope_shops, scope_prod_rows, q, scope_shop_map, currency)
    if stock_answer is not None:
        return respond(stock_answer)

    price_answer = answer_marketplace_price_query(scope_shops, scope_prod_rows, q, scope_shop_map, currency)
    if price_answer is not None:
        return respond(price_answer)

    recommendation_answer = answer_marketplace_recommendation_query(scope_shops, scope_prod_rows, q, scope_shop_map, currency)
    if recommendation_answer is not None:
        return respond(recommendation_answer)

    ranked = match_marketplace_products(scope_prod_rows, q, scope_shop_map)
    picked = ranked[:8] if ranked else rank_marketplace_products(scope_prod_rows, q, scope_shop_map)[:8]
    suggestions = build_marketplace_chat_suggestions(q, picked, scope_shops, scope_shop_map)

    if is_greeting(q):
        return respond({
            "answer": (
                f"Hi, I'm **Atlantica**. I can help you search across **{summary['business_count']}** businesses and **{summary['offering_count']}** offerings, compare prices, and point you to the right business page."
            ),
            "products": [],
            "meta": {"llm_used": False, "reason": "market_greeting", "suggestions": suggestions},
        })

    if is_list_intent(q) or any(token in norm_text(q) for token in ["buy", "find", "looking for", "need", "want"]):
        return respond(answer_marketplace_catalog_query(scope_shops, scope_prod_rows, q, scope_shop_map, currency))

    try:
        system_prompt = ATLANTICA_SYSTEM + "\n" + marketplace_response_style_instructions(q, focus_shop)
        context_blob = f"MARKETPLACE CONTEXT:\n{build_marketplace_context(scope_shops, picked, scope_prod_rows, scope_shop_map, focus_shop=focus_shop)}\n\nUSER: {q}"
        llm_budget = marketplace_chat_max_tokens(q, picked)
        llm_res = llm_chat(system_prompt, context_blob, max_tokens=llm_budget)
        if should_retry_truncated_chat(llm_res):
            retry_prompt = system_prompt + "\nThe previous draft was cut off. Retry with a complete answer. Keep it concise, but finish every sentence and list cleanly."
            llm_res = llm_chat(retry_prompt, context_blob, max_tokens=max(llm_budget + 300, 1100))
        flagged_terms = find_marketplace_out_of_scope_bold_terms(llm_res.get("content", ""), scope_shops, scope_prod_rows)
        if flagged_terms:
            print(f"[Atlantica Scope Guard] blocked out-of-scope terms: {flagged_terms}")
            card_rows = take_chat_card_rows(picked) if product_card_query_intent(q, "", picked, focus_shop) else []
            return respond({
                "answer": fallback_marketplace_answer(scope_shops, picked, q, scope_shop_map, focus_shop=focus_shop),
                "products": serialize_marketplace_products(card_rows, scope_shop_map, currency) if card_rows else [],
                "meta": {"llm_used": False, "reason": "scope_guard", "suggestions": suggestions},
            })
        attached: List[Dict[str, Any]] = []
        if product_card_query_intent(q, llm_res.get("content", ""), picked, focus_shop):
            attached = picked
            if wants_product_image(q):
                attached = [row for row in picked if normalize_image_list(row.get("shop_id", ""), row.get("images", []))] or picked
            attached = take_chat_card_rows(attached)
        return respond({
            "answer": llm_res.get("content", "").strip() or fallback_marketplace_answer(scope_shops, picked, q, scope_shop_map, focus_shop=focus_shop),
            "products": serialize_marketplace_products(attached, scope_shop_map, currency) if attached else [],
            "meta": {
                "llm_used": True,
                "model": llm_res.get("model") or OPENROUTER_MODEL,
                "finish_reason": llm_res.get("finish_reason") or "stop",
                "suggestions": suggestions,
            },
        })
    except Exception as e:
        err_msg = str(e)
        if hasattr(e, "response") and getattr(e, "response") is not None:
            err_msg += f" | {e.response.text}"
        print(f"[Atlantica LLM Exception] Fallback triggered: {err_msg}")
        card_rows = take_chat_card_rows(picked) if product_card_query_intent(q, "", picked, focus_shop) else []
        return respond({
            "answer": fallback_marketplace_answer(scope_shops, picked, q, scope_shop_map, focus_shop=focus_shop),
            "products": serialize_marketplace_products(card_rows, scope_shop_map, currency) if card_rows else [],
            "meta": {"llm_used": False, "reason": "fallback_after_llm_error", "suggestions": suggestions},
        })

@app.get("/chat")
def chat_endpoint(request: Request, shop_id: str = Query(""), business_id: str = Query(""), q: str = Query(...), currency: str = Query("")):
    shop_ref = business_id or shop_id
    enforce_rate_limit(request, "chat", limit=40, window_seconds=300, key_suffix=str(shop_ref or "").strip().lower())
    q = (q or "").strip()
    if not q: raise HTTPException(400, "Missing q")
    shop = ensure_market_shop_access(resolve_shop_by_ref(shop_ref))
    shop_id = shop["shop_id"]
    def respond(payload: Dict[str, Any]) -> Dict[str, Any]:
        return alias_catalog_response({
            **(payload or {}),
            "business_id": shop_id,
            "business_slug": shop.get("shop_slug", ""),
        })
    track(shop_id, "chat")
    
    raw_prod_rows = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute().data or []
    review_map, view_map = load_product_metric_maps(raw_prod_rows)
    prod_rows = []
    for row in raw_prod_rows:
        key = (str(row.get("shop_id", "")), str(row.get("product_id", "")))
        ratings = review_map.get(key, [])
        prod_rows.append({
            **row,
            **get_display_price_fields(row, shop, currency),
            "avg_rating": round(sum(ratings) / len(ratings), 1) if ratings else 0,
            "review_count": len(ratings),
            "product_views": view_map.get(key, 0),
        })

    info_answer = answer_shop_info_query(shop, q)
    if info_answer is not None:
        return respond(info_answer)

    profile_answer = answer_business_shop_profile_query(shop, prod_rows, q, currency)
    if profile_answer is not None:
        return respond(profile_answer)

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
    response_cards: List[Dict[str, Any]] = []
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
        gallery_cards = select_chat_product_cards(prod_rows, shop, prefer_images=True)
        return respond({"answer": ans, "products": serialize_products_bulk(gallery_cards, None, {shop_id: shop}, currency), "meta": {"llm_used": False, "suggestions": suggestions}})

    # 2. Shortcut: Greeting
    if is_greeting(q):
        nouns = offering_nouns(shop, prod_rows)
        return respond({"answer": f"Hi! Welcome to **{shop['name']}**! Ask me about {nouns['plural']}, prices, availability, opening hours, or say '{default_catalog_question(shop, prod_rows).lower()}'.", "products": [], "meta": {"llm_used": False, "suggestions": suggestions}})

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
            if product_card_query_intent(q, "", picked, shop):
                response_cards = take_chat_card_rows(picked)
            return respond({
                "answer": fallback_answer_v2(shop, picked, q),
                "products": serialize_products_bulk(response_cards, None, {shop_id: shop}, currency) if response_cards else [],
                "meta": {
                    "llm_used": False,
                    "reason": "scope_guard",
                    "suggestions": suggestions,
                    "rag_matches": len(rag.get('matches', [])),
                }
            })
        if product_card_query_intent(q, llm_res["content"], picked, shop):
            attached = choose_chat_products(prod_rows, q, llm_res["content"], prefer_images=wants_product_image(q), limit=CHAT_CARD_MAX_RESULTS, category=shop.get("category", ""), business_type=shop.get("business_type", ""))
            if attached:
                response_cards = take_chat_card_rows(attached)
            elif wants_product_image(q):
                response_cards = select_chat_product_cards(picked, shop, prefer_images=True)
            else:
                response_cards = take_chat_card_rows(picked or select_chat_product_cards(prod_rows, shop))
        return respond({"answer": llm_res["content"], "products": serialize_products_bulk(response_cards, None, {shop_id: shop}, currency) if response_cards else [], "meta": {"llm_used": True, "model": llm_res.get("model") or OPENROUTER_MODEL, "suggestions": suggestions, "rag_matches": len(rag.get('matches', [])), "finish_reason": llm_res.get("finish_reason") or "stop"}})
    except Exception as e:
        err_msg = str(e)
        if hasattr(e, "response") and getattr(e, "response") is not None:
            err_msg += f" | {e.response.text}"
            
        print(f"[Chat LLM Exception] Fallback triggered: {err_msg}")
        
        ans = fallback_answer_v2(shop, picked, q)
        if product_card_query_intent(q, ans, picked, shop):
            response_cards = take_chat_card_rows(picked)

        return respond({
            "answer": ans,
            "products": serialize_products_bulk(response_cards, None, {shop_id: shop}, currency) if response_cards else [],
            "meta": {"llm_used": False, "reason": "fallback_after_llm_error", "suggestions": suggestions, "rag_matches": len(rag.get('matches', []))}
        })

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Admin 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_shop_owner(user_id: str, shop_id: str):
    rows = supabase.table("shops").select("*").eq("shop_id", shop_id).limit(1).execute().data or []
    if not rows:
        raise HTTPException(404, "Not found or not yours")
    shop = normalize_shop_record(rows[0])
    if str(shop.get("owner_user_id", "")) == str(user_id or ""):
        return
    if shop.get("is_platform_managed") and is_admin_profile(load_profile(user_id)):
        return
    raise HTTPException(404, "Not found or not yours")

def allocate_shop_identity(name: str) -> Tuple[str, str]:
    sid = gen_shop_id(name)
    shop_slug = unique_shop_slug(name)
    for _ in range(5):
        exists_res = supabase.table("shops").select("shop_id").eq("shop_id", sid).execute()
        if not exists_res.data:
            return sid, shop_slug
        sid = gen_shop_id(name)
    raise HTTPException(500, "Could not generate a unique shop ID")

def shop_create_strict_columns(shop: Dict[str, Any]) -> List[str]:
    strict_cols = list(SHOP_REVIEW_WRITE_COLUMNS)
    if shop.get("business_type") != "retail":
        strict_cols.append("business_type")
    if shop.get("location_mode") != "storefront":
        strict_cols.append("location_mode")
    if shop.get("service_area"):
        strict_cols.append("service_area")
    return strict_cols

def build_shop_insert_payload(
    shop: Dict[str, Any],
    user: Any,
    prof: Dict[str, Any],
    shop_slug: str,
    trust_snapshot: Dict[str, Any],
    *,
    listing_status: str,
    listing_source: str,
    ownership_status: str,
    owner_user_id: str,
    owner_contact_name: str,
    claimed_at: Optional[str],
    verified_at: Optional[str] = None,
    verification_submitted_at: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "shop_slug": shop_slug,
        "owner_user_id": owner_user_id,
        "listing_status": listing_status,
        "listing_source": listing_source,
        "ownership_status": ownership_status,
        "claimed_at": claimed_at,
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
        "phone_public": shop.get("phone_public", False),
        "website": shop.get("website", ""),
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
        "owner_contact_name": owner_contact_name or shop.get("owner_contact_name") or normalize_display_name(prof.get("display_name", ""), getattr(user, "email", "")),
        "verification_method": shop.get("verification_method", ""),
        "verification_evidence": shop.get("verification_evidence", ""),
        "verification_submitted_at": verification_submitted_at,
        "verified_at": verified_at,
        "verification_rejection_reason": "",
        "trust_flags": trust_snapshot["trust_flags"],
        "risk_score": trust_snapshot["risk_score"],
        "risk_level": trust_snapshot["risk_level"],
    }

@app.post("/admin/create-business")
@app.post("/admin/create-shop")
def create_shop(body: CreateShopReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    prof = safe_ensure_profile_row(user, prof)
    raw_business = body.business or body.shop
    if raw_business is None:
        raise HTTPException(400, "Business payload is required")
    shop = validate_shop_payload(raw_business)
    owner_rows, all_rows = load_shop_trust_rows(user.id)
    enforce_creator_draft_limits(user, owner_rows)
    trust_snapshot = build_shop_trust_snapshot(shop, user, owner_rows, all_rows, projected_new_unpublished=True)
    sid, shop_slug = allocate_shop_identity(shop["name"])
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = build_shop_insert_payload(
        shop,
        user,
        prof,
        shop_slug,
        trust_snapshot,
        listing_status=LISTING_STATUS_DRAFT,
        listing_source=LISTING_SOURCE_OWNER_CREATED,
        ownership_status=OWNERSHIP_STATUS_CLAIMED,
        owner_user_id=user.id,
        owner_contact_name=shop.get("owner_contact_name") or normalize_display_name(prof.get("display_name", ""), getattr(user, "email", "")),
        claimed_at=now_iso,
    )
    strict_cols = shop_create_strict_columns(shop)
    strict_cols.extend(SHOP_OWNERSHIP_WRITE_COLUMNS)
    payload, unsupported_cols = shop_write_payload_with_fallback(sid, payload, False, strict_cols)
    if unsupported_cols:
        print(f"[Shop Schema Warning] {sid}: saved without optional columns {unsupported_cols}")
    current_role = str((prof or {}).get("role", "") or "").strip().lower()
    next_role = current_role if current_role in {"admin", "superadmin"} else "shopkeeper"
    supabase.table("profiles").update({"role": next_role}).eq("id", user.id).execute()
    
    rebuild_kb(sid)
    return {"ok": True, "shop_id": sid, "business_id": sid, "shop_slug": shop_slug, "business_slug": shop_slug, "listing_status": LISTING_STATUS_DRAFT}

@app.post("/admin/managed-businesses")
@app.post("/admin/managed-shops")
def create_managed_shop(body: CreateManagedShopReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    prof = safe_ensure_profile_row(user, prof)
    require_admin_role(prof)
    raw_business = body.business or body.shop
    if raw_business is None:
        raise HTTPException(400, "Business payload is required")
    shop = validate_shop_payload(raw_business)
    shop["owner_contact_name"] = PLATFORM_MANAGED_OWNER_CONTACT
    if not shop.get("verification_method"):
        shop["verification_method"] = "manual"
    if not shop.get("verification_evidence"):
        shop["verification_evidence"] = "Staff-created platform-managed listing. Public page is claimable after publication."
    owner_rows, all_rows = load_shop_trust_rows("")
    trust_snapshot = build_shop_trust_snapshot(shop, user, owner_rows, all_rows, projected_new_unpublished=not bool(body.publish))
    sid, shop_slug = allocate_shop_identity(shop["name"])
    now_iso = datetime.now(timezone.utc).isoformat()
    listing_status = LISTING_STATUS_VERIFIED if body.publish else LISTING_STATUS_DRAFT
    payload = build_shop_insert_payload(
        shop,
        user,
        prof,
        shop_slug,
        trust_snapshot,
        listing_status=listing_status,
        listing_source=LISTING_SOURCE_PLATFORM_IMPORT,
        ownership_status=OWNERSHIP_STATUS_PLATFORM_MANAGED,
        owner_user_id=user.id,
        owner_contact_name=PLATFORM_MANAGED_OWNER_CONTACT,
        claimed_at=None,
        verified_at=now_iso if listing_status == LISTING_STATUS_VERIFIED else None,
        verification_submitted_at=now_iso if listing_status == LISTING_STATUS_VERIFIED else None,
    )
    strict_cols = shop_create_strict_columns(shop)
    strict_cols.extend(SHOP_OWNERSHIP_WRITE_COLUMNS)
    payload, unsupported_cols = shop_write_payload_with_fallback(sid, payload, False, strict_cols)
    if unsupported_cols:
        print(f"[Managed Shop Schema Warning] {sid}: saved without optional columns {unsupported_cols}")
    rebuild_kb(sid)
    return {
        "ok": True,
        "shop_id": sid,
        "business_id": sid,
        "shop_slug": shop_slug,
        "business_slug": shop_slug,
        "listing_status": listing_status,
        "listing_source": LISTING_SOURCE_PLATFORM_IMPORT,
        "ownership_status": OWNERSHIP_STATUS_PLATFORM_MANAGED,
        "is_platform_managed": True,
    }

@app.get("/admin/my-businesses")
@app.get("/admin/my-shops")
def my_shops(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    shops_res = supabase.table("shops").select("*").eq("owner_user_id", user.id).order("created_at", desc=True).execute()
    rows = [row for row in (shops_res.data or []) if not shop_is_platform_managed(row)]
    owner_rows, all_rows = load_shop_trust_rows(user.id)
    
    for r in rows:
        r.update(normalize_shop_record(r))
        trust_snapshot = build_shop_trust_snapshot(r, user, owner_rows, all_rows, ignore_shop_id=r.get("shop_id", ""))
        stats = shop_stats(r["shop_id"])
        r["stats"] = stats
        r["trust_flags"] = trust_snapshot["trust_flags"]
        r["risk_score"] = trust_snapshot["risk_score"]
        r["risk_level"] = trust_snapshot["risk_level"]
        r["quality_flags"] = [*shop_completeness_flags(r, stats), *r["trust_flags"]]
        r["review_requirements"] = shop_review_requirements(r, stats)
        r["review_ready"] = not r["review_requirements"]
        r["is_publicly_listed"] = shop_is_publicly_listable(r)
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
        _, persisted = fill_missing_shop_coordinates(row, persist=True, warning_label="Admin Geocode")
        if persisted:
            updated += 1
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
        "formatted_address", "profile_image_url", "timezone_name", "currency_code", "hours", "phone", "phone_public", "website", "whatsapp",
        "supports_pickup", "supports_delivery", "supports_walk_in", "delivery_radius_km", "delivery_fee", "pickup_notes", "overview"
    ])
    for row in rows:
        writer.writerow([
            row.get("shop_id", ""), row.get("name", ""), row.get("business_type", ""), row.get("location_mode", ""), row.get("service_area", ""), row.get("category", ""), row.get("country_code", ""),
            row.get("country_name", ""), row.get("region", ""), row.get("city", ""), row.get("postal_code", ""),
            row.get("formatted_address", "") or row.get("address", ""), row.get("profile_image_url", ""), row.get("timezone_name", ""),
            row.get("currency_code", ""), row.get("hours", ""), row.get("phone", ""), row.get("phone_public", False), row.get("website", ""), row.get("whatsapp", ""),
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
    writer.writerow(["shop_id", "product_id", "offering_type", "price_mode", "availability_mode", "name", "price_amount", "currency_code", "price", "stock", "stock_quantity", "duration_minutes", "capacity", "variants", "variant_data_json", "variant_matrix_json", "attribute_data_json", "external_links_json", "overview", "image_urls"])
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
            json.dumps(normalize_offering_external_links(row.get("external_links")), ensure_ascii=False),
            row.get("overview", ""),
            "|".join(normalize_image_list(row.get("shop_id", ""), row.get("images", []))),
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
    owner_rows, all_rows = load_shop_trust_rows(user.id)
    trust_snapshot = build_shop_trust_snapshot(shop, user, owner_rows, all_rows, ignore_shop_id=shop_id)
    prods = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute().data
    stats = shop_stats(shop_id)
    shop["trust_flags"] = trust_snapshot["trust_flags"]
    shop["risk_score"] = trust_snapshot["risk_score"]
    shop["risk_level"] = trust_snapshot["risk_level"]
    shop["quality_flags"] = [*shop_completeness_flags(shop, stats), *shop["trust_flags"]]
    shop["review_requirements"] = shop_review_requirements(shop, stats)
    shop["review_ready"] = not shop["review_requirements"]
    shop["is_publicly_listed"] = shop_is_publicly_listable(shop)
    
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
    existing_rows = supabase.table("shops").select("*").eq("shop_id", shop_id).limit(1).execute().data or []
    if not existing_rows:
        raise HTTPException(404, "Business not found")
    existing_shop = normalize_shop_record(existing_rows[0])
    shop = validate_shop_payload(body)
    owner_rows, all_rows = load_shop_trust_rows(user.id)
    trust_snapshot = build_shop_trust_snapshot(shop, user, owner_rows, all_rows, ignore_shop_id=shop_id)
    shop_slug = unique_shop_slug(shop["name"], shop_id)
    listing_status = existing_shop.get("listing_status", LISTING_STATUS_VERIFIED)
    owner_contact_name = shop.get("owner_contact_name") or existing_shop.get("owner_contact_name", "") or normalize_display_name((prof or {}).get("display_name", ""), getattr(user, "email", ""))
    payload = {
        "name": shop["name"].strip(),
        "shop_slug": shop_slug,
        "listing_status": listing_status,
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
        "phone_public": shop.get("phone_public", False),
        "website": shop.get("website", ""),
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
        "owner_contact_name": owner_contact_name,
        "verification_method": shop.get("verification_method", "") or existing_shop.get("verification_method", ""),
        "verification_evidence": shop.get("verification_evidence", "") or existing_shop.get("verification_evidence", ""),
        "verification_submitted_at": existing_shop.get("verification_submitted_at"),
        "verified_at": existing_shop.get("verified_at"),
        "verification_rejection_reason": existing_shop.get("verification_rejection_reason", "") if listing_status == LISTING_STATUS_REJECTED else "",
        "trust_flags": trust_snapshot["trust_flags"],
        "risk_score": trust_snapshot["risk_score"],
        "risk_level": trust_snapshot["risk_level"],
    }
    strict_cols = list(SHOP_REVIEW_WRITE_COLUMNS)
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
    return {"ok": True, "business_id": shop_id, "listing_status": listing_status}

@app.post("/admin/business/{shop_id}/submit-for-review")
@app.post("/admin/shop/{shop_id}/submit-for-review")
def admin_submit_shop_for_review(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    check_shop_owner(user.id, shop_id)
    rows = supabase.table("shops").select("*").eq("shop_id", shop_id).limit(1).execute().data or []
    if not rows:
        raise HTTPException(404, "Business not found")
    shop = normalize_shop_record(rows[0])
    if shop.get("listing_status") == LISTING_STATUS_VERIFIED:
        raise HTTPException(400, "This business is already verified and live.")
    if shop.get("listing_status") == LISTING_STATUS_PENDING_REVIEW:
        raise HTTPException(400, "This business is already waiting for review.")
    owner_rows, all_rows = load_shop_trust_rows(user.id)
    trust_snapshot = build_shop_trust_snapshot(shop, user, owner_rows, all_rows, ignore_shop_id=shop_id)
    stats = shop_stats(shop_id)
    requirements = shop_review_requirements(shop, stats)
    if requirements:
        raise HTTPException(400, requirements[0])
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = {
        "listing_status": LISTING_STATUS_PENDING_REVIEW,
        "verification_submitted_at": now_iso,
        "verification_rejection_reason": "",
        "trust_flags": trust_snapshot["trust_flags"],
        "risk_score": trust_snapshot["risk_score"],
        "risk_level": trust_snapshot["risk_level"],
    }
    if not shop.get("verified_at"):
        payload["verified_at"] = None
    payload, unsupported_cols = shop_write_payload_with_fallback(shop_id, payload, True, SHOP_REVIEW_WRITE_COLUMNS)
    if unsupported_cols:
        print(f"[Shop Schema Warning] {shop_id}: saved without optional columns {unsupported_cols}")
    try:
        send_review_notification(
            f"Business review needed: {shop.get('name', 'Business')}",
            [
                "A business was submitted for review in Atlantic Ordinate.",
                "",
                f"Business: {shop.get('name', 'Business')}",
                f"Business ID: {shop_id}",
                f"Owner contact: {shop.get('owner_contact_name', '') or 'Not provided'}",
                f"Owner account email: {clean_email(getattr(user, 'email', '') or '') or 'Not provided'}",
                f"Internal review phone: {shop.get('phone', '') or 'Not provided'}",
                f"Public WhatsApp: {shop.get('whatsapp', '') or 'Not provided'}",
                f"Location: {shop.get('formatted_address', '') or shop.get('service_area', '') or shop.get('address', '') or 'Not provided'}",
                f"Verification path: {shop.get('verification_method', '') or 'Not provided'}",
                f"Verification note: {shop.get('verification_evidence', '') or 'Not provided'}",
                "",
                f"Review in app: {app_route_url('/internal/review')}",
            ],
        )
    except Exception as notify_err:
        print(f"[Review Notification Warning] Business review email failed for {shop_id}: {notify_err}")
    return {"ok": True, "business_id": shop_id, "listing_status": LISTING_STATUS_PENDING_REVIEW, "verification_submitted_at": now_iso}

@app.post("/admin/business/{shop_id}/move-to-draft")
@app.post("/admin/shop/{shop_id}/move-to-draft")
def admin_move_shop_to_draft(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    check_shop_owner(user.id, shop_id)
    payload = {"listing_status": LISTING_STATUS_DRAFT}
    payload, unsupported_cols = shop_write_payload_with_fallback(shop_id, payload, True, SHOP_REVIEW_WRITE_COLUMNS)
    if unsupported_cols:
        print(f"[Shop Schema Warning] {shop_id}: saved without optional columns {unsupported_cols}")
    return {"ok": True, "business_id": shop_id, "listing_status": LISTING_STATUS_DRAFT}

@app.get("/admin/managed-businesses")
@app.get("/admin/managed-shops")
def admin_managed_businesses(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    prof = safe_ensure_profile_row(user, prof)
    require_admin_role(prof)
    rows = supabase.table("shops").select("*").order("created_at", desc=True).execute().data or []
    managed_rows = [normalize_shop_record(row) for row in rows if shop_is_platform_managed(row)]
    owner_email_map = load_auth_email_map([row.get("owner_user_id", "") for row in managed_rows])
    pending_counts: Dict[str, int] = {}
    shop_ids = [row.get("shop_id") for row in managed_rows if row.get("shop_id")]
    if shop_ids:
        try:
            pending_claims = supabase.table("business_claims").select("shop_id").in_("shop_id", shop_ids).eq("status", CLAIM_STATUS_PENDING).execute().data or []
            for claim in pending_claims:
                sid = str(claim.get("shop_id", "") or "")
                if sid:
                    pending_counts[sid] = pending_counts.get(sid, 0) + 1
        except Exception:
            pending_counts = {}
    owner_rows, all_rows = load_shop_trust_rows("")
    for row in managed_rows:
        row["owner_email"] = owner_email_map.get(str(row.get("owner_user_id", "")).strip(), "")
        stats = shop_stats(row["shop_id"])
        row["stats"] = stats
        trust_snapshot = build_shop_trust_snapshot(row, user, owner_rows, all_rows, ignore_shop_id=row.get("shop_id", ""))
        row["trust_flags"] = trust_snapshot["trust_flags"]
        row["risk_score"] = trust_snapshot["risk_score"]
        row["risk_level"] = trust_snapshot["risk_level"]
        row["quality_flags"] = [*shop_completeness_flags(row, stats), *row["trust_flags"]]
        row["review_requirements"] = shop_review_requirements(row, stats)
        row["review_ready"] = not row["review_requirements"]
        row["is_publicly_listed"] = shop_is_publicly_listable(row)
        row["pending_claim_count"] = pending_counts.get(str(row.get("shop_id", "")), 0)
    return alias_catalog_response({"ok": True, "shops": managed_rows})

@app.get("/admin/review/businesses")
@app.get("/admin/review/shops")
def admin_review_businesses(status: str = Query(LISTING_STATUS_PENDING_REVIEW), authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    prof = safe_ensure_profile_row(user, prof)
    require_admin_role(prof)
    status_key = normalize_listing_status(status, default=LISTING_STATUS_PENDING_REVIEW)
    rows = supabase.table("shops").select("*").eq("listing_status", status_key).order("verification_submitted_at", desc=True).order("created_at", desc=True).execute().data or []
    owner_email_map = load_auth_email_map([row.get("owner_user_id", "") for row in rows])
    for row in rows:
        row.update(normalize_shop_record(row))
        row["owner_email"] = owner_email_map.get(str(row.get("owner_user_id", "")).strip(), "")
        stats = shop_stats(row["shop_id"])
        row["stats"] = stats
        row["quality_flags"] = [*shop_completeness_flags(row, stats), *(row.get("trust_flags", []) or [])]
        row["review_requirements"] = shop_review_requirements(row, stats)
        row["review_ready"] = not row["review_requirements"]
        row["is_publicly_listed"] = shop_is_publicly_listable(row)
    rows = sorted(
        rows,
        key=lambda row: (
            int(row.get("risk_score") or 0),
            str(row.get("verification_submitted_at") or row.get("created_at") or ""),
        ),
        reverse=True,
    )
    return alias_catalog_response({"ok": True, "shops": rows})

@app.get("/admin/review/notification-status")
def admin_review_notification_status(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    prof = safe_ensure_profile_row(user, prof)
    require_admin_role(prof)
    return review_notification_status_payload()

@app.post("/admin/review/test-notification")
def admin_test_review_notification(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    prof = safe_ensure_profile_row(user, prof)
    require_admin_role(prof)
    status = review_notification_status_payload()
    if not status.get("smtp_ready"):
        raise HTTPException(400, "SMTP is not configured for review notifications.")
    if not status.get("recipient_count"):
        raise HTTPException(400, "No review notification recipients are configured.")
    now_iso = datetime.now(timezone.utc).isoformat()
    report = send_review_notification_report(
        "Atlantic Ordinate review notification test",
        [
            "This is a test review notification from Atlantic Ordinate.",
            "",
            f"Triggered by: {clean_email(getattr(user, 'email', '') or '') or 'Unknown admin'}",
            f"Sent at: {now_iso}",
            f"App: {app_route_url('/internal/review')}",
        ],
    )
    sent = int(report.get("sent_count") or 0)
    if sent <= 0:
        errors = report.get("errors") or []
        detail = str((errors[0] or {}).get("error") or "").strip() if errors else ""
        raise HTTPException(400, detail or "The test notification did not send. Check SMTP settings and recipient emails.")
    return {
        "ok": True,
        "sent_count": sent,
        "recipient_count": status.get("recipient_count", 0),
        "sent_at": now_iso,
    }

@app.post("/admin/review/business/{shop_id}/approve")
@app.post("/admin/review/shop/{shop_id}/approve")
def admin_approve_business(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    prof = safe_ensure_profile_row(user, prof)
    require_admin_role(prof)
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = {
        "listing_status": LISTING_STATUS_VERIFIED,
        "verified_at": now_iso,
        "verification_rejection_reason": "",
    }
    payload, unsupported_cols = shop_write_payload_with_fallback(shop_id, payload, True, SHOP_REVIEW_WRITE_COLUMNS)
    if unsupported_cols:
        print(f"[Shop Schema Warning] {shop_id}: saved without optional columns {unsupported_cols}")
    return {"ok": True, "business_id": shop_id, "listing_status": LISTING_STATUS_VERIFIED, "verified_at": now_iso}

@app.post("/admin/review/business/{shop_id}/reject")
@app.post("/admin/review/shop/{shop_id}/reject")
def admin_reject_business(shop_id: str, body: ReviewDecisionReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    prof = safe_ensure_profile_row(user, prof)
    require_admin_role(prof)
    reason = re.sub(r"\s+", " ", str(body.reason or "")).strip()
    if len(reason) < 5:
        raise HTTPException(400, "Add a short reason before rejecting the business.")
    payload = {
        "listing_status": LISTING_STATUS_REJECTED,
        "verification_rejection_reason": reason[:500],
    }
    payload, unsupported_cols = shop_write_payload_with_fallback(shop_id, payload, True, SHOP_REVIEW_WRITE_COLUMNS)
    if unsupported_cols:
        print(f"[Shop Schema Warning] {shop_id}: saved without optional columns {unsupported_cols}")
    return {"ok": True, "business_id": shop_id, "listing_status": LISTING_STATUS_REJECTED, "reason": reason[:500]}

@app.get("/admin/business-claim/candidates")
@app.get("/admin/business-claims/candidates")
def admin_business_claim_candidates(q: str = Query(""), limit: int = Query(BUSINESS_CLAIM_SEARCH_LIMIT, ge=1, le=20), authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    safe_ensure_profile_row(user, prof)
    rows = search_claimable_businesses(q, viewer_user_id=user.id, limit=limit)
    shop_ids = [row.get("shop_id") for row in rows if row.get("shop_id")]
    pending_by_shop: Dict[str, List[Dict[str, Any]]] = {}
    if shop_ids:
        try:
            pending_rows = supabase.table("business_claims").select("*").in_("shop_id", shop_ids).eq("status", CLAIM_STATUS_PENDING).execute().data or []
        except Exception as e:
            raise HTTPException(500, "Business claim database table is not ready. Run migrations/20260428_business_claims.sql.") from e
        for claim in pending_rows:
            pending_by_shop.setdefault(str(claim.get("shop_id", "")), []).append(claim)
    out = []
    for row in rows:
        shop = normalize_shop_record(row)
        pending = pending_by_shop.get(str(shop.get("shop_id", "")), [])
        shop["has_pending_claim"] = bool(pending)
        shop["has_my_pending_claim"] = any(str(claim.get("claimant_user_id", "")) == str(user.id) for claim in pending)
        out.append(shop)
    return {"ok": True, "businesses": out, "shops": out}

@app.post("/admin/business/{shop_id}/claim")
@app.post("/admin/shop/{shop_id}/claim")
def admin_create_business_claim(shop_id: str, body: BusinessClaimReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    prof = safe_ensure_profile_row(user, prof)
    shop_rows = supabase.table("shops").select(CLAIMABLE_SHOP_FIELDS).eq("shop_id", shop_id).limit(1).execute().data or []
    if not shop_rows:
        raise HTTPException(404, "Business not found")
    shop = normalize_shop_record(shop_rows[0])
    if str(shop.get("owner_user_id", "")) == str(user.id):
        raise HTTPException(400, "This business is already in your account.")
    if shop.get("listing_status") not in PUBLIC_LISTING_STATUSES:
        raise HTTPException(400, "Only live business pages can be claimed.")
    note = normalize_business_claim_note(body.note)
    if len(note) < BUSINESS_CLAIM_NOTE_MIN_LEN:
        raise HTTPException(400, "Add a short ownership note before requesting access.")
    try:
        existing_pending = supabase.table("business_claims").select("*").eq("shop_id", shop_id).eq("claimant_user_id", user.id).eq("status", CLAIM_STATUS_PENDING).limit(1).execute().data or []
        if existing_pending:
            claim = attach_business_claim_shops(existing_pending)[0]
            return {"ok": True, "claim": claim, "already_pending": True}
        any_pending = supabase.table("business_claims").select("claim_id").eq("shop_id", shop_id).eq("status", CLAIM_STATUS_PENDING).limit(1).execute().data or []
        if any_pending:
            raise HTTPException(409, "This business already has a pending ownership claim. Staff must review it first.")
        now_iso = datetime.now(timezone.utc).isoformat()
        payload = {
            "claim_id": gen_business_claim_id(shop_id),
            "shop_id": shop_id,
            "claimant_user_id": user.id,
            "claimant_display_name": normalize_display_name(prof.get("display_name", ""), getattr(user, "email", "")),
            "claimant_email": clean_email(getattr(user, "email", "") or ""),
            "note": note,
            "status": CLAIM_STATUS_PENDING,
            "review_note": "",
            "created_at": now_iso,
            "updated_at": now_iso,
        }
        supabase.table("business_claims").insert(payload).execute()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, "Could not create the claim request. Confirm the business_claims migration has run.") from e
    claim = attach_business_claim_shops([payload])[0]
    return {"ok": True, "claim": claim}

@app.get("/admin/my-business-claims")
@app.get("/admin/my-shop-claims")
def admin_my_business_claims(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    safe_ensure_profile_row(user, prof)
    try:
        rows = supabase.table("business_claims").select("*").eq("claimant_user_id", user.id).order("created_at", desc=True).execute().data or []
    except Exception as e:
        raise HTTPException(500, "Business claim database table is not ready. Run migrations/20260428_business_claims.sql.") from e
    claims = attach_business_claim_shops(rows)
    return {"ok": True, "claims": claims}

@app.get("/admin/review/business-claims")
@app.get("/admin/review/shop-claims")
def admin_review_business_claims(status: str = Query(CLAIM_STATUS_PENDING), authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    prof = safe_ensure_profile_row(user, prof)
    require_admin_role(prof)
    status_key = normalize_claim_status(status, default=CLAIM_STATUS_PENDING)
    try:
        rows = supabase.table("business_claims").select("*").eq("status", status_key).order("created_at", desc=False).execute().data or []
    except Exception as e:
        raise HTTPException(500, "Business claim database table is not ready. Run migrations/20260428_business_claims.sql.") from e
    claims = attach_business_claim_shops(rows)
    return {"ok": True, "claims": claims}

@app.post("/admin/review/business-claim/{claim_id}/approve")
@app.post("/admin/review/shop-claim/{claim_id}/approve")
def admin_approve_business_claim(claim_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    prof = safe_ensure_profile_row(user, prof)
    require_admin_role(prof)
    try:
        rows = supabase.table("business_claims").select("*").eq("claim_id", claim_id).limit(1).execute().data or []
    except Exception as e:
        raise HTTPException(500, "Business claim database table is not ready. Run migrations/20260428_business_claims.sql.") from e
    if not rows:
        raise HTTPException(404, "Claim not found")
    claim = normalize_business_claim_record(rows[0])
    if claim.get("status") != CLAIM_STATUS_PENDING:
        raise HTTPException(400, "Only pending claims can be approved.")
    shop_id = claim.get("shop_id", "")
    shop_rows = supabase.table("shops").select("*").eq("shop_id", shop_id).limit(1).execute().data or []
    if not shop_rows:
        raise HTTPException(404, "Business not found")
    shop = normalize_shop_record(shop_rows[0])
    now_iso = datetime.now(timezone.utc).isoformat()
    shop_payload = {
        "owner_user_id": claim.get("claimant_user_id"),
        "ownership_status": OWNERSHIP_STATUS_CLAIMED,
        "claimed_at": now_iso,
    }
    if shop_is_platform_managed(shop) and str(shop.get("owner_contact_name", "")).strip().lower() == PLATFORM_MANAGED_OWNER_CONTACT.lower():
        shop_payload["owner_contact_name"] = claim.get("claimant_display_name") or claim.get("claimant_email") or ""
    shop_payload, unsupported_cols = shop_write_payload_with_fallback(shop_id, shop_payload, True, [])
    if unsupported_cols:
        print(f"[Shop Schema Warning] {shop_id}: claim transfer saved without optional columns {unsupported_cols}")
    review_note = f"Approved by {clean_email(getattr(user, 'email', '') or '') or 'reviewer'}."
    supabase.table("business_claims").update({
        "status": CLAIM_STATUS_APPROVED,
        "review_note": review_note,
        "updated_at": now_iso,
    }).eq("claim_id", claim_id).execute()
    other_pending = supabase.table("business_claims").select("claim_id").eq("shop_id", shop_id).eq("status", CLAIM_STATUS_PENDING).execute().data or []
    for other in other_pending:
        other_id = str(other.get("claim_id", "") or "")
        if other_id and other_id != claim_id:
            supabase.table("business_claims").update({
                "status": CLAIM_STATUS_REJECTED,
                "review_note": "Another ownership claim for this business was approved.",
                "updated_at": now_iso,
            }).eq("claim_id", other_id).execute()
    return {"ok": True, "business_id": shop_id, "shop_id": shop_id, "claim_id": claim_id, "owner_user_id": claim.get("claimant_user_id")}

@app.post("/admin/review/business-claim/{claim_id}/reject")
@app.post("/admin/review/shop-claim/{claim_id}/reject")
def admin_reject_business_claim(claim_id: str, body: ReviewDecisionReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    prof = safe_ensure_profile_row(user, prof)
    require_admin_role(prof)
    reason = normalize_business_claim_note(body.reason)
    if len(reason) < 5:
        raise HTTPException(400, "Add a short reason before rejecting the claim.")
    try:
        rows = supabase.table("business_claims").select("*").eq("claim_id", claim_id).limit(1).execute().data or []
    except Exception as e:
        raise HTTPException(500, "Business claim database table is not ready. Run migrations/20260428_business_claims.sql.") from e
    if not rows:
        raise HTTPException(404, "Claim not found")
    claim = normalize_business_claim_record(rows[0])
    if claim.get("status") != CLAIM_STATUS_PENDING:
        raise HTTPException(400, "Only pending claims can be rejected.")
    now_iso = datetime.now(timezone.utc).isoformat()
    supabase.table("business_claims").update({
        "status": CLAIM_STATUS_REJECTED,
        "review_note": reason,
        "updated_at": now_iso,
    }).eq("claim_id", claim_id).execute()
    return {"ok": True, "claim_id": claim_id, "status": CLAIM_STATUS_REJECTED, "reason": reason}

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
        raise HTTPException(400, "This business does not offer delivery")
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

def delete_business_data(shop_id: str) -> Dict[str, Any]:
    sid = str(shop_id or "").strip()
    if not sid:
        raise HTTPException(400, "Business ID is required.")
    sb = require_supabase()
    image_names: List[str] = []
    profile_image_url = ""
    try:
        shop_rows = sb.table("shops").select("profile_image_url").eq("shop_id", sid).limit(1).execute().data or []
        profile_image_url = normalize_shop_record(shop_rows[0]).get("profile_image_url", "") if shop_rows else ""
    except Exception as e:
        print(f"[Business Delete Warning] could not load business profile image for {sid}: {e}")
    try:
        product_rows = sb.table("products").select("images").eq("shop_id", sid).execute().data or []
        for row in product_rows:
            for image_url in normalize_image_list(sid, row.get("images", [])):
                img_name = os.path.basename(str(image_url or "").rstrip("/"))
                if img_name and img_name not in image_names:
                    image_names.append(img_name)
    except Exception as e:
        print(f"[Business Delete Warning] could not enumerate product images for {sid}: {e}")
    for table_name in ("favourites", "reviews", "analytics", "order_requests", "products"):
        try:
            sb.table(table_name).delete().eq("shop_id", sid).execute()
        except Exception as e:
            print(f"[Business Delete Warning] could not delete {table_name} rows for {sid}: {e}")
    if image_names:
        try:
            sb.storage.from_(IMAGE_BUCKET).remove([f"{sid}/{img_name}" for img_name in image_names])
        except Exception as e:
            print(f"[Business Delete Warning] could not remove storage images for {sid}: {e}")
    if profile_image_url:
        remove_public_image(IMAGE_BUCKET, profile_image_url)
    sb.table("shops").delete().eq("shop_id", sid).execute()
    invalidate_shop_product_search_cache(sid)
    shutil.rmtree(os.path.join(SHOPS_DIR, sid), ignore_errors=True)
    return {"business_id": sid, "removed_product_images": len(image_names), "removed_profile_image": bool(profile_image_url)}

def safe_table_count_eq(table_name: str, column_name: str, value: Any) -> Optional[int]:
    try:
        res = require_supabase().table(table_name).select("*", count="exact", head=True).eq(column_name, value).execute()
        return int(getattr(res, "count", 0) or 0)
    except Exception as e:
        print(f"[Account Delete Preview Warning] could not count {table_name}.{column_name}: {e}")
        return None

def safe_table_count_in(table_name: str, column_name: str, values: List[Any]) -> Optional[int]:
    vals = [v for v in values if str(v or "").strip()]
    if not vals:
        return 0
    try:
        res = require_supabase().table(table_name).select("*", count="exact", head=True).in_(column_name, vals).execute()
        return int(getattr(res, "count", 0) or 0)
    except Exception as e:
        print(f"[Account Delete Preview Warning] could not count {table_name}.{column_name}: {e}")
        return None

def account_deletion_snapshot(target_user_id: str, target_email: str = "") -> Dict[str, Any]:
    sb = require_supabase()
    uid = str(target_user_id or "").strip()
    email = clean_email(target_email)
    profile_rows = sb.table("profiles").select("*").eq("id", uid).limit(1).execute().data or []
    profile = profile_rows[0] if profile_rows else {}
    owned_businesses = sb.table("shops").select("shop_id, name, profile_image_url").eq("owner_user_id", uid).execute().data or []
    business_ids = [row.get("shop_id") for row in owned_businesses if row.get("shop_id")]
    counts = {
        "profile_rows": len(profile_rows),
        "owned_businesses": len(owned_businesses),
        "owned_products": safe_table_count_in("products", "shop_id", business_ids),
        "owned_business_reviews": safe_table_count_in("reviews", "shop_id", business_ids),
        "owned_business_favourites": safe_table_count_in("favourites", "shop_id", business_ids),
        "owned_business_analytics": safe_table_count_in("analytics", "shop_id", business_ids),
        "owned_business_order_requests": safe_table_count_in("order_requests", "shop_id", business_ids),
        "authored_reviews": safe_table_count_eq("reviews", "user_id", uid),
        "saved_favourites": safe_table_count_eq("favourites", "user_id", uid),
        "customer_order_requests": safe_table_count_eq("order_requests", "customer_email", email) if email else 0,
    }
    return {
        "user_id": uid,
        "email": email,
        "display_name": profile.get("display_name", ""),
        "role": profile.get("role", "customer") or "customer",
        "avatar_url": profile.get("avatar_url", ""),
        "owned_businesses": [{"shop_id": row.get("shop_id", ""), "name": row.get("name", "")} for row in owned_businesses],
        "counts": counts,
    }

def execute_account_deletion(target_user_id: str, target_email: str, delete_owned_businesses: bool) -> Dict[str, Any]:
    sb = require_supabase()
    uid = str(target_user_id or "").strip()
    email = clean_email(target_email)
    snapshot = account_deletion_snapshot(uid, email)
    if snapshot["owned_businesses"] and not delete_owned_businesses:
        raise HTTPException(400, "This user owns businesses. Check 'delete owned businesses' or transfer them before deleting the account.")
    removed_businesses = []
    for row in snapshot["owned_businesses"]:
        removed_businesses.append(delete_business_data(row.get("shop_id", "")))
    if snapshot.get("avatar_url"):
        remove_public_image(IMAGE_BUCKET, snapshot.get("avatar_url", ""))
    if email:
        try:
            sb.table("order_requests").update({
                "customer_name": "Deleted user",
                "customer_email": "",
                "phone": "",
                "delivery_address": "",
                "note": "",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }).eq("customer_email", email).execute()
        except Exception as e:
            print(f"[Account Delete Warning] could not anonymize order requests for {email}: {e}")
    for table_name in ("favourites", "reviews"):
        try:
            sb.table(table_name).delete().eq("user_id", uid).execute()
        except Exception as e:
            print(f"[Account Delete Warning] could not delete {table_name} rows for {uid}: {e}")
    try:
        sb.table("profiles").delete().eq("id", uid).execute()
    except Exception as e:
        print(f"[Account Delete Warning] could not delete profile row for {uid}: {e}")
    delete_supabase_auth_user(uid)
    return {"snapshot": snapshot, "removed_businesses": removed_businesses}

@app.post("/admin/account-deletion/{target_user_id}/process")
@app.post("/admin/user/{target_user_id}/delete-account")
def admin_process_account_deletion(target_user_id: str, body: AdminAccountDeletionProcessReq, authorization: Optional[str] = Header(None)):
    admin_user, admin_prof = get_user(authorization)
    admin_prof = safe_ensure_profile_row(admin_user, admin_prof)
    require_admin_role(admin_prof)
    uid = str(target_user_id or "").strip()
    if not uid:
        raise HTTPException(400, "User ID is required.")
    if uid == str(admin_user.id):
        raise HTTPException(400, "Use another superadmin account to delete the account you are currently signed in with.")
    target_auth_user = load_auth_user_by_id(uid)
    target_email = auth_user_email(target_auth_user)
    if not target_email:
        raise HTTPException(400, "This auth user does not have an email address to confirm against.")
    supplied_email = clean_email(body.confirm_email)
    if supplied_email and supplied_email != target_email:
        raise HTTPException(400, "The confirmation email does not match this auth user.")
    snapshot = account_deletion_snapshot(uid, target_email)
    target_role = str(snapshot.get("role", "") or "").strip().lower()
    admin_role = str((admin_prof or {}).get("role", "") or "").strip().lower()
    if target_role in {"admin", "superadmin"} and admin_role != "superadmin":
        raise HTTPException(403, "Only a superadmin can delete another admin account.")
    if body.dry_run:
        return {"ok": True, "dry_run": True, "snapshot": snapshot}
    expected_phrase = f"DELETE {target_email}"
    if not target_email or supplied_email != target_email or str(body.confirm_text or "").strip() != expected_phrase:
        raise HTTPException(400, f"Type the exact confirmation phrase: {expected_phrase}")
    result = execute_account_deletion(uid, target_email, bool(body.delete_owned_businesses))
    try:
        send_review_notification_report(
            "Account deleted",
            [
                "An admin processed an account deletion in Atlantic Ordinate.",
                f"Deleted user ID: {uid}",
                f"Deleted email: {target_email}",
                f"Admin user ID: {admin_user.id}",
                f"Admin email: {clean_email(getattr(admin_user, 'email', '') or '')}",
                f"Owned businesses deleted: {len(result.get('removed_businesses') or [])}",
            ],
        )
    except Exception as e:
        print(f"[Account Delete Warning] deletion notification failed for {uid}: {e}")
    return {"ok": True, "dry_run": False, "message": "Account deleted from Supabase auth and app database.", **result}

@app.delete("/admin/business/{shop_id}")
@app.delete("/admin/shop/{shop_id}")
def admin_delete_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    return {"ok": True, **delete_business_data(shop_id)}

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
    strict_cols = product_strict_columns_for_data(data)
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
    invalidate_shop_product_search_cache(shop_id)
    rebuild_kb(shop_id)
    return {"ok": True, "business_id": shop_id, "offering_id": product_id}

def catalog_product_image_count(product: Product) -> int:
    count = len(product.images or [])
    count += sum(len(item.get("images") or []) for item in (product.variant_data or []) if isinstance(item, dict))
    count += sum(len(item.get("images") or []) for item in (product.variant_matrix or []) if isinstance(item, dict))
    return count

def read_catalog_upload(data: bytes, filename: str) -> Tuple[List[Dict[str, str]], Dict[str, Tuple[str, bytes]], List[str]]:
    csv_text, assets = build_catalog_asset_map(data, filename)
    try:
        reader = csv.DictReader(StringIO(csv_text))
        raw_rows = list(reader)
    except Exception:
        raise HTTPException(400, "Could not read the catalog spreadsheet.")
    if not raw_rows:
        raise HTTPException(400, "The catalog spreadsheet has no offering rows.")
    headers = [normalize_catalog_header(field or "") for field in (reader.fieldnames or []) if field]
    if "name" not in headers and "product_name" not in headers and "offering_name" not in headers and "title" not in headers:
        raise HTTPException(400, "The catalog spreadsheet needs a name column.")
    rows = [
        {normalize_catalog_header(key): str(value or "").strip() for key, value in (raw_row or {}).items() if key}
        for raw_row in raw_rows
    ]
    return rows, assets, headers

@app.post("/admin/business/{shop_id}/offerings-import-preview")
@app.post("/admin/shop/{shop_id}/products-import-preview")
async def admin_preview_offerings_package(
    shop_id: str,
    catalog_package: UploadFile = File(...),
    authorization: Optional[str] = Header(None),
):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    shop_rows = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data
    if not shop_rows:
        raise HTTPException(404, "Business not found")
    shop = normalize_shop_record(shop_rows[0])
    raw = await catalog_package.read(MAX_CATALOG_PACKAGE_BYTES + 1)
    if not raw:
        raise HTTPException(400, "Upload a catalog ZIP package, CSV file, or Excel file.")
    if len(raw) > MAX_CATALOG_PACKAGE_BYTES:
        raise HTTPException(400, "Catalog package is too large.")
    rows, assets, headers = read_catalog_upload(raw, str(catalog_package.filename or ""))
    existing_ids = {
        str(row.get("product_id") or "")
        for row in (supabase.table("products").select("product_id").eq("shop_id", shop_id).execute().data or [])
        if row.get("product_id")
    }
    uploaded: Dict[str, str] = {}
    errors: List[str] = []
    preview_rows: List[Dict[str, Any]] = []
    seen_ids: Dict[str, int] = {}
    valid = 0
    failed = 0
    for idx, row in enumerate(rows, start=2):
        row_errors: List[str] = []
        try:
            pid, product = build_product_from_catalog_row(shop_id, shop, row, idx, assets, uploaded, row_errors, save_assets=False)
            if row_errors:
                raise ValueError("; ".join(row_errors))
            if pid in seen_ids:
                raise ValueError(f"duplicate SKU/product_id also appears on row {seen_ids[pid]}")
            seen_ids[pid] = idx
            data = normalize_product_payload(product, shop)
            image_count = catalog_product_image_count(product)
            valid += 1
            preview_rows.append({
                "row": idx,
                "product_id": pid,
                "offering_id": pid,
                "name": product.name,
                "offering_type": data.get("offering_type", ""),
                "action": "update" if pid in existing_ids else "create",
                "price": data.get("price", ""),
                "stock_quantity": data.get("stock_quantity"),
                "image_count": image_count,
                "variant_count": len(product.variant_data or []),
                "combination_count": len(product.variant_matrix or []),
            })
        except Exception as e:
            failed += 1
            label = catalog_value(row, "product_id", "offering_id", "sku", "name", "product_name") or f"row {idx}"
            detail = str(getattr(e, "detail", None) or e)
            errors.append(f"Row {idx} ({label}): {detail}")
    return {
        "ok": failed == 0,
        "business_id": shop_id,
        "total": len(rows),
        "valid": valid,
        "failed": failed,
        "asset_count": len(assets),
        "headers": headers,
        "rows": preview_rows[:80],
        "errors": errors[:200],
        "error_count": len(errors),
    }

@app.post("/admin/business/{shop_id}/offerings-import-package")
@app.post("/admin/shop/{shop_id}/products-import-package")
async def admin_import_offerings_package(
    shop_id: str,
    catalog_package: UploadFile = File(...),
    authorization: Optional[str] = Header(None),
):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    shop_rows = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data
    if not shop_rows:
        raise HTTPException(404, "Business not found")
    shop = normalize_shop_record(shop_rows[0])
    raw = await catalog_package.read(MAX_CATALOG_PACKAGE_BYTES + 1)
    if not raw:
        raise HTTPException(400, "Upload a catalog ZIP package or CSV file.")
    if len(raw) > MAX_CATALOG_PACKAGE_BYTES:
        raise HTTPException(400, "Catalog package is too large.")
    raw_rows, assets, _headers = read_catalog_upload(raw, str(catalog_package.filename or ""))

    uploaded: Dict[str, str] = {}
    imported = 0
    failed = 0
    errors: List[str] = []
    for idx, row in enumerate(raw_rows, start=2):
        row_errors: List[str] = []
        try:
            pid, product = build_product_from_catalog_row(shop_id, shop, row, idx, assets, uploaded, row_errors)
            if row_errors:
                raise ValueError("; ".join(row_errors))
            existing = supabase.table("products").select("images").eq("shop_id", shop_id).eq("product_id", pid).execute().data
            if not existing and not catalog_value(row, "product_id", "offering_id", "sku", "id"):
                for _ in range(5):
                    dupe = supabase.table("products").select("product_id").eq("shop_id", shop_id).eq("product_id", pid).execute().data
                    if not dupe:
                        break
                    pid = gen_product_id(product.name)
                    product.product_id = pid
                    product.offering_id = pid
                else:
                    raise ValueError("could not generate a unique product ID")
            data = normalize_product_payload(product, shop)
            if existing and not data.get("images"):
                data["images"] = existing[0].get("images", [])
            elif existing and data.get("images"):
                data["images"] = dedup(normalize_image_list(shop_id, existing[0].get("images", [])) + data.get("images", []))
            data["product_slug"] = unique_product_slug(shop_id, product.name, pid if existing else "")
            data, unsupported_cols = product_write_payload_with_fallback(shop_id, pid, data, bool(existing), product_strict_columns_for_data(data))
            if unsupported_cols:
                print(f"[Product Schema Warning] {shop_id}/{pid}: saved without optional columns {unsupported_cols}")
            imported += 1
        except Exception as e:
            failed += 1
            label = catalog_value(row, "product_id", "sku", "name", "product_name") or f"row {idx}"
            detail = str(getattr(e, "detail", None) or e)
            errors.append(f"Row {idx} ({label}): {detail}")
    if imported:
        rebuild_kb(shop_id)
    return {
        "ok": failed == 0,
        "business_id": shop_id,
        "imported": imported,
        "failed": failed,
        "errors": errors[:200],
        "error_count": len(errors),
    }

@app.post("/admin/business/{shop_id}/offering-with-images")
@app.post("/admin/shop/{shop_id}/product-with-images")
async def admin_product_with_images(
    request: Request,
    shop_id: str, authorization: Optional[str] = Header(None),
    product_id: str = Form(""), offering_id: str = Form(""), name: str = Form(...), overview: str = Form(""), price: str = Form(""),
    price_amount: str = Form(""), currency_code: str = Form(""),
    offering_type: str = Form(""), price_mode: str = Form("fixed"), availability_mode: str = Form(""),
    stock: str = Form("in"), stock_quantity: str = Form(""), duration_minutes: str = Form(""), capacity: str = Form(""),
    variants: str = Form(""), variant_data_json: str = Form(""), variant_matrix_json: str = Form(""), attribute_data_json: str = Form(""), external_links_json: str = Form(""),
    image_urls: str = Form(""), images_json: str = Form(""), images: List[UploadFile] = File(default=[])
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

    referenced_urls = parse_image_ref_field(shop_id, images_json or image_urls)
    new_urls = save_images(shop_id, images)
    merged = dedup(current_imgs + referenced_urls + new_urls)
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
    parsed_external_links = normalize_offering_external_links(external_links_json, strict=True)
    
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
        external_links=parsed_external_links,
        images=merged,
    ), shop)
    data["product_slug"] = unique_product_slug(shop_id, name.strip(), pid if existing else "")
    strict_cols = product_strict_columns_for_data(data)
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
