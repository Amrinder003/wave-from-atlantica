"""
Wave API  v4.0 — Supabase Edition (Stable & Fixed Chat)
"""

from fastapi import FastAPI, Query, Header, HTTPException, UploadFile, File, Request, Form
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import os, re, json, time, uuid, shutil
from datetime import datetime, timezone
from difflib import SequenceMatcher
import requests
from typing import Any, Dict, List, Optional
from supabase import create_client, Client

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
os.makedirs(SHOPS_DIR, exist_ok=True)

PAGE_SIZE         = 24                   
ALLOWED_IMG_EXTS  = {".png", ".jpg", ".jpeg", ".webp", ".jfif", ".jif"}

OPENROUTER_KEY    = os.environ.get("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL  = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4.1-mini").strip()
OPENROUTER_FALLBACK_MODELS = [m.strip() for m in os.environ.get("OPENROUTER_FALLBACK_MODELS", "").split(",") if m.strip()]
OPENROUTER_TEMPERATURE = float(os.environ.get("OPENROUTER_TEMPERATURE", "0.2") or 0.2)
OPENROUTER_MAX_TOKENS = int(os.environ.get("OPENROUTER_MAX_TOKENS", "400") or 400)
OPENROUTER_URL    = "https://openrouter.ai/api/v1/chat/completions"

SUPABASE_URL      = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY      = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
APP_BASE_URL      = os.environ.get("APP_BASE_URL", "http://localhost:8001").strip()
CORS_ORIGINS      = [o.strip() for o in os.environ.get("CORS_ORIGINS", APP_BASE_URL).split(",") if o.strip()]

if not SUPABASE_URL or not SUPABASE_KEY:
    print("⚠️ WARNING: Missing SUPABASE_URL or SUPABASE_SERVICE_KEY.")
supabase: Optional[Client] = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PYDANTIC MODELS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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

class ShopInfo(BaseModel):
    name: str
    address: str
    overview: str = ""
    phone: str = ""
    hours: str = ""
    category: str = ""
    whatsapp: str = ""

class Product(BaseModel):
    product_id: str = Field(..., description="e.g. p001")
    name: str
    overview: str = ""
    price: str = ""
    stock: str = "in"
    variants: str = ""
    images: List[str] = []

class CreateShopReq(BaseModel):
    shop_id: Optional[str] = None
    shop: ShopInfo

class ReviewReq(BaseModel):
    rating: int = Field(..., ge=1, le=5)
    body: str

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AUTH HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def bearer(auth: Optional[str]) -> str:
    if not auth: raise HTTPException(401, "Missing Authorization header")
    m = re.match(r"Bearer\s+(.+)", auth.strip(), re.I)
    if not m: raise HTTPException(401, "Use: Bearer <token>")
    return m.group(1).strip()

def get_user(auth: Optional[str]):
    token = bearer(auth)
    try:
        res = supabase.auth.get_user(token)
        user = res.user
        prof_res = supabase.table("profiles").select("*").eq("id", user.id).execute()
        prof = prof_res.data[0] if prof_res.data else {}
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

def ui_redirect_url() -> str:
    base = (APP_BASE_URL or "http://localhost:8001").strip().rstrip("/")
    return base if base.endswith("/ui") else f"{base}/ui"

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
    return {
        "content": (data["choices"][0]["message"]["content"] or "").strip(),
        "model": data.get("model") or OPENROUTER_MODEL,
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

def norm_ext(ext: str) -> str:
    return ".jpg" if ext.lower() in {".jfif", ".jif"} else ext.lower()

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
    sb = require_supabase()
    out = []
    for f in files or []:
        if not f or not getattr(f, "filename", None): continue
        ext = os.path.splitext(f.filename.lower())[1]
        if ext not in ALLOWED_IMG_EXTS: raise HTTPException(400, f"Unsupported image type: {ext}")
        name = f"{uuid.uuid4().hex}{norm_ext(ext)}"
        path = f"{shop_id}/{name}"
        
        try:
            sb.storage.from_("product-images").upload(path, f.file.read(), {"content-type": f.content_type})
            out.append(sb.storage.from_("product-images").get_public_url(path))
        except Exception as e:
            print(f"[Supabase Storage Error] {e}")
            raise HTTPException(500, "Failed to upload image. Ensure the 'product-images' bucket is created and public in Supabase.")
    return out

def serialize_product(row: dict, user_id: str = None) -> Dict[str, Any]:
    shop_id = row.get("shop_id", "")
    prod_id = row.get("product_id", "")
    imgs = normalize_image_list(shop_id, row.get("images", []))
    
    try:
        rv = supabase.table("reviews").select("rating").eq("shop_id", shop_id).eq("product_id", prod_id).execute().data
    except:
        rv = []
        
    is_fav = False
    if user_id:
        try:
            favs = supabase.table("favourites").select("shop_id").eq("user_id", user_id).eq("shop_id", shop_id).eq("product_id", prod_id).execute().data
            is_fav = len(favs) > 0
        except: pass
        
    return {
        "product_id": prod_id, "shop_id": shop_id, "name": row.get("name", ""),
        "overview": row.get("overview", ""), "price": row.get("price", ""),
        "stock": row.get("stock", "in"), "variants": row.get("variants", ""),
        "images": imgs, "image_count": len(imgs),
        "avg_rating": round(sum(r["rating"] for r in rv)/len(rv) if rv else 0, 1),
        "review_count": len(rv), "is_favourite": is_fav,
    }

def serialize_products_bulk(rows: List[dict], user_id: str = None) -> List[Dict[str, Any]]:
    if not rows:
        return []

    pairs = {(str(r.get("shop_id", "")), str(r.get("product_id", ""))) for r in rows if r.get("shop_id") and r.get("product_id")}
    shop_ids = sorted({shop_id for shop_id, _ in pairs})
    product_ids = sorted({product_id for _, product_id in pairs})

    review_map: Dict[tuple, List[int]] = {}
    if shop_ids and product_ids and supabase is not None:
        try:
            review_rows = supabase.table("reviews").select("shop_id, product_id, rating").in_("shop_id", shop_ids).in_("product_id", product_ids).execute().data
            for review in review_rows:
                key = (str(review.get("shop_id", "")), str(review.get("product_id", "")))
                if key in pairs:
                    review_map.setdefault(key, []).append(int(review.get("rating", 0)))
        except Exception:
            review_map = {}

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
        out.append({
            "product_id": product_id,
            "shop_id": shop_id,
            "name": row.get("name", ""),
            "overview": row.get("overview", ""),
            "price": row.get("price", ""),
            "stock": row.get("stock", "in"),
            "variants": row.get("variants", ""),
            "images": imgs,
            "image_count": len(imgs),
            "avg_rating": round(sum(ratings) / len(ratings), 1) if ratings else 0,
            "review_count": len(ratings),
            "is_favourite": key in fav_set,
        })
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
        "image_count": imgs,
        "products_with_images": with_imgs,
        "chat_hits_30d": len(chats), 
        "shop_views_30d": len(shop_views),
        "product_views_30d": len(product_views), 
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
    
    prods = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute().data
    
    p_serialized = []
    for p in prods:
        imgs = p.get("images", [])
        if isinstance(imgs, str):
            try: imgs = json.loads(imgs)
            except: imgs = []
        p_serialized.append({
            "product_id": p["product_id"], "name": p["name"], "overview": p.get("overview", ""),
            "price": p.get("price", ""), "stock": p.get("stock", "in"), "images": imgs
        })
    
    obj = {"shop": {k: shop[0].get(k, "") for k in ("name","address","overview","phone","hours","category")},
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
    return any(t in qn for t in ["what do you sell","what products","what items","what do you have","show products","product list","catalog","menu","inventory","show all products","list products","all products"])

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
    return any(t in qn for t in ["in stock", "available", "availability", "what's in stock", "what is in stock", "instock"])

def is_cheapest_query(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in ["cheapest", "lowest price", "least expensive", "most affordable", "budget friendly"])

def is_recommendation_query(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in ["recommend", "suggest", "best", "popular", "top pick", "top picks", "good option", "good options"])

def is_price_lookup_query(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in ["how much", "price of", "price for", "cost of", "cost for"])

def answer_budget_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    limit = extract_budget_limit(q)
    if limit is None:
        return None

    matches = []
    for row in prod_rows:
        value = parse_price_value(row.get("price", ""))
        if value is None or value > limit:
            continue
        enriched = {
            "shop_id": row.get("shop_id", ""),
            "product_id": row.get("product_id", ""),
            "name": row.get("name", ""),
            "overview": row.get("overview", ""),
            "price": row.get("price", ""),
            "stock": row.get("stock", "in"),
            "images": normalize_image_list(row.get("shop_id", ""), row.get("images", [])),
            "_price_value": value,
        }
        matches.append(enriched)

    matches.sort(key=lambda item: (item["_price_value"], item["name"].lower()))
    suggestions = ["Show all products", "What's in stock?", "Do you have anything cheaper?"]

    if not matches:
        return {
            "answer": f"I couldn't find anything at {shop_label(shop)} priced below **{limit:g}**.",
            "products": [],
            "meta": {"llm_used": False, "reason": "budget_filter", "suggestions": suggestions},
        }

    top = matches[:6]
    lines = [f"Here {'is' if len(top)==1 else 'are'} what I found at {shop_label(shop)} under **{limit:g}**:"]
    for item in top:
        lines.append(f"• **{item['name']}** — {item.get('price','Price not listed')} *({item.get('stock','in')})*")
    if len(matches) > len(top):
        lines.append(f"\nThere are **{len(matches)}** matching products in total.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top),
        "meta": {"llm_used": False, "reason": "budget_filter", "suggestions": suggestions},
    }

def answer_shop_info_query(shop: dict, q: str) -> Optional[Dict[str, Any]]:
    suggestions = ["Show all products", "What's in stock?", "What are your prices?"]
    if is_location_query(q):
        address = (shop.get("address") or "").strip()
        if address:
            return {
                "answer": f"You can find {shop_label(shop)} at **{address}**.",
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

    matches = []
    for row in prod_rows:
        stock = str(row.get("stock", "in") or "in").strip().lower()
        if stock == "out":
            continue
        matches.append({
            "shop_id": row.get("shop_id", ""),
            "product_id": row.get("product_id", ""),
            "name": row.get("name", ""),
            "overview": row.get("overview", ""),
            "price": row.get("price", ""),
            "stock": row.get("stock", "in"),
            "images": normalize_image_list(row.get("shop_id", ""), row.get("images", [])),
        })

    if not matches:
        return {
            "answer": f"I don't see any in-stock products at {shop_label(shop)} right now.",
            "products": [],
            "meta": {"llm_used": False, "reason": "stock_filter", "suggestions": ["Show all products", "Do you restock often?"]},
        }

    top = matches[:6]
    lines = [f"Here {'is' if len(top)==1 else 'are'} what {shop_label(shop)} currently has in stock:"]
    for item in top:
        price = item.get("price") or "Price not listed"
        lines.append(f"- **{item['name']}** - {price} *({item.get('stock', 'in')})*")
    if len(matches) > len(top):
        lines.append(f"\nThere are **{len(matches)}** in-stock products in total.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top),
        "meta": {"llm_used": False, "reason": "stock_filter", "suggestions": ["Show all products", "What are your prices?", "Do you have anything cheaper?"]},
    }

def answer_cheapest_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    if not is_cheapest_query(q):
        return None

    ranked = []
    for row in prod_rows:
        value = parse_price_value(row.get("price", ""))
        if value is None:
            continue
        ranked.append({
            "shop_id": row.get("shop_id", ""),
            "product_id": row.get("product_id", ""),
            "name": row.get("name", ""),
            "overview": row.get("overview", ""),
            "price": row.get("price", ""),
            "stock": row.get("stock", "in"),
            "images": normalize_image_list(row.get("shop_id", ""), row.get("images", [])),
            "_price_value": value,
        })

    if not ranked:
        return None

    ranked.sort(key=lambda item: (item["_price_value"], item["name"].lower()))
    top = ranked[:6]
    lines = [f"These are the lowest-priced items I found at {shop_label(shop)}:"]
    for item in top:
        lines.append(f"- **{item['name']}** - {item.get('price', 'Price not listed')} *({item.get('stock', 'in')})*")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top),
        "meta": {"llm_used": False, "reason": "cheapest_filter", "suggestions": ["Do you have anything below 5 dollars?", "What's in stock?", "Show all products"]},
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

def choose_chat_products(prod_rows: List[Dict], q: str, answer_text: str = "", prefer_images: bool = False, limit: int = 4) -> List[Dict]:
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
        row_tokens = set(re.findall(r"[a-z0-9]+", norm_text(f"{name} {row.get('overview','')} {row.get('product_id','')}")))
        score += len(q_tokens & row_tokens) * 0.9
        if imgs:
            score += 0.5
        if score > 0:
            enriched = {
                "shop_id": row.get("shop_id", ""),
                "product_id": row.get("product_id", ""),
                "name": name,
                "overview": row.get("overview", ""),
                "price": row.get("price", ""),
                "stock": row.get("stock", "in"),
                "images": imgs,
                "_score": score,
            }
            ranked.append(enriched)
    ranked.sort(key=lambda item: (item["_score"], len(item.get("images", []))), reverse=True)
    return ranked[:limit]

def answer_product_image_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    if not wants_product_image(q):
        return None
    matches = choose_chat_products(prod_rows, q, prefer_images=True, limit=4)
    if not matches:
        return {
            "answer": f"I couldn't find a matching product photo at {shop_label(shop)} yet. Try asking with the product name or say **show all products**.",
            "products": [],
            "meta": {"llm_used": False, "reason": "product_image_missing", "suggestions": ["Show all products", "Show all images", "What's in stock?"]},
        }
    top = matches[0]
    lines = [f"Here {'is' if len(matches)==1 else 'are'} the photo{'s' if len(matches)>1 else ''} I found from {shop_label(shop)}:"]
    lines.append(f"- **{top['name']}** - {top.get('price','Price not listed')} *({top.get('stock','in')})*")
    if len(matches) > 1:
        lines.append(f"\nI also found **{len(matches)-1}** more matching product card{'s' if len(matches)-1 != 1 else ''} below.")
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(matches),
        "meta": {"llm_used": False, "reason": "product_image", "suggestions": [f"What is the price of {top['name']}?", "Show all images", "Do you have more like this?"]},
    }

def answer_price_lookup_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    if not is_price_lookup_query(q):
        return None
    picked = rank_products(prod_rows, q)
    if not picked:
        return None
    top = picked[0]
    price = top.get("price") or "Price not listed"
    stock = top.get("stock") or "in"
    answer = f"{shop_label(shop)} has **{top['name']}** listed at **{price}**"
    if stock:
        answer += f" and it is currently **{stock}**."
    else:
        answer += "."
    return {
        "answer": answer,
        "products": serialize_products_bulk([top]),
        "meta": {"llm_used": False, "reason": "price_lookup", "suggestions": [f"Show me photos of {top['name']}", f"Do you have more like {top['name']}?", "What's in stock?"]},
    }

def answer_recommendation_query(shop: dict, prod_rows: List[Dict], q: str) -> Optional[Dict[str, Any]]:
    if not is_recommendation_query(q):
        return None

    budget = extract_budget_limit(q)
    candidates = []
    for row in prod_rows:
        stock = str(row.get("stock", "in") or "in").strip().lower()
        if stock == "out":
            continue
        price_value = parse_price_value(row.get("price", ""))
        if budget is not None and price_value is not None and price_value > budget:
            continue
        candidates.append(row)

    if not candidates:
        return {
            "answer": f"I could not find a good in-stock recommendation at {shop_label(shop)} for that request.",
            "products": [],
            "meta": {"llm_used": False, "reason": "recommendation_empty", "suggestions": ["Show all products", "What's in stock?", "Do you have anything cheaper?"]},
        }

    ranked = rank_products(candidates, q)
    if not ranked:
        ranked = rank_products(candidates, shop.get("category", "")) or [
            {
                "shop_id": row.get("shop_id", ""),
                "product_id": row.get("product_id", ""),
                "name": row.get("name", ""),
                "overview": row.get("overview", ""),
                "price": row.get("price", ""),
                "stock": row.get("stock", "in"),
                "images": normalize_image_list(row.get("shop_id", ""), row.get("images", [])),
            }
            for row in candidates
        ]

    top = ranked[:4]
    opener = f"Here are a few good options from {shop_label(shop)}:"
    if budget is not None:
        opener = f"Here are a few good options from {shop_label(shop)} under **{budget:g}**:"
    lines = [opener]
    for item in top:
        line = f"- **{item['name']}** - {item.get('price', 'Price not listed')} *({item.get('stock', 'in')})*"
        overview = (item.get("overview") or "").strip()
        if overview:
            line += f"\n  {overview[:120]}"
        lines.append(line)
    return {
        "answer": "\n".join(lines),
        "products": serialize_products_bulk(top),
        "meta": {"llm_used": False, "reason": "recommendation", "suggestions": build_chat_suggestions(q, shop, top)},
    }

def rank_products(products: List[Dict], q: str) -> List[Dict]:
    qn = norm_text(q); qt = set(re.findall(r"[a-z0-9]+", qn))
    scored = []
    for r in products:
        name = (r.get("name") or "").strip(); ov = (r.get("overview") or "").strip()
        hay = norm_text(f"{name} {ov}"); ht = set(re.findall(r"[a-z0-9]+", hay))
        s = 0.0
        if norm_text(name) and norm_text(name) in qn: s += 8
        if qt & ht: s += len(qt & ht) * 1.4
        if qn and qn in hay: s += 5
        if s > 0: 
            imgs = normalize_image_list(r.get("shop_id", ""), r.get("images", []))
            scored.append((s, {
                "shop_id": r.get("shop_id", ""),
                "product_id": r["product_id"], 
                "name": r["name"], 
                "overview": r.get("overview",""), 
                "price": r.get("price",""), 
                "stock": r.get("stock","in"), 
                "images": imgs
            }))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored]

def build_context(shop: dict, picked: List[Dict], all_rows: List, rag_chunks: Optional[List[str]] = None) -> str:
    lines = [f"Shop: {shop['name']}", f"Address: {shop.get('address','')}"]
    if shop.get("phone"): lines.append(f"Phone: {shop['phone']}")
    if shop.get("hours"): lines.append(f"Hours: {shop['hours']}")
    if shop.get("category"): lines.append(f"Category: {shop['category']}")
    if shop.get("whatsapp"): lines.append(f"WhatsApp: {shop['whatsapp']}")
    if shop.get("overview"): lines.append(f"About: {shop['overview']}")
    lines.append(f"Total products: {len(all_rows)}")
    if picked:
        lines.append("\nRelevant products:")
        for p in picked[:6]:
            img_part = f' | Photo: ![{p["name"]}]({p["images"][0]})' if p.get("images") else ""
            lines.append(f'- {p["name"]} | Price: {p.get("price","N/A")} | Stock: {p.get("stock","in")}{img_part}')
    elif all_rows:
        lines.append("\nSample products from this shop:")
        for row in all_rows[:8]:
            imgs = normalize_image_list(row.get("shop_id", ""), row.get("images", []))
            img_part = f' | Photo: ![{row.get("name","Product")}]({imgs[0]})' if imgs else ""
            lines.append(f'- {row.get("name","Product")} | Price: {row.get("price","N/A")} | Stock: {row.get("stock","in")}{img_part}')
    if rag_chunks:
        lines.append("\nKnowledge base notes:")
        for chunk in rag_chunks[:4]:
            lines.append(chunk.strip())
    return "\n".join(lines)

SHOP_ASSISTANT_SYSTEM = """You are the live shopping assistant for a local marketplace shop.
Answer using ONLY the supplied shop context.
Be concise, warm, and professional.
Mention price and stock when available.
If the user asks what the shop sells, summarize first and then list examples.
If information is missing, say so clearly and suggest a useful follow-up question.
Use simple markdown.
Prefer short sections or bullets over long paragraphs.
If you mention products, format them as clean bullets with name, price, and stock when available.
Sound natural and conversational, not robotic or overly salesy.
Do not say things like "based on the provided context" or "I found a likely match" unless necessary.
When answering simple customer questions, speak like a real shop assistant would.
Do not mention being an AI, model, chatbot, or system unless the user directly asks.
Prefer clear direct answers over meta explanations.
If the customer asks for a recommendation, suggest a few suitable products from the shop and briefly say why.
If the customer asks a broad question, answer naturally as a helpful shopkeeper would, but stay grounded in the shop context.
If the customer asks something unrelated to the shop, reply briefly and politely in a human tone without pretending the shop has facts you do not have.
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
            "Response format: start with one short natural sentence, then list up to 4 product bullets. "
            "Each bullet should include name, price, stock, and a short reason it fits."
        )
    if is_list_intent(qn):
        return (
            "Response format: start with one short summary sentence, then list products as bullets with name, "
            "price, and stock. Keep it tidy and easy to scan."
        )
    if is_price_lookup_query(qn) or is_budget_query(qn) or is_cheapest_query(qn):
        return "Response format: answer directly in 1 to 4 lines. Put the exact price or budget result first."
    if is_location_query(qn) or is_hours_query(qn) or is_contact_query(qn):
        return "Response format: answer directly and briefly in 1 to 3 lines."
    return "Response format: give a short direct answer first. Use bullets only if they clearly help."

def build_chat_suggestions(q: str, shop: dict, picked: List[Dict]) -> List[str]:
    suggestions: List[str] = []
    qn = norm_text(q)
    category = (shop.get("category") or "").strip()
    if picked:
        top = picked[0]
        suggestions.extend([
            f"What is the price of {top['name']}?",
            f"Show me photos of {top['name']}",
            f"Do you have more like {top['name']}?",
        ])
    else:
        suggestions.extend(["Show all products", "What's in stock?", "What are your prices?"])
    if category:
        suggestions.append(f"What are your best {category.lower()} products?")
        suggestions.append(f"Show me your most popular {category.lower()} items")
    if "hour" not in qn and shop.get("hours"):
        suggestions.append("What are your opening hours?")
    if "address" not in qn and shop.get("address"):
        suggestions.append("Where is this shop located?")
    return dedup(suggestions)[:4]

def shop_label(shop: dict) -> str:
    return f"**{shop['name']}**"

def fallback_answer_v2(shop: dict, picked: List[Dict], q: str) -> str:
    if is_greeting(q):
        category = (shop.get("category") or "").strip().lower()
        opener = "Hi"
        if "food" in category or "bakery" in category or "cafe" in category:
            opener = "Hi there"
        elif "electronics" in category:
            opener = "Hello"
        elif "clothing" in category or "fashion" in category:
            opener = "Hey"
        return f"{opener}! Welcome to {shop_label(shop)}. Ask me about products, prices, stock, hours, or just say **show all products**."
    if picked:
        top = picked[0]
        lines = [
            f"This looks like a good match from {shop_label(shop)}:",
            "",
            f"**{top['name']}** - {top.get('price','Price not listed')} *({top.get('stock','in')})*",
        ]
        if top.get("overview"):
            lines.append(str(top["overview"])[:160])
        if top.get("images"):
            lines.append(f"![{top['name']}]({top['images'][0]})")
        return "\n".join(lines)
    return f"I couldn't find a clear match at {shop_label(shop)}. Try **show all products**, **what's in stock**, or **do you have anything below 10 dollars**."

def fallback_answer(shop: dict, picked: List[Dict], q: str) -> str:
    if is_greeting(q): return f"Hi! Welcome to **{shop['name']}**! Ask me about our products, or say 'show all products'."
    if picked:
        top = picked[0]
        s = f"**{top['name']}** — {top.get('price','')} *({top.get('stock','in')})*"
        if top.get("images"): s += f"\n![{top['name']}]({top['images'][0]})"
        return s
    return "I couldn't find a direct match. Try asking 'show all products' to see everything we have."

def answer_catalog_query(shop: dict, prod_rows: List[Dict]) -> Dict[str, Any]:
    suggestions = build_chat_suggestions("show all products", shop, prod_rows)
    if not prod_rows:
        return {
            "answer": f"{shop_label(shop)} has not added any products yet.",
            "products": [],
            "meta": {"llm_used": False, "reason": "catalog_empty", "suggestions": suggestions},
        }

    top = prod_rows[:10]
    category = (shop.get("category") or "").strip()
    intro = f"Here is what you can browse at {shop_label(shop)}:"
    if category:
        intro = f"Here is a quick look at the {category.lower()} items available at {shop_label(shop)}:"
    lines = [intro]
    for item in top:
        price = item.get("price") or "Price not listed"
        stock = item.get("stock") or "in"
        overview = (item.get("overview") or "").strip()
        detail = f"- **{item.get('name','Product')}** - {price} *({stock})*"
        if overview:
            detail += f"\n  {overview[:120]}"
        lines.append(detail)
    if len(prod_rows) > len(top):
        lines.append(f"\nThere are **{len(prod_rows)}** products in total. Ask if you want something specific.")
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

@app.get("/favicon.ico")
def favicon():
    raise HTTPException(204)

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
def register(body: RegisterReq):
    try:
        require_supabase()
        supabase.auth.sign_up({
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
def login(body: LoginReq):
    try:
        res = supabase.auth.sign_in_with_password({"email": body.email, "password": body.password})
        user = res.user
        prof_res = supabase.table("profiles").select("*").eq("id", user.id).execute()
        prof = prof_res.data[0] if prof_res.data else {}
        return {"ok": True, "token": res.session.access_token, "email": user.email, "display_name": prof.get("display_name", ""), "avatar_url": prof.get("avatar_url", ""), "email_verified": user.email_confirmed_at is not None, "role": prof.get("role", "customer")}
    except Exception:
        raise HTTPException(401, "Invalid email or password")

@app.get("/auth/me")
def auth_me(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    shops = supabase.table("shops").select("shop_id, name").eq("owner_user_id", user.id).execute().data
    favs = supabase.table("favourites").select("shop_id").eq("user_id", user.id).execute().data
    revs = supabase.table("reviews").select("id").eq("user_id", user.id).execute().data
    return {
        "ok": True, "user_id": user.id, "email": user.email,
        "display_name": prof.get("display_name", ""),
        "avatar_url": prof.get("avatar_url", ""),
        "email_verified": user.email_confirmed_at is not None,
        "role": prof.get("role", "customer"),
        "my_shops": shops, "fav_count": len(favs), "review_count": len(revs),
    }

@app.post("/auth/logout")
def logout(authorization: Optional[str] = Header(None)): return {"ok": True}

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
def forgot_password(body: ForgotPasswordReq):
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
        ext = norm_ext(os.path.splitext(avatar.filename.lower())[1])
        filename = f"user_{user.id}_{uuid.uuid4().hex[:8]}{ext}"
        path = f"avatars/{filename}"
        sb.storage.from_("product-images").upload(path, avatar.file.read(), {"content-type": avatar.content_type})
        url = sb.storage.from_("product-images").get_public_url(path)
        sb.table("profiles").update({"avatar_url": url}).eq("id", user.id).execute()
        return {"ok": True, "avatar_url": url}
    except Exception as e:
        raise HTTPException(500, f"Avatar upload failed: {str(e)}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Customer Interactions
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.post("/customer/favourite/{shop_id}/{product_id}")
def toggle_favourite(shop_id: str, product_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    favs = supabase.table("favourites").select("shop_id").eq("user_id", user.id).eq("shop_id", shop_id).eq("product_id", product_id).execute().data
    exists = len(favs) > 0
    if exists: supabase.table("favourites").delete().eq("user_id", user.id).eq("shop_id", shop_id).eq("product_id", product_id).execute()
    else: supabase.table("favourites").insert({"user_id": user.id, "shop_id": shop_id, "product_id": product_id}).execute()
    return {"ok": True, "saved": not exists}

@app.get("/customer/favourites")
def get_favourites(request: Request, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    favs = supabase.table("favourites").select("shop_id, product_id").eq("user_id", user.id).order("created_at", desc=True).execute().data
    product_rows = []
    for f in favs:
        p = supabase.table("products").select("*").eq("shop_id", f["shop_id"]).eq("product_id", f["product_id"]).execute().data
        if p: product_rows.append(p[0])
    out = serialize_products_bulk(product_rows, user.id)
    return {"ok": True, "favourites": out}

@app.get("/public/reviews/{shop_id}/{product_id}")
def get_reviews(shop_id: str, product_id: str):
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
        return {"ok": True, "reviews": out}
    except Exception as e:
        print(f"Reviews Error: {e}")
        return {"ok": True, "reviews": []}

@app.post("/public/track-view/{shop_id}/{product_id}")
def track_product_view(shop_id: str, product_id: str):
    track(shop_id, "view", product_id)
    return {"ok": True}

@app.post("/customer/review/{shop_id}/{product_id}")
def post_review(shop_id: str, product_id: str, body: ReviewReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    if not body.body.strip(): raise HTTPException(400, "Review cannot be empty")
    
    prod_check = supabase.table("products").select("product_id").eq("shop_id", shop_id).eq("product_id", product_id).execute().data
    if not prod_check: raise HTTPException(404, "Product not found")
    
    existing = supabase.table("reviews").select("id").eq("shop_id", shop_id).eq("product_id", product_id).eq("user_id", user.id).execute().data
    if existing:
        supabase.table("reviews").update({"rating": body.rating, "body": body.body.strip(), "created_at": "now()"}).eq("id", existing[0]["id"]).execute()
    else:
        supabase.table("reviews").insert({"shop_id": shop_id, "product_id": product_id, "user_id": user.id, "rating": body.rating, "body": body.body.strip()}).execute()
    return {"ok": True, "message": "Review saved"}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Public Browsing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/public/shops")
def public_shops(category: Optional[str] = None):
    q = supabase.table("shops").select("*").order("created_at", desc=True)
    if category: q = q.ilike("category", f"%{category}%")
    rows = q.execute().data
    for r in rows:
        try:
            r["stats"] = shop_stats(r["shop_id"])
        except Exception as e:
            print(f"[Shop Stats Warning] {r.get('shop_id')}: {e}")
            r["stats"] = {"product_count": 0, "image_count": 0, "products_with_images": 0, "chat_hits_30d": 0, "shop_views_30d": 0, "product_views_30d": 0, "avg_rating": 0}
    return {"ok": True, "shops": rows}

@app.get("/public/shop/{shop_id}")
def public_shop(shop_id: str, request: Request, sort: str = Query("default"), stock: str = Query(""), page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=100), authorization: Optional[str] = Header(None)):
    shop_res = supabase.table("shops").select("*").eq("shop_id", shop_id).execute()
    if not shop_res.data: raise HTTPException(404, "Shop not found")
    
    user_id = None
    if authorization:
        try: user_id = get_user(authorization)[0].id
        except: pass
        
    q = supabase.table("products").select("*").eq("shop_id", shop_id)
    if stock in ("in", "low", "out"): q = q.eq("stock", stock)
    all_prods = q.execute().data
    
    if sort == "price-asc": all_prods.sort(key=lambda x: float(re.sub(r'[^\d.]', '', x.get("price") or "0") or 0))
    elif sort == "price-desc": all_prods.sort(key=lambda x: float(re.sub(r'[^\d.]', '', x.get("price") or "0") or 0), reverse=True)
    else: all_prods.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    
    track(shop_id, "shop_view")
    
    paged = paginate_list(all_prods, page, limit)
    ser_prods = serialize_products_bulk(paged["items"], user_id)
    
    return {
        "ok": True, "shop_id": shop_id, "shop": shop_res.data[0],
        "products": ser_prods,
        "pagination": paged["pagination"],
        "stats": shop_stats(shop_id),
        "suggested_questions": ["What products do you have?", "Show all products", "What's in stock?", "Show me your best product", "Show all images"]
    }

@app.get("/public/search")
def search_shop(request: Request, shop_id: str = Query(...), q: str = Query(...), page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=100)):
    qn = (q or "").strip()
    if not qn: return {"ok": True, "shop_id": shop_id, "q": q, "results": [], "total": 0, "pagination": paginate_list([], 1, limit)["pagination"]}
    rows = supabase.table("products").select("*").eq("shop_id", shop_id).or_(f"name.ilike.%{qn}%,overview.ilike.%{qn}%").order("updated_at", desc=True).execute().data
    paged = paginate_list(rows, page, limit)
    return {"ok": True, "shop_id": shop_id, "q": q, "results": serialize_products_bulk(paged["items"]), "total": len(rows), "pagination": paged["pagination"]}

@app.get("/public/search/global")
def search_global(request: Request, q: str = Query(...), page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=60)):
    qn = (q or "").strip()
    if not qn: return {"ok": True, "q": q, "results": [], "total": 0, "pagination": paginate_list([], 1, limit)["pagination"]}
    rows = supabase.table("products").select("*, shops(name, address, whatsapp)").or_(f"name.ilike.%{qn}%,overview.ilike.%{qn}%").order("updated_at", desc=True).execute().data
    
    paged = paginate_list(rows, page, limit)
    results = serialize_products_bulk(paged["items"])
    for r, prod in zip(paged["items"], results):
        prod["shop_name"] = r.get("shops", {}).get("name", "")
        prod["shop_address"] = r.get("shops", {}).get("address", "")
    return {"ok": True, "q": q, "results": results, "total": len(rows), "pagination": paged["pagination"]}

@app.get("/public/top-products")
def top_products(request: Request, page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=60), category: str = Query("")):
    q = supabase.table("products").select("*, shops!inner(name, category)").neq("stock", "out")
    if category: q = q.ilike("shops.category", f"%{category}%")
    rows = q.execute().data
    
    paged = paginate_list(rows, page, limit)
    results = serialize_products_bulk(paged["items"])
    for r, prod in zip(paged["items"], results):
        prod["shop_name"] = r.get("shops", {}).get("name", "")
    return {"ok": True, "products": results, "pagination": paged["pagination"]}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Chat 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/chat")
def chat_endpoint(request: Request, shop_id: str = Query(...), q: str = Query(...)):
    q = (q or "").strip()
    if not q: raise HTTPException(400, "Missing q")
    track(shop_id, "chat")

    shop_res = supabase.table("shops").select("*").eq("shop_id", shop_id).execute()
    if not shop_res.data: return {"answer": "Shop not found.", "products": [], "meta": {"llm_used": False}}
    shop = shop_res.data[0]
    
    prod_rows = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute().data

    info_answer = answer_shop_info_query(shop, q)
    if info_answer is not None:
        return info_answer

    budget_answer = answer_budget_query(shop, prod_rows, q)
    if budget_answer is not None:
        return budget_answer

    stock_answer = answer_stock_query(shop, prod_rows, q)
    if stock_answer is not None:
        return stock_answer

    cheapest_answer = answer_cheapest_query(shop, prod_rows, q)
    if cheapest_answer is not None:
        return cheapest_answer

    product_image_answer = answer_product_image_query(shop, prod_rows, q)
    if product_image_answer is not None:
        return product_image_answer

    picked = rank_products(prod_rows, q)
    abs_picked = serialize_products_bulk(picked[:4])
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
            ans = "This shop hasn't uploaded any product photos yet."
        return {"answer": ans, "products": abs_picked, "meta": {"llm_used": False, "suggestions": suggestions}}

    # 2. Shortcut: Greeting
    if is_greeting(q):
        return {"answer": f"Hi! Welcome to **{shop['name']}**! Ask me about products, prices, stock, opening hours, or say 'show all products'.", "products": abs_picked, "meta": {"llm_used": False, "suggestions": suggestions}}

    # 3. Handle Full Catalog Requests safely
    if is_list_intent(q):
        return answer_catalog_query(shop, prod_rows)

    try:
        system_prompt = SHOP_ASSISTANT_SYSTEM + "\n" + shop_voice_instructions(shop) + "\n" + shop_persona_instructions(shop) + "\n" + response_style_instructions(q)
        llm_res = llm_chat(system_prompt, f"CONTEXT:\n{build_context(shop, picked, prod_rows, rag.get('chunks', []))}\n\nCUSTOMER: {q}")
        attached = choose_chat_products(prod_rows, q, llm_res["content"], prefer_images=wants_product_image(q), limit=4)
        if attached:
            abs_picked = serialize_products_bulk(attached)
        elif wants_product_image(q):
            with_images = [p for p in picked if p.get("images")]
            if with_images:
                abs_picked = serialize_products_bulk(with_images[:4])
        return {"answer": llm_res["content"], "products": abs_picked, "meta": {"llm_used": True, "model": llm_res.get("model") or OPENROUTER_MODEL, "suggestions": suggestions, "rag_matches": len(rag.get('matches', []))}}
    except Exception as e:
        err_msg = str(e)
        if hasattr(e, "response") and getattr(e, "response") is not None:
            err_msg += f" | {e.response.text}"
            
        print(f"[Chat LLM Exception] Fallback triggered: {err_msg}")
        
        ans = fallback_answer_v2(shop, picked, q)

        return {
            "answer": ans,
            "products": abs_picked,
            "meta": {"llm_used": False, "reason": "fallback_after_llm_error", "suggestions": suggestions, "rag_matches": len(rag.get('matches', []))}
        }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Admin 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_shop_owner(user_id: str, shop_id: str):
    res = supabase.table("shops").select("shop_id").eq("shop_id", shop_id).eq("owner_user_id", user_id).execute()
    if not res.data: raise HTTPException(404, "Not found or not yours")

@app.post("/admin/create-shop")
def create_shop(body: CreateShopReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    shop = body.shop
    if not shop.name.strip() or not shop.address.strip(): raise HTTPException(400, "Shop name and address required")
    sid = slug(body.shop_id) if body.shop_id else gen_shop_id(shop.name)
    
    exists_res = supabase.table("shops").select("shop_id").eq("shop_id", sid).execute()
    if exists_res.data: raise HTTPException(409, "shop_id already exists")
    
    supabase.table("shops").insert({"shop_id": sid, "owner_user_id": user.id, "name": shop.name.strip(), "address": shop.address.strip(), "overview": shop.overview, "phone": shop.phone, "hours": shop.hours, "category": shop.category, "whatsapp": shop.whatsapp}).execute()
    supabase.table("profiles").update({"role": "shopkeeper"}).eq("id", user.id).execute()
    
    rebuild_kb(sid)
    return {"ok": True, "shop_id": sid}

@app.get("/admin/my-shops")
def my_shops(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    shops_res = supabase.table("shops").select("*").eq("owner_user_id", user.id).order("created_at", desc=True).execute()
    rows = shops_res.data
    
    for r in rows: 
        r["stats"] = shop_stats(r["shop_id"])
    return {"ok": True, "shops": rows}

@app.get("/admin/shop/{shop_id}")
def admin_get_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    
    shop = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data[0]
    prods = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute().data
    stats = shop_stats(shop_id)
    
    ser_prods = serialize_products_bulk(prods)
    return {"ok": True, "shop_id": shop_id, "data": {"shop": shop, "products": ser_prods, "stats": stats}}

@app.put("/admin/shop/{shop_id}")
def admin_update_shop(shop_id: str, body: ShopInfo, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    supabase.table("shops").update({"name": body.name.strip(), "address": body.address.strip(), "overview": body.overview, "phone": body.phone, "hours": body.hours, "category": body.category, "whatsapp": body.whatsapp}).eq("shop_id", shop_id).execute()
    rebuild_kb(shop_id)
    return {"ok": True}

@app.delete("/admin/shop/{shop_id}")
def admin_delete_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    supabase.table("shops").delete().eq("shop_id", shop_id).execute()
    shutil.rmtree(os.path.join(SHOPS_DIR, shop_id), ignore_errors=True)
    return {"ok": True}

@app.post("/admin/shop/{shop_id}/product")
def admin_upsert_product(shop_id: str, product: Product, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    pid = slug(product.product_id, 60)
    if not product.name.strip(): raise HTTPException(400, "Product name required")
    
    existing = supabase.table("products").select("images").eq("shop_id", shop_id).eq("product_id", pid).execute().data
    imgs = dedup(product.images or [])
    if existing and not imgs: imgs = existing[0].get("images", [])
    
    data = {"name": product.name.strip(), "overview": product.overview, "price": product.price, "stock": product.stock, "variants": product.variants, "images": imgs, "updated_at": "now()"}
    if existing: supabase.table("products").update(data).eq("shop_id", shop_id).eq("product_id", pid).execute()
    else: supabase.table("products").insert({**data, "shop_id": shop_id, "product_id": pid}).execute()
    
    rebuild_kb(shop_id)
    return {"ok": True, "product_id": pid}

@app.delete("/admin/shop/{shop_id}/product/{product_id}")
def admin_delete_product(shop_id: str, product_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    supabase.table("products").delete().eq("shop_id", shop_id).eq("product_id", product_id).execute()
    rebuild_kb(shop_id)
    return {"ok": True}

@app.post("/admin/shop/{shop_id}/product-with-images")
def admin_product_with_images(
    shop_id: str, authorization: Optional[str] = Header(None),
    product_id: str = Form(...), name: str = Form(...), overview: str = Form(""), price: str = Form(""),
    stock: str = Form("in"), variants: str = Form(""), images: List[UploadFile] = File(default=[])
):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    pid = slug(product_id, 60)
    if not name.strip(): raise HTTPException(400, "Name required")
    
    new_urls = save_images(shop_id, images)
    
    existing = supabase.table("products").select("images").eq("shop_id", shop_id).eq("product_id", pid).execute().data
    
    current_imgs = []
    if existing:
        current_imgs = existing[0].get("images", [])
        if isinstance(current_imgs, str):
            try: current_imgs = json.loads(current_imgs)
            except: current_imgs = []
            
    merged = dedup(current_imgs + new_urls)
    
    data = {"name": name.strip(), "overview": overview, "price": price, "stock": stock, "variants": variants, "images": merged, "updated_at": "now()"}
    if existing: supabase.table("products").update(data).eq("shop_id", shop_id).eq("product_id", pid).execute()
    else: supabase.table("products").insert({**data, "shop_id": shop_id, "product_id": pid}).execute()
    
    rebuild_kb(shop_id)
    return {"ok": True, "product_id": pid, "images": merged}

@app.delete("/admin/shop/{shop_id}/product/{product_id}/image")
def admin_delete_image(shop_id: str, product_id: str, image_path: str = Query(...), authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    
    row = supabase.table("products").select("images").eq("shop_id", shop_id).eq("product_id", product_id).execute().data
    if not row: raise HTTPException(404, "Product not found")
    
    img_name = os.path.basename(image_path.rstrip("/"))
    try: supabase.storage.from_("product-images").remove([f"{shop_id}/{img_name}"])
    except Exception: pass
    
    current_imgs = row[0].get("images", [])
    if isinstance(current_imgs, str):
        try: current_imgs = json.loads(current_imgs)
        except: current_imgs = []
        
    new_imgs = [u for u in current_imgs if os.path.basename(u) != img_name]
    supabase.table("products").update({"images": new_imgs, "updated_at": "now()"}).eq("shop_id", shop_id).eq("product_id", product_id).execute()
    rebuild_kb(shop_id)
    return {"ok": True, "remaining": len(new_imgs)}

@app.post("/admin/shop/{shop_id}/rebuild-kb")
def admin_rebuild_kb(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    rebuild_kb(shop_id)
    return {"ok": True}

@app.get("/admin/shop/{shop_id}/analytics")
def admin_analytics(shop_id: str, days: int = 30, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    
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

    return {"ok": True, "totals": totals, "top_products": top, "daily_chats": daily, "recent_events": recent_events}
