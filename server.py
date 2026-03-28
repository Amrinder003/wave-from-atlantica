"""
Wave API  v4.0 — Supabase Edition (Stable & Fixed Chat)
"""

from fastapi import FastAPI, Query, Header, HTTPException, UploadFile, File, Request, Form
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import os, re, json, time, uuid, shutil
from datetime import datetime, timezone
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
OPENROUTER_MODEL  = os.environ.get("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free")
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LLM 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def llm_chat(system: str, user: str, max_tokens: int = 400) -> str:
    if not OPENROUTER_KEY:
        raise ValueError("Missing OPENROUTER_API_KEY in environment variables.")
        
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    r = requests.post(
        OPENROUTER_URL,
        headers={
            "Authorization": f"Bearer {OPENROUTER_KEY}", 
            "Content-Type": "application/json",
            "HTTP-Referer": APP_BASE_URL,
            "X-Title": "Wave from Atlantica"
        },
        json={"model": OPENROUTER_MODEL, "messages": messages, "max_tokens": max_tokens, "temperature": 0.2},
        timeout=60,
    )
    r.raise_for_status()
    return (r.json()["choices"][0]["message"]["content"] or "").strip()

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
    imgs = row.get("images", [])
    if isinstance(imgs, str):
        try: imgs = json.loads(imgs)
        except: imgs = []
    if not isinstance(imgs, list): imgs = []
    
    shop_id = row.get("shop_id", "")
    prod_id = row.get("product_id", "")
    
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

def shop_stats(shop_id: str) -> Dict:
    sb = require_supabase()
    prods = sb.table("products").select("images").eq("shop_id", shop_id).execute().data
    with_imgs = sum(1 for p in prods if p.get("images"))
    imgs = sum(len(p.get("images", [])) for p in prods)
    
    since = int(time.time()) - 86400 * 30
    since_iso = datetime.fromtimestamp(since, tz=timezone.utc).isoformat()
    
    chats = sb.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "chat").gte("created_at", since_iso).execute().data
    shop_views = sb.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "shop_view").gte("created_at", since_iso).execute().data
    product_views = sb.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "view").gte("created_at", since_iso).execute().data
    rvs = sb.table("reviews").select("rating").eq("shop_id", shop_id).execute().data
    
    avg_r = sum(r["rating"] for r in rvs)/len(rvs) if rvs else 0
    
    return {
        "product_count": len(prods), 
        "chat_hits_30d": len(chats), 
        "shop_views_30d": len(shop_views),
        "product_views_30d": len(product_views), 
        "avg_rating": round(avg_r, 1)
    }

def track(shop_id: str, event: str, product_id: Optional[str] = None):
    if supabase is None:
        return
    try: supabase.table("analytics").insert({"shop_id": shop_id, "product_id": product_id, "event": event}).execute()
    except Exception: pass

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
            imgs = r.get("images", [])
            if isinstance(imgs, str):
                try: imgs = json.loads(imgs)
                except: imgs = []
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

def build_context(shop: dict, picked: List[Dict], all_rows: List) -> str:
    lines = [f"Shop: {shop['name']}", f"Address: {shop.get('address','')}"]
    if shop.get("phone"): lines.append(f"Phone: {shop['phone']}")
    if shop.get("hours"): lines.append(f"Hours: {shop['hours']}")
    if shop.get("overview"): lines.append(f"About: {shop['overview']}")
    lines.append(f"Total products: {len(all_rows)}")
    if picked:
        lines.append("\nRelevant products:")
        for p in picked[:6]:
            img_part = f' | Photo: ![{p["name"]}]({p["images"][0]})' if p.get("images") else ""
            lines.append(f'• {p["name"]} | Price: {p.get("price","N/A")} | Stock: {p.get("stock","in")}{img_part}')
    return "\n".join(lines)

SHOP_ASSISTANT_SYSTEM = """You are a friendly, knowledgeable sales assistant. Answer based ONLY on the shop context provided. Be warm and concise. Use markdown. Mention price and stock status when relevant."""

def fallback_answer(shop: dict, picked: List[Dict], q: str) -> str:
    if is_greeting(q): return f"Hi! Welcome to **{shop['name']}**! Ask me about our products, or say 'show all products'."
    if picked:
        top = picked[0]
        s = f"**{top['name']}** — {top.get('price','')} *({top.get('stock','in')})*"
        if top.get("images"): s += f"\n![{top['name']}]({top['images'][0]})"
        return s
    return "I couldn't find a direct match. Try asking 'show all products' to see everything we have."

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — System
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/ui", response_class=HTMLResponse)
def serve_ui():
    with open(os.path.join(SERVER_DIR, "ui.html"), encoding="utf-8") as f: return f.read()

@app.get("/")
def root(): return {"status": "running", "version": "4.1 Supabase"}

@app.get("/health")
def health(): return {"ok": True}

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
        res = supabase.auth.sign_up({"email": body.email, "password": body.password, "options": {"data": {"display_name": body.display_name.strip()}}})
        return {"ok": True, "message": "Account created! Check your email to verify."}
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
    try: supabase.auth.resend({"type": "signup", "email": user.email})
    except Exception: pass
    return {"ok": True, "message": "Verification email sent"}

@app.post("/auth/forgot-password")
def forgot_password(body: ForgotPasswordReq):
    try: supabase.auth.reset_password_for_email(body.email.strip(), options={"redirect_to": f"{APP_BASE_URL}/ui"})
    except Exception: pass
    return {"ok": True, "message": "If that email exists, a reset link has been sent."}

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
    out = []
    for f in favs:
        p = supabase.table("products").select("*").eq("shop_id", f["shop_id"]).eq("product_id", f["product_id"]).execute().data
        if p: out.append(serialize_product(p[0], user.id))
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
        r["stats"] = shop_stats(r["shop_id"])
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
    ser_prods = [serialize_product(p, user_id) for p in paged["items"]]
    
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
    return {"ok": True, "shop_id": shop_id, "q": q, "results": [serialize_product(r) for r in paged["items"]], "total": len(rows), "pagination": paged["pagination"]}

@app.get("/public/search/global")
def search_global(request: Request, q: str = Query(...), page: int = Query(1, ge=1), limit: int = Query(PAGE_SIZE, ge=1, le=60)):
    qn = (q or "").strip()
    if not qn: return {"ok": True, "q": q, "results": [], "total": 0, "pagination": paginate_list([], 1, limit)["pagination"]}
    rows = supabase.table("products").select("*, shops(name, address, whatsapp)").or_(f"name.ilike.%{qn}%,overview.ilike.%{qn}%").order("updated_at", desc=True).execute().data
    
    paged = paginate_list(rows, page, limit)
    results = [serialize_product(r) for r in paged["items"]]
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
    results = [serialize_product(r) for r in paged["items"]]
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

    picked = rank_products(prod_rows, q)
    abs_picked = [serialize_product(p) for p in picked[:4]]

    # 1. Shortcut: Show Images
    if wants_all_images(q):
        gallery = []
        for r in prod_rows:
            imgs = r.get("images", [])
            if isinstance(imgs, str):
                try: imgs = json.loads(imgs)
                except: imgs = []
            gallery.extend(imgs)
        if gallery:
            ans = f"Here are all photos from **{shop['name']}**:\n" + "\n".join(f"![Image]({url})" for url in gallery[:40])
        else:
            ans = "This shop hasn't uploaded any product photos yet."
        return {"answer": ans, "products": abs_picked, "meta": {"llm_used": False}}

    # 2. Shortcut: Greeting
    if is_greeting(q):
        return {"answer": f"Hi! Welcome to **{shop['name']}**! Ask me about our products, or say 'show all products'.", "products": abs_picked, "meta": {"llm_used": False}}

    # 3. Handle Full Catalog Requests safely
    if is_list_intent(q):
        picked = prod_rows
        abs_picked = [serialize_product(p) for p in prod_rows[:8]]

    try:
        ans = llm_chat(SHOP_ASSISTANT_SYSTEM, f"CONTEXT:\n{build_context(shop, picked, prod_rows)}\n\nCUSTOMER: {q}")
        return {"answer": ans, "products": abs_picked, "meta": {"llm_used": True, "model": OPENROUTER_MODEL}}
    except Exception as e:
        err_msg = str(e)
        if hasattr(e, "response") and getattr(e, "response") is not None:
            err_msg += f" | {e.response.text}"
            
        print(f"[Chat LLM Exception] Fallback triggered: {err_msg}")
        
        if is_list_intent(q):
            ans = f"Here's what's available at **{shop['name']}**:\n" + "\n".join(f"• {r['name']} — {r.get('price','')} *({r.get('stock','in')})*" for r in prod_rows[:30])
        else:
            ans = fallback_answer(shop, picked, q)

        return {
            "answer": ans,
            "products": abs_picked,
            "meta": {"llm_used": False, "reason": "fallback_after_llm_error"}
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
    
    ser_prods = [serialize_product(p) for p in prods]
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
    
    evs = supabase.table("analytics").select("event, product_id").eq("shop_id", shop_id).gte("created_at", since_iso).execute().data
    
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
        
    return {"ok": True, "totals": totals, "top_products": top, "daily_chats": daily}
