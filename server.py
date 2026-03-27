"""
Wave API  v4.0 — Supabase Edition
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from fastapi import FastAPI, Query, Header, HTTPException, UploadFile, File, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
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

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL else None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# APP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
app = FastAPI(title="Wave API (Supabase)", version="4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/shops", StaticFiles(directory=SHOPS_DIR), name="shops")

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
        prof_data = supabase.table("profiles").select("*").eq("id", user.id).execute().data
        prof = prof_data[0] if prof_data else {}
        return user, prof
    except Exception:
        raise HTTPException(401, "Session expired or invalid")

def require_verified(user):
    if not user.email_confirmed_at:
        raise HTTPException(403, "Email not verified. Check your inbox.")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LLM 
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def llm_chat(system: str, user: str, max_tokens: int = 400) -> str:
    if not OPENROUTER_KEY:
        raise ValueError("Missing LLM API Key")
        
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
    out = []
    for f in files or []:
        if not f or not getattr(f, "filename", None): continue
        ext = os.path.splitext(f.filename.lower())[1]
        if ext not in ALLOWED_IMG_EXTS: raise HTTPException(400, f"Unsupported type: {ext}")
        name = f"{uuid.uuid4().hex}{norm_ext(ext)}"
        path = f"{shop_id}/{name}"
        
        try:
            supabase.storage.from_("product-images").upload(path, f.file.read(), {"content-type": f.content_type})
            out.append(supabase.storage.from_("product-images").get_public_url(path))
        except Exception as e:
            print(f"[Supabase Storage Error] {e}")
            raise HTTPException(500, "Failed to save image. Did you run the SQL query to create the 'product-images' bucket in Supabase?")
    return out

def serialize_product(row: dict, req: Request = None, user_id: str = None) -> Dict[str, Any]:
    imgs = row.get("images", [])
    rv = supabase.table("reviews").select("rating").eq("shop_id", row["shop_id"]).eq("product_id", row["product_id"]).execute().data
    is_fav = False
    if user_id:
        is_fav = len(supabase.table("favourites").select("shop_id").eq("user_id", user_id).eq("shop_id", row["shop_id"]).eq("product_id", row["product_id"]).execute().data) > 0
    return {
        "product_id": row["product_id"], "shop_id": row["shop_id"], "name": row["name"],
        "overview": row.get("overview", ""), "price": row.get("price", ""),
        "stock": row.get("stock", "in"), "variants": row.get("variants", ""),
        "images": imgs, "image_count": len(imgs),
        "avg_rating": round(sum(r["rating"] for r in rv)/len(rv) if rv else 0, 1),
        "review_count": len(rv), "is_favourite": is_fav,
    }

def shop_stats(shop_id: str) -> Dict:
    prods = supabase.table("products").select("images").eq("shop_id", shop_id).execute().data
    with_imgs = sum(1 for p in prods if p.get("images"))
    imgs = sum(len(p.get("images", [])) for p in prods)
    
    since = int(time.time()) - 86400 * 30
    since_iso = datetime.fromtimestamp(since, tz=timezone.utc).isoformat()
    
    chats = len(supabase.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "chat").gte("created_at", since_iso).execute().data)
    views = len(supabase.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "view").gte("created_at", since_iso).execute().data)
    rvs = supabase.table("reviews").select("rating").eq("shop_id", shop_id).execute().data
    avg_r = sum(r["rating"] for r in rvs)/len(rvs) if rvs else 0
    return {"product_count": len(prods), "products_with_images": with_imgs, "image_count": imgs, "chat_hits_30d": chats, "product_views_30d": views, "avg_rating": round(avg_r, 1)}

def track(shop_id: str, event: str, product_id: Optional[str] = None):
    try: supabase.table("analytics").insert({"shop_id": shop_id, "product_id": product_id, "event": event}).execute()
    except Exception: pass

def write_shop_json(shop_id: str):
    shop = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data
    if not shop: return
    prods = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute().data
    obj = {"shop": {k: shop[0].get(k, "") for k in ("name","address","overview","phone","hours","category")},
           "products": [serialize_product(p) for p in prods]}
    d = os.path.join(SHOPS_DIR, shop_id)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "shop.json"), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def rebuild_kb(shop_id: str):
    write_shop_json(shop_id)
    if HAS_RAG:
        try: _build_kb(os.path.join(SHOPS_DIR, shop_id))
        except Exception: pass

# ── Chat helpers ──
def wants_all_images(q: str) -> bool:
    qn = norm_text(q)
    if "gallery" in qn: return True
    if "all" not in qn and "every" not in qn: return False
    return any(v in qn for v in ["image","images","photo","photos","picture","pictures"])

def is_greeting(q: str) -> bool:
    return norm_text(q) in {"hi","hello","hey","hii","hlo","good morning","good evening","good afternoon"}

def is_list_intent(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in ["what do you sell","what products","what items","what do you have","show products","product list","catalog","menu","inventory","show all products","list products","all products"])

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
        if s > 0: scored.append((s, {"product_id": r["product_id"], "name": r["name"], "overview": r.get("overview",""), "price": r.get("price",""), "stock": r.get("stock","in"), "images": r.get("images",[])}))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored]

def build_context(shop: dict, picked: List[Dict], include_all: bool, all_rows: List) -> str:
    lines = [f"Shop: {shop['name']}", f"Address: {shop.get('address','')}"]
    if shop.get("phone"): lines.append(f"Phone: {shop['phone']}")
    if shop.get("hours"): lines.append(f"Hours: {shop['hours']}")
    if shop.get("overview"): lines.append(f"About: {shop['overview']}")
    lines.append(f"Total products: {len(all_rows)}")
    if include_all: lines.append("All products: " + ", ".join(p["name"] for p in all_rows[:40]))
    if picked:
        lines.append("\nMatching products:")
        for p in picked[:6]:
            img_line = f' | Photo: ![{p["name"]}]({p["images"][0]})' if p.get("images") else ""
            lines.append(f'• {p["name"]} | Price: {p.get("price","N/A")} | Stock: {p.get("stock","in")} | {p.get("overview","")}{img_line}')
    return "\n".join(lines)

SHOP_ASSISTANT_SYSTEM = """\
You are a friendly, knowledgeable sales assistant for a local shop.
Rules:
- Use ONLY information from the shop context provided. Never invent products, prices, or policies.
- Be conversational, warm, and concise.
- When a product has a Photo field with a markdown image like ![name](url), include it exactly as-is in your response — the app will render it as an actual photo.
- Mention price and stock status when relevant.
- If something is out of stock, say so clearly.
"""

def fallback_answer(shop: dict, picked: List[Dict], all_rows: List, q: str) -> str:
    if is_greeting(q): return f"Hi! Welcome to **{shop['name']}**! Ask me about anything, or say 'show all products'."
    if is_list_intent(q) and all_rows:
        return f"Here's what's available at **{shop['name']}**:\n" + "\n".join(f"• {r['name']} — {r.get('price','')} *({r.get('stock','in')})*" for r in all_rows[:30])
    if picked:
        top = picked[0]
        s = f"**{top['name']}** — {top.get('price','')}\n{top.get('overview','')}"
        if top.get("images"): s += f"\n![{top['name']}]({top['images'][0]})"
        if len(picked) > 1: s += "\n\nYou may also like: " + ", ".join(p["name"] for p in picked[1:4])
        return s
    return "I couldn't find a specific match, but I can help you explore our products! Try asking 'show all products'."

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
        prof = supabase.table("profiles").select("*").eq("id", user.id).execute().data[0]
        return {"ok": True, "token": res.session.access_token, "email": user.email, "display_name": prof.get("display_name", ""), "avatar_url": prof.get("avatar_url", ""), "email_verified": user.email_confirmed_at is not None, "role": prof.get("role", "customer")}
    except Exception:
        raise HTTPException(401, "Invalid email or password")

@app.get("/auth/me")
def auth_me(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    shops = supabase.table("shops").select("shop_id, name").eq("owner_user_id", user.id).execute().data
    fav_count = len(supabase.table("favourites").select("shop_id").eq("user_id", user.id).execute().data)
    rev_count = len(supabase.table("reviews").select("id").eq("user_id", user.id).execute().data)
    return {
        "ok": True, "user_id": user.id, "email": user.email,
        "display_name": prof.get("display_name", ""),
        "avatar_url": prof.get("avatar_url", ""),
        "email_verified": user.email_confirmed_at is not None,
        "role": prof.get("role", "customer"),
        "my_shops": shops, "fav_count": fav_count, "review_count": rev_count,
    }

@app.post("/auth/logout")
def logout(authorization: Optional[str] = Header(None)):
    return {"ok": True}

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
    if not avatar.filename: raise HTTPException(400, "No file provided")
    try:
        ext = norm_ext(os.path.splitext(avatar.filename.lower())[1])
        path = f"avatars/user_{user.id}_{uuid.uuid4().hex[:8]}{ext}"
        supabase.storage.from_("product-images").upload(path, avatar.file.read(), {"content-type": avatar.content_type})
        url = supabase.storage.from_("product-images").get_public_url(path)
        supabase.table("profiles").update({"avatar_url": url}).eq("id", user.id).execute()
        return {"ok": True, "avatar_url": url}
    except Exception as e:
        print(f"Avatar Upload Error: {e}")
        raise HTTPException(500, "Avatar upload failed. Is your storage bucket configured?")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Customer
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.post("/customer/favourite/{shop_id}/{product_id}")
def toggle_favourite(shop_id: str, product_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    exists = len(supabase.table("favourites").select("shop_id").eq("user_id", user.id).eq("shop_id", shop_id).eq("product_id", product_id).execute().data) > 0
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
        if p: out.append(serialize_product(p[0], request, user.id))
    return {"ok": True, "favourites": out}

@app.get("/public/reviews/{shop_id}/{product_id}")
def get_reviews(shop_id: str, product_id: str):
    rows = supabase.table("reviews").select("*, profiles(display_name)").eq("shop_id", shop_id).eq("product_id", product_id).order("created_at", desc=True).execute().data
    return {"ok": True, "reviews": [{"id": r["id"], "rating": r["rating"], "body": r["body"], "author": r["profiles"].get("display_name") or "User", "created_at": r["created_at"]} for r in rows]}

@app.post("/customer/review/{shop_id}/{product_id}")
def post_review(shop_id: str, product_id: str, body: ReviewReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    if not body.body.strip(): raise HTTPException(400, "Review cannot be empty")
    if not supabase.table("products").select("product_id").eq("shop_id", shop_id).eq("product_id", product_id).execute().data: raise HTTPException(404, "Product not found")
    try: supabase.table("reviews").insert({"shop_id": shop_id, "product_id": product_id, "user_id": user.id, "rating": body.rating, "body": body.body.strip()}).execute()
    except Exception: supabase.table("reviews").update({"rating": body.rating, "body": body.body.strip()}).eq("shop_id", shop_id).eq("product_id", product_id).eq("user_id", user.id).execute()
    return {"ok": True, "message": "Review saved"}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Public Browsing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/public/shops")
def public_shops(category: Optional[str] = None):
    q = supabase.table("shops").select("*").order("created_at", desc=True)
    if category: q = q.ilike("category", category)
    rows = q.execute().data
    for r in rows: r["stats"] = shop_stats(r["shop_id"])
    return {"ok": True, "shops": rows}

@app.get("/public/shop/{shop_id}")
def public_shop(shop_id: str, request: Request, page: int = Query(1, ge=1), sort: str = Query("default"), stock: str = Query(""), authorization: Optional[str] = Header(None)):
    shop = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data
    if not shop: raise HTTPException(404, "Shop not found")
    user_id = get_user(authorization)[0].id if authorization else None
    
    q = supabase.table("products").select("*").eq("shop_id", shop_id)
    if stock in ("in", "low", "out"): q = q.eq("stock", stock)
    all_prods = q.execute().data
    
    if sort == "price-asc": all_prods.sort(key=lambda x: float(re.sub(r'[^\d.]', '', x.get("price") or "0") or 0))
    elif sort == "price-desc": all_prods.sort(key=lambda x: float(re.sub(r'[^\d.]', '', x.get("price") or "0") or 0), reverse=True)
    else: all_prods.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    
    offset = (page - 1) * PAGE_SIZE
    track(shop_id, "shop_view")
    return {
        "ok": True, "shop_id": shop_id, "shop": shop[0],
        "products": [serialize_product(p, request, user_id) for p in all_prods[offset:offset + PAGE_SIZE]],
        "pagination": {"page": page, "page_size": PAGE_SIZE, "total": len(all_prods), "pages": max(1, -(-len(all_prods) // PAGE_SIZE))},
        "stats": shop_stats(shop_id), "suggested_questions": ["What products do you have?", "Show all products", "What's in stock?", "Show me your best product", "Show all images"],
    }

@app.get("/public/search")
def search_shop(request: Request, shop_id: str = Query(...), q: str = Query(...), page: int = Query(1, ge=1), limit: int = Query(24, le=100)):
    qn = (q or "").strip()
    if not qn: return {"ok": True, "shop_id": shop_id, "q": q, "results": [], "total": 0}
    rows = supabase.table("products").select("*").eq("shop_id", shop_id).or_(f"name.ilike.%{qn}%,overview.ilike.%{qn}%").order("updated_at", desc=True).execute().data
    offset = (page - 1) * limit
    return {"ok": True, "shop_id": shop_id, "q": q, "results": [serialize_product(r, request) for r in rows[offset:offset + limit]], "total": len(rows), "pagination": {"page": page, "page_size": limit, "total": len(rows), "pages": max(1, -(-len(rows)//limit))}}

@app.get("/public/search/global")
def search_global(request: Request, q: str = Query(...), page: int = Query(1, ge=1), limit: int = Query(24, le=60)):
    qn = (q or "").strip()
    if not qn: return {"ok": True, "q": q, "results": [], "total": 0}
    rows = supabase.table("products").select("*, shops(name, address, whatsapp)").or_(f"name.ilike.%{qn}%,overview.ilike.%{qn}%").order("updated_at", desc=True).execute().data
    offset = (page - 1) * limit
    results = []
    for r in rows[offset:offset + limit]:
        prod = serialize_product(r, request)
        prod["shop_name"] = r.get("shops", {}).get("name", "")
        prod["shop_address"] = r.get("shops", {}).get("address", "")
        prod["shop_whatsapp"] = r.get("shops", {}).get("whatsapp", "")
        results.append(prod)
    return {"ok": True, "q": q, "results": results, "total": len(rows), "pagination": {"page": page, "page_size": limit, "total": len(rows), "pages": max(1, -(-len(rows)//limit))}}

@app.get("/public/top-products")
def top_products(request: Request, limit: int = Query(12, le=40), category: str = Query("")):
    q = supabase.table("products").select("*, shops!inner(name, category)").neq("stock", "out")
    if category: q = q.ilike("shops.category", category)
    rows = q.limit(limit).execute().data
    results = []
    for r in rows:
        prod = serialize_product(r, request)
        prod["shop_name"] = r.get("shops", {}).get("name", "")
        results.append(prod)
    return {"ok": True, "products": results}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Chat
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/chat")
def chat_endpoint(request: Request, shop_id: str = Query(...), q: str = Query(...)):
    q = (q or "").strip()
    if not q: raise HTTPException(400, "Missing q")
    track(shop_id, "chat")

    shop = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data
    if not shop: return {"answer": "Shop not found.", "products": [], "meta": {"llm_used": False}}
    shop = shop[0]
    prod_rows = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute().data

    if is_greeting(q):
        cards = [serialize_product(p, request) for p in prod_rows[:4]]
        return {"answer": fallback_answer(shop, cards, prod_rows, q), "products": cards, "meta": {"llm_used": False, "source": "greeting", "suggestions": ["What products do you have?","Show all products","Show me your best product"]}}

    picked = rank_products(prod_rows, q)
    source = "keyword"
    if not picked and HAS_RAG:
        try:
            ret = _retrieve(shop_dir(shop_id), q, top_k=6)
            pids = dedup([re.search(r"Product ID:\s*([A-Za-z0-9\-_]+)", m.get("text","")).group(1) for m in ret.get("matches", []) if re.search(r"Product ID:\s*([A-Za-z0-9\-_]+)", m.get("text",""))])
            for pid in pids[:4]:
                row = supabase.table("products").select("*").eq("shop_id", shop_id).eq("product_id", pid).execute().data
                if row: picked.append(row[0])
            if picked: source = "rag"
        except Exception: pass
    if not picked:
        picked = supabase.table("products").select("*").eq("shop_id", shop_id).or_(f"name.ilike.%{q}%,overview.ilike.%{q}%").order("updated_at", desc=True).limit(4).execute().data
        source = "db"

    abs_picked = [serialize_product(p, request) for p in picked]

    if wants_all_images(q):
        gallery = []
        for r in prod_rows: gallery.extend(r.get("images", []))
        answer = f"Here are all photos from **{shop['name']}**:\n" + "\n".join(f"![Image]({url})" for url in gallery[:40]) if gallery else "This shop hasn't uploaded any product photos yet."
        return {"answer": answer, "products": abs_picked, "meta": {"llm_used": False, "source": "gallery"}}

    if is_list_intent(q):
        try:
            answer = llm_chat(SHOP_ASSISTANT_SYSTEM, f"SHOP CONTEXT:\n{build_context(shop, prod_rows, True, prod_rows)}\n\nCUSTOMER:\n{q}", 500)
            return {"answer": answer, "products": [serialize_product(p, request) for p in prod_rows[:8]], "meta": {"llm_used": True, "source": "list"}}
        except Exception as e: 
            print(f"[Chat LLM Exception] {e}") # Silently log, don't show user
            pass

    try:
        answer = llm_chat(SHOP_ASSISTANT_SYSTEM, f"SHOP CONTEXT:\n{build_context(shop, picked, False, prod_rows)}\n\nCUSTOMER:\n{q}", 350)
        llm_used = True
    except Exception as e:
        print(f"[Chat LLM Exception] {e}") # Silently log, don't show user
        answer = fallback_answer(shop, abs_picked, prod_rows, q)
        llm_used = False

    return {"answer": answer, "products": abs_picked, "meta": {"llm_used": llm_used, "source": source, "model": OPENROUTER_MODEL if llm_used else "None", "suggestions": [f"Tell me more about {p['name']}" for p in abs_picked[:2]] + ["Show all products", "What's in stock?"] if llm_used else []}}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Admin
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_shop_owner(user_id: str, shop_id: str):
    if not supabase.table("shops").select("shop_id").eq("shop_id", shop_id).eq("owner_user_id", user_id).execute().data:
        raise HTTPException(404, "Not found or not yours")

@app.post("/admin/create-shop")
def create_shop(body: CreateShopReq, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    require_verified(user)
    shop = body.shop
    if not shop.name.strip() or not shop.address.strip(): raise HTTPException(400, "Shop name and address required")
    sid = slug(body.shop_id) if body.shop_id else gen_shop_id(shop.name)
    if supabase.table("shops").select("shop_id").eq("shop_id", sid).execute().data: raise HTTPException(409, "shop_id already exists")
    
    supabase.table("shops").insert({"shop_id": sid, "owner_user_id": user.id, "name": shop.name.strip(), "address": shop.address.strip(), "overview": shop.overview, "phone": shop.phone, "hours": shop.hours, "category": shop.category, "whatsapp": shop.whatsapp}).execute()
    supabase.table("profiles").update({"role": "shopkeeper"}).eq("id", user.id).execute()
    os.makedirs(os.path.join(SHOPS_DIR, sid), exist_ok=True)
    rebuild_kb(sid)
    return {"ok": True, "shop_id": sid, "message": "Shop created"}

@app.get("/admin/my-shops")
def my_shops(authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    rows = supabase.table("shops").select("*").eq("owner_user_id", user.id).order("created_at", desc=True).execute().data
    for r in rows: r["stats"] = shop_stats(r["shop_id"])
    return {"ok": True, "shops": rows}

@app.get("/admin/shop/{shop_id}")
def admin_get_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = get_user(authorization)
    check_shop_owner(user.id, shop_id)
    shop = supabase.table("shops").select("*").eq("shop_id", shop_id).execute().data[0]
    prods = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute().data
    return {"ok": True, "shop_id": shop_id, "data": {"shop": shop, "products": [serialize_product(p) for p in prods], "stats": shop_stats(shop_id)}}

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
    merged = dedup((existing[0].get("images", []) if existing else []) + new_urls)
    
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
    try:
        supabase.storage.from_("product-images").remove([f"{shop_id}/{img_name}"])
    except Exception: pass
    
    new_imgs = [u for u in row[0].get("images", []) if os.path.basename(u) != img_name]
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
    
    evs = supabase.table("analytics").select("event").eq("shop_id", shop_id).gte("created_at", since_iso).execute().data
    totals = {"chat": 0, "shop_view": 0, "view": 0}
    for e in evs:
        if e["event"] in totals: totals[e["event"]] += 1
        
    top_data = supabase.table("analytics").select("product_id").eq("shop_id", shop_id).eq("event", "view").gte("created_at", since_iso).execute().data
    counts = {}
    for d in top_data:
        if d["product_id"]: counts[d["product_id"]] = counts.get(d["product_id"], 0) + 1
    top = [{"product_id": pid, "views": c} for pid, c in sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]]
    for t in top:
        p = supabase.table("products").select("name").eq("shop_id", shop_id).eq("product_id", t["product_id"]).execute().data
        t["name"] = p[0]["name"] if p else t["product_id"]

    now = int(time.time())
    daily = []
    for d in range(13, -1, -1):
        s = now - 86400 * (d + 1); e = now - 86400 * d
        s_iso = datetime.fromtimestamp(s, tz=timezone.utc).isoformat()
        e_iso = datetime.fromtimestamp(e, tz=timezone.utc).isoformat()
        
        c = len(supabase.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "chat").gte("created_at", s_iso).lt("created_at", e_iso).execute().data)
        daily.append({"date": datetime.fromtimestamp(e, tz=timezone.utc).strftime("%b %d"), "chats": c})
        
    return {"ok": True, "shop_id": shop_id, "days": days, "totals": totals, "top_products": top, "daily_chats": daily}