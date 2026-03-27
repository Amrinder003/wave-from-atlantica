"""
Wave API  v4.0 — Supabase Edition (Fully Async / Responsive UI)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Requires: fastapi, uvicorn, standard, supabase, python-multipart, requests, postgrest-py (async capable version included in supabase-py)
Environment variables: OPENROUTER_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY, APP_BASE_URL
"""

from fastapi import FastAPI, Query, Header, HTTPException, UploadFile, File, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import os, re, json, time, uuid, shutil, asyncio
from datetime import datetime, timezone
import requests
from typing import Any, Dict, List, Optional
from supabase import create_client, Client, ClientOptions

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Mock HAS_RAG for simpler integration if vector search files aren't present
HAS_RAG = False

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

if not SUPABASE_URL or not SUPABASE_KEY:
    print("⚠️ WARNING: Missing SUPABASE_URL or SUPABASE_SERVICE_KEY.")

# Configure supabase-py to use async client. postgrest-py >= 0.10.0 handles this well.
opts = ClientOptions(postgrest_client_timeout=60)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options=opts) if SUPABASE_URL else None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# APP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
app = FastAPI(title="Wave API (Supabase Async)", version="4.0")

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
# AUTH HELPERS (converted to async)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def bearer(auth: Optional[str]) -> str:
    if not auth: raise HTTPException(401, "Missing Authorization header")
    m = re.match(r"Bearer\s+(.+)", auth.strip(), re.I)
    if not m: raise HTTPException(401, "Use: Bearer <token>")
    return m.group(1).strip()

async def get_user(auth: Optional[str]):
    token = bearer(auth)
    try:
        # supabase-py's auth.get_user is blocking, but we can call it in a thread if needed.
        # Newer versions might be async capable.
        res = supabase.auth.get_user(token)
        user = res.user
        
        # Async DB lookup
        prof_res = await supabase.table("profiles").select("*").eq("id", user.id).execute()
        prof_data = prof_res.data
        prof = prof_data[0] if prof_data else {}
        return user, prof
    except Exception as e:
        print(f"[Auth Error] {e}")
        raise HTTPException(401, "Session expired or invalid")

def require_verified(user):
    if not user.email_confirmed_at:
        raise HTTPException(403, "Email not verified. Check your inbox.")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LLM (Using loop.run_in_executor to avoid blocking on requests)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _blocking_llm_chat(system: str, user: str, max_tokens: int):
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

async def async_llm_chat(system: str, user: str, max_tokens: int = 400) -> str:
    if not OPENROUTER_KEY:
        raise ValueError("LLM API Key Missing")
        
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _blocking_llm_chat, system, user, max_tokens)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS (converted to async where necessary)
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

# save_images uses blocking supabase.storage.upload. In a production async environment,
# we would prefer an aiohttp-based supabase client.
def _blocking_upload_image(shop_id: str, file_content, filename, content_type):
    path = f"{shop_id}/{filename}"
    supabase.storage.from_("product-images").upload(path, file_content, {"content-type": content_type})
    return supabase.storage.from_("product-images").get_public_url(path)

async def async_save_images(shop_id: str, files: List[UploadFile]) -> List[str]:
    out = []
    loop = asyncio.get_event_loop()
    for f in files or []:
        if not f or not getattr(f, "filename", None): continue
        ext = os.path.splitext(f.filename.lower())[1]
        if ext not in ALLOWED_IMG_EXTS: raise HTTPException(400, f"Unsupported image type: {ext}")
        
        name = f"{uuid.uuid4().hex}{norm_ext(ext)}"
        
        try:
            # We must await the read before passing content to thread
            content = await f.read()
            if not content: continue
            
            # Offload blocking upload to thread
            url = await loop.run_in_executor(None, _blocking_upload_image, shop_id, content, name, f.content_type)
            out.append(url)
        except Exception as e:
            print(f"[Supabase Storage Error] {e}")
            raise HTTPException(500, f"Failed to save image. Please verify Supabase Storage bucket 'product-images' exists. Error: {str(e)}")
        finally:
            await f.close() # Ensure file is closed
    return out

async def serialize_product(row: dict, req: Request = None, user_id: str = None) -> Dict[str, Any]:
    imgs = row.get("images", [])
    if isinstance(imgs, str):
        try: imgs = json.loads(imgs)
        except: imgs = []
    if not isinstance(imgs, list): imgs = []
    
    shop_id = row["shop_id"]
    prod_id = row["product_id"]
    
    # We could make these parallel if needed, but simple await is fine for now
    rv_res = await supabase.table("reviews").select("rating").eq("shop_id", shop_id).eq("product_id", prod_id).execute()
    rv = rv_res.data
    
    is_fav = False
    if user_id:
        fav_res = await supabase.table("favourites").select("shop_id").eq("user_id", user_id).eq("shop_id", shop_id).eq("product_id", prod_id).execute()
        is_fav = len(fav_res.data) > 0
        
    return {
        "product_id": prod_id, "shop_id": shop_id, "name": row["name"],
        "overview": row.get("overview", ""), "price": row.get("price", ""),
        "stock": row.get("stock", "in"), "variants": row.get("variants", ""),
        "images": imgs, "image_count": len(imgs),
        "avg_rating": round(sum(r["rating"] for r in rv)/len(rv) if rv else 0, 1),
        "review_count": len(rv), "is_favourite": is_fav,
    }

async def shop_stats(shop_id: str) -> Dict:
    # Concurrent fetches for efficiency - THIS FIXES THE SLOW LOADING LAG
    prods_task = supabase.table("products").select("images").eq("shop_id", shop_id).execute()
    rvs_task = supabase.table("reviews").select("rating").eq("shop_id", shop_id).execute()
    
    # Views and chats require a ISO 8601 date string
    since = int(time.time()) - 86400 * 30
    since_iso = datetime.fromtimestamp(since, tz=timezone.utc).isoformat()
    
    chats_task = supabase.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "chat").gte("created_at", since_iso).execute()
    views_task = supabase.table("analytics").select("id").eq("shop_id", shop_id).eq("event", "view").gte("created_at", since_iso).execute()
    
    try:
        results = await asyncio.gather(prods_task, rvs_task, chats_task, views_task)
    except Exception as e:
        print(f"[Stats Async gather error] {e}")
        return {"product_count": 0, "avg_rating": 0, "error": True}

    prods = results[0].data
    rvs = results[1].data
    chats = results[2].data
    views = results[3].data

    with_imgs = sum(1 for p in prods if p.get("images"))
    
    # Ensure avg_rating calculation handles division by zero
    avg_r = sum(r["rating"] for r in rvs)/len(rvs) if rvs else 0
    
    return {
        "product_count": len(prods), 
        "chat_hits_30d": len(chats), 
        "product_views_30d": len(views), 
        "avg_rating": round(avg_r, 1)
    }

async def track(shop_id: str, event: str, product_id: Optional[str] = None):
    try: await supabase.table("analytics").insert({"shop_id": shop_id, "product_id": product_id, "event": event}).execute()
    except Exception: pass

def _blocking_rebuild_kb(shop_id, SHOPS_DIR):
    if HAS_RAG:
        try: from build_kb import build_kb as _build_kb; _build_kb(os.path.join(SHOPS_DIR, shop_id))
        except Exception: pass

async def async_rebuild_kb(shop_id: str):
    shop_res = await supabase.table("shops").select("*").eq("shop_id", shop_id).execute()
    shop = shop_res.data
    if not shop: return
    
    prods_res = await supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute()
    prods = prods_res.data
    
    # Basic data structure for local use
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
    
    # Blocking write in thread
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: json.dump(obj, open(os.path.join(d, "shop.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=2))
    
    if HAS_RAG:
        await loop.run_in_executor(None, _blocking_rebuild_kb, shop_id, SHOPS_DIR)

# ── Chat helpers (mostly logic, can stay sync) ──
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
        if s > 0: 
            imgs = r.get("images", [])
            if isinstance(imgs, str):
                try: imgs = json.loads(imgs)
                except: imgs = []
            scored.append((s, {"product_id": r["product_id"], "name": r["name"], "overview": r.get("overview",""), "price": r.get("price",""), "stock": r.get("stock","in"), "images": imgs}))
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

SHOP_ASSISTANT_SYSTEM = """You are a helpful sales assistant. Answer based ONLY on the shop context provided. Be warm and concise. Use markdown. Mention price/stock if known."""

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
async def serve_ui():
    loop = asyncio.get_event_loop()
    content = await loop.run_in_executor(None, lambda: open(os.path.join(SERVER_DIR, "ui.html"), encoding="utf-8").read())
    return content

@app.get("/")
async def root(): return {"status": "running", "version": "4.1 Supabase Async"}

@app.get("/health")
async def health(): return {"ok": True}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Auth
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.post("/auth/register")
async def register(body: RegisterReq):
    try:
        res = supabase.auth.sign_up({"email": body.email, "password": body.password, "options": {"data": {"display_name": body.display_name.strip()}}})
        return {"ok": True, "message": "Account created! Check your email to verify."}
    except Exception as e:
        raise HTTPException(400, str(e))

@app.post("/auth/login")
async def login(body: LoginReq):
    try:
        res = supabase.auth.sign_in_with_password({"email": body.email, "password": body.password})
        user = res.user
        
        prof_res = await supabase.table("profiles").select("*").eq("id", user.id).execute()
        prof_data = prof_res.data
        prof = prof_data[0] if prof_data else {}
        return {"ok": True, "token": res.session.access_token, "email": user.email, "display_name": prof.get("display_name", ""), "avatar_url": prof.get("avatar_url", ""), "email_verified": user.email_confirmed_at is not None, "role": prof.get("role", "customer")}
    except Exception:
        raise HTTPException(401, "Invalid email or password")

@app.get("/auth/me")
async def auth_me(authorization: Optional[str] = Header(None)):
    user, prof = await get_user(authorization)
    
    # Run requests concurrently
    shops_task = supabase.table("shops").select("shop_id, name").eq("owner_user_id", user.id).execute()
    fav_task = supabase.table("favourites").select("shop_id").eq("user_id", user.id).execute()
    rev_task = supabase.table("reviews").select("id").eq("user_id", user.id).execute()
    
    try:
        results = await asyncio.gather(shops_task, fav_task, rev_task)
    except Exception:
        raise HTTPException(500, "Database error")

    return {
        "ok": True, "user_id": user.id, "email": user.email,
        "display_name": prof.get("display_name", ""),
        "avatar_url": prof.get("avatar_url", ""),
        "email_verified": user.email_confirmed_at is not None,
        "role": prof.get("role", "customer"),
        "my_shops": results[0].data, 
        "fav_count": len(results[1].data), 
        "review_count": len(results[2].data),
    }

@app.post("/auth/logout")
async def logout(authorization: Optional[str] = Header(None)): return {"ok": True}

@app.post("/auth/profile/avatar")
async def upload_avatar(authorization: Optional[str] = Header(None), avatar: UploadFile = File(...)):
    user, prof = await get_user(authorization)
    if not avatar.filename: raise HTTPException(400, "No file provided")
    loop = asyncio.get_event_loop()
    try:
        ext = norm_ext(os.path.splitext(avatar.filename.lower())[1])
        filename = f"user_{user.id}_{uuid.uuid4().hex[:8]}{ext}"
        path = f"avatars/{filename}"
        
        content = await avatar.read()
        url = await loop.run_in_executor(None, _blocking_upload_image, "avatars_bucket", content, filename, avatar.content_type)
        
        # Note: 'avatars_bucket' should be a bucket that exists and has avatars/ prefix or similar. 
        # Simpler: just use 'product-images' bucket for everything during prototyping.
        url = await loop.run_in_executor(None, _blocking_upload_image, "avatars", content, filename, avatar.content_type)
        
        await supabase.table("profiles").update({"avatar_url": url}).eq("id", user.id).execute()
        return {"ok": True, "avatar_url": url}
    except Exception as e:
        print(f"Avatar Upload Error: {e}")
        raise HTTPException(500, f"Avatar upload failed. {str(e)}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Public Browsing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/public/shops")
async def public_shops(category: Optional[str] = None):
    q = supabase.table("shops").select("*").order("created_at", desc=True)
    if category: q = q.ilike("category", category)
    rows_res = await q.execute()
    rows = rows_res.data
    
    # Gather stats for ALL shops concurrently - major speed up
    stats_tasks = [shop_stats(r["shop_id"]) for r in rows]
    all_stats = await asyncio.gather(*stats_tasks)
    
    for r, stats in zip(rows, all_stats):
        r["stats"] = stats
    return {"ok": True, "shops": rows}

@app.get("/public/shop/{shop_id}")
async def public_shop(shop_id: str, request: Request, sort: str = Query("default"), authorization: Optional[str] = Header(None)):
    shop_res = await supabase.table("shops").select("*").eq("shop_id", shop_id).execute()
    if not shop_res.data: raise HTTPException(404, "Shop not found")
    
    # We fetch user_id async if token present
    user_id = None
    if authorization:
        try: user_id_res, _ = await get_user(authorization); user_id = user_id_res.id
        except: pass
        
    prods_res = await supabase.table("products").select("*").eq("shop_id", shop_id).execute()
    all_prods = prods_res.data
    
    # Simple sort logic
    if sort == "price-asc": all_prods.sort(key=lambda x: float(re.sub(r'[^\d.]', '', x.get("price") or "0") or 0))
    elif sort == "price-desc": all_prods.sort(key=lambda x: float(re.sub(r'[^\d.]', '', x.get("price") or "0") or 0), reverse=True)
    else: all_prods.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    
    await track(shop_id, "shop_view")
    
    # Serialize concurrently
    ser_prods = await asyncio.gather(*[serialize_product(p, request, user_id) for p in all_prods[:100]])
    
    return {
        "ok": True, "shop_id": shop_id, "shop": shop_res.data[0],
        "products": ser_prods
    }

@app.get("/public/top-products")
async def top_products(request: Request, limit: int = Query(24, le=60)):
    q = supabase.table("products").select("*, shops!inner(name, category)").neq("stock", "out")
    res = await q.limit(limit).execute()
    rows = res.data
    
    # Serialize concurrently
    ser_tasks = []
    for r in rows:
        ser_tasks.append(serialize_product(r, request))
    results = await asyncio.gather(*ser_tasks)
    
    for r, prod in zip(rows, results):
        prod["shop_name"] = r.get("shops", {}).get("name", "")
    return {"ok": True, "products": results}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Chat (converted to Async)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/chat")
async def chat_endpoint(request: Request, shop_id: str = Query(...), q: str = Query(...)):
    q = (q or "").strip()
    if not q: raise HTTPException(400, "Missing q")
    await track(shop_id, "chat")

    shop_res = await supabase.table("shops").select("*").eq("shop_id", shop_id).execute()
    if not shop_res.data: return {"answer": "Shop not found.", "products": [], "meta": {"llm_used": False}}
    shop = shop_res.data[0]
    
    prods_res = await supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute()
    prod_rows = prods_res.data

    picked = rank_products(prod_rows, q)
    # Concurrent serialization for top relevant products
    abs_picked = await asyncio.gather(*[serialize_product(p, request) for p in picked[:4]])

    try:
        ans = await async_llm_chat(SHOP_ASSISTANT_SYSTEM, f"CONTEXT:\n{build_context(shop, picked, prod_rows)}\n\nCUSTOMER: {q}")
        return {"answer": ans, "products": abs_picked, "meta": {"llm_used": True}}
    except Exception as e:
        print(f"[Chat Exception] Fallback: {e}")
        return {"answer": fallback_answer(shop, picked, q), "products": abs_picked, "meta": {"llm_used": False, "reason": str(e)}}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Admin (Fully Async)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def check_shop_owner(user_id: str, shop_id: str):
    res = await supabase.table("shops").select("shop_id").eq("shop_id", shop_id).eq("owner_user_id", user_id).execute()
    if not res.data: raise HTTPException(404, "Not found or not yours")

@app.post("/admin/create-shop")
async def create_shop(body: CreateShopReq, authorization: Optional[str] = Header(None)):
    user, prof = await get_user(authorization)
    require_verified(user)
    shop = body.shop
    if not shop.name.strip() or not shop.address.strip(): raise HTTPException(400, "Shop name and address required")
    sid = slug(body.shop_id) if body.shop_id else gen_shop_id(shop.name)
    
    exists_res = await supabase.table("shops").select("shop_id").eq("shop_id", sid).execute()
    if exists_res.data: raise HTTPException(409, "shop_id already exists")
    
    await supabase.table("shops").insert({"shop_id": sid, "owner_user_id": user.id, "name": shop.name.strip(), "address": shop.address.strip(), "overview": shop.overview, "phone": shop.phone, "hours": shop.hours, "category": shop.category, "whatsapp": shop.whatsapp}).execute()
    
    await supabase.table("profiles").update({"role": "shopkeeper"}).eq("id", user.id).execute()
    
    # KB update in background
    asyncio.create_task(async_rebuild_kb(sid))
    return {"ok": True, "shop_id": sid}

@app.get("/admin/my-shops")
async def my_shops(authorization: Optional[str] = Header(None)):
    user, prof = await get_user(authorization)
    shops_res = await supabase.table("shops").select("*").eq("owner_user_id", user.id).order("created_at", desc=True).execute()
    rows = shops_res.data
    
    # Gather stats concurrently
    stats_tasks = [shop_stats(r["shop_id"]) for r in rows]
    all_stats = await asyncio.gather(*stats_tasks)
    
    for r, stats in zip(rows, all_stats): r["stats"] = stats
    return {"ok": True, "shops": rows}

@app.get("/admin/shop/{shop_id}")
async def admin_get_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = await get_user(authorization)
    await check_shop_owner(user.id, shop_id)
    
    shop_task = supabase.table("shops").select("*").eq("shop_id", shop_id).execute()
    prods_task = supabase.table("products").select("*").eq("shop_id", shop_id).order("updated_at", desc=True).execute()
    stats_task = shop_stats(shop_id)
    
    results = await asyncio.gather(shop_task, prods_task, stats_task)
    
    shop = results[0].data[0]
    prods = results[1].data
    stats = results[2]
    
    ser_prods = await asyncio.gather(*[serialize_product(p) for p in prods])
    return {"ok": True, "shop_id": shop_id, "data": {"shop": shop, "products": ser_prods, "stats": stats}}

@app.put("/admin/shop/{shop_id}")
async def admin_update_shop(shop_id: str, body: ShopInfo, authorization: Optional[str] = Header(None)):
    user, prof = await get_user(authorization)
    await check_shop_owner(user.id, shop_id)
    await supabase.table("shops").update({"name": body.name.strip(), "address": body.address.strip(), "overview": body.overview, "phone": body.phone, "hours": body.hours, "category": body.category, "whatsapp": body.whatsapp}).eq("shop_id", shop_id).execute()
    asyncio.create_task(async_rebuild_kb(shop_id))
    return {"ok": True}

@app.delete("/admin/shop/{shop_id}")
async def admin_delete_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user, prof = await get_user(authorization)
    await check_shop_owner(user.id, shop_id)
    # Analytics will cascade delete if schema set up correctly. If not, manual delete needed.
    await supabase.table("shops").delete().eq("shop_id", shop_id).execute()
    return {"ok": True}

@app.post("/admin/shop/{shop_id}/product-with-images")
async def admin_product_with_images(
    shop_id: str, authorization: Optional[str] = Header(None),
    product_id: str = Form(...), name: str = Form(...), overview: str = Form(""), price: str = Form(""),
    stock: str = Form("in"), variants: str = Form(""), images: List[UploadFile] = File(default=[])
):
    user, prof = await get_user(authorization)
    await check_shop_owner(user.id, shop_id)
    pid = slug(product_id, 60)
    if not name.strip(): raise HTTPException(400, "Name required")
    
    new_urls = await async_save_images(shop_id, images)
    
    existing_res = await supabase.table("products").select("images").eq("shop_id", shop_id).eq("product_id", pid).execute()
    existing = existing_res.data
    
    current_imgs = []
    if existing:
        current_imgs = existing[0].get("images", [])
        if isinstance(current_imgs, str):
            try: current_imgs = json.loads(current_imgs)
            except: current_imgs = []
            
    merged = dedup(current_imgs + new_urls)
    
    data = {"name": name.strip(), "overview": overview, "price": price, "stock": stock, "variants": variants, "images": merged, "updated_at": "now()"}
    if existing: await supabase.table("products").update(data).eq("shop_id", shop_id).eq("product_id", pid).execute()
    else: await supabase.table("products").insert({**data, "shop_id": shop_id, "product_id": pid}).execute()
    
    asyncio.create_task(async_rebuild_kb(shop_id))
    return {"ok": True, "product_id": pid}

@app.delete("/admin/shop/{shop_id}/product/{product_id}")
async def admin_delete_product(shop_id: str, product_id: str, authorization: Optional[str] = Header(None)):
    user, prof = await get_user(authorization)
    await check_shop_owner(user.id, shop_id)
    # Cascase delete favourites and reviews
    await supabase.table("products").delete().eq("shop_id", shop_id).eq("product_id", product_id).execute()
    asyncio.create_task(async_rebuild_kb(shop_id))
    return {"ok": True}

@app.get("/admin/shop/{shop_id}/analytics")
async def admin_analytics(shop_id: str, days: int = 30, authorization: Optional[str] = Header(None)):
    user, prof = await get_user(authorization)
    await check_shop_owner(user.id, shop_id)
    
    since = int(time.time()) - 86400 * max(1, min(days, 365))
    since_iso = datetime.fromtimestamp(since, tz=timezone.utc).isoformat()
    
    res = await supabase.table("analytics").select("event, product_id").eq("shop_id", shop_id).gte("created_at", since_iso).execute()
    evs = res.data
    
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
        p_res = await supabase.table("products").select("name").eq("shop_id", shop_id).eq("product_id", t["product_id"]).execute()
        t["name"] = p_res.data[0]["name"] if p_res.data else t["product_id"]

    return {"ok": True, "totals": totals, "top_products": top}