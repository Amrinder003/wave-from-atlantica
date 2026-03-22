from fastapi import FastAPI, Query, Header, HTTPException, UploadFile, File, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import os
import re
import json
import time
import shutil
import sqlite3
import secrets
import hashlib
import uuid
import mimetypes
import requests
from typing import Any, Dict, List, Optional, Tuple

from retrieval_chat import retrieve
from build_kb import build_kb

app = FastAPI(title="Shop Chatbot", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mimetypes.add_type("image/jpeg", ".jfif")
mimetypes.add_type("image/jpeg", ".jif")

SERVER_DIR = os.path.dirname(os.path.abspath(__file__))
SHOPS_DIR = os.path.join(SERVER_DIR, "shops")
os.makedirs(SHOPS_DIR, exist_ok=True)
app.mount("/shops", StaticFiles(directory=SHOPS_DIR), name="shops")

DB_PATH = os.path.join(SERVER_DIR, "app.db")
TOKEN_TTL_SECONDS = 60 * 60 * 24 * 7
ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".jfif", ".jif"}
OLLAMA_CHAT_URL = os.environ.get("OLLAMA_CHAT_URL", "http://127.0.0.1:11434/api/chat")
CHAT_MODEL = os.environ.get("CHAT_MODEL", "llama3.2:3b")


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    con = db()
    cur = con.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            pass_hash TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        """
    )

    cur.execute("""
        CREATE TABLE IF NOT EXISTS shops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id TEXT UNIQUE NOT NULL,
            owner_user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            address TEXT NOT NULL,
            overview TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            hours TEXT DEFAULT '',
            category TEXT DEFAULT '',
            created_at INTEGER NOT NULL,
            FOREIGN KEY(owner_user_id) REFERENCES users(id)
        );
    """)
    try: cur.execute("ALTER TABLE shops ADD COLUMN category TEXT DEFAULT ''"); con.commit()
    except Exception: pass

    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            name TEXT NOT NULL,
            overview TEXT DEFAULT '',
            price TEXT DEFAULT '',
            stock TEXT DEFAULT 'in',
            variants TEXT DEFAULT '',
            images_json TEXT DEFAULT '[]',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(shop_id, product_id)
        );
    """)
    for _col in ["ADD COLUMN stock TEXT DEFAULT 'in'", "ADD COLUMN variants TEXT DEFAULT ''"]:
        try: cur.execute(f"ALTER TABLE products {_col}"); con.commit()
        except Exception: pass

    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id TEXT NOT NULL,
            product_id TEXT,
            event TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_an ON analytics(shop_id,created_at);")

    con.commit()
    con.close()


init_db()


class RegisterRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class AuthResponse(BaseModel):
    ok: bool = True
    token: str
    email: str


class ShopInfo(BaseModel):
    name: str
    address: str
    overview: str = ""
    phone: str = ""
    hours: str = ""
    category: str = ""


class Product(BaseModel):
    product_id: str = Field(..., description="Unique id for product, e.g. p001")
    name: str
    overview: str = ""
    price: str = ""
    stock: str = "in"
    variants: str = ""
    images: List[str] = []


class CreateShopRequest(BaseModel):
    shop_id: Optional[str] = None
    shop: ShopInfo


class CreateShopResponse(BaseModel):
    ok: bool = True
    shop_id: str
    message: str


# =========================================================
# Helpers
# =========================================================
def pbkdf2_hash(password: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    h = pbkdf2_hash(password, salt)
    return f"{salt.hex()}:{h.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, hash_hex = stored.split(":")
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
        got = pbkdf2_hash(password, salt)
        return secrets.compare_digest(got, expected)
    except Exception:
        return False


def cleanup_expired_sessions():
    now = int(time.time())
    con = db()
    con.execute("DELETE FROM sessions WHERE expires_at <= ?", (now,))
    con.commit()
    con.close()


def create_session(user_id: int) -> str:
    cleanup_expired_sessions()
    token = secrets.token_urlsafe(32)
    now = int(time.time())
    exp = now + TOKEN_TTL_SECONDS
    con = db()
    con.execute(
        "INSERT INTO sessions(token,user_id,expires_at,created_at) VALUES(?,?,?,?)",
        (token, user_id, exp, now),
    )
    con.commit()
    con.close()
    return token


def extract_bearer_token(auth_header: Optional[str]) -> str:
    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    m = re.match(r"Bearer\s+(.+)", auth_header.strip(), re.IGNORECASE)
    if not m:
        raise HTTPException(status_code=401, detail="Invalid Authorization header. Use: Bearer <token>")
    token = m.group(1).strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    return token


def get_user_from_token(auth_header: Optional[str]) -> sqlite3.Row:
    token = extract_bearer_token(auth_header)

    now = int(time.time())
    con = db()
    row = con.execute(
        """SELECT u.* FROM sessions s
           JOIN users u ON u.id = s.user_id
           WHERE s.token = ? AND s.expires_at > ?""",
        (token, now),
    ).fetchone()
    con.close()

    if not row:
        raise HTTPException(status_code=401, detail="Session expired or invalid token")
    return row


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def sanitize_shop_id(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9\-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    if not s:
        raise HTTPException(status_code=400, detail="Invalid shop_id")
    return s[:40]


def sanitize_product_id(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9\-_]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    if not s:
        raise HTTPException(status_code=400, detail="Invalid product_id")
    return s[:60]


def generate_shop_id_from_name(name: str) -> str:
    base = sanitize_shop_id(name)[:24]
    suffix = secrets.token_hex(2)
    return f"{base}-{suffix}" if base else f"shop-{suffix}"


def shop_folder(shop_id: str) -> str:
    return os.path.join(SHOPS_DIR, shop_id)


def images_folder(shop_id: str) -> str:
    return os.path.join(shop_folder(shop_id), "images")


def ensure_shop_folders(shop_id: str):
    os.makedirs(shop_folder(shop_id), exist_ok=True)
    os.makedirs(images_folder(shop_id), exist_ok=True)


def build_absolute_url(request: Request, maybe_path: str) -> str:
    if not maybe_path:
        return ""
    s = str(maybe_path).strip()
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if s.startswith("shops/"):
        s = "/" + s
    if not s.startswith("/"):
        s = "/" + s
    return str(request.base_url).rstrip("/") + s


def dedupe_keep_order(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items or []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def wants_image(q: str) -> bool:
    qn = normalize_text(q)
    return any(w in qn for w in ["image", "photo", "pic", "picture", "show", "dikhao"])


def wants_all_images(q: str) -> bool:
    qn = normalize_text(q)
    if "gallery" in qn:
        return True
    if "all" not in qn and "every" not in qn:
        return False
    variants = [
        "image", "images", "img", "imgs",
        "photo", "photos",
        "picture", "pictures",
        "imge", "iamge", "imges", "inage", "inages", "imags", "imag",
    ]
    return any(v in qn for v in variants)


def is_greeting(q: str) -> bool:
    qn = normalize_text(q)
    return qn in {"hi", "hello", "hey", "hii", "hlo", "good morning", "good evening", "good afternoon"}


def is_list_products_intent(q: str) -> bool:
    qn = normalize_text(q)
    triggers = [
        "what do you sell", "what you sell", "what products", "what items",
        "what do you have", "what all do you have", "show products",
        "product list", "items list", "catalog", "catalogue",
        "menu", "inventory", "show all products", "list products",
    ]
    return any(t in qn for t in triggers)


def normalize_ext(ext: str) -> str:
    ext = (ext or "").lower()
    if ext in [".jfif", ".jif"]:
        return ".jpg"
    return ext


def parse_images_json(s: str) -> List[str]:
    try:
        imgs = json.loads(s or "[]")
        if isinstance(imgs, list):
            return dedupe_keep_order([str(x) for x in imgs if str(x).strip()])
    except Exception:
        pass
    return []


def save_uploaded_images_unique(shop_id: str, files: List[UploadFile]) -> List[str]:
    ensure_shop_folders(shop_id)
    out_urls: List[str] = []
    for f in files or []:
        if not f or not getattr(f, "filename", None):
            continue
        ext = os.path.splitext(f.filename.lower())[1]
        if ext not in ALLOWED_IMAGE_EXTS:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")

        ext = normalize_ext(ext)
        safe_name = f"{uuid.uuid4().hex}{ext}"
        save_path = os.path.join(images_folder(shop_id), safe_name)

        with open(save_path, "wb") as out:
            shutil.copyfileobj(f.file, out)

        out_urls.append(f"/shops/{shop_id}/images/{safe_name}")
    return dedupe_keep_order(out_urls)


def validate_shop_payload(shop: ShopInfo) -> ShopInfo:
    shop.name = (shop.name or "").strip()
    shop.address = (shop.address or "").strip()
    shop.overview = (shop.overview or "").strip()
    shop.phone = (shop.phone or "").strip()
    shop.hours = (shop.hours or "").strip()
    if not shop.name:
        raise HTTPException(status_code=400, detail="Shop name is required")
    if not shop.address:
        raise HTTPException(status_code=400, detail="Shop address is required")
    return shop


def validate_product_payload(product: Product) -> Product:
    product.product_id = sanitize_product_id(product.product_id)
    product.name = (product.name or "").strip()
    product.overview = (product.overview or "").strip()
    product.price = (product.price or "").strip()
    product.images = dedupe_keep_order(product.images or [])
    if not product.name:
        raise HTTPException(status_code=400, detail="Product name is required")
    return product


def shop_stats(shop_id: str) -> Dict[str, int]:
    con = db()
    rows = con.execute(
        "SELECT images_json FROM products WHERE shop_id=?",
        (shop_id,),
    ).fetchall()
    product_count = len(rows)
    total_images = 0
    products_with_images = 0
    for r in rows:
        imgs = parse_images_json(r["images_json"])
        total_images += len(imgs)
        if imgs:
            products_with_images += 1
    con.close()
    return {
        "product_count": product_count,
        "products_with_images": products_with_images,
        "image_count": total_images,
    }


def serialize_product_row(row: sqlite3.Row, request: Optional[Request] = None) -> Dict[str, Any]:
    images = parse_images_json(row["images_json"])
    if request:
        images = [build_absolute_url(request, u) for u in images]
    cols = row.keys() if hasattr(row, "keys") else []
    return {
        "product_id": row["product_id"],
        "name": row["name"],
        "overview": row["overview"] or "",
        "price": row["price"] or "",
        "stock": row["stock"] if "stock" in cols else "in",
        "variants": row["variants"] if "variants" in cols else "",
        "images": images,
        "image_count": len(images),
    }

def track(shop_id: str, event: str, product_id: Optional[str] = None):
    try:
        con = db()
        con.execute("INSERT INTO analytics(shop_id,product_id,event,created_at) VALUES(?,?,?,?)",
                    (shop_id, product_id, event, int(time.time())))
        con.commit(); con.close()
    except Exception:
        pass


def write_shop_json_from_db(shop_id: str):
    con = db()
    shop = con.execute("SELECT * FROM shops WHERE shop_id = ?", (shop_id,)).fetchone()
    if not shop:
        con.close()
        return

    products = con.execute(
        "SELECT * FROM products WHERE shop_id = ? ORDER BY updated_at DESC",
        (shop_id,),
    ).fetchall()
    con.close()

    shop_obj = {
        "shop": {
            "name": shop["name"],
            "address": shop["address"],
            "overview": shop["overview"] or "",
            "phone": shop["phone"] or "",
            "hours": shop["hours"] or "",
        },
        "products": [serialize_product_row(p) for p in products],
    }

    ensure_shop_folders(shop_id)
    path = os.path.join(shop_folder(shop_id), "shop.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(shop_obj, f, ensure_ascii=False, indent=2)


def rebuild_kb_for_shop(shop_id: str):
    write_shop_json_from_db(shop_id)
    build_kb(shop_folder(shop_id))


def ollama_chat(system_prompt: str, user_prompt: str, timeout_sec: int = 180) -> str:
    payload = {
        "model": CHAT_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "stream": False,
        "options": {"temperature": 0.2, "num_predict": 240},
    }
    r = requests.post(OLLAMA_CHAT_URL, json=payload, timeout=timeout_sec)
    r.raise_for_status()
    data = r.json()
    msg = (data.get("message") or {}).get("content") or ""
    return msg.strip()


def get_shop_and_products(shop_id: str) -> Tuple[Optional[sqlite3.Row], List[sqlite3.Row]]:
    con = db()
    shop = con.execute("SELECT * FROM shops WHERE shop_id=?", (shop_id,)).fetchone()
    products = []
    if shop:
        products = con.execute(
            "SELECT product_id, name, overview, price, images_json FROM products WHERE shop_id=? ORDER BY updated_at DESC",
            (shop_id,),
        ).fetchall()
    con.close()
    return shop, products


def abs_products(request: Request, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for p in products:
        imgs = p.get("images") or []
        abs_imgs = [build_absolute_url(request, u) for u in imgs]
        out.append({
            "product_id": p.get("product_id", ""),
            "name": p.get("name", ""),
            "overview": p.get("overview", ""),
            "price": p.get("price", ""),
            "images": dedupe_keep_order(abs_imgs),
            "image_count": len(dedupe_keep_order(abs_imgs)),
        })
    return out


def rank_products_by_query(products: List[sqlite3.Row], q: str) -> List[Dict[str, Any]]:
    qn = normalize_text(q)
    q_tokens = set(re.findall(r"[a-z0-9]+", qn))

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for r in products:
        pid = (r["product_id"] or "").strip().lower()
        name = (r["name"] or "").strip()
        overview = (r["overview"] or "").strip()
        haystack = normalize_text(f"{name} {overview}")
        name_tokens = set(re.findall(r"[a-z0-9]+", haystack))

        s = 0.0
        if pid and pid in qn:
            s += 10.0
        if normalize_text(name) and normalize_text(name) in qn:
            s += 8.0
        overlap = len(q_tokens & name_tokens)
        s += overlap * 1.4
        if qn and qn in haystack:
            s += 5.0

        if s <= 0:
            continue

        scored.append((s, {
            "product_id": r["product_id"],
            "name": r["name"],
            "overview": r["overview"] or "",
            "price": r["price"] or "",
            "images": parse_images_json(r["images_json"]),
        }))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored]


def products_from_retrieval(shop_id: str, ret: Dict[str, Any], limit: int = 4) -> List[Dict[str, Any]]:
    matches = ret.get("matches", []) or []
    pids = []
    for m in matches:
        txt = m.get("text", "") or ""
        found = re.search(r"Product ID:\s*([A-Za-z0-9\-_]+)", txt)
        if found:
            pids.append(found.group(1).strip())
    pids = dedupe_keep_order(pids)

    if not pids:
        return []

    con = db()
    out = []
    for pid in pids[:limit]:
        row = con.execute(
            "SELECT product_id, name, overview, price, images_json FROM products WHERE shop_id=? AND product_id=?",
            (shop_id, pid),
        ).fetchone()
        if not row:
            continue
        out.append({
            "product_id": row["product_id"],
            "name": row["name"],
            "overview": row["overview"] or "",
            "price": row["price"] or "",
            "images": parse_images_json(row["images_json"]),
        })
    con.close()
    return out


def db_search(shop_id: str, q: str, limit: int = 4) -> List[Dict[str, Any]]:
    like = f"%{(q or '').strip()}%"
    con = db()
    rows = con.execute(
        """
        SELECT product_id, name, overview, price, images_json
        FROM products
        WHERE shop_id=? AND (LOWER(name) LIKE LOWER(?) OR LOWER(overview) LIKE LOWER(?))
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (shop_id, like, like, int(limit)),
    ).fetchall()
    con.close()
    return [
        {
            "product_id": r["product_id"],
            "name": r["name"],
            "overview": r["overview"] or "",
            "price": r["price"] or "",
            "images": parse_images_json(r["images_json"]),
        }
        for r in rows
    ]


def collect_gallery_urls(shop_id: str, request: Request, limit: int = 40) -> List[str]:
    seen = set()
    gallery: List[str] = []

    con = db()
    rows = con.execute(
        "SELECT images_json FROM products WHERE shop_id=? ORDER BY updated_at DESC",
        (shop_id,),
    ).fetchall()
    con.close()

    for r in rows:
        for u in parse_images_json(r["images_json"]):
            au = build_absolute_url(request, u)
            if au and au not in seen:
                seen.add(au)
                gallery.append(au)
                if len(gallery) >= limit:
                    return gallery

    folder = images_folder(shop_id)
    if os.path.isdir(folder):
        for fn in sorted(os.listdir(folder)):
            ext = os.path.splitext(fn.lower())[1]
            if ext in ALLOWED_IMAGE_EXTS:
                url = build_absolute_url(request, f"/shops/{shop_id}/images/{fn}")
                if url and url not in seen:
                    seen.add(url)
                    gallery.append(url)
                    if len(gallery) >= limit:
                        return gallery

    return gallery


def build_context(shop: sqlite3.Row, picked: List[Dict[str, Any]], include_all_names: bool, all_products: List[sqlite3.Row]) -> str:
    lines = [
        f"Shop Name: {shop['name']}",
        f"Address: {shop['address']}",
    ]
    if shop["phone"]:
        lines.append(f"Phone: {shop['phone']}")
    if shop["hours"]:
        lines.append(f"Hours: {shop['hours']}")
    if shop["overview"]:
        lines.append(f"Overview: {shop['overview']}")

    lines.append(f"Product Count: {len(all_products)}")
    lines.append("")

    if include_all_names:
        names = [p["name"] for p in all_products][:40]
        lines.append("Catalog (product names): " + (", ".join(names) if names else "empty"))
        lines.append("")

    if picked:
        lines.append("Relevant products:")
        for p in picked[:8]:
            img = p.get("images") or []
            img_md = ""
            if img:
                # Provide first image as markdown so the LLM can reference it naturally.
                # The UI will render markdown ![name](url) as an actual <img>.
                img_md = f" | Image: ![{p.get('name','')}]({img[0]})"
            lines.append(
                f"- {p.get('name', '')} | Price: {p.get('price', '')} | Overview: {p.get('overview', '')}{img_md}"
            )
        lines.append("")
    return "\n".join(lines).strip()


def build_fallback_answer(shop: sqlite3.Row, picked: List[Dict[str, Any]], all_rows: List[sqlite3.Row], q: str) -> str:
    if is_greeting(q):
        return (
            f"Hi! Welcome to {shop['name']}. "
            f"We currently have {len(all_rows)} products listed. "
            f"You can ask about prices, recommendations, product details, or say 'show all products' or 'show all images'."
        )

    if is_list_products_intent(q):
        if not all_rows:
            return "This shop has no products listed yet."
        lines = ["Here are the products currently available:"]
        for row in all_rows[:25]:
            line = f"- {row['name']}"
            if row['price']:
                line += f" | {row['price']}"
            lines.append(line)
        return "\n".join(lines)

    if picked:
        top = picked[0]
        answer = f"Closest match: {top['name']}"
        if top.get("price"):
            answer += f"\nPrice: {top['price']}"
        if top.get("overview"):
            answer += f"\n{top['overview']}"
        if len(picked) > 1:
            others = ", ".join([p["name"] for p in picked[1:4]])
            answer += f"\nYou may also like: {others}"
        return answer

    names = [r["name"] for r in all_rows[:8]]
    if names:
        return (
            "I couldn't find an exact match for that, but here are some products in this shop: "
            + ", ".join(names)
            + "."
        )
    return "I couldn't find anything yet because this shop has no products listed."


def chat_suggestions(shop: sqlite3.Row) -> List[str]:
    return [
        f"What do you sell at {shop['name']}?",
        "Show all products",
        "Show all images",
        "What is your best product?",
    ]


# =========================================================
# UI + system routes
# =========================================================
@app.get("/ui", response_class=HTMLResponse)
def ui():
    path = os.path.join(SERVER_DIR, "ui.html")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/")
def root():
    return {
        "status": "Running",
        "db": DB_PATH,
        "chat_model": CHAT_MODEL,
        "shops_dir": SHOPS_DIR,
    }


@app.get("/health")
def health():
    return {"ok": True, "chat_model": CHAT_MODEL}


# =========================================================
# Auth
# =========================================================
@app.post("/auth/register", response_model=AuthResponse)
def register(body: RegisterRequest):
    email = body.email.strip().lower()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Invalid email")
    if not body.password or len(body.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")

    ph = hash_password(body.password)
    now = int(time.time())

    con = db()
    try:
        con.execute("INSERT INTO users(email, pass_hash, created_at) VALUES(?,?,?)", (email, ph, now))
        con.commit()
    except sqlite3.IntegrityError:
        con.close()
        raise HTTPException(status_code=409, detail="Email already registered")
    user = con.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    con.close()

    token = create_session(user["id"])
    return AuthResponse(token=token, email=email)


@app.post("/auth/login", response_model=AuthResponse)
def login(body: LoginRequest):
    email = body.email.strip().lower()
    con = db()
    user = con.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    con.close()
    if not user or not verify_password(body.password, user["pass_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token = create_session(user["id"])
    return AuthResponse(token=token, email=email)


@app.get("/auth/me")
def auth_me(authorization: Optional[str] = Header(None)):
    user = get_user_from_token(authorization)
    return {"ok": True, "email": user["email"], "user_id": user["id"]}


@app.post("/auth/logout")
def auth_logout(authorization: Optional[str] = Header(None)):
    token = extract_bearer_token(authorization)
    con = db()
    con.execute("DELETE FROM sessions WHERE token = ?", (token,))
    con.commit()
    con.close()
    return {"ok": True, "message": "Logged out successfully."}


# =========================================================
# Admin
# =========================================================
@app.post("/admin/create-shop", response_model=CreateShopResponse)
def create_shop(body: CreateShopRequest, authorization: Optional[str] = Header(None)):
    user = get_user_from_token(authorization)
    body.shop = validate_shop_payload(body.shop)

    sid = sanitize_shop_id(body.shop_id) if body.shop_id and body.shop_id.strip() else generate_shop_id_from_name(body.shop.name)

    now = int(time.time())
    con = db()
    existing = con.execute("SELECT 1 FROM shops WHERE shop_id = ?", (sid,)).fetchone()
    if existing:
        con.close()
        raise HTTPException(status_code=409, detail="shop_id already exists. Try another.")

    con.execute(
        """
        INSERT INTO shops(shop_id, owner_user_id, name, address, overview, phone, hours, category, created_at)
        VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (sid, user["id"], body.shop.name, body.shop.address, body.shop.overview, body.shop.phone, body.shop.hours, getattr(body.shop,"category",""), now),
    )
    con.commit()
    con.close()

    ensure_shop_folders(sid)
    rebuild_kb_for_shop(sid)
    return CreateShopResponse(shop_id=sid, message="Shop created successfully.")


@app.get("/admin/my-shops")
def my_shops(authorization: Optional[str] = Header(None)):
    user = get_user_from_token(authorization)
    con = db()
    rows = con.execute(
        "SELECT shop_id, name, address, overview, phone, hours, created_at FROM shops WHERE owner_user_id = ? ORDER BY created_at DESC",
        (user["id"],),
    ).fetchall()
    con.close()
    shops = []
    for r in rows:
        item = dict(r)
        item["stats"] = shop_stats(r["shop_id"])
        shops.append(item)
    return {"ok": True, "shops": shops}


@app.get("/admin/shop/{shop_id}")
def admin_get_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user = get_user_from_token(authorization)
    con = db()

    shop = con.execute("SELECT * FROM shops WHERE shop_id = ? AND owner_user_id = ?", (shop_id, user["id"])).fetchone()
    if not shop:
        con.close()
        raise HTTPException(status_code=404, detail="Shop not found or not yours")

    products = con.execute("SELECT * FROM products WHERE shop_id = ? ORDER BY updated_at DESC", (shop_id,)).fetchall()
    con.close()

    out_products = [serialize_product_row(p) for p in products]
    return {
        "ok": True,
        "shop_id": shop_id,
        "data": {
            "shop": {
                "name": shop["name"],
                "address": shop["address"],
                "overview": shop["overview"] or "",
                "phone": shop["phone"] or "",
                "hours": shop["hours"] or "",
            },
            "products": out_products,
            "stats": shop_stats(shop_id),
        },
    }


@app.put("/admin/shop/{shop_id}")
def admin_update_shop(shop_id: str, body: ShopInfo, authorization: Optional[str] = Header(None)):
    user = get_user_from_token(authorization)
    body = validate_shop_payload(body)
    con = db()

    shop = con.execute("SELECT 1 FROM shops WHERE shop_id = ? AND owner_user_id = ?", (shop_id, user["id"])).fetchone()
    if not shop:
        con.close()
        raise HTTPException(status_code=404, detail="Shop not found or not yours")

    con.execute(
        """
        UPDATE shops SET name=?, address=?, overview=?, phone=?, hours=?, category=?
        WHERE shop_id=? AND owner_user_id=?
        """,
        (body.name, body.address, body.overview, body.phone, body.hours, getattr(body,"category",""), shop_id, user["id"]),
    )
    con.commit()
    con.close()

    rebuild_kb_for_shop(shop_id)
    return {"ok": True, "message": "Shop updated successfully."}


@app.post("/admin/shop/{shop_id}/product")
def admin_add_or_update_product(shop_id: str, product: Product, authorization: Optional[str] = Header(None)):
    user = get_user_from_token(authorization)
    product = validate_product_payload(product)
    con = db()

    shop = con.execute("SELECT 1 FROM shops WHERE shop_id = ? AND owner_user_id = ?", (shop_id, user["id"])).fetchone()
    if not shop:
        con.close()
        raise HTTPException(status_code=404, detail="Shop not found or not yours")

    now = int(time.time())
    existing = con.execute(
        "SELECT images_json FROM products WHERE shop_id=? AND product_id=?",
        (shop_id, product.product_id),
    ).fetchone()

    incoming_images = dedupe_keep_order(product.images or [])
    if existing and not incoming_images:
        incoming_images = parse_images_json(existing["images_json"])
    images_json = json.dumps(incoming_images, ensure_ascii=False)

    if existing:
        con.execute(
            "UPDATE products SET name=?, overview=?, price=?, stock=?, variants=?, images_json=?, updated_at=? WHERE shop_id=? AND product_id=?",
            (product.name, product.overview, product.price, getattr(product,"stock","in"), getattr(product,"variants",""), images_json, now, shop_id, product.product_id),
        )
        updated = True
    else:
        con.execute(
            "INSERT INTO products(shop_id,product_id,name,overview,price,stock,variants,images_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (shop_id, product.product_id, product.name, product.overview, product.price, getattr(product,"stock","in"), getattr(product,"variants",""), images_json, now, now),
        )
        updated = False

    con.commit()
    con.close()

    rebuild_kb_for_shop(shop_id)
    return {"ok": True, "shop_id": shop_id, "product_id": product.product_id, "updated": updated}


@app.delete("/admin/shop/{shop_id}/product/{product_id}")
def admin_delete_product(shop_id: str, product_id: str, authorization: Optional[str] = Header(None)):
    user = get_user_from_token(authorization)
    con = db()

    shop = con.execute("SELECT 1 FROM shops WHERE shop_id = ? AND owner_user_id = ?", (shop_id, user["id"])).fetchone()
    if not shop:
        con.close()
        raise HTTPException(status_code=404, detail="Shop not found or not yours")

    cur = con.execute("DELETE FROM products WHERE shop_id=? AND product_id=?", (shop_id, product_id))
    con.commit()
    con.close()

    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Product not found")

    rebuild_kb_for_shop(shop_id)
    return {"ok": True, "deleted_product_id": product_id}


@app.post("/admin/shop/{shop_id}/upload-image")
def admin_upload_image(shop_id: str, file: UploadFile = File(...), authorization: Optional[str] = Header(None)):
    user = get_user_from_token(authorization)
    con = db()
    shop = con.execute("SELECT 1 FROM shops WHERE shop_id=? AND owner_user_id=?", (shop_id, user["id"])).fetchone()
    con.close()
    if not shop:
        raise HTTPException(status_code=404, detail="Shop not found or not yours")

    ext = os.path.splitext((file.filename or "").lower())[1]
    if ext not in ALLOWED_IMAGE_EXTS:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")

    ext = normalize_ext(ext)
    ensure_shop_folders(shop_id)
    safe_name = f"{uuid.uuid4().hex}{ext}"
    save_path = os.path.join(images_folder(shop_id), safe_name)
    with open(save_path, "wb") as out:
        shutil.copyfileobj(file.file, out)

    public_url = f"/shops/{shop_id}/images/{safe_name}"
    return {"ok": True, "shop_id": shop_id, "filename": safe_name, "url": public_url}


@app.post("/admin/shop/{shop_id}/product-with-images")
def admin_product_with_images(
    shop_id: str,
    authorization: Optional[str] = Header(None),
    product_id: str = Form(...),
    name: str = Form(...),
    overview: str = Form(""),
    price: str = Form(""),
    images: List[UploadFile] = File(default=[]),
):
    user = get_user_from_token(authorization)
    con = db()

    shop = con.execute("SELECT 1 FROM shops WHERE shop_id=? AND owner_user_id=?", (shop_id, user["id"])).fetchone()
    if not shop:
        con.close()
        raise HTTPException(status_code=404, detail="Shop not found or not yours")

    clean_product = validate_product_payload(
        Product(product_id=product_id, name=name, overview=overview, price=price, images=[])
    )
    new_urls = save_uploaded_images_unique(shop_id, images)

    existing = con.execute("SELECT images_json FROM products WHERE shop_id=? AND product_id=?", (shop_id, clean_product.product_id)).fetchone()
    existing_urls = parse_images_json(existing["images_json"]) if existing else []
    merged = dedupe_keep_order(existing_urls + new_urls)
    images_json = json.dumps(merged, ensure_ascii=False)

    now = int(time.time())
    exists = con.execute("SELECT 1 FROM products WHERE shop_id=? AND product_id=?", (shop_id, clean_product.product_id)).fetchone()

    if exists:
        con.execute(
            """
            UPDATE products
            SET name=?, overview=?, price=?, images_json=?, updated_at=?
            WHERE shop_id=? AND product_id=?
            """,
            (clean_product.name, clean_product.overview, clean_product.price, images_json, now, shop_id, clean_product.product_id),
        )
        updated = True
    else:
        con.execute(
            """
            INSERT INTO products(shop_id, product_id, name, overview, price, images_json, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (shop_id, clean_product.product_id, clean_product.name, clean_product.overview, clean_product.price, images_json, now, now),
        )
        updated = False

    con.commit()
    con.close()

    rebuild_kb_for_shop(shop_id)
    return {
        "ok": True,
        "shop_id": shop_id,
        "product_id": clean_product.product_id,
        "updated": updated,
        "images": merged,
    }


# =========================================================
# Public
# =========================================================
@app.get("/public/shops")
def public_shops(category: Optional[str] = None):
    con = db()
    if category:
        rows = con.execute(
            "SELECT shop_id, name, address, overview, phone, hours, category, created_at FROM shops WHERE LOWER(category)=LOWER(?) ORDER BY created_at DESC",
            (category,),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT shop_id, name, address, overview, phone, hours, category, created_at FROM shops ORDER BY created_at DESC"
        ).fetchall()
    con.close()
    shops = []
    for r in rows:
        item = dict(r)
        item["stats"] = shop_stats(r["shop_id"])
        shops.append(item)
    return {"ok": True, "shops": shops}


@app.get("/public/shop/{shop_id}")
def public_shop(shop_id: str, request: Request):
    con = db()
    shop = con.execute(
        "SELECT shop_id, name, address, overview, phone, hours, category FROM shops WHERE shop_id=?",
        (shop_id,),
    ).fetchone()
    if not shop:
        con.close()
        raise HTTPException(status_code=404, detail="Shop not found")
    products = con.execute(
        "SELECT product_id, name, overview, price, stock, variants, images_json FROM products WHERE shop_id=? ORDER BY updated_at DESC",
        (shop_id,),
    ).fetchall()
    con.close()
    track(shop_id, "shop_view")
    out_products = [serialize_product_row(p, request=request) for p in products]
    return {
        "ok": True,
        "shop_id": shop_id,
        "shop": dict(shop),
        "products": out_products,
        "stats": shop_stats(shop_id),
        "suggested_questions": [
            "What do you sell?",
            "Show all products",
            "Show all images",
            "What is your best product?",
        ],
    }


@app.get("/public/search")
def public_search(request: Request, shop_id: str = Query(...), q: str = Query(...), limit: int = 20):
    qn = (q or "").strip()
    if not qn:
        return {"ok": True, "shop_id": shop_id, "q": q, "results": []}

    like = f"%{qn}%"
    con = db()
    rows = con.execute(
        """
        SELECT product_id, name, overview, price, images_json
        FROM products
        WHERE shop_id=? AND (LOWER(name) LIKE LOWER(?) OR LOWER(overview) LIKE LOWER(?))
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (shop_id, like, like, int(limit)),
    ).fetchall()
    con.close()

    results = [serialize_product_row(r, request=request) if request else serialize_product_row(r) for r in rows]
    return {"ok": True, "shop_id": shop_id, "q": q, "results": results}



@app.get("/public/search/global")
def public_search_global(request: Request, q: str = Query(...), limit: int = 40):
    qn = (q or "").strip()
    if not qn:
        return {"ok": True, "q": q, "results": []}
    like = f"%{qn}%"
    con = db()
    rows = con.execute(
        """SELECT p.product_id, p.name, p.overview, p.price, p.stock, p.variants, p.images_json,
                  p.shop_id, s.name AS shop_name, s.address AS shop_address
           FROM products p JOIN shops s ON s.shop_id=p.shop_id
           WHERE LOWER(p.name) LIKE LOWER(?) OR LOWER(p.overview) LIKE LOWER(?)
           ORDER BY p.updated_at DESC LIMIT ?""",
        (like, like, int(limit))
    ).fetchall()
    con.close()
    results = []
    for r in rows:
        prod = serialize_product_row(r, request=request)
        prod["shop_id"] = r["shop_id"]
        prod["shop_name"] = r["shop_name"]
        prod["shop_address"] = r["shop_address"]
        results.append(prod)
    return {"ok": True, "q": q, "results": results}


@app.post("/admin/shop/{shop_id}/rebuild-kb")
def admin_rebuild_kb(shop_id: str, authorization: Optional[str] = Header(None)):
    user = get_user_from_token(authorization)
    con = db()
    if not con.execute("SELECT 1 FROM shops WHERE shop_id=? AND owner_user_id=?", (shop_id, user["id"])).fetchone():
        con.close(); raise HTTPException(status_code=404, detail="Shop not found or not yours")
    con.close()
    rebuild_kb_for_shop(shop_id)
    return {"ok": True, "message": f"Knowledge base rebuilt for {shop_id}"}


@app.delete("/admin/shop/{shop_id}")
def admin_delete_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user = get_user_from_token(authorization)
    con = db()
    if not con.execute("SELECT 1 FROM shops WHERE shop_id=? AND owner_user_id=?", (shop_id, user["id"])).fetchone():
        con.close(); raise HTTPException(status_code=404, detail="Shop not found or not yours")
    con.execute("DELETE FROM products WHERE shop_id=?", (shop_id,))
    con.execute("DELETE FROM shops WHERE shop_id=?", (shop_id,))
    con.commit(); con.close()
    folder = shop_folder(shop_id)
    if os.path.isdir(folder): shutil.rmtree(folder, ignore_errors=True)
    return {"ok": True, "deleted_shop_id": shop_id}


@app.delete("/admin/shop/{shop_id}/product/{product_id}/image")
def admin_delete_product_image(
    shop_id: str, product_id: str,
    image_path: str = Query(...),
    authorization: Optional[str] = Header(None),
):
    user = get_user_from_token(authorization)
    con = db()
    if not con.execute("SELECT 1 FROM shops WHERE shop_id=? AND owner_user_id=?", (shop_id, user["id"])).fetchone():
        con.close(); raise HTTPException(status_code=404, detail="Shop not found or not yours")
    row = con.execute("SELECT images_json FROM products WHERE shop_id=? AND product_id=?", (shop_id, product_id)).fetchone()
    if not row: con.close(); raise HTTPException(status_code=404, detail="Product not found")
    imgs = parse_images_json(row["images_json"])
    img_name = os.path.basename(image_path.rstrip("/"))
    new_imgs = [u for u in imgs if os.path.basename(u) != img_name]
    con.execute("UPDATE products SET images_json=?, updated_at=? WHERE shop_id=? AND product_id=?",
                (json.dumps(new_imgs), int(time.time()), shop_id, product_id))
    con.commit(); con.close()
    disk_path = os.path.join(images_folder(shop_id), img_name)
    if os.path.isfile(disk_path): os.remove(disk_path)
    rebuild_kb_for_shop(shop_id)
    return {"ok": True, "removed": img_name, "remaining": len(new_imgs)}


@app.get("/admin/shop/{shop_id}/analytics")
def admin_analytics(shop_id: str, days: int = 30, authorization: Optional[str] = Header(None)):
    user = get_user_from_token(authorization)
    con = db()
    if not con.execute("SELECT 1 FROM shops WHERE shop_id=? AND owner_user_id=?", (shop_id, user["id"])).fetchone():
        con.close(); raise HTTPException(status_code=404, detail="Shop not found or not yours")
    since = int(time.time()) - 86400 * max(1, min(days, 365))
    totals = con.execute("SELECT event, COUNT(*) as c FROM analytics WHERE shop_id=? AND created_at>=? GROUP BY event", (shop_id, since)).fetchall()
    top_products = con.execute(
        """SELECT a.product_id, p.name, COUNT(*) as views FROM analytics a
           LEFT JOIN products p ON p.shop_id=a.shop_id AND p.product_id=a.product_id
           WHERE a.shop_id=? AND a.event='view' AND a.created_at>=? AND a.product_id IS NOT NULL
           GROUP BY a.product_id ORDER BY views DESC LIMIT 10""", (shop_id, since)).fetchall()
    daily = []
    now = int(time.time())
    from datetime import datetime, timezone
    for d_offset in range(13, -1, -1):
        d_start = now - 86400 * (d_offset + 1)
        d_end = now - 86400 * d_offset
        c = con.execute("SELECT COUNT(*) as c FROM analytics WHERE shop_id=? AND event='chat' AND created_at>=? AND created_at<?", (shop_id, d_start, d_end)).fetchone()["c"]
        label = datetime.fromtimestamp(d_end, tz=timezone.utc).strftime("%b %d")
        daily.append({"date": label, "chats": c})
    con.close()
    return {"ok": True, "shop_id": shop_id, "days": days,
            "totals": {r["event"]: r["c"] for r in totals},
            "top_products": [dict(r) for r in top_products],
            "daily_chats": daily}

# =========================================================
# Chat
# =========================================================
@app.get("/chat")
def chat(request: Request, shop_id: str = Query(...), q: str = Query(...), debug: int = 0):
    q = (q or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="Missing q")
    track(shop_id, "chat")
    shop, products_rows = get_shop_and_products(shop_id)
    if not shop:
        return {
            "shop_id": shop_id,
            "question": q,
            "answer": "Shop not found.",
            "cached": False,
            "products": [],
            "meta": {"llm_used": False, "source": "shop_not_found", "chat_model": ""},
        }

    all_products = [
        {
            "product_id": r["product_id"],
            "name": r["name"],
            "overview": r["overview"] or "",
            "price": r["price"] or "",
            "images": parse_images_json(r["images_json"]),
        }
        for r in products_rows
    ]

    if is_greeting(q):
        greeting_cards = abs_products(request, all_products[:4])
        return {
            "shop_id": shop_id,
            "question": q,
            "answer": build_fallback_answer(shop, greeting_cards, products_rows, q),
            "cached": False,
            "products": greeting_cards,
            "meta": {
                "llm_used": False,
                "source": "greeting",
                "chat_model": "",
                "shop_name": shop["name"],
                "product_count": len(products_rows),
                "suggestions": chat_suggestions(shop),
            },
        }

    picked = rank_products_by_query(products_rows, q)
    source = "direct_match"

    if not picked:
        source = "retrieval"
        try:
            ret = retrieve(shop_folder(shop_id), q, top_k=6)
            picked = products_from_retrieval(shop_id, ret, limit=4)
        except Exception:
            picked = []

    if not picked:
        source = "db_search"
        picked = db_search(shop_id, q, limit=4)

    abs_picked = abs_products(request, picked)

    if is_list_products_intent(q):
        all_abs = abs_products(request, all_products)
        context = build_context(shop, all_products, include_all_names=True, all_products=products_rows)
        system_prompt = (
            "You are a helpful shop sales assistant. "
            "Use only the provided shop context. "
            "Be concise, friendly, and conversational. "
            "When listing products, use bullets and include price if available. "
            "If a product has an Image field, include it in your response using the exact markdown format: ![Product Name](url). "
            "Never invent products or image URLs."
        )
        user_prompt = f"SHOP CONTEXT:\n{context}\n\nCUSTOMER QUESTION:\n{q}"

        try:
            answer = ollama_chat(system_prompt, user_prompt, timeout_sec=180)
            llm_used = True
            llm_error = ""
        except Exception as e:
            llm_used = False
            llm_error = str(e)
            answer = build_fallback_answer(shop, all_abs, products_rows, q)

        return {
            "shop_id": shop_id,
            "question": q,
            "answer": answer,
            "cached": False,
            "products": all_abs,
            "meta": {
                "llm_used": llm_used,
                "source": "list_products",
                "llm_error": llm_error,
                "chat_model": CHAT_MODEL if llm_used else "",
                "shop_name": shop["name"],
                "product_count": len(products_rows),
                "suggestions": chat_suggestions(shop),
            },
        }

    if wants_all_images(q):
        gallery = collect_gallery_urls(shop_id, request, limit=40)
        if not gallery:
            answer = "This shop doesn’t have any images uploaded yet."
        else:
            lines = [f"Sure — here are all images from {shop['name']} ({len(gallery)}):", ""]
            for i, url in enumerate(gallery, 1):
                lines.append(f"![Image {i}]({url})")
            answer = "\n".join(lines)
        return {
            "shop_id": shop_id,
            "question": q,
            "answer": answer,
            "cached": False,
            "products": abs_picked,
            "meta": {
                "llm_used": False,
                "source": "shop_gallery",
                "chat_model": "",
                "shop_name": shop["name"],
                "product_count": len(products_rows),
                "suggestions": chat_suggestions(shop),
            },
        }

    if wants_image(q) and abs_picked:
        p = abs_picked[0]
        imgs = p.get("images") or []
        if imgs:
            answer = f"Sure — here it is:\n\n![{p.get('name') or 'Product'}]({imgs[0]})"
            return {
                "shop_id": shop_id,
                "question": q,
                "answer": answer,
                "cached": False,
                "products": abs_picked,
                "meta": {
                    "llm_used": False,
                    "source": "image_direct",
                    "chat_model": "",
                    "shop_name": shop["name"],
                    "product_count": len(products_rows),
                    "suggestions": chat_suggestions(shop),
                },
            }

    context = build_context(shop, picked, include_all_names=False, all_products=products_rows)
    system_prompt = (
        "You are a world-class sales assistant for a local shop.\n"
        "Rules:\n"
        "- Use ONLY the provided shop context and product info.\n"
        "- Be natural, conversational, and helpful.\n"
        "- Keep answers practical and easy to read.\n"
        "- If price is available and relevant, mention it.\n"
        "- If a product has an Image field in the context, show it using markdown: ![Product Name](url). "
        "The UI will render it as a real photo.\n"
        "- If you are unsure, say you could not find an exact match.\n"
        "- Never invent products, URLs, or policies.\n"
    )
    user_prompt = f"SHOP CONTEXT:\n{context}\n\nCUSTOMER QUESTION:\n{q}"

    try:
        answer = ollama_chat(system_prompt, user_prompt, timeout_sec=180)
        llm_used = True
        llm_error = ""
    except Exception as e:
        llm_used = False
        llm_error = str(e)
        answer = build_fallback_answer(shop, abs_picked, products_rows, q)

    payload = {
        "shop_id": shop_id,
        "question": q,
        "answer": answer,
        "cached": False,
        "products": abs_picked,
        "meta": {
            "llm_used": llm_used,
            "source": source,
            "llm_error": llm_error,
            "chat_model": CHAT_MODEL if llm_used else "",
            "shop_name": shop["name"],
            "product_count": len(products_rows),
            "suggestions": chat_suggestions(shop),
        },
    }
    if debug:
        payload["debug"] = {"source": source, "picked_count": len(abs_picked)}
    return payload
