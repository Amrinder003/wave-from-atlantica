"""
LocalMarket API  v4.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Environment variables (create a .env file or set in Railway/Render):

  # ── LLM ──────────────────────────────────────────────────────────
  OPENROUTER_API_KEY=sk-or-...          # Get free key at openrouter.ai
  OPENROUTER_MODEL=meta-llama/llama-3.3-70b-instruct:free   # or any free model
  # If OPENROUTER_API_KEY is not set, falls back to local Ollama:
  OLLAMA_CHAT_URL=http://127.0.0.1:11434/api/chat
  CHAT_MODEL=llama3.2:3b

  # ── EMAIL VERIFICATION ───────────────────────────────────────────
  SMTP_HOST=smtp.gmail.com
  SMTP_PORT=587
  SMTP_USER=your@gmail.com
  SMTP_PASS=your_app_password        # Gmail: 16-char app password
  EMAIL_FROM=LocalMarket <your@gmail.com>
  APP_BASE_URL=https://your-app.railway.app   # used for verify links

  # ── SUPABASE (optional — leave blank to use SQLite) ─────────────
  SUPABASE_URL=https://xxx.supabase.co
  SUPABASE_SERVICE_KEY=eyJ...
  USE_SUPABASE_STORAGE=false         # set true to store images in Supabase

  # ── SECURITY ─────────────────────────────────────────────────────
  SECRET_KEY=change-me-to-a-random-64-char-string
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from fastapi import FastAPI, Query, Header, HTTPException, UploadFile, File, Request, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import os, re, json, time, shutil, sqlite3, secrets, hashlib, uuid, mimetypes
import requests, smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Any, Dict, List, Optional, Tuple

# ── optional: load .env if python-dotenv is installed ──
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── optional: RAG modules (not required, graceful fallback) ──
try:
    from retrieval_chat import retrieve as _retrieve
    from build_kb import build_kb as _build_kb
    HAS_RAG = True
except ImportError:
    HAS_RAG = False
    def _retrieve(*a, **k): return {"matches": []}
    def _build_kb(*a, **k): pass

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SERVER_DIR        = os.path.dirname(os.path.abspath(__file__))
SHOPS_DIR         = os.path.join(SERVER_DIR, "shops")
os.makedirs(SHOPS_DIR, exist_ok=True)

DB_PATH           = os.path.join(SERVER_DIR, "app.db")
TOKEN_TTL         = 60 * 60 * 24 * 30   # 30 days
VERIFY_TTL        = 60 * 60 * 24        # 24 hours for email verification
ALLOWED_IMG_EXTS  = {".png", ".jpg", ".jpeg", ".webp", ".jfif", ".jif"}
PAGE_SIZE         = 24                   # products per page

# LLM — OpenRouter first, Ollama fallback
OPENROUTER_KEY    = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL  = os.environ.get("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free")
OPENROUTER_URL    = "https://openrouter.ai/api/v1/chat/completions"
OLLAMA_URL        = os.environ.get("OLLAMA_CHAT_URL", "http://127.0.0.1:11434/api/chat")
OLLAMA_MODEL      = os.environ.get("CHAT_MODEL", "llama3.2:3b")

# Email
SMTP_HOST         = os.environ.get("SMTP_HOST", "")
SMTP_PORT         = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER         = os.environ.get("SMTP_USER", "")
SMTP_PASS         = os.environ.get("SMTP_PASS", "")
EMAIL_FROM        = os.environ.get("EMAIL_FROM", "LocalMarket <noreply@localmarket.app>")
APP_BASE_URL      = os.environ.get("APP_BASE_URL", "http://localhost:8001")

SECRET_KEY        = os.environ.get("SECRET_KEY", secrets.token_hex(32))

mimetypes.add_type("image/jpeg", ".jfif")
mimetypes.add_type("image/jpeg", ".jif")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# APP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
app = FastAPI(title="LocalMarket API", version="4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/shops", StaticFiles(directory=SHOPS_DIR), name="shops")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATABASE — SQLite with Supabase Postgres migration path
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    return con


def init_db():
    con = db()
    c = con.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS users (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        email        TEXT UNIQUE NOT NULL,
        pass_hash    TEXT NOT NULL,
        display_name TEXT DEFAULT '',
        avatar_url   TEXT DEFAULT '',
        role         TEXT DEFAULT 'customer',   -- 'customer' | 'shopkeeper' | 'admin'
        email_verified INTEGER DEFAULT 0,
        created_at   INTEGER NOT NULL
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS email_verifications (
        token      TEXT PRIMARY KEY,
        user_id    INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        used       INTEGER DEFAULT 0,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS password_resets (
        token      TEXT PRIMARY KEY,
        user_id    INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        used       INTEGER DEFAULT 0,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS sessions (
        token      TEXT PRIMARY KEY,
        user_id    INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS shops (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_id        TEXT UNIQUE NOT NULL,
        owner_user_id  INTEGER NOT NULL,
        name           TEXT NOT NULL,
        address        TEXT NOT NULL,
        overview       TEXT DEFAULT '',
        phone          TEXT DEFAULT '',
        hours          TEXT DEFAULT '',
        category       TEXT DEFAULT '',
        whatsapp       TEXT DEFAULT '',
        created_at     INTEGER NOT NULL,
        FOREIGN KEY(owner_user_id) REFERENCES users(id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS products (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_id      TEXT NOT NULL,
        product_id   TEXT NOT NULL,
        name         TEXT NOT NULL,
        overview     TEXT DEFAULT '',
        price        TEXT DEFAULT '',
        stock        TEXT DEFAULT 'in',
        variants     TEXT DEFAULT '',
        images_json  TEXT DEFAULT '[]',
        created_at   INTEGER NOT NULL,
        updated_at   INTEGER NOT NULL,
        UNIQUE(shop_id, product_id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS reviews (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_id     TEXT NOT NULL,
        product_id  TEXT NOT NULL,
        user_id     INTEGER NOT NULL,
        rating      INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
        body        TEXT NOT NULL,
        created_at  INTEGER NOT NULL,
        UNIQUE(shop_id, product_id, user_id),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS favourites (
        user_id    INTEGER NOT NULL,
        shop_id    TEXT NOT NULL,
        product_id TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY(user_id, shop_id, product_id),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS analytics (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        shop_id    TEXT NOT NULL,
        product_id TEXT,
        event      TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )""")

    # Safe migrations for existing databases
    for stmt in [
        "ALTER TABLE users ADD COLUMN display_name TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN avatar_url TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'customer'",
        "ALTER TABLE users ADD COLUMN email_verified INTEGER DEFAULT 0",
        "ALTER TABLE shops ADD COLUMN category TEXT DEFAULT ''",
        "ALTER TABLE shops ADD COLUMN whatsapp TEXT DEFAULT ''",
        "ALTER TABLE products ADD COLUMN stock TEXT DEFAULT 'in'",
        "ALTER TABLE products ADD COLUMN variants TEXT DEFAULT ''",
    ]:
        try: c.execute(stmt); con.commit()
        except Exception: pass

    c.execute("CREATE INDEX IF NOT EXISTS idx_prod_shop ON products(shop_id, updated_at DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_rev_prod ON reviews(shop_id, product_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_fav_user ON favourites(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_an_shop ON analytics(shop_id, created_at)")
    con.commit()
    con.close()


init_db()

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

class VerifyEmailReq(BaseModel):
    token: str

class ForgotPasswordReq(BaseModel):
    email: str

class ResetPasswordReq(BaseModel):
    token: str
    new_password: str

class UpdateProfileReq(BaseModel):
    display_name: str = ""

class AuthResp(BaseModel):
    ok: bool = True
    token: str
    email: str
    display_name: str = ""
    email_verified: bool = False
    role: str = "customer"

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
def pbkdf2(pw: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", pw.encode(), salt, 260_000)

def hash_pw(pw: str) -> str:
    s = secrets.token_bytes(16)
    return f"{s.hex()}:{pbkdf2(pw, s).hex()}"

def verify_pw(pw: str, stored: str) -> bool:
    try:
        sh, hh = stored.split(":")
        return secrets.compare_digest(pbkdf2(pw, bytes.fromhex(sh)), bytes.fromhex(hh))
    except Exception:
        return False

def new_token() -> str:
    return secrets.token_urlsafe(40)

def create_session(user_id: int) -> str:
    token = new_token()
    now = int(time.time())
    con = db()
    con.execute("DELETE FROM sessions WHERE expires_at <= ?", (now,))
    con.execute("INSERT INTO sessions(token,user_id,expires_at,created_at) VALUES(?,?,?,?)",
                (token, user_id, now + TOKEN_TTL, now))
    con.commit(); con.close()
    return token

def bearer(auth: Optional[str]) -> str:
    if not auth: raise HTTPException(401, "Missing Authorization header")
    m = re.match(r"Bearer\s+(.+)", auth.strip(), re.I)
    if not m: raise HTTPException(401, "Use: Bearer <token>")
    return m.group(1).strip()

def get_user(auth: Optional[str]) -> sqlite3.Row:
    token = bearer(auth)
    now = int(time.time())
    con = db()
    row = con.execute(
        "SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token=? AND s.expires_at>?",
        (token, now)
    ).fetchone()
    con.close()
    if not row: raise HTTPException(401, "Session expired or invalid")
    return row

def require_verified(user: sqlite3.Row):
    if not user["email_verified"]:
        raise HTTPException(403, "Email not verified. Check your inbox.")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# EMAIL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def send_email(to: str, subject: str, html_body: str):
    """Send email via SMTP. Silently skips if SMTP not configured."""
    if not SMTP_HOST or not SMTP_USER:
        print(f"[EMAIL SKIP] To={to} Subject={subject}")
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = to
        msg.attach(MIMEText(html_body, "html"))
        ctx = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.ehlo(); s.starttls(context=ctx); s.ehlo()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, to, msg.as_string())
    except Exception as e:
        print(f"[EMAIL ERROR] {e}")

def send_verification_email(email: str, user_id: int, bg: BackgroundTasks):
    token = new_token()
    exp = int(time.time()) + VERIFY_TTL
    con = db()
    con.execute("INSERT OR REPLACE INTO email_verifications(token,user_id,expires_at) VALUES(?,?,?)",
                (token, user_id, exp))
    con.commit(); con.close()

    link = f"{APP_BASE_URL}/auth/verify-email?token={token}"
    html = f"""
    <div style="font-family:sans-serif;max-width:480px;margin:auto;padding:32px">
      <h2 style="color:#c94f14">Verify your LocalMarket email</h2>
      <p>Thanks for signing up! Click the button below to verify your email address.</p>
      <a href="{link}" style="display:inline-block;margin:20px 0;padding:12px 24px;
         background:#c94f14;color:#fff;border-radius:8px;text-decoration:none;font-weight:700">
        Verify Email
      </a>
      <p style="color:#888;font-size:12px">Link expires in 24 hours. If you didn't sign up, ignore this.</p>
    </div>"""
    bg.add_task(send_email, email, "Verify your LocalMarket account", html)

def send_reset_email(email: str, user_id: int, bg: BackgroundTasks):
    token = new_token()
    exp = int(time.time()) + 3600  # 1 hour
    con = db()
    con.execute("INSERT OR REPLACE INTO password_resets(token,user_id,expires_at) VALUES(?,?,?)",
                (token, user_id, exp))
    con.commit(); con.close()

    link = f"{APP_BASE_URL.rstrip('/')}/reset-password?token={token}"
    html = f"""
    <div style="font-family:sans-serif;max-width:480px;margin:auto;padding:32px">
      <h2 style="color:#c94f14">Reset your password</h2>
      <p>We received a request to reset your LocalMarket password.</p>
      <a href="{link}" style="display:inline-block;margin:20px 0;padding:12px 24px;
         background:#c94f14;color:#fff;border-radius:8px;text-decoration:none;font-weight:700">
        Reset Password
      </a>
      <p style="color:#888;font-size:12px">Link expires in 1 hour. If you didn't request this, ignore it.</p>
    </div>"""
    bg.add_task(send_email, email, "Reset your LocalMarket password", html)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LLM — OpenRouter + Ollama fallback
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def llm_chat(system: str, user: str, max_tokens: int = 400) -> str:
    """Try OpenRouter first; fall back to Ollama if no key or error."""
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]

    if OPENROUTER_KEY:
        try:
            r = requests.post(
                OPENROUTER_URL,
                headers={
                    "Authorization": f"Bearer {OPENROUTER_KEY}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": APP_BASE_URL,
                    "X-Title": "LocalMarket",
                },
                json={
                    "model": OPENROUTER_MODEL,
                    "messages": messages,
                    "max_tokens": max_tokens,
                    "temperature": 0.2,
                },
                timeout=60,
            )
            r.raise_for_status()
            data = r.json()
            content = data["choices"][0]["message"]["content"]
            return (content or "").strip()
        except Exception as e:
            print(f"[OpenRouter error] {e} — falling back to Ollama")

    # Ollama fallback
    try:
        r = requests.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "messages": messages, "stream": False,
                  "options": {"temperature": 0.2, "num_predict": max_tokens}},
            timeout=120,
        )
        r.raise_for_status()
        return ((r.json().get("message") or {}).get("content") or "").strip()
    except Exception as e:
        raise HTTPException(503, f"LLM unavailable: {e}")

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
    return f"{base}-{secrets.token_hex(2)}" if base else f"shop-{secrets.token_hex(3)}"

def shop_dir(shop_id: str) -> str: return os.path.join(SHOPS_DIR, shop_id)
def img_dir(shop_id: str) -> str:  return os.path.join(shop_dir(shop_id), "images")
def ensure_dirs(shop_id: str):
    os.makedirs(shop_dir(shop_id), exist_ok=True)
    os.makedirs(img_dir(shop_id), exist_ok=True)

def abs_url(req: Request, path: str) -> str:
    if not path: return ""
    if path.startswith("http"): return path
    p = path if path.startswith("/") else "/" + path
    return str(req.base_url).rstrip("/") + p

def dedup(items: List[str]) -> List[str]:
    out, seen = [], set()
    for x in items or []:
        v = str(x or "").strip()
        if v and v not in seen: seen.add(v); out.append(v)
    return out

def parse_imgs(s: str) -> List[str]:
    try:
        imgs = json.loads(s or "[]")
        if isinstance(imgs, list): return dedup([str(x) for x in imgs if str(x).strip()])
    except Exception: pass
    return []

def norm_ext(ext: str) -> str:
    return ".jpg" if ext.lower() in {".jfif", ".jif"} else ext.lower()

def save_images(shop_id: str, files: List[UploadFile]) -> List[str]:
    ensure_dirs(shop_id)
    out = []
    for f in files or []:
        if not f or not getattr(f, "filename", None): continue
        ext = os.path.splitext(f.filename.lower())[1]
        if ext not in ALLOWED_IMG_EXTS:
            raise HTTPException(400, f"Unsupported image type: {ext}")
        ext = norm_ext(ext)
        name = f"{uuid.uuid4().hex}{ext}"
        with open(os.path.join(img_dir(shop_id), name), "wb") as o:
            shutil.copyfileobj(f.file, o)
        out.append(f"/shops/{shop_id}/images/{name}")
    return dedup(out)

def serialize_product(row: sqlite3.Row, req: Optional[Request] = None, user_id: Optional[int] = None) -> Dict[str, Any]:
    imgs = parse_imgs(row["images_json"])
    if req: imgs = [abs_url(req, u) for u in imgs]
    cols = row.keys() if hasattr(row, "keys") else []

    # avg rating
    con = db()
    rv = con.execute(
        "SELECT AVG(rating) as avg, COUNT(*) as n FROM reviews WHERE shop_id=? AND product_id=?",
        (row["shop_id"], row["product_id"])
    ).fetchone()
    is_fav = False
    if user_id:
        is_fav = bool(con.execute(
            "SELECT 1 FROM favourites WHERE user_id=? AND shop_id=? AND product_id=?",
            (user_id, row["shop_id"], row["product_id"])
        ).fetchone())
    con.close()

    return {
        "product_id": row["product_id"],
        "shop_id":    row["shop_id"],
        "name":       row["name"],
        "overview":   row["overview"] or "",
        "price":      row["price"] or "",
        "stock":      row["stock"] if "stock" in cols else "in",
        "variants":   row["variants"] if "variants" in cols else "",
        "images":     imgs,
        "image_count": len(imgs),
        "avg_rating": round(rv["avg"] or 0, 1),
        "review_count": rv["n"] or 0,
        "is_favourite": is_fav,
    }

def shop_stats(shop_id: str) -> Dict:
    con = db()
    rows = con.execute("SELECT images_json FROM products WHERE shop_id=?", (shop_id,)).fetchall()
    n = len(rows); imgs = 0; with_imgs = 0
    for r in rows:
        i = parse_imgs(r["images_json"]); imgs += len(i)
        if i: with_imgs += 1
    since30 = int(time.time()) - 86400 * 30
    chats = con.execute("SELECT COUNT(*) as c FROM analytics WHERE shop_id=? AND event='chat' AND created_at>=?",
                        (shop_id, since30)).fetchone()["c"]
    views = con.execute("SELECT COUNT(*) as c FROM analytics WHERE shop_id=? AND event='view' AND created_at>=?",
                        (shop_id, since30)).fetchone()["c"]
    avg_r = con.execute(
        "SELECT AVG(r.rating) as avg FROM reviews r JOIN products p ON p.shop_id=r.shop_id AND p.product_id=r.product_id WHERE r.shop_id=?",
        (shop_id,)
    ).fetchone()["avg"]
    con.close()
    return {"product_count": n, "products_with_images": with_imgs, "image_count": imgs,
            "chat_hits_30d": chats, "product_views_30d": views,
            "avg_rating": round(avg_r or 0, 1)}

def track(shop_id: str, event: str, product_id: Optional[str] = None):
    try:
        con = db()
        con.execute("INSERT INTO analytics(shop_id,product_id,event,created_at) VALUES(?,?,?,?)",
                    (shop_id, product_id, event, int(time.time())))
        con.commit(); con.close()
    except Exception: pass

def write_shop_json(shop_id: str):
    con = db()
    shop = con.execute("SELECT * FROM shops WHERE shop_id=?", (shop_id,)).fetchone()
    if not shop: con.close(); return
    prods = con.execute("SELECT * FROM products WHERE shop_id=? ORDER BY updated_at DESC", (shop_id,)).fetchall()
    con.close()
    obj = {
        "shop": {k: shop[k] for k in ("name","address","overview","phone","hours","category")},
        "products": [serialize_product(p) for p in prods],
    }
    ensure_dirs(shop_id)
    with open(os.path.join(shop_dir(shop_id), "shop.json"), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def rebuild_kb(shop_id: str):
    write_shop_json(shop_id)
    if HAS_RAG:
        try: _build_kb(shop_dir(shop_id))
        except Exception as e: print(f"[KB] {e}")

# ── Chat helpers ──
def wants_image(q: str) -> bool:
    return any(w in norm_text(q) for w in ["image","photo","pic","picture","show","dikhao"])

def wants_all_images(q: str) -> bool:
    qn = norm_text(q)
    if "gallery" in qn: return True
    if "all" not in qn and "every" not in qn: return False
    return any(v in qn for v in ["image","images","photo","photos","picture","pictures"])

def is_greeting(q: str) -> bool:
    return norm_text(q) in {"hi","hello","hey","hii","hlo","good morning","good evening","good afternoon"}

def is_list_intent(q: str) -> bool:
    qn = norm_text(q)
    return any(t in qn for t in [
        "what do you sell","what products","what items","what do you have",
        "show products","product list","catalog","catalogue","menu","inventory",
        "show all products","list products","all products",
    ])

def rank_products(products: List[sqlite3.Row], q: str) -> List[Dict]:
    qn = norm_text(q)
    qt = set(re.findall(r"[a-z0-9]+", qn))
    scored = []
    for r in products:
        name = (r["name"] or "").strip()
        ov   = (r["overview"] or "").strip()
        hay  = norm_text(f"{name} {ov}")
        ht   = set(re.findall(r"[a-z0-9]+", hay))
        s = 0.0
        if norm_text(name) and norm_text(name) in qn: s += 8
        if qt & ht: s += len(qt & ht) * 1.4
        if qn and qn in hay: s += 5
        if s <= 0: continue
        scored.append((s, {
            "product_id": r["product_id"], "name": r["name"],
            "overview": r["overview"] or "", "price": r["price"] or "",
            "stock": r["stock"] if "stock" in r.keys() else "in",
            "images": parse_imgs(r["images_json"]),
        }))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored]

def db_search_products(shop_id: str, q: str, limit: int = 6) -> List[Dict]:
    like = f"%{q.strip()}%"
    con = db()
    rows = con.execute(
        "SELECT product_id,name,overview,price,stock,variants,images_json FROM products "
        "WHERE shop_id=? AND (LOWER(name) LIKE LOWER(?) OR LOWER(overview) LIKE LOWER(?)) "
        "ORDER BY updated_at DESC LIMIT ?",
        (shop_id, like, like, limit)
    ).fetchall()
    con.close()
    return [{"product_id":r["product_id"],"name":r["name"],"overview":r["overview"] or "",
             "price":r["price"] or "","stock":r["stock"] if "stock" in r.keys() else "in",
             "images":parse_imgs(r["images_json"])} for r in rows]

def abs_products_list(req: Request, products: List[Dict]) -> List[Dict]:
    out = []
    for p in products:
        imgs = [abs_url(req, u) for u in (p.get("images") or [])]
        out.append({**p, "images": dedup(imgs), "image_count": len(dedup(imgs))})
    return out

def build_context(shop: sqlite3.Row, picked: List[Dict], include_all: bool, all_rows: List) -> str:
    lines = [f"Shop: {shop['name']}", f"Address: {shop['address']}"]
    if shop["phone"]: lines.append(f"Phone: {shop['phone']}")
    if shop["hours"]: lines.append(f"Hours: {shop['hours']}")
    if shop["overview"]: lines.append(f"About: {shop['overview']}")
    lines.append(f"Total products: {len(all_rows)}")
    if include_all:
        lines.append("All products: " + ", ".join(p["name"] for p in all_rows[:40]))
    if picked:
        lines.append("\nMatching products:")
        for p in picked[:6]:
            img_line = ""
            imgs = p.get("images") or []
            if imgs:
                # Use markdown image — UI renders it as a real photo
                img_line = f' | Photo: ![{p["name"]}]({imgs[0]})'
            lines.append(
                f'• {p["name"]} | Price: {p.get("price","N/A")} | Stock: {p.get("stock","in")} | {p.get("overview","")}{img_line}'
            )
    return "\n".join(lines)

SHOP_ASSISTANT_SYSTEM = """\
You are a friendly, knowledgeable sales assistant for a local shop.
Rules:
- Use ONLY information from the shop context provided. Never invent products, prices, or policies.
- Be conversational, warm, and concise.
- When a product has a Photo field with a markdown image like ![name](url), include it exactly as-is in your response — the app will render it as an actual photo.
- Mention price and stock status when relevant.
- If something is out of stock, say so clearly.
- If you can't find a match, say so and suggest browsing the full catalogue.
- Keep responses under 200 words unless listing all products.
"""

def fallback_answer(shop: sqlite3.Row, picked: List[Dict], all_rows: List, q: str) -> str:
    if is_greeting(q):
        return (f"Hi! Welcome to **{shop['name']}**! We have {len(all_rows)} products. "
                "Ask me about anything, or say 'show all products'.")
    if is_list_intent(q) and all_rows:
        lines = [f"Here's what's available at **{shop['name']}**:\n"]
        for r in all_rows[:30]:
            line = f"• {r['name']}"
            if r["price"]: line += f" — {r['price']}"
            stock = r["stock"] if "stock" in r.keys() else "in"
            if stock == "out": line += " *(out of stock)*"
            elif stock == "low": line += " *(low stock)*"
            lines.append(line)
        return "\n".join(lines)
    if picked:
        top = picked[0]
        s = f"**{top['name']}**"
        if top.get("price"): s += f" — {top['price']}"
        if top.get("overview"): s += f"\n{top['overview']}"
        imgs = top.get("images") or []
        if imgs: s += f"\n![{top['name']}]({imgs[0]})"
        if len(picked) > 1:
            s += "\n\nYou may also like: " + ", ".join(p["name"] for p in picked[1:4])
        return s
    return "I couldn't find a match. Try asking 'show all products' to see everything we have."

def collect_gallery(shop_id: str, req: Request, limit: int = 40) -> List[str]:
    con = db()
    rows = con.execute("SELECT images_json FROM products WHERE shop_id=? ORDER BY updated_at DESC", (shop_id,)).fetchall()
    con.close()
    seen, out = set(), []
    for r in rows:
        for u in parse_imgs(r["images_json"]):
            au = abs_url(req, u)
            if au and au not in seen: seen.add(au); out.append(au)
            if len(out) >= limit: return out
    return out

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — System
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/ui", response_class=HTMLResponse)
def serve_ui():
    path = os.path.join(SERVER_DIR, "ui.html")
    with open(path, encoding="utf-8") as f: return f.read()

@app.get("/")
def root():
    llm = f"OpenRouter ({OPENROUTER_MODEL})" if OPENROUTER_KEY else f"Ollama ({OLLAMA_MODEL})"
    return {"status": "running", "version": "4.0", "llm": llm, "rag": HAS_RAG}

@app.get("/health")
def health():
    return {"ok": True, "llm": "openrouter" if OPENROUTER_KEY else "ollama",
            "model": OPENROUTER_MODEL if OPENROUTER_KEY else OLLAMA_MODEL}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Auth (customer + shopkeeper, unified)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.post("/auth/register")
def register(body: RegisterReq, bg: BackgroundTasks):
    email = body.email.strip().lower()
    if not email or "@" not in email:
        raise HTTPException(400, "Invalid email address")
    if not body.password or len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")

    con = db()
    try:
        con.execute(
            "INSERT INTO users(email,pass_hash,display_name,role,email_verified,created_at) VALUES(?,?,?,?,?,?)",
            (email, hash_pw(body.password), body.display_name.strip(), "customer", 0, int(time.time()))
        )
        con.commit()
    except sqlite3.IntegrityError:
        con.close(); raise HTTPException(409, "Email already registered")
    user = con.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    con.close()

    send_verification_email(email, user["id"], bg)
    token = create_session(user["id"])
    return {"ok": True, "token": token, "email": email,
            "display_name": user["display_name"], "email_verified": False,
            "role": "customer", "message": "Account created! Check your email to verify."}


@app.post("/auth/login")
def login(body: LoginReq):
    email = body.email.strip().lower()
    con = db()
    user = con.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    con.close()
    if not user or not verify_pw(body.password, user["pass_hash"]):
        raise HTTPException(401, "Invalid email or password")
    token = create_session(user["id"])
    return {"ok": True, "token": token, "email": email,
            "display_name": user["display_name"] or "",
            "email_verified": bool(user["email_verified"]),
            "role": user["role"]}


@app.get("/auth/me")
def auth_me(authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    shops = con.execute("SELECT shop_id, name FROM shops WHERE owner_user_id=?", (user["id"],)).fetchall()
    fav_count = con.execute("SELECT COUNT(*) as c FROM favourites WHERE user_id=?", (user["id"],)).fetchone()["c"]
    rev_count = con.execute("SELECT COUNT(*) as c FROM reviews WHERE user_id=?", (user["id"],)).fetchone()["c"]
    con.close()
    return {
        "ok": True, "user_id": user["id"], "email": user["email"],
        "display_name": user["display_name"] or "",
        "avatar_url": user["avatar_url"] or "",
        "email_verified": bool(user["email_verified"]),
        "role": user["role"],
        "my_shops": [dict(s) for s in shops],
        "fav_count": fav_count,
        "review_count": rev_count,
    }


@app.post("/auth/logout")
def logout(authorization: Optional[str] = Header(None)):
    token = bearer(authorization)
    con = db()
    con.execute("DELETE FROM sessions WHERE token=?", (token,))
    con.commit(); con.close()
    return {"ok": True}


@app.get("/auth/verify-email")
def verify_email_link(token: str, request: Request):
    """Handles the link clicked in the verification email."""
    con = db()
    row = con.execute(
        "SELECT * FROM email_verifications WHERE token=? AND used=0 AND expires_at>?",
        (token, int(time.time()))
    ).fetchone()
    if not row:
        con.close()
        return HTMLResponse("<h2>Link expired or already used. Please request a new verification email.</h2>", 400)
    con.execute("UPDATE users SET email_verified=1 WHERE id=?", (row["user_id"],))
    con.execute("UPDATE email_verifications SET used=1 WHERE token=?", (token,))
    con.commit(); con.close()
    # Redirect to UI
    return HTMLResponse(f"""
    <html><head><meta http-equiv="refresh" content="3;url={APP_BASE_URL}/ui"></head>
    <body style="font-family:sans-serif;text-align:center;padding:60px">
      <h2 style="color:#1a6e42">✅ Email verified!</h2>
      <p>Redirecting you back to LocalMarket…</p>
    </body></html>""")


@app.post("/auth/resend-verification")
def resend_verification(authorization: Optional[str] = Header(None), bg: BackgroundTasks = None):
    user = get_user(authorization)
    if user["email_verified"]: return {"ok": True, "message": "Already verified"}
    send_verification_email(user["email"], user["id"], bg)
    return {"ok": True, "message": "Verification email sent"}


@app.post("/auth/forgot-password")
def forgot_password(body: ForgotPasswordReq, bg: BackgroundTasks):
    email = body.email.strip().lower()
    con = db()
    user = con.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    con.close()
    if user: send_reset_email(email, user["id"], bg)
    # Always return ok — don't reveal if email exists
    return {"ok": True, "message": "If that email exists, a reset link has been sent."}


@app.post("/auth/reset-password")
def reset_password(body: ResetPasswordReq):
    if len(body.new_password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    con = db()
    row = con.execute(
        "SELECT * FROM password_resets WHERE token=? AND used=0 AND expires_at>?",
        (body.token, int(time.time()))
    ).fetchone()
    if not row: con.close(); raise HTTPException(400, "Invalid or expired reset link")
    con.execute("UPDATE users SET pass_hash=? WHERE id=?", (hash_pw(body.new_password), row["user_id"]))
    con.execute("UPDATE password_resets SET used=1 WHERE token=?", (body.token,))
    con.commit(); con.close()
    return {"ok": True, "message": "Password updated. You can now log in."}


@app.put("/auth/profile")
def update_profile(body: UpdateProfileReq, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    con.execute("UPDATE users SET display_name=? WHERE id=?", (body.display_name.strip(), user["id"]))
    con.commit(); con.close()
    return {"ok": True}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Customer: Favourites
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.post("/customer/favourite/{shop_id}/{product_id}")
def toggle_favourite(shop_id: str, product_id: str, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    require_verified(user)
    con = db()
    exists = con.execute("SELECT 1 FROM favourites WHERE user_id=? AND shop_id=? AND product_id=?",
                         (user["id"], shop_id, product_id)).fetchone()
    if exists:
        con.execute("DELETE FROM favourites WHERE user_id=? AND shop_id=? AND product_id=?",
                    (user["id"], shop_id, product_id))
        saved = False
    else:
        con.execute("INSERT INTO favourites(user_id,shop_id,product_id,created_at) VALUES(?,?,?,?)",
                    (user["id"], shop_id, product_id, int(time.time())))
        saved = True
    con.commit(); con.close()
    return {"ok": True, "saved": saved}


@app.get("/customer/favourites")
def get_favourites(request: Request, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    rows = con.execute(
        """SELECT p.*, p.shop_id as shop_id FROM favourites f
           JOIN products p ON p.shop_id=f.shop_id AND p.product_id=f.product_id
           WHERE f.user_id=? ORDER BY f.created_at DESC""",
        (user["id"],)
    ).fetchall()
    con.close()
    return {"ok": True, "favourites": [serialize_product(r, request, user["id"]) for r in rows]}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Reviews
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/public/reviews/{shop_id}/{product_id}")
def get_reviews(shop_id: str, product_id: str):
    con = db()
    rows = con.execute(
        """SELECT r.*, u.display_name, u.email FROM reviews r
           JOIN users u ON u.id=r.user_id
           WHERE r.shop_id=? AND r.product_id=? ORDER BY r.created_at DESC""",
        (shop_id, product_id)
    ).fetchall()
    con.close()
    return {"ok": True, "reviews": [{
        "id": r["id"], "rating": r["rating"], "body": r["body"],
        "author": r["display_name"] or r["email"].split("@")[0],
        "created_at": r["created_at"]
    } for r in rows]}


@app.post("/customer/review/{shop_id}/{product_id}")
def post_review(shop_id: str, product_id: str, body: ReviewReq,
                authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    require_verified(user)
    if not body.body.strip(): raise HTTPException(400, "Review cannot be empty")
    con = db()
    # Check product exists
    if not con.execute("SELECT 1 FROM products WHERE shop_id=? AND product_id=?",
                       (shop_id, product_id)).fetchone():
        con.close(); raise HTTPException(404, "Product not found")
    try:
        con.execute(
            "INSERT INTO reviews(shop_id,product_id,user_id,rating,body,created_at) VALUES(?,?,?,?,?,?)",
            (shop_id, product_id, user["id"], body.rating, body.body.strip(), int(time.time()))
        )
        con.commit()
    except sqlite3.IntegrityError:
        # Update existing review
        con.execute("UPDATE reviews SET rating=?, body=?, created_at=? WHERE shop_id=? AND product_id=? AND user_id=?",
                    (body.rating, body.body.strip(), int(time.time()), shop_id, product_id, user["id"]))
        con.commit()
    con.close()
    return {"ok": True, "message": "Review saved"}


@app.delete("/customer/review/{shop_id}/{product_id}")
def delete_review(shop_id: str, product_id: str, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    con.execute("DELETE FROM reviews WHERE shop_id=? AND product_id=? AND user_id=?",
                (shop_id, product_id, user["id"]))
    con.commit(); con.close()
    return {"ok": True}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Public shop / product browsing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@app.get("/public/shops")
def public_shops(category: Optional[str] = None):
    con = db()
    if category:
        rows = con.execute(
            "SELECT shop_id,name,address,overview,phone,hours,category,whatsapp,created_at FROM shops WHERE LOWER(category)=LOWER(?) ORDER BY created_at DESC",
            (category,)
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT shop_id,name,address,overview,phone,hours,category,whatsapp,created_at FROM shops ORDER BY created_at DESC"
        ).fetchall()
    con.close()
    shops = []
    for r in rows:
        item = dict(r); item["stats"] = shop_stats(r["shop_id"]); shops.append(item)
    return {"ok": True, "shops": shops}


@app.get("/public/shop/{shop_id}")
def public_shop(shop_id: str, request: Request,
                page: int = Query(1, ge=1),
                sort: str = Query("default"),
                stock: str = Query(""),
                authorization: Optional[str] = Header(None)):
    con = db()
    shop = con.execute(
        "SELECT shop_id,name,address,overview,phone,hours,category,whatsapp FROM shops WHERE shop_id=?",
        (shop_id,)
    ).fetchone()
    if not shop: con.close(); raise HTTPException(404, "Shop not found")

    # Get user_id if logged in
    user_id = None
    if authorization:
        try: user_id = get_user(authorization)["id"]
        except Exception: pass

    # Build product query
    q = "SELECT * FROM products WHERE shop_id=?"
    params: List = [shop_id]
    if stock in ("in", "low", "out"):
        q += " AND stock=?"; params.append(stock)
    if sort == "price-asc":   q += " ORDER BY CAST(REPLACE(REPLACE(price,'$',''),'Rs','') AS REAL) ASC"
    elif sort == "price-desc": q += " ORDER BY CAST(REPLACE(REPLACE(price,'$',''),'Rs','') AS REAL) DESC"
    elif sort == "rating":     q += " ORDER BY updated_at DESC"  # approximate
    else:                      q += " ORDER BY updated_at DESC"

    all_prods = con.execute(q, params).fetchall()
    total = len(all_prods)
    offset = (page - 1) * PAGE_SIZE
    page_prods = all_prods[offset:offset + PAGE_SIZE]
    con.close()

    track(shop_id, "shop_view")
    return {
        "ok": True, "shop_id": shop_id, "shop": dict(shop),
        "products": [serialize_product(p, request, user_id) for p in page_prods],
        "pagination": {
            "page": page, "page_size": PAGE_SIZE,
            "total": total, "pages": max(1, -(-total // PAGE_SIZE))
        },
        "stats": shop_stats(shop_id),
        "suggested_questions": [
            "What products do you have?", "Show all products",
            "What's in stock?", "Show me your best product", "Show all images",
        ],
    }


@app.get("/public/search")
def search_shop(request: Request, shop_id: str = Query(...), q: str = Query(...),
                page: int = Query(1, ge=1), limit: int = Query(24, le=100)):
    qn = (q or "").strip()
    if not qn: return {"ok": True, "shop_id": shop_id, "q": q, "results": [], "total": 0}
    like = f"%{qn}%"
    con = db()
    rows = con.execute(
        "SELECT * FROM products WHERE shop_id=? AND (LOWER(name) LIKE LOWER(?) OR LOWER(overview) LIKE LOWER(?))"
        " ORDER BY updated_at DESC",
        (shop_id, like, like)
    ).fetchall()
    con.close()
    total = len(rows)
    offset = (page - 1) * limit
    return {
        "ok": True, "shop_id": shop_id, "q": q,
        "results": [serialize_product(r, request) for r in rows[offset:offset + limit]],
        "total": total,
        "pagination": {"page": page, "page_size": limit, "total": total, "pages": max(1,-(-total//limit))}
    }


@app.get("/public/search/global")
def search_global(request: Request, q: str = Query(...),
                  page: int = Query(1, ge=1), limit: int = Query(24, le=60)):
    qn = (q or "").strip()
    if not qn: return {"ok": True, "q": q, "results": [], "total": 0}
    like = f"%{qn}%"
    con = db()
    rows = con.execute(
        """SELECT p.*, s.name AS shop_name, s.address AS shop_address, s.whatsapp AS shop_whatsapp
           FROM products p JOIN shops s ON s.shop_id=p.shop_id
           WHERE LOWER(p.name) LIKE LOWER(?) OR LOWER(p.overview) LIKE LOWER(?)
           ORDER BY p.updated_at DESC""",
        (like, like)
    ).fetchall()
    con.close()
    total = len(rows)
    offset = (page - 1) * limit
    results = []
    for r in rows[offset:offset + limit]:
        prod = serialize_product(r, request)
        prod["shop_name"]    = r["shop_name"]
        prod["shop_address"] = r["shop_address"]
        prod["shop_whatsapp"]= r["shop_whatsapp"] or ""
        results.append(prod)
    return {
        "ok": True, "q": q, "results": results, "total": total,
        "pagination": {"page": page, "page_size": limit, "total": total, "pages": max(1,-(-total//limit))}
    }


@app.get("/public/top-products")
def top_products(request: Request, limit: int = Query(12, le=40), category: str = Query("")):
    """Top-rated products across all shops — shown on home page."""
    con = db()
    if category:
        rows = con.execute(
            """SELECT p.*, s.name AS shop_name,
                      AVG(r.rating) as avg_r, COUNT(r.id) as rev_n
               FROM products p
               JOIN shops s ON s.shop_id=p.shop_id
               LEFT JOIN reviews r ON r.shop_id=p.shop_id AND r.product_id=p.product_id
               WHERE LOWER(s.category)=LOWER(?) AND p.stock != 'out'
               GROUP BY p.shop_id, p.product_id
               ORDER BY avg_r DESC, rev_n DESC, p.updated_at DESC LIMIT ?""",
            (category, limit)
        ).fetchall()
    else:
        rows = con.execute(
            """SELECT p.*, s.name AS shop_name,
                      AVG(r.rating) as avg_r, COUNT(r.id) as rev_n
               FROM products p
               JOIN shops s ON s.shop_id=p.shop_id
               LEFT JOIN reviews r ON r.shop_id=p.shop_id AND r.product_id=p.product_id
               WHERE p.stock != 'out'
               GROUP BY p.shop_id, p.product_id
               ORDER BY avg_r DESC, rev_n DESC, p.updated_at DESC LIMIT ?""",
            (limit,)
        ).fetchall()
    con.close()
    results = []
    for r in rows:
        prod = serialize_product(r, request)
        prod["shop_name"] = r["shop_name"]
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

    con = db()
    shop = con.execute("SELECT * FROM shops WHERE shop_id=?", (shop_id,)).fetchone()
    prod_rows = con.execute(
        "SELECT product_id,name,overview,price,stock,variants,images_json FROM products WHERE shop_id=? ORDER BY updated_at DESC",
        (shop_id,)
    ).fetchall() if shop else []
    con.close()

    if not shop:
        return {"answer": "Shop not found.", "products": [], "meta": {"llm_used": False}}

    all_prods = [{"product_id":r["product_id"],"name":r["name"],"overview":r["overview"] or "",
                  "price":r["price"] or "","stock":r["stock"] if "stock" in r.keys() else "in",
                  "images":parse_imgs(r["images_json"])} for r in prod_rows]

    if is_greeting(q):
        cards = abs_products_list(request, all_prods[:4])
        return {"answer": fallback_answer(shop, cards, prod_rows, q),
                "products": cards, "meta": {"llm_used": False, "source": "greeting",
                "suggestions": ["What products do you have?","Show all products","Show me your best product"]}}

    # Retrieve matching products
    picked = rank_products(prod_rows, q)
    source = "keyword"
    if not picked and HAS_RAG:
        try:
            ret = _retrieve(shop_dir(shop_id), q, top_k=6)
            pids = []
            for m in (ret.get("matches") or []):
                found = re.search(r"Product ID:\s*([A-Za-z0-9\-_]+)", m.get("text",""))
                if found: pids.append(found.group(1))
            if pids:
                con2 = db()
                picked = []
                for pid in dedup(pids)[:4]:
                    row = con2.execute("SELECT product_id,name,overview,price,stock,variants,images_json FROM products WHERE shop_id=? AND product_id=?", (shop_id, pid)).fetchone()
                    if row: picked.append({"product_id":row["product_id"],"name":row["name"],"overview":row["overview"] or "","price":row["price"] or "","stock":row["stock"] if "stock" in row.keys() else "in","images":parse_imgs(row["images_json"])})
                con2.close()
                source = "rag"
        except Exception: pass
    if not picked:
        picked = db_search_products(shop_id, q, 4); source = "db"

    abs_picked = abs_products_list(request, picked)

    if wants_all_images(q):
        gallery = collect_gallery(shop_id, request)
        if not gallery:
            answer = "This shop hasn't uploaded any product photos yet."
        else:
            parts = [f"Here are all photos from **{shop['name']}** ({len(gallery)} images):\n"]
            for i, url in enumerate(gallery, 1):
                parts.append(f"![Image {i}]({url})")
            answer = "\n".join(parts)
        return {"answer": answer, "products": abs_picked, "meta": {"llm_used": False, "source": "gallery"}}

    if is_list_intent(q):
        ctx = build_context(shop, all_prods, True, prod_rows)
        try:
            answer = llm_chat(SHOP_ASSISTANT_SYSTEM, f"SHOP CONTEXT:\n{ctx}\n\nCUSTOMER:\n{q}", 500)
            llm_used = True
        except Exception:
            answer = fallback_answer(shop, abs_products_list(request, all_prods), prod_rows, q)
            llm_used = False
        return {"answer": answer, "products": abs_products_list(request, all_prods[:8]),
                "meta": {"llm_used": llm_used, "source": "list"}}

    ctx = build_context(shop, picked, False, prod_rows)
    try:
        answer = llm_chat(SHOP_ASSISTANT_SYSTEM, f"SHOP CONTEXT:\n{ctx}\n\nCUSTOMER:\n{q}", 350)
        llm_used = True
    except Exception:
        answer = fallback_answer(shop, abs_picked, prod_rows, q)
        llm_used = False

    return {
        "answer": answer, "products": abs_picked,
        "meta": {"llm_used": llm_used, "source": source,
                 "model": OPENROUTER_MODEL if (llm_used and OPENROUTER_KEY) else OLLAMA_MODEL,
                 "suggestions": [f"Tell me more about {p['name']}" for p in abs_picked[:2]]
                                 + ["Show all products", "What's in stock?"]}
    }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTES — Admin (shopkeeper)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def owns_shop(user: sqlite3.Row, shop_id: str, con: sqlite3.Connection) -> bool:
    return bool(con.execute("SELECT 1 FROM shops WHERE shop_id=? AND owner_user_id=?",
                            (shop_id, user["id"])).fetchone())

@app.post("/admin/create-shop")
def create_shop(body: CreateShopReq, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    require_verified(user)
    shop = body.shop
    shop.name = shop.name.strip(); shop.address = shop.address.strip()
    if not shop.name: raise HTTPException(400, "Shop name required")
    if not shop.address: raise HTTPException(400, "Shop address required")
    sid = slug(body.shop_id) if (body.shop_id and body.shop_id.strip()) else gen_shop_id(shop.name)
    now = int(time.time())
    con = db()
    if con.execute("SELECT 1 FROM shops WHERE shop_id=?", (sid,)).fetchone():
        con.close(); raise HTTPException(409, "shop_id already exists")
    con.execute(
        "INSERT INTO shops(shop_id,owner_user_id,name,address,overview,phone,hours,category,whatsapp,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (sid, user["id"], shop.name, shop.address, shop.overview, shop.phone, shop.hours, shop.category, shop.whatsapp, now)
    )
    # Upgrade role to shopkeeper
    con.execute("UPDATE users SET role='shopkeeper' WHERE id=? AND role='customer'", (user["id"],))
    con.commit(); con.close()
    ensure_dirs(sid); rebuild_kb(sid)
    return {"ok": True, "shop_id": sid, "message": "Shop created"}


@app.get("/admin/my-shops")
def my_shops(authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    rows = con.execute(
        "SELECT shop_id,name,address,overview,phone,hours,category,whatsapp,created_at FROM shops WHERE owner_user_id=? ORDER BY created_at DESC",
        (user["id"],)
    ).fetchall()
    con.close()
    return {"ok": True, "shops": [{**dict(r), "stats": shop_stats(r["shop_id"])} for r in rows]}


@app.get("/admin/shop/{shop_id}")
def admin_get_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    if not owns_shop(user, shop_id, con): con.close(); raise HTTPException(404, "Not found or not yours")
    shop = con.execute("SELECT * FROM shops WHERE shop_id=?", (shop_id,)).fetchone()
    prods = con.execute("SELECT * FROM products WHERE shop_id=? ORDER BY updated_at DESC", (shop_id,)).fetchall()
    con.close()
    return {"ok": True, "shop_id": shop_id, "data": {
        "shop": {k: shop[k] for k in ("name","address","overview","phone","hours","category","whatsapp")},
        "products": [serialize_product(p) for p in prods],
        "stats": shop_stats(shop_id),
    }}


@app.put("/admin/shop/{shop_id}")
def admin_update_shop(shop_id: str, body: ShopInfo, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    if not owns_shop(user, shop_id, con): con.close(); raise HTTPException(404, "Not found or not yours")
    con.execute(
        "UPDATE shops SET name=?,address=?,overview=?,phone=?,hours=?,category=?,whatsapp=? WHERE shop_id=?",
        (body.name.strip(), body.address.strip(), body.overview, body.phone, body.hours, body.category, body.whatsapp, shop_id)
    )
    con.commit(); con.close()
    rebuild_kb(shop_id)
    return {"ok": True}


@app.delete("/admin/shop/{shop_id}")
def admin_delete_shop(shop_id: str, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    if not owns_shop(user, shop_id, con): con.close(); raise HTTPException(404, "Not found or not yours")
    con.execute("DELETE FROM products WHERE shop_id=?", (shop_id,))
    con.execute("DELETE FROM reviews WHERE shop_id=?", (shop_id,))
    con.execute("DELETE FROM favourites WHERE shop_id=?", (shop_id,))
    con.execute("DELETE FROM analytics WHERE shop_id=?", (shop_id,))
    con.execute("DELETE FROM shops WHERE shop_id=?", (shop_id,))
    con.commit(); con.close()
    d = shop_dir(shop_id)
    if os.path.isdir(d): shutil.rmtree(d, ignore_errors=True)
    return {"ok": True}


@app.post("/admin/shop/{shop_id}/product")
def admin_upsert_product(shop_id: str, product: Product, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    product.product_id = slug(product.product_id, 60)
    product.name = product.name.strip()
    if not product.name: raise HTTPException(400, "Product name required")
    con = db()
    if not owns_shop(user, shop_id, con): con.close(); raise HTTPException(404, "Not found or not yours")
    now = int(time.time())
    existing = con.execute("SELECT images_json FROM products WHERE shop_id=? AND product_id=?",
                           (shop_id, product.product_id)).fetchone()
    imgs = dedup(product.images or [])
    if existing and not imgs: imgs = parse_imgs(existing["images_json"])
    imgs_json = json.dumps(imgs)
    if existing:
        con.execute("UPDATE products SET name=?,overview=?,price=?,stock=?,variants=?,images_json=?,updated_at=? WHERE shop_id=? AND product_id=?",
                    (product.name, product.overview, product.price, product.stock, product.variants, imgs_json, now, shop_id, product.product_id))
    else:
        con.execute("INSERT INTO products(shop_id,product_id,name,overview,price,stock,variants,images_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (shop_id, product.product_id, product.name, product.overview, product.price, product.stock, product.variants, imgs_json, now, now))
    con.commit(); con.close()
    rebuild_kb(shop_id)
    return {"ok": True, "product_id": product.product_id}


@app.delete("/admin/shop/{shop_id}/product/{product_id}")
def admin_delete_product(shop_id: str, product_id: str, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    if not owns_shop(user, shop_id, con): con.close(); raise HTTPException(404, "Not found or not yours")
    cur = con.execute("DELETE FROM products WHERE shop_id=? AND product_id=?", (shop_id, product_id))
    con.commit(); con.close()
    if cur.rowcount == 0: raise HTTPException(404, "Product not found")
    rebuild_kb(shop_id)
    return {"ok": True}


@app.post("/admin/shop/{shop_id}/product-with-images")
def admin_product_with_images(
    shop_id: str,
    authorization: Optional[str] = Header(None),
    product_id: str = Form(...), name: str = Form(...),
    overview: str = Form(""), price: str = Form(""),
    stock: str = Form("in"), variants: str = Form(""),
    images: List[UploadFile] = File(default=[])
):
    user = get_user(authorization)
    pid = slug(product_id, 60); name = name.strip()
    if not name: raise HTTPException(400, "Name required")
    con = db()
    if not owns_shop(user, shop_id, con): con.close(); raise HTTPException(404, "Not found or not yours")
    new_urls = save_images(shop_id, images)
    existing = con.execute("SELECT images_json FROM products WHERE shop_id=? AND product_id=?",
                           (shop_id, pid)).fetchone()
    merged = dedup((parse_imgs(existing["images_json"]) if existing else []) + new_urls)
    now = int(time.time())
    if existing:
        con.execute("UPDATE products SET name=?,overview=?,price=?,stock=?,variants=?,images_json=?,updated_at=? WHERE shop_id=? AND product_id=?",
                    (name, overview, price, stock, variants, json.dumps(merged), now, shop_id, pid))
    else:
        con.execute("INSERT INTO products(shop_id,product_id,name,overview,price,stock,variants,images_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (shop_id, pid, name, overview, price, stock, variants, json.dumps(merged), now, now))
    con.commit(); con.close()
    rebuild_kb(shop_id)
    return {"ok": True, "product_id": pid, "images": merged}


@app.delete("/admin/shop/{shop_id}/product/{product_id}/image")
def admin_delete_image(shop_id: str, product_id: str,
                       image_path: str = Query(...),
                       authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    if not owns_shop(user, shop_id, con): con.close(); raise HTTPException(404, "Not found or not yours")
    row = con.execute("SELECT images_json FROM products WHERE shop_id=? AND product_id=?",
                      (shop_id, product_id)).fetchone()
    if not row: con.close(); raise HTTPException(404, "Product not found")
    img_name = os.path.basename(image_path.rstrip("/"))
    new_imgs = [u for u in parse_imgs(row["images_json"]) if os.path.basename(u) != img_name]
    con.execute("UPDATE products SET images_json=?,updated_at=? WHERE shop_id=? AND product_id=?",
                (json.dumps(new_imgs), int(time.time()), shop_id, product_id))
    con.commit(); con.close()
    disk = os.path.join(img_dir(shop_id), img_name)
    if os.path.isfile(disk): os.remove(disk)
    rebuild_kb(shop_id)
    return {"ok": True, "remaining": len(new_imgs)}


@app.post("/admin/shop/{shop_id}/rebuild-kb")
def admin_rebuild_kb(shop_id: str, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    if not owns_shop(user, shop_id, con): con.close(); raise HTTPException(404, "Not found or not yours")
    con.close(); rebuild_kb(shop_id)
    return {"ok": True}


@app.get("/admin/shop/{shop_id}/analytics")
def admin_analytics(shop_id: str, days: int = 30, authorization: Optional[str] = Header(None)):
    user = get_user(authorization)
    con = db()
    if not owns_shop(user, shop_id, con): con.close(); raise HTTPException(404, "Not found or not yours")
    since = int(time.time()) - 86400 * max(1, min(days, 365))
    totals = {r["event"]: r["c"] for r in
              con.execute("SELECT event, COUNT(*) as c FROM analytics WHERE shop_id=? AND created_at>=? GROUP BY event",
                          (shop_id, since)).fetchall()}
    top = con.execute(
        """SELECT a.product_id, p.name, COUNT(*) as views FROM analytics a
           LEFT JOIN products p ON p.shop_id=a.shop_id AND p.product_id=a.product_id
           WHERE a.shop_id=? AND a.event='view' AND a.created_at>=? AND a.product_id IS NOT NULL
           GROUP BY a.product_id ORDER BY views DESC LIMIT 10""",
        (shop_id, since)
    ).fetchall()
    now = int(time.time())
    from datetime import datetime, timezone
    daily = []
    for d in range(13, -1, -1):
        s = now - 86400 * (d + 1); e = now - 86400 * d
        c = con.execute("SELECT COUNT(*) as c FROM analytics WHERE shop_id=? AND event='chat' AND created_at>=? AND created_at<?",
                        (shop_id, s, e)).fetchone()["c"]
        daily.append({"date": datetime.fromtimestamp(e, tz=timezone.utc).strftime("%b %d"), "chats": c})
    con.close()
    return {"ok": True, "shop_id": shop_id, "days": days, "totals": totals,
            "top_products": [dict(r) for r in top], "daily_chats": daily}
