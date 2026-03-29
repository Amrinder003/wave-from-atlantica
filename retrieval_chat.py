# retrieval_chat.py
import os
import json
import re
import math
import requests
from typing import Dict, Any, List, Tuple

OLLAMA_EMBED_URL = os.environ.get("OLLAMA_EMBED_URL", "http://127.0.0.1:11434/api/embeddings")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "nomic-embed-text")


def normalize(text: str) -> str:
    text = (text or "").lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", normalize(text))


def lexical_overlap_score(question: str, text: str) -> float:
    q_tokens = set(tokenize(question))
    t_tokens = set(tokenize(text))
    if not q_tokens or not t_tokens:
        return 0.0
    overlap = q_tokens & t_tokens
    score = len(overlap) / max(len(q_tokens), 1)
    qn = normalize(question)
    tn = normalize(text)
    if qn and qn in tn:
        score += 0.35
    return score


def is_greeting(q: str) -> bool:
    qn = normalize(q)
    return qn in {"hi", "hello", "hey", "hii", "hlo", "good morning", "good evening"}


def guess_query_type(q: str) -> str:
    """
    We only use this to slightly bias retrieval.
    Not hardcoding products — just recognizing question intent.
    """
    qn = normalize(q)

    # asking what the shop sells / list products
    if any(p in qn for p in [
        "what do you sell",
        "what kind of products",
        "what products",
        "what items",
        "show products",
        "list products",
        "catalog",
        "menu",
        "inventory",
        "what do you have"
    ]):
        return "shop"

    if any(k in qn for k in ["address", "located", "location", "hours", "phone", "open", "close", "timing"]):
        return "shop"

    if any(k in qn for k in ["price", "cost", "rate", "rs", "₹", "dollar", "cad", "usd"]):
        return "product"

    # default: user probably means product search
    return "product"


def resolve_shop_folder(shop_folder: str) -> str:
    if os.path.isdir(shop_folder):
        return shop_folder

    base_dir = os.path.dirname(os.path.abspath(__file__))
    shops_dir = os.path.join(base_dir, "shops")

    candidate1 = os.path.join(base_dir, shop_folder)  # "shops/abc123"
    if os.path.isdir(candidate1):
        return candidate1

    return os.path.join(shops_dir, shop_folder)  # "abc123"


def ollama_embed(text: str) -> List[float]:
    text = (text or "").strip()
    if not text:
        return []
    payload = {"model": EMBED_MODEL, "prompt": text}
    r = requests.post(OLLAMA_EMBED_URL, json=payload, timeout=30)
    r.raise_for_status()
    return r.json().get("embedding", []) or []


def cosine(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))


def load_embedding_rows(shop_folder: str) -> List[Dict[str, Any]]:
    """
    Requires: kb_embeddings.jsonl created by build_kb.py
    """
    p_embed = os.path.join(shop_folder, "kb_embeddings.jsonl")
    if not os.path.exists(p_embed):
        raise FileNotFoundError(
            f"Missing kb_embeddings.jsonl in: {shop_folder}\n"
            f"Run: python build_kb.py {shop_folder}"
        )

    rows: List[Dict[str, Any]] = []
    with open(p_embed, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def retrieve(shop_folder: str, question: str, top_k: int = 5) -> Dict[str, Any]:
    """
    Semantic retrieval using embeddings (cosine similarity).
    Returns stable dict.
    """
    folder = resolve_shop_folder(shop_folder)
    rows = load_embedding_rows(folder)

    qtype = guess_query_type(question)
    q_emb = ollama_embed(question)

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for r in rows:
        ctype = (r.get("type") or "").lower().strip()
        emb = r.get("embedding", []) or []
        text = r.get("text", "") or ""

        semantic = cosine(q_emb, emb) if emb else 0.0
        lexical = lexical_overlap_score(question, text)
        s = semantic * 0.72 + lexical * 0.28

        # bias only a little
        if qtype == "product" and ctype == "product":
            s *= 1.08
        if qtype == "product" and ctype == "shop":
            s *= 0.92
        if qtype == "shop" and ctype == "shop":
            s *= 1.08
        if qtype == "shop" and ctype == "product":
            s *= 0.95

        scored.append((s, {"type": ctype, "text": text, "semantic": float(semantic), "lexical": float(lexical)}))

    scored.sort(key=lambda x: x[0], reverse=True)

    # Embedding threshold: keep moderate so “head phone” still finds earbuds
    threshold = 0.22

    kept = [(s, c) for (s, c) in scored[:max(top_k, 1)] if s >= threshold]

    best_score = kept[0][0] if kept else 0.0
    chunks = [c["text"] for (s, c) in kept]
    matches = [{"score": float(s), "type": c["type"], "text": c["text"], "semantic": c["semantic"], "lexical": c["lexical"]} for (s, c) in kept]

    return {
        "best_score": float(best_score),
        "threshold": float(threshold),
        "chunks": chunks,
        "matches": matches,
        "embed_model": EMBED_MODEL,
        "query_type": qtype,
        "query_is_greeting": is_greeting(question),
    }
