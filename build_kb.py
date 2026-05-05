# build_kb.py
import os
import sys
import json
import requests
from typing import Any, Dict, List

OLLAMA_EMBED_URL = os.environ.get("OLLAMA_EMBED_URL", "http://127.0.0.1:11434/api/embeddings")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "nomic-embed-text")


def ollama_embed(text: str) -> List[float]:
    """
    Get embedding vector from Ollama (local).
    """
    text = (text or "").strip()
    if not text:
        return []
    payload = {"model": EMBED_MODEL, "prompt": text}
    r = requests.post(OLLAMA_EMBED_URL, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    # Ollama returns: {"embedding": [..]}
    return data.get("embedding", []) or []


def chunk_shop(shop_obj: dict, shop_id: str) -> List[Dict[str, Any]]:
    """
    Turns a shop.json into text chunks for retrieval.

    Supports BOTH formats:
    New format:
      {"shop": {...}, "products":[...]}
    Old format:
      {"shop_id":"abc123","shop_name":"...","address":"...","products":[...]}
    """
    chunks = []

    # --- NEW FORMAT ---
    if "shop" in shop_obj:
        shop = shop_obj.get("shop", {}) or {}
        products = shop_obj.get("products", []) or []

        shop_name = shop.get("name", "")
        address = shop.get("address", "")
        overview = shop.get("overview", "")
        website = shop.get("website", "")
        phone = shop.get("phone", "")
        hours = shop.get("hours", "")
        category = shop.get("category", "")
        whatsapp = shop.get("whatsapp", "")

        shop_text = (
            f"Shop ID: {shop_id}\n"
            f"Shop Name: {shop_name}\n"
            f"Category: {category}\n"
            f"Address: {address}\n"
            f"Overview: {overview}\n"
            f"Website: {website}\n"
            f"Phone: {phone}\n"
            f"Hours: {hours}\n"
            f"WhatsApp: {whatsapp}\n"
        ).strip()

        chunks.append({"type": "shop", "text": shop_text})

        for p in products:
            pid = p.get("product_id", "")
            name = p.get("name", "")
            pov = p.get("overview", "")
            price = p.get("price", "")
            stock = p.get("stock", "")
            variants = p.get("variants", "")
            variant_data = p.get("variant_data", [])
            variant_matrix = p.get("variant_matrix", [])
            attribute_data = p.get("attribute_data", {})
            images = p.get("images", [])
            if isinstance(images, list):
                images_str = ", ".join(images)
            else:
                images_str = str(images)

            prod_text = (
                f"Shop ID: {shop_id}\n"
                f"Product ID: {pid}\n"
                f"Product: {name}\n"
                f"Overview: {pov}\n"
                f"Price: {price}\n"
                f"Stock: {stock}\n"
                f"Variants: {variants}\n"
                f"Variant Data: {json.dumps(variant_data, ensure_ascii=False)}\n"
                f"Variant Matrix: {json.dumps(variant_matrix, ensure_ascii=False)}\n"
                f"Attributes: {json.dumps(attribute_data, ensure_ascii=False)}\n"
                f"Shop Category: {category}\n"
                f"Images: {images_str}\n"
            ).strip()

            chunks.append({"type": "product", "text": prod_text})

        return chunks

    # --- OLD FORMAT (fallback) ---
    old_shop_id = shop_obj.get("shop_id", shop_id)
    shop_name = shop_obj.get("shop_name", shop_obj.get("name", ""))
    address = shop_obj.get("address", "")
    overview = shop_obj.get("overview", "")
    website = shop_obj.get("website", "")
    phone = shop_obj.get("phone", "")
    hours = shop_obj.get("hours", "")
    category = shop_obj.get("category", "")
    whatsapp = shop_obj.get("whatsapp", "")
    products = shop_obj.get("products", []) or []

    shop_text = (
        f"Shop ID: {old_shop_id}\n"
        f"Shop Name: {shop_name}\n"
        f"Category: {category}\n"
        f"Address: {address}\n"
        f"Overview: {overview}\n"
        f"Website: {website}\n"
        f"Phone: {phone}\n"
        f"Hours: {hours}\n"
        f"WhatsApp: {whatsapp}\n"
    ).strip()
    chunks.append({"type": "shop", "text": shop_text})

    for p in products:
        pid = p.get("product_id", "")
        name = p.get("name", "")
        pov = p.get("overview", "")
        price = p.get("price", "")
        stock = p.get("stock", "")
        variants = p.get("variants", "")
        variant_data = p.get("variant_data", [])
        variant_matrix = p.get("variant_matrix", [])
        attribute_data = p.get("attribute_data", {})
        images = p.get("images", [])
        if isinstance(images, list):
            images_str = ", ".join(images)
        else:
            images_str = str(images)

        prod_text = (
            f"Shop ID: {old_shop_id}\n"
            f"Product ID: {pid}\n"
            f"Product: {name}\n"
            f"Overview: {pov}\n"
            f"Price: {price}\n"
            f"Stock: {stock}\n"
            f"Variants: {variants}\n"
            f"Variant Data: {json.dumps(variant_data, ensure_ascii=False)}\n"
            f"Variant Matrix: {json.dumps(variant_matrix, ensure_ascii=False)}\n"
            f"Attributes: {json.dumps(attribute_data, ensure_ascii=False)}\n"
            f"Shop Category: {category}\n"
            f"Images: {images_str}\n"
        ).strip()
        chunks.append({"type": "product", "text": prod_text})

    return chunks


def build_kb(shop_folder: str):
    """
    Reads shops/<shop_id>/shop.json and writes:
      - kb.jsonl          (plain chunks)
      - kb_embeddings.jsonl (chunks + embedding vectors)
    """
    shop_folder = os.path.abspath(shop_folder)
    shop_id = os.path.basename(shop_folder)

    shop_json = os.path.join(shop_folder, "shop.json")
    if not os.path.exists(shop_json):
        raise FileNotFoundError(f"Missing shop.json: {shop_json}")

    with open(shop_json, "r", encoding="utf-8") as f:
        shop_obj = json.load(f)

    chunks = chunk_shop(shop_obj, shop_id)

    # 1) Plain chunks
    out_chunks_path = os.path.join(shop_folder, "kb.jsonl")
    with open(out_chunks_path, "w", encoding="utf-8") as f:
        for ch in chunks:
            f.write(json.dumps(ch, ensure_ascii=False) + "\n")

    # 2) Embeddings
    out_embed_path = os.path.join(shop_folder, "kb_embeddings.jsonl")
    with open(out_embed_path, "w", encoding="utf-8") as f:
        for ch in chunks:
            text = ch.get("text", "")
            emb = ollama_embed(text)
            row = {
                "type": ch.get("type", ""),
                "text": text,
                "embedding": emb,
                "embed_model": EMBED_MODEL,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"[OK] Wrote {len(chunks)} chunks to {out_chunks_path}")
    print(f"[OK] Wrote {len(chunks)} embeddings to {out_embed_path} (model={EMBED_MODEL})")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python build_kb.py shops/<shop_id>")
        sys.exit(1)
    build_kb(sys.argv[1])
