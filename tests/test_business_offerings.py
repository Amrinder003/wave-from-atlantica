import copy
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

if "supabase" not in sys.modules:
    supabase_module = types.ModuleType("supabase")

    class _Client:
        pass

    class _ClientOptions:
        def __init__(self, *args, **kwargs):
            pass

    def _create_client(*args, **kwargs):
        return None

    supabase_module.Client = _Client
    supabase_module.ClientOptions = _ClientOptions
    supabase_module.create_client = _create_client

    supabase_client_module = types.ModuleType("supabase.client")
    supabase_client_module.ClientOptions = _ClientOptions

    supabase_lib_module = types.ModuleType("supabase.lib")
    supabase_lib_client_options_module = types.ModuleType("supabase.lib.client_options")
    supabase_lib_client_options_module.ClientOptions = _ClientOptions

    sys.modules["supabase"] = supabase_module
    sys.modules["supabase.client"] = supabase_client_module
    sys.modules["supabase.lib"] = supabase_lib_module
    sys.modules["supabase.lib.client_options"] = supabase_lib_client_options_module

import server


class FakeStorageBucket:
    def __init__(self, storage, bucket_name):
        self.storage = storage
        self.bucket_name = bucket_name

    def upload(self, path, data, options=None):
        payload = data.read() if hasattr(data, "read") else data
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        self.storage.objects.setdefault(self.bucket_name, {})[path] = {
            "data": bytes(payload or b""),
            "options": copy.deepcopy(options or {}),
        }
        return {"path": path}

    def get_public_url(self, path):
        return f"https://storage.example/{self.bucket_name}/{path}"

    def remove(self, paths):
        for path in paths or []:
            self.storage.removed.append((self.bucket_name, path))
            self.storage.objects.setdefault(self.bucket_name, {}).pop(path, None)
        return {"paths": list(paths or [])}


class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.removed = []

    def from_(self, bucket_name):
        return FakeStorageBucket(self, bucket_name)


class FakeQuery:
    def __init__(self, db, table_name):
        self.db = db
        self.table_name = table_name
        self.mode = "select"
        self.fields = "*"
        self.filters = []
        self.order_field = None
        self.order_desc = False
        self.limit_n = None
        self.payload = None

    def select(self, fields="*"):
        self.mode = "select"
        self.fields = fields
        return self

    def eq(self, key, value):
        self.filters.append(lambda row, key=key, value=value: self.db.field_value(self.table_name, row, key) == value)
        return self

    def neq(self, key, value):
        self.filters.append(lambda row, key=key, value=value: self.db.field_value(self.table_name, row, key) != value)
        return self

    def in_(self, key, values):
        allowed = {str(v) for v in values}
        self.filters.append(lambda row, key=key, allowed=allowed: str(self.db.field_value(self.table_name, row, key)) in allowed)
        return self

    def ilike(self, key, pattern):
        needle = str(pattern or "").replace("%", "").lower()
        self.filters.append(lambda row, key=key, needle=needle: needle in str(self.db.field_value(self.table_name, row, key) or "").lower())
        return self

    def gte(self, key, value):
        self.filters.append(lambda row, key=key, value=value: str(self.db.field_value(self.table_name, row, key) or "") >= str(value))
        return self

    def lt(self, key, value):
        self.filters.append(lambda row, key=key, value=value: str(self.db.field_value(self.table_name, row, key) or "") < str(value))
        return self

    def order(self, key, desc=False):
        self.order_field = key
        self.order_desc = bool(desc)
        return self

    def limit(self, limit_n):
        self.limit_n = int(limit_n)
        return self

    def insert(self, payload):
        self.mode = "insert"
        self.payload = copy.deepcopy(payload)
        return self

    def update(self, payload):
        self.mode = "update"
        self.payload = copy.deepcopy(payload)
        return self

    def delete(self):
        self.mode = "delete"
        self.payload = None
        return self

    def _matching_rows(self):
        rows = self.db.tables.setdefault(self.table_name, [])
        matched = [row for row in rows if all(check(row) for check in self.filters)]
        if self.order_field:
            matched = sorted(
                matched,
                key=lambda row: self.db.field_value(self.table_name, row, self.order_field) or "",
                reverse=self.order_desc,
            )
        if self.limit_n is not None:
            matched = matched[: self.limit_n]
        return matched

    def execute(self):
        rows = self.db.tables.setdefault(self.table_name, [])
        if self.mode == "select":
            data = [self.db.enrich_row(self.table_name, row, self.fields) for row in self._matching_rows()]
            return SimpleNamespace(data=data)
        if self.mode == "insert":
            payloads = self.payload if isinstance(self.payload, list) else [self.payload]
            inserted = []
            for payload in payloads:
                row = copy.deepcopy(payload)
                rows.append(row)
                inserted.append(copy.deepcopy(row))
            return SimpleNamespace(data=inserted)
        if self.mode == "update":
            updated = []
            for row in rows:
                if all(check(row) for check in self.filters):
                    row.update(copy.deepcopy(self.payload or {}))
                    updated.append(copy.deepcopy(row))
            return SimpleNamespace(data=updated)
        if self.mode == "delete":
            kept = []
            removed = []
            for row in rows:
                if all(check(row) for check in self.filters):
                    removed.append(copy.deepcopy(row))
                else:
                    kept.append(row)
            self.db.tables[self.table_name] = kept
            return SimpleNamespace(data=removed)
        raise AssertionError(f"Unsupported query mode: {self.mode}")


class FakeSupabase:
    def __init__(self, tables):
        self.tables = copy.deepcopy(tables)
        self.auth = SimpleNamespace(get_user=lambda token: SimpleNamespace(user=SimpleNamespace(id="owner-1", email_confirmed_at="2026-04-13T00:00:00Z")))
        self.storage = FakeStorage()

    def table(self, table_name):
        return FakeQuery(self, table_name)

    def _shop_for_row(self, row):
        shop_id = row.get("shop_id")
        for shop in self.tables.get("shops", []):
            if shop.get("shop_id") == shop_id:
                return shop
        return {}

    def field_value(self, table_name, row, key):
        if "." in key:
            head, tail = key.split(".", 1)
            if table_name == "products" and head == "shops":
                return self.field_value("shops", self._shop_for_row(row), tail)
            current = row
            for part in key.split("."):
                if not isinstance(current, dict):
                    return None
                current = current.get(part)
            return current
        return row.get(key)

    def enrich_row(self, table_name, row, fields):
        enriched = copy.deepcopy(row)
        if table_name == "products" and "shops" in str(fields or ""):
            enriched["shops"] = copy.deepcopy(self._shop_for_row(row))
        return enriched


class BusinessOfferingRoutesTest(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        server.RATE_LIMIT_STATE.clear()
        self.fake_supabase = FakeSupabase(
            {
                "shops": [
                    {
                        "shop_id": "svc-1",
                        "shop_slug": "north-coast-travel",
                        "owner_user_id": "owner-1",
                        "name": "North Coast Travel",
                        "profile_image_url": "https://storage.example/product-images/business-profiles/svc-1/original.png",
                        "address": "Halifax and Dartmouth",
                        "formatted_address": "Halifax and Dartmouth",
                        "overview": "Trip planning, booking support, and travel guidance.",
                        "phone": "+1 902 555 0100",
                        "hours": "Mon-Fri 09:00-17:00",
                        "hours_structured": [
                            {"day": "mon", "start": "09:00", "end": "17:00"},
                            {"day": "tue", "start": "09:00", "end": "17:00"},
                        ],
                        "category": "Travel",
                        "business_type": "service",
                        "location_mode": "service_area",
                        "service_area": "Halifax and Dartmouth",
                        "whatsapp": "",
                        "country_code": "CA",
                        "country_name": "Canada",
                        "timezone_name": "America/Halifax",
                        "region": "Nova Scotia",
                        "city": "Halifax",
                        "postal_code": "",
                        "street_line1": "",
                        "street_line2": "",
                        "currency_code": "CAD",
                        "latitude": 44.6488,
                        "longitude": -63.5752,
                        "supports_pickup": False,
                        "supports_delivery": False,
                        "supports_walk_in": True,
                        "delivery_radius_km": None,
                        "delivery_fee": None,
                        "pickup_notes": "Appointments preferred before visiting.",
                        "created_at": "2026-04-13T00:00:00+00:00",
                    }
                ],
                "products": [
                    {
                        "shop_id": "svc-1",
                        "product_id": "offer-1",
                        "product_slug": "trip-planning-session",
                        "name": "Trip Planning Session",
                        "overview": "A one-hour travel planning consultation.",
                        "price": "From 120 CAD",
                        "price_amount": 120.0,
                        "currency_code": "CAD",
                        "offering_type": "service",
                        "price_mode": "starting_at",
                        "availability_mode": "available",
                        "stock": "in",
                        "stock_quantity": None,
                        "duration_minutes": 60,
                        "capacity": 1,
                        "variants": "",
                        "variant_data": [],
                        "variant_matrix": [],
                        "attribute_data": {"service_mode": "Online", "provider": "Senior agent"},
                        "images": [],
                        "updated_at": "2026-04-13T00:00:00+00:00",
                    }
                ],
                "profiles": [
                    {"id": "owner-1", "display_name": "Owner", "role": "customer"},
                    {"id": "user-1", "display_name": "Verified User", "role": "customer"},
                ],
                "favourites": [],
                "reviews": [
                    {
                        "id": 1,
                        "shop_id": "svc-1",
                        "product_id": "offer-1",
                        "user_id": "user-1",
                        "rating": 5,
                        "body": "Very helpful planning session.",
                        "created_at": "2026-04-13T00:00:00+00:00",
                    }
                ],
                "analytics": [],
                "order_requests": [],
            }
        )
        self.user = SimpleNamespace(id="user-1", email_confirmed_at="2026-04-13T00:00:00Z")
        self.owner = SimpleNamespace(id="owner-1", email_confirmed_at="2026-04-13T00:00:00Z")
        self.patchers = [
            patch.object(server, "supabase", self.fake_supabase),
            patch.object(server, "rebuild_kb", lambda shop_id: None),
            patch.object(server, "send_request_confirmation_email", lambda shop, row: None),
            patch.object(server, "send_request_status_email", lambda shop, row, status: None),
            patch.object(server, "geocode_structured_address", lambda row: {}),
            patch.object(server, "issue_request_track_token", lambda request_id, phone: f"track::{request_id}"),
            patch.object(server, "verify_request_track_token", lambda token, request_id, phone: token == f"track::{request_id}"),
        ]
        for patcher in self.patchers:
            patcher.start()
        self.client = TestClient(server.app)

    def tearDown(self):
        server.RATE_LIMIT_STATE.clear()
        for patcher in reversed(self.patchers):
            patcher.stop()

    def _owner_headers(self):
        return {"Authorization": "Bearer owner-token"}

    def _user_headers(self):
        return {"Authorization": "Bearer user-token"}

    def _patch_get_user(self, user):
        return patch.object(server, "get_user", lambda auth, request=None, user=user: (user, {"display_name": getattr(user, "id", "user")}))

    def test_alias_catalog_response_populates_business_and_offering_aliases(self):
        payload = server.alias_catalog_response(
            {
                "shop_id": "svc-1",
                "shop_slug": "north-coast-travel",
                "shop": {"shop_id": "svc-1", "name": "North Coast Travel"},
                "products": [{"product_id": "offer-1", "shop_id": "svc-1", "name": "Trip Planning Session"}],
            }
        )

        self.assertEqual(payload["business_id"], "svc-1")
        self.assertEqual(payload["business_slug"], "north-coast-travel")
        self.assertEqual(payload["business"]["name"], "North Coast Travel")
        self.assertEqual(payload["offerings"][0]["offering_id"], "offer-1")
        self.assertEqual(payload["offerings"][0]["business_id"], "svc-1")

    def test_public_business_endpoint_returns_canonical_and_legacy_keys(self):
        response = self.client.get("/public/business/north-coast-travel")
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["business_id"], "svc-1")
        self.assertEqual(data["shop_id"], "svc-1")
        self.assertEqual(data["business"]["business_id"], "svc-1")
        self.assertEqual(data["shop"]["shop_id"], "svc-1")
        self.assertEqual(data["business"]["profile_image_url"], "https://storage.example/product-images/business-profiles/svc-1/original.png")
        self.assertEqual(data["business"]["business_profile_image_url"], "https://storage.example/product-images/business-profiles/svc-1/original.png")
        self.assertEqual(data["offerings"][0]["offering_id"], "offer-1")
        self.assertEqual(data["products"][0]["product_id"], "offer-1")
        self.assertEqual(data["offerings"][0]["business_id"], "svc-1")

    def test_admin_profile_image_upload_updates_business_record(self):
        with self._patch_get_user(self.owner):
            response = self.client.post(
                "/admin/business/svc-1/profile-image",
                headers=self._owner_headers(),
                files={"profile_image": ("logo.png", b"business-image", "image/png")},
            )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["business_id"], "svc-1")
        self.assertIn("/product-images/business-profiles/svc-1/", data["profile_image_url"])

        shop = next(row for row in self.fake_supabase.tables["shops"] if row["shop_id"] == "svc-1")
        self.assertEqual(shop["profile_image_url"], data["profile_image_url"])
        self.assertIn(
            ("product-images", "business-profiles/svc-1/original.png"),
            self.fake_supabase.storage.removed,
        )
        uploaded_paths = set(self.fake_supabase.storage.objects.get("product-images", {}).keys())
        self.assertTrue(any(path.startswith("business-profiles/svc-1/") for path in uploaded_paths))

    def test_chat_accepts_business_id_query_param(self):
        with patch.object(server, "default_catalog_question", lambda *args, **kwargs: "Show all offerings"):
            response = self.client.get("/chat", params={"business_id": "svc-1", "q": "hello"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["business_id"], "svc-1")
        self.assertEqual(data["business_slug"], "north-coast-travel")
        self.assertIn("North Coast Travel", data["answer"])
        self.assertTrue(isinstance(data.get("offerings"), list))

    def test_customer_favourite_canonical_route_and_listing_work(self):
        with self._patch_get_user(self.user):
            save_response = self.client.post("/customer/favourite/svc-1/offer-1", headers=self._user_headers())
            self.assertEqual(save_response.status_code, 200)
            self.assertEqual(save_response.json()["business_id"], "svc-1")
            self.assertEqual(save_response.json()["offering_id"], "offer-1")

            list_response = self.client.get("/customer/favourites", headers=self._user_headers())
        self.assertEqual(list_response.status_code, 200)
        data = list_response.json()

        self.assertEqual(len(data["favourites"]), 1)
        self.assertEqual(data["offerings"][0]["offering_id"], "offer-1")
        self.assertEqual(data["offerings"][0]["business_id"], "svc-1")

    def test_public_fulfillment_request_accepts_business_and_offering_ids(self):
        response = self.client.post(
            "/public/fulfillment-request",
            json={
                "business_id": "svc-1",
                "fulfillment_type": "walk_in",
                "customer_name": "Alex Buyer",
                "phone": "+1 902 555 0111",
                "items": [
                    {
                        "business_id": "svc-1",
                        "offering_id": "offer-1",
                        "qty": 1,
                    }
                ],
            },
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["business_id"], "svc-1")
        self.assertTrue(data["track_token"].startswith("track::req_"))

        stored = self.fake_supabase.tables["order_requests"][0]
        self.assertEqual(stored["shop_id"], "svc-1")
        self.assertEqual(stored["items"][0]["product_id"], "offer-1")
        self.assertEqual(stored["items"][0]["currency_code"], "CAD")

    def test_public_fulfillment_request_requires_business_with_clear_message(self):
        response = self.client.post(
            "/public/fulfillment-request",
            json={
                "fulfillment_type": "walk_in",
                "customer_name": "Alex Buyer",
                "phone": "+1 902 555 0111",
                "items": [{"offering_id": "offer-1", "qty": 1}],
            },
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "Business is required")

    def test_create_business_accepts_business_payload(self):
        with self._patch_get_user(self.owner), \
            patch.object(server, "gen_shop_id", lambda name: "mentor-studio-1001"), \
            patch.object(server, "unique_shop_slug", lambda name, ignore_shop_id="": "mentor-studio"):
            response = self.client.post(
                "/admin/create-business",
                headers=self._owner_headers(),
                json={
                    "business": {
                        "name": "Mentor Studio",
                        "overview": "Private coaching sessions for creative founders.",
                        "category": "Education",
                        "business_type": "education",
                        "location_mode": "online",
                        "country_code": "CA",
                        "country_name": "Canada",
                        "timezone_name": "America/Halifax",
                        "currency_code": "CAD",
                        "hours_structured": [{"day": "mon", "start": "10:00", "end": "16:00"}],
                        "supports_pickup": False,
                        "supports_delivery": False,
                        "supports_walk_in": True,
                    }
                },
            )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["business_id"], "mentor-studio-1001")
        self.assertEqual(data["business_slug"], "mentor-studio")
        self.assertEqual(data["shop_id"], "mentor-studio-1001")

        inserted = next(row for row in self.fake_supabase.tables["shops"] if row["shop_id"] == "mentor-studio-1001")
        self.assertEqual(inserted["business_type"], "education")
        self.assertEqual(inserted["location_mode"], "online")
        self.assertEqual(inserted["formatted_address"], "Online")

        owner_profile = next(row for row in self.fake_supabase.tables["profiles"] if row["id"] == "owner-1")
        self.assertEqual(owner_profile["role"], "shopkeeper")

    def test_delete_business_cleans_related_rows(self):
        self.fake_supabase.tables["favourites"].append(
            {"user_id": "user-1", "shop_id": "svc-1", "product_id": "offer-1", "created_at": "2026-04-13T00:00:00+00:00"}
        )
        self.fake_supabase.tables["analytics"].extend(
            [
                {"id": 1, "shop_id": "svc-1", "product_id": "offer-1", "event": "view", "created_at": "2026-04-13T00:00:00+00:00"},
                {"id": 2, "shop_id": "svc-1", "product_id": None, "event": "chat", "created_at": "2026-04-13T00:00:00+00:00"},
            ]
        )
        self.fake_supabase.tables["order_requests"].append(
            {
                "request_id": "req-1",
                "shop_id": "svc-1",
                "status": "new",
                "items": [{"product_id": "offer-1", "shop_id": "svc-1"}],
            }
        )

        with self._patch_get_user(self.owner):
            response = self.client.delete("/admin/business/svc-1", headers=self._owner_headers())

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["business_id"], "svc-1")
        self.assertEqual([row for row in self.fake_supabase.tables["shops"] if row.get("shop_id") == "svc-1"], [])
        self.assertEqual([row for row in self.fake_supabase.tables["products"] if row.get("shop_id") == "svc-1"], [])
        self.assertEqual([row for row in self.fake_supabase.tables["order_requests"] if row.get("shop_id") == "svc-1"], [])
        self.assertEqual([row for row in self.fake_supabase.tables["analytics"] if row.get("shop_id") == "svc-1"], [])
        self.assertEqual([row for row in self.fake_supabase.tables["reviews"] if row.get("shop_id") == "svc-1"], [])
        self.assertEqual([row for row in self.fake_supabase.tables["favourites"] if row.get("shop_id") == "svc-1"], [])
        self.assertIn(
            ("product-images", "business-profiles/svc-1/original.png"),
            self.fake_supabase.storage.removed,
        )


if __name__ == "__main__":
    unittest.main()
