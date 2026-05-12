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
        self.count_requested = False
        self.head_only = False
        self.filters = []
        self.order_field = None
        self.order_desc = False
        self.limit_n = None
        self.range_start = None
        self.range_end = None
        self.payload = None

    def select(self, fields="*", count=None, head=False):
        self.mode = "select"
        self.fields = fields
        self.count_requested = count is not None
        self.head_only = bool(head)
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

    def range(self, start, end):
        self.range_start = int(start)
        self.range_end = int(end)
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
        return matched

    def execute(self):
        rows = self.db.tables.setdefault(self.table_name, [])
        if self.mode == "select":
            matched = self._matching_rows()
            total_count = len(matched)
            if self.range_start is not None:
                start = max(0, self.range_start)
                end = self.range_end if self.range_end is not None else total_count - 1
                matched = matched[start : max(start, end) + 1]
            if self.limit_n is not None:
                matched = matched[: self.limit_n]
            data = [] if self.head_only else [self.db.enrich_row(self.table_name, row, self.fields) for row in matched]
            return SimpleNamespace(data=data, count=total_count if self.count_requested else None)
        if self.mode == "insert":
            payloads = self.payload if isinstance(self.payload, list) else [self.payload]
            inserted = []
            for payload in payloads:
                self.db.reject_missing_columns(self.table_name, payload)
                row = copy.deepcopy(payload)
                rows.append(row)
                inserted.append(copy.deepcopy(row))
            return SimpleNamespace(data=inserted)
        if self.mode == "update":
            updated = []
            self.db.reject_missing_columns(self.table_name, self.payload or {})
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
        self.missing_columns = {}
        self.auth = SimpleNamespace(get_user=lambda token: SimpleNamespace(user=SimpleNamespace(id="owner-1", email_confirmed_at="2026-04-13T00:00:00Z")))
        self.storage = FakeStorage()

    def table(self, table_name):
        return FakeQuery(self, table_name)

    def reject_missing_columns(self, table_name, payload):
        missing = set(self.missing_columns.get(table_name, set()))
        for key in payload or {}:
            if key in missing:
                raise Exception(f"Could not find the '{key}' column of '{table_name}' in the schema cache")

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
                    },
                    {
                        "shop_id": "shoe-1",
                        "shop_slug": "alpha-shoes",
                        "owner_user_id": "owner-1",
                        "name": "Alpha Shoes",
                        "address": "10 Spring Garden Rd, Halifax",
                        "formatted_address": "10 Spring Garden Rd, Halifax",
                        "overview": "Athletic and casual shoes for everyday wear.",
                        "phone": "+1 902 555 0200",
                        "hours": "Mon-Sat 10:00-18:00",
                        "hours_structured": [
                            {"day": "mon", "start": "10:00", "end": "18:00"},
                            {"day": "tue", "start": "10:00", "end": "18:00"},
                        ],
                        "category": "Clothing",
                        "business_type": "retail",
                        "location_mode": "storefront",
                        "service_area": "",
                        "whatsapp": "",
                        "country_code": "CA",
                        "country_name": "Canada",
                        "timezone_name": "America/Halifax",
                        "region": "Nova Scotia",
                        "city": "Halifax",
                        "postal_code": "B3J 3N5",
                        "street_line1": "10 Spring Garden Rd",
                        "street_line2": "",
                        "currency_code": "CAD",
                        "latitude": 44.6422,
                        "longitude": -63.5800,
                        "supports_pickup": True,
                        "supports_delivery": False,
                        "supports_walk_in": True,
                        "delivery_radius_km": None,
                        "delivery_fee": None,
                        "pickup_notes": "",
                        "created_at": "2026-04-12T00:00:00+00:00",
                    },
                    {
                        "shop_id": "shoe-2",
                        "shop_slug": "budget-steps",
                        "owner_user_id": "owner-1",
                        "name": "Budget Steps",
                        "address": "25 Queen St, Halifax",
                        "formatted_address": "25 Queen St, Halifax",
                        "overview": "Budget-friendly shoe options and simple daily basics.",
                        "phone": "+1 902 555 0300",
                        "hours": "Mon-Fri 09:00-17:00",
                        "hours_structured": [
                            {"day": "mon", "start": "09:00", "end": "17:00"},
                            {"day": "tue", "start": "09:00", "end": "17:00"},
                        ],
                        "category": "Clothing",
                        "business_type": "retail",
                        "location_mode": "storefront",
                        "service_area": "",
                        "whatsapp": "",
                        "country_code": "CA",
                        "country_name": "Canada",
                        "timezone_name": "America/Halifax",
                        "region": "Nova Scotia",
                        "city": "Halifax",
                        "postal_code": "B3J 2H1",
                        "street_line1": "25 Queen St",
                        "street_line2": "",
                        "currency_code": "CAD",
                        "latitude": 44.6460,
                        "longitude": -63.5740,
                        "supports_pickup": True,
                        "supports_delivery": False,
                        "supports_walk_in": True,
                        "delivery_radius_km": None,
                        "delivery_fee": None,
                        "pickup_notes": "",
                        "created_at": "2026-04-11T00:00:00+00:00",
                    },
                    {
                        "shop_id": "drink-1",
                        "shop_slug": "harbour-shakes",
                        "owner_user_id": "owner-1",
                        "name": "Harbour Shakes",
                        "address": "40 Water St, Halifax",
                        "formatted_address": "40 Water St, Halifax",
                        "overview": "Fresh shakes, juices, and cold drinks.",
                        "phone": "+1 902 555 0400",
                        "hours": "Mon-Sat 09:00-19:00",
                        "hours_structured": [
                            {"day": "mon", "start": "09:00", "end": "19:00"},
                            {"day": "tue", "start": "09:00", "end": "19:00"},
                        ],
                        "category": "Food",
                        "business_type": "retail",
                        "location_mode": "storefront",
                        "service_area": "",
                        "whatsapp": "",
                        "country_code": "CA",
                        "country_name": "Canada",
                        "timezone_name": "America/Halifax",
                        "region": "Nova Scotia",
                        "city": "Halifax",
                        "postal_code": "B3J 1N8",
                        "street_line1": "40 Water St",
                        "street_line2": "",
                        "currency_code": "CAD",
                        "latitude": 44.6480,
                        "longitude": -63.5710,
                        "supports_pickup": True,
                        "supports_delivery": False,
                        "supports_walk_in": True,
                        "delivery_radius_km": None,
                        "delivery_fee": None,
                        "pickup_notes": "",
                        "created_at": "2026-04-10T00:00:00+00:00",
                    },
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
                    },
                    {
                        "shop_id": "shoe-1",
                        "product_id": "shoe-a1",
                        "product_slug": "trail-runner",
                        "name": "Trail Runner Shoes",
                        "overview": "Lightweight daily running shoes with extra grip.",
                        "price": "CAD 89.00",
                        "price_amount": 89.0,
                        "currency_code": "CAD",
                        "offering_type": "product",
                        "price_mode": "fixed",
                        "availability_mode": "available",
                        "stock": "in",
                        "stock_quantity": 12,
                        "duration_minutes": None,
                        "capacity": None,
                        "variants": "Size 9, Size 10",
                        "variant_data": [],
                        "variant_matrix": [],
                        "attribute_data": {"material": "Mesh", "style": "Running"},
                        "images": [],
                        "updated_at": "2026-04-12T00:00:00+00:00",
                    },
                    {
                        "shop_id": "shoe-2",
                        "product_id": "shoe-b1",
                        "product_slug": "city-walker",
                        "name": "City Walker Shoes",
                        "overview": "Affordable walking shoes for everyday errands.",
                        "price": "CAD 49.00",
                        "price_amount": 49.0,
                        "currency_code": "CAD",
                        "offering_type": "product",
                        "price_mode": "fixed",
                        "availability_mode": "available",
                        "stock": "in",
                        "stock_quantity": 8,
                        "duration_minutes": None,
                        "capacity": None,
                        "variants": "Size 8, Size 9",
                        "variant_data": [],
                        "variant_matrix": [],
                        "attribute_data": {"material": "Canvas", "style": "Walking"},
                        "images": [],
                        "updated_at": "2026-04-11T00:00:00+00:00",
                    },
                    {
                        "shop_id": "drink-1",
                        "product_id": "drink-mango",
                        "product_slug": "mango-shake",
                        "name": "Mango Shake",
                        "overview": "Chilled mango shake.",
                        "price": "CAD 3.00",
                        "price_amount": 3.0,
                        "currency_code": "CAD",
                        "offering_type": "product",
                        "price_mode": "fixed",
                        "availability_mode": "available",
                        "stock": "in",
                        "stock_quantity": 10,
                        "duration_minutes": None,
                        "capacity": None,
                        "variants": "",
                        "variant_data": [],
                        "variant_matrix": [],
                        "attribute_data": {"type": "Drink", "flavor": "Mango"},
                        "images": [],
                        "updated_at": "2026-04-10T00:00:00+00:00",
                    },
                    {
                        "shop_id": "drink-1",
                        "product_id": "drink-banana",
                        "product_slug": "banana-shake",
                        "name": "Banana Shake",
                        "overview": "Creamy banana shake.",
                        "price": "CAD 4.00",
                        "price_amount": 4.0,
                        "currency_code": "CAD",
                        "offering_type": "product",
                        "price_mode": "fixed",
                        "availability_mode": "available",
                        "stock": "in",
                        "stock_quantity": 9,
                        "duration_minutes": None,
                        "capacity": None,
                        "variants": "",
                        "variant_data": [],
                        "variant_matrix": [],
                        "attribute_data": {"type": "Drink", "flavor": "Banana"},
                        "images": [],
                        "updated_at": "2026-04-10T00:10:00+00:00",
                    },
                    {
                        "shop_id": "drink-1",
                        "product_id": "drink-premium-banana",
                        "product_slug": "premium-banana-shake",
                        "name": "Premium Banana Shake",
                        "overview": "Banana shake with extra toppings.",
                        "price": "CAD 6.00",
                        "price_amount": 6.0,
                        "currency_code": "CAD",
                        "offering_type": "product",
                        "price_mode": "fixed",
                        "availability_mode": "available",
                        "stock": "in",
                        "stock_quantity": 7,
                        "duration_minutes": None,
                        "capacity": None,
                        "variants": "",
                        "variant_data": [],
                        "variant_matrix": [],
                        "attribute_data": {"type": "Drink", "flavor": "Banana"},
                        "images": [],
                        "updated_at": "2026-04-10T00:20:00+00:00",
                    },
                ],
                "profiles": [
                    {"id": "owner-1", "display_name": "Owner", "role": "customer"},
                    {"id": "user-1", "display_name": "Verified User", "role": "customer"},
                    {"id": "admin-1", "display_name": "Admin User", "role": "admin"},
                ],
                "business_claims": [],
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
                    },
                    {
                        "id": 2,
                        "shop_id": "shoe-1",
                        "product_id": "shoe-a1",
                        "user_id": "user-1",
                        "rating": 5,
                        "body": "Great fit and very comfortable.",
                        "created_at": "2026-04-13T00:10:00+00:00",
                    },
                    {
                        "id": 3,
                        "shop_id": "shoe-2",
                        "product_id": "shoe-b1",
                        "user_id": "user-1",
                        "rating": 5,
                        "body": "Excellent value for the price.",
                        "created_at": "2026-04-13T00:20:00+00:00",
                    },
                ],
                "analytics": [],
                "order_requests": [],
                "business_audit_events": [],
            }
        )
        self.user = SimpleNamespace(id="user-1", email_confirmed_at="2026-04-13T00:00:00Z")
        self.owner = SimpleNamespace(id="owner-1", email_confirmed_at="2026-04-13T00:00:00Z")
        self.admin = SimpleNamespace(id="admin-1", email="admin@example.com", email_confirmed_at="2026-04-13T00:00:00Z")
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

    def _admin_headers(self):
        return {"Authorization": "Bearer admin-token"}

    def _profile_for_user(self, user):
        for row in self.fake_supabase.tables["profiles"]:
            if row.get("id") == getattr(user, "id", ""):
                return copy.deepcopy(row)
        return {"display_name": getattr(user, "id", "user")}

    def _patch_get_user(self, user):
        return patch.object(server, "get_user", lambda auth, request=None, user=user: (user, self._profile_for_user(user)))

    def _add_alpha_shoe_products(self, count=8):
        base = next(row for row in self.fake_supabase.tables["products"] if row["product_id"] == "shoe-a1")
        for idx in range(2, count + 2):
            row = copy.deepcopy(base)
            row["product_id"] = f"shoe-a{idx}"
            row["product_slug"] = f"alpha-shoe-{idx}"
            row["name"] = f"Alpha Shoe {idx}"
            row["overview"] = f"Alpha shoe option {idx} for everyday wear."
            row["price"] = f"CAD {70 + idx}.00"
            row["price_amount"] = float(70 + idx)
            row["updated_at"] = f"2026-04-{10 + idx:02d}T00:00:00+00:00"
            self.fake_supabase.tables["products"].append(row)

    def _add_platform_managed_shop(self):
        shop = copy.deepcopy(self.fake_supabase.tables["shops"][0])
        shop.update(
            {
                "shop_id": "import-1",
                "shop_slug": "imported-tech",
                "owner_user_id": "imports-1",
                "owner_contact_name": "Platform imported listing",
                "listing_source": "platform_import",
                "ownership_status": "platform_managed",
                "name": "Imported Tech",
                "category": "Professional Services",
                "overview": "Imported computer support listing managed by Atlantic Ordinate until claimed.",
            }
        )
        self.fake_supabase.tables["shops"].append(shop)
        self.fake_supabase.tables["profiles"].append({"id": "imports-1", "display_name": "Atlantic Ordinate Imports", "role": "shopkeeper"})
        return shop

    def _managed_business_payload(self):
        return {
            "business": {
                "name": "Managed Repair",
                "overview": "Staff-created computer support listing for verified public discovery.",
                "category": "Professional Services",
                "business_type": "service",
                "location_mode": "service_area",
                "service_area": "Summerside, Prince Edward Island",
                "phone": "+1 902 555 0900",
                "country_code": "CA",
                "country_name": "Canada",
                "timezone_name": "America/Halifax",
                "currency_code": "CAD",
                "region": "Prince Edward Island",
                "city": "Summerside",
                "hours_structured": [{"day": "mon", "start": "09:00", "end": "17:00"}],
                "supports_pickup": False,
                "supports_delivery": False,
                "supports_walk_in": True,
                "verification_method": "website",
                "verification_evidence": "https://managed.example",
            },
            "publish": True,
        }

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
        self.assertEqual(data["business"]["business_transparency"]["manager_type"], "business_owner")
        self.assertEqual(data["business"]["business_transparency"]["managed_by"], "Verified business owner")
        self.assertFalse(data["business"]["business_transparency"]["claim_available"])
        self.assertEqual(data["offerings"][0]["offering_id"], "offer-1")
        self.assertEqual(data["products"][0]["product_id"], "offer-1")
        self.assertEqual(data["offerings"][0]["business_id"], "svc-1")
        self.assertIsNone(data["business"]["latitude"])
        self.assertIsNone(data["business"]["longitude"])

    def test_admin_offering_accepts_named_external_links(self):
        with self._patch_get_user(self.owner):
            response = self.client.post(
                "/admin/business/svc-1/offering",
                headers=self._owner_headers(),
                json={
                    "product_id": "offer-1",
                    "name": "Trip Planning Session",
                    "overview": "A one-hour travel planning consultation.",
                    "price_amount": 120,
                    "currency_code": "CAD",
                    "offering_type": "service",
                    "price_mode": "starting_at",
                    "availability_mode": "available",
                    "stock": "in",
                    "duration_minutes": 60,
                    "attribute_data": {"service_mode": "Online", "provider": "Senior agent"},
                    "external_links": [
                        {"label": "Book Session", "url": "northcoast.example/book/trip-planning"},
                        {"label": "Planning Form", "url": "https://forms.example/intake"},
                    ],
                },
            )

        self.assertEqual(response.status_code, 200)
        stored = next(row for row in self.fake_supabase.tables["products"] if row["product_id"] == "offer-1")
        self.assertEqual(
            stored["external_links"],
            [
                {"label": "Book Session", "url": "https://northcoast.example/book/trip-planning"},
                {"label": "Planning Form", "url": "https://forms.example/intake"},
            ],
        )

        public_response = self.client.get("/public/business/north-coast-travel")
        self.assertEqual(public_response.status_code, 200)
        offering = public_response.json()["offerings"][0]
        self.assertEqual(offering["external_links"], stored["external_links"])
        self.assertEqual(offering["offering_links"], stored["external_links"])

    def test_public_business_list_hides_coordinates_for_non_mappable_locations(self):
        response = self.client.get("/public/businesses")
        self.assertEqual(response.status_code, 200)
        data = response.json()

        business = data["businesses"][0]
        self.assertEqual(business["business_id"], "svc-1")
        self.assertEqual(business["location_mode"], "service_area")
        self.assertIsNone(business["latitude"])
        self.assertIsNone(business["longitude"])
        self.assertNotIn("business_transparency", business)

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

    def test_business_chat_greeting_does_not_attach_product_cards(self):
        response = self.client.get("/chat", params={"business_id": "svc-1", "q": "hello"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertIn("North Coast Travel", data["answer"])
        self.assertEqual(data.get("offerings"), [])

    def test_business_chat_shop_profile_caps_product_cards_for_large_catalog(self):
        self._add_alpha_shoe_products(8)
        response = self.client.get("/chat", params={"business_id": "alpha-shoes", "q": "Tell me about this shop"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertIn("Alpha Shoes", data["answer"])
        self.assertIn("popular", data["answer"])
        self.assertEqual(len(data.get("offerings", [])), 4)
        self.assertTrue(all(item["business_id"] == "shoe-1" for item in data["offerings"]))

    def test_global_chat_returns_marketplace_summary_for_app_question(self):
        response = self.client.get("/chat/global", params={"q": "What is this app?"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["assistant"], "Atlantica")
        self.assertEqual(data["mode"], "global")
        self.assertEqual(data["context"]["summary"]["business_count"], 4)
        self.assertEqual(data["context"]["summary"]["offering_count"], 6)
        self.assertIn("Atlantica", data["answer"])
        self.assertIn("Atlantic Ordinate", data["answer"])

    def test_global_chat_finds_cheapest_matching_shoes(self):
        response = self.client.get("/chat/global", params={"q": "Find the cheapest shoes"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["assistant"], "Atlantica")
        self.assertEqual(data["mode"], "global")
        self.assertIn("City Walker Shoes", data["answer"])
        self.assertIn("Budget Steps", data["answer"])
        self.assertGreaterEqual(len(data["offerings"]), 1)
        self.assertEqual(data["offerings"][0]["business_name"], "Budget Steps")
        self.assertEqual(data["offerings"][0]["price"], "CAD 49.00")

    def test_global_chat_cheapest_banana_shake_uses_exact_subject(self):
        response = self.client.get("/chat/global", params={"q": "Show me the cheapest banana shake"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["assistant"], "Atlantica")
        self.assertEqual(data["mode"], "global")
        self.assertIn("Banana Shake", data["answer"])
        self.assertNotIn("Mango Shake", data["answer"])
        offerings = data.get("offerings", [])
        self.assertGreaterEqual(len(offerings), 1)
        self.assertEqual(offerings[0]["name"], "Banana Shake")
        self.assertEqual(offerings[0]["price"], "CAD 4.00")
        self.assertTrue(all("Banana" in item["name"] for item in offerings))

    def test_global_chat_show_all_drinks_matches_drink_synonyms(self):
        response = self.client.get("/chat/global", params={"q": "Show all drinks"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        names = {item["name"] for item in data.get("offerings", [])}
        self.assertIn("Mango Shake", names)
        self.assertIn("Banana Shake", names)
        self.assertIn("Premium Banana Shake", names)
        self.assertTrue(all(item["business_name"] == "Harbour Shakes" for item in data.get("offerings", [])))

    def test_global_chat_banana_shake_available_finds_existing_item(self):
        response = self.client.get("/chat/global", params={"q": "Is banana shake available?"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertIn("Banana Shake", data["answer"])
        self.assertNotIn("Mango Shake", data["answer"])
        names = {item["name"] for item in data.get("offerings", [])}
        self.assertIn("Banana Shake", names)
        self.assertIn("Premium Banana Shake", names)

    def test_global_chat_food_intent_uses_semantic_grounding_and_followup(self):
        with patch.object(server, "llm_chat", side_effect=RuntimeError("offline test")):
            response = self.client.get("/chat/global", params={"q": "I want to eat something"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        answer = data["answer"].lower()
        self.assertIn("help you find something to eat", answer)
        self.assertIn("full meal", answer)
        self.assertNotIn("keyword", answer)
        names = {item["name"] for item in data.get("offerings", [])}
        businesses = {item["name"] for item in data.get("businesses", [])}
        self.assertIn("Mango Shake", names)
        self.assertIn("Harbour Shakes", businesses)

    def test_global_chat_receptionist_prompt_examples_stay_human_and_grounded(self):
        prompts = [
            "hi",
            "hello",
            "I want to eat something",
            "I am hungry",
            "show me food",
            "do you have juice",
            "I want something cheap",
            "what can I buy here",
            "recommend something",
            "find restaurants near me",
            "do you have mango shake",
            "I need a phone charger",
            "what shops are available",
        ]

        results = {}
        with patch.object(server, "llm_chat", side_effect=RuntimeError("offline test")):
            for prompt in prompts:
                with self.subTest(prompt=prompt):
                    response = self.client.get("/chat/global", params={"q": prompt})
                    self.assertEqual(response.status_code, 200)
                    data = response.json()
                    self.assertTrue(str(data.get("answer", "")).strip())
                    self.assertNotIn("keyword", str(data.get("answer", "")).lower())
                    results[prompt] = data

        self.assertIn("Atlantica", results["what can I buy here"]["answer"])
        self.assertTrue(results["I want something cheap"].get("offerings"))
        self.assertTrue(results["find restaurants near me"].get("businesses"))
        self.assertEqual(results["do you have mango shake"]["offerings"][0]["name"], "Mango Shake")
        self.assertIn("phone charger", results["I need a phone charger"]["answer"].lower())
        self.assertEqual(results["I need a phone charger"].get("offerings"), [])
        self.assertTrue(results["what shops are available"].get("businesses"))

    def test_global_chat_about_specific_shop_returns_only_that_shop_and_highlight(self):
        response = self.client.get("/chat/global", params={"q": "Tell me about Alpha Shoes"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["assistant"], "Atlantica")
        self.assertEqual(data["mode"], "global")
        self.assertIn("Alpha Shoes", data["answer"])
        self.assertEqual(len(data.get("businesses", [])), 1)
        self.assertEqual(data["businesses"][0]["name"], "Alpha Shoes")
        self.assertEqual(data["businesses"][0]["stats"]["offering_count"], 1)
        self.assertEqual(len(data.get("offerings", [])), 1)
        self.assertEqual(data["offerings"][0]["business_name"], "Alpha Shoes")
        self.assertEqual(data["offerings"][0]["name"], "Trail Runner Shoes")

    def test_global_chat_shop_profile_caps_product_cards_for_large_catalog(self):
        self._add_alpha_shoe_products(8)
        response = self.client.get("/chat/global", params={"q": "Tell me about Alpha Shoes"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(len(data.get("businesses", [])), 1)
        self.assertEqual(data["businesses"][0]["name"], "Alpha Shoes")
        self.assertEqual(len(data.get("offerings", [])), 4)
        self.assertTrue(all(item["business_name"] == "Alpha Shoes" for item in data["offerings"]))

    def test_global_chat_shop_hours_does_not_attach_product_cards(self):
        response = self.client.get("/chat/global", params={"q": "What are Alpha Shoes hours?"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertIn("Alpha Shoes", data["answer"])
        self.assertEqual(len(data.get("businesses", [])), 1)
        self.assertEqual(data.get("offerings"), [])

    def test_global_chat_finds_five_star_products_across_shops(self):
        response = self.client.get("/chat/global", params={"q": "Show me products with five star ratings"})
        self.assertEqual(response.status_code, 200)
        data = response.json()

        self.assertEqual(data["assistant"], "Atlantica")
        self.assertEqual(data["mode"], "global")
        self.assertIn("5-star", data["answer"])
        offerings = data.get("offerings", [])
        names = {item["name"] for item in offerings}
        businesses = {item["business_name"] for item in offerings}
        self.assertIn("Trail Runner Shoes", names)
        self.assertIn("City Walker Shoes", names)
        self.assertIn("Alpha Shoes", businesses)
        self.assertIn("Budget Steps", businesses)
        self.assertTrue(all(item["avg_rating"] == 5.0 for item in offerings))

    def test_build_chat_suggestions_returns_question_list_for_ranked_results(self):
        shop = server.normalize_shop_record(self.fake_supabase.tables["shops"][0])
        picked = [self.fake_supabase.tables["products"][0]]

        suggestions = server.build_chat_suggestions("Tell me more about your services", shop, picked)

        self.assertIsInstance(suggestions, list)
        self.assertIn("What is the price of Trip Planning Session?", suggestions)
        self.assertIn("Show me photos of Trip Planning Session", suggestions)

    def test_normalize_availability_mode_maps_legacy_booked_out_to_unavailable(self):
        self.assertEqual(server.normalize_availability_mode("booked_out", "service"), "unavailable")
        self.assertEqual(server.normalize_availability_mode("on_request", "service"), "on_request")

    def test_chat_retries_truncated_llm_answer(self):
        calls = []

        def fake_llm(system, user, max_tokens=None):
            calls.append(max_tokens)
            if len(calls) == 1:
                return {
                    "content": "Trip Planning Session is a strong fit for family travel because it includes itinerary guidance, booking support, and practical planning for",
                    "model": "test-model",
                    "finish_reason": "length",
                    "truncated": True,
                }
            return {
                "content": "Trip Planning Session is a strong fit for family travel because it includes itinerary guidance, booking support, and practical planning for a group. It lasts 60 minutes and starts at 120 CAD.",
                "model": "test-model",
                "finish_reason": "stop",
                "truncated": False,
            }

        with patch.object(server, "HAS_RAG", False), patch.object(server, "llm_chat", side_effect=fake_llm):
            response = self.client.get(
                "/chat",
                params={"business_id": "svc-1", "q": "I need help choosing the right option for a family trip."},
            )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(calls), 2)
        self.assertGreater(calls[1], calls[0])
        self.assertIn("family travel", data["answer"])
        self.assertEqual(data["meta"]["finish_reason"], "stop")
        self.assertIsInstance(data["meta"]["suggestions"], list)

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
                        "phone": "+1 902 555 0400",
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

    def test_admin_can_create_platform_managed_business(self):
        with self._patch_get_user(self.admin), \
            patch.object(server, "gen_shop_id", lambda name: "managed-repair-1001"), \
            patch.object(server, "unique_shop_slug", lambda name, ignore_shop_id="": "managed-repair"):
            response = self.client.post(
                "/admin/managed-businesses",
                headers=self._admin_headers(),
                json=self._managed_business_payload(),
            )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["business_id"], "managed-repair-1001")
        self.assertEqual(data["business_slug"], "managed-repair")
        self.assertEqual(data["listing_status"], "verified")
        self.assertEqual(data["listing_source"], "platform_import")
        self.assertEqual(data["ownership_status"], "platform_managed")
        self.assertTrue(data["is_platform_managed"])

        inserted = next(row for row in self.fake_supabase.tables["shops"] if row["shop_id"] == "managed-repair-1001")
        self.assertEqual(inserted["owner_user_id"], "admin-1")
        self.assertEqual(inserted["owner_contact_name"], server.PLATFORM_MANAGED_OWNER_CONTACT)
        self.assertEqual(inserted["listing_source"], "platform_import")
        self.assertEqual(inserted["ownership_status"], "platform_managed")
        self.assertIsNone(inserted["claimed_at"])
        self.assertTrue(inserted["verified_at"])

        with self._patch_get_user(self.admin):
            managed_response = self.client.get("/admin/managed-businesses", headers=self._admin_headers())
            my_businesses_response = self.client.get("/admin/my-businesses", headers=self._admin_headers())
            detail_response = self.client.get("/admin/business/managed-repair-1001", headers=self._admin_headers())
        public_response = self.client.get("/public/business/managed-repair")

        self.assertEqual([row["business_id"] for row in managed_response.json()["businesses"]], ["managed-repair-1001"])
        self.assertEqual(my_businesses_response.json()["businesses"], [])
        self.assertEqual(detail_response.status_code, 200)
        detail_events = detail_response.json()["data"]["audit_events"]
        self.assertEqual(detail_events[0]["event_type"], "managed_listing_created")
        self.assertEqual(detail_events[0]["actor_user_id"], "admin-1")
        audit_rows = [row for row in self.fake_supabase.tables["business_audit_events"] if row["shop_id"] == "managed-repair-1001"]
        self.assertEqual([row["event_type"] for row in audit_rows], ["managed_listing_created"])
        self.assertEqual(public_response.status_code, 200)
        public_business = public_response.json()["business"]
        self.assertEqual(public_business["business_id"], "managed-repair-1001")
        self.assertEqual(public_business["business_transparency"]["manager_type"], "atlantic_ordinate")
        self.assertEqual(public_business["business_transparency"]["managed_by"], "Atlantic Ordinate staff")
        self.assertEqual(public_business["business_transparency"]["claim_status"], "Not yet claimed by the business owner")
        self.assertTrue(public_business["business_transparency"]["claim_available"])
        self.assertNotIn("ownership_status", public_business)
        self.assertNotIn("listing_source", public_business)

    def test_admin_can_create_platform_managed_business_with_legacy_ownership_schema(self):
        self.fake_supabase.missing_columns["shops"] = {"listing_source", "ownership_status", "claimed_at"}

        with self._patch_get_user(self.admin), \
            patch.object(server, "gen_shop_id", lambda name: "managed-repair-1001"), \
            patch.object(server, "unique_shop_slug", lambda name, ignore_shop_id="": "managed-repair"):
            response = self.client.post(
                "/admin/managed-businesses",
                headers=self._admin_headers(),
                json=self._managed_business_payload(),
            )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["business_id"], "managed-repair-1001")
        self.assertEqual(data["listing_status"], "verified")
        self.assertEqual(data["listing_source"], "platform_import")
        self.assertEqual(data["ownership_status"], "platform_managed")
        self.assertTrue(data["is_platform_managed"])

        inserted = next(row for row in self.fake_supabase.tables["shops"] if row["shop_id"] == "managed-repair-1001")
        self.assertEqual(inserted["owner_contact_name"], server.PLATFORM_MANAGED_OWNER_CONTACT)
        self.assertNotIn("listing_source", inserted)
        self.assertNotIn("ownership_status", inserted)
        self.assertNotIn("claimed_at", inserted)

        with self._patch_get_user(self.admin):
            managed_response = self.client.get("/admin/managed-businesses", headers=self._admin_headers())

        self.assertEqual([row["business_id"] for row in managed_response.json()["businesses"]], ["managed-repair-1001"])

    def test_non_admin_cannot_create_platform_managed_business(self):
        with self._patch_get_user(self.user):
            response = self.client.post(
                "/admin/managed-businesses",
                headers=self._user_headers(),
                json=self._managed_business_payload(),
            )

        self.assertEqual(response.status_code, 403)
        self.assertFalse(any(row.get("name") == "Managed Repair" for row in self.fake_supabase.tables["shops"]))

    def test_admin_can_manage_platform_import_until_claimed(self):
        self._add_platform_managed_shop()

        with self._patch_get_user(self.admin):
            list_response = self.client.get("/admin/managed-businesses", headers=self._admin_headers())
            detail_response = self.client.get("/admin/business/import-1", headers=self._admin_headers())

        self.assertEqual(list_response.status_code, 200)
        self.assertEqual([row["business_id"] for row in list_response.json()["businesses"]], ["import-1"])
        self.assertEqual(detail_response.status_code, 200)
        self.assertEqual(detail_response.json()["data"]["business"]["name"], "Imported Tech")

    def test_non_admin_cannot_manage_platform_import(self):
        self._add_platform_managed_shop()

        with self._patch_get_user(self.user):
            response = self.client.get("/admin/business/import-1", headers=self._user_headers())

        self.assertEqual(response.status_code, 404)

    def test_claim_candidates_mark_claimed_and_claimable_businesses(self):
        claimed_shop = next(row for row in self.fake_supabase.tables["shops"] if row["shop_id"] == "svc-1")
        claimed_shop.update(
            {
                "listing_status": "verified",
                "owner_contact_name": "Owner",
                "listing_source": "owner_created",
                "ownership_status": "claimed",
                "verified_at": "2026-04-13T00:00:00+00:00",
            }
        )
        managed_shop = self._add_platform_managed_shop()
        managed_shop["listing_status"] = "verified"
        managed_shop["verified_at"] = "2026-04-14T00:00:00+00:00"

        with self._patch_get_user(self.user):
            response = self.client.get("/admin/business-claim/candidates?q=Halifax", headers=self._user_headers())

        self.assertEqual(response.status_code, 200)
        businesses = {row["business_id"]: row for row in response.json()["businesses"]}
        self.assertFalse(businesses["svc-1"]["claim_available"])
        self.assertEqual(businesses["svc-1"]["claim_state"], "claimed")
        self.assertTrue(businesses["import-1"]["claim_available"])
        self.assertEqual(businesses["import-1"]["claim_state"], "claimable")

    def test_user_cannot_request_access_to_already_claimed_business(self):
        with self._patch_get_user(self.user):
            response = self.client.post(
                "/admin/business/svc-1/claim",
                headers=self._user_headers(),
                json={"note": "I want access to this existing page."},
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("already claimed", response.json()["detail"])
        self.assertEqual(self.fake_supabase.tables["business_claims"], [])

    def test_user_can_request_access_to_platform_managed_business(self):
        self._add_platform_managed_shop()

        with self._patch_get_user(self.user):
            response = self.client.post(
                "/admin/business/import-1/claim",
                headers=self._user_headers(),
                json={"note": "I own this business and can verify by website and phone."},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["claim"]["business_id"], "import-1")
        self.assertEqual(len(self.fake_supabase.tables["business_claims"]), 1)

    def test_admin_claim_queue_includes_transfer_context(self):
        self._add_platform_managed_shop()
        self.fake_supabase.tables["business_claims"].append(
            {
                "claim_id": "claim-1",
                "shop_id": "import-1",
                "claimant_user_id": "user-1",
                "claimant_display_name": "Verified User",
                "claimant_email": "verified@example.com",
                "note": "I own this business.",
                "status": "pending",
                "review_note": "",
            }
        )

        with self._patch_get_user(self.admin):
            admin_response = self.client.get("/admin/review/business-claims", headers=self._admin_headers())
        with self._patch_get_user(self.user):
            owner_response = self.client.get("/admin/my-business-claims", headers=self._user_headers())

        self.assertEqual(admin_response.status_code, 200)
        claim = admin_response.json()["claims"][0]
        self.assertEqual(claim["current_manager_label"], "Atlantic Ordinate staff")
        self.assertEqual(claim["current_manager_account_id"], "imports-1")
        self.assertEqual(claim["current_ownership_state"], "Staff-managed until claim approval")
        self.assertEqual(claim["transfer_target_label"], "Verified User")

        self.assertEqual(owner_response.status_code, 200)
        owner_claim = owner_response.json()["claims"][0]
        self.assertNotIn("current_manager_account_id", owner_claim)
        self.assertNotIn("current_ownership_state", owner_claim)

    def test_claim_approval_transfers_platform_import_out_of_managed_queue(self):
        self._add_platform_managed_shop()
        self.fake_supabase.tables["business_claims"].append(
            {
                "claim_id": "claim-1",
                "shop_id": "import-1",
                "claimant_user_id": "user-1",
                "claimant_display_name": "Verified User",
                "claimant_email": "verified@example.com",
                "note": "I own this business.",
                "status": "pending",
                "review_note": "",
            }
        )

        with self._patch_get_user(self.admin):
            response = self.client.post("/admin/review/business-claim/claim-1/approve", headers=self._admin_headers())
            detail_response = self.client.get("/admin/business/import-1", headers=self._admin_headers())
            list_response = self.client.get("/admin/managed-businesses", headers=self._admin_headers())

        self.assertEqual(response.status_code, 200)
        shop = next(row for row in self.fake_supabase.tables["shops"] if row["shop_id"] == "import-1")
        self.assertEqual(shop["owner_user_id"], "user-1")
        self.assertEqual(shop["listing_source"], "owner_created")
        self.assertEqual(shop["ownership_status"], "claimed")
        self.assertEqual(shop["owner_contact_name"], "Verified User")
        self.assertEqual(detail_response.status_code, 404)
        self.assertEqual(list_response.json()["businesses"], [])
        audit_rows = [row for row in self.fake_supabase.tables["business_audit_events"] if row["shop_id"] == "import-1"]
        self.assertEqual([row["event_type"] for row in audit_rows], ["ownership_claim_approved"])
        self.assertEqual(audit_rows[0]["metadata"]["previous_owner_user_id"], "imports-1")
        self.assertEqual(audit_rows[0]["metadata"]["new_owner_user_id"], "user-1")

        with self._patch_get_user(self.user):
            owner_detail_response = self.client.get("/admin/business/import-1", headers=self._user_headers())

        self.assertEqual(owner_detail_response.status_code, 200)
        owner_business = owner_detail_response.json()["data"]["business"]
        self.assertFalse(owner_business["is_platform_managed"])
        self.assertEqual(owner_business["ownership_status"], "claimed")
        self.assertEqual(owner_business["managed_by_label"], "Verified User")
        self.assertEqual(owner_business["management_account_id"], "user-1")

    def test_claimed_business_detail_repairs_stale_platform_metadata_for_approved_owner(self):
        shop = self._add_platform_managed_shop()
        shop["owner_user_id"] = "user-1"
        shop["ownership_status"] = "platform_managed"
        shop["listing_source"] = "platform_import"
        shop["owner_contact_name"] = server.PLATFORM_MANAGED_OWNER_CONTACT
        self.fake_supabase.tables["business_claims"].append(
            {
                "claim_id": "claim-1",
                "shop_id": "import-1",
                "claimant_user_id": "user-1",
                "claimant_display_name": "Verified User",
                "claimant_email": "verified@example.com",
                "note": "I own this business.",
                "status": "approved",
                "review_note": "Approved.",
            }
        )

        with self._patch_get_user(self.user):
            response = self.client.get("/admin/business/import-1", headers=self._user_headers())

        self.assertEqual(response.status_code, 200)
        business = response.json()["data"]["business"]
        self.assertFalse(business["is_platform_managed"])
        self.assertEqual(business["ownership_status"], "claimed")
        self.assertEqual(business["managed_by_label"], "Verified User")
        self.assertEqual(business["management_account_id"], "user-1")

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
