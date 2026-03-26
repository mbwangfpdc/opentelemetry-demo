#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import random
import sys
import time
import uuid

import grpc
from locust import User, between, events, task

from opentelemetry import baggage, context, trace
from opentelemetry.context import Context

from openfeature import api
from openfeature.contrib.hook.opentelemetry import TracingHook
from openfeature.contrib.provider.ofrep import OFREPProvider


def _env_seconds(name: str, default: float = 0.0) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        seconds = float(value)
    except ValueError:
        logging.warning("Invalid %s value %r; using default %s", name, value, default)
        return default

    if seconds < 0:
        logging.warning("Negative %s value %s is not allowed; using default %s", name, seconds, default)
        return default

    return seconds


try:
    import grpc.experimental.gevent as grpc_gevent
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(
        "grpc gevent integration is required for this load generator, "
        "but grpc.experimental.gevent could not be imported."
    ) from exc

try:
    grpc_gevent.init_gevent()
except Exception as exc:  # noqa: BLE001
    raise RuntimeError(
        "grpc gevent integration is required for this load generator, "
        "but grpc_gevent.init_gevent() failed."
    ) from exc

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RECOMMENDATION_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "recommendation"))

for path in (BASE_DIR, RECOMMENDATION_DIR):
    if os.path.isdir(path) and path not in sys.path:
        sys.path.insert(0, path)

try:
    import demo_pb2
    import demo_pb2_grpc
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "Could not import demo_pb2/demo_pb2_grpc. Generate protobuf stubs in "
        "src/recommendation or ensure PYTHONPATH includes src/recommendation."
    ) from exc


logging.basicConfig(level=logging.INFO)
logging.info("gRPC load generator initialized")

GRPC_WARMUP_SECONDS = _env_seconds("GRPC_WARMUP", default=0.0)


@events.test_start.add_listener
def _schedule_stats_reset_after_warmup(environment, **kwargs):
    del kwargs
    if GRPC_WARMUP_SECONDS <= 0:
        return

    def _reset_stats():
        environment.stats.reset_all()
        logging.info("Locust stats reset after GRPC_WARMUP=%s seconds", GRPC_WARMUP_SECONDS)

    import gevent

    gevent.spawn_later(GRPC_WARMUP_SECONDS, _reset_stats)
    logging.info("GRPC_WARMUP enabled: stats will reset after %s seconds", GRPC_WARMUP_SECONDS)


def _normalize_grpc_target(value: str) -> str:
    target = value.strip()
    if target.startswith("http://"):
        target = target[len("http://"):]
    elif target.startswith("https://"):
        target = target[len("https://"):]
    return target


def _load_people() -> list[dict]:
    people_path = os.path.join(BASE_DIR, "people.json")
    with open(people_path, encoding="utf-8") as people_file:
        return json.load(people_file)


# Initialize Flagd provider
base_url = f"http://{os.environ.get('FLAGD_HOST', 'localhost')}:{os.environ.get('FLAGD_OFREP_PORT', 8016)}"
api.set_provider(OFREPProvider(base_url=base_url))
api.add_hooks([TracingHook()])


def get_flagd_value(flag_name: str) -> int:
    client = api.get_client()
    return client.get_integer_value(flag_name, 0)


categories = [
    "binoculars",
    "telescopes",
    "accessories",
    "assembly",
    "travel",
    "books",
    None,
]

products = [
    "0PUK6V6EV0",
    "1YMWWN1N4O",
    "2ZYFJ3GM2N",
    "66VCHSJNUP",
    "6E92ZMYYFZ",
    "9SIQT8TOJO",
    "L9ECAV7KIM",
    "LS4PSXUNUM",
    "OLJCESPC7Z",
    "HQTGWGPNH4",
]

people = _load_people()


class WebsiteGrpcUser(User):
    wait_time = between(1, 10)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tracer = trace.get_tracer(__name__)
        self.session_id = ""
        self.user_id = ""
        self.catalog_channel = None
        self.recommendation_channel = None
        self.ad_channel = None
        self.cart_channel = None
        self.checkout_channel = None

    def _build_channels_and_stubs(self):
        product_catalog_addr = _normalize_grpc_target(
            os.environ["PRODUCT_CATALOG_ADDR"]
        )
        recommendation_addr = _normalize_grpc_target(
            os.environ["RECOMMENDATION_ADDR"]
        )
        ad_addr = _normalize_grpc_target(os.environ["AD_ADDR"])
        cart_addr = _normalize_grpc_target(os.environ["CART_ADDR"])
        checkout_addr = _normalize_grpc_target(
            os.environ["CHECKOUT_ADDR"]
        )

        self.catalog_channel = grpc.insecure_channel(product_catalog_addr)
        self.recommendation_channel = grpc.insecure_channel(recommendation_addr)
        self.ad_channel = grpc.insecure_channel(ad_addr)
        self.cart_channel = grpc.insecure_channel(cart_addr)
        self.checkout_channel = grpc.insecure_channel(checkout_addr)

        self.product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(self.catalog_channel)
        self.recommendation_stub = demo_pb2_grpc.RecommendationServiceStub(
            self.recommendation_channel
        )
        self.ad_stub = demo_pb2_grpc.AdServiceStub(self.ad_channel)
        self.cart_stub = demo_pb2_grpc.CartServiceStub(self.cart_channel)
        self.checkout_stub = demo_pb2_grpc.CheckoutServiceStub(self.checkout_channel)

    def _metadata(self):
        return [
            ("baggage", f"session.id={self.session_id},synthetic_request=true"),
        ]

    def _grpc_unary(self, name: str, method, request):
        start_time = time.perf_counter()
        response = None
        exception = None
        response_length = 0

        try:
            with self.tracer.start_as_current_span(name, context=Context()):
                response = method(request, metadata=self._metadata(), timeout=5.0)
                if response is not None and hasattr(response, "ByteSize"):
                    response_length = response.ByteSize()
        except grpc.RpcError as exc:
            exception = exc
            logging.error("gRPC call failed for %s: %s", name, exc)
        except Exception as exc:  # noqa: BLE001
            exception = exc
            logging.error("Unexpected error for %s: %s", name, exc)
        finally:
            # NOTE: We report the response time in nanoseconds to measure more granularly
            # The UI will say milliseconds. It's wrong!!!
            elapsed_nanos = (time.perf_counter() - start_time) * 1_000_000_000
            events.request.fire(
                request_type="grpc",
                name=name,
                response_time=elapsed_nanos,
                response_length=response_length,
                response=response,
                context={},
                exception=exception,
            )

        return response

    @task(0)
    def index(self):
        logging.info("User listing products via gRPC")
        self._grpc_unary(
            "ProductCatalog/ListProducts",
            self.product_catalog_stub.ListProducts,
            demo_pb2.Empty(),
        )

    @task(0)
    def browse_product(self):
        product = random.choice(products)
        logging.info("User browsing product via gRPC: %s", product)
        self._grpc_unary(
            "ProductCatalog/GetProduct",
            self.product_catalog_stub.GetProduct,
            demo_pb2.GetProductRequest(id=product),
        )

    @task(0)
    def get_recommendations(self):
        product = random.choice(products)
        logging.info("User getting recommendations via gRPC for product: %s", product)
        self._grpc_unary(
            "Recommendation/ListRecommendations",
            self.recommendation_stub.ListRecommendations,
            demo_pb2.ListRecommendationsRequest(
                user_id=self.user_id,
                product_ids=[product],
            ),
        )

    @task(0)
    def get_ads(self):
        category = random.choice(categories)
        logging.info("User getting ads via gRPC for category: %s", category)
        context_keys = [category] if category else []
        self._grpc_unary(
            "Ad/GetAds",
            self.ad_stub.GetAds,
            demo_pb2.AdRequest(context_keys=context_keys),
        )

    @task(3)
    def view_cart(self):
        logging.info("User viewing cart via gRPC")
        self._grpc_unary(
            "Cart/GetCart",
            self.cart_stub.GetCart,
            demo_pb2.GetCartRequest(user_id=self.user_id),
        )

    @task(2)
    def add_to_cart(self, user: str = ""):
        user_id = user or self.user_id
        product = random.choice(products)
        quantity = random.choice([1, 2, 3, 4, 5, 10])
        logging.info("User %s adding %s of product %s via gRPC", user_id, quantity, product)

        self._grpc_unary(
            "Cart/AddItem",
            self.cart_stub.AddItem,
            demo_pb2.AddItemRequest(
                user_id=user_id,
                item=demo_pb2.CartItem(product_id=product, quantity=quantity),
            ),
        )

    def _place_order(self, user: str):
        checkout_person = random.choice(people)
        address = checkout_person["address"]
        credit_card = checkout_person["creditCard"]

        self._grpc_unary(
            "Checkout/PlaceOrder",
            self.checkout_stub.PlaceOrder,
            demo_pb2.PlaceOrderRequest(
                user_id=user,
                user_currency=checkout_person["userCurrency"],
                email=checkout_person["email"],
                address=demo_pb2.Address(
                    street_address=address["streetAddress"],
                    city=address["city"],
                    state=address["state"],
                    country=address["country"],
                    zip_code=address["zipCode"],
                ),
                credit_card=demo_pb2.CreditCardInfo(
                    credit_card_number=credit_card["creditCardNumber"],
                    credit_card_cvv=credit_card["creditCardCvv"],
                    credit_card_expiration_year=credit_card["creditCardExpirationYear"],
                    credit_card_expiration_month=credit_card["creditCardExpirationMonth"],
                ),
            ),
        )

    @task(1)
    def checkout(self):
        user = str(uuid.uuid1())
        self.add_to_cart(user=user)
        self._place_order(user)
        logging.info("Checkout completed via gRPC for user %s", user)

    @task(1)
    def checkout_multi(self):
        user = str(uuid.uuid1())
        item_count = random.choice([2, 3, 4])
        for _ in range(item_count):
            self.add_to_cart(user=user)
        self._place_order(user)
        logging.info("Multi-item checkout completed via gRPC for user %s", user)

    @task(0)
    def flood_home(self):
        flood_count = get_flagd_value("loadGeneratorFloodHomepage")
        if flood_count > 0:
            logging.info("User flooding product list %s times via gRPC", flood_count)
            for _ in range(flood_count):
                self._grpc_unary(
                    "ProductCatalog/ListProducts",
                    self.product_catalog_stub.ListProducts,
                    demo_pb2.Empty(),
                )

    def on_start(self):
        self._build_channels_and_stubs()

        self.session_id = str(uuid.uuid4())
        self.user_id = str(uuid.uuid1())

        ctx = baggage.set_baggage("session.id", self.session_id)
        ctx = baggage.set_baggage("synthetic_request", "true", context=ctx)
        context.attach(ctx)

        logging.info("Starting gRPC user session: %s", self.session_id)
        self.view_cart()

    def on_stop(self):
        for channel in (
            self.catalog_channel,
            self.recommendation_channel,
            self.ad_channel,
            self.cart_channel,
            self.checkout_channel,
        ):
            if channel is not None:
                channel.close()
