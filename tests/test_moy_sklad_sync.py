import tempfile
import unittest
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from models import Base, MoySkladOrder, OrderItem, User
from services.moy_sklad_sync import (
    MoySkladDataError,
    MoySkladWebhookPayloadError,
    extract_performer_name,
    processing_order_hrefs,
    sync_processing_order,
)


def make_order_payload(
    *,
    updated: str,
    performer=None,
    name: str = "Order 1",
):
    attributes = []
    if performer is not None:
        attributes.append({"name": "Исполнитель", "value": performer})
    return {
        "id": "order-id",
        "name": name,
        "code": "code-1",
        "externalCode": "external-1",
        "description": "Description",
        "moment": "2026-07-17 10:00:00.000",
        "deliveryPlannedMoment": "2026-07-20 10:00:00.000",
        "created": "2026-07-17 09:00:00.000",
        "updated": updated,
        "applicable": True,
        "quantity": 3.5,
        "attributes": attributes,
        "state": {
            "id": "state-id",
            "name": "New",
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/"
                "entity/processingorder/metadata/states/state-id"
            },
        },
    }


def make_position(position_id: str, quantity: float, name: str):
    return {
        "id": position_id,
        "quantity": quantity,
        "reserve": 1,
        "assortment": {
            "id": f"product-{position_id}",
            "name": name,
            "code": f"code-{position_id}",
            "meta": {
                "href": "https://api.moysklad.ru/api/remap/1.2/"
                f"entity/product/product-{position_id}",
                "type": "product",
            },
        },
    }


class MoySkladSyncTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = Path(self.temp_dir.name) / "test.db"
        self.engine = create_engine(
            f"sqlite:///{database_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )

    def tearDown(self):
        self.engine.dispose()
        self.temp_dir.cleanup()

    def test_extracts_string_and_dictionary_performer(self):
        string_payload = make_order_payload(
            updated="2026-07-17 10:00:00.000",
            performer="Alice",
        )
        dictionary_payload = make_order_payload(
            updated="2026-07-17 10:00:00.000",
            performer={"name": "Bob"},
        )

        self.assertEqual(extract_performer_name(string_payload), "Alice")
        self.assertEqual(extract_performer_name(dictionary_payload), "Bob")

    def test_deduplicates_relevant_webhook_hrefs(self):
        event = {
            "action": "UPDATE",
            "meta": {
                "type": "processingorder",
                "href": "https://api.moysklad.ru/api/remap/1.2/"
                "entity/processingorder/order-id",
            },
        }

        self.assertEqual(
            processing_order_hrefs({"events": [event, event]}),
            [event["meta"]["href"]],
        )
        with self.assertRaises(MoySkladWebhookPayloadError):
            processing_order_hrefs({})

    def test_creates_updates_and_ignores_stale_order(self):
        with self.Session.begin() as session:
            session.add(
                User(
                    name="Alice",
                    password_hash="hashed-password",
                    is_admin=False,
                )
            )

        created = sync_processing_order(
            self.Session,
            make_order_payload(
                updated="2026-07-17 10:00:00.000",
                performer={"name": "Alice"},
            ),
            [
                make_position("position-1", 2, "Product 1"),
                make_position("position-2", 4, "Product 2"),
            ],
        )

        self.assertTrue(created.created)
        self.assertEqual(created.item_count, 2)
        self.assertIsNotNone(created.user_id)

        with self.assertLogs("services.moy_sklad_sync", level="WARNING"):
            updated = sync_processing_order(
                self.Session,
                make_order_payload(
                    updated="2026-07-17 11:00:00.000",
                    performer="alice",
                    name="Updated order",
                ),
                [make_position("position-2", 7, "Updated product")],
            )

        self.assertFalse(updated.created)
        self.assertIsNone(updated.user_id)
        with self.Session() as session:
            order = session.scalar(select(MoySkladOrder))
            items = list(session.scalars(select(OrderItem)))
            self.assertEqual(order.name, "Updated order")
            self.assertEqual(order.performer_name, "alice")
            self.assertIsNone(order.user_id)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].moysklad_position_id, "position-2")
            self.assertEqual(items[0].assortment_name, "Updated product")
            self.assertEqual(float(items[0].quantity), 7)

        stale = sync_processing_order(
            self.Session,
            make_order_payload(
                updated="2026-07-17 10:30:00.000",
                performer="Alice",
                name="Stale order",
            ),
            [make_position("position-3", 1, "Stale product")],
        )

        self.assertTrue(stale.stale)
        with self.Session() as session:
            order = session.scalar(select(MoySkladOrder))
            items = list(session.scalars(select(OrderItem)))
            self.assertEqual(order.name, "Updated order")
            self.assertEqual([item.moysklad_position_id for item in items], ["position-2"])

    def test_rolls_back_order_when_positions_are_invalid(self):
        duplicate = make_position("position-1", 2, "Product 1")

        with self.assertRaises(MoySkladDataError):
            sync_processing_order(
                self.Session,
                make_order_payload(
                    updated="2026-07-17 10:00:00.000",
                    performer=None,
                ),
                [duplicate, duplicate],
            )

        with self.Session() as session:
            self.assertIsNone(session.scalar(select(MoySkladOrder)))
            self.assertIsNone(session.scalar(select(OrderItem)))
