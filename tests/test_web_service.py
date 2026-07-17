import re
import unittest
from datetime import datetime
from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from models import Base, MoySkladOrder, OrderItem, User
from web_service import create_web_router
from web_service.auth import hash_password, verify_password


class WebServiceTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.app = FastAPI()
        self.app.include_router(
            create_web_router(
                self.Session,
                session_secret="test-session-secret",
                cookie_secure=False,
            )
        )
        self.client = TestClient(self.app)

        with self.Session.begin() as db:
            admin = User(
                name="Администратор",
                password_hash=hash_password("admin-password"),
                is_admin=True,
                is_active=True,
            )
            alice = User(
                name="Алиса",
                password_hash=hash_password("alice-password"),
                is_admin=False,
                is_active=True,
            )
            bob = User(
                name="Борис",
                password_hash=hash_password("bob-password"),
                is_admin=False,
                is_active=True,
            )
            db.add_all([admin, alice, bob])
            db.flush()
            self.admin_id = admin.id
            self.alice_id = alice.id
            self.bob_id = bob.id
            alice_order = self.make_order("Заказ Алисы", alice.id, "order-alice")
            bob_order = self.make_order("Заказ Бориса", bob.id, "order-bob")
            db.add_all([alice_order, bob_order])
            db.flush()
            self.alice_order_id = alice_order.id
            self.bob_order_id = bob_order.id
            db.add(
                OrderItem(
                    order_id=alice_order.id,
                    moysklad_position_id="position-alice",
                    assortment_id="item-alice",
                    assortment_type="product",
                    assortment_name="Корпус изделия",
                    assortment_code="A-01",
                    quantity=Decimal("2"),
                    reserve=Decimal("1"),
                    raw_payload={},
                )
            )

    def tearDown(self):
        self.client.close()
        self.engine.dispose()

    @staticmethod
    def make_order(name, user_id, moysklad_id):
        now = datetime(2026, 7, 17, 12, 0)
        return MoySkladOrder(
            moysklad_id=moysklad_id,
            user_id=user_id,
            name=name,
            code=None,
            external_code=None,
            description="Описание заказа",
            moment=now,
            delivery_planned_moment=now,
            moysklad_created_at=now,
            moysklad_updated_at=now,
            applicable=True,
            production_quantity=Decimal("2"),
            performer_name=None,
            state_id="state-id",
            state_name="В производстве",
            raw_payload={},
            synced_at=now,
        )

    @staticmethod
    def csrf_from(response):
        match = re.search(r'name="csrf_token" value="([^"]+)"', response.text)
        if match is None:
            raise AssertionError("CSRF token not found")
        return match.group(1)

    def login(self, name, password, *, client=None):
        client = client or self.client
        page = client.get("/cabinet/login")
        token = self.csrf_from(page)
        return client.post(
            "/cabinet/login",
            data={"name": name, "password": password, "csrf_token": token},
            follow_redirects=False,
        )

    def session_csrf(self, *, client=None):
        client = client or self.client
        response = client.get("/cabinet/orders")
        return self.csrf_from(response)

    def test_password_hash_is_salted_and_verifiable(self):
        first = hash_password("correct horse battery staple")
        second = hash_password("correct horse battery staple")
        self.assertNotEqual(first, second)
        self.assertTrue(verify_password("correct horse battery staple", first))
        self.assertFalse(verify_password("wrong", first))
        self.assertFalse(verify_password("password", "not-a-valid-hash"))

    def test_guest_is_redirected_to_login(self):
        response = self.client.get("/cabinet/orders", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/cabinet/login")

    def test_regular_user_sees_only_own_orders_and_items(self):
        response = self.login("Алиса", "alice-password")
        self.assertEqual(response.status_code, 303)

        order_list = self.client.get("/cabinet/orders")
        self.assertEqual(order_list.status_code, 200)
        self.assertIn("Заказ Алисы", order_list.text)
        self.assertNotIn("Заказ Бориса", order_list.text)

        detail = self.client.get(f"/cabinet/orders/{self.alice_order_id}")
        self.assertEqual(detail.status_code, 200)
        self.assertIn("Корпус изделия", detail.text)
        forbidden = self.client.get(f"/cabinet/orders/{self.bob_order_id}")
        self.assertEqual(forbidden.status_code, 404)

    def test_admin_sees_all_orders_and_can_filter_by_user(self):
        self.login("Администратор", "admin-password")
        order_list = self.client.get("/cabinet/orders")
        self.assertIn("Заказ Алисы", order_list.text)
        self.assertIn("Заказ Бориса", order_list.text)

        filtered = self.client.get(f"/cabinet/orders?user_id={self.alice_id}")
        self.assertIn("Заказ Алисы", filtered.text)
        self.assertNotIn("Заказ Бориса", filtered.text)

    def test_regular_user_cannot_open_user_administration(self):
        self.login("Алиса", "alice-password")
        response = self.client.get("/cabinet/admin/users")
        self.assertEqual(response.status_code, 403)

    def test_admin_creates_user_with_hashed_password(self):
        self.login("Администратор", "admin-password")
        csrf_token = self.session_csrf()
        response = self.client.post(
            "/cabinet/admin/users",
            data={
                "name": "Новый исполнитель",
                "password": "new-password",
                "password_confirmation": "new-password",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            user = db.scalar(select(User).where(User.name == "Новый исполнитель"))
            self.assertIsNotNone(user)
            self.assertNotEqual(user.password_hash, "new-password")
            self.assertTrue(verify_password("new-password", user.password_hash))
            self.assertFalse(user.is_admin)
            self.assertTrue(user.is_active)

    def test_disabling_user_invalidates_existing_session_and_keeps_orders(self):
        user_client = TestClient(self.app)
        try:
            self.login("Алиса", "alice-password", client=user_client)
            self.assertEqual(user_client.get("/cabinet/orders").status_code, 200)

            self.login("Администратор", "admin-password")
            csrf_token = self.session_csrf()
            response = self.client.post(
                f"/cabinet/admin/users/{self.alice_id}/disable",
                data={"csrf_token": csrf_token},
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)

            rejected = user_client.get("/cabinet/orders", follow_redirects=False)
            self.assertEqual(rejected.status_code, 303)
            self.assertEqual(rejected.headers["location"], "/cabinet/login")
            with self.Session() as db:
                self.assertFalse(db.get(User, self.alice_id).is_active)
                self.assertIsNotNone(db.get(MoySkladOrder, self.alice_order_id))
        finally:
            user_client.close()

    def test_admin_cannot_disable_itself_or_post_without_csrf(self):
        self.login("Администратор", "admin-password")
        no_csrf = self.client.post(
            f"/cabinet/admin/users/{self.bob_id}/disable",
            data={"csrf_token": "invalid"},
        )
        self.assertEqual(no_csrf.status_code, 400)

        csrf_token = self.session_csrf()
        self_disable = self.client.post(
            f"/cabinet/admin/users/{self.admin_id}/disable",
            data={"csrf_token": csrf_token},
        )
        self.assertEqual(self_disable.status_code, 400)


if __name__ == "__main__":
    unittest.main()
