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
from web_service.router import calculate_readiness


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
            alice_order.device_name = "Устройство Алисы"
            bob_order = self.make_order("Заказ Бориса", bob.id, "order-bob")
            external_order = self.make_order("Заказ внешнего", None, "order-external")
            external_order.performer_name = "Внешний исполнитель"
            unassigned_order = self.make_order("Заказ без исполнителя", None, "order-unassigned")
            db.add_all([alice_order, bob_order, external_order, unassigned_order])
            db.flush()
            self.alice_order_id = alice_order.id
            self.bob_order_id = bob_order.id
            self.unassigned_order_id = unassigned_order.id
            alice_item = OrderItem(
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
            alice_second_item = OrderItem(
                order_id=alice_order.id,
                moysklad_position_id="position-alice-2",
                assortment_id="item-alice-2",
                assortment_type="product",
                assortment_name="Крепёж",
                assortment_code="A-02",
                quantity=Decimal("8"),
                reserve=Decimal("0"),
                raw_payload={},
            )
            bob_item = OrderItem(
                order_id=bob_order.id,
                moysklad_position_id="position-bob",
                assortment_id="item-bob",
                assortment_type="product",
                assortment_name="Изделие Бориса",
                assortment_code="B-01",
                quantity=Decimal("4"),
                reserve=Decimal("0"),
                raw_payload={},
            )
            db.add_all([alice_item, alice_second_item, bob_item])
            db.flush()
            self.alice_item_id = alice_item.id
            self.bob_item_id = bob_item.id

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
            produced_quantity=Decimal("0"),
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

    def test_readiness_calculation_handles_zero_partial_complete_and_overrun(self):
        zero = calculate_readiness(Decimal("5"), Decimal("0"))
        partial = calculate_readiness(Decimal("1"), Decimal("2"))
        complete = calculate_readiness(Decimal("2"), Decimal("2"))
        overrun = calculate_readiness(Decimal("3"), Decimal("2"))

        self.assertEqual((zero.label, zero.width, zero.complete), ("0%", "0", False))
        self.assertEqual(
            (partial.label, partial.width, partial.complete),
            ("50%", "50", False),
        )
        self.assertEqual(
            (complete.label, complete.width, complete.complete),
            ("100%", "100", True),
        )
        self.assertEqual(
            (overrun.label, overrun.width, overrun.complete),
            ("150%", "100", True),
        )

    def test_guest_is_redirected_to_login(self):
        response = self.client.get("/cabinet/orders", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/cabinet/login")

    def test_regular_user_sees_only_own_orders_and_items(self):
        response = self.login("Алиса", "alice-password")
        self.assertEqual(response.status_code, 303)

        order_list = self.client.get("/cabinet/orders")
        self.assertEqual(order_list.status_code, 200)
        self.assertNotIn("<th>Исполнитель</th>", order_list.text)
        self.assertRegex(
            order_list.text,
            r"<th>Заказ</th>\s*<th>Устройство</th>",
        )
        self.assertIn("<th>Готовность</th>", order_list.text)
        self.assertIn("Устройство Алисы", order_list.text)
        self.assertIn("Заказ Алисы", order_list.text)
        self.assertNotIn("Заказ Бориса", order_list.text)

        detail = self.client.get(f"/cabinet/orders/{self.alice_order_id}")
        self.assertEqual(detail.status_code, 200)
        self.assertIn("Корпус изделия", detail.text)
        self.assertIn("<dt>Устройство</dt><dd>Устройство Алисы</dd>", detail.text)
        self.assertIn("<dt>Произведено</dt>", detail.text)
        self.assertIn("<th class=\"numeric\">Затрачено</th>", detail.text)
        self.assertNotIn("<th class=\"numeric\">Факт</th>", detail.text)
        self.assertNotIn("<th>Готовность</th>", detail.text)
        self.assertIn("Сохранить", detail.text)
        forbidden = self.client.get(f"/cabinet/orders/{self.bob_order_id}")
        self.assertEqual(forbidden.status_code, 404)

    def test_admin_sees_all_orders_and_can_filter_by_user(self):
        self.login("Администратор", "admin-password")
        order_list = self.client.get("/cabinet/orders")
        self.assertRegex(
            order_list.text,
            r"<th>Заказ</th>\s*<th>Устройство</th>\s*<th>Исполнитель</th>",
        )
        self.assertIn("<th>Готовность</th>", order_list.text)
        self.assertIn('data-label="Устройство">—</td>', order_list.text)
        self.assertIn("Заказ Алисы", order_list.text)
        self.assertIn("Заказ Бориса", order_list.text)
        self.assertIn("Внешний исполнитель", order_list.text)
        self.assertIn("Не назначен", order_list.text)

        filtered = self.client.get(f"/cabinet/orders?user_id={self.alice_id}")
        self.assertIn("Заказ Алисы", filtered.text)
        self.assertNotIn("Заказ Бориса", filtered.text)

        unfiltered = self.client.get("/cabinet/orders?user_id=")
        self.assertEqual(unfiltered.status_code, 200)
        self.assertIn("Заказ Алисы", unfiltered.text)
        self.assertIn("Заказ Бориса", unfiltered.text)
        self.assertEqual(
            self.client.get("/cabinet/orders?user_id=invalid").status_code,
            422,
        )
        self.assertEqual(self.client.get("/cabinet/orders?user_id=0").status_code, 422)

    def test_user_saves_produced_and_partial_spent_quantities(self):
        self.login("Алиса", "alice-password")
        csrf_token = self.session_csrf()
        response = self.client.post(
            f"/cabinet/orders/{self.alice_order_id}/production",
            data={
                "position_id": "position-alice",
                "spent_quantity": "1",
                "produced_quantity": "1",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            response.headers["location"],
            f"/cabinet/orders/{self.alice_order_id}?saved=1",
        )
        with self.Session() as db:
            items = {
                item.moysklad_position_id: item
                for item in db.scalars(
                    select(OrderItem).where(OrderItem.order_id == self.alice_order_id)
                )
            }
            order = db.get(MoySkladOrder, self.alice_order_id)
            self.assertEqual(order.produced_quantity, Decimal("1"))
            self.assertEqual(items["position-alice"].spent_quantity, Decimal("1"))
            self.assertEqual(items["position-alice-2"].spent_quantity, Decimal("0"))

        detail = self.client.get(f"/cabinet/orders/{self.alice_order_id}?saved=1")
        self.assertIn("Данные производства сохранены", detail.text)
        order_list = self.client.get("/cabinet/orders")
        self.assertIn("50%", order_list.text)

    def test_admin_can_save_production_for_any_order_and_order_without_items(self):
        self.login("Администратор", "admin-password")
        csrf_token = self.session_csrf()
        response = self.client.post(
            f"/cabinet/orders/{self.bob_order_id}/production",
            data={
                "position_id": "position-bob",
                "spent_quantity": "4",
                "produced_quantity": "4",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        order_list = self.client.get("/cabinet/orders")
        self.assertIn("200%", order_list.text)
        self.assertIn("readiness-track complete", order_list.text)

        no_items = self.client.post(
            f"/cabinet/orders/{self.unassigned_order_id}/production",
            data={
                "produced_quantity": "7",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(no_items.status_code, 303)
        with self.Session() as db:
            self.assertEqual(
                db.get(MoySkladOrder, self.unassigned_order_id).produced_quantity,
                Decimal("7"),
            )

    def test_production_quantity_validation_access_and_stale_position(self):
        self.login("Алиса", "alice-password")
        csrf_token = self.session_csrf()
        url = f"/cabinet/orders/{self.alice_order_id}/production"
        for invalid in ("", "-1", "1.5", "abc", "1000000000000", "9" * 5000):
            with self.subTest(invalid=invalid):
                response = self.client.post(
                    url,
                    data={
                        "position_id": "position-alice",
                        "spent_quantity": "0",
                        "produced_quantity": invalid,
                        "csrf_token": csrf_token,
                    },
                )
                self.assertEqual(response.status_code, 400)

        for invalid in ("", "-1", "1.5", "abc", "3", "1000000000000"):
            with self.subTest(invalid_spent=invalid):
                response = self.client.post(
                    url,
                    data={
                        "position_id": "position-alice",
                        "spent_quantity": invalid,
                        "produced_quantity": "10",
                        "csrf_token": csrf_token,
                    },
                )
                self.assertEqual(response.status_code, 400)

        duplicate = self.client.post(
            url,
            data={
                "position_id": ["position-alice", "position-alice"],
                "spent_quantity": ["1", "2"],
                "produced_quantity": "1",
                "csrf_token": csrf_token,
            },
        )
        self.assertEqual(duplicate.status_code, 400)
        mismatched = self.client.post(
            url,
            data={
                "position_id": ["position-alice", "position-alice-2"],
                "spent_quantity": "1",
                "produced_quantity": "1",
                "csrf_token": csrf_token,
            },
        )
        self.assertEqual(mismatched.status_code, 400)
        stale = self.client.post(
            url,
            data={
                "position_id": "removed-position",
                "spent_quantity": "1",
                "produced_quantity": "1",
                "csrf_token": csrf_token,
            },
        )
        self.assertEqual(stale.status_code, 409)
        invalid_csrf = self.client.post(
            url,
            data={
                "position_id": "position-alice",
                "spent_quantity": "1",
                "produced_quantity": "1",
                "csrf_token": "invalid",
            },
        )
        self.assertEqual(invalid_csrf.status_code, 400)
        foreign_order = self.client.post(
            f"/cabinet/orders/{self.bob_order_id}/production",
            data={
                "position_id": "position-bob",
                "spent_quantity": "1",
                "produced_quantity": "1",
                "csrf_token": csrf_token,
            },
        )
        self.assertEqual(foreign_order.status_code, 404)
        with self.Session() as db:
            self.assertEqual(
                db.get(OrderItem, self.alice_item_id).spent_quantity,
                Decimal("0"),
            )
            self.assertEqual(
                db.get(MoySkladOrder, self.alice_order_id).produced_quantity,
                Decimal("0"),
            )

        zero = self.client.post(
            url,
            data={
                "position_id": "position-alice",
                "spent_quantity": "0",
                "produced_quantity": "0",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(zero.status_code, 303)
        overproduction = self.client.post(
            url,
            data={
                "position_id": "position-alice",
                "spent_quantity": "2",
                "produced_quantity": "999999999999",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(overproduction.status_code, 303)

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
                "password": "x",
                "password_confirmation": "x",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        with self.Session() as db:
            user = db.scalar(select(User).where(User.name == "Новый исполнитель"))
            self.assertIsNotNone(user)
            self.assertNotEqual(user.password_hash, "x")
            self.assertTrue(verify_password("x", user.password_hash))
            self.assertFalse(user.is_admin)
            self.assertTrue(user.is_active)
        empty_password = self.client.post(
            "/cabinet/admin/users",
            data={
                "name": "Без пароля",
                "password": "",
                "password_confirmation": "",
                "csrf_token": csrf_token,
            },
        )
        self.assertEqual(empty_password.status_code, 400)

    def test_admin_changes_user_password_without_ending_existing_session(self):
        user_client = TestClient(self.app)
        old_password_client = TestClient(self.app)
        new_password_client = TestClient(self.app)
        try:
            self.login("Алиса", "alice-password", client=user_client)
            self.login("Администратор", "admin-password")
            csrf_token = self.session_csrf()
            user_list = self.client.get("/cabinet/admin/users")
            self.assertIn(
                f'/cabinet/admin/users/{self.alice_id}/password',
                user_list.text,
            )
            password_page = self.client.get(
                f"/cabinet/admin/users/{self.alice_id}/password"
            )
            self.assertEqual(password_page.status_code, 200)
            self.assertIn("Алиса", password_page.text)

            response = self.client.post(
                f"/cabinet/admin/users/{self.alice_id}/password",
                data={
                    "password": "z",
                    "password_confirmation": "z",
                    "csrf_token": csrf_token,
                },
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)
            self.assertEqual(
                response.headers["location"],
                "/cabinet/admin/users?password_changed=1",
            )
            self.assertEqual(user_client.get("/cabinet/orders").status_code, 200)
            self.assertEqual(
                self.login(
                    "Алиса",
                    "alice-password",
                    client=old_password_client,
                ).status_code,
                401,
            )
            self.assertEqual(
                self.login("Алиса", "z", client=new_password_client).status_code,
                303,
            )
        finally:
            user_client.close()
            old_password_client.close()
            new_password_client.close()

    def test_admin_can_change_own_password_and_keep_session(self):
        self.login("Администратор", "admin-password")
        csrf_token = self.session_csrf()
        response = self.client.post(
            f"/cabinet/admin/users/{self.admin_id}/password",
            data={
                "password": "a",
                "password_confirmation": "a",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(self.client.get("/cabinet/orders").status_code, 200)
        fresh_client = TestClient(self.app)
        try:
            self.assertEqual(
                self.login("Администратор", "a", client=fresh_client).status_code,
                303,
            )
        finally:
            fresh_client.close()

    def test_change_password_validation_and_access_control(self):
        self.login("Администратор", "admin-password")
        csrf_token = self.session_csrf()
        mismatch = self.client.post(
            f"/cabinet/admin/users/{self.alice_id}/password",
            data={
                "password": "a",
                "password_confirmation": "b",
                "csrf_token": csrf_token,
            },
        )
        self.assertEqual(mismatch.status_code, 400)
        empty = self.client.post(
            f"/cabinet/admin/users/{self.alice_id}/password",
            data={
                "password": "",
                "password_confirmation": "",
                "csrf_token": csrf_token,
            },
        )
        self.assertEqual(empty.status_code, 400)
        too_long = self.client.post(
            f"/cabinet/admin/users/{self.alice_id}/password",
            data={
                "password": "a" * 129,
                "password_confirmation": "a" * 129,
                "csrf_token": csrf_token,
            },
        )
        self.assertEqual(too_long.status_code, 400)
        invalid_csrf = self.client.post(
            f"/cabinet/admin/users/{self.alice_id}/password",
            data={
                "password": "a",
                "password_confirmation": "a",
                "csrf_token": "invalid",
            },
        )
        self.assertEqual(invalid_csrf.status_code, 400)
        missing_user = self.client.get("/cabinet/admin/users/99999/password")
        self.assertEqual(missing_user.status_code, 404)
        missing_user_post = self.client.post(
            "/cabinet/admin/users/99999/password",
            data={
                "password": "a",
                "password_confirmation": "a",
                "csrf_token": csrf_token,
            },
        )
        self.assertEqual(missing_user_post.status_code, 404)

        user_client = TestClient(self.app)
        try:
            self.login("Алиса", "alice-password", client=user_client)
            forbidden = user_client.get(
                f"/cabinet/admin/users/{self.bob_id}/password"
            )
            self.assertEqual(forbidden.status_code, 403)
            user_csrf = self.session_csrf(client=user_client)
            forbidden_post = user_client.post(
                f"/cabinet/admin/users/{self.bob_id}/password",
                data={
                    "password": "a",
                    "password_confirmation": "a",
                    "csrf_token": user_csrf,
                },
            )
            self.assertEqual(forbidden_post.status_code, 403)
        finally:
            user_client.close()

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
