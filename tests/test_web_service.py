import html
import re
import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from models import Base, MoySkladOrder, OrderItem, OrderSuborder, User
from web_service import create_web_router
from web_service.auth import hash_password, verify_password
from web_service.router import _order_status_class, calculate_readiness


class WebServiceTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(
            bind=self.engine,
            autoflush=False,
            expire_on_commit=False,
        )
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
            alice_order.code = "AL-42"
            alice_order.device_name = "Устройство Алисы"
            alice_order.processing_plan_name = "Техкарта Алисы"
            alice_order.state_name = "Готово"
            bob_order = self.make_order("Заказ Бориса", bob.id, "order-bob")
            bob_order.device_name = "Устройство Бориса"
            bob_order.processing_plan_name = "Техкарта Бориса"
            bob_order.state_name = "Планируется"
            external_order = self.make_order("Заказ внешнего", None, "order-external")
            external_order.performer_name = "Внешний исполнитель"
            unassigned_order = self.make_order("Заказ без исполнителя", None, "order-unassigned")
            unassigned_order.state_name = None
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

    def test_order_status_classification(self):
        expected = {
            "Готово": "ready",
            "В работе": "in-work",
            "К комплектовке": "picking",
            "Планируется": "planned",
            "Резерв": "reserve",
            "Ремонт": "repair",
            "Пауза": "paused",
            "Ожидание": "waiting",
        }
        for state_name, css_class in expected.items():
            with self.subTest(state_name=state_name):
                self.assertEqual(_order_status_class(state_name), css_class)
        self.assertEqual(_order_status_class("Неизвестный статус"), "neutral")
        self.assertEqual(_order_status_class(None), "neutral")

    def test_guest_is_redirected_to_login(self):
        response = self.client.get("/cabinet/orders", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/cabinet/login")

    def test_regular_user_sees_only_own_orders_and_items(self):
        response = self.login("Алиса", "alice-password")
        self.assertEqual(response.status_code, 303)

        order_list = self.client.get("/cabinet/orders")
        self.assertEqual(order_list.status_code, 200)
        self.assertNotIn("sort=performer", order_list.text)
        self.assertIn("sort=order&amp;direction=asc", order_list.text)
        self.assertIn("sort=processing_plan&amp;direction=asc", order_list.text)
        self.assertIn("sort=next_stage&amp;direction=asc", order_list.text)
        self.assertIn("sort=readiness&amp;direction=asc", order_list.text)
        self.assertIn("Устройство Алисы", order_list.text)
        self.assertIn("Техкарта Алисы", order_list.text)
        self.assertIn("Заказ Алисы", order_list.text)
        self.assertNotIn("Заказ Бориса", order_list.text)
        self.assertIn(
            'class="order-status order-status--ready"><span class="status-dot"></span>Готово',
            order_list.text,
        )

        detail = self.client.get(f"/cabinet/orders/{self.alice_order_id}")
        self.assertEqual(detail.status_code, 200)
        self.assertIn("Корпус изделия", detail.text)
        self.assertIn("<dt>Устройство</dt><dd>Устройство Алисы</dd>", detail.text)
        self.assertIn(
            "<dt>Технологическая карта</dt><dd>Техкарта Алисы</dd>",
            detail.text,
        )
        self.assertIn("<dt>Произведено</dt>", detail.text)
        self.assertIn("<th class=\"numeric\">Затрачено</th>", detail.text)
        self.assertNotIn("<th class=\"numeric\">Факт</th>", detail.text)
        self.assertNotIn("<th>Готовность</th>", detail.text)
        self.assertIn("Сохранить", detail.text)
        self.assertIn(
            'class="order-status order-status--ready"><span class="status-dot"></span>Готово',
            detail.text,
        )
        self.assertIn(
            'href="https://online.moysklad.ru/app/#processingorder/edit?id=order-alice" target="_blank" rel="noopener noreferrer"',
            detail.text,
        )
        self.assertIn("Открыть заказ в МоемСкладе", detail.text)
        forbidden = self.client.get(f"/cabinet/orders/{self.bob_order_id}")
        self.assertEqual(forbidden.status_code, 404)

    def test_admin_sees_all_orders_and_can_filter_by_user(self):
        self.login("Администратор", "admin-password")
        order_list = self.client.get("/cabinet/orders")
        self.assertIn("sort=order&amp;direction=asc", order_list.text)
        self.assertIn("sort=processing_plan&amp;direction=asc", order_list.text)
        self.assertIn("sort=performer&amp;direction=asc", order_list.text)
        self.assertIn("sort=next_stage&amp;direction=asc", order_list.text)
        self.assertIn("sort=readiness&amp;direction=asc", order_list.text)
        self.assertIn('data-label="Устройство">—</td>', order_list.text)
        self.assertIn(
            'data-label="Технологическая карта">—</td>',
            order_list.text,
        )
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

    def test_order_filters_cover_text_state_dates_and_access(self):
        with self.Session.begin() as db:
            alice = db.get(MoySkladOrder, self.alice_order_id)
            alice.last_suborder_number = 1
            db.add(
                OrderSuborder(
                    order_id=alice.id,
                    number=1,
                    planned_quantity=Decimal("2"),
                    actual_quantity=Decimal("0"),
                    planned_date=date(2026, 7, 17),
                )
            )
        self.login("Администратор", "admin-password")

        cases = [
            ("q=алис", "Заказ Алисы", "Заказ Бориса"),
            ("q=al-42", "Заказ Алисы", "Заказ Бориса"),
            ("device=АЛИСЫ", "Заказ Алисы", "Заказ Бориса"),
            ("processing_plan=карта+алисы", "Заказ Алисы", "Заказ Бориса"),
            ("state=Готово", "Заказ Алисы", "Заказ Бориса"),
            ("state=__none__", "Заказ без исполнителя", "Заказ Алисы"),
            (f"user_id={self.bob_id}", "Заказ Бориса", "Заказ Алисы"),
            (
                "moment_from=2026-07-17&moment_to=2026-07-17"
                "&next_stage_from=2026-07-17&next_stage_to=2026-07-17",
                "Заказ Алисы",
                None,
            ),
        ]
        for query, included, excluded in cases:
            with self.subTest(query=query):
                response = self.client.get(f"/cabinet/orders?{query}")
                self.assertEqual(response.status_code, 200)
                self.assertIn(included, response.text)
                if excluded is not None:
                    self.assertNotIn(excluded, response.text)

        combined = self.client.get(
            "/cabinet/orders?q=заказ&device=устройство&processing_plan=техкарта"
            f"&state=Готово&user_id={self.alice_id}"
            "&moment_from=2026-07-17&moment_to=2026-07-17"
        )
        self.assertIn("Заказ Алисы", combined.text)
        self.assertNotIn("Заказ Бориса", combined.text)

        empty = self.client.get("/cabinet/orders?q=несуществующий")
        self.assertIn("По заданным фильтрам совпадений нет", empty.text)
        self.assertIn("Сбросить фильтры", empty.text)
        wildcard = self.client.get("/cabinet/orders?q=%25")
        self.assertNotIn("Заказ Алисы", wildcard.text)

        regular_client = TestClient(self.app)
        try:
            self.login("Алиса", "alice-password", client=regular_client)
            hidden = regular_client.get("/cabinet/orders?q=Бориса")
            self.assertNotIn("Заказ Бориса", hidden.text)
            self.assertNotIn('name="user_id"', hidden.text)
        finally:
            regular_client.close()

    def test_order_filter_and_sort_validation(self):
        self.login("Администратор", "admin-password")
        browser_form = self.client.get(
            "/cabinet/orders?moment_from=2026-07-17&moment_to=2026-07-17"
            "&next_stage_from=&next_stage_to="
        )
        self.assertEqual(browser_form.status_code, 200)
        self.assertIn("Заказ Алисы", browser_form.text)
        all_empty = self.client.get(
            "/cabinet/orders?moment_from=&moment_to=&next_stage_from=&next_stage_to="
        )
        self.assertEqual(all_empty.status_code, 200)

        invalid_queries = [
            "sort=unknown",
            "direction=sideways",
            "moment_from=not-a-date",
            "moment_from=2026-07-18&moment_to=2026-07-17",
            "next_stage_from=2026-07-18&next_stage_to=2026-07-17",
        ]
        for query in invalid_queries:
            with self.subTest(query=query):
                self.assertEqual(
                    self.client.get(f"/cabinet/orders?{query}").status_code,
                    422,
                )

    def test_sorts_every_order_column_in_both_directions(self):
        with self.Session.begin() as db:
            alice = db.get(MoySkladOrder, self.alice_order_id)
            bob = db.get(MoySkladOrder, self.bob_order_id)
            unassigned = db.get(MoySkladOrder, self.unassigned_order_id)
            alice.name = "A order"
            alice.device_name = "A device"
            alice.processing_plan_name = "A plan"
            alice.state_name = "A state"
            alice.moment = datetime(2026, 7, 16, 12, 0)
            alice.delivery_planned_moment = datetime(2026, 7, 18, 12, 0)
            alice.production_quantity = Decimal("1")
            alice.produced_quantity = Decimal("0")
            bob.name = "B order"
            bob.device_name = "B device"
            bob.processing_plan_name = "B plan"
            bob.state_name = "B state"
            bob.moment = datetime(2026, 7, 17, 12, 0)
            bob.delivery_planned_moment = datetime(2026, 7, 19, 12, 0)
            bob.production_quantity = Decimal("2")
            bob.produced_quantity = Decimal("2")
            unassigned.production_quantity = Decimal("0")
            unassigned.produced_quantity = Decimal("5")
            alice.last_suborder_number = 1
            bob.last_suborder_number = 1
            db.add_all(
                [
                    OrderSuborder(
                        order_id=alice.id,
                        number=1,
                        planned_quantity=Decimal("1"),
                        actual_quantity=Decimal("0"),
                        planned_date=date(2026, 7, 18),
                    ),
                    OrderSuborder(
                        order_id=bob.id,
                        number=1,
                        planned_quantity=Decimal("2"),
                        actual_quantity=Decimal("0"),
                        planned_date=date(2026, 7, 19),
                    ),
                ]
            )

        self.login("Администратор", "admin-password")
        for key in (
            "order",
            "device",
            "processing_plan",
            "performer",
            "state",
            "moment",
            "next_stage",
            "quantity",
            "readiness",
        ):
            with self.subTest(sort=key, direction="asc"):
                ascending = self.client.get(
                    f"/cabinet/orders?sort={key}&direction=asc"
                )
                self.assertLess(
                    ascending.text.index("A order"),
                    ascending.text.index("B order"),
                )
            with self.subTest(sort=key, direction="desc"):
                descending = self.client.get(
                    f"/cabinet/orders?sort={key}&direction=desc"
                )
                self.assertLess(
                    descending.text.index("B order"),
                    descending.text.index("A order"),
                )

        nulls_last = self.client.get("/cabinet/orders?sort=device&direction=asc")
        self.assertGreater(
            nulls_last.text.index("Заказ внешнего"),
            nulls_last.text.index("B order"),
        )
        stable = self.client.get("/cabinet/orders?sort=moment&direction=asc")
        self.assertLess(
            stable.text.index("Заказ без исполнителя"),
            stable.text.index("Заказ внешнего"),
        )

    def test_filter_and_sort_parameters_survive_sorting_and_pagination(self):
        with self.Session.begin() as db:
            for index in range(26):
                order = self.make_order(
                    f"Страница {index:02d}",
                    self.alice_id,
                    f"page-order-{index}",
                )
                order.device_name = "Общее устройство"
                order.processing_plan_name = "Общая карта"
                order.state_name = "Планируется"
                order.moment += timedelta(minutes=index)
                db.add(order)

        self.login("Администратор", "admin-password")
        response = self.client.get(
            "/cabinet/orders?q=Страница&device=Общее&processing_plan=Общая"
            "&state=Планируется&sort=order&direction=asc"
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("1 из 2", response.text)

        next_match = re.search(r'href="([^"]+)">Далее', response.text)
        self.assertIsNotNone(next_match)
        next_params = parse_qs(urlsplit(html.unescape(next_match.group(1))).query)
        self.assertEqual(
            next_params,
            {
                "q": ["Страница"],
                "device": ["Общее"],
                "processing_plan": ["Общая"],
                "state": ["Планируется"],
                "sort": ["order"],
                "direction": ["asc"],
                "page": ["2"],
            },
        )

        sort_match = re.search(
            r'href="([^"]*sort=device[^"]*)"',
            response.text,
        )
        self.assertIsNotNone(sort_match)
        sort_params = parse_qs(urlsplit(html.unescape(sort_match.group(1))).query)
        self.assertEqual(sort_params["q"], ["Страница"])
        self.assertEqual(sort_params["processing_plan"], ["Общая"])
        self.assertEqual(sort_params["sort"], ["device"])
        self.assertEqual(sort_params["direction"], ["asc"])
        self.assertNotIn("page", sort_params)

    def test_order_list_shows_next_incomplete_stage_and_expands_rows(self):
        with self.Session.begin() as db:
            order = db.get(MoySkladOrder, self.alice_order_id)
            order.last_suborder_number = 2
            order.produced_quantity = Decimal("3")
            db.add_all(
                [
                    OrderSuborder(
                        order_id=order.id,
                        number=1,
                        planned_quantity=Decimal("2"),
                        actual_quantity=Decimal("2"),
                        planned_date=date(2026, 7, 18),
                    ),
                    OrderSuborder(
                        order_id=order.id,
                        number=2,
                        planned_quantity=Decimal("3"),
                        actual_quantity=Decimal("1"),
                        planned_date=date(2026, 7, 21),
                    ),
                ]
            )

        self.login("Алиса", "alice-password")
        order_list = self.client.get("/cabinet/orders")
        self.assertIn("Дата создания", order_list.text)
        self.assertIn("Дата следующего этапа", order_list.text)
        self.assertNotIn("План производства</th>", order_list.text)
        self.assertIn("21.07.2026", order_list.text)
        self.assertIn(f"expanded={self.alice_order_id}", order_list.text)
        self.assertNotIn("Этап 1", order_list.text)

        expanded = self.client.get(
            f"/cabinet/orders?expanded={self.alice_order_id}"
        )
        self.assertIn("Этап 1", expanded.text)
        self.assertIn("Этап 2", expanded.text)
        self.assertIn("100%", expanded.text)
        self.assertIn("33.3%", expanded.text)
        self.assertNotIn('name="planned_quantity" value="2"', expanded.text)
        self.assertIn(
            f'action="/cabinet/orders/{self.alice_order_id}/suborders/1/actual"',
            expanded.text,
        )

        completed = self.client.post(
            f"/cabinet/orders/{self.alice_order_id}/suborders/2/actual",
            data={
                "actual_quantity": "3",
                "return_url": f"/cabinet/orders?expanded={self.alice_order_id}",
                "csrf_token": self.session_csrf(),
            },
            follow_redirects=False,
        )
        self.assertEqual(completed.status_code, 303)
        refreshed = self.client.get("/cabinet/orders")
        self.assertIn('data-label="Дата следующего этапа">—</td>', refreshed.text)

    def test_admin_edits_stage_plan_date_and_actual_from_order_list(self):
        with self.Session.begin() as db:
            order = db.get(MoySkladOrder, self.alice_order_id)
            order.last_suborder_number = 1
            db.add(
                OrderSuborder(
                    order_id=order.id,
                    number=1,
                    planned_quantity=Decimal("2"),
                    actual_quantity=Decimal("0"),
                    planned_date=date(2026, 7, 20),
                )
            )

        self.login("Администратор", "admin-password")
        expanded_url = (
            f"/cabinet/orders?sort=order&direction=asc"
            f"&expanded={self.alice_order_id}"
        )
        expanded = self.client.get(expanded_url)
        self.assertIn('name="planned_quantity" value="2"', expanded.text)
        self.assertIn('name="planned_date" value="2026-07-20"', expanded.text)
        self.assertIn('name="actual_quantity" value="0"', expanded.text)

        response = self.client.post(
            f"/cabinet/orders/{self.alice_order_id}/suborders/1",
            data={
                "planned_quantity": "5",
                "actual_quantity": "2",
                "planned_date": "2026-07-24",
                "return_url": expanded_url,
                "csrf_token": self.session_csrf(),
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], expanded_url)
        with self.Session() as db:
            order = db.get(MoySkladOrder, self.alice_order_id)
            suborder = db.get(OrderSuborder, 1)
            self.assertEqual(order.production_quantity, Decimal("2"))
            self.assertEqual(order.produced_quantity, Decimal("2"))
            self.assertEqual(suborder.planned_quantity, Decimal("5"))
            self.assertEqual(suborder.actual_quantity, Decimal("2"))
            self.assertEqual(suborder.planned_date, date(2026, 7, 24))
        refreshed = self.client.get(expanded_url)
        self.assertIn("24.07.2026", refreshed.text)
        self.assertIn("40%", refreshed.text)

    def test_admin_splits_order_into_stages_and_preserves_actual(self):
        with self.Session.begin() as db:
            order = db.get(MoySkladOrder, self.alice_order_id)
            order.production_quantity = Decimal("10")
            order.produced_quantity = Decimal("5")

        self.login("Администратор", "admin-password")
        csrf_token = self.session_csrf()
        list_page = self.client.get("/cabinet/orders")
        self.assertIn("Разбить на этапы", list_page.text)
        return_url = (
            f"/cabinet/orders?sort=order&direction=asc"
            f"&expanded={self.alice_order_id}"
        )
        response = self.client.post(
            f"/cabinet/orders/{self.alice_order_id}/suborders/split",
            data={
                "stage_quantity": "3",
                "return_url": return_url,
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], return_url)
        with self.Session() as db:
            suborders = list(
                db.scalars(
                    select(OrderSuborder)
                    .where(OrderSuborder.order_id == self.alice_order_id)
                    .order_by(OrderSuborder.number)
                )
            )
            self.assertEqual(
                [item.planned_quantity for item in suborders],
                [Decimal("3"), Decimal("3"), Decimal("3"), Decimal("1")],
            )
            self.assertEqual(
                [item.actual_quantity for item in suborders],
                [Decimal("3"), Decimal("2"), Decimal("0"), Decimal("0")],
            )
            self.assertTrue(
                all(item.planned_date == date.today() for item in suborders)
            )
            order = db.get(MoySkladOrder, self.alice_order_id)
            self.assertEqual(order.produced_quantity, Decimal("5"))
            self.assertEqual(order.last_suborder_number, 4)

        duplicate = self.client.post(
            f"/cabinet/orders/{self.alice_order_id}/suborders/split",
            data={"stage_quantity": "3", "csrf_token": csrf_token},
        )
        self.assertEqual(duplicate.status_code, 409)

    def test_split_stages_handles_overproduction_limits_and_access(self):
        self.login("Администратор", "admin-password")
        csrf_token = self.session_csrf()
        split_url = f"/cabinet/orders/{self.bob_order_id}/suborders/split"
        with self.Session.begin() as db:
            bob = db.get(MoySkladOrder, self.bob_order_id)
            bob.production_quantity = Decimal("2")
            bob.produced_quantity = Decimal("5")

        overproduced = self.client.post(
            split_url,
            data={"stage_quantity": "3", "csrf_token": csrf_token},
            follow_redirects=False,
        )
        self.assertEqual(overproduced.status_code, 303)
        self.assertEqual(
            overproduced.headers["location"],
            f"/cabinet/orders?expanded={self.bob_order_id}",
        )
        with self.Session() as db:
            suborder = db.scalar(
                select(OrderSuborder).where(
                    OrderSuborder.order_id == self.bob_order_id
                )
            )
            self.assertEqual(suborder.planned_quantity, Decimal("2"))
            self.assertEqual(suborder.actual_quantity, Decimal("5"))

        with self.Session.begin() as db:
            unassigned = db.get(MoySkladOrder, self.unassigned_order_id)
            unassigned.production_quantity = Decimal("1001")
        limited_url = (
            f"/cabinet/orders/{self.unassigned_order_id}/suborders/split"
        )
        self.assertEqual(
            self.client.post(
                limited_url,
                data={"stage_quantity": "1", "csrf_token": csrf_token},
            ).status_code,
            400,
        )
        for invalid in ("", "0", "-1", "1.5"):
            with self.subTest(invalid=invalid):
                self.assertEqual(
                    self.client.post(
                        limited_url,
                        data={"stage_quantity": invalid, "csrf_token": csrf_token},
                    ).status_code,
                    400,
                )
        safe_redirect = self.client.post(
            limited_url,
            data={
                "stage_quantity": "1001",
                "return_url": "https://example.com/redirect",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(safe_redirect.status_code, 303)
        self.assertEqual(
            safe_redirect.headers["location"],
            f"/cabinet/orders?expanded={self.unassigned_order_id}",
        )
        self.assertEqual(
            self.client.post(
                limited_url,
                data={"stage_quantity": "2", "csrf_token": "invalid"},
            ).status_code,
            400,
        )

        regular_client = TestClient(self.app)
        try:
            self.login("Алиса", "alice-password", client=regular_client)
            user_csrf = self.session_csrf(client=regular_client)
            forbidden = regular_client.post(
                f"/cabinet/orders/{self.alice_order_id}/suborders/split",
                data={"stage_quantity": "1", "csrf_token": user_csrf},
            )
            self.assertEqual(forbidden.status_code, 403)
        finally:
            regular_client.close()

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

    def test_admin_manages_suborders_and_numbers_are_not_reused(self):
        self.login("Администратор", "admin-password")
        csrf_token = self.session_csrf()
        create_url = f"/cabinet/orders/{self.alice_order_id}/suborders"

        first = self.client.post(
            create_url,
            data={
                "planned_quantity": "3",
                "actual_quantity": "1",
                "planned_date": "2026-07-21",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        second = self.client.post(
            create_url,
            data={
                "planned_quantity": "7",
                "actual_quantity": "2",
                "planned_date": "2026-07-22",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(first.status_code, 303)
        self.assertEqual(second.status_code, 303)

        with self.Session() as db:
            suborders = list(
                db.scalars(
                    select(OrderSuborder)
                    .where(OrderSuborder.order_id == self.alice_order_id)
                    .order_by(OrderSuborder.number)
                )
            )
            self.assertEqual([item.number for item in suborders], [1, 2])
            self.assertEqual(
                db.get(MoySkladOrder, self.alice_order_id).produced_quantity,
                Decimal("3"),
            )
            first_id, second_id = suborders[0].id, suborders[1].id

        update = self.client.post(
            f"{create_url}/{first_id}",
            data={
                "planned_quantity": "4",
                "actual_quantity": "5",
                "planned_date": "2026-07-23",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(update.status_code, 303)
        with self.Session() as db:
            suborder = db.get(OrderSuborder, first_id)
            self.assertEqual(suborder.planned_quantity, Decimal("4"))
            self.assertEqual(suborder.actual_quantity, Decimal("5"))
            self.assertEqual(suborder.planned_date, date(2026, 7, 23))
            self.assertEqual(
                db.get(MoySkladOrder, self.alice_order_id).produced_quantity,
                Decimal("7"),
            )

        for suborder_id in (second_id, first_id):
            deleted = self.client.post(
                f"{create_url}/{suborder_id}/delete",
                data={"csrf_token": csrf_token},
                follow_redirects=False,
            )
            self.assertEqual(deleted.status_code, 303)
        with self.Session() as db:
            order = db.get(MoySkladOrder, self.alice_order_id)
            self.assertEqual(order.produced_quantity, Decimal("5"))
            self.assertEqual(order.last_suborder_number, 2)

        third = self.client.post(
            create_url,
            data={
                "planned_quantity": "1",
                "actual_quantity": "0",
                "planned_date": "2026-07-24",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(third.status_code, 303)
        with self.Session() as db:
            suborder = db.scalar(
                select(OrderSuborder).where(
                    OrderSuborder.order_id == self.alice_order_id
                )
            )
            self.assertEqual(suborder.number, 3)
            self.assertEqual(
                db.get(MoySkladOrder, self.alice_order_id).produced_quantity,
                Decimal("0"),
            )

    def test_assigned_user_updates_only_suborder_actual(self):
        with self.Session.begin() as db:
            order = db.get(MoySkladOrder, self.alice_order_id)
            order.last_suborder_number = 1
            order.produced_quantity = Decimal("1")
            suborder = OrderSuborder(
                order_id=order.id,
                number=1,
                planned_quantity=Decimal("3"),
                actual_quantity=Decimal("1"),
                planned_date=date(2026, 7, 25),
            )
            db.add(suborder)
            db.flush()
            suborder_id = suborder.id

        self.login("Алиса", "alice-password")
        csrf_token = self.session_csrf()
        detail = self.client.get(f"/cabinet/orders/{self.alice_order_id}")
        self.assertIn("Подзаказы", detail.text)
        self.assertIn("25.07.2026", detail.text)
        self.assertNotIn(
            f'action="/cabinet/orders/{self.alice_order_id}/suborders"',
            detail.text,
        )
        order_list_before = self.client.get("/cabinet/orders")
        self.assertIn("50%", order_list_before.text)

        actual = self.client.post(
            f"/cabinet/orders/{self.alice_order_id}/suborders/{suborder_id}/actual",
            data={"actual_quantity": "4", "csrf_token": csrf_token},
            follow_redirects=False,
        )
        self.assertEqual(actual.status_code, 303)
        with self.Session() as db:
            saved = db.get(OrderSuborder, suborder_id)
            self.assertEqual(saved.actual_quantity, Decimal("4"))
            self.assertEqual(saved.planned_quantity, Decimal("3"))
            self.assertEqual(
                db.get(MoySkladOrder, self.alice_order_id).produced_quantity,
                Decimal("4"),
            )
        order_list_after = self.client.get("/cabinet/orders")
        self.assertIn("200%", order_list_after.text)

        admin_update = self.client.post(
            f"/cabinet/orders/{self.alice_order_id}/suborders/{suborder_id}",
            data={
                "planned_quantity": "9",
                "actual_quantity": "9",
                "planned_date": "2026-08-01",
                "csrf_token": csrf_token,
            },
        )
        create = self.client.post(
            f"/cabinet/orders/{self.alice_order_id}/suborders",
            data={
                "planned_quantity": "1",
                "actual_quantity": "0",
                "planned_date": "2026-08-01",
                "csrf_token": csrf_token,
            },
        )
        delete = self.client.post(
            f"/cabinet/orders/{self.alice_order_id}/suborders/{suborder_id}/delete",
            data={"csrf_token": csrf_token},
        )
        self.assertEqual(admin_update.status_code, 403)
        self.assertEqual(create.status_code, 403)
        self.assertEqual(delete.status_code, 403)

    def test_suborder_validation_access_and_produced_quantity_is_derived(self):
        self.login("Администратор", "admin-password")
        csrf_token = self.session_csrf()
        create_url = f"/cabinet/orders/{self.alice_order_id}/suborders"
        valid_data = {
            "planned_quantity": "2",
            "actual_quantity": "6",
            "planned_date": "2026-07-25",
            "csrf_token": csrf_token,
        }
        for field, invalid, status_code in (
            ("planned_quantity", "-1", 400),
            ("actual_quantity", "1.5", 400),
            ("planned_date", "", 422),
            ("planned_date", "2026-02-30", 422),
            ("csrf_token", "invalid", 400),
        ):
            with self.subTest(field=field, invalid=invalid):
                data = {**valid_data, field: invalid}
                self.assertEqual(self.client.post(create_url, data=data).status_code, status_code)

        created = self.client.post(create_url, data=valid_data, follow_redirects=False)
        self.assertEqual(created.status_code, 303)
        forged_total = self.client.post(
            f"/cabinet/orders/{self.alice_order_id}/production",
            data={
                "position_id": ["position-alice", "position-alice-2"],
                "spent_quantity": ["0", "0"],
                "produced_quantity": "999",
                "csrf_token": csrf_token,
            },
            follow_redirects=False,
        )
        self.assertEqual(forged_total.status_code, 303)
        with self.Session() as db:
            self.assertEqual(
                db.get(MoySkladOrder, self.alice_order_id).produced_quantity,
                Decimal("6"),
            )

        regular_client = TestClient(self.app)
        try:
            self.login("Борис", "bob-password", client=regular_client)
            bob_csrf = self.session_csrf(client=regular_client)
            with self.Session() as db:
                suborder_id = db.scalar(
                    select(OrderSuborder.id).where(
                        OrderSuborder.order_id == self.alice_order_id
                    )
                )
            foreign = regular_client.post(
                f"/cabinet/orders/{self.alice_order_id}/suborders/{suborder_id}/actual",
                data={"actual_quantity": "1", "csrf_token": bob_csrf},
            )
            self.assertEqual(foreign.status_code, 404)
        finally:
            regular_client.close()

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
