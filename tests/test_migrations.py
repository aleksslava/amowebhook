import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select, text

from models import EducationVisit


class AlembicMigrationTests(unittest.TestCase):
    def alembic_config(self):
        config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
        config.attributes["configure_logger"] = False
        return config

    def test_upgrade_head_on_clean_database(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "clean.db"
            database_url = f"sqlite:///{database_path.as_posix()}"
            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.upgrade(self.alembic_config(), "head")
                command.check(self.alembic_config())
                command.downgrade(self.alembic_config(), "base")
                command.upgrade(self.alembic_config(), "head")

            engine = create_engine(database_url)
            self.assertEqual(
                set(inspect(engine).get_table_names()),
                {
                    "alembic_version",
                    "education_visits",
                    "users",
                    "orders",
                    "order_items",
                },
            )
            engine.dispose()

    def test_production_progress_normalizes_existing_values(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "production-progress.db"
            database_url = f"sqlite:///{database_path.as_posix()}"
            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.upgrade(self.alembic_config(), "0005_order_device")

            engine = create_engine(database_url)
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO orders "
                        "(id, moysklad_id, name, raw_payload, synced_at) "
                        "VALUES (1, 'progress-order', 'Progress order', '{}', "
                        "'2026-07-17 12:00:00')"
                    )
                )
                connection.execute(
                    text(
                        "INSERT INTO order_items "
                        "(order_id, moysklad_position_id, quantity, "
                        "actual_quantity, raw_payload) VALUES "
                        "(1, 'valid', 5, 2, '{}'), "
                        "(1, 'fractional', 5, 3.8, '{}'), "
                        "(1, 'over', 4.7, 7, '{}'), "
                        "(1, 'negative', 5, -2, '{}'), "
                        "(1, 'zero-plan', 0, 3, '{}')"
                    )
                )

            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.upgrade(self.alembic_config(), "head")

            order_columns = {
                column["name"] for column in inspect(engine).get_columns("orders")
            }
            item_columns = {
                column["name"]
                for column in inspect(engine).get_columns("order_items")
            }
            self.assertIn("produced_quantity", order_columns)
            self.assertIn("spent_quantity", item_columns)
            self.assertNotIn("actual_quantity", item_columns)
            with engine.connect() as connection:
                produced = connection.scalar(
                    text(
                        "SELECT produced_quantity FROM orders "
                        "WHERE moysklad_id = 'progress-order'"
                    )
                )
                spent = dict(
                    connection.execute(
                        text(
                            "SELECT moysklad_position_id, spent_quantity "
                            "FROM order_items"
                        )
                    ).all()
                )
            self.assertEqual(produced, 0)
            self.assertEqual(
                spent,
                {
                    "valid": 2,
                    "fractional": 3,
                    "over": 4,
                    "negative": 0,
                    "zero-plan": 0,
                },
            )

            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.downgrade(self.alembic_config(), "0005_order_device")
            order_columns = {
                column["name"] for column in inspect(engine).get_columns("orders")
            }
            item_columns = {
                column["name"]
                for column in inspect(engine).get_columns("order_items")
            }
            self.assertNotIn("produced_quantity", order_columns)
            self.assertIn("actual_quantity", item_columns)
            self.assertNotIn("spent_quantity", item_columns)
            engine.dispose()

    def test_stamp_preserves_existing_education_visits(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "existing.db"
            database_url = f"sqlite:///{database_path.as_posix()}"
            engine = create_engine(database_url)
            EducationVisit.__table__.create(engine)
            with engine.begin() as connection:
                connection.execute(
                    EducationVisit.__table__.insert(),
                    {"utm_source": "existing", "created_at": datetime.utcnow()},
                )

            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.stamp(self.alembic_config(), "0001_existing_schema")
                command.upgrade(self.alembic_config(), "head")

            with engine.connect() as connection:
                source = connection.scalar(
                    select(EducationVisit.utm_source).where(EducationVisit.id == 1)
                )
            self.assertEqual(source, "existing")
            self.assertIn("orders", inspect(engine).get_table_names())
            user_columns = {column["name"] for column in inspect(engine).get_columns("users")}
            self.assertIn("is_active", user_columns)
            engine.dispose()

    def test_actual_quantity_defaults_existing_items_to_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "existing-items.db"
            database_url = f"sqlite:///{database_path.as_posix()}"
            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.upgrade(self.alembic_config(), "0003_user_active")

            engine = create_engine(database_url)
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO order_items "
                        "(order_id, moysklad_position_id, quantity, raw_payload) "
                        "VALUES (1, 'legacy-position', 2, '{}')"
                    )
                )

            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.upgrade(self.alembic_config(), "0004_item_actual_quantity")
            with engine.connect() as connection:
                actual_quantity = connection.scalar(
                    text(
                        "SELECT actual_quantity FROM order_items "
                        "WHERE moysklad_position_id = 'legacy-position'"
                    )
                )
            self.assertEqual(actual_quantity, 0)
            self.assertIn(
                "actual_quantity",
                {
                    column["name"]
                    for column in inspect(engine).get_columns("order_items")
                },
            )
            engine.dispose()

    def test_device_name_preserves_existing_orders(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "existing-orders.db"
            database_url = f"sqlite:///{database_path.as_posix()}"
            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.upgrade(self.alembic_config(), "0004_item_actual_quantity")

            engine = create_engine(database_url)
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO orders "
                        "(moysklad_id, name, raw_payload, synced_at) "
                        "VALUES ('legacy-order', 'Legacy order', '{}', "
                        "'2026-07-17 12:00:00')"
                    )
                )

            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.upgrade(self.alembic_config(), "head")
            columns = {
                column["name"] for column in inspect(engine).get_columns("orders")
            }
            self.assertIn("device_name", columns)
            with engine.connect() as connection:
                row = connection.execute(
                    text(
                        "SELECT name, device_name FROM orders "
                        "WHERE moysklad_id = 'legacy-order'"
                    )
                ).one()
            self.assertEqual(row.name, "Legacy order")
            self.assertIsNone(row.device_name)

            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.downgrade(self.alembic_config(), "0004_item_actual_quantity")
            columns = {
                column["name"] for column in inspect(engine).get_columns("orders")
            }
            self.assertNotIn("device_name", columns)
            engine.dispose()

    def test_processing_plan_name_preserves_existing_orders(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "processing-plan.db"
            database_url = f"sqlite:///{database_path.as_posix()}"
            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.upgrade(self.alembic_config(), "0006_production_progress")

            engine = create_engine(database_url)
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO orders "
                        "(moysklad_id, name, raw_payload, synced_at) "
                        "VALUES ('legacy-order', 'Legacy order', '{}', "
                        "'2026-07-17 12:00:00')"
                    )
                )

            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.upgrade(self.alembic_config(), "head")
            columns = {
                column["name"] for column in inspect(engine).get_columns("orders")
            }
            self.assertIn("processing_plan_name", columns)
            with engine.connect() as connection:
                row = connection.execute(
                    text(
                        "SELECT name, processing_plan_name FROM orders "
                        "WHERE moysklad_id = 'legacy-order'"
                    )
                ).one()
            self.assertEqual(row.name, "Legacy order")
            self.assertIsNone(row.processing_plan_name)

            with patch.dict(os.environ, {"DATABASE_URL": database_url}):
                command.downgrade(self.alembic_config(), "0006_production_progress")
            columns = {
                column["name"] for column in inspect(engine).get_columns("orders")
            }
            self.assertNotIn("processing_plan_name", columns)
            engine.dispose()
