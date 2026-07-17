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
                command.upgrade(self.alembic_config(), "head")
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
