import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select

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
