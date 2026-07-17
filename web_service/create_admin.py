from __future__ import annotations

import argparse
import getpass

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models import User
from settings.settings import load_config
from web_service.auth import hash_password


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the first cabinet administrator")
    parser.add_argument("--name", required=True, help="Administrator login")
    args = parser.parse_args()
    name = args.name.strip()
    if not name or len(name) > 255:
        raise SystemExit("Login must contain from 1 to 255 characters")

    password = getpass.getpass("Password: ")
    confirmation = getpass.getpass("Repeat password: ")
    if not 1 <= len(password) <= 128:
        raise SystemExit("Password must contain from 1 to 128 characters")
    if password != confirmation:
        raise SystemExit("Passwords do not match")

    config = load_config()
    engine = create_engine(config.database_url)
    try:
        with Session(engine) as db, db.begin():
            if db.scalar(select(User.id).where(User.name == name)) is not None:
                raise SystemExit(f"User already exists: {name}")
            db.add(
                User(
                    name=name,
                    password_hash=hash_password(password),
                    is_admin=True,
                    is_active=True,
                )
            )
    finally:
        engine.dispose()
    print(f"Administrator created: {name}")


if __name__ == "__main__":
    main()
