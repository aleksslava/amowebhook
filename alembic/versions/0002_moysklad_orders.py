"""Add users and MoySklad order storage."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0002_moysklad_orders"
down_revision: Union[str, Sequence[str], None] = "0001_existing_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("is_admin", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_users_name", "users", ["name"], unique=True)

    op.create_table(
        "orders",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("moysklad_id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("code", sa.String(length=255), nullable=True),
        sa.Column("external_code", sa.String(length=255), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("moment", sa.DateTime(), nullable=True),
        sa.Column("delivery_planned_moment", sa.DateTime(), nullable=True),
        sa.Column("moysklad_created_at", sa.DateTime(), nullable=True),
        sa.Column("moysklad_updated_at", sa.DateTime(), nullable=True),
        sa.Column("applicable", sa.Boolean(), nullable=True),
        sa.Column("production_quantity", sa.Numeric(18, 6), nullable=True),
        sa.Column("performer_name", sa.String(length=255), nullable=True),
        sa.Column("state_id", sa.String(length=36), nullable=True),
        sa.Column("state_name", sa.String(length=255), nullable=True),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("synced_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_orders_moysklad_id", "orders", ["moysklad_id"], unique=True)
    op.create_index("ix_orders_user_id", "orders", ["user_id"], unique=False)
    op.create_index(
        "ix_orders_moysklad_updated_at",
        "orders",
        ["moysklad_updated_at"],
        unique=False,
    )
    op.create_index("ix_orders_performer_name", "orders", ["performer_name"], unique=False)

    op.create_table(
        "order_items",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("order_id", sa.Integer(), nullable=False),
        sa.Column("moysklad_position_id", sa.String(length=36), nullable=False),
        sa.Column("assortment_id", sa.String(length=36), nullable=True),
        sa.Column("assortment_type", sa.String(length=255), nullable=True),
        sa.Column("assortment_name", sa.String(length=255), nullable=True),
        sa.Column("assortment_code", sa.String(length=255), nullable=True),
        sa.Column("quantity", sa.Numeric(18, 6), nullable=False),
        sa.Column("reserve", sa.Numeric(18, 6), nullable=True),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "order_id",
            "moysklad_position_id",
            name="uq_order_items_order_position",
        ),
    )
    op.create_index("ix_order_items_order_id", "order_items", ["order_id"], unique=False)
    op.create_index(
        "ix_order_items_assortment_id",
        "order_items",
        ["assortment_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_order_items_assortment_id", table_name="order_items")
    op.drop_index("ix_order_items_order_id", table_name="order_items")
    op.drop_table("order_items")
    op.drop_index("ix_orders_performer_name", table_name="orders")
    op.drop_index("ix_orders_moysklad_updated_at", table_name="orders")
    op.drop_index("ix_orders_user_id", table_name="orders")
    op.drop_index("ix_orders_moysklad_id", table_name="orders")
    op.drop_table("orders")
    op.drop_index("ix_users_name", table_name="users")
    op.drop_table("users")
