"""Add local suborders to production orders."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0008_order_suborders"
down_revision: Union[str, Sequence[str], None] = "0007_order_processing_plan"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.add_column(
            sa.Column(
                "last_suborder_number",
                sa.Integer(),
                server_default=sa.text("0"),
                nullable=False,
            )
        )

    op.create_table(
        "order_suborders",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("order_id", sa.Integer(), nullable=False),
        sa.Column("number", sa.Integer(), nullable=False),
        sa.Column("planned_quantity", sa.Numeric(18, 6), nullable=False),
        sa.Column(
            "actual_quantity",
            sa.Numeric(18, 6),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("planned_date", sa.Date(), nullable=False),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "order_id",
            "number",
            name="uq_order_suborders_order_number",
        ),
    )
    op.create_index(
        "ix_order_suborders_order_id",
        "order_suborders",
        ["order_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_order_suborders_order_id", table_name="order_suborders")
    op.drop_table("order_suborders")
    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_column("last_suborder_number")
