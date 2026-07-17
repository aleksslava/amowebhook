"""Replace item actuals with spent and add produced quantity."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0006_production_progress"
down_revision: Union[str, Sequence[str], None] = "0005_order_device"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.add_column(
            sa.Column(
                "produced_quantity",
                sa.Numeric(18, 6),
                server_default=sa.text("0"),
                nullable=False,
            )
        )

    with op.batch_alter_table("order_items") as batch_op:
        batch_op.alter_column(
            "actual_quantity",
            new_column_name="spent_quantity",
            existing_type=sa.Numeric(18, 6),
            existing_server_default=sa.text("0"),
            existing_nullable=False,
        )

    op.execute(
        sa.text(
            "UPDATE order_items SET spent_quantity = CASE "
            "WHEN spent_quantity < 0 OR quantity <= 0 THEN 0 "
            "WHEN CAST(spent_quantity AS INTEGER) > quantity "
            "THEN CAST(quantity AS INTEGER) "
            "ELSE CAST(spent_quantity AS INTEGER) END"
        )
    )


def downgrade() -> None:
    with op.batch_alter_table("order_items") as batch_op:
        batch_op.alter_column(
            "spent_quantity",
            new_column_name="actual_quantity",
            existing_type=sa.Numeric(18, 6),
            existing_server_default=sa.text("0"),
            existing_nullable=False,
        )

    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_column("produced_quantity")
