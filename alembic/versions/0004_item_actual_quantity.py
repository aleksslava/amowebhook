"""Add actual production quantity to order items."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0004_item_actual_quantity"
down_revision: Union[str, Sequence[str], None] = "0003_user_active"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("order_items") as batch_op:
        batch_op.add_column(
            sa.Column(
                "actual_quantity",
                sa.Numeric(18, 6),
                server_default=sa.text("0"),
                nullable=False,
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("order_items") as batch_op:
        batch_op.drop_column("actual_quantity")
