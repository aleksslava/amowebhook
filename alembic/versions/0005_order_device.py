"""Add device name to production orders."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0005_order_device"
down_revision: Union[str, Sequence[str], None] = "0004_item_actual_quantity"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.add_column(sa.Column("device_name", sa.String(255), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_column("device_name")
