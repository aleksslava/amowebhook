"""Add processing plan name to production orders."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0007_order_processing_plan"
down_revision: Union[str, Sequence[str], None] = "0006_production_progress"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.add_column(
            sa.Column("processing_plan_name", sa.String(255), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("orders") as batch_op:
        batch_op.drop_column("processing_plan_name")
