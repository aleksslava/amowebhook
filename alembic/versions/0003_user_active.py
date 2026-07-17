"""Add user activation state."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0003_user_active"
down_revision: Union[str, Sequence[str], None] = "0002_moysklad_orders"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("users") as batch_op:
        batch_op.add_column(
            sa.Column(
                "is_active",
                sa.Boolean(),
                server_default=sa.true(),
                nullable=False,
            )
        )
        batch_op.create_index("ix_users_is_active", ["is_active"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("users") as batch_op:
        batch_op.drop_index("ix_users_is_active")
        batch_op.drop_column("is_active")
