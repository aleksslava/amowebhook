"""Baseline the existing education visits table."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0001_existing_schema"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "education_visits",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("utm_source", sa.String(length=255), nullable=True),
        sa.Column("utm_medium", sa.String(length=255), nullable=True),
        sa.Column("utm_campaign", sa.String(length=255), nullable=True),
        sa.Column("utm_content", sa.String(length=255), nullable=True),
        sa.Column("utm_term", sa.String(length=255), nullable=True),
        sa.Column("yclid", sa.String(length=255), nullable=True),
        sa.Column("cm_id", sa.String(length=255), nullable=True),
        sa.Column("block", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_education_visits_created_at",
        "education_visits",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_education_visits_created_at", table_name="education_visits")
    op.drop_table("education_visits")
