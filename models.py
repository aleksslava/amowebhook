from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class EducationVisit(Base):
    __tablename__ = "education_visits"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    utm_source: Mapped[str | None] = mapped_column(String(255))
    utm_medium: Mapped[str | None] = mapped_column(String(255))
    utm_campaign: Mapped[str | None] = mapped_column(String(255))
    utm_content: Mapped[str | None] = mapped_column(String(255))
    utm_term: Mapped[str | None] = mapped_column(String(255))
    yclid: Mapped[str | None] = mapped_column(String(255))
    cm_id: Mapped[str | None] = mapped_column(String(255))
    block: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        index=True,
    )


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    orders: Mapped[list[MoySkladOrder]] = relationship(back_populates="user")


class MoySkladOrder(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    moysklad_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    user_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        index=True,
    )
    name: Mapped[str] = mapped_column(String(255))
    code: Mapped[str | None] = mapped_column(String(255))
    external_code: Mapped[str | None] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text)
    moment: Mapped[datetime | None] = mapped_column(DateTime)
    delivery_planned_moment: Mapped[datetime | None] = mapped_column(DateTime)
    moysklad_created_at: Mapped[datetime | None] = mapped_column(DateTime)
    moysklad_updated_at: Mapped[datetime | None] = mapped_column(DateTime, index=True)
    applicable: Mapped[bool | None] = mapped_column(Boolean)
    production_quantity: Mapped[Decimal | None] = mapped_column(Numeric(18, 6))
    produced_quantity: Mapped[Decimal] = mapped_column(
        Numeric(18, 6),
        default=Decimal("0"),
        server_default="0",
    )
    last_suborder_number: Mapped[int] = mapped_column(default=0, server_default="0")
    performer_name: Mapped[str | None] = mapped_column(String(255), index=True)
    device_name: Mapped[str | None] = mapped_column(String(255))
    processing_plan_name: Mapped[str | None] = mapped_column(String(255))
    state_id: Mapped[str | None] = mapped_column(String(36))
    state_name: Mapped[str | None] = mapped_column(String(255))
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    user: Mapped[User | None] = relationship(back_populates="orders")
    items: Mapped[list[OrderItem]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    suborders: Mapped[list[OrderSuborder]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class OrderItem(Base):
    __tablename__ = "order_items"
    __table_args__ = (
        UniqueConstraint(
            "order_id",
            "moysklad_position_id",
            name="uq_order_items_order_position",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(
        ForeignKey("orders.id", ondelete="CASCADE"),
        index=True,
    )
    moysklad_position_id: Mapped[str] = mapped_column(String(36))
    assortment_id: Mapped[str | None] = mapped_column(String(36), index=True)
    assortment_type: Mapped[str | None] = mapped_column(String(255))
    assortment_name: Mapped[str | None] = mapped_column(String(255))
    assortment_code: Mapped[str | None] = mapped_column(String(255))
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    spent_quantity: Mapped[Decimal] = mapped_column(
        Numeric(18, 6),
        default=Decimal("0"),
        server_default="0",
    )
    reserve: Mapped[Decimal | None] = mapped_column(Numeric(18, 6))
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON)

    order: Mapped[MoySkladOrder] = relationship(back_populates="items")


class OrderSuborder(Base):
    __tablename__ = "order_suborders"
    __table_args__ = (
        UniqueConstraint(
            "order_id",
            "number",
            name="uq_order_suborders_order_number",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(
        ForeignKey("orders.id", ondelete="CASCADE"),
        index=True,
    )
    number: Mapped[int] = mapped_column()
    planned_quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6))
    actual_quantity: Mapped[Decimal] = mapped_column(
        Numeric(18, 6),
        default=Decimal("0"),
        server_default="0",
    )
    planned_date: Mapped[date] = mapped_column(Date)

    order: Mapped[MoySkladOrder] = relationship(back_populates="suborders")
