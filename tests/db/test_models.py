from sqlalchemy import CheckConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from app.db.models import Transaction


def test_transaction_table_matches_schema_contract() -> None:
    table = Transaction.__table__

    assert table.primary_key.columns.keys() == ["id"]
    assert table.c.original_amount.type.precision == 20
    assert table.c.original_amount.type.scale == 8
    assert table.c.usd_rate.type.precision == 20
    assert table.c.usd_rate.type.scale == 10
    assert table.c.amount_usd.type.precision == 20
    assert table.c.amount_usd.type.scale == 2
    assert table.c.event_timestamp.type.timezone is True
    assert table.c.processed_at.type.timezone is True

    constraint_names = {
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert constraint_names == {
        "ck_transactions_amount_usd_non_negative",
        "ck_transactions_currency_length",
        "ck_transactions_currency_uppercase",
        "ck_transactions_original_amount_positive",
        "ck_transactions_usd_rate_positive",
    }


def test_transaction_ddl_contains_required_index() -> None:
    dialect = postgresql.dialect()
    table_ddl = str(CreateTable(Transaction.__table__).compile(dialect=dialect))
    index = next(iter(Transaction.__table__.indexes))
    index_ddl = str(CreateIndex(index).compile(dialect=dialect))

    assert "PRIMARY KEY (id)" in table_ddl
    assert "CREATE INDEX ix_transactions_user_timestamp_id" in index_ddl
    assert "(user_id, event_timestamp DESC, id DESC)" in index_ddl
