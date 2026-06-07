from alembic.config import Config
from alembic.script import ScriptDirectory


def test_migration_history_has_one_head() -> None:
    config = Config("alembic.ini")
    scripts = ScriptDirectory.from_config(config)

    assert scripts.get_heads() == ["20260607_01"]
