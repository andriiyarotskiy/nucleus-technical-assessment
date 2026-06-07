from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import build_async_engine, build_session_factory


async def test_session_factory_creates_async_sessions() -> None:
    engine = build_async_engine(
        "postgresql+asyncpg://postgres:postgres@localhost:5432/transactions"
    )
    session_factory = build_session_factory(engine)

    async with session_factory() as session:
        assert isinstance(session, AsyncSession)
        assert session.sync_session.expire_on_commit is False

    await engine.dispose()
