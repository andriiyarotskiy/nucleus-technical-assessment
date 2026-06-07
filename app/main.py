from fastapi import FastAPI

from app.config import get_settings
from app.logging import configure_logging


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings)
    return FastAPI(title=settings.app_name)


app = create_app()
