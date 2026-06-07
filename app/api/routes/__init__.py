from fastapi import APIRouter

from app.api.routes.events import router as events_router
from app.api.routes.metrics import router as metrics_router
from app.api.routes.users import router as users_router

router = APIRouter()
router.include_router(events_router)
router.include_router(metrics_router)
router.include_router(users_router)
