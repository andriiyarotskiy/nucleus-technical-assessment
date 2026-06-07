from pydantic import BaseModel, ConfigDict, Field


class ErrorDetail(BaseModel):
    code: str = Field(
        description="Stable machine-readable error code.",
        examples=["database_unavailable"],
    )
    message: str = Field(
        description="Safe human-readable error message.",
        examples=["Database is unavailable"],
    )


class ErrorResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "detail": {
                    "code": "database_unavailable",
                    "message": "Database is unavailable",
                }
            }
        }
    )

    detail: ErrorDetail = Field(description="Structured error details.")
