from pydantic import BaseModel


class ReleaseRequestBody(BaseModel):
    domain: str
    timestamp: float
