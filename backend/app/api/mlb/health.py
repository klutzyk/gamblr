from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
def mlb_health():
    return {
        "sport": "mlb",
        "status": "ok",
        "namespace": "ready",
    }


@router.get("/status")
def mlb_status():
    return {
        "sport": "mlb",
        "status": "planned",
        "ingestion": "not_started",
        "models": "not_started",
        "predictions": "not_started",
    }
