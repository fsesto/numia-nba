"""
NBA Operador — API REST (FastAPI)
Endpoint que consume el motor de reglas en tiempo real.

Instalar dependencias:
    pip install fastapi uvicorn sqlalchemy pymysql python-dotenv

Correr:
    uvicorn 03_api:app --host 0.0.0.0 --port 8000 --reload

Endpoint principal:
    POST /nba/suggest
"""

import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Importar el motor desde el mismo directorio
from nba_engine import NBAEngine, TurnContext  # type: ignore

# ──────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("nba.api")

app = FastAPI(
    title="Numia NBA — Next-Best-Action para Operadores",
    description="Sugerencias en tiempo real en el momento de LLAMADA a turno",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restringir en producción al dominio de Numia
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


# ──────────────────────────────────────────────────────────────
# CONEXIÓN A LA BASE DE DATOS
# ──────────────────────────────────────────────────────────────

def get_db_connection():
    """
    Retorna una conexión SQLAlchemy.
    Configurar via variables de entorno o .env
    """
    try:
        from sqlalchemy import create_engine, text
        from dotenv import load_dotenv
        load_dotenv()

        db_url = os.getenv(
            "DATABASE_URL",
            "mysql+pymysql://user:password@localhost:3306/numia_db"
        )
        engine = create_engine(db_url, pool_pre_ping=True, pool_size=5)
        return engine
    except Exception as e:
        logger.warning("No se pudo conectar a la DB: %s — modo offline activado", e)
        return None


# Inicializar engine una sola vez (singleton)
_db = get_db_connection()
_nba_engine = NBAEngine(db_conn=_db)


# ──────────────────────────────────────────────────────────────
# SCHEMAS DE REQUEST / RESPONSE
# ──────────────────────────────────────────────────────────────

class SuggestRequest(BaseModel):
    """
    Payload que llega cuando se dispara el evento LLAMADA en Numia.
    Mapear desde el webhook de qmovements.
    """
    turn_id: str
    customer_id: Optional[str] = None
    turn_email: Optional[str] = None
    queue_name: str
    branch_name: str
    operator_id: str
    wait_time_seconds: float
    appointment_code: Optional[str] = None
    llamada_ts: Optional[datetime] = None

    class Config:
        json_schema_extra = {
            "example": {
                "turn_id": "T-123456",
                "customer_id": "C-789012",
                "turn_email": "juan.garcia@email.com",
                "queue_name": "Solicitud de Préstamos",
                "branch_name": "Sucursal Centro",
                "operator_id": "OP-042",
                "wait_time_seconds": 1840,
                "appointment_code": "APT-XYZ99",
                "llamada_ts": "2026-03-19T11:30:00"
            }
        }


class SuggestionItem(BaseModel):
    layer: str
    action: str
    priority: int
    label: str
    message: str
    evidence: list[str]
    confidence: float


class SuggestResponse(BaseModel):
    turn_id: str
    customer_id: Optional[str]
    suggestions: list[SuggestionItem]
    generated_at: datetime
    engine_version: str = "rules-v1"


class FeedbackRequest(BaseModel):
    """
    Feedback del operador sobre la sugerencia mostrada.
    Alimenta el loop de entrenamiento del modelo.
    """
    turn_id: str
    action_shown: str       # acción que se mostró
    feedback: str           # 'USEFUL' | 'NOT_USEFUL' | 'NOT_SHOWN'
    operator_id: str
    notes: Optional[str] = None


# ──────────────────────────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "engine": "rules-v1", "db_connected": _db is not None}


@app.post("/nba/suggest", response_model=SuggestResponse)
def suggest(payload: SuggestRequest):
    """
    Endpoint principal.
    Recibe el contexto del turno en el momento de LLAMADA
    y devuelve hasta 2 sugerencias priorizadas.

    Latencia objetivo: < 300ms (feature store en cache, reglas en memoria)
    """
    ctx = TurnContext(
        turn_id=payload.turn_id,
        customer_id=payload.customer_id,
        turn_email=payload.turn_email,
        queue_name=payload.queue_name,
        branch_name=payload.branch_name,
        operator_id=payload.operator_id,
        wait_time_seconds=payload.wait_time_seconds,
        appointment_code=payload.appointment_code,
        llamada_ts=payload.llamada_ts or datetime.now(),
    )

    try:
        raw_suggestions = _nba_engine.suggest(ctx)
    except Exception as e:
        logger.error("Error en NBA engine para turn=%s: %s", payload.turn_id, e)
        raise HTTPException(status_code=500, detail="Error generando sugerencias")

    # Loguear en tabla nba_log (si hay DB)
    _log_suggestion(payload.turn_id, payload.customer_id, payload.operator_id, raw_suggestions)

    return SuggestResponse(
        turn_id=payload.turn_id,
        customer_id=payload.customer_id,
        suggestions=[SuggestionItem(**s) for s in raw_suggestions],
        generated_at=datetime.now(),
    )


@app.post("/nba/feedback")
def feedback(payload: FeedbackRequest):
    """
    Registra el feedback del operador sobre una sugerencia.
    Tabla nba_feedback_log — base del loop de reentrenamiento.
    """
    if payload.feedback not in ("USEFUL", "NOT_USEFUL", "NOT_SHOWN"):
        raise HTTPException(
            status_code=400,
            detail="feedback debe ser USEFUL, NOT_USEFUL o NOT_SHOWN"
        )

    _log_feedback(
        turn_id=payload.turn_id,
        action_shown=payload.action_shown,
        feedback=payload.feedback,
        operator_id=payload.operator_id,
        notes=payload.notes,
    )

    return {"status": "ok", "turn_id": payload.turn_id}


@app.get("/nba/stats")
def stats():
    """
    Métricas básicas de adopción — para el dashboard de monitoreo.
    """
    if not _db:
        return {"error": "DB no disponible"}

    try:
        from sqlalchemy import text
        with _db.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    DATE(shown_at)                          AS fecha,
                    COUNT(*)                                AS sugerencias_mostradas,
                    SUM(feedback = 'USEFUL')                AS utiles,
                    SUM(feedback = 'NOT_USEFUL')            AS no_utiles,
                    ROUND(
                        SUM(feedback = 'USEFUL') * 100.0
                        / NULLIF(COUNT(*), 0), 1
                    )                                       AS adoption_rate_pct,
                    COUNT(DISTINCT action_shown)            AS tipos_accion_distintos
                FROM nba_feedback_log
                WHERE shown_at >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
                GROUP BY DATE(shown_at)
                ORDER BY fecha DESC
            """)).fetchall()

        return {"stats": [dict(row) for row in result]}
    except Exception as e:
        logger.error("Error en /stats: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────────────────────────
# HELPERS DE LOGGING
# ──────────────────────────────────────────────────────────────

def _log_suggestion(turn_id: str, customer_id: Optional[str],
                    operator_id: str, suggestions: list[dict]) -> None:
    """Registra las sugerencias mostradas en nba_log."""
    if not _db:
        return
    try:
        from sqlalchemy import text
        with _db.connect() as conn:
            for s in suggestions:
                conn.execute(text("""
                    INSERT INTO nba_log
                        (turn_id, customer_id, operator_id, action_shown,
                         layer, priority, shown_at)
                    VALUES
                        (:turn_id, :customer_id, :operator_id, :action_shown,
                         :layer, :priority, NOW())
                    ON DUPLICATE KEY UPDATE shown_at = NOW()
                """), {
                    "turn_id": turn_id,
                    "customer_id": customer_id,
                    "operator_id": operator_id,
                    "action_shown": s["action"],
                    "layer": s["layer"],
                    "priority": s["priority"],
                })
            conn.commit()
    except Exception as e:
        logger.warning("No se pudo loguear sugerencia: %s", e)


def _log_feedback(turn_id: str, action_shown: str, feedback: str,
                  operator_id: str, notes: Optional[str]) -> None:
    """Registra el feedback del operador en nba_feedback_log."""
    if not _db:
        return
    try:
        from sqlalchemy import text
        with _db.connect() as conn:
            conn.execute(text("""
                INSERT INTO nba_feedback_log
                    (turn_id, action_shown, feedback, operator_id, notes, feedback_at)
                VALUES
                    (:turn_id, :action_shown, :feedback, :operator_id, :notes, NOW())
                ON DUPLICATE KEY UPDATE
                    feedback = VALUES(feedback),
                    feedback_at = NOW()
            """), {
                "turn_id": turn_id,
                "action_shown": action_shown,
                "feedback": feedback,
                "operator_id": operator_id,
                "notes": notes,
            })
            conn.commit()
    except Exception as e:
        logger.warning("No se pudo loguear feedback: %s", e)
