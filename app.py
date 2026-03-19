"""
NBA Operador — Backend principal (FastAPI)
Sirve el frontend React y expone la API.

Instalar:
    pip install fastapi uvicorn pandas python-multipart

Correr:
    uvicorn app:app --reload --port 8000

Abrir: http://localhost:8000
"""

import io
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from nba_engine_v2 import CompanyContext, CustomerFeatures, NBAEngineV2, TurnContext

app = FastAPI(title="Numia NBA")
app.mount("/static", StaticFiles(directory="static"), name="static")


# ──────────────────────────────────────────────────────────────
# FRONTEND
# ──────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def root():
    with open("static/index.html", encoding="utf-8") as f:
        return f.read()


# ──────────────────────────────────────────────────────────────
# UPLOAD CSV
# ──────────────────────────────────────────────────────────────

@app.post("/api/upload")
async def upload(file: UploadFile = File(...)):
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content), low_memory=False)
    except Exception as e:
        raise HTTPException(400, f"Error leyendo CSV: {e}")

    LLAMADA_ACTIONS = ["LLAMADA", "LLAMADA AUTOMATICA", "LLAMADA MANUAL", "LLAMADO DIRECTO"]
    FIN_ACTIONS = [
        "TIPIFICADO Y FINALIZADO", "FINALIZACION", "FINALIZACION AUTOMATICA",
        "TIPIFICADO Y DERIVADO", "TIPIFICADO",
    ]

    llamadas = df[df["action_text"].isin(LLAMADA_ACTIONS)].copy()
    fins     = df[df["action_text"].isin(FIN_ACTIONS)].copy()

    if len(llamadas) == 0:
        raise HTTPException(400, "No se encontraron filas de LLAMADA en el CSV.")

    # ── Stats generales ────────────────────────────────────────
    email_col = "turn_email"
    appt_col  = "appointment_code"

    email_cov = 0
    appt_cov  = 0
    if email_col in llamadas.columns:
        email_cov = round(llamadas[email_col].notna().sum() * 100 / len(llamadas), 1)
    if appt_col in llamadas.columns:
        appt_cov = round(
            ((llamadas[appt_col].notna()) & (llamadas[appt_col] != "")).sum() * 100 / len(llamadas), 1
        )

    # ── Stats por cola ─────────────────────────────────────────
    queue_stats = []
    for qname, grp in llamadas.groupby("queue_name"):
        fin_grp = fins[fins["queue_name"] == qname]
        espera_vals = grp["wait_time"].dropna()
        espera_vals = espera_vals[espera_vals >= 0]
        aten_vals   = fin_grp["attention_time"].dropna() if len(fin_grp) else pd.Series(dtype=float)
        aten_vals   = aten_vals[aten_vals >= 0]
        queue_stats.append({
            "queue_name":      str(qname),
            "turnos":          int(len(grp)),
            "espera_prom_min": round(float(espera_vals.mean()) / 60, 1) if len(espera_vals) else None,
            "atencion_prom_min": round(float(aten_vals.mean()) / 60, 1) if len(aten_vals) else None,
        })
    queue_stats.sort(key=lambda x: x["turnos"], reverse=True)

    # ── Muestra de turnos para el simulador ───────────────────
    sample_df = llamadas[llamadas["wait_time"].notna()].head(100)
    sample_turns = []
    for _, row in sample_df.iterrows():
        ts = str(row.get("action_time", ""))
        hour = 10
        try:
            hour = pd.to_datetime(ts).hour
        except Exception:
            pass
        sample_turns.append({
            "turn_id":           str(row.get("turn_id", "")),
            "queue_name":        str(row.get("queue_name", "")),
            "branch_name":       str(row.get("branch_name", "")) if pd.notna(row.get("branch_name")) else "",
            "wait_time_seconds": float(row.get("wait_time", 0)),
            "turn_email":        str(row.get("turn_email", "")) if pd.notna(row.get("turn_email")) else None,
            "hour":              hour,
        })

    queues_list   = sorted(llamadas["queue_name"].dropna().unique().tolist())
    branches_list = sorted(llamadas["branch_name"].dropna().unique().tolist()) if "branch_name" in llamadas.columns else []

    # ── Umbrales reales del dataset ────────────────────────────
    all_wait = llamadas["wait_time"].dropna()
    all_wait = all_wait[all_wait >= 0]
    p75 = round(float(all_wait.quantile(0.75)) / 60, 1) if len(all_wait) else 16
    p90 = round(float(all_wait.quantile(0.90)) / 60, 1) if len(all_wait) else 33

    return {
        "total_llamadas":          len(llamadas),
        "queues":                  len(queues_list),
        "branches":                len(branches_list),
        "email_coverage_pct":      email_cov,
        "appointment_coverage_pct": appt_cov,
        "espera_p75_min":          p75,
        "espera_p90_min":          p90,
        "queue_stats":             queue_stats[:25],
        "sample_turns":            sample_turns,
        "queues_list":             queues_list,
        "branches_list":           branches_list,
    }


# ──────────────────────────────────────────────────────────────
# SUGERENCIAS
# ──────────────────────────────────────────────────────────────

class SuggestRequest(BaseModel):
    # Turno
    turn_id:           str   = "SIM-001"
    queue_name:        str
    branch_name:       str   = ""
    wait_time_seconds: float
    hour:              int   = 10
    turn_email:        Optional[str] = None

    # Perfil del cliente (ingresado manualmente en el simulador)
    visitas_total:          int            = 0
    nps_promedio:           Optional[float] = None
    veces_nps_bajo:         int            = 0
    dias_desde_ultima_visita: Optional[int] = None

    # Contexto de empresa
    company_name:         str = ""
    sector:               str = "banca"
    process_descriptions: str = ""

    # Umbrales del dataset cargado (si aplica)
    espera_p75_min: Optional[float] = None
    espera_p90_min: Optional[float] = None


@app.post("/api/suggest")
def suggest(req: SuggestRequest):
    company = CompanyContext(
        company_name=req.company_name,
        sector=req.sector,
        process_descriptions=req.process_descriptions,
    )

    engine = NBAEngineV2(company_context=company)

    # Ajustar umbrales si vienen del dataset real
    if req.espera_p75_min:
        engine.t.ESPERA_LARGA_MIN     = req.espera_p75_min
    if req.espera_p90_min:
        engine.t.ESPERA_MUY_LARGA_MIN = req.espera_p90_min

    turn = TurnContext(
        turn_id=req.turn_id,
        queue_name=req.queue_name,
        branch_name=req.branch_name,
        operator_id="OP-SIM",
        wait_time_seconds=req.wait_time_seconds,
        turn_email=req.turn_email or None,
        llamada_ts=datetime.now().replace(hour=max(0, min(23, req.hour))),
    )

    features = CustomerFeatures(
        email=req.turn_email or "anon",
        visitas_total=req.visitas_total,
        dias_desde_ultima_visita=req.dias_desde_ultima_visita,
        encuestas_respondidas=1 if req.nps_promedio is not None else 0,
        nps_promedio=req.nps_promedio,
        nps_minimo=req.nps_promedio,
        veces_nps_bajo=req.veces_nps_bajo,
        flag_cliente_insatisfecho=(
            req.nps_promedio is not None and req.nps_promedio <= 2.5
        ),
        flag_visita_reciente=(
            req.dias_desde_ultima_visita is not None and req.dias_desde_ultima_visita <= 7
        ),
        flag_primera_visita=req.visitas_total <= 1,
        flag_insatisfaccion_repetida=req.veces_nps_bajo >= 2,
    )

    suggestions = engine.suggest(turn, features)
    return {"suggestions": suggestions}
