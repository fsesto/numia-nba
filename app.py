"""
Numia NBA — Server principal (FastAPI + Uvicorn)
Sirve frontend + API. Sin límite de tamaño de CSV.
Para deploy en Railway, Render, o cualquier VPS.
"""

import csv
import io
import os
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from nba_engine import (
    SECTORS, CompanyContext, CustomerFeatures, NBAEngine, Thresholds, TurnContext,
)

app = FastAPI(title="Numia NBA", version="2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

LLAMADA = {"LLAMADA", "LLAMADA AUTOMATICA", "LLAMADA MANUAL", "LLAMADO DIRECTO"}
FIN = {
    "TIPIFICADO Y FINALIZADO", "FINALIZACION", "FINALIZACION AUTOMATICA",
    "TIPIFICADO Y DERIVADO", "TIPIFICADO",
}


# ── Frontend ──────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def root():
    with open("public/index.html", encoding="utf-8") as f:
        return f.read()


# ── API ───────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "version": "2.0"}


@app.get("/api/sectors")
def sectors():
    return {
        sid: {"label": s["label"], "icon": s["icon"],
              "customer": s["customer"], "sample_processes": s["sample_processes"]}
        for sid, s in SECTORS.items()
    }


def _pct(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    d = sorted(data)
    i = (p / 100) * (len(d) - 1)
    lo = int(i)
    hi = min(lo + 1, len(d) - 1)
    return d[lo] + (i - lo) * (d[hi] - d[lo])


@app.post("/api/upload")
async def upload(file: UploadFile = File(...)):
    """Acepta CSV de cualquier tamaño. Usa pandas para performance."""
    content = await file.read()
    file_mb = len(content) / (1024 * 1024)

    # Solo leemos las columnas que usamos para no reventar la RAM
    COLS_NEEDED = [
        "action_text", "queue_name", "branch_name", "turn_id",
        "wait_time", "attention_time", "action_time",
        "turn_email", "appointment_code",
    ]

    try:
        # Detectar columnas disponibles leyendo solo el header
        header = pd.read_csv(io.BytesIO(content), nrows=0)
        usecols = [c for c in COLS_NEEDED if c in header.columns]
        df = pd.read_csv(io.BytesIO(content), usecols=usecols, low_memory=False)
    except Exception as e:
        raise HTTPException(400, f"Error leyendo CSV: {e}")

    required = {"action_text", "queue_name"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise HTTPException(400, f"Columnas faltantes: {', '.join(missing)}")

    # Sampleo automático si hay demasiadas filas — mantiene representatividad
    MAX_ROWS = 200_000
    total_rows_original = len(df)
    sampled = False
    if len(df) > MAX_ROWS:
        df = df.sample(n=MAX_ROWS, random_state=42)
        sampled = True

    llamadas = df[df["action_text"].isin(LLAMADA)].copy()
    fins = df[df["action_text"].isin(FIN)].copy()

    if len(llamadas) == 0:
        raise HTTPException(400, "No se encontraron filas de LLAMADA en el CSV.")

    n = len(llamadas)

    # Cobertura
    email_cov = round(llamadas["turn_email"].notna().sum() * 100 / n, 1) if "turn_email" in llamadas.columns else 0
    appt_cov = 0
    if "appointment_code" in llamadas.columns:
        appt_cov = round(
            ((llamadas["appointment_code"].notna()) & (llamadas["appointment_code"] != "")).sum() * 100 / n, 1
        )

    # Por cola
    queue_stats = []
    for qname, grp in llamadas.groupby("queue_name"):
        fg = fins[fins["queue_name"] == qname]
        w = grp["wait_time"].dropna()
        w = w[w >= 0]
        a = fg["attention_time"].dropna() if len(fg) else pd.Series(dtype=float)
        a = a[a >= 0]
        queue_stats.append({
            "queue_name": str(qname),
            "turnos": int(len(grp)),
            "espera_prom_min": round(float(w.mean()) / 60, 1) if len(w) else None,
            "atencion_prom_min": round(float(a.mean()) / 60, 1) if len(a) else None,
        })
    queue_stats.sort(key=lambda x: x["turnos"], reverse=True)

    # Umbrales
    all_w = llamadas["wait_time"].dropna()
    all_w = all_w[all_w >= 0]
    p75 = round(float(all_w.quantile(0.75)) / 60, 1) if len(all_w) else 16
    p90 = round(float(all_w.quantile(0.90)) / 60, 1) if len(all_w) else 33

    # Distribucion horaria
    hourly = [0] * 24
    if "action_time" in llamadas.columns:
        try:
            hours = pd.to_datetime(llamadas["action_time"], errors="coerce").dt.hour
            for h in hours.dropna().astype(int):
                if 0 <= h < 24:
                    hourly[h] += 1
        except Exception:
            pass

    # Muestra
    sample = []
    for _, row in llamadas[llamadas["wait_time"].notna()].head(80).iterrows():
        hour = 10
        try:
            hour = pd.to_datetime(str(row.get("action_time", ""))).hour
        except Exception:
            pass
        sample.append({
            "turn_id": str(row.get("turn_id", "")),
            "queue_name": str(row.get("queue_name", "")),
            "branch_name": str(row.get("branch_name", "")) if pd.notna(row.get("branch_name")) else "",
            "wait_time_seconds": float(row.get("wait_time", 0)),
            "turn_email": str(row.get("turn_email", "")) if pd.notna(row.get("turn_email")) else None,
            "hour": hour,
        })

    queues_list = sorted(llamadas["queue_name"].dropna().unique().tolist())
    branches_list = sorted(llamadas["branch_name"].dropna().unique().tolist()) if "branch_name" in llamadas.columns else []

    return {
        "total_llamadas": n,
        "total_rows_original": total_rows_original,
        "sampled": sampled,
        "sample_pct": round(MAX_ROWS * 100 / total_rows_original, 1) if sampled else 100,
        "file_mb": round(file_mb, 1),
        "queues": len(queues_list),
        "branches": len(branches_list),
        "email_coverage_pct": email_cov,
        "appointment_coverage_pct": appt_cov,
        "espera_p75_min": p75,
        "espera_p90_min": p90,
        "hourly": hourly,
        "queue_stats": queue_stats[:25],
        "sample_turns": sample,
        "queues_list": queues_list,
        "branches_list": branches_list,
    }


class SuggestReq(BaseModel):
    queue_name: str
    wait_time_seconds: float = Field(ge=0)
    hour: int = Field(ge=0, le=23, default=10)
    branch_name: str = ""
    turn_email: Optional[str] = None
    visitas: int = Field(ge=0, default=0)
    nps: Optional[float] = Field(ge=1, le=5, default=None)
    nps_low_count: int = Field(ge=0, default=0)
    dias_desde_ultima_visita: Optional[int] = Field(ge=0, default=None)
    company_name: str = ""
    sector: str = "banca"
    process_descriptions: str = ""
    espera_p75: Optional[float] = None
    espera_p90: Optional[float] = None


@app.post("/api/suggest")
def suggest(req: SuggestReq):
    co = CompanyContext(req.company_name, req.sector, req.process_descriptions)
    th = Thresholds()
    if req.espera_p75:
        th.wait_high = req.espera_p75
    if req.espera_p90:
        th.wait_critical = req.espera_p90

    engine = NBAEngine(co, th)
    turn = TurnContext(
        turn_id="SIM",
        queue_name=req.queue_name,
        branch_name=req.branch_name,
        wait_time_seconds=req.wait_time_seconds,
        turn_email=req.turn_email,
        llamada_ts=datetime.now().replace(hour=req.hour),
    )
    feat = CustomerFeatures(
        visitas=req.visitas,
        dias_ultima_visita=req.dias_desde_ultima_visita,
        nps=req.nps,
        nps_low_count=req.nps_low_count,
        is_new=req.visitas <= 1,
        is_unhappy=req.nps is not None and req.nps <= 2.5,
        is_repeat_unhappy=req.nps_low_count >= 2,
        is_recent=req.dias_desde_ultima_visita is not None and req.dias_desde_ultima_visita <= 7,
    )
    return {"suggestions": engine.suggest(turn, feat)}
