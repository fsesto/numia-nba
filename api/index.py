"""
Numia NBA — Vercel Serverless API.
Zero pandas. stdlib csv only.
"""

import csv
import io
import os
import sys
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Vercel resolves imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nba_engine import (
    SECTORS, CompanyContext, CustomerFeatures, NBAEngine, Thresholds, TurnContext,
)

app = FastAPI(title="Numia NBA", version="2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

LLAMADA = {"LLAMADA", "LLAMADA AUTOMATICA", "LLAMADA MANUAL", "LLAMADO DIRECTO"}
FIN = {"TIPIFICADO Y FINALIZADO", "FINALIZACION", "FINALIZACION AUTOMATICA",
       "TIPIFICADO Y DERIVADO", "TIPIFICADO"}


# ── helpers ───────────────────────────────────────────────────

def _float(v) -> Optional[float]:
    try:
        f = float(v)
        return f if f >= 0 else None
    except (TypeError, ValueError):
        return None


def _pct(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    d = sorted(data)
    i = (p / 100) * (len(d) - 1)
    lo = int(i)
    hi = min(lo + 1, len(d) - 1)
    return d[lo] + (i - lo) * (d[hi] - d[lo])


def _hour(ts: str) -> int:
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(ts[:19], fmt.split(".")[0]).hour
        except Exception:
            continue
    return 10


# ── endpoints ─────────────────────────────────────────────────

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


@app.post("/api/upload")
async def upload(file: UploadFile = File(...)):
    content = await file.read()
    if len(content) > 4_500_000:
        raise HTTPException(413, "Archivo mayor a 4.5 MB. Subi una muestra de ~5.000 filas.")

    try:
        text = content.decode("utf-8", errors="replace")
        rows = list(csv.DictReader(io.StringIO(text)))
    except Exception as e:
        raise HTTPException(400, f"Error leyendo CSV: {e}")

    required = {"action_text", "queue_name"}
    if rows and not required.issubset(rows[0].keys()):
        missing = required - set(rows[0].keys())
        raise HTTPException(400, f"Columnas faltantes: {', '.join(missing)}")

    calls, ends = [], []
    for r in rows:
        a = r.get("action_text", "")
        if a in LLAMADA:
            calls.append(r)
        elif a in FIN:
            ends.append(r)

    if not calls:
        raise HTTPException(400, "No se encontraron filas de LLAMADA en el CSV.")

    n = len(calls)
    email_pct = round(sum(1 for r in calls if r.get("turn_email", "").strip()) * 100 / n, 1)
    appt_pct  = round(sum(1 for r in calls if r.get("appointment_code", "").strip()) * 100 / n, 1)

    # waits/attention by queue
    from collections import defaultdict
    q_waits, q_counts = defaultdict(list), defaultdict(int)
    q_attn = defaultdict(list)
    for r in calls:
        q = r.get("queue_name", "")
        q_counts[q] += 1
        v = _float(r.get("wait_time"))
        if v is not None:
            q_waits[q].append(v)
    for r in ends:
        q = r.get("queue_name", "")
        v = _float(r.get("attention_time"))
        if v is not None:
            q_attn[q].append(v)

    queue_stats = []
    for q, cnt in q_counts.items():
        w = q_waits[q]
        a = q_attn.get(q, [])
        queue_stats.append({
            "queue_name": q, "turnos": cnt,
            "espera_prom_min": round(sum(w) / len(w) / 60, 1) if w else None,
            "atencion_prom_min": round(sum(a) / len(a) / 60, 1) if a else None,
        })
    queue_stats.sort(key=lambda x: x["turnos"], reverse=True)

    all_w = [v for vs in q_waits.values() for v in vs]
    p75 = round(_pct(all_w, 75) / 60, 1) if all_w else 16
    p90 = round(_pct(all_w, 90) / 60, 1) if all_w else 33

    # hourly distribution
    hourly = [0] * 24
    for r in calls:
        h = _hour(r.get("action_time", ""))
        hourly[h] += 1

    # sample turns
    sample = []
    for r in calls[:80]:
        wt = _float(r.get("wait_time"))
        if wt is None:
            continue
        sample.append({
            "turn_id": r.get("turn_id", ""),
            "queue_name": r.get("queue_name", ""),
            "branch_name": r.get("branch_name", ""),
            "wait_time_seconds": wt,
            "turn_email": r.get("turn_email") or None,
            "hour": _hour(r.get("action_time", "")),
        })

    return {
        "total_llamadas": n,
        "queues": len(q_counts),
        "branches": len({r.get("branch_name", "") for r in calls if r.get("branch_name", "").strip()}),
        "email_coverage_pct": email_pct,
        "appointment_coverage_pct": appt_pct,
        "espera_p75_min": p75,
        "espera_p90_min": p90,
        "hourly": hourly,
        "queue_stats": queue_stats[:25],
        "sample_turns": sample,
        "queues_list": sorted(q_counts.keys()),
        "branches_list": sorted({r.get("branch_name", "") for r in calls if r.get("branch_name", "").strip()}),
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
    dias_ultima_visita: Optional[int] = Field(ge=0, default=None)
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
        dias_ultima_visita=req.dias_ultima_visita,
        nps=req.nps,
        nps_low_count=req.nps_low_count,
        is_new=req.visitas <= 1,
        is_unhappy=req.nps is not None and req.nps <= 2.5,
        is_repeat_unhappy=req.nps_low_count >= 2,
        is_recent=req.dias_ultima_visita is not None and req.dias_ultima_visita <= 7,
    )
    return {"suggestions": engine.suggest(turn, feat)}
