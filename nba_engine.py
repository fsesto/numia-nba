"""
Numia NBA Engine — Sector-aware Next-Best-Action rules engine.
Zero external dependencies. Pure Python 3.10+ stdlib.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

__all__ = [
    "CompanyContext", "TurnContext", "CustomerFeatures",
    "Suggestion", "NBAEngine", "SECTORS",
]

# ─────────────────────────────────────────────────────────────
# SECTOR CONFIGURATION
# ─────────────────────────────────────────────────────────────

SECTORS: dict[str, dict] = {
    "banca": {
        "label": "Banca y Servicios Financieros",
        "icon": "\U0001f3e6",
        "customer": "cliente",
        "complaint_kw": ["reclamo", "queja", "felicitaci", "oirs"],
        "commercial_kw": ["venta", "comercial", "préstamo", "prestamo",
                          "crédito", "credito", "inversion", "inversión",
                          "seguro", "hipoteca", "tarjeta", "asesor"],
        "operational_kw": ["caja", "transacci", "pago", "depósito", "deposito",
                           "extracci", "transferencia", "pila", "cambio divisa"],
        "digital": "homebanking o la app",
        "digital_verb": "operar sin venir a la sucursal",
        "sample_processes": (
            "Caja: depositos, extracciones y pagos basicos\n"
            "Venta de Productos: cross-sell de creditos y seguros\n"
            "Reclamos: gestion de quejas y revisiones\n"
            "Asesores Integrales: asesoria financiera integral"
        ),
    },
    "salud": {
        "label": "Salud y Laboratorios",
        "icon": "\U0001f3e5",
        "customer": "paciente",
        "complaint_kw": ["reclamo", "queja", "felicitaci", "oirs"],
        "commercial_kw": ["venta", "plan", "prepaga", "seguro", "medicin"],
        "operational_kw": ["caja", "pago", "extracci", "muestra",
                           "resultado", "pila", "admision"],
        "digital": "el portal de turnos online o la app",
        "digital_verb": "agendar turno sin venir",
        "sample_processes": (
            "Extraccion: toma de muestras de sangre\n"
            "Entrega de Resultados: retiro de estudios\n"
            "Caja: pagos y facturacion\n"
            "Asesores de Salud: consultas de cobertura medica"
        ),
    },
    "retail": {
        "label": "Retail y Comercio",
        "icon": "\U0001f6cd\ufe0f",
        "customer": "cliente",
        "complaint_kw": ["reclamo", "queja", "devoluci", "garantía", "garantia"],
        "commercial_kw": ["venta", "promoci", "crédito", "credito",
                          "fidelidad", "puntos", "membresi"],
        "operational_kw": ["caja", "pago", "retiro", "cambio", "envío", "envio"],
        "digital": "la tienda online o la app",
        "digital_verb": "comprar y gestionar sin ir a la tienda",
        "sample_processes": (
            "Caja: cobro y pagos\n"
            "Postventa: cambios, devoluciones, garantias\n"
            "Atencion Comercial: asesoria de productos\n"
            "Fidelizacion: gestion de programa de puntos"
        ),
    },
    "gobierno": {
        "label": "Gobierno y Sector Publico",
        "icon": "\U0001f3db\ufe0f",
        "customer": "ciudadano",
        "complaint_kw": ["reclamo", "queja", "denuncia", "recurso"],
        "commercial_kw": [],
        "operational_kw": ["trámite", "tramite", "certificado", "documento",
                           "caja", "pago", "registro", "licencia"],
        "digital": "el portal de tramites online",
        "digital_verb": "completar el tramite sin venir presencialmente",
        "sample_processes": (
            "Tramites Generales: certificados y registros\n"
            "Caja: pagos de tasas y multas\n"
            "Atencion al Ciudadano: consultas generales\n"
            "Reclamos: quejas y sugerencias"
        ),
    },
    "telco": {
        "label": "Telecomunicaciones",
        "icon": "\U0001f4e1",
        "customer": "cliente",
        "complaint_kw": ["reclamo", "queja", "avería", "averia",
                         "falla", "técnico", "tecnico", "soporte"],
        "commercial_kw": ["venta", "upgrade", "plan", "portabilidad",
                          "renovaci", "fibra", "equipo"],
        "operational_kw": ["pago", "factura", "caja", "recarga", "sim"],
        "digital": "la app o el portal de autogestion",
        "digital_verb": "gestionar sin ir a la sucursal",
        "sample_processes": (
            "Caja: pagos de facturas y recargas\n"
            "Ventas: alta de lineas y portabilidad\n"
            "Soporte Tecnico: averias y reclamos\n"
            "Atencion Comercial: cambio de plan y upgrades"
        ),
    },
    "otro": {
        "label": "Otro sector",
        "icon": "\U0001f4cb",
        "customer": "cliente",
        "complaint_kw": ["reclamo", "queja"],
        "commercial_kw": ["venta", "comercial"],
        "operational_kw": ["caja", "pago"],
        "digital": "los canales digitales disponibles",
        "digital_verb": "gestionar sin necesidad de venir",
        "sample_processes": (
            "Atencion General: consultas y tramites\n"
            "Caja: pagos y cobros\n"
            "Reclamos: quejas y sugerencias"
        ),
    },
}


# ─────────────────────────────────────────────────────────────
# DATA MODELS
# ─────────────────────────────────────────────────────────────

def _queue_base(name: str) -> str:
    """Strip geographic suffix: 'Caja A' -> 'Caja'."""
    return re.sub(r"\s+[A-Z]$", "", name.strip())


@dataclass
class CompanyContext:
    company_name: str = ""
    sector: str = "banca"
    process_descriptions: str = ""

    def __post_init__(self):
        self.sector = self.sector.lower()
        if self.sector not in SECTORS:
            self.sector = "otro"
        self._cfg = SECTORS[self.sector]
        self._procs = self._parse()

    def _parse(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for line in self.process_descriptions.replace(";", "\n").splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                out[k.strip().lower()] = v.strip().lower()
        return out

    @property
    def customer(self) -> str:
        return self._cfg["customer"]

    @property
    def digital(self) -> str:
        return self._cfg["digital"]

    @property
    def digital_verb(self) -> str:
        return self._cfg["digital_verb"]

    def intent(self, queue: str) -> str:
        """Classify queue: 'complaint' | 'commercial' | 'operational' | 'other'."""
        q = queue.lower()
        qb = _queue_base(queue).lower()
        desc = self._procs.get(q, self._procs.get(qb, ""))
        text = f"{q} {desc}"
        c = self._cfg
        if any(kw in text for kw in c["complaint_kw"]):
            return "complaint"
        if any(kw in text for kw in c["commercial_kw"]):
            return "commercial"
        if any(kw in text for kw in c["operational_kw"]):
            return "operational"
        return "other"

    def hint(self, queue: str) -> Optional[str]:
        q = queue.lower()
        qb = _queue_base(queue).lower()
        return self._procs.get(q) or self._procs.get(qb)


@dataclass
class TurnContext:
    turn_id: str
    queue_name: str
    branch_name: str = ""
    operator_id: str = ""
    wait_time_seconds: float = 0
    turn_email: Optional[str] = None
    llamada_ts: datetime = field(default_factory=datetime.now)

    @property
    def queue_base(self) -> str:
        return _queue_base(self.queue_name)

    @property
    def wait_min(self) -> float:
        return self.wait_time_seconds / 60.0

    @property
    def hour(self) -> int:
        return self.llamada_ts.hour

    @property
    def is_peak(self) -> bool:
        return self.hour in (10, 11, 12, 15, 16)


@dataclass
class CustomerFeatures:
    visitas: int = 0
    dias_ultima_visita: Optional[int] = None
    nps: Optional[float] = None
    nps_low_count: int = 0
    is_new: bool = True
    is_unhappy: bool = False
    is_repeat_unhappy: bool = False
    is_recent: bool = False
    segment: Optional[str] = None


@dataclass
class Suggestion:
    layer: str
    action: str
    priority: int
    label: str
    message: str
    evidence: list[str]
    confidence: float = 1.0

    def dict(self) -> dict:
        return {
            "layer": self.layer,
            "action": self.action,
            "priority": self.priority,
            "label": self.label,
            "message": self.message,
            "evidence": [e for e in self.evidence if e],
            "confidence": round(self.confidence, 2),
        }


# ─────────────────────────────────────────────────────────────
# THRESHOLDS
# ─────────────────────────────────────────────────────────────

@dataclass
class Thresholds:
    wait_high: float = 16.0      # p75 default (minutes)
    wait_critical: float = 33.0  # p90 default (minutes)
    nps_low: float = 2.5
    propensity_min: float = 0.65
    visits_frequent: int = 6


# ─────────────────────────────────────────────────────────────
# ENGINE
# ─────────────────────────────────────────────────────────────

class NBAEngine:

    def __init__(self, company: CompanyContext, thresholds: Optional[Thresholds] = None):
        self.co = company
        self.th = thresholds or Thresholds()

    def suggest(self, turn: TurnContext, feat: Optional[CustomerFeatures] = None) -> list[dict]:
        f = feat or CustomerFeatures()
        pool: list[Suggestion] = []
        pool += self._risk(turn, f)
        pool += self._commercial(turn, f)
        pool += self._service(turn, f)
        pool.sort(key=lambda s: s.priority)
        top = pool[:2] if pool else [self._fallback(turn)]
        return [s.dict() for s in top]

    # ── RISK ──────────────────────────────────────────────────

    def _risk(self, t: TurnContext, f: CustomerFeatures) -> list[Suggestion]:
        s: list[Suggestion] = []
        c = self.co.customer

        if f.is_repeat_unhappy:
            s.append(Suggestion("risk", "REPEAT_UNHAPPY", 1,
                f"{c.capitalize()} con experiencia negativa repetida",
                (f"Tuvo puntajes bajos en {f.nps_low_count} visitas. "
                 "Escucha activa primero — sin interrumpir. "
                 "No ofrezcas nada comercial en esta atencion."),
                [f"NPS promedio: {f.nps:.1f}/5" if f.nps else "",
                 f"Veces NPS bajo: {f.nps_low_count}"]))
        elif f.is_unhappy:
            s.append(Suggestion("risk", "UNHAPPY", 2,
                "Experiencia negativa en visita anterior",
                (f"Este {c} tuvo una mala experiencia previamente. "
                 "Reconoce la situacion antes de avanzar."),
                [f"NPS: {f.nps:.1f}/5" if f.nps else "NPS bajo registrado"]))

        intent = self.co.intent(t.queue_name)
        if intent == "complaint":
            s.append(Suggestion("risk", "COMPLAINT_QUEUE", 2,
                "Cola de quejas — escucha activa",
                (f"El {c} viene a reclamar. Escucha el problema completo "
                 "antes de responder. Confirma que entendiste antes de actuar."),
                [f"Cola: {t.queue_name}"]))

        if t.wait_min >= self.th.wait_critical:
            s.append(Suggestion("risk", "WAIT_CRITICAL", 2,
                f"Espera critica ({t.wait_min:.0f} min)",
                (f"Espero {t.wait_min:.0f} minutos — muy por encima del promedio. "
                 "Disculpate por la demora antes de cualquier gestion."),
                [f"Espera: {t.wait_min:.0f} min | Umbral: {self.th.wait_critical:.0f} min"]))
        elif t.wait_min >= self.th.wait_high:
            s.append(Suggestion("risk", "WAIT_HIGH", 3,
                f"Espera elevada ({t.wait_min:.0f} min)",
                f"Espero {t.wait_min:.0f} minutos. Reconoce la espera al saludar.",
                [f"Espera: {t.wait_min:.0f} min | Umbral: {self.th.wait_high:.0f} min"]))

        if f.is_recent and f.visitas > 1:
            s.append(Suggestion("risk", "RECENT_REPEAT", 4,
                "Segunda visita esta semana",
                (f"El {c} ya vino esta semana. "
                 "Pregunta si hay algo pendiente de la visita anterior."),
                [f"Ultima visita hace {f.dias_ultima_visita} dia(s)"]))

        return s

    # ── COMMERCIAL ────────────────────────────────────────────

    def _commercial(self, t: TurnContext, f: CustomerFeatures) -> list[Suggestion]:
        s: list[Suggestion] = []
        intent = self.co.intent(t.queue_name)

        if f.is_repeat_unhappy or intent == "complaint":
            return s

        if intent == "commercial":
            hint = self.co.hint(t.queue_name)
            detail = f" — {hint}" if hint else ""
            s.append(Suggestion("commercial", "REINFORCE", 5,
                "Cola con alta intencion comercial",
                (f"El {self.co.customer} vino por una gestion comercial{detail}. "
                 "Confirma que no tiene dudas antes de cerrar. "
                 "Ofrece productos complementarios al finalizar, no al inicio."),
                [f"Cola: {t.queue_name}"],
                confidence=0.8))

        elif intent == "operational":
            hint = self.co.hint(t.queue_name)
            detail = f" ({hint})" if hint else ""
            s.append(Suggestion("commercial", "DIGITAL_REDIRECT", 7,
                "Oportunidad de derivacion digital",
                (f"Este tramite{detail} se puede hacer desde {self.co.digital}. "
                 f"Al finalizar, mostra como {self.co.digital_verb}."),
                [f"Cola operativa: {t.queue_name}"],
                confidence=0.7))

        if f.segment and f.segment.upper() in ("PREMIUM", "PLATINUM", "GOLD", "SELECT", "VIP"):
            s.append(Suggestion("commercial", "PREMIUM_CLIENT", 5,
                f"{self.co.customer.capitalize()} {f.segment}",
                (f"Segmento {f.segment}. Asegurate de que salga con todas "
                 "sus consultas resueltas. Ofrece derivar a un ejecutivo "
                 "si el tramite lo requiere."),
                [f"Segmento: {f.segment}"],
                confidence=0.9))

        return s

    # ── SERVICE ───────────────────────────────────────────────

    def _service(self, t: TurnContext, f: CustomerFeatures) -> list[Suggestion]:
        s: list[Suggestion] = []
        hint = self.co.hint(t.queue_name)

        if hint:
            s.append(Suggestion("service", "PROCESS_CONTEXT", 6,
                "Contexto del proceso disponible",
                (f"Proceso: '{hint}'. "
                 f"Usa este contexto para anticipar la necesidad del {self.co.customer}."),
                [f"Descripcion cargada para '{t.queue_base}'"],
                confidence=0.85))

        if f.is_new:
            s.append(Suggestion("service", "FIRST_VISIT", 7,
                "Primera visita registrada",
                (f"Sin historial previo. Al finalizar, presenta {self.co.digital} "
                 f"y explica como {self.co.digital_verb}."),
                ["Sin historial en el sistema"]))

        if f.visitas >= self.th.visits_frequent:
            s.append(Suggestion("service", "FREQUENT_NO_APPT", 8,
                f"{self.co.customer.capitalize()} frecuente ({f.visitas} visitas)",
                (f"Viene seguido. Al cerrar, mostra como agendar turno "
                 f"desde {self.co.digital} para evitar esperas."),
                [f"Visitas: {f.visitas}"]))

        if t.is_peak:
            s.append(Suggestion("service", "PEAK_HOUR", 9,
                "Hora pico — resolucion eficiente",
                (f"Alta demanda ahora. Resolve el tramite principal. "
                 f"Consultas adicionales, derivalas a {self.co.digital}."),
                [f"Hora: {t.hour}:00 hs"],
                confidence=0.75))

        return s

    # ── FALLBACK ──────────────────────────────────────────────

    def _fallback(self, t: TurnContext) -> Suggestion:
        return Suggestion("service", "STANDARD", 10,
            "Atencion estandar",
            (f"Gestion de '{t.queue_name}'. "
             f"Al finalizar, invita al {self.co.customer} a usar {self.co.digital}."),
            ["Sin señales especificas"],
            confidence=1.0)
