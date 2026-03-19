"""
NBA Engine v2 — Sector-aware
Adaptable a banca, salud, retail, gobierno, telco y otro.
El contexto de empresa y las descripciones de procesos mejoran las sugerencias.
"""

from __future__ import annotations
import re
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# CONFIGURACIÓN POR SECTOR
# ──────────────────────────────────────────────────────────────

SECTOR_CONFIG: dict[str, dict] = {
    "banca": {
        "customer_label": "cliente",
        "complaint_kw":    ["reclamo", "queja", "felicitaci", "oirs"],
        "commercial_kw":   ["venta", "comercial", "préstamo", "prestamo", "crédito", "credito", "inversion", "inversión", "seguro", "hipoteca", "tarjeta"],
        "operational_kw":  ["caja", "transacci", "pago", "depósito", "deposito", "extracci", "transferencia", "pila", "cambio divisa"],
        "digital_channel": "homebanking o la app del banco",
        "digital_action":  "operar sin venir a la sucursal",
    },
    "salud": {
        "customer_label": "paciente",
        "complaint_kw":   ["reclamo", "queja", "felicitaci", "oirs"],
        "commercial_kw":  ["venta", "plan", "prepaga", "seguro", "medicin"],
        "operational_kw": ["caja", "pago", "extracci", "muestra", "resultado", "turismo", "pila"],
        "digital_channel": "el portal de turnos online o la app",
        "digital_action":  "agendar turno sin venir",
    },
    "retail": {
        "customer_label": "cliente",
        "complaint_kw":   ["reclamo", "queja", "devoluci", "garantía", "garantia"],
        "commercial_kw":  ["venta", "promoci", "crédito", "credito", "fidelidad", "puntos"],
        "operational_kw": ["caja", "pago", "retiro", "cambio"],
        "digital_channel": "la tienda online o la app",
        "digital_action":  "comprar y gestionar sin ir a la tienda",
    },
    "gobierno": {
        "customer_label": "ciudadano",
        "complaint_kw":   ["reclamo", "queja", "denuncia", "recurso"],
        "commercial_kw":  [],
        "operational_kw": ["trámite", "tramite", "certificado", "documento", "caja", "pago"],
        "digital_channel": "el portal de trámites online",
        "digital_action":  "hacer el trámite sin venir presencialmente",
    },
    "telco": {
        "customer_label": "cliente",
        "complaint_kw":   ["reclamo", "queja", "avería", "averia", "falla", "técnico", "tecnico"],
        "commercial_kw":  ["venta", "upgrade", "plan", "portabilidad", "renovaci"],
        "operational_kw": ["pago", "factura", "caja", "recarga"],
        "digital_channel": "la app o el portal de autogestión",
        "digital_action":  "gestionar sin ir a la sucursal",
    },
    "otro": {
        "customer_label": "cliente",
        "complaint_kw":   ["reclamo", "queja"],
        "commercial_kw":  ["venta", "comercial"],
        "operational_kw": ["caja", "pago"],
        "digital_channel": "los canales digitales disponibles",
        "digital_action":  "gestionar sin necesidad de venir",
    },
}


# ──────────────────────────────────────────────────────────────
# CONTEXTO DE EMPRESA
# ──────────────────────────────────────────────────────────────

@dataclass
class CompanyContext:
    company_name: str = ""
    sector: str = "banca"
    process_descriptions: str = ""   # texto libre: "Caja: pagos básicos\nVenta: cross-sell..."

    def __post_init__(self):
        self.sector = self.sector.lower()
        if self.sector not in SECTOR_CONFIG:
            self.sector = "otro"
        self._cfg = SECTOR_CONFIG[self.sector]
        self._parsed_processes = self._parse_processes()

    def _parse_processes(self) -> dict[str, str]:
        """Parsea 'Cola: descripción' line by line."""
        result = {}
        for line in self.process_descriptions.splitlines():
            if ":" in line:
                parts = line.split(":", 1)
                key = parts[0].strip().lower()
                val = parts[1].strip().lower()
                result[key] = val
        return result

    @property
    def customer_label(self) -> str:
        return self._cfg["customer_label"]

    @property
    def digital_channel(self) -> str:
        return self._cfg["digital_channel"]

    @property
    def digital_action(self) -> str:
        return self._cfg["digital_action"]

    def queue_intent(self, queue_name: str) -> str:
        """
        Clasifica la intención de una cola: 'complaint' | 'commercial' | 'operational' | 'other'
        Prioriza la descripción del proceso si fue definida, luego usa keywords del sector.
        """
        q_lower = queue_name.lower()
        q_base = queue_base(queue_name).lower()

        # 1. Buscar descripción explícita del proceso
        desc = self._parsed_processes.get(q_lower) or self._parsed_processes.get(q_base) or ""

        # 2. Texto a analizar = nombre + descripción
        text = f"{q_lower} {desc}"

        cfg = self._cfg
        if any(kw in text for kw in cfg["complaint_kw"]):
            return "complaint"
        if any(kw in text for kw in cfg["commercial_kw"]):
            return "commercial"
        if any(kw in text for kw in cfg["operational_kw"]):
            return "operational"
        return "other"

    def process_hint(self, queue_name: str) -> Optional[str]:
        """Devuelve la descripción del proceso si fue ingresada por el usuario."""
        q_lower = queue_name.lower()
        q_base = queue_base(queue_name).lower()
        return self._parsed_processes.get(q_lower) or self._parsed_processes.get(q_base)


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────

def queue_base(queue_name: str) -> str:
    """Elimina sufijo geográfico de una letra: 'Caja A' → 'Caja'."""
    return re.sub(r'\s+[A-Z]$', '', queue_name.strip())


# ──────────────────────────────────────────────────────────────
# MODELOS DE DATOS
# ──────────────────────────────────────────────────────────────

@dataclass
class TurnContext:
    turn_id: str
    queue_name: str
    branch_name: str
    operator_id: str
    wait_time_seconds: float
    turn_email: Optional[str] = None
    customer_id: Optional[str] = None
    llamada_ts: datetime = field(default_factory=datetime.now)

    @property
    def queue_base(self) -> str:
        return queue_base(self.queue_name)

    @property
    def wait_minutes(self) -> float:
        return self.wait_time_seconds / 60.0

    @property
    def hour(self) -> int:
        return self.llamada_ts.hour

    @property
    def is_peak_hour(self) -> bool:
        return self.hour in (10, 11, 15, 16)


@dataclass
class CustomerFeatures:
    email: str = ""
    visitas_total: int = 0
    dias_desde_ultima_visita: Optional[int] = None
    encuestas_respondidas: int = 0
    nps_promedio: Optional[float] = None
    nps_minimo: Optional[float] = None
    veces_nps_bajo: int = 0
    sucursales_distintas: int = 0
    # Flags
    flag_cliente_insatisfecho: bool = False
    flag_visita_reciente: bool = False
    flag_primera_visita: bool = False
    flag_insatisfaccion_repetida: bool = False
    # CRM (Fase 2)
    segmento: Optional[str] = None
    propension_credito: Optional[float] = None
    propension_inversion: Optional[float] = None
    propension_seguro: Optional[float] = None


@dataclass
class Suggestion:
    layer: str       # 'risk' | 'commercial' | 'service'
    action: str
    priority: int
    label: str
    message: str
    evidence: list[str]
    confidence: float = 1.0

    def to_dict(self) -> dict:
        return {
            "layer": self.layer,
            "action": self.action,
            "priority": self.priority,
            "label": self.label,
            "message": self.message,
            "evidence": self.evidence,
            "confidence": round(self.confidence, 2),
        }


# ──────────────────────────────────────────────────────────────
# UMBRALES
# ──────────────────────────────────────────────────────────────

class Thresholds:
    NPS_BAJO             = 2.5
    ESPERA_LARGA_MIN     = 16    # p75 Bancoomeva
    ESPERA_MUY_LARGA_MIN = 33    # p90 Bancoomeva
    VISITA_RECIENTE_DIAS = 7
    PROPENSION_ALTA      = 0.65
    VISITAS_FRECUENTE    = 6


# ──────────────────────────────────────────────────────────────
# MOTOR v2
# ──────────────────────────────────────────────────────────────

class NBAEngineV2:

    def __init__(self, company_context: Optional[CompanyContext] = None, db_conn=None):
        self.ctx = company_context or CompanyContext()
        self.db = db_conn
        self.t = Thresholds()

    def suggest(self, turn: TurnContext, features: Optional[CustomerFeatures] = None) -> list[dict]:
        if features is None:
            features = self._load_features(turn)

        candidates: list[Suggestion] = []
        candidates += self._risk(turn, features)
        candidates += self._commercial(turn, features)
        candidates += self._service(turn, features)

        candidates.sort(key=lambda s: s.priority)
        top = candidates[:2]
        if not top:
            top = [self._default(turn)]

        return [s.to_dict() for s in top]

    # ── CARGA DE FEATURES ──────────────────────────────────────

    def _load_features(self, turn: TurnContext) -> Optional[CustomerFeatures]:
        identifier = turn.turn_email or turn.customer_id
        if not identifier or not self.db:
            return None
        try:
            from sqlalchemy import text
            with self.db.connect() as conn:
                field = "turn_email" if turn.turn_email else "customer_id"
                row = conn.execute(
                    text(f"SELECT * FROM nba_customer_features WHERE {field} = :id"),
                    {"id": identifier}
                ).fetchone()
            if not row:
                return None
            r = dict(row._mapping)
            return CustomerFeatures(
                email=identifier,
                visitas_total=r.get("visitas_total", 0),
                dias_desde_ultima_visita=r.get("dias_desde_ultima_visita"),
                encuestas_respondidas=r.get("encuestas_respondidas", 0),
                nps_promedio=r.get("nps_promedio"),
                nps_minimo=r.get("nps_minimo"),
                veces_nps_bajo=r.get("veces_nps_bajo", 0),
                flag_cliente_insatisfecho=bool(r.get("flag_cliente_insatisfecho")),
                flag_visita_reciente=bool(r.get("flag_visita_reciente")),
                flag_primera_visita=bool(r.get("flag_primera_visita")),
                flag_insatisfaccion_repetida=bool(r.get("flag_insatisfaccion_repetida")),
                segmento=r.get("segmento"),
                propension_credito=r.get("propension_credito"),
                propension_inversion=r.get("propension_inversion"),
                propension_seguro=r.get("propension_seguro"),
            )
        except Exception as e:
            logger.error("Feature load error: %s", e)
            return None

    # ── CAPA 1: RIESGO ─────────────────────────────────────────

    def _risk(self, turn: TurnContext, f: Optional[CustomerFeatures]) -> list[Suggestion]:
        s = []
        lbl = self.ctx.customer_label.capitalize()

        # Insatisfacción repetida
        if f and f.flag_insatisfaccion_repetida:
            s.append(Suggestion(
                layer="risk", action="INSATISFACCION_REPETIDA", priority=1,
                label=f"{lbl} con experiencia negativa repetida",
                message=(
                    f"Tuvo puntajes bajos en {f.veces_nps_bajo} visitas anteriores. "
                    "Escuchá primero, sin interrumpir. No avances con ninguna oferta en esta atención."
                ),
                evidence=[
                    f"NPS promedio: {f.nps_promedio:.1f}/5" if f.nps_promedio else "NPS bajo registrado",
                    f"Veces con NPS <= 2: {f.veces_nps_bajo}",
                ],
            ))

        elif f and f.flag_cliente_insatisfecho:
            s.append(Suggestion(
                layer="risk", action="INSATISFECHO", priority=2,
                label="Experiencia negativa en visita anterior",
                message=(
                    f"Este {self.ctx.customer_label} tuvo una mala experiencia previamente. "
                    "Reconocé la situación antes de avanzar con el trámite."
                ),
                evidence=[
                    f"NPS ultima visita: {f.nps_minimo:.1f}/5" if f.nps_minimo else "NPS bajo",
                ],
            ))

        # Cola de quejas
        intent = self.ctx.queue_intent(turn.queue_name)
        if intent == "complaint":
            s.append(Suggestion(
                layer="risk", action="COLA_QUEJA", priority=2,
                label="Cola de quejas — escucha activa primero",
                message=(
                    f"El {self.ctx.customer_label} viene a presentar una queja o reclamo. "
                    "Escuchá el problema completo antes de responder. "
                    "Confirmá que entendiste antes de ofrecer soluciones."
                ),
                evidence=[f"Cola '{turn.queue_name}' clasificada como queja/reclamo"],
            ))

        # Espera muy larga
        if turn.wait_minutes >= self.t.ESPERA_MUY_LARGA_MIN:
            s.append(Suggestion(
                layer="risk", action="ESPERA_MUY_LARGA", priority=2,
                label=f"Espera muy larga ({turn.wait_minutes:.0f} min)",
                message=(
                    f"El {self.ctx.customer_label} esperó {turn.wait_minutes:.0f} minutos. "
                    "Comenzá disculpándote antes de cualquier gestión."
                ),
                evidence=[f"Espera: {turn.wait_minutes:.0f} min (p90: {self.t.ESPERA_MUY_LARGA_MIN} min)"],
            ))
        elif turn.wait_minutes >= self.t.ESPERA_LARGA_MIN:
            s.append(Suggestion(
                layer="risk", action="ESPERA_LARGA", priority=3,
                label=f"Espera por encima del promedio ({turn.wait_minutes:.0f} min)",
                message=(
                    f"El {self.ctx.customer_label} esperó {turn.wait_minutes:.0f} minutos. "
                    "Reconocé la espera al saludar."
                ),
                evidence=[f"Espera: {turn.wait_minutes:.0f} min (p75: {self.t.ESPERA_LARGA_MIN} min)"],
            ))

        # Reincidencia
        if f and f.flag_visita_reciente and f.visitas_total > 1:
            s.append(Suggestion(
                layer="risk", action="REINCIDENCIA", priority=4,
                label="Segunda visita esta semana",
                message=(
                    f"El {self.ctx.customer_label} ya vino esta semana. "
                    "Preguntá si hay algo pendiente de la visita anterior."
                ),
                evidence=[f"Ultima visita hace {f.dias_desde_ultima_visita} dia(s)"],
            ))

        return s

    # ── CAPA 2: COMERCIAL ──────────────────────────────────────

    def _commercial(self, turn: TurnContext, f: Optional[CustomerFeatures]) -> list[Suggestion]:
        s = []
        intent = self.ctx.queue_intent(turn.queue_name)

        # No ofrecer nada a clientes en riesgo activo o en cola de queja
        if f and f.flag_insatisfaccion_repetida:
            return s
        if intent == "complaint":
            return s

        # Con CRM — propensiones
        if f and f.propension_credito and f.propension_credito >= self.t.PROPENSION_ALTA:
            s.append(Suggestion(
                layer="commercial", action="OFERTA_CREDITO", priority=5,
                label=f"Propension alta a credito ({f.propension_credito:.0%})",
                message="CRM indica propension alta. Al cerrar el tramite principal, presenta la simulacion de cuotas. No al inicio.",
                evidence=[f"Propension CRM: {f.propension_credito:.0%}"],
                confidence=f.propension_credito,
            ))
        elif f and f.propension_inversion and f.propension_inversion >= self.t.PROPENSION_ALTA:
            s.append(Suggestion(
                layer="commercial", action="OFERTA_INVERSION", priority=5,
                label=f"Propension alta a inversion ({f.propension_inversion:.0%})",
                message="CRM indica propension a inversion. Al cerrar, consulta si tiene liquidez disponible.",
                evidence=[f"Propension inversion: {f.propension_inversion:.0%}"],
                confidence=f.propension_inversion,
            ))
        elif f and f.propension_seguro and f.propension_seguro >= self.t.PROPENSION_ALTA:
            s.append(Suggestion(
                layer="commercial", action="OFERTA_SEGURO", priority=5,
                label=f"Propension alta a seguro ({f.propension_seguro:.0%})",
                message="CRM indica propension a seguro. Menciona la opcion brevemente al cerrar el tramite.",
                evidence=[f"Propension seguro: {f.propension_seguro:.0%}"],
                confidence=f.propension_seguro,
            ))

        # Sin CRM — reglas por intención de cola + contexto del sector
        elif intent == "commercial":
            hint = self.ctx.process_hint(turn.queue_name)
            extra = f" ({hint})" if hint else ""
            s.append(Suggestion(
                layer="commercial", action="REFUERZO_COMERCIAL", priority=6,
                label="Cola comercial — reforzar cierre",
                message=(
                    f"Cola de alta intencion comercial{extra}. "
                    "Confirma si el cliente tiene dudas sobre el producto antes de cerrar. "
                    "No apures el cierre."
                ),
                evidence=[f"Cola '{turn.queue_name}' clasificada como comercial"],
                confidence=0.75,
            ))

        elif intent == "operational":
            hint = self.ctx.process_hint(turn.queue_name)
            extra = f" ({hint})" if hint else ""
            s.append(Suggestion(
                layer="commercial", action="DERIVACION_DIGITAL", priority=7,
                label="Derivar a canal digital",
                message=(
                    f"Este tramite{extra} puede hacerse desde {self.ctx.digital_channel}. "
                    f"Al finalizar, mostra como {self.ctx.digital_action}."
                ),
                evidence=[
                    f"Cola operativa con equivalente digital: '{turn.queue_name}'",
                ],
                confidence=0.7,
            ))

        # Segmento premium
        if f and f.segmento in ("PREMIUM", "PLATINUM", "GOLD", "SELECT"):
            s.append(Suggestion(
                layer="commercial", action="CLIENTE_PREMIUM", priority=5,
                label=f"Cliente {f.segmento} — atencion diferenciada",
                message=(
                    f"Cliente segmento {f.segmento}. "
                    "Asegurate de que salga con todas sus consultas resueltas. "
                    "Ofrece derivar a ejecutivo si el tramite lo requiere."
                ),
                evidence=[f"Segmento CRM: {f.segmento}"],
                confidence=0.9,
            ))

        return s

    # ── CAPA 3: MODO DE ATENCIÓN ───────────────────────────────

    def _service(self, turn: TurnContext, f: Optional[CustomerFeatures]) -> list[Suggestion]:
        s = []
        intent = self.ctx.queue_intent(turn.queue_name)
        hint = self.ctx.process_hint(turn.queue_name)

        # Primera visita
        if not f or f.flag_primera_visita:
            s.append(Suggestion(
                layer="service", action="PRIMERA_VISITA", priority=7,
                label="Primera visita registrada",
                message=(
                    f"Sin historial previo. Al finalizar, presenta {self.ctx.digital_channel} "
                    f"y explica como {self.ctx.digital_action}."
                ),
                evidence=["Sin historial previo en el sistema"],
                confidence=0.9,
            ))

        # Proceso con descripcion explicita → mas especifico
        if hint and intent not in ("complaint",):
            s.append(Suggestion(
                layer="service", action="CONTEXTO_PROCESO", priority=6,
                label="Contexto del proceso disponible",
                message=(
                    f"Este proceso fue descripto como: '{hint}'. "
                    "Usa ese contexto para anticipar la necesidad del cliente "
                    "antes de que termine de explicarla."
                ),
                evidence=[f"Descripcion de proceso cargada para '{turn.queue_base}'"],
                confidence=0.85,
            ))

        # Cliente frecuente sin turno
        if f and f.visitas_total >= self.t.VISITAS_FRECUENTE:
            s.append(Suggestion(
                layer="service", action="FRECUENTE_SIN_TURNO", priority=8,
                label=f"Cliente frecuente ({f.visitas_total} visitas)",
                message=(
                    f"Viene seguido pero sin turno agendado. "
                    f"Al cerrar, mostra como sacar turno desde {self.ctx.digital_channel} "
                    "para evitar esperas en proximas visitas."
                ),
                evidence=[f"Visitas historicas: {f.visitas_total}"],
                confidence=0.8,
            ))

        # Hora pico
        if turn.is_peak_hour:
            s.append(Suggestion(
                layer="service", action="HORA_PICO", priority=9,
                label="Hora pico — resolucion en primera instancia",
                message=(
                    "Alta demanda ahora. Resolve el tramite principal en esta atencion. "
                    f"Deriva consultas adicionales a {self.ctx.digital_channel}."
                ),
                evidence=[f"Hora pico: {turn.hour}:00 hs"],
                confidence=0.75,
            ))

        return s

    # ── FALLBACK ───────────────────────────────────────────────

    def _default(self, turn: TurnContext) -> Suggestion:
        return Suggestion(
            layer="service", action="ATENCION_ESTANDAR", priority=10,
            label="Atencion estandar",
            message=(
                f"Gestion de '{turn.queue_name}'. "
                f"Al finalizar, invita al {self.ctx.customer_label} a usar {self.ctx.digital_channel}."
            ),
            evidence=["Sin señales especificas detectadas"],
            confidence=1.0,
        )
