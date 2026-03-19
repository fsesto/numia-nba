"""
NBA Operador — Motor de reglas (Fase 1 / MVP)
Calibrado con datos reales de Bancoomeva (qmovements_2026-03-18.csv)

Recibe el contexto del turno en el momento de LLAMADA
y devuelve hasta 2 sugerencias priorizadas para el operador.

Uso:
    engine = NBAEngine(db_conn)
    sugerencias = engine.suggest(turn_context)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────

def queue_base(queue_name: str) -> str:
    """
    Extrae el nombre base de la cola eliminando el sufijo geográfico.
    Bancoomeva usa sufijos de una letra mayúscula: 'Caja A' → 'Caja'
    Funciona para cualquier banco que use este patrón.
    Ejemplo: 'Venta de Productos C' → 'Venta de Productos'
    """
    return re.sub(r'\s+[A-Z]$', '', queue_name.strip())


# ──────────────────────────────────────────────────────────────
# MODELOS DE DATOS
# ──────────────────────────────────────────────────────────────

@dataclass
class TurnContext:
    """Lo que llega en el momento de la LLAMADA desde qmovements."""
    turn_id: str
    turn_email: Optional[str]
    queue_name: str
    branch_name: str
    operator_id: str
    wait_time_seconds: float
    llamada_ts: datetime = field(default_factory=datetime.now)
    customer_id: Optional[str] = None      # 0% cobertura en Bancoomeva, queda como fallback
    appointment_code: Optional[str] = None # 0% cobertura en Bancoomeva, ignorado en reglas

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
        # Hora pico calibrada con datos reales Bancoomeva
        return self.hour in (10, 11, 15, 16)


@dataclass
class CustomerFeatures:
    """Features del cliente desde nba_customer_features (batch nocturno)."""
    email: str

    # Volumen y recencia
    visitas_total: int = 0
    dias_desde_ultima_visita: Optional[int] = None
    ultima_cola: Optional[str] = None
    cola_mas_frecuente: Optional[str] = None
    sucursales_distintas: int = 0

    # Tiempos históricos
    atencion_prom_min: Optional[float] = None

    # Satisfacción
    encuestas_respondidas: int = 0
    nps_promedio: Optional[float] = None
    nps_minimo: Optional[float] = None
    veces_nps_bajo: int = 0

    # Flags pre-calculados en el feature store
    flag_cliente_insatisfecho: bool = False
    flag_visita_reciente: bool = False      # volvió en ≤ 7 días
    flag_primera_visita: bool = False
    flag_insatisfaccion_repetida: bool = False

    # CRM (Fase 2 — None hasta integración)
    segmento: Optional[str] = None              # 'PLATINUM' | 'GOLD' | 'CLASICO'
    productos_activos: Optional[int] = None
    propension_credito: Optional[float] = None
    propension_inversion: Optional[float] = None
    propension_seguro: Optional[float] = None


@dataclass
class Suggestion:
    layer: str          # 'risk' | 'commercial' | 'service'
    action: str
    priority: int       # 1 = más urgente
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
            "confidence": self.confidence,
        }


# ──────────────────────────────────────────────────────────────
# UMBRALES — calibrados con datos reales Bancoomeva
# ──────────────────────────────────────────────────────────────

class Thresholds:
    NPS_BAJO             = 2.5
    # p75 espera = 16 min, p90 = 33 min
    ESPERA_LARGA_MIN     = 16
    ESPERA_MUY_LARGA_MIN = 33
    VISITA_RECIENTE_DIAS = 7
    PROPENSION_ALTA      = 0.65


# ──────────────────────────────────────────────────────────────
# CLASIFICACIÓN DE COLAS — base names (sin sufijo geográfico)
# Bancoomeva: A=Armenia, C=Cali, T=Tunja, P=Popayán/Palmira
# Las reglas se aplican sobre queue_base(), no sobre el nombre exacto.
# ──────────────────────────────────────────────────────────────

# Colas operativas / transaccionales
COLAS_OPERATIVAS: set[str] = {
    "Caja",
    "CAJA - BANCOOMEVA",
    "Pila",             # PILA = aportes parafiscales, trámite rutinario
}

# Cola de reclamos — cliente viene predispuesto
COLAS_RECLAMO: set[str] = {
    "Felicitaciones- Quejas y Reclamos",
    "Felicitaciones, Quejas y Reclamos",
    "Felicitaciones- Quejas y Recla",   # variante truncada
}

# Colas comerciales — alto potencial de cross-sell
COLAS_COMERCIALES: set[str] = {
    "Venta de Productos",
    "Asesores Integrales",
    "Asesores Soluciones Comerciales",
    "Asesor Turismo",
}

# Colas complejas — atención típicamente > 15 min (validado en datos)
COLAS_COMPLEJAS: set[str] = {
    "Venta de Productos",       # media 17 min
    "Asesores Integrales",      # media 15 min
    "Asesores de Salud",
    "Asesores Soluciones Comerciales",
    "Asesor Turismo",
    "Otros Servicios",
}

# Colas de salud (Bancoomeva es cooperativa con beneficios de salud)
COLAS_SALUD: set[str] = {
    "Asesores de Salud",
}

# Mapa cola base → producto a mencionar (MVP sin CRM)
COLA_A_PRODUCTO: dict[str, str] = {
    "Caja":                             "Canales digitales — homebanking o app para operar sin venir",
    "Pila":                             "Pago automático de PILA desde la app, sin filas",
    "Venta de Productos":               "Confirmar si tiene dudas sobre cobertura del producto",
    "Asesores Integrales":              "Recordar beneficios de salud y turismo disponibles como asociado",
    "Asesor Turismo":                   "Paquetes adicionales de turismo y recreación para asociados",
    "Asesores de Salud":                "Medicina prepagada y servicios de salud complementarios",
}


# ──────────────────────────────────────────────────────────────
# MOTOR PRINCIPAL
# ──────────────────────────────────────────────────────────────

class NBAEngine:
    """
    Motor de Next-Best-Action para operadores.
    Fase 1 (MVP): sistema de reglas determinístico.
    Fase 2: reemplazar _evaluate_commercial() por scoring CRM + ML.
    """

    def __init__(self, db_conn=None):
        self.db = db_conn
        self.t = Thresholds()

    # ── ENTRADA PÚBLICA ────────────────────────────────────────

    def suggest(self, ctx: TurnContext) -> list[dict]:
        """Devuelve hasta 2 sugerencias priorizadas para el operador."""
        features = self._load_features(ctx)

        candidates: list[Suggestion] = []
        candidates += self._evaluate_risk(ctx, features)
        candidates += self._evaluate_commercial(ctx, features)
        candidates += self._evaluate_service_mode(ctx, features)

        candidates.sort(key=lambda s: s.priority)
        top = candidates[:2]

        if not top:
            top = [self._default_suggestion(ctx)]

        logger.info(
            "NBA | turn=%s email=%s queue=%s (base=%s) -> %s",
            ctx.turn_id, ctx.turn_email, ctx.queue_name, ctx.queue_base,
            [s.action for s in top],
        )
        return [s.to_dict() for s in top]

    # ── CARGA DE FEATURES ──────────────────────────────────────

    def _load_features(self, ctx: TurnContext) -> Optional[CustomerFeatures]:
        """
        Busca features por turn_email (83% cobertura en Bancoomeva).
        """
        identifier = ctx.turn_email or ctx.customer_id
        if not identifier or not self.db:
            return None

        field_name = "turn_email" if ctx.turn_email else "customer_id"
        try:
            from sqlalchemy import text
            with self.db.connect() as conn:
                row = conn.execute(
                    text(f"SELECT * FROM nba_customer_features WHERE {field_name} = :id"),
                    {"id": identifier},
                ).fetchone()
            if not row:
                return None
            r = dict(row._mapping)
            return CustomerFeatures(
                email=r.get("turn_email", identifier),
                visitas_total=r.get("visitas_total", 0),
                dias_desde_ultima_visita=r.get("dias_desde_ultima_visita"),
                ultima_cola=r.get("ultima_cola"),
                cola_mas_frecuente=r.get("cola_mas_frecuente"),
                sucursales_distintas=r.get("sucursales_distintas", 0),
                atencion_prom_min=r.get("atencion_prom_min"),
                encuestas_respondidas=r.get("encuestas_respondidas", 0),
                nps_promedio=r.get("nps_promedio"),
                nps_minimo=r.get("nps_minimo"),
                veces_nps_bajo=r.get("veces_nps_bajo", 0),
                flag_cliente_insatisfecho=bool(r.get("flag_cliente_insatisfecho")),
                flag_visita_reciente=bool(r.get("flag_visita_reciente")),
                flag_primera_visita=bool(r.get("flag_primera_visita")),
                flag_insatisfaccion_repetida=bool(r.get("flag_insatisfaccion_repetida")),
                segmento=r.get("segmento"),
                productos_activos=r.get("productos_activos"),
                propension_credito=r.get("propension_credito"),
                propension_inversion=r.get("propension_inversion"),
                propension_seguro=r.get("propension_seguro"),
            )
        except Exception as e:
            logger.error("Error cargando features para %s=%s: %s", field_name, identifier, e)
            return None

    # ── CAPA 1: RIESGO ─────────────────────────────────────────

    def _evaluate_risk(
        self, ctx: TurnContext, f: Optional[CustomerFeatures]
    ) -> list[Suggestion]:
        suggestions = []

        # R1: Insatisfacción repetida — no avanzar con ninguna oferta
        if f and f.flag_insatisfaccion_repetida:
            suggestions.append(Suggestion(
                layer="risk",
                action="INSATISFACCION_REPETIDA",
                priority=1,
                label="Asociado con experiencia negativa repetida",
                message=(
                    f"Este asociado dio puntajes bajos en {f.veces_nps_bajo} visitas anteriores. "
                    "Escuchá primero. No avances con ninguna oferta comercial en esta atención."
                ),
                evidence=[
                    f"NPS promedio: {f.nps_promedio:.1f}/5" if f.nps_promedio else "NPS bajo registrado",
                    f"Veces con NPS ≤ 2: {f.veces_nps_bajo}",
                ],
            ))

        # R2: Insatisfacción puntual
        elif f and f.flag_cliente_insatisfecho:
            suggestions.append(Suggestion(
                layer="risk",
                action="ASOCIADO_INSATISFECHO",
                priority=2,
                label="Experiencia negativa en visita previa",
                message=(
                    "El asociado tuvo una experiencia negativa anteriormente. "
                    "Reconocé la situación antes de avanzar con el trámite."
                ),
                evidence=[
                    f"NPS última visita: {f.nps_minimo:.1f}/5" if f.nps_minimo else "NPS bajo",
                ],
            ))

        # R3: Cola de quejas — ya viene predispuesto
        if ctx.queue_base in COLAS_RECLAMO:
            suggestions.append(Suggestion(
                layer="risk",
                action="COLA_QUEJA",
                priority=2,
                label="Cola de quejas y reclamos",
                message=(
                    "El asociado viene a presentar una queja o reclamo. "
                    "Escuchá el problema completo antes de responder. "
                    "Confirmá que entendiste antes de ofrecer soluciones."
                ),
                evidence=[f"Cola: '{ctx.queue_name}'"],
            ))

        # R4: Espera muy larga (p90 = 33 min)
        if ctx.wait_minutes >= self.t.ESPERA_MUY_LARGA_MIN:
            suggestions.append(Suggestion(
                layer="risk",
                action="ESPERA_MUY_LARGA",
                priority=2,
                label=f"Espera muy larga ({ctx.wait_minutes:.0f} min)",
                message=(
                    f"El asociado esperó {ctx.wait_minutes:.0f} minutos (por encima del percentil 90). "
                    "Comenzá disculpándote — reduce riesgo de reclamo y mejora el NPS final."
                ),
                evidence=[f"Espera: {ctx.wait_minutes:.0f} min | p90 sucursal: {self.t.ESPERA_MUY_LARGA_MIN} min"],
            ))
        elif ctx.wait_minutes >= self.t.ESPERA_LARGA_MIN:
            suggestions.append(Suggestion(
                layer="risk",
                action="ESPERA_LARGA",
                priority=3,
                label=f"Espera por encima del promedio ({ctx.wait_minutes:.0f} min)",
                message=(
                    f"El asociado esperó {ctx.wait_minutes:.0f} minutos. "
                    "Reconocé la espera al saludar."
                ),
                evidence=[f"Espera: {ctx.wait_minutes:.0f} min | p75 sucursal: {self.t.ESPERA_LARGA_MIN} min"],
            ))

        # R5: Reincidencia reciente — posible problema no resuelto
        if f and f.flag_visita_reciente and f.visitas_total > 1:
            suggestions.append(Suggestion(
                layer="risk",
                action="REINCIDENCIA_RECIENTE",
                priority=4,
                label="Segunda visita esta semana",
                message=(
                    "El asociado ya vino esta semana. "
                    "Preguntá si hay algo pendiente de la visita anterior "
                    "antes de iniciar el trámite actual."
                ),
                evidence=[f"Última visita hace {f.dias_desde_ultima_visita} día(s)"],
            ))

        return suggestions

    # ── CAPA 2: COMERCIAL ──────────────────────────────────────

    def _evaluate_commercial(
        self, ctx: TurnContext, f: Optional[CustomerFeatures]
    ) -> list[Suggestion]:
        """
        MVP: reglas por queue_base.
        Fase 2: activar con propension_* del CRM de Bancoomeva.
        """
        suggestions = []

        # Nunca hacer ofertas a asociados en riesgo o en cola de reclamos
        if f and f.flag_insatisfaccion_repetida:
            return suggestions
        if ctx.queue_base in COLAS_RECLAMO:
            return suggestions

        # ── Con CRM (Fase 2) ──────────────────────────────────

        if f and f.propension_credito and f.propension_credito >= self.t.PROPENSION_ALTA:
            suggestions.append(Suggestion(
                layer="commercial",
                action="OFERTA_CREDITO",
                priority=5,
                label=f"Propensión alta a crédito ({f.propension_credito:.0%})",
                message=(
                    "CRM indica propensión alta a crédito. "
                    "Al cerrar el trámite principal, presentá la simulación de cuotas. "
                    "No al inicio de la atención."
                ),
                evidence=[f"Propensión CRM: {f.propension_credito:.0%}"],
                confidence=f.propension_credito,
            ))

        elif f and f.propension_inversion and f.propension_inversion >= self.t.PROPENSION_ALTA:
            suggestions.append(Suggestion(
                layer="commercial",
                action="OFERTA_INVERSION",
                priority=5,
                label=f"Propensión alta a inversión ({f.propension_inversion:.0%})",
                message=(
                    "CRM indica propensión a producto de ahorro/inversión. "
                    "Al cerrar, consultá si tiene liquidez disponible "
                    "y presentá la opción de CDT o fondo de inversión."
                ),
                evidence=[f"Propensión inversión CRM: {f.propension_inversion:.0%}"],
                confidence=f.propension_inversion,
            ))

        elif f and f.propension_seguro and f.propension_seguro >= self.t.PROPENSION_ALTA:
            suggestions.append(Suggestion(
                layer="commercial",
                action="OFERTA_SEGURO",
                priority=5,
                label=f"Propensión alta a seguro ({f.propension_seguro:.0%})",
                message=(
                    "CRM indica propensión a seguro. "
                    "Mencioná brevemente la opción al cerrar el trámite."
                ),
                evidence=[f"Propensión seguro CRM: {f.propension_seguro:.0%}"],
                confidence=f.propension_seguro,
            ))

        # ── Sin CRM: reglas por cola base (MVP) ──────────────

        elif ctx.queue_base in COLAS_COMERCIALES:
            # Cola comercial → el asociado ya tiene intención de producto
            producto = COLA_A_PRODUCTO.get(ctx.queue_base, "")
            if producto:
                suggestions.append(Suggestion(
                    layer="commercial",
                    action="REFUERZO_COMERCIAL",
                    priority=6,
                    label="Cola comercial — reforzar cierre",
                    message=producto,
                    evidence=[f"Cola de alta intención comercial: '{ctx.queue_name}'"],
                    confidence=0.75,
                ))

        elif ctx.queue_base in COLAS_OPERATIVAS:
            # Cola operativa → ofrecer autogestión digital para próximas veces
            producto = COLA_A_PRODUCTO.get(ctx.queue_base, "")
            if producto:
                suggestions.append(Suggestion(
                    layer="commercial",
                    action="DERIVACION_DIGITAL",
                    priority=7,
                    label="Derivar a canal digital",
                    message=(
                        f"Este trámite puede hacerse sin venir a la sucursal. "
                        f"Al finalizar, mostrá cómo: {producto}."
                    ),
                    evidence=[
                        f"Cola operativa con equivalente digital: '{ctx.queue_name}'",
                        "Reduce próximas visitas y mejora experiencia del asociado",
                    ],
                    confidence=0.7,
                ))

        # Segmento premium (cuando llegue CRM)
        if f and f.segmento in ("PLATINUM", "GOLD") and not f.propension_credito:
            suggestions.append(Suggestion(
                layer="commercial",
                action="ASOCIADO_PREMIUM",
                priority=5,
                label=f"Asociado {f.segmento} — atención diferenciada",
                message=(
                    f"Asociado segmento {f.segmento}. "
                    "Asegurate de que salga con todas sus consultas resueltas. "
                    "Ofrecé derivar a ejecutivo de cuenta si el trámite lo requiere."
                ),
                evidence=[f"Segmento CRM: {f.segmento}"],
                confidence=0.9,
            ))

        return suggestions

    # ── CAPA 3: MODO DE ATENCIÓN ───────────────────────────────

    def _evaluate_service_mode(
        self, ctx: TurnContext, f: Optional[CustomerFeatures]
    ) -> list[Suggestion]:
        suggestions = []

        # S1: Trámite complejo — gestionar expectativas de tiempo
        if ctx.queue_base in COLAS_COMPLEJAS:
            suggestions.append(Suggestion(
                layer="service",
                action="TRAMITE_COMPLEJO",
                priority=6,
                label="Trámite complejo — informar tiempo estimado",
                message=(
                    f"'{ctx.queue_name}' tiene atención promedio de 15-17 min. "
                    "Informá el tiempo estimado al inicio "
                    "para gestionar las expectativas del asociado."
                ),
                evidence=[f"Cola con atención media > 15 min: '{ctx.queue_name}'"],
                confidence=0.9,
            ))

        # S2: Cola de salud — requiere contexto médico
        if ctx.queue_base in COLAS_SALUD:
            suggestions.append(Suggestion(
                layer="service",
                action="ATENCION_SALUD",
                priority=6,
                label="Cola de salud — validar cobertura al inicio",
                message=(
                    "Validá el plan de salud del asociado al inicio de la atención "
                    "para confirmar qué servicios cubre antes de avanzar."
                ),
                evidence=["Cola de salud — cobertura varía por plan"],
                confidence=1.0,
            ))

        # S3: Primera visita — presentar canales y beneficios
        if not f or f.flag_primera_visita:
            suggestions.append(Suggestion(
                layer="service",
                action="PRIMERA_VISITA",
                priority=7,
                label="Primera visita registrada",
                message=(
                    "Sin historial previo. Al finalizar, presentá la app y el homebanking. "
                    "Mencioná brevemente los beneficios de ser asociado Coomeva."
                ),
                evidence=["Sin historial previo en el sistema"],
                confidence=0.9,
            ))

        # S4: Asociado frecuente sin turno — activar agendamiento
        if f and f.visitas_total >= 6 and ctx.appointment_code is None:
            suggestions.append(Suggestion(
                layer="service",
                action="FRECUENTE_SIN_TURNO",
                priority=8,
                label="Asociado frecuente sin turno previo",
                message=(
                    f"El asociado vino {f.visitas_total} veces pero nunca con turno agendado. "
                    "Al cerrar, mostrá cómo sacar turno online para evitar esperas futuras."
                ),
                evidence=[
                    f"Visitas históricas: {f.visitas_total}",
                    "Vino sin turno hoy",
                ],
                confidence=0.8,
            ))

        # S5: Hora pico — foco en resolución en primera instancia
        if ctx.is_peak_hour:
            suggestions.append(Suggestion(
                layer="service",
                action="HORA_PICO",
                priority=9,
                label="Hora pico — resolución en primera instancia",
                message=(
                    "Alta demanda ahora. "
                    "Resolvé el trámite principal en esta atención. "
                    "Derivá consultas adicionales al homebanking o a un nuevo turno."
                ),
                evidence=[f"Hora pico: {ctx.hour}:00 hs"],
                confidence=0.75,
            ))

        return suggestions

    # ── FALLBACK ───────────────────────────────────────────────

    def _default_suggestion(self, ctx: TurnContext) -> Suggestion:
        return Suggestion(
            layer="service",
            action="ATENCION_ESTANDAR",
            priority=10,
            label="Atención estándar",
            message=(
                f"Gestión de '{ctx.queue_name}'. "
                "Al finalizar, invitá al asociado a usar la app para próximas operaciones."
            ),
            evidence=["Sin señales específicas detectadas"],
            confidence=1.0,
        )


# ──────────────────────────────────────────────────────────────
# TEST OFFLINE — casos reales de Bancoomeva
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    engine = NBAEngine(db_conn=None)

    casos = [
        ("Caja A + espera p90", TurnContext(
            turn_id="T-001", turn_email="wilpos24@gmail.com",
            queue_name="Caja A", branch_name="EJEARM Norte Armenia",
            operator_id="OP-42", wait_time_seconds=2100,  # 35 min
            llamada_ts=datetime(2026, 1, 2, 11, 0),
        )),
        ("Venta de Productos C + hora pico", TurnContext(
            turn_id="T-002", turn_email="mariap_gamboa@coomeva.com.co",
            queue_name="Venta de Productos C", branch_name="CALCAL Chipichape Cali",
            operator_id="OP-15", wait_time_seconds=1200,  # 20 min
            llamada_ts=datetime(2026, 1, 2, 10, 30),
        )),
        ("Quejas + espera larga", TurnContext(
            turn_id="T-003", turn_email=None,
            queue_name="Felicitaciones- Quejas y Reclamos A", branch_name="MDEMDE Oviedo Medellin",
            operator_id="OP-08", wait_time_seconds=600,   # 10 min
            llamada_ts=datetime(2026, 1, 2, 15, 0),
        )),
        ("Asesores Integrales T + primera visita", TurnContext(
            turn_id="T-004", turn_email="nuevo@correo.com",
            queue_name="Asesores Integrales T", branch_name="CALCAL Oasis Unicentro",
            operator_id="OP-22", wait_time_seconds=400,
            llamada_ts=datetime(2026, 1, 2, 9, 0),
        )),
    ]

    for titulo, ctx in casos:
        print(f"\n=== {titulo} (queue_base='{ctx.queue_base}') ===")
        result = engine.suggest(ctx)
        for s in result:
            print(f"  [{s['layer'].upper()}] {s['label']}")
            print(f"  -> {s['message']}")
