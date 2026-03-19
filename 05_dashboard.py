"""
NBA Operador — Dashboard de monitoreo (Streamlit)

Muestra:
- Adoption rate del NBA en tiempo real
- A/B: NPS con vs. sin sugerencia útil
- Ranking de acciones por utilidad
- Simulador para testear el motor sin DB

Correr:
    streamlit run 05_dashboard.py
"""

import json
import os
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

# ──────────────────────────────────────────────────────────────
# CONFIGURACIÓN DE PÁGINA
# ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Numia NBA — Monitor",
    page_icon="🎯",
    layout="wide",
)

st.title("🎯 Numia NBA — Next-Best-Action para Operadores")
st.caption("Monitor de adopción y efectividad del motor de sugerencias")


# ──────────────────────────────────────────────────────────────
# CONEXIÓN A LA BASE (OPCIONAL)
# ──────────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    try:
        from sqlalchemy import create_engine
        db_url = os.getenv(
            "DATABASE_URL",
            "mysql+pymysql://user:password@localhost:3306/numia_db"
        )
        return create_engine(db_url, pool_pre_ping=True)
    except Exception:
        return None


engine = get_engine()


def query_df(sql: str) -> pd.DataFrame:
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql(sql, engine)
    except Exception as e:
        st.warning(f"Error en query: {e}")
        return pd.DataFrame()


# ──────────────────────────────────────────────────────────────
# TABS PRINCIPALES
# ──────────────────────────────────────────────────────────────

tab_monitor, tab_ab, tab_acciones, tab_simulador = st.tabs([
    "📊 Adopción",
    "🔬 A/B Test",
    "🏆 Por Acción",
    "🧪 Simulador",
])


# ── TAB 1: ADOPCIÓN GENERAL ────────────────────────────────────

with tab_monitor:
    st.subheader("Adopción semanal del motor NBA")

    if engine:
        df_weekly = query_df("""
            SELECT
                semana_inicio,
                SUM(total_mostradas)    AS sugerencias,
                SUM(utiles)             AS utiles,
                SUM(no_utiles)          AS no_utiles,
                ROUND(SUM(utiles) * 100.0 / NULLIF(SUM(total_mostradas), 0), 1)
                                        AS adoption_pct
            FROM v_nba_adoption_weekly
            GROUP BY semana_inicio
            ORDER BY semana_inicio DESC
            LIMIT 12
        """)
    else:
        # Datos demo si no hay DB
        df_weekly = pd.DataFrame({
            "semana_inicio": pd.date_range(end=datetime.today(), periods=6, freq="W"),
            "sugerencias": [340, 390, 420, 445, 460, 480],
            "utiles": [102, 132, 160, 187, 207, 220],
            "no_utiles": [68, 78, 80, 89, 92, 100],
            "adoption_pct": [30.0, 33.8, 38.1, 42.0, 45.0, 45.8],
        })
        st.info("Modo demo — conectar DATABASE_URL para datos reales")

    if not df_weekly.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            last = df_weekly.iloc[0]
            st.metric(
                "Adoption rate (última semana)",
                f"{last['adoption_pct']:.1f}%",
                delta=f"{last['adoption_pct'] - df_weekly.iloc[1]['adoption_pct']:.1f}pp"
                if len(df_weekly) > 1 else None,
            )
        with col2:
            st.metric("Sugerencias mostradas (última semana)", int(last["sugerencias"]))
        with col3:
            st.metric("Marcadas como útiles", int(last["utiles"]))
        with col4:
            st.metric("Target adoption rate", "40%")

        st.line_chart(
            df_weekly.set_index("semana_inicio")[["adoption_pct"]],
            use_container_width=True,
        )

        with st.expander("Ver tabla completa"):
            st.dataframe(df_weekly, use_container_width=True)


# ── TAB 2: A/B TEST ────────────────────────────────────────────

with tab_ab:
    st.subheader("Efectividad: sugerencia útil vs. no útil vs. no mostrada")
    st.caption(
        "Compara NPS post-atención y tiempo de atención según si el operador "
        "consideró útil la sugerencia"
    )

    if engine:
        df_ab = query_df("SELECT * FROM v_nba_ab_summary")
    else:
        df_ab = pd.DataFrame({
            "grupo": ["USEFUL", "NOT_USEFUL"],
            "n": [847, 312],
            "nps_promedio": [3.9, 3.4],
            "atencion_prom_min": [11.2, 14.8],
        })

    if not df_ab.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.write("**NPS promedio post-atención por grupo**")
            st.bar_chart(df_ab.set_index("grupo")["nps_promedio"], use_container_width=True)

        with col2:
            st.write("**Tiempo de atención promedio (min) por grupo**")
            st.bar_chart(df_ab.set_index("grupo")["atencion_prom_min"], use_container_width=True)

        st.dataframe(df_ab, use_container_width=True)

        if len(df_ab) >= 2:
            useful_nps = df_ab.loc[df_ab["grupo"] == "USEFUL", "nps_promedio"].values
            not_useful_nps = df_ab.loc[df_ab["grupo"] == "NOT_USEFUL", "nps_promedio"].values
            if len(useful_nps) and len(not_useful_nps):
                delta = useful_nps[0] - not_useful_nps[0]
                st.success(
                    f"📈 Delta NPS: **+{delta:.2f} puntos** cuando el operador "
                    "considera útil la sugerencia"
                )


# ── TAB 3: RANKING POR ACCIÓN ──────────────────────────────────

with tab_acciones:
    st.subheader("Efectividad por tipo de acción NBA")

    if engine:
        df_acc = query_df("""
            SELECT
                action_shown            AS accion,
                SUM(total_mostradas)    AS mostradas,
                SUM(utiles)             AS utiles,
                ROUND(SUM(utiles) * 100.0
                    / NULLIF(SUM(total_mostradas),0), 1) AS adoption_pct,
                ROUND(AVG(nps_promedio_post), 2) AS nps_prom
            FROM v_nba_adoption_weekly
            GROUP BY action_shown
            ORDER BY adoption_pct DESC
        """)
    else:
        df_acc = pd.DataFrame({
            "accion": [
                "ESPERA_MUY_LARGA", "CLIENTE_INSATISFECHO", "CLIENTE_CON_CITA",
                "CROSS_SELL_COLA", "PRIMERA_VISITA", "HORA_PICO",
            ],
            "mostradas": [210, 185, 340, 420, 156, 290],
            "utiles": [147, 120, 204, 189, 59, 87],
            "adoption_pct": [70.0, 64.9, 60.0, 45.0, 37.8, 30.0],
            "nps_prom": [4.1, 3.9, 4.0, 3.8, 3.6, 3.5],
        })

    if not df_acc.empty:
        # Color por adoption rate
        def color_adoption(val):
            if val >= 60:
                return "background-color: #d4edda"
            elif val >= 40:
                return "background-color: #fff3cd"
            else:
                return "background-color: #f8d7da"

        styled = df_acc.style.applymap(color_adoption, subset=["adoption_pct"])
        st.dataframe(styled, use_container_width=True)

        st.caption(
            "🟢 ≥ 60% adoption | 🟡 40-60% | 🔴 < 40% — "
            "Las acciones con baja adopción necesitan ajuste de mensaje o umbral"
        )


# ── TAB 4: SIMULADOR ───────────────────────────────────────────

with tab_simulador:
    st.subheader("Simulador de sugerencias NBA")
    st.caption(
        "Probá el motor con un turno hipotético sin conectar a la DB. "
        "Útil para testear reglas y mensajes."
    )

    col1, col2 = st.columns(2)

    with col1:
        st.write("**Contexto del turno**")
        queue_name = st.selectbox(
            "Cola (queue_name)",
            options=[
                "Solicitud de Préstamos",
                "Cuenta Corriente",
                "Cajas",
                "Plazos Fijos",
                "Inversiones",
                "Atención al Cliente",
                "Otra Cola",
            ],
        )
        wait_minutes = st.slider("Tiempo de espera (minutos)", 0, 60, 25)
        hora_dia = st.slider("Hora del día", 8, 18, 11)
        tiene_cita = st.checkbox("¿Vino con cita agendada?", value=False)

    with col2:
        st.write("**Perfil del cliente (historial)**")
        nps_hist = st.select_slider(
            "NPS histórico promedio",
            options=[None, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0],
            value=None,
            format_func=lambda x: "Sin historial" if x is None else f"{x}/5",
        )
        veces_nps_bajo = st.number_input("Veces con NPS ≤ 2", min_value=0, max_value=10, value=0)
        visitas_total = st.number_input("Visitas históricas totales", min_value=0, value=3)
        tasa_ausentismo = st.slider("Tasa de ausentismo en citas (%)", 0, 100, 0)
        citas_total = st.number_input("Total de citas históricas", min_value=0, value=2)
        dias_ultima_visita = st.number_input("Días desde última visita", min_value=0, value=30)

    if st.button("🎯 Generar sugerencias NBA", type="primary"):
        # Importar el motor de reglas en modo offline
        import sys
        import importlib.util

        try:
            spec = importlib.util.spec_from_file_location(
                "nba_engine",
                os.path.join(os.path.dirname(__file__), "02_nba_engine.py"),
            )
            mod = importlib.util.load_from_spec(spec)
            spec.loader.exec_module(mod)

            ctx = mod.TurnContext(
                turn_id="SIM-001",
                customer_id="SIM-CUSTOMER" if visitas_total > 0 else None,
                turn_email=None,
                queue_name=queue_name,
                branch_name="Sucursal Simulada",
                operator_id="OP-SIM",
                wait_time_seconds=wait_minutes * 60,
                appointment_code="APT-SIM" if tiene_cita else None,
                llamada_ts=datetime(2026, 3, 19, hora_dia, 30),
            )

            # Construir features manualmente para el simulador
            features = mod.CustomerFeatures(
                customer_id="SIM-CUSTOMER",
                visitas_total=visitas_total,
                dias_desde_ultima_visita=dias_ultima_visita,
                atencion_prom_min=12.0,
                citas_total=citas_total,
                citas_ausente=int(citas_total * tasa_ausentismo / 100),
                tasa_ausentismo_pct=tasa_ausentismo if citas_total >= 2 else None,
                encuestas_respondidas=1 if nps_hist else 0,
                nps_promedio=nps_hist,
                nps_minimo=nps_hist,
                veces_nps_bajo=veces_nps_bajo,
                flag_cliente_insatisfecho=(
                    nps_hist is not None and nps_hist <= 2.5 and nps_hist > 0
                ),
                flag_ausentismo_frecuente=(
                    tasa_ausentismo >= 40 and citas_total >= 2
                ),
                flag_visita_reciente=dias_ultima_visita <= 7,
                flag_primera_visita=visitas_total <= 1,
                flag_insatisfaccion_repetida=veces_nps_bajo >= 2,
            )

            engine_sim = mod.NBAEngine(db_conn=None)
            # Parchear _load_features para devolver las features del simulador
            engine_sim._load_features = lambda ctx: features

            suggestions = engine_sim.suggest(ctx)

            st.success(f"**{len(suggestions)} sugerencia(s) generada(s)**")

            for s in suggestions:
                layer_icon = {"risk": "⚠️", "commercial": "💡", "service": "ℹ️"}.get(
                    s["layer"], "•"
                )
                with st.expander(
                    f"{layer_icon} [{s['layer'].upper()}] {s['label']} "
                    f"— Prioridad {s['priority']}",
                    expanded=True,
                ):
                    st.write(f"**Mensaje para el operador:**")
                    st.info(s["message"])
                    st.write("**Evidencia:**")
                    for e in s["evidence"]:
                        st.write(f"- {e}")
                    st.caption(f"Acción: `{s['action']}` | Confianza: {s['confidence']:.0%}")

        except Exception as e:
            st.error(f"Error en simulador: {e}")
            st.code(str(e))


# ──────────────────────────────────────────────────────────────
# FOOTER
# ──────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "Numia NBA v1.0 — Motor de reglas | "
    f"Última actualización: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
)
