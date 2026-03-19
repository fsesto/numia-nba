"""
NBA Operador — Construcción del dataset de entrenamiento (Fase 2)
Correr cuando haya ~3 meses de datos de feedback acumulados.

Genera: nba_training_dataset.parquet
"""

import logging
import os
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# QUERY PRINCIPAL DEL DATASET
# ──────────────────────────────────────────────────────────────

SQL_TRAINING_DATASET = """
SELECT
    -- Identificadores
    l.turn_id,
    l.operator_id,

    -- Features del turno (contexto en momento de LLAMADA)
    HOUR(l.llamada_ts)                              AS hora_dia,
    DAYOFWEEK(l.llamada_ts)                         AS dia_semana,
    l.queue_name,
    l.branch_name,
    l.wait_time_seconds / 60.0                      AS espera_minutos,
    CASE WHEN l.appointment_code IS NOT NULL
         THEN 1 ELSE 0 END                          AS vino_con_cita,

    -- Features del cliente (de nba_customer_features)
    COALESCE(cf.visitas_total, 0)                   AS visitas_total,
    COALESCE(cf.dias_desde_ultima_visita, 999)      AS dias_ultima_visita,
    COALESCE(cf.tasa_ausentismo_pct, 0)             AS tasa_ausentismo_pct,
    COALESCE(cf.citas_total, 0)                     AS citas_total,
    COALESCE(cf.nps_promedio, -1)                   AS nps_promedio,          -- -1 = sin dato
    COALESCE(cf.encuestas_respondidas, 0)           AS encuestas_respondidas,
    COALESCE(cf.veces_nps_bajo, 0)                  AS veces_nps_bajo,
    COALESCE(cf.flag_primera_visita, 1)             AS flag_primera_visita,
    COALESCE(cf.flag_cliente_insatisfecho, 0)       AS flag_cliente_insatisfecho,
    COALESCE(cf.flag_ausentismo_frecuente, 0)       AS flag_ausentismo_frecuente,

    -- Target 1: ¿el feedback fue útil?
    CASE WHEN fb.feedback = 'USEFUL' THEN 1 ELSE 0 END AS target_feedback_util,

    -- Target 2: NPS post-atención (outcome real)
    fb.nps_post_atencion                            AS target_nps_post,

    -- Target 3: tiempo de atención (eficiencia)
    fb.atencion_minutos                             AS target_atencion_min,

    -- Target compuesto (para ranking)
    -- Score 0-6: eficiencia (1) + satisfacción (2) + comercial (3)
    (
        CASE WHEN fb.atencion_minutos < pc.atencion_p75_cola_min THEN 1 ELSE 0 END
        + CASE WHEN fb.nps_post_atencion >= 4 THEN 2 ELSE 0 END
        + CASE WHEN fb.feedback = 'USEFUL' THEN 1 ELSE 0 END
    )                                               AS target_score_compuesto,

    -- Acción NBA que se mostró (para entender correlaciones)
    fb.action_shown                                 AS action_mostrada

FROM nba_feedback_log fb
-- Join con el log de sugerencias para obtener el contexto del turno
INNER JOIN (
    SELECT
        turn_id,
        operator_id,
        action_shown                                AS action_log,
        shown_at                                    AS llamada_ts,
        -- Reconstruir wait_time y queue desde qmovements
        q.wait_time                                 AS wait_time_seconds,
        q.queue_name,
        q.branch_name,
        q.appointment_code
    FROM nba_log nl
    INNER JOIN qmovements q
        ON nl.turn_id = q.turn_id
       AND q.action_text IN ('LLAMADA','LLAMADA AUTOMATICA','LLAMADA MANUAL')
) l ON fb.turn_id = l.turn_id AND fb.action_shown = l.action_log

-- Features del cliente en el momento de la atención
LEFT JOIN nba_customer_features cf
    ON l.turn_id IN (
        SELECT turn_id FROM qmovements
        WHERE turn_customer_number = cf.customer_id
        LIMIT 1
    )

-- Percentiles de cola para el target de eficiencia
LEFT JOIN (
    SELECT
        queue_name,
        PERCENTILE_CONT(0.75)
            WITHIN GROUP (ORDER BY attention_time) / 60.0 AS atencion_p75_cola_min
    FROM qmovements
    WHERE action_text IN ('FINALIZACION','FINALIZACION AUTOMATICA','TIPIFICADO Y FINALIZADO')
      AND attention_time IS NOT NULL AND attention_time >= 0
    GROUP BY queue_name
) pc ON l.queue_name = pc.queue_name

WHERE fb.feedback IS NOT NULL
;
"""


# ──────────────────────────────────────────────────────────────
# CONSTRUCCIÓN DEL DATASET
# ──────────────────────────────────────────────────────────────

def build_training_dataset(db_engine, output_path: str = "nba_training_dataset.parquet"):
    """
    Construye y guarda el dataset de entrenamiento.

    Args:
        db_engine: SQLAlchemy engine conectado a la DB
        output_path: ruta de salida del parquet
    """
    logger.info("Construyendo dataset de entrenamiento NBA...")

    df = pd.read_sql(SQL_TRAINING_DATASET, db_engine)
    logger.info("Filas cargadas: %d", len(df))

    if df.empty:
        logger.warning("Dataset vacío — ¿hay datos en nba_feedback_log?")
        return df

    # ── FEATURE ENGINEERING ──────────────────────────────────

    # Encoding de variables categóricas
    df["queue_encoded"] = pd.Categorical(df["queue_name"]).codes
    df["branch_encoded"] = pd.Categorical(df["branch_name"]).codes
    df["action_encoded"] = pd.Categorical(df["action_mostrada"]).codes

    # Feature: hora del día en bloques
    df["bloque_horario"] = pd.cut(
        df["hora_dia"],
        bins=[0, 9, 12, 15, 18, 24],
        labels=["apertura", "manana", "mediodia", "tarde", "cierre"],
        include_lowest=True,
    ).astype(str)

    # Feature: relación espera vs. promedio de la cola
    espera_prom_cola = df.groupby("queue_name")["espera_minutos"].transform("mean")
    df["ratio_espera_vs_cola"] = df["espera_minutos"] / espera_prom_cola.replace(0, 1)

    # Flag: cliente con datos suficientes para features históricas
    df["tiene_historial"] = (df["visitas_total"] >= 2).astype(int)

    # ── ESTADÍSTICAS DEL DATASET ─────────────────────────────

    logger.info("\n── DISTRIBUCIÓN DEL TARGET SCORE COMPUESTO ──")
    logger.info(df["target_score_compuesto"].value_counts(normalize=True).to_string())

    logger.info("\n── ADOPTION RATE POR ACCIÓN ──")
    adoption = df.groupby("action_mostrada")["target_feedback_util"].agg(
        ["mean", "count"]
    ).rename(columns={"mean": "adoption_rate", "count": "n"})
    logger.info(adoption.to_string())

    logger.info("\n── NPS PROMEDIO POR GRUPO DE FEEDBACK ──")
    nps_group = df.groupby("target_feedback_util")["target_nps_post"].mean()
    logger.info(nps_group.to_string())

    # ── GUARDAR ──────────────────────────────────────────────

    df.to_parquet(output_path, index=False)
    logger.info("Dataset guardado en: %s (%d filas)", output_path, len(df))

    return df


# ──────────────────────────────────────────────────────────────
# ENTRENAMIENTO DEL MODELO (Fase 2)
# Descomentar cuando haya suficiente data (≥ 500 filas con feedback)
# ──────────────────────────────────────────────────────────────

def train_model(df: pd.DataFrame, output_path: str = "nba_model.pkl"):
    """
    Entrena un clasificador LightGBM para predecir el score compuesto.
    Requiere: pip install lightgbm scikit-learn
    """
    try:
        import lightgbm as lgb
        import pickle
        from sklearn.model_selection import TimeSeriesSplit
        from sklearn.metrics import mean_absolute_error, classification_report
        from sklearn.preprocessing import LabelEncoder
    except ImportError:
        logger.error("Instalar: pip install lightgbm scikit-learn")
        return

    FEATURE_COLS = [
        "hora_dia", "dia_semana", "espera_minutos", "vino_con_cita",
        "queue_encoded", "branch_encoded",
        "visitas_total", "dias_ultima_visita",
        "tasa_ausentismo_pct", "citas_total",
        "nps_promedio", "encuestas_respondidas", "veces_nps_bajo",
        "flag_primera_visita", "flag_cliente_insatisfecho",
        "flag_ausentismo_frecuente", "ratio_espera_vs_cola",
        "tiene_historial",
    ]

    TARGET_COL = "target_score_compuesto"

    df_clean = df[FEATURE_COLS + [TARGET_COL]].dropna(subset=[TARGET_COL])
    X = df_clean[FEATURE_COLS].fillna(-1)
    y = df_clean[TARGET_COL].astype(int)

    logger.info("Training set: %d filas, %d features", len(X), len(FEATURE_COLS))
    logger.info("Distribución del target: %s", y.value_counts().to_dict())

    # TimeSeriesSplit para validación (respetar el orden temporal)
    tscv = TimeSeriesSplit(n_splits=3)
    maes = []

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model = lgb.LGBMClassifier(
            n_estimators=200,
            learning_rate=0.05,
            num_leaves=31,
            random_state=42,
            class_weight="balanced",
        )
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)])
        preds = model.predict(X_val)
        mae = mean_absolute_error(y_val, preds)
        maes.append(mae)
        logger.info("Fold %d — MAE: %.3f", fold + 1, mae)

    logger.info("MAE promedio: %.3f", sum(maes) / len(maes))

    # Entrenar modelo final con todos los datos
    final_model = lgb.LGBMClassifier(
        n_estimators=200,
        learning_rate=0.05,
        num_leaves=31,
        random_state=42,
        class_weight="balanced",
    )
    final_model.fit(X, y)

    # Feature importance
    fi = pd.Series(
        final_model.feature_importances_,
        index=FEATURE_COLS
    ).sort_values(ascending=False)
    logger.info("\n── TOP 10 FEATURES MÁS IMPORTANTES ──\n%s", fi.head(10).to_string())

    with open(output_path, "wb") as f:
        pickle.dump({"model": final_model, "features": FEATURE_COLS}, f)

    logger.info("Modelo guardado en: %s", output_path)
    return final_model


# ──────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import os

    try:
        from sqlalchemy import create_engine
        from dotenv import load_dotenv
        load_dotenv()

        db = create_engine(os.getenv("DATABASE_URL"))
        df = build_training_dataset(db)

        if len(df) >= 500:
            logger.info("Dataset suficiente — entrenando modelo ML...")
            train_model(df)
        else:
            logger.info(
                "Dataset pequeño (%d filas) — acumular más feedback antes de entrenar. "
                "Mínimo recomendado: 500 filas con feedback.", len(df)
            )
    except Exception as e:
        logger.error("Error: %s", e)
