-- ============================================================
-- NBA Operador — Feature Store (batch nocturno)
-- Crea / reemplaza la tabla nba_customer_features
-- Correr 1x por día, idealmente 02:00 hs
-- ============================================================

DROP TABLE IF EXISTS nba_customer_features;

CREATE TABLE nba_customer_features AS

WITH

-- ────────────────────────────────────────────────────────────
-- BASE: todas las llamadas de los últimos 12 meses
-- ────────────────────────────────────────────────────────────
llamadas AS (
    SELECT
        turn_id,
        turn_customer_number        AS customer_id,
        turn_email,
        branch_name,
        queue_name,
        service_name,
        user_id                     AS operator_id,
        action_time                 AS llamada_ts,
        wait_time,
        appointment_code
    FROM qmovements
    WHERE action_text IN ('LLAMADA', 'LLAMADA AUTOMATICA', 'LLAMADA MANUAL')
      AND wait_time IS NOT NULL
      AND wait_time >= 0
      AND action_time >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
),

-- ────────────────────────────────────────────────────────────
-- FINALIZACIONES para obtener attention_time y tipificación
-- ────────────────────────────────────────────────────────────
finalizaciones AS (
    SELECT
        turn_id,
        attention_time,
        service_name                AS service_tipificado,
        action_time                 AS fin_ts
    FROM qmovements
    WHERE action_text IN (
        'FINALIZACION', 'FINALIZACION AUTOMATICA',
        'TIPIFICADO Y FINALIZADO', 'TIPIFICADO'
    )
      AND attention_time IS NOT NULL
      AND attention_time >= 0
),

-- ────────────────────────────────────────────────────────────
-- TURNOS COMPLETOS (llamada + finalización)
-- ────────────────────────────────────────────────────────────
turnos AS (
    SELECT
        l.*,
        f.attention_time,
        f.service_tipificado,
        f.fin_ts
    FROM llamadas l
    INNER JOIN finalizaciones f ON l.turn_id = f.turn_id
),

-- ────────────────────────────────────────────────────────────
-- HISTORIAL POR CLIENTE — desde qmovements
-- ────────────────────────────────────────────────────────────
hist_qm AS (
    SELECT
        customer_id,

        -- Volumen
        COUNT(*)                                            AS visitas_total,
        COUNT(DISTINCT DATE(llamada_ts))                    AS dias_con_visita,
        COUNT(DISTINCT branch_name)                         AS sucursales_distintas,
        COUNT(DISTINCT queue_name)                          AS colas_distintas,

        -- Recencia
        MAX(llamada_ts)                                     AS ultima_visita_ts,
        DATEDIFF(CURDATE(), MAX(DATE(llamada_ts)))          AS dias_desde_ultima_visita,

        -- Tiempos promedio
        ROUND(AVG(wait_time) / 60.0, 1)                    AS espera_prom_min,
        ROUND(AVG(attention_time) / 60.0, 1)               AS atencion_prom_min,

        -- Última visita
        (SELECT queue_name FROM turnos t2
         WHERE t2.customer_id = t.customer_id
         ORDER BY llamada_ts DESC LIMIT 1)                  AS ultima_cola,

        -- Cola más frecuente (proxy de "por qué viene")
        (SELECT queue_name FROM turnos t3
         WHERE t3.customer_id = t.customer_id
         GROUP BY queue_name ORDER BY COUNT(*) DESC LIMIT 1) AS cola_mas_frecuente

    FROM turnos t
    WHERE customer_id IS NOT NULL AND customer_id != ''
    GROUP BY customer_id
),

-- ────────────────────────────────────────────────────────────
-- HISTORIAL DE CITAS — desde a_appointment
-- ────────────────────────────────────────────────────────────
hist_citas AS (
    SELECT
        customer_id,

        COUNT(*)                                            AS citas_total,
        SUM(status = 'CHECKED_IN')                         AS citas_checkin,
        SUM(status = 'ABSENT')                             AS citas_ausente,
        SUM(status = 'CANCELED')                           AS citas_cancelada,
        SUM(status = 'EXPIRED_CONFIRMATION_TIME')          AS citas_expirada,

        ROUND(
            SUM(status = 'ABSENT') * 100.0
            / NULLIF(COUNT(*), 0), 1
        )                                                   AS tasa_ausentismo_pct,

        ROUND(
            SUM(status = 'CHECKED_IN') * 100.0
            / NULLIF(COUNT(*), 0), 1
        )                                                   AS tasa_checkin_pct,

        ROUND(
            AVG(DATEDIFF(start_at, creation_date)), 1
        )                                                   AS anticipacion_prom_dias,

        MAX(start_at)                                       AS ultima_cita_ts,
        DATEDIFF(CURDATE(), MAX(DATE(start_at)))            AS dias_desde_ultima_cita

    FROM a_appointment
    WHERE customer_id IS NOT NULL AND customer_id != ''
    GROUP BY customer_id
),

-- ────────────────────────────────────────────────────────────
-- NPS / CSAT HISTÓRICO — desde survey_answers
-- ────────────────────────────────────────────────────────────
hist_nps AS (
    SELECT
        q.turn_customer_number                              AS customer_id,

        COUNT(DISTINCT s.id)                               AS encuestas_respondidas,
        ROUND(AVG(
            CASE WHEN s.question_type = 'LINEAR_SCALE'
                 THEN CAST(s.answer AS FLOAT) END
        ), 2)                                               AS nps_promedio,
        MIN(CASE WHEN s.question_type = 'LINEAR_SCALE'
                 THEN CAST(s.answer AS FLOAT) END)          AS nps_minimo,
        MAX(created_at)                                     AS ultima_encuesta_ts,

        -- Flag: ¿el cliente dio NPS bajo alguna vez?
        SUM(CASE WHEN s.question_type = 'LINEAR_SCALE'
                      AND CAST(s.answer AS FLOAT) <= 2
                 THEN 1 ELSE 0 END)                         AS veces_nps_bajo,

        -- Comentario más reciente (para futura capa de texto)
        (SELECT sa2.answer FROM survey_answers sa2
         WHERE sa2.turn_id IN (
             SELECT q2.turn_id FROM qmovements q2
             WHERE q2.turn_customer_number = q.turn_customer_number
         )
         AND sa2.question_type IN ('SHORT','LONG')
         AND sa2.movement_status = 'ANSWERED'
         ORDER BY sa2.created_at DESC LIMIT 1)              AS ultimo_comentario

    FROM qmovements q
    INNER JOIN survey_answers s
        ON q.turn_id = s.turn_id
       AND s.movement_status = 'ANSWERED'
    WHERE q.turn_customer_number IS NOT NULL
      AND q.turn_customer_number != ''
    GROUP BY q.turn_customer_number
),

-- ────────────────────────────────────────────────────────────
-- PERCENTILES POR COLA — para clasificar si un cliente es
-- "rápido" o "lento" relativo a su tipo de trámite
-- ────────────────────────────────────────────────────────────
percentiles_cola AS (
    SELECT
        queue_name,
        ROUND(AVG(attention_time) / 60.0, 1)                AS atencion_prom_cola_min,
        ROUND(PERCENTILE_CONT(0.75)
            WITHIN GROUP (ORDER BY attention_time) / 60.0, 1)
                                                            AS atencion_p75_cola_min,
        ROUND(AVG(wait_time) / 60.0, 1)                     AS espera_prom_cola_min
    FROM qmovements
    WHERE action_text IN (
        'FINALIZACION','FINALIZACION AUTOMATICA','TIPIFICADO Y FINALIZADO'
    )
      AND attention_time IS NOT NULL AND attention_time >= 0
      AND action_time >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    GROUP BY queue_name
)


-- ────────────────────────────────────────────────────────────
-- TABLA FINAL: 1 fila por customer_id
-- ────────────────────────────────────────────────────────────
SELECT
    hq.customer_id,
    NOW()                                   AS features_calculadas_ts,

    -- Volumen y recencia (qmovements)
    hq.visitas_total,
    hq.dias_con_visita,
    hq.sucursales_distintas,
    hq.colas_distintas,
    hq.dias_desde_ultima_visita,
    hq.ultima_cola,
    hq.cola_mas_frecuente,

    -- Tiempos (qmovements)
    hq.espera_prom_min,
    hq.atencion_prom_min,

    -- Citas (a_appointment)
    COALESCE(hc.citas_total, 0)             AS citas_total,
    COALESCE(hc.citas_checkin, 0)           AS citas_checkin,
    COALESCE(hc.citas_ausente, 0)           AS citas_ausente,
    COALESCE(hc.citas_cancelada, 0)         AS citas_cancelada,
    COALESCE(hc.tasa_ausentismo_pct, NULL)  AS tasa_ausentismo_pct,
    COALESCE(hc.tasa_checkin_pct, NULL)     AS tasa_checkin_pct,
    hc.anticipacion_prom_dias,
    hc.dias_desde_ultima_cita,

    -- Satisfacción (survey_answers)
    COALESCE(hn.encuestas_respondidas, 0)   AS encuestas_respondidas,
    hn.nps_promedio,
    hn.nps_minimo,
    COALESCE(hn.veces_nps_bajo, 0)          AS veces_nps_bajo,
    hn.ultimo_comentario,

    -- Flags de riesgo (pre-calculados para el motor de reglas)
    CASE WHEN hn.nps_promedio <= 2.5
              AND hn.encuestas_respondidas >= 1
         THEN 1 ELSE 0 END                  AS flag_cliente_insatisfecho,

    CASE WHEN hc.tasa_ausentismo_pct >= 40
              AND hc.citas_total >= 2
         THEN 1 ELSE 0 END                  AS flag_ausentismo_frecuente,

    CASE WHEN hq.dias_desde_ultima_visita <= 7
         THEN 1 ELSE 0 END                  AS flag_visita_reciente,

    CASE WHEN hq.visitas_total = 1
         THEN 1 ELSE 0 END                  AS flag_primera_visita,

    CASE WHEN hn.veces_nps_bajo >= 2
         THEN 1 ELSE 0 END                  AS flag_insatisfaccion_repetida

FROM hist_qm hq
LEFT JOIN hist_citas hc ON hq.customer_id = hc.customer_id
LEFT JOIN hist_nps hn   ON hq.customer_id = hn.customer_id
;

-- Índice para lookup rápido en tiempo real
CREATE INDEX idx_nba_customer ON nba_customer_features (customer_id);
