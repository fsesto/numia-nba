-- ============================================================
-- NBA Operador — Auditoría de datos (SQL Server)
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. COBERTURA DE customer_id EN QMOVEMENTS
-- ────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                                    AS total_llamadas,
    SUM(CASE WHEN turn_customer_number IS NOT NULL
              AND turn_customer_number != '' THEN 1 ELSE 0 END) AS con_customer_id,
    SUM(CASE WHEN turn_email IS NOT NULL
              AND turn_email != '' THEN 1 ELSE 0 END)           AS con_email,
    ROUND(
        SUM(CASE WHEN turn_customer_number IS NOT NULL
                  AND turn_customer_number != '' THEN 1.0 ELSE 0 END)
        * 100.0 / COUNT(*), 1
    )                                                           AS pct_con_customer_id
FROM qmovements
WHERE action_text IN ('LLAMADA', 'LLAMADA AUTOMATICA', 'LLAMADA MANUAL')
  AND action_time >= DATEADD(DAY, -90, GETDATE());


-- ────────────────────────────────────────────────────────────
-- 2. VOLUMEN HISTÓRICO POR MES
-- ────────────────────────────────────────────────────────────
SELECT
    FORMAT(action_time, 'yyyy-MM')                              AS mes,
    COUNT(DISTINCT turn_id)                                     AS turnos_totales,
    COUNT(DISTINCT CASE
        WHEN action_text IN ('LLAMADA','LLAMADA AUTOMATICA','LLAMADA MANUAL')
        THEN turn_id END)                                       AS turnos_atendidos,
    COUNT(DISTINCT branch_name)                                 AS sucursales_activas,
    COUNT(DISTINCT user_id)                                     AS operadores_activos
FROM qmovements
WHERE action_time >= DATEADD(MONTH, -12, GETDATE())
GROUP BY FORMAT(action_time, 'yyyy-MM')
ORDER BY mes;


-- ────────────────────────────────────────────────────────────
-- 3. COBERTURA DE ENCUESTAS
-- ────────────────────────────────────────────────────────────
SELECT
    COUNT(DISTINCT q.turn_id)                                   AS turnos_finalizados,
    COUNT(DISTINCT s.turn_id)                                   AS turnos_con_encuesta,
    ROUND(
        COUNT(DISTINCT s.turn_id) * 100.0
        / NULLIF(COUNT(DISTINCT q.turn_id), 0), 1
    )                                                           AS pct_cobertura_encuesta,
    AVG(CASE WHEN s.question_type = 'LINEAR_SCALE'
             THEN TRY_CAST(s.answer AS FLOAT) END)              AS nps_promedio_global
FROM qmovements q
LEFT JOIN survey_answers s
    ON q.turn_id = s.turn_id
   AND s.movement_status = 'ANSWERED'
WHERE q.action_text IN (
    'FINALIZACION','FINALIZACION AUTOMATICA','TIPIFICADO Y FINALIZADO'
)
  AND q.action_time >= DATEADD(DAY, -90, GETDATE());


-- ────────────────────────────────────────────────────────────
-- 4. COBERTURA DE CITAS
-- ────────────────────────────────────────────────────────────
SELECT
    COUNT(DISTINCT q.turn_id)                                   AS turnos_atendidos,
    COUNT(DISTINCT CASE
        WHEN q.appointment_code IS NOT NULL THEN q.turn_id END) AS turnos_con_cita,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN q.appointment_code IS NOT NULL THEN q.turn_id END) * 100.0
        / NULLIF(COUNT(DISTINCT q.turn_id), 0), 1
    )                                                           AS pct_con_cita,
    COUNT(DISTINCT a.id)                                        AS citas_checkin_periodo
FROM qmovements q
LEFT JOIN a_appointment a
    ON q.appointment_code = a.code
WHERE q.action_text IN ('LLAMADA','LLAMADA AUTOMATICA','LLAMADA MANUAL')
  AND q.action_time >= DATEADD(DAY, -90, GETDATE());


-- ────────────────────────────────────────────────────────────
-- 5. CLIENTES CON HISTORIAL SUFICIENTE
-- ────────────────────────────────────────────────────────────
SELECT
    visitas_bucket,
    COUNT(*)                                                    AS clientes,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)         AS pct
FROM (
    SELECT
        turn_customer_number,
        COUNT(DISTINCT CAST(action_time AS DATE))               AS dias_con_visita,
        CASE
            WHEN COUNT(DISTINCT CAST(action_time AS DATE)) = 1
                THEN '1 visita (cold start)'
            WHEN COUNT(DISTINCT CAST(action_time AS DATE)) BETWEEN 2 AND 4
                THEN '2-4 visitas'
            WHEN COUNT(DISTINCT CAST(action_time AS DATE)) BETWEEN 5 AND 9
                THEN '5-9 visitas'
            ELSE '10+ visitas (cliente recurrente)'
        END                                                     AS visitas_bucket
    FROM qmovements
    WHERE action_text IN ('LLAMADA','LLAMADA AUTOMATICA','LLAMADA MANUAL')
      AND turn_customer_number IS NOT NULL
      AND turn_customer_number != ''
      AND action_time >= DATEADD(MONTH, -12, GETDATE())
    GROUP BY turn_customer_number
) sub
GROUP BY visitas_bucket
ORDER BY MIN(dias_con_visita);


-- ────────────────────────────────────────────────────────────
-- 6. TOP COLAS — tiempos promedio y p75
-- ────────────────────────────────────────────────────────────
SELECT TOP 20
    queue_name,
    COUNT(DISTINCT turn_id)                                     AS turnos,
    ROUND(AVG(attention_time) / 60.0, 1)                        AS atencion_prom_min,
    ROUND(
        PERCENTILE_CONT(0.75)
            WITHIN GROUP (ORDER BY attention_time)
            OVER (PARTITION BY queue_name) / 60.0
        , 1)                                                    AS atencion_p75_min,
    ROUND(AVG(wait_time) / 60.0, 1)                             AS espera_prom_min
FROM qmovements
WHERE action_text IN (
    'FINALIZACION','FINALIZACION AUTOMATICA','TIPIFICADO Y FINALIZADO'
)
  AND attention_time IS NOT NULL
  AND attention_time >= 0
  AND action_time >= DATEADD(DAY, -90, GETDATE())
GROUP BY queue_name
ORDER BY turnos DESC;
