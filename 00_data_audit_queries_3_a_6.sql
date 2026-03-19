-- ============================================================
-- Queries 3 a 6 — versión liviana (SQL Server)
-- Ventana: últimos 7 días para evitar scans completos
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 3. COBERTURA DE ENCUESTAS
--    Sin JOIN — cuenta por separado en cada tabla
-- ────────────────────────────────────────────────────────────
SELECT
    (
        SELECT COUNT(DISTINCT turn_id)
        FROM qmovements
        WHERE action_text IN ('FINALIZACION','FINALIZACION AUTOMATICA','TIPIFICADO Y FINALIZADO')
          AND action_time >= DATEADD(DAY, -7, GETDATE())
    )                                   AS turnos_finalizados,

    (
        SELECT COUNT(DISTINCT turn_id)
        FROM survey_answers
        WHERE movement_status = 'ANSWERED'
          AND created_at >= DATEADD(DAY, -7, GETDATE())
    )                                   AS turnos_con_encuesta,

    (
        SELECT AVG(TRY_CAST(answer AS FLOAT))
        FROM survey_answers
        WHERE movement_status = 'ANSWERED'
          AND question_type = 'LINEAR_SCALE'
          AND created_at >= DATEADD(DAY, -7, GETDATE())
    )                                   AS nps_promedio_global;


-- ────────────────────────────────────────────────────────────
-- 4. COBERTURA DE CITAS
--    Sin JOIN — cuenta appointment_code no nulos directamente
-- ────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                            AS llamadas_7dias,
    SUM(CASE WHEN appointment_code IS NOT NULL
             AND appointment_code != ''
             THEN 1 ELSE 0 END)         AS con_cita,
    ROUND(
        SUM(CASE WHEN appointment_code IS NOT NULL
                  AND appointment_code != ''
                  THEN 1.0 ELSE 0 END)
        * 100.0 / COUNT(*), 1
    )                                   AS pct_con_cita
FROM qmovements
WHERE action_text IN ('LLAMADA','LLAMADA AUTOMATICA','LLAMADA MANUAL')
  AND action_time >= DATEADD(DAY, -7, GETDATE());


-- ────────────────────────────────────────────────────────────
-- 5. CLIENTES CON HISTORIAL — solo por email (customer_id es 5%)
--    Ventana 30 días, agrupado por email
-- ────────────────────────────────────────────────────────────
SELECT
    visitas_bucket,
    COUNT(*)                            AS emails_distintos
FROM (
    SELECT
        turn_email,
        COUNT(DISTINCT CAST(action_time AS DATE)) AS dias_visita,
        CASE
            WHEN COUNT(DISTINCT CAST(action_time AS DATE)) = 1
                THEN '1 dia (cold start)'
            WHEN COUNT(DISTINCT CAST(action_time AS DATE)) BETWEEN 2 AND 4
                THEN '2-4 dias'
            ELSE '5+ dias (recurrente)'
        END AS visitas_bucket
    FROM qmovements
    WHERE action_text IN ('LLAMADA','LLAMADA AUTOMATICA','LLAMADA MANUAL')
      AND turn_email IS NOT NULL
      AND turn_email != ''
      AND action_time >= DATEADD(DAY, -30, GETDATE())
    GROUP BY turn_email
) sub
GROUP BY visitas_bucket
ORDER BY MIN(dias_visita);


-- ────────────────────────────────────────────────────────────
-- 6. TOP COLAS — sin PERCENTILE, solo promedio y conteo
-- ────────────────────────────────────────────────────────────
SELECT TOP 15
    queue_name,
    COUNT(DISTINCT turn_id)             AS turnos,
    ROUND(AVG(attention_time) / 60.0, 1) AS atencion_prom_min,
    ROUND(AVG(wait_time) / 60.0, 1)     AS espera_prom_min
FROM qmovements
WHERE action_text IN ('FINALIZACION','FINALIZACION AUTOMATICA','TIPIFICADO Y FINALIZADO')
  AND attention_time IS NOT NULL
  AND attention_time >= 0
  AND action_time >= DATEADD(DAY, -7, GETDATE())
GROUP BY queue_name
ORDER BY turnos DESC;
