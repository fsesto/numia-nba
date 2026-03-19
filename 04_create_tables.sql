-- ============================================================
-- NBA Operador — Tablas de logging
-- Correr una sola vez antes de levantar la API
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- Registro de sugerencias mostradas al operador
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nba_log (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    turn_id         VARCHAR(64)     NOT NULL,
    customer_id     VARCHAR(64),
    operator_id     VARCHAR(64)     NOT NULL,
    action_shown    VARCHAR(64)     NOT NULL,   -- acción NBA mostrada
    layer           VARCHAR(16)     NOT NULL,   -- risk | commercial | service
    priority        TINYINT         NOT NULL,
    shown_at        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_turn      (turn_id),
    INDEX idx_customer  (customer_id),
    INDEX idx_operator  (operator_id),
    INDEX idx_shown_at  (shown_at),
    UNIQUE KEY uq_turn_action (turn_id, action_shown)
);


-- ────────────────────────────────────────────────────────────
-- Feedback del operador sobre cada sugerencia
-- Fuente principal para el loop de reentrenamiento
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS nba_feedback_log (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    turn_id         VARCHAR(64)     NOT NULL,
    action_shown    VARCHAR(64)     NOT NULL,
    feedback        ENUM('USEFUL','NOT_USEFUL','NOT_SHOWN') NOT NULL,
    operator_id     VARCHAR(64)     NOT NULL,
    notes           TEXT,
    feedback_at     DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Outcome real (se puede poblar post-hoc desde survey_answers)
    nps_post_atencion   FLOAT,
    atencion_minutos    FLOAT,

    INDEX idx_turn          (turn_id),
    INDEX idx_operator      (operator_id),
    INDEX idx_feedback_at   (feedback_at),
    INDEX idx_action        (action_shown),
    UNIQUE KEY uq_turn_action (turn_id, action_shown)
);


-- ────────────────────────────────────────────────────────────
-- Vista de adopción semanal — para el dashboard Streamlit
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_nba_adoption_weekly AS
SELECT
    DATE_FORMAT(f.feedback_at, '%Y-%u')         AS semana,
    MIN(DATE(f.feedback_at))                    AS semana_inicio,
    f.action_shown                              AS accion,
    COUNT(*)                                    AS total_mostradas,
    SUM(f.feedback = 'USEFUL')                  AS utiles,
    SUM(f.feedback = 'NOT_USEFUL')              AS no_utiles,
    ROUND(
        SUM(f.feedback = 'USEFUL') * 100.0
        / NULLIF(COUNT(*), 0), 1
    )                                           AS adoption_rate_pct,
    ROUND(AVG(f.nps_post_atencion), 2)          AS nps_promedio_post,
    ROUND(AVG(f.atencion_minutos), 1)           AS atencion_prom_min
FROM nba_feedback_log f
GROUP BY DATE_FORMAT(f.feedback_at, '%Y-%u'), f.action_shown;


-- ────────────────────────────────────────────────────────────
-- Vista A/B: comparar NPS con y sin sugerencia útil
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_nba_ab_summary AS
SELECT
    f.feedback                                  AS grupo,
    COUNT(*)                                    AS n,
    ROUND(AVG(f.nps_post_atencion), 2)          AS nps_promedio,
    ROUND(AVG(f.atencion_minutos), 1)           AS atencion_prom_min
FROM nba_feedback_log f
WHERE f.nps_post_atencion IS NOT NULL
GROUP BY f.feedback;
