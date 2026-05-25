-- ============================================================================
-- Operational & Compliance KPI Metrics
-- Banking Operations & Compliance Monitoring Platform
-- Designed for Power BI Direct Query or Import
-- ============================================================================
-- NOTE: DROP before CREATE is required because PostgreSQL's
--       CREATE OR REPLACE VIEW cannot alter existing column definitions.
-- ============================================================================

---------------------------------------------------------
-- 1. COMPLIANCE ALERT TRENDS (Time Series)
-- Purpose: Monitor if suspicious activity is increasing.
---------------------------------------------------------
DROP VIEW IF EXISTS dashboard_compliance_trends CASCADE;

CREATE VIEW dashboard_compliance_trends AS
SELECT
    DATE_TRUNC('day', flagged_at)::DATE AS alert_date,
    rule_id,
    risk_level,
    COUNT(*)                            AS alert_count
FROM compliance_flags
GROUP BY
    DATE_TRUNC('day', flagged_at)::DATE,
    rule_id,
    risk_level
ORDER BY alert_date DESC;

---------------------------------------------------------
-- 2. TRANSACTION SUCCESS VS RISK RATIO (Global KPIs)
-- Purpose: Global health metric of the banking platform.
---------------------------------------------------------
DROP VIEW IF EXISTS dashboard_global_kpis CASCADE;

CREATE VIEW dashboard_global_kpis AS
WITH tx_stats AS (
    SELECT
        COUNT(*)                                                    AS total_tx,
        COALESCE(SUM(amount), 0)                                    AS total_volume,
        COUNT(CASE WHEN status = 'Completed' THEN 1 END)            AS success_tx,
        COUNT(CASE WHEN status = 'Failed'    THEN 1 END)            AS failed_tx
    FROM transactions
),
flag_stats AS (
    SELECT COUNT(DISTINCT transaction_id) AS flagged_tx
    FROM compliance_flags
)
SELECT
    total_tx,
    ROUND(total_volume, 2)                                          AS total_volume,
    ROUND(CASE WHEN total_tx > 0
          THEN success_tx::NUMERIC / total_tx * 100
          ELSE 0 END, 2)                                            AS success_rate_pct,
    ROUND(CASE WHEN total_tx > 0
          THEN failed_tx::NUMERIC  / total_tx * 100
          ELSE 0 END, 2)                                            AS failure_rate_pct,
    ROUND(CASE WHEN total_tx > 0
          THEN flagged_tx::NUMERIC / total_tx * 100
          ELSE 0 END, 4)                                            AS suspicious_rate_pct,
    ROUND(CASE WHEN total_tx > 0
          THEN total_volume / total_tx
          ELSE 0 END, 2)                                            AS avg_tx_amount
FROM tx_stats, flag_stats;

---------------------------------------------------------
-- 3. CUSTOMER RISK PROFILING
-- Purpose: Identify high-risk segments for targeted auditing.
---------------------------------------------------------
DROP VIEW IF EXISTS dashboard_customer_risk_kpis CASCADE;

CREATE VIEW dashboard_customer_risk_kpis AS
SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_id)                                   AS total_customers,
    COUNT(cf.flag_id)                                               AS total_alerts,
    ROUND(
        CASE WHEN COUNT(DISTINCT c.customer_id) > 0
        THEN COUNT(cf.flag_id)::NUMERIC / COUNT(DISTINCT c.customer_id)
        ELSE 0 END, 2
    )                                                               AS alerts_per_customer,
    SUM(CASE WHEN cf.risk_level IN ('High', 'Critical')
        THEN 1 ELSE 0 END)                                          AS high_risk_alerts
FROM customers c
LEFT JOIN accounts        a  ON c.customer_id  = a.customer_id
LEFT JOIN compliance_flags cf ON a.account_id  = cf.account_id
GROUP BY c.customer_segment;

---------------------------------------------------------
-- 4. BRANCH COMPLIANCE EFFICIENCY
-- Purpose: Measure branch-level operational risk.
---------------------------------------------------------
DROP VIEW IF EXISTS dashboard_branch_compliance_kpis CASCADE;

CREATE VIEW dashboard_branch_compliance_kpis AS
SELECT
    b.branch_name,
    b.city,
    b.state,
    COUNT(t.transaction_id)                                         AS tx_volume,
    COUNT(cf.flag_id)                                               AS alert_volume,
    ROUND(
        COALESCE(
            COUNT(cf.flag_id)::NUMERIC / NULLIF(COUNT(t.transaction_id), 0) * 100,
        0), 3)                                                      AS flag_rate_pct,
    SUM(CASE WHEN t.status = 'Failed' THEN 1 ELSE 0 END)           AS failed_tx,
    ROUND(COALESCE(SUM(t.amount), 0), 2)                           AS total_volume_usd
FROM branches b
JOIN     accounts         a  ON b.branch_id    = a.branch_id
LEFT JOIN transactions    t  ON a.account_id   = t.account_id
LEFT JOIN compliance_flags cf ON t.transaction_id = cf.transaction_id
GROUP BY b.branch_id, b.branch_name, b.city, b.state
ORDER BY flag_rate_pct DESC;

---------------------------------------------------------
-- 5. SEGMENT ANALYSIS (used by insight_generator.py)
-- Purpose: Power BI customer segment risk and volume view.
---------------------------------------------------------
DROP VIEW IF EXISTS dashboard_segment_analysis CASCADE;

CREATE VIEW dashboard_segment_analysis AS
SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_id)                                   AS total_customers,
    COUNT(t.transaction_id)                                         AS transaction_count,
    ROUND(COALESCE(SUM(t.amount), 0), 2)                           AS total_volume,
    COUNT(cf.flag_id)                                               AS risk_flags_count,
    ROUND(
        COALESCE(
            COUNT(cf.flag_id)::NUMERIC / NULLIF(COUNT(t.transaction_id), 0) * 100,
        0), 2)                                                      AS flag_rate_pct
FROM customers c
LEFT JOIN accounts         a  ON c.customer_id  = a.customer_id
LEFT JOIN transactions     t  ON a.account_id   = t.account_id
LEFT JOIN compliance_flags cf ON t.transaction_id = cf.transaction_id
GROUP BY c.customer_segment;
