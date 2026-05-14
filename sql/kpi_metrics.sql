-- Operational & Compliance KPI Metrics
-- Designed for Power BI Direct Query or Import

---------------------------------------------------------
-- 1. COMPLIANCE ALERT TRENDS (Time Series)
-- Purpose: Monitor if suspicious activity is increasing.
---------------------------------------------------------
CREATE OR REPLACE VIEW dashboard_compliance_trends AS
SELECT 
    DATE_TRUNC('day', flagged_at) AS alert_date,
    rule_id,
    risk_level,
    COUNT(*) AS alert_count
FROM compliance_flags
GROUP BY alert_date, rule_id, risk_level
ORDER BY alert_date DESC;

---------------------------------------------------------
-- 2. TRANSACTION SUCCESS VS RISK RATIO
-- Purpose: Global health metric of the banking platform.
---------------------------------------------------------
CREATE OR REPLACE VIEW dashboard_global_kpis AS
WITH tx_stats AS (
    SELECT 
        COUNT(*) AS total_tx,
        SUM(amount) AS total_volume,
        COUNT(CASE WHEN status = 'Completed' THEN 1 END) AS success_tx,
        COUNT(CASE WHEN status = 'Failed' THEN 1 END) AS failed_tx
    FROM transactions
),
flag_stats AS (
    SELECT COUNT(DISTINCT transaction_id) AS flagged_tx
    FROM compliance_flags
)
SELECT 
    total_tx,
    total_volume,
    ROUND(success_tx::NUMERIC / total_tx * 100, 2) AS success_rate_pct,
    ROUND(failed_tx::NUMERIC / total_tx * 100, 2) AS failure_rate_pct,
    ROUND(flagged_tx::NUMERIC / total_tx * 100, 4) AS suspicious_rate_pct,
    ROUND(total_volume / total_tx, 2) AS avg_tx_amount
FROM tx_stats, flag_stats;

---------------------------------------------------------
-- 3. CUSTOMER RISK PROFILING
-- Purpose: Identify high-risk segments for targeted auditing.
---------------------------------------------------------
CREATE OR REPLACE VIEW dashboard_customer_risk_kpis AS
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COUNT(cf.flag_id) AS total_alerts,
    ROUND(COUNT(cf.flag_id)::NUMERIC / COUNT(DISTINCT c.customer_id), 2) AS alerts_per_customer,
    SUM(CASE WHEN cf.risk_level = 'High' OR cf.risk_level = 'Critical' THEN 1 ELSE 0 END) AS high_risk_alerts
FROM customers c
LEFT JOIN accounts a ON c.customer_id = a.customer_id
LEFT JOIN compliance_flags cf ON a.account_id = cf.account_id
GROUP BY c.customer_segment;

---------------------------------------------------------
-- 4. BRANCH COMPLIANCE EFFICIENCY
-- Purpose: Measure branch-level operational risk.
---------------------------------------------------------
CREATE OR REPLACE VIEW dashboard_branch_compliance_kpis AS
SELECT 
    b.branch_name,
    COUNT(t.transaction_id) AS tx_volume,
    COUNT(cf.flag_id) AS alert_volume,
    ROUND(COUNT(cf.flag_id)::NUMERIC / NULLIF(COUNT(t.transaction_id), 0) * 100, 3) AS flag_rate_pct,
    SUM(CASE WHEN t.status = 'Failed' THEN 1 ELSE 0 END) AS failed_tx
FROM branches b
JOIN accounts a ON b.branch_id = a.branch_id
LEFT JOIN transactions t ON a.account_id = t.account_id
LEFT JOIN compliance_flags cf ON t.transaction_id = cf.transaction_id
GROUP BY b.branch_id, b.branch_name
ORDER BY flag_rate_pct DESC;
