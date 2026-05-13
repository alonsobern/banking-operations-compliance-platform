-- Analytical SQL Queries for Banking Operations & Compliance Monitoring
-- Optimized for Power BI Dashboards and Executive Reporting

---------------------------------------------------------
-- 1. DAILY TRANSACTION OVERVIEW
-- Purpose: Monitor liquidity and operational load over time.
---------------------------------------------------------
CREATE OR REPLACE VIEW dashboard_daily_stats AS
SELECT 
    transaction_date,
    COUNT(transaction_id) AS total_transactions,
    SUM(CASE WHEN status = 'Completed' THEN amount ELSE 0 END) AS successful_volume,
    COUNT(CASE WHEN status = 'Failed' THEN 1 END) AS failed_count,
    ROUND(COUNT(CASE WHEN status = 'Failed' THEN 1 END)::NUMERIC / COUNT(transaction_id) * 100, 2) AS failure_rate_pct
FROM transactions
GROUP BY transaction_date
ORDER BY transaction_date DESC;

---------------------------------------------------------
-- 2. CUSTOMER SEGMENT PROFITABILITY & RISK
-- Purpose: Understand which segments drive volume vs. risk flags.
---------------------------------------------------------
CREATE OR REPLACE VIEW dashboard_segment_analysis AS
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    COUNT(t.transaction_id) AS tx_count,
    ROUND(SUM(t.amount), 2) AS total_volume,
    ROUND(AVG(t.amount), 2) AS avg_tx_value,
    COUNT(cf.flag_id) AS risk_flags_count,
    ROUND(COUNT(cf.flag_id)::NUMERIC / COUNT(t.transaction_id) * 100, 3) AS flag_rate_pct
FROM customers c
LEFT JOIN accounts a ON c.customer_id = a.customer_id
LEFT JOIN transactions t ON a.account_id = t.account_id
LEFT JOIN compliance_flags cf ON t.transaction_id = cf.transaction_id
GROUP BY c.customer_segment;

---------------------------------------------------------
-- 3. BRANCH OPERATIONAL ACTIVITY
-- Purpose: Identify top-performing and high-risk branches.
---------------------------------------------------------
CREATE OR REPLACE VIEW dashboard_branch_ranking AS
SELECT 
    b.branch_name,
    b.city,
    COUNT(t.transaction_id) AS tx_volume,
    SUM(t.amount) AS total_amount,
    COUNT(DISTINCT a.account_id) AS active_accounts,
    COUNT(cf.flag_id) AS alert_count
FROM branches b
JOIN accounts a ON b.branch_id = a.branch_id
JOIN transactions t ON a.account_id = t.account_id
LEFT JOIN compliance_flags cf ON t.transaction_id = cf.transaction_id
GROUP BY b.branch_id, b.branch_name, b.city
ORDER BY tx_volume DESC;

---------------------------------------------------------
-- 4. PEAK TRANSACTION HOURS (HEATMAP DATA)
-- Purpose: Resource planning for server load and support.
---------------------------------------------------------
CREATE OR REPLACE VIEW dashboard_peak_hours AS
SELECT 
    EXTRACT(HOUR FROM transaction_timestamp) AS tx_hour,
    transaction_type,
    COUNT(*) AS volume,
    ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
GROUP BY tx_hour, transaction_type
ORDER BY tx_hour;

---------------------------------------------------------
-- 5. SUSPICIOUS ACTIVITY SUMMARY (KYC/AML)
-- Purpose: Real-time monitoring of rule violations.
---------------------------------------------------------
CREATE OR REPLACE VIEW dashboard_compliance_alerts AS
SELECT 
    rule_id,
    risk_level,
    status,
    COUNT(*) AS alert_count,
    ROUND(AVG(risk_score), 2) AS avg_risk_score,
    SUM(amount) AS exposure_amount
FROM compliance_flags
GROUP BY rule_id, risk_level, status
ORDER BY alert_count DESC;

---------------------------------------------------------
-- 6. CHANNEL PREFERENCE BY SEGMENT
-- Purpose: Marketing and digital transformation insights.
---------------------------------------------------------
CREATE OR REPLACE VIEW dashboard_channel_usage AS
SELECT 
    customer_segment,
    channel,
    COUNT(*) AS usage_count,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER(PARTITION BY customer_segment) * 100, 2) AS channel_share_pct
FROM transactions
GROUP BY customer_segment, channel;

---------------------------------------------------------
-- 7. TOP MERCHANTS & CATEGORIES BY VOLUME
-- Purpose: Spending pattern analysis for retail banking.
---------------------------------------------------------
CREATE OR REPLACE VIEW dashboard_merchant_insights AS
SELECT 
    merchant_category,
    COUNT(*) AS tx_count,
    SUM(amount) AS total_spent,
    ROUND(AVG(amount), 2) AS avg_ticket_size
FROM transactions
WHERE merchant_category IS NOT NULL
GROUP BY merchant_category
ORDER BY total_spent DESC
LIMIT 10;
