-- Advanced Analytical Reporting Views
-- Optimized for Power BI with Business-Friendly Aliases

---------------------------------------------------------
-- 1. BRANCH PERFORMANCE SUMMARY
---------------------------------------------------------
CREATE OR REPLACE VIEW branch_performance_summary AS
SELECT 
    b.branch_name AS "Branch_Name",
    b.city AS "City",
    b.state AS "State",
    COUNT(t.transaction_id) AS "Total_Transactions",
    SUM(t.amount) AS "Total_Volume_USD",
    ROUND(AVG(t.amount), 2) AS "Average_Transaction_Size",
    COUNT(DISTINCT a.customer_id) AS "Unique_Customers_Served",
    ROUND(SUM(CASE WHEN t.status = 'Completed' THEN t.amount ELSE 0 END), 2) AS "Successful_Volume",
    ROUND(SUM(CASE WHEN t.status = 'Failed' THEN t.amount ELSE 0 END), 2) AS "Failed_Volume"
FROM branches b
JOIN accounts a ON b.branch_id = a.branch_id
JOIN transactions t ON a.account_id = t.account_id
GROUP BY b.branch_id, b.branch_name, b.city, b.state;

---------------------------------------------------------
-- 2. SUSPICIOUS ACTIVITY SUMMARY
---------------------------------------------------------
CREATE OR REPLACE VIEW suspicious_activity_summary AS
SELECT 
    cf.rule_id AS "Detection_Rule",
    cf.risk_level AS "Risk_Level",
    cf.status AS "Alert_Status",
    COUNT(*) AS "Total_Alerts",
    ROUND(AVG(cf.risk_score), 1) AS "Average_Risk_Score",
    SUM(cf.amount) AS "Total_Flagged_Amount",
    COUNT(DISTINCT cf.account_id) AS "Unique_Accounts_Flagged",
    cf.analyst_assigned AS "Assigned_Analyst"
FROM compliance_flags cf
GROUP BY cf.rule_id, cf.risk_level, cf.status, cf.analyst_assigned;

---------------------------------------------------------
-- 3. CUSTOMER TRANSACTION SUMMARY
---------------------------------------------------------
CREATE OR REPLACE VIEW customer_transaction_summary AS
SELECT 
    c.customer_id AS "Customer_ID",
    c.first_name || ' ' || c.last_name AS "Customer_Name",
    c.customer_segment AS "Segment",
    c.kyc_status AS "KYC_Status",
    COUNT(t.transaction_id) AS "Transaction_Count",
    SUM(t.amount) AS "Life_Time_Volume",
    ROUND(AVG(t.amount), 2) AS "Average_Spend",
    MAX(t.transaction_timestamp) AS "Last_Transaction_Date",
    COUNT(cf.flag_id) AS "Compliance_Flags_Raised"
FROM customers c
LEFT JOIN accounts a ON c.customer_id = a.customer_id
LEFT JOIN transactions t ON a.account_id = t.account_id
LEFT JOIN compliance_flags cf ON t.transaction_id = cf.transaction_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_segment, c.kyc_status;

---------------------------------------------------------
-- 4. TRANSACTION FAILURE ANALYSIS
---------------------------------------------------------
CREATE OR REPLACE VIEW transaction_failure_analysis AS
SELECT 
    failure_reason AS "Failure_Reason",
    transaction_type AS "Transaction_Type",
    payment_method AS "Payment_Method",
    COUNT(*) AS "Failure_Count",
    SUM(amount) AS "Lost_Volume_USD",
    ROUND(AVG(amount), 2) AS "Average_Failed_Amount"
FROM transactions
WHERE status = 'Failed'
GROUP BY failure_reason, transaction_type, payment_method;

---------------------------------------------------------
-- 5. COMPLIANCE ALERT TRENDS
---------------------------------------------------------
CREATE OR REPLACE VIEW compliance_alert_trends AS
SELECT 
    DATE_TRUNC('month', flagged_at) AS "Month",
    rule_id AS "Detection_Rule",
    risk_level AS "Risk_Level",
    COUNT(*) AS "Monthly_Alert_Count",
    SUM(amount) AS "Monthly_Flagged_Volume"
FROM compliance_flags
GROUP BY "Month", rule_id, risk_level
ORDER BY "Month" DESC;

---------------------------------------------------------
-- 6. DAILY OPERATIONS SUMMARY
---------------------------------------------------------
CREATE OR REPLACE VIEW daily_operations_summary AS
SELECT 
    transaction_date AS "Operation_Date",
    COUNT(*) AS "Daily_Tx_Count",
    SUM(amount) AS "Daily_Volume_USD",
    COUNT(CASE WHEN status = 'Completed' THEN 1 END) AS "Successful_Tx",
    COUNT(CASE WHEN status = 'Failed' THEN 1 END) AS "Failed_Tx",
    ROUND(COUNT(CASE WHEN status = 'Failed' THEN 1 END)::NUMERIC / COUNT(*) * 100, 2) AS "Error_Rate_Pct",
    COUNT(DISTINCT account_id) AS "Active_Accounts_Today"
FROM transactions
GROUP BY transaction_date
ORDER BY transaction_date DESC;
