# Executive Business Insights: Banking Operations & Compliance

This library provides production-ready, executive-style **Business Insight Card templates** and dynamic data examples designed for Power BI Smart Narrative visuals, multi-row KPI cards, or executive email summaries.

---

## 📈 1. Transaction Volume Insights

These insights help treasury and cash-flow management teams monitor liquidity and channel operational capacity.

### **[Insight 01] Daily Volume Surge Alert**
*   **Visual Type**: Smart Narrative Header / Callout Card
*   **Semantic Color**: 🔵 Indigo Blue (Neutral Load Shift)
*   **Insight Text**: 
    > **"Daily transaction volume reached $41.8M today, a +12.4% increase over the 30-day moving average. Operations remain stable, with card processing capacity operating at 74% of maximum threshold."**
*   **Dynamic DAX Variables**:
    *   `[Current_Volume]` = `$41,824,190`
    *   `[Volume_YoY_Change]` = `+12.4%`
    *   `[System_Load_Capacity]` = `74%`

### **[Insight 02] Weekend Channel Shift**
*   **Visual Type**: Multi-Row KPI Description
*   **Semantic Color**: 🟢 Emerald Mint (Positive Efficiency)
*   **Insight Text**: 
    > **"Mobile banking transactions increased to 89.2% of total retail volume this weekend (+4.1% vs. prior period), offsetting physical ATM cash demand by an estimated $1.2M in holding costs."**
*   **Dynamic DAX Variables**:
    *   `[Mobile_Share]` = `89.2%`
    *   `[ATM_Cost_Savings]` = `$1,200,000`

---

## 🛡️ 2. Suspicious Activity Spikes (Risk & Compliance)

These insights enable AML (Anti-Money Laundering) and fraud detection officers to identify coordinate risk attacks.

### **[Insight 03] Suspicious High-Value Activity Spike**
*   **Visual Type**: High-Attention Alert Banner
*   **Semantic Color**: 🔴 Crimson Coral (High Risk Alert)
*   **Insight Text**: 
    > **"CRITICAL: A surge of 14 new compliance flags was detected in the last 4 hours, primarily triggered by RULE_001 (Transfers > $10k) in the North Judithbury Branch. Flagged amount: $480,000."**
*   **Dynamic DAX Variables**:
    *   `[Recent_Alert_Count]` = `14`
    *   `[Flagged_Value]` = `$480,000`
    *   `[Trigger_Rule]` = `"RULE_001"`

### **[Insight 04] Sudden Inactive Account Activation**
*   **Visual Type**: Warning Alert Callout
*   **Semantic Color**: 🟡 Amber Gold (Medium Risk Alert)
*   **Insight Text**: 
    > **"WARNING: 8 accounts previously dormant for over 180 days exhibited sudden outbound international transfers totaling $94,000 within 24 hours. Accounts temporarily restricted pending KYC verification."**
*   **Dynamic DAX Variables**:
    *   `[Dormant_Flag_Count]` = `8`
    *   `[Outbound_Total]` = `$94,000`

---

## 🏢 3. Branch Performance & Operational Changes

These insights assist regional executives in identifying branch inefficiencies or localized market trends.

### **[Insight 05] Branch Deposit Concentration**
*   **Visual Type**: Regional Performance Executive Summary
*   **Semantic Color**: 🟢 Emerald Mint (Top Performer)
*   **Insight Text**: 
    > **"The North Judithbury Central Branch continues to lead operational performance, generating $12.1M in transaction volume (29% of total bank throughput) with an exceptional 99.8% customer satisfaction score."**
*   **Dynamic DAX Variables**:
    *   `[Top_Branch_Name]` = `"North Judithbury Central Branch"`
    *   `[Branch_Volume]` = `$12,120,400`
    *   `[Branch_Share]` = `29%`

### **[Insight 06] Regional Service Bottle-Neck**
*   **Visual Type**: Warning Indicator
*   **Semantic Color**: 🟡 Amber Gold (Attention Required)
*   **Insight Text**: 
    > **"The East Judithbury Branch experienced an average teller wait time increase of +6 minutes during peak hours (11:00 AM - 1:00 PM), coinciding with a 15% increase in physical deposit actions."**
*   **Dynamic DAX Variables**:
    *   `[Wait_Time_Increase]` = `"+6 minutes"`
    *   `[Deposit_Vol_Increase]` = `15%`

---

## ⚙️ 4. Payment Failure Anomalies (IT Operations)

These insights flag service interruptions and payment gateway issues for database and site reliability engineers.

### **[Insight 07] Payment Gateway Timeout Alert**
*   **Visual Type**: IT Operational Alert Header
*   **Semantic Color**: 🔴 Crimson Coral (Service Drop Alert)
*   **Insight Text**: 
    > **"ANOMALY: Transaction failure rates for Online Banking rose to 7.82% today (vs. SLA limit of 2.0%). Core error logs isolate 94% of failures to 'External Gateway Timeout' on the primary Visa network."**
*   **Dynamic DAX Variables**:
    *   `[Failure_Rate]` = `7.82%`
    *   `[SLA_Limit]` = `2.0%`
    *   `[Error_Source]` = `"External Gateway Timeout"`

### **[Insight 08] Insufficient Funds Trend**
*   **Visual Type**: Consumer Behavior Metric
*   **Semantic Color**: 🔵 Indigo Blue (Neutral Financial Behavior)
*   **Insight Text**: 
    > **"Non-technical failures peaked at 1,420 incidents, with 'Insufficient Funds' accounting for 84% of failed retail checkout transfers, consistent with historical end-of-month salary cycle behaviors."**
*   **Dynamic DAX Variables**:
    *   `[Failure_Count]` = `1,420`
    *   `[NSF_Share]` = `84%`
