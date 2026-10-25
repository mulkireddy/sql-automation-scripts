# PRODUCTION DATABASE INCIDENT POST-MORTEM & ROOT CAUSE ANALYSIS (RCA)

## Incident Metadata

| Incident Metadata | Details |
| :--- | :--- |
| **Incident Title / ID** | [e.g., INC-2026-0412: Production SQL Server OrderDB Blocking Outage] |
| **Date & Time (UTC)** | [Start Time] to [Resolution Time] (Total Duration: XX mins) |
| **Severity Level** | [P1 - Critical / P2 - High] |
| **Affected Instance / DB** | [ServerName \ DatabaseName] |
| **Incident Commander / DBA** | [Your Name] |

---

## 1. Executive Summary
Provide a 3-sentence non-technical summary for leadership explaining what happened, the customer impact, and how it was resolved.

---

## 2. Incident Timeline (All Times UTC)
* **HH:MM** — Monitoring alert triggered for [Symptom: e.g., CPU at 100% / Connection Timeouts].
* **HH:MM** — DBA executed `01_First_60_Seconds_Triage.sql` and identified [Root Cause metric].
* **HH:MM** — Remediation applied: [e.g., Terminated orphaned sleeping SPID 142 / Forced Query Store Plan ID 48].
* **HH:MM** — Server vital signs (`SQL_CPU_Pct`, `Blocked_Sessions`) returned to baseline.

---

## 3. Technical Root Cause Analysis (5 Whys)
* **Primary Bottleneck Wait Type:** [e.g., LCK_M_X / RESOURCE_SEMAPHORE / SOS_SCHEDULER_YIELD]
* **Culprit Query / Object / Host:** [Paste SQL Snippet or Procedure Name from Toolkit Output]
* **Why did it happen?** [Explain parameter sniffing, missing index, or uncommitted app transaction]

---

## 4. Permanent Corrective & Preventive Actions

| # | Action Item | Owner | Target Date | Status |
| :- | :--- | :--- | :--- | :--- |
| 1 | [e.g., Add non-clustered covering index on dbo.Orders(CustomerID, Status)] | DBA | YYYY-MM-DD | Open |
| 2 | [e.g., Wrap microservice call in using block with 30s command timeout] | App Team | YYYY-MM-DD | Open |
