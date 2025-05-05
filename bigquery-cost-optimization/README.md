# BigQuery Cost Optimization with UPSERT Strategy

## Overview
This project focuses on optimizing BigQuery ETL costs by replacing full-table loads with incremental `MERGE` (UPSERT) operations. Instead of reloading entire datasets daily, we update only the changed or new records.

## Problem
Previously, our ETL jobs performed full refreshes on large tables regardless of whether data had changed, leading to:
- High BigQuery costs
- Longer job runtimes
- Inefficient resource usage

## Solution
I implemented an UPSERT mechanism using BigQuery's `MERGE` statement. It identifies changes by comparing keys (e.g., `order_id`) and performs insert or update operations conditionally.

### Sample SQL Logic:
```sql
MERGE target_table t
USING staging_table s
ON t.order_id = s.order_id
WHEN MATCHED THEN
  UPDATE SET t.status = s.status, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (order_id, status, updated_at)
  VALUES (s.order_id, s.status, s.updated_at)
```

## Real Cost Comparison
To show the impact, here’s a simulated comparison between my previous approach and the optimized one:
![Image description](ttps://lh4.googleusercontent.com/d6XpPf_HQxFM5LLLyCD94tMYoSPjUzGoS4KSBcqGHRk-5RC7J52DEbBb6pDznlg-UaOxaE5pANJvEua5IKpA7LixN_0EADLJI79XW0hUvTqfYAKi4jUl3G9Fws5uPBJSEg=w1280)
Cost Saving: ~Rp 1.93 million per month, or 99% reduction in query cost. And thats only the cost of one datamart ETL.
