# Logistics Order Journey Scorecards

This repository contains the SQL queries and documentation for the Logistics Order Journey Scorecards dashboard, developed using Looker Studio and Google BigQuery.

## 🔍 Project Overview

The dashboard tracks package movements from order initiation to delivery, highlighting key performance indicators (KPIs) such as pickup rates, transit volumes, and delivery success. It also identifies exceptions like unpicked, canceled, and returned orders to facilitate prompt issue resolution.

![Dashboard Overview](images/dashboard_overview.png)

For a comprehensive project description, visit the [project page](https://www.aditoworks.my.id/portfolio/logistics-order-journey-scorecards).

## 🧩 Key Challenges

- **Data Derivation:** Essential metrics like `is_pickup` and `is_delivered` are not directly available in the main order table.
- **Historical Data Analysis:** Required extracting and interpreting data from the historical scan records table, which lacks explicit flags or a predefined chronological order.

## 🛠️ Technical Approach

- **Process Level Classification:** Implemented logic to determine the shipment stage (First Mile, Medium Mile, Last Mile) based on branch prefixes and scan types.

  ```sql
  CASE
    WHEN SUBSTR(sc.operation_branch_name,1,2) IN ('MH','DC','HQ') THEN 'Medium Mile'
    WHEN SUBSTR(sc.operation_branch_name,1,2) IN ('TH','FB','PD') 
         AND t1.option_name IN ('Pick up scan','Loading scan','Packing scan','Sending scan') THEN 'First Mile'
    WHEN ...
    ELSE 'Not Defined'
  END AS process_level

