## Finance Settlement Report Dashboard

This project enhances accuracy and efficiency in financial reconciliation by transforming raw transaction data into an optimized BI solution.  It empowers finance teams to validate settlements, match aggregated figures with raw data, and seamlessly download reports via Google Cloud Storage.

**Key Features and Contributions:**

* **Crucial Raw Data for Reconciliation:**
    * Integrated scattered datasets—transaction records, payment logs, shipping fees, and platform deductions—into a structured datamart.
    * Reduced manual effort, ensured data accuracy, and provided finance teams with a final dataset tailored for reconciliation and revenue validation.
* **Accurate Aggregation Matching Raw Data:**
    * Designed a monitoring dashboard to track the number of raw records inserted into each datamart.
    * Ensured every transaction processed aligns with the aggregated totals in the final reports.
    * Enabled early detection of anomalies, prevented revenue misallocation, and ensured compliance with financial audits.
* **Automated CSV Download via Google Cloud Storage:**
    * Maintained and troubleshooted the data pipeline, including identifying errors and backfilling missing data.
    * Ensured finance teams receive complete reports.
    * Enabled finance teams to seamlessly download structured reports for audits, analysis, and system integration from Google Cloud Storage.
    * Reduced manual workload and improved efficiency.
* **Additional Features & Impact:**
    * **Multi-Level Filtering & Drilldowns:** Users can segment financial data by platform, payment type, or region for deep-dive analysis.
    * **Real-Time Data Processing:** Google Cloud Functions and BigQuery ensure near-instant data availability, minimizing reporting delays.
    * **Operational Cost Savings:** Shifted data processing from the main system to BI dashboards, significantly cutting costs associated with raw data extraction.

**Overall Impact:**

By integrating structured financial data, automated reporting, and scalable reconciliation tools, this Finance Settlement Report dashboard has become an essential asset for finance teams—improving efficiency, ensuring accuracy, and optimizing operational costs. 🚀

**Tech Stack:**

* SQL
* Google Cloud Platform (GCP)
    * BigQuery
    * Google Cloud Storage
    * Google Cloud Functions

**Explore the Project:**

This project demonstrates how data engineering and BI development can be combined to solve a critical business problem in finance. Here's what you'll find in the [GitHub Repository]([Your Repository Link Here]):

* **SQL Code:** The SQL scripts used to create the datamart, including data transformation, cleaning, and aggregation logic. This showcases how the raw data was structured for efficient analysis.
* **Data Pipeline Architecture:** Details on how data flows from the source systems into BigQuery, including the role of Google Cloud Functions in automating data processing.
* **Monitoring Implementation:** Code and configuration related to the data monitoring dashboard, demonstrating how data accuracy and completeness are validated.
* **GCS Integration:** Information on how reports are automatically generated and stored in Google Cloud Storage, including any relevant scripts or configurations.
* **Key Learnings:** Reflections on the project, including challenges faced, solutions implemented, and lessons learned about building robust financial reporting systems.

By exploring these resources, you can gain a deeper understanding of how the Finance Settlement Report dashboard was designed and implemented, and how it delivers value to finance teams.

