WITH
  standard_discount_rates AS (
    SELECT
      sender_location_id,
      recipient_location_id,
      express_type,
      shipping_client_id,
      discount_rate
    FROM
      `my_project.my_datawarehouse.dim_standard_rates` -- Anonymized table name
    WHERE
      DATE(end_expire_time, 'Asia/Jakarta') > CURRENT_DATE('Asia/Jakarta')
      AND deleted = '0'
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          search_code
        ORDER BY
          created_at DESC
      ) = 1
  ),
  pickup AS (
    SELECT
      DATE(shipping_time, 'Asia/Jakarta') date_key,
      'pickup' AS flag,
      CASE
        WHEN t1.option_name IN ('iDtruck') THEN 'Cargo' -- Kept specific type name as example
        ELSE 'Non Cargo'
      END AS parent_source,
      CASE
        WHEN s2.finance_source_category IS NOT NULL THEN s2.finance_source_category
        WHEN s1.finance_source_category = 'Partner C Legal Entity' THEN 'PartnerC' -- Anonymized specific partner name
        WHEN s1.finance_source_category = 'Partner D Legal Entity' THEN 'PartnerD' -- Anonymized specific partner name
        ELSE s1.finance_source_category
      END AS waybill_finance_source_category,
      -- Additional columns for detail
      s0.option_name AS waybill_source,
      ww.parent_client_identifier AS parent_shipping_client, -- Generalized column name
      'pod delivery' AS pod_category, -- Defaulting pickup to delivery category here, review if intended
      SUM(IF(waybill_no IS NOT NULL, 1, 0)) AS volume,
      SUM(CAST(item_calculated_weight AS NUMERIC)) AS weight,
      0 AS gross_shipping_fee, -- Placeholder for pickup stage
      0 AS net_shipping_fee, -- Placeholder for pickup stage
      0 AS cod_fee, -- Placeholder for pickup stage
      0 AS return_shipping_fee -- Placeholder for pickup stage
    FROM
      `my_project.my_datawarehouse.fact_waybill_base` ww -- Anonymized table name
      LEFT JOIN `my_project.my_datawarehouse.dim_system_codes` t1 ON express_type = t1.option_value AND t1.type_option = 'expressType'
      LEFT JOIN `my_project.my_datawarehouse.dim_system_codes` s0 ON ww.waybill_source = s0.option_value AND s0.type_option = 'waybillSource'
      LEFT JOIN `my_project.my_datawarehouse.dim_system_codes` t3 ON waybill_status = t3.option_value AND t3.type_option = 'waybillStatus'
      LEFT JOIN `my_project.my_datamart.dim_source_finance_mapping` s1 ON s0.option_name = s1.waybill_source AND s1.type_option = 'waybillSource' -- Anonymized table name
      LEFT JOIN `my_project.my_datamart.dim_source_finance_mapping` s2 ON s2.parent_shipping_cleint = ww.parent_client_identifier AND s2.type_option = 'vipSeller' AND ww.waybill_source = '00' -- Anonymized table name
    WHERE
      ww.pickup_branch_name NOT IN (
        'AGENT_LIAONING', 'AGENT_SHANDONG', 'TH TESTING HQ', 'TH_DALIAN',
        'HEADQUARTER', 'TH_SHENYANG', 'TH_QINGDAO', 'PDB_AIRPORT', 'PDB_TECH',
        'PDB_SHENYANG', 'PDB_QINGDAO', 'TH_DALIAN1', 'TH_QINGDAO1', 'PDB_SHENYANG1'
      ) -- Kept branch exclusions as examples
      AND ww.void_flag = '0'
      AND ww.deleted = '0'
      AND DATE(ww.shipping_time, 'Asia/Jakarta') >= DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -93 DAY)
    GROUP BY
      7, 6, 5, 4, 3, 2, 1
  ),
  delivered AS (
    SELECT
      DATE(pod_date) date_key,
      'delivered' AS flag,
      CASE
        WHEN ww.express_type IN ('iDtruck') THEN 'Cargo' -- Kept specific type name as example
        ELSE 'Non Cargo'
      END AS parent_source,
      CASE
        WHEN s2.finance_source_category IS NOT NULL THEN s2.finance_source_category
        WHEN s1.finance_source_category = 'Partner C Legal Entity' THEN 'PartnerC' -- Anonymized specific partner name
        WHEN s1.finance_source_category = 'Partner D Legal Entity' THEN 'PartnerD' -- Anonymized specific partner name
        ELSE s1.finance_source_category
      END AS waybill_finance_source_category,
      ww.waybill_source,
      ww.parent_shipping_cleint, -- Assuming this name is expected from the source table
      ww.pod_category,
      SUM(IF(ww.waybill_no IS NOT NULL, 1, 0)) AS volume,
      SUM(ww.cargo_weight) AS weight, -- Assuming cargo_weight is relevant here
      SUM(ww.total_shipping_fee) AS gross_shipping_fee,
      SUM(ww.net_shipping_fee) AS net_shipping_fee,
      SUM(IF(ww.cod_fee > 0, ww.cod_fee, 0)) AS cod_fee,
      SUM(IF(ww.return_shipping_fee > 0, ww.return_shipping_fee, 0)) AS return_shipping_fee
    FROM
      `my_project.my_datamart.fact_pod_details` ww -- Anonymized table name (from previous query's target)
      LEFT JOIN `my_project.my_datamart.dim_source_finance_mapping` s1 ON ww.waybill_source = s1.waybill_source AND s1.type_option = 'waybillSource' -- Anonymized table name
      LEFT JOIN `my_project.my_datamart.dim_source_finance_mapping` s2 ON s2.parent_shipping_cleint = ww.parent_shipping_cleint AND s2.type_option = 'vipSeller' AND ww.waybill_source = 'Key Account Portal' -- Anonymized table name & source name
    WHERE
      DATE(pod_date) >= DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -120 DAY)
    GROUP BY
      7, 6, 5, 4, 3, 2, 1
  ),
  cross_docking AS (
    -- Pickup side for cross-docking
    SELECT
      DATE(inbound_time, 'Asia/Jakarta') AS date_key,
      'pickup' AS flag,
      'Non Cargo' AS parent_source,
      'Cross Docking' AS waybill_finance_source_category,
      '' AS waybill_source, -- Placeholder
      '' AS parent_shipping_cleint, -- Placeholder
      'pod delivery' AS pod_category, -- Defaulting
      COUNT(booking_code) AS volume,
      0 AS weight, -- Placeholder
      0 AS gross_shipping_fee, -- Placeholder
      0 AS net_shipping_fee, -- Placeholder
      0 AS cod_fee, -- Placeholder
      0 AS return_shipping_fee -- Placeholder
    FROM
      `my_project.my_datawarehouse.fact_crossdock_orders` -- Anonymized table name
    WHERE
      inbound_hub_name <> 'TH TESTING HQ' -- Example exclusion
    GROUP BY
      1, 2, 3, 4, 5, 6, 7
    UNION ALL
    -- Delivered/Processed side for cross-docking
    SELECT
      DATE(print_group_time) AS date_key, -- Using print_group_time as the key date here
      'delivered' AS flag,
      'Non Cargo' AS parent_source,
      'Cross Docking' AS waybill_finance_source_category,
      '' AS waybill_source, -- Placeholder
      '' AS parent_shipping_cleint, -- Placeholder
      'pod delivery' AS pod_category, -- Defaulting
      COUNT(booking_code) AS volume,
      0 AS weight, -- Placeholder
      0 AS gross_shipping_fee, -- Placeholder
      SUM(invoice) AS net_shipping_fee, -- Assuming 'invoice' represents the net fee
      0 AS cod_fee, -- Placeholder
      0 AS return_shipping_fee -- Placeholder
    FROM
      `my_project.my_datamart.agg_partnerB_crossdock_summary` -- Anonymized table name
    GROUP BY
      1, 2, 3, 4, 5, 6, 7
  ),
  union_data AS (
    SELECT * FROM pickup
    UNION ALL
    SELECT * FROM delivered
    UNION ALL
    SELECT * FROM cross_docking
  )
SELECT
  date_key,
  parent_source,
  IF(
    waybill_finance_source_category IS NULL,
    'Not Defined',
    waybill_finance_source_category
  ) AS waybill_finance_source_category,
  waybill_source,
  parent_shipping_cleint,
  pod_category,
  SUM(IF(flag = 'pickup', volume, 0)) AS pickup_volume,
  SUM(IF(flag = 'pickup' AND parent_source = 'Cargo', weight, 0)) AS pickup_weight,
  SUM(IF(flag = 'delivered', volume, 0)) AS delivered_volume,
  SUM(IF(flag = 'delivered' AND parent_source = 'Cargo', weight, 0)) AS delivered_weight,
  SUM(
    IF(
      flag = 'delivered',
      gross_shipping_fee + IFNULL(cod_fee, 0) + IFNULL(return_shipping_fee, 0),
      0
    )
  ) AS gross_revenue, -- Renamed for clarity
  ROUND(
    SUM(
      IF(
        flag = 'delivered',
        net_shipping_fee + IFNULL(cod_fee, 0) + IFNULL(return_shipping_fee, 0),
        0
      )
    ),
    0
  ) AS indicative_net_revenue -- Renamed for clarity
FROM
  union_data
WHERE
  DATE(date_key) >= DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -120 DAY)
GROUP BY
  1, 2, 3, 4, 5, 6
