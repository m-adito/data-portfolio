-- CREATE OR REPLACE TABLE `project_id.dataset_id.tbl_dashboard_order_monitoring`
-- PARTITION BY DATE(order_input_datetime)
-- CLUSTER BY waybill_number, order_source_name, customer_vip_name
-- AS

WITH order_data AS (
  SELECT
    DATETIME(ord.input_time, 'Asia/Jakarta') AS order_input_datetime,
    ord.sender_province AS sender_province_name,
    ord.sender_city AS sender_city_name,
    ord.sender_district AS sender_district_name,
    ord.recipient_province AS recipient_province_name,
    ord.recipient_city AS recipient_city_name,
    ord.recipient_address,
    src.option_name AS order_source_name,
    svc.option_name AS service_type_name,
    exp.option_name AS express_type_name,
    ord.waybill_no AS waybill_number,
    ord.order_no AS order_number,
    ord.ecommerce_order_no AS ecommerce_order_number,
    wb.return_flag AS is_returned_flag,
    ord.vip_customer_name AS customer_vip_name,
    IF(ord.input_time IS NOT NULL, 1, 0) AS is_order_created,
    IF(ord.order_status = '03' OR wb.waybill_no IS NOT NULL, 1, 0) AS is_picked_up,
    IF(ord.order_status = '04', 1, 0) AS is_order_canceled,
    IF(wb.void_flag = '1', 1, 0) AS is_void,
    IF(wb.pod_flag = '1' AND wb.pod_record_time IS NOT NULL, 1, 0) AS is_delivered,
    IF(wb.return_flag = '1' AND wb.return_pod_record_time IS NOT NULL, 1, 0) AS is_returned,
    IF(wb.deleted = '1', 1, 0) AS is_deleted,
    DATE(wb.update_time, 'Asia/Jakarta') AS update_date,
    ord.pickup_branch_name
  FROM `datawarehouse.shipping_order_dm` ord
  FULL JOIN `datawarehouse.shipping_waybill_dm` wb
    ON wb.waybill_no = ord.waybill_no
    AND DATE(wb.update_time, 'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 14 DAY)
  LEFT JOIN `datawarehouse.system_option` src
    ON ord.order_source = src.option_value AND src.type_option = 'orderSource'
  LEFT JOIN `datawarehouse.system_option` svc
    ON ord.service_type = svc.option_value AND svc.type_option = 'serviceType'
  LEFT JOIN `datawarehouse.system_option` exp
    ON ord.express_type = exp.option_value AND exp.type_option = 'expressType'
  WHERE DATE(ord.update_time, 'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 14 DAY)

  UNION ALL

  SELECT
    DATETIME(wb.create_time, 'Asia/Jakarta') AS order_input_datetime,
    wb.sender_province_name,
    wb.sender_city_name,
    wb.sender_district_name,
    wb.recipient_province_name,
    wb.recipient_city_name,
    wb.recipient_address,
    src.option_name AS order_source_name,
    svc.option_name AS service_type_name,
    exp.option_name AS express_type_name,
    wb.waybill_no AS waybill_number,
    wb.order_no AS order_number,
    wb.ecommerce_order_no AS ecommerce_order_number,
    wb.return_flag AS is_returned_flag,
    wb.vip_customer_name AS customer_vip_name,
    IF(wb.shipping_time IS NOT NULL, 1, 0) AS is_order_created,
    IF(wb.waybill_no IS NOT NULL, 1, 0) AS is_picked_up,
    0 AS is_order_canceled,
    IF(wb.void_flag = '1', 1, 0) AS is_void,
    IF(wb.pod_flag = '1' AND wb.pod_record_time IS NOT NULL, 1, 0) AS is_delivered,
    IF(wb.return_flag = '1' AND wb.return_pod_record_time IS NOT NULL, 1, 0) AS is_returned,
    IF(wb.deleted = '1', 1, 0) AS is_deleted,
    DATE(wb.update_time, 'Asia/Jakarta') AS update_date,
    wb.pickup_branch_name
  FROM `datawarehouse.shipping_waybill_dm` wb
  LEFT JOIN `datawarehouse.system_option` src
    ON wb.waybill_source = src.option_value AND src.type_option = 'orderSource'
  LEFT JOIN `datawarehouse.system_option` svc
    ON wb.service_type = svc.option_value AND svc.type_option = 'serviceType'
  LEFT JOIN `datawarehouse.system_option` exp
    ON wb.express_type = exp.option_value AND exp.type_option = 'expressType'
  WHERE DATE(wb.update_time, 'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 14 DAY)
    AND wb.waybill_source IN ('01', '03')
),

latest_scan AS (
  SELECT
    scn.waybill_no AS waybill_number,
    wb.sorting_code,
    scn.operation_branch_name,
    opt.option_name AS scan_operation_type,
    scn.operation_user_name,
    scn.destination_branch_name AS next_branch_name,
    DATETIME(scn.record_time, 'Asia/Jakarta') AS last_scan_time,
    REGEXP_SUBSTR(br.branch_no, "_(.*)") AS scan_branch_code,
    REGEXP_SUBSTR(ar.sorting_code, '[^-]+') AS mh_origin_code,
    REGEXP_SUBSTR(wb.sorting_code, '[^-]+') AS mh_destination_code,
    wb.waybill_status,
    wb.return_flag,
    prb.register_reason_bahasa AS problem_reason,
    scn.bag_no,
    CASE
      WHEN SUBSTR(scn.operation_branch_name, 1, 2) IN ('MH','DC','HQ') THEN 'Medium Mile'
      WHEN SUBSTR(scn.operation_branch_name, 1, 2) IN ('TH','FB','PD') AND opt.option_name IN ('Pick up scan','Loading scan','Packing scan','Sending scan') THEN 'First Mile'
      WHEN (SUBSTR(scn.operation_branch_name, 1, 2) IN ('VH','VT','TH','FB','PD') AND opt.option_name NOT IN ('Pick up scan','Loading scan','Packing scan','Sending scan') AND scn.operation_branch_name <> wb.pickup_branch_name)
        OR (SUBSTR(scn.origin_branch_name, 1, 2) IN ('MH','TH','DC') AND opt.option_name IN ('Arrival scan','Unloading scan','Unpacking scan','Delivery scan','Create Return Bill','Delivery Task Accept','Collection')) THEN 'Last Mile'
      WHEN scn.operation_branch_name = wb.pickup_branch_name AND wb.waybill_status = '03' THEN 'Last Mile'
      WHEN scn.operation_branch_name = wb.pickup_branch_name AND wb.waybill_status = '05' THEN 'First Mile'
      WHEN scn.operation_branch_name = wb.pickup_branch_name AND opt.option_name IN ('Arrival scan','Unloading scan','Unpacking scan','Delivery scan','Create Return Bill','Delivery Task Accept','Collection') THEN 'Last Mile'
      ELSE 'Not Defined'
    END AS process_level
  FROM `datawarehouse.shipping_line` scn
  LEFT JOIN `datawarehouse.shipping_waybill_dm` wb
    ON wb.waybill_no = scn.waybill_no
    AND DATE(wb.update_time, 'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 14 DAY)
  LEFT JOIN `datawarehouse.system_option` opt
    ON scn.operation_type = opt.option_value AND opt.type_option = 'operationType'
  LEFT JOIN `datawarehouse.res_branch` br
    ON br.branch_name = scn.operation_branch_name
  LEFT JOIN `datawarehouse.res_area` ar
    ON ar.name = wb.sender_district_name
  LEFT JOIN `datawarehouse.res_problem_package` prb
    ON prb.code = scn.problem_code
  WHERE DATE(scn.record_time, 'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 1 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY scn.waybill_no ORDER BY scn.record_time DESC) = 1
),

arrival_status AS (
  SELECT
    waybill_number,
    operation_branch_name,
    next_branch_name,
    IF(process_level = 'First Mile', 1, 0) AS is_first_mile_arrived,
    IF(process_level = 'Medium Mile' AND scan_branch_code = mh_origin_code, 1, 0) AS is_mh_origin_arrived,
    IF(process_level = 'Medium Mile' AND (scan_branch_code != mh_origin_code OR scan_branch_code != mh_destination_code), 1, 0) AS is_mh_transit,
    IF(process_level = 'Medium Mile' AND scan_branch_code = mh_destination_code, 1, 0) AS is_mh_destination_arrived,
    IF(process_level = 'Last Mile', 1, 0) AS is_last_mile_arrived,
    IF(process_level = 'Not Defined', 1, 0) AS is_arrival_unknown
  FROM latest_scan
  WHERE waybill_status NOT IN ('04','06')
    AND return_flag = '0'
)

-- FINAL SELECT
SELECT
  od.*,
  ls.last_scan_time,
  ls.scan_operation_type,
  ls.operation_branch_name AS last_scan_branch_name,
  ls.next_branch_name,
  ls.problem_reason,
  ls.process_level,
  arv.is_first_mile_arrived,
  arv.is_mh_origin_arrived,
  arv.is_mh_transit,
  arv.is_mh_destination_arrived,
  arv.is_last_mile_arrived,
  arv.is_arrival_unknown
FROM order_data od
LEFT JOIN latest_scan ls
  ON od.waybill_number = ls.waybill_number
LEFT JOIN arrival_status arv
  ON od.waybill_number = arv.waybill_number
