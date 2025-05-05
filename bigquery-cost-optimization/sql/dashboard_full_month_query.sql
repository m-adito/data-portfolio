-- CREATE TABLE `project.dataset.final_dashboard_table`
-- PARTITION BY DATE(event_time)
-- CLUSTER BY tracking_no, source_branch_name, target_branch_name
-- AS

WITH source_schedule AS (
    SELECT * FROM (
        SELECT DISTINCT 
            a.tracking_no, 
            a.partner_name, 
            a.location_code, 
            a.location_address, 
            b.partner_category, 
            a.created_at,
            c.region_province, 
            a.location_name,
            c.region_city,
            c.region_district
        FROM `project.dataset.schedule_source` a
        LEFT JOIN `project.dataset.location_master` b ON b.code = a.location_code
        LEFT JOIN `project.dataset.region_mapping` c ON c.district_id = b.region_id
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tracking_no ORDER BY created_at DESC)=1
)

, dropoff_events AS (
    SELECT
        x.tracking_no, 
        x.source_branch_name, 
        DATETIME(x.event_time,'Asia/Jakarta') AS event_time
    FROM `project.dataset.dropoff_events_table` x
    WHERE DATE(x.event_time,'Asia/Jakarta') >= DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -3 MONTH)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY x.tracking_no ORDER BY x.event_time ASC)=1
)

, arrival_events AS (
    SELECT
        x.tracking_no, 
        x.source_branch_name, 
        DATETIME(x.event_time,'Asia/Jakarta') AS event_time
    FROM `project.dataset.shipment_events_table` x
    WHERE DATE(x.event_time,'Asia/Jakarta') >= DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -3 MONTH)
        AND SUBSTR(x.source_branch_name,1,2) IN ('MH','DC')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY x.tracking_no ORDER BY x.event_time ASC)=1
)

, shipment_summary AS (
    SELECT tracking_no, event_type, source_branch_name, next_branch_name AS next_location_name, event_time
    FROM `project.dataset.shipment_events_table`
    WHERE DATE(event_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -3 MONTH))
        AND event_type IN ('61', '02', '04', '09')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tracking_no
        ORDER BY CASE
                    WHEN event_type = '61' THEN 0
                    ELSE 1
                END,
                event_time ASC
    ) = 1
)

, detail_view AS (
    SELECT
        a.tracking_no,
        b.order_id,
        b.external_order_id,
        DATETIME(b.ordered_at, 'Asia/Jakarta') AS ordered_at,
        b.sender_name,
        b.sender_phone,
        opt1.option_value AS order_channel,
        opt2.option_value AS service_category,
        a.region_province AS sender_province,
        a.region_city AS sender_city,
        a.region_district AS sender_district,
        DATETIME(a.created_at, 'Asia/Jakarta') AS event_time,
        a.location_name AS source_branch_name,
        a.location_address,
        a.location_code,
        a.partner_category,
        CASE 
            WHEN b.handover_partner = 'THIRDPARTY' THEN b.handover_partner_name
            ELSE 'DEFAULT'
        END AS final_partner_name,
        DATETIME(b.pickup_start, 'Asia/Jakarta') AS pickup_start,
        DATETIME(b.pickup_end, 'Asia/Jakarta') AS pickup_end,
        IF(c.cancel_flag = '1', 'Canceled', opt3.option_value) AS order_status,
        DATETIME(c.pickup_recorded_at, 'Asia/Jakarta') AS pickup_event_time,
        IF(b.pickup_time IS NULL, b.target_branch, b.pickup_branch) AS pickup_target_branch,
        IF(c.pickup_recorded_at IS NULL AND b.status <> '04', DATE_DIFF(CURRENT_DATE('Asia/Jakarta'),DATE(a.created_at, 'Asia/Jakarta'),DAY), NULL) AS aging_days,
        CASE
            WHEN IF(b.handover_partner = 'THIRDPARTY', b.handover_partner_name, a.partner_name) != 'DEFAULT' AND s.event_type = '61' THEN DATETIME(s.event_time,'Asia/Jakarta')
            WHEN IF(b.handover_partner = 'THIRDPARTY', b.handover_partner_name, a.partner_name) = 'DEFAULT' AND s.event_type = '02' AND s.source_branch_name = b.pickup_branch THEN DATETIME(s.event_time,'Asia/Jakarta')
            WHEN IF(b.handover_partner = 'THIRDPARTY', b.handover_partner_name, a.partner_name) = 'DEFAULT' AND s.event_type = '04' AND s.source_branch_name = b.pickup_branch THEN DATETIME(s.event_time,'Asia/Jakarta')
            WHEN c.pod_event_time IS NOT NULL THEN DATETIME(c.pod_event_time,'Asia/Jakarta')
        END AS delivery_event_time,
        CASE
            WHEN IF(b.handover_partner = 'THIRDPARTY', b.handover_partner_name, a.partner_name) != 'DEFAULT' AND s.event_type = '61' THEN 'Handover Completed'
            WHEN IF(b.handover_partner = 'THIRDPARTY', b.handover_partner_name, a.partner_name) = 'DEFAULT' AND s.event_type = '04' THEN 'Dispatched'
            WHEN c.pod_event_time IS NOT NULL THEN 'Delivered'
        END AS delivery_status,
        DATETIME(c.pod_event_time, 'Asia/Jakarta') AS pod_event_time,
        DATETIME(b.failed_pickup_time) AS failed_pickup_time,
        opt4.failure_reason AS failed_pickup_reason,
        opt5.option_value AS cancel_status,
        CURRENT_DATETIME('Asia/Jakarta') AS data_updated_at,
        d.event_time AS arrival_event_time,
        e.event_time AS dropoff_event_time
    FROM source_schedule a
    LEFT JOIN `project.dataset.orders_table` b ON b.tracking_no = a.tracking_no
        AND DATE(b.updated_at, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -6 MONTH))
    LEFT JOIN shipment_summary s ON s.tracking_no = a.tracking_no
    LEFT JOIN `project.dataset.tracking_table` c ON c.tracking_no = a.tracking_no
        AND DATE(c.updated_at, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -3 MONTH))
    LEFT JOIN `project.dataset.options_table` opt1 ON opt1.option_id = b.source_channel_id
    LEFT JOIN `project.dataset.options_table` opt2 ON opt2.option_id = b.service_type_id
    LEFT JOIN `project.dataset.options_table` opt3 ON opt3.option_id = b.order_status
    LEFT JOIN `project.dataset.failure_reasons` opt4 ON opt4.reason_id = b.failed_pickup_reason_id
    LEFT JOIN `project.dataset.options_table` opt5 ON opt5.option_id = c.cancel_flag
    LEFT JOIN arrival_events d ON d.tracking_no = a.tracking_no
    LEFT JOIN dropoff_events e ON e.tracking_no = a.tracking_no
)

SELECT * FROM detail_view
WHERE partner_category IN ('SAT', 'SIL', 'MUI')
QUALIFY ROW_NUMBER() OVER (PARTITION BY tracking_no ORDER BY event_time, delivery_event_time ASC)=1
