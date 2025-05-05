-- MERGE ke final_table
MERGE `project.dataset.final_table` T
USING (

    -- Main query dengan interval -1 DAY
    WITH src_data AS (
        SELECT * FROM (
            SELECT DISTINCT 
                a.tracking_id, 
                a.partner_name, 
                a.loc_code, 
                a.loc_address, 
                b.partner_type, 
                a.created_time,
                c.province, 
                a.loc_name,
                c.city,
                c.district
            FROM `project.dataset.source_table` a
            LEFT JOIN `project.dataset.location_ref` b ON b.code = a.loc_code
            LEFT JOIN `project.dataset.region_ref` c ON c.district_id = b.region_id
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tracking_id ORDER BY created_time DESC)=1
    )

    , dropoff_evt AS (
        SELECT
            x.tracking_id, 
            x.source_hub, 
            DATETIME(x.event_time,'Asia/Jakarta') AS event_time
        FROM `project.dataset.dropoff_table` x
        WHERE DATE(x.event_time,'Asia/Jakarta') = DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 1 DAY)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY x.tracking_id ORDER BY x.event_time ASC)=1
    )

    , arrival_evt AS (
        SELECT
            x.tracking_id, 
            x.source_hub, 
            DATETIME(x.event_time,'Asia/Jakarta') AS event_time
        FROM `project.dataset.event_table` x
        WHERE DATE(x.event_time,'Asia/Jakarta') = DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 1 DAY)
            AND SUBSTR(x.source_hub,1,2) IN ('MH','DC')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY x.tracking_id ORDER BY x.event_time ASC)=1
    )

    , summary_evt AS (
        SELECT tracking_id, evt_type, source_hub, next_hub AS next_location, event_time
        FROM `project.dataset.event_table`
        WHERE DATE(event_time, 'Asia/Jakarta') = DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 1 DAY)
            AND evt_type IN ('61', '02', '04', '09')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY tracking_id
            ORDER BY CASE
                        WHEN evt_type = '61' THEN 0
                        ELSE 1
                    END,
                    event_time ASC
        ) = 1
    )

    , main_view AS (
        SELECT
            a.tracking_id,
            b.order_id,
            b.ext_order_id,
            DATETIME(b.order_time, 'Asia/Jakarta') AS order_time,
            b.sender_name,
            b.sender_contact,
            o1.val AS channel,
            o2.val AS service_type,
            a.province AS sender_province,
            a.city AS sender_city,
            a.district AS sender_district,
            DATETIME(a.created_time, 'Asia/Jakarta') AS event_time,
            a.loc_name AS source_hub,
            a.loc_address,
            a.loc_code,
            a.partner_type,
            CASE 
                WHEN b.handled_by = 'THIRDPARTY' THEN b.partner_handler
                ELSE 'DEFAULT'
            END AS final_partner,
            DATETIME(b.pickup_start, 'Asia/Jakarta') AS pickup_start,
            DATETIME(b.pickup_end, 'Asia/Jakarta') AS pickup_end,
            IF(c.cancel_flag = '1', 'Canceled', o3.val) AS order_status,
            DATETIME(c.pickup_logged, 'Asia/Jakarta') AS pickup_event_time,
            IF(b.pickup_time IS NULL, b.target_hub, b.pickup_hub) AS pickup_target,
            IF(c.pickup_logged IS NULL AND b.status <> '04', DATE_DIFF(CURRENT_DATE('Asia/Jakarta'),DATE(a.created_time, 'Asia/Jakarta'),DAY), NULL) AS backlog_days,
            CASE
                WHEN IF(b.handled_by = 'THIRDPARTY', b.partner_handler, a.partner_name) != 'DEFAULT' AND s.evt_type = '61' THEN DATETIME(s.event_time,'Asia/Jakarta')
                WHEN IF(b.handled_by = 'THIRDPARTY', b.partner_handler, a.partner_name) = 'DEFAULT' AND s.evt_type = '02' AND s.source_hub = b.pickup_hub THEN DATETIME(s.event_time,'Asia/Jakarta')
                WHEN IF(b.handled_by = 'THIRDPARTY', b.partner_handler, a.partner_name) = 'DEFAULT' AND s.evt_type = '04' AND s.source_hub = b.pickup_hub THEN DATETIME(s.event_time,'Asia/Jakarta')
                WHEN c.pod_time IS NOT NULL THEN DATETIME(c.pod_time,'Asia/Jakarta')
            END AS delivery_time,
            CASE
                WHEN IF(b.handled_by = 'THIRDPARTY', b.partner_handler, a.partner_name) != 'DEFAULT' AND s.evt_type = '61' THEN 'Handover Complete'
                WHEN IF(b.handled_by = 'THIRDPARTY', b.partner_handler, a.partner_name) = 'DEFAULT' AND s.evt_type = '04' THEN 'Dispatched'
                WHEN c.pod_time IS NOT NULL THEN 'Delivered'
            END AS delivery_status,
            DATETIME(c.pod_time, 'Asia/Jakarta') AS pod_time,
            DATETIME(b.failed_pickup_time) AS failed_pickup_time,
            o4.reason AS failed_reason,
            o5.val AS cancel_status,
            CURRENT_DATETIME('Asia/Jakarta') AS data_timestamp,
            d.event_time AS arrival_time,
            e.event_time AS dropoff_time
        FROM src_data a
        LEFT JOIN `project.dataset.orders` b ON b.tracking_id = a.tracking_id
            AND DATE(b.updated_at, 'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 1 DAY)
        LEFT JOIN summary_evt s ON s.tracking_id = a.tracking_id
        LEFT JOIN `project.dataset.tracking` c ON c.tracking_id = a.tracking_id
            AND DATE(c.updated_at, 'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 1 DAY)
        LEFT JOIN `project.dataset.options` o1 ON o1.option_id = b.channel_id
        LEFT JOIN `project.dataset.options` o2 ON o2.option_id = b.service_id
        LEFT JOIN `project.dataset.options` o3 ON o3.option_id = b.status_id
        LEFT JOIN `project.dataset.failures` o4 ON o4.id = b.failed_reason_id
        LEFT JOIN `project.dataset.options` o5 ON o5.option_id = c.cancel_flag
        LEFT JOIN arrival_evt d ON d.tracking_id = a.tracking_id
        LEFT JOIN dropoff_evt e ON e.tracking_id = a.tracking_id
        WHERE a.partner_type IN ('A', 'B', 'C')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY a.tracking_id ORDER BY a.created_time, delivery_time ASC)=1
    )

    SELECT * FROM main_view

) S

ON T.tracking_id = S.tracking_id

WHEN MATCHED THEN
    UPDATE SET
        order_id = S.order_id,
        ext_order_id = S.ext_order_id,
        order_time = S.order_time,
        sender_name = S.sender_name,
        sender_contact = S.sender_contact,
        channel = S.channel,
        service_type = S.service_type,
        sender_province = S.sender_province,
        sender_city = S.sender_city,
        sender_district = S.sender_district,
        event_time = S.event_time,
        source_hub = S.source_hub,
        loc_address = S.loc_address,
        loc_code = S.loc_code,
        partner_type = S.partner_type,
        final_partner = S.final_partner,
        pickup_start = S.pickup_start,
        pickup_end = S.pickup_end,
        order_status = S.order_status,
        pickup_event_time = S.pickup_event_time,
        pickup_target = S.pickup_target,
        backlog_days = S.backlog_days,
        delivery_time = S.delivery_time,
        delivery_status = S.delivery_status,
        pod_time = S.pod_time,
        failed_pickup_time = S.failed_pickup_time,
        failed_reason = S.failed_reason,
        cancel_status = S.cancel_status,
        data_timestamp = S.data_timestamp,
        arrival_time = S.arrival_time,
        dropoff_time = S.dropoff_time

WHEN NOT MATCHED THEN
    INSERT (
        tracking_id,
        order_id,
        ext_order_id,
        order_time,
        sender_name,
        sender_contact,
        channel,
        service_type,
        sender_province,
        sender_city,
        sender_district,
        event_time,
        source_hub,
        loc_address,
        loc_code,
        partner_type,
        final_partner,
        pickup_start,
        pickup_end,
        order_status,
        pickup_event_time,
        pickup_target,
        backlog_days,
        delivery_time,
        delivery_status,
        pod_time,
        failed_pickup_time,
        failed_reason,
        cancel_status,
        data_timestamp,
        arrival_time,
        dropoff_time
    )
    VALUES (
        tracking_id,
        order_id,
        ext_order_id,
        order_time,
        sender_name,
        sender_contact,
        channel,
        service_type,
        sender_province,
        sender_city,
        sender_district,
        event_time,
        source_hub,
        loc_address,
        loc_code,
        partner_type,
        final_partner,
        pickup_start,
        pickup_end,
        order_status,
        pickup_event_time,
        pickup_target,
        backlog_days,
        delivery_time,
        delivery_status,
        pod_time,
        failed_pickup_time,
        failed_reason,
        cancel_status,
        data_timestamp,
        arrival_time,
        dropoff_time
    );
