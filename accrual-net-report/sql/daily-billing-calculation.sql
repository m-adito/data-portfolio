INSERT INTO `my_project.my_datamart.fact_pod_details`

WITH
    standard_discount_rates AS (
        SELECT DISTINCT
            sender_location_id,
            recipient_location_id,
            express_type,
            shipping_client_id,
            discount_rate
        FROM
            `my_project.my_datawarehouse.dim_standard_rates`
        WHERE
            DATE(end_expire_time, 'Asia/Jakarta') > CURRENT_DATE('Asia/Jakarta')
            AND deleted = '0' QUALIFY ROW_NUMBER() OVER (PARTITION BY search_code ORDER BY created_at DESC)=1
    ),

    wallet_payments AS (
        SELECT
            waybill_no,
            SUM(total_shipping_fee) AS total_shipping_fee,
            SUM(discount_amount) AS discount_amount,
            SUM(net_shipping_fee) AS net_shipping_fee
        FROM (SELECT * FROM `my_datamart.agg_wallet_summary` wallet)
        GROUP BY 1
    ),

    return_shipments AS (
        SELECT DISTINCT
            rr.return_waybill_no,
            rr.waybill_no,
            rr.return_shipping_fee
        FROM `my_datawarehouse.fact_returns` rr
        LEFT JOIN `my_datawarehouse.dim_system_codes` s0 ON rr.order_source = s0.option_value AND s0.type_option = 'waybillSource'
        LEFT JOIN `my_datamart.dim_source_finance_mapping` s1 ON s0.option_name = s1.waybill_source AND s1.type_option = 'waybillSource'
        LEFT JOIN `my_datamart.dim_source_finance_mapping` s2 ON s2.parent_shipping_cleint = rr.parent_client_name AND s2.type_option = 'vipSeller' AND rr.order_source = '00'
        WHERE
            (s1.finance_source_category != 'E-Commerce' OR s2.finance_source_category != 'E-Commerce') -- Example: Excluding specific categories
            AND DATE(rr.return_record_time,'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'),INTERVAL 24 MONTH)
    ),

    shipment_cost_details AS (
        SELECT DISTINCT
            ww.waybill_no,
            IF(ww.pod_record_time IS NULL, DATETIME(DATE(ww.return_pod_record_time,'Asia/Jakarta'), '00:00:00'), DATETIME(DATE(ww.pod_record_time,'Asia/Jakarta'), '00:00:00')) AS pod_date,
            t1.option_name AS waybill_source_name, -- Renamed alias slightly for clarity
            api.partner_company_name AS company_name, -- Generalized column name
            ww.key_account_name, -- Generalized column name
            ww.parent_client_identifier, -- Generalized column name
            IF(ww.pod_record_time IS NULL, 'pod return', 'pod delivery') AS pod_category,
            IF(ww.cod_amount = 0, 'Non COD', 'COD') AS cod_flag,
            t2.option_name AS express_type_name, -- Renamed alias slightly for clarity
            IF(t2.option_name = 'iDtruck', CAST(ww.item_calculated_weight AS NUMERIC), NULL) AS cargo_weight, -- Kept specific type name as example
            ww.total_shipping_fee AS original_total_shipping_fee,
            CASE
                WHEN ww.waybill_source = '101' THEN (spx.pricing * spx_w.Multiplier) -- Partner A Express pricing
                WHEN ww.waybill_source = '114' THEN (cb.pricing * cb_w.Multiplier) -- Partner A Crossborder pricing
                WHEN wallet.waybill_no IS NOT NULL THEN wallet.total_shipping_fee -- Wallet payment override
                ELSE (ww.total_shipping_fee) -- Default original fee
            END AS total_shipping_fee,
            CASE
                WHEN ww.waybill_source = '104' THEN ww.total_shipping_fee - (ww.standard_shipping_fee * (IF(CAST(ww.item_actual_weight AS NUMERIC) < 0.51 AND sp.IDlite_availability = 'Available', sp.IDlite, sp.STD))) -- Partner A Platform discount logic
                WHEN ww.waybill_source = '114' THEN ((cb.pricing * cb_w.Multiplier) -((cb.pricing * cb_w.Multiplier) * cb.discount_rate)) -- Partner A Crossborder non-COD net fee logic
                WHEN ww.waybill_source = '101' THEN ((spx.pricing * spx_w.Multiplier) -((spx.pricing * spx_w.Multiplier) * spx.discount)) + (((spx.pricing * spx_w.Multiplier) -((spx.pricing * spx_w.Multiplier) * spx.discount)) * 0.011) -- Partner A Express net fee logic (with tax)
                -- new price with Partner B for origin outside KR (v1 April 2025)
                WHEN ww.waybill_source = '756' AND DATE(ww.shipping_time,'Asia/Jakarta') >= '2025-04-01' AND tts1.status = 'Origin Non KR' THEN (kr_w.Multiplier * tts1.e2e_tts)
                -- Partner B Origin KR Tier M1/M2 Full Service
                WHEN ww.waybill_source = '756' AND tts1.status = 'Origin KR' AND DATE(ww.shipping_time,'Asia/Jakarta') >= '2025-04-01' AND tts1.type_m IN ('M1', 'M2') AND tokorder.role_miles = 'fm,mm,lm' THEN (kr_w.Multiplier * tts1.e2e)
                -- Partner B Origin KR Tier M3 Whitelisted Full Service
                WHEN ww.waybill_source = '756' AND tts1.status = 'Origin KR' AND DATE(ww.shipping_time,'Asia/Jakarta') >= '2025-04-01' AND tts1.type_m = 'M3' AND tokorder.role_miles = 'fm,mm,lm' AND tokorder.is_kr_whitelist = 1 THEN (kr_w.Multiplier * tts1.e2e)
                -- Partner B Origin KR Tier M3 Non-Whitelisted Full Service (Complex calculation)
                WHEN ww.waybill_source = '756' AND tts1.status = 'Origin KR' AND DATE(ww.shipping_time,'Asia/Jakarta') >= '2025-04-01' AND tts1.type_m = 'M3' AND tokorder.role_miles = 'fm,mm,lm' THEN (kr_w.Multiplier * tts1.fm) + (CAST(ww.item_actual_weight AS FLOAT64) * (tts1.e2e - tts1.fm - tts1.lm)) + (kr_w.Multiplier * tts1.lm)
                -- Partner B Origin KR Tier M1/M2/M3 First Mile Pickup Only
                WHEN ww.waybill_source = '756' AND tts1.status = 'Origin KR' AND DATE(ww.shipping_time,'Asia/Jakarta') >= '2025-04-01' AND tts1.type_m IN ('M1', 'M2', 'M3') AND tokorder.role_miles = 'fm' THEN (kr_w.Multiplier * tts1.fm)
                -- Partner B Origin KR Tier M1/M2 Mid/Last Mile Only
                WHEN ww.waybill_source = '756' AND tts1.status = 'Origin KR' AND DATE(ww.shipping_time,'Asia/Jakarta') >= '2025-04-01' AND tts1.type_m IN ('M1', 'M2') AND tokorder.role_miles = 'mm,lm' THEN (kr_w.Multiplier * tts1.mm ) + (kr_w.Multiplier * tts1.lm )
                 -- Partner B Origin KR Tier M3 Mid/Last Mile Only
                WHEN ww.waybill_source = '756' AND tts1.status = 'Origin KR' AND DATE(ww.shipping_time,'Asia/Jakarta') >= '2025-04-01' AND tts1.type_m = 'M3' AND tokorder.role_miles = 'mm,lm' THEN (CAST(ww.item_actual_weight AS FLOAT64) * (tts1.mm)) + (kr_w.Multiplier * tts1.lm)
                WHEN ww.key_account_name = 'PartnerB_SpecialService' AND sw._gross_ IS NOT NULL THEN ww.receivable_shipping_fee - (ww.receivable_shipping_fee * ((sw._gross_ - sw._nett_) / sw._gross_ )) -- Anonymized specific customer
                WHEN ww.key_account_name = 'PartnerB_SpecialService' AND sw._gross_ IS NULL THEN ww.total_shipping_fee -- Anonymized specific customer
                WHEN ww.waybill_source IN ('116', '801') THEN ww.total_shipping_fee - (ww.standard_shipping_fee * bb.discount) -- Partner C discount logic
                WHEN ww.waybill_source IN ('803','804') THEN ww.total_shipping_fee - (ww.total_shipping_fee * ((zal.gross - zal.nett) / zal.gross)) -- Partner D discount logic
                WHEN t4.waybill_no IS NOT NULL THEN t4.after_discount_shipping_fee -- VIP Customer Portal (netoff) calculation
                WHEN wallet.waybill_no IS NOT NULL THEN wallet.net_shipping_fee -- Wallet payment net fee override
                WHEN price.shipping_client_id IS NOT NULL THEN (ww.total_shipping_fee) - (ww.standard_shipping_fee * (IFNULL(0,price.discount_rate)/100)) -- Standard system pricing discount
                WHEN price.shipping_client_id IS NOT NULL AND price.discount_rate IS NULL THEN ww.total_shipping_fee -- Standard system pricing, no discount defined
                WHEN api.order_source IS NOT NULL AND t2.option_name = 'STD' THEN (ww.total_shipping_fee - (ww.standard_shipping_fee * api.STD)) -- Platform API discount (STD)
                WHEN api.order_source IS NOT NULL AND t2.option_name = 'IDlite' THEN (ww.total_shipping_fee - (ww.standard_shipping_fee * api.iDlite)) -- Platform API discount (IDlite)
                WHEN api.order_source IS NOT NULL AND t2.option_name = 'SMD' THEN (ww.total_shipping_fee - (ww.standard_shipping_fee * api.SMD)) -- Platform API discount (SMD)
                WHEN api.order_source IS NOT NULL AND t2.option_name = 'iDtruck' THEN (ww.total_shipping_fee - (ww.standard_shipping_fee * api.iDtruck)) -- Platform API discount (iDtruck)
                ELSE (ww.total_shipping_fee) -- Default case, no discount applied
            END AS net_shipping_fee,
            CASE
                WHEN ww.pod_record_time IS NULL THEN 0 -- No COD fee if not PODed (delivery)
                WHEN ww.waybill_source = '104' AND ww.cod_amount > 0 THEN (ww.cod_amount * 0.01) + ((ww.cod_amount * 0.01) * 0.11) -- Partner A Platform COD fee (with tax)
                WHEN ww.waybill_source = '101' AND ww.cod_amount > 0 THEN (ww.cod_amount * 0.01) + ((ww.cod_amount * 0.01) * 0.11) -- Partner A Express COD fee (with tax)
                WHEN ww.waybill_source = '114' AND ww.cod_amount > 0 AND ww.sender_city_name = 'SIDOARJO' THEN ww.cod_amount * 0.01 -- Partner A Crossborder COD fee (specific origin)
                WHEN t4.waybill_no IS NOT NULL AND ww.cod_amount > 0 THEN t4.cod_fee + t4.vat_cod_fee -- VIP Customer Portal COD fee
                WHEN api.order_source IS NOT NULL AND t1.option_name NOT IN ('partner_c_legal_entity','partner_d_legal_entity_1','partner_d_legal_entity_2') AND ww.cod_amount > 0 THEN (ww.cod_amount * api.cod_fee_percentage) + ((ww.cod_amount * api.cod_fee_percentage) * 0.11) -- Platform API COD fee (with tax), excluding specific partners
                ELSE 0 -- Default case, no COD fee
            END AS cod_fee,
            CAST(CASE
                WHEN t4.waybill_no IS NOT NULL THEN t4.after_discount_return_shipping_fee -- VIP Customer Portal return fee
                WHEN api.order_source IS NOT NULL THEN rr.return_shipping_fee * api.return_fee_percentage -- Platform API return fee
                ELSE 0 -- Default case, no return fee applied here
            END AS NUMERIC) AS return_shipping_fee,
            ww.cod_amount,
            CAST(ww.item_calculated_weight as NUMERIC) AS actual_weight,
            CASE
                WHEN CAST(ww.item_calculated_weight AS NUMERIC) <= 1.309 THEN 1 -- Weight rounding rule 1
                ELSE CAST(CEIL(CAST(ww.item_calculated_weight AS NUMERIC) - 0.31) AS NUMERIC) -- Weight rounding rule 2
            END AS chargeable_weight,
            ww.sender_name
        FROM
            `my_datawarehouse.fact_waybill_details` ww
        LEFT JOIN `my_datawarehouse.fact_orders` oo ON ww.waybill_no = oo.waybill_no AND DATE(oo.input_time,'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'),INTERVAL 24 MONTH) AND oo.order_no NOT IN ('EXAMPLE_ORDER_EXCLUSION') -- Anonymized exclusion
        LEFT JOIN return_shipments rr ON ww.waybill_no = rr.waybill_no AND ww.return_waybill_no = rr.return_waybill_no
        LEFT JOIN `my_datawarehouse.dim_system_codes` t1 ON t1.option_value = ww.waybill_source AND t1.type_option = 'waybillSource'
        LEFT JOIN `my_datawarehouse.dim_system_codes` t2 ON t2.option_value = ww.express_type AND t2.type_option = 'expressType'
        LEFT JOIN `my_datamart.dim_partnerA_platform_discounts` sp ON sp.Lookup = CONCAT(ww.recipient_city_name,ww.recipient_district_name) AND sp.Origin = ww.sender_city_name AND (ww.waybill_source = '104' OR (ww.waybill_source = '114' AND ww.cod_amount > 0 AND ww.sender_city_name = 'SIDOARJO'))
        LEFT JOIN `my_datamart.dim_partnerA_express_discounts` spx ON spx.kecamatan = ww.recipient_district_name AND spx.kota_kabupaten = ww.recipient_city_name AND spx.origin = ww.sender_city_name AND spx.express_type = t2.option_name AND ww.waybill_source = '101'
        LEFT JOIN `my_datamart.dim_partnerA_express_weight_tiers` spx_w ON CAST(ww.item_actual_weight AS NUMERIC) >= spx_w.Bottom AND CAST(ww.item_actual_weight AS NUMERIC) <= spx_w.Up AND ww.waybill_source = '101'
        LEFT JOIN `my_datamart.dim_partnerA_crossborder_discounts` cb ON cb.lookup_destination = CONCAT(ww.recipient_city_name,ww.recipient_district_name) AND cb.origin = ww.sender_city_name AND ww.waybill_source = '114'
        LEFT JOIN `my_datamart.dim_partnerA_crossborder_weight_tiers` cb_w ON CAST(ww.item_actual_weight AS NUMERIC) >= cb_w.Bottom AND CAST(ww.item_actual_weight AS NUMERIC) <= cb_w.Up AND ww.waybill_source = '114'
        LEFT JOIN `my_datawarehouse.fact_partnerB_orders` tokorder ON tokorder.waybill_number = ww.waybill_no AND ww.waybill_source = '756' AND DATE(tokorder.create_time,'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 6 MONTH)
        LEFT JOIN `my_datamart.dim_partnerB_weight_tiers` kr_w ON CAST(ww.item_actual_weight AS NUMERIC) >= kr_w.Bottom AND CAST(ww.item_actual_weight AS NUMERIC) <= kr_w.Up AND ww.waybill_source = '756'
        LEFT JOIN `my_datamart.dim_partnerB_new_pricing_scheme` tts1 ON oo.sender_city_name = tts1.Origin_City AND oo.recipient_city_name = tts1.Dest_City AND oo.order_source = '756'
        LEFT JOIN `my_datamart.dim_partnerB_special_discounts` sw ON sw.origin_city = ww.sender_city_name AND sw.dest_city = ww.recipient_city_name AND sw.dest_district = ww.recipient_district_name AND ww.key_account_name = 'PartnerB_SpecialService' -- Anonymized specific customer
        LEFT JOIN `my_datamart.dim_partnerC_coverage` bc ON bc.dest_city = ww.recipient_city_name AND bc.dest_district = ww.recipient_district_name AND ww.waybill_source IN ('116', '801')
        LEFT JOIN `my_datamart.dim_partnerC_discounts` bb ON bb.area = bc.master_agent AND bb.express_type = t2.option_name AND ww.waybill_source IN ('116', '801')
        LEFT JOIN `my_datamart.dim_partnerD_discounts` zal ON zal.dest_city = ww.recipient_city_name AND zal.dest_district = ww.recipient_district_name AND zal.origin_city = ww.sender_city_name AND zal.waybill_source = t1.option_name AND ww.waybill_source IN ('803', '804')
        LEFT JOIN standard_discount_rates price ON price.shipping_client_id = ww.vip_customer_id AND ww.sender_city_id = price.sender_location_id AND ww.recipient_district_id = price.recipient_location_id AND ww.express_type = price.express_type
        LEFT JOIN `my_datawarehouse.agg_daily_netoff_report` t4 ON t4.waybill_no = ww.waybill_no AND DATE(t4.created_at) >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 6 MONTH)
        LEFT JOIN `my_datamart.dim_api_platform_discounts` api ON api.option_value = IF(ww.waybill_source = '835', '829', ww.waybill_source) AND api.shipping_client IS NOT NULL AND ww.waybill_source NOT IN ('813') -- Excluding specific source
        LEFT JOIN wallet_payments wallet ON wallet.waybill_no = ww.waybill_no
        WHERE
            (DATE(ww.pod_record_time,'Asia/Jakarta') = DATE_SUB(CURRENT_DATE('Asia/Jakarta'),INTERVAL 1 DAY) OR DATE(ww.return_pod_record_time,'Asia/Jakarta') = DATE_SUB(CURRENT_DATE('Asia/Jakarta'),INTERVAL 1 DAY)) -- Logic for daily incremental load
            AND ww.pickup_branch_name NOT IN ('AGENT_LIAONING','AGENT_SHANDONG','TH TESTING HQ','TH_DALIAN','HEADQUARTER','TH_SHENYANG','TH_QINGDAO','PDB_AIRPORT','PDB_TECH','PDB_SHENYANG','PDB_QINGDAO','TH_DALIAN1','TH_QINGDAO1','PDB_SHENYANG1') -- Kept as examples of exclusion criteria
            AND ww.void_flag = '0'
            AND ww.deleted = '0'
    )

SELECT
    ww.waybill_no,
    ww.pod_date,
    ww.waybill_source_name,
    ww.company_name,
    ww.key_account_name,
    ww.parent_client_identifier AS parent_shipping_client, -- Renamed back for consistency if expected downstream
    ww.pod_category,
    ww.cod_flag,
    ww.express_type_name,
    ww.cargo_weight,
    ww.original_total_shipping_fee,
    ww.total_shipping_fee,
    IF(ww.net_shipping_fee IS NULL, sf.net_shipping_fee, ww.net_shipping_fee) AS net_shipping_fee, -- Handling potential nulls from join
    ww.cod_fee,
    IF(ww.return_shipping_fee IS NULL, sf.return_shipping_fee, ww.return_shipping_fee) AS return_shipping_fee, -- Handling potential nulls from join
    ww.cod_amount,
    ww.actual_weight,
    ww.chargeable_weight,
    ww.sender_name
FROM shipment_cost_details ww
LEFT JOIN `my_datawarehouse.fact_waybill_fees` sf ON sf.waybill_no = ww.waybill_no AND DATE(sf.create_time,'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'), INTERVAL 1 MONTH) -- Join for fallback fee info
