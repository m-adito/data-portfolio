WITH periodic AS (SELECT
                    pod_record_time,
                    return_pod_record_time,
                    calculated_date,
                    waybill_source,
                    parent_source,
                    -- source_category,
                    vip_customer_name,
                    payment_type,
                    SUM(count_transaction) AS count_transaction,
                    -- SUM(cod_amount) AS cod_amount,
                    -- SUM(total_fee) AS total_fee,
                    -- SUM(cod_transaction) AS cod_transaction,
                    -- SUM(withdrawal_from_ids) AS withdrawal_from_ids,
                    -- SUM(disbursement_to_seller) AS disbursement_to_seller,
                    -- SUM(outstanding_seller) AS outstanding_seller,
                    SUM(net_shipping_fee) AS net_shipping_fee,
                    -- SUM(cod_fee_count_transaction) AS cod_fee_count_transaction,
                    -- SUM(amount_cod_fee) AS amount_cod_fee,
                    -- SUM(non_cod_amount) AS non_cod_amount,
                    -- SUM(non_cod_transaction) AS non_cod_transaction

                FROM `datamart_idexp.dashboard_finance_disbursement`
                GROUP BY 1,2,3,4,5,6,7)

                , cash_wallet AS (
                    SELECT
                    DATE(ww.pod_record_time,'Asia/Jakarta') AS pod_record_time,
                    DATE(ww.return_pod_record_time,'Asia/Jakarta') AS return_pod_record_time,
                    CASE WHEN ww.pod_record_time IS NOT NULL THEN DATETIME(DATE(ww.pod_record_time,'Asia/Jakarta'), '00:00:00')
                            WHEN ww.return_pod_record_time IS NOT NULL THEN DATETIME(DATE(ww.return_pod_record_time,'Asia/Jakarta'), '00:00:00')
                            END AS calculated_date,
                    t1.option_name AS waybill_source,
                    CASE WHEN ww.express_type = '06' AND s1.parent_source <> 'E-Commerce' THEN 'Cargo'
                        WHEN s2.parent_source IS NOT NULL THEN s2.parent_source
                        ELSE s1.parent_source END AS parent_source,
                    ww.vip_customer_name,
                    CASE WHEN ww.payment_type = '00' THEN 'Cash'
                            WHEN ww.invoice_amount > 0 THEN 'Wallet'
                            END AS payment_type,
                    COUNT(ww.waybill_no) AS count_transaction,
                    SUM(ww.total_shipping_fee) AS net_shipping_fee,
                    FROM `datawarehouse_idexp.dm_waybill_waybill` ww
                    LEFT JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = ww.waybill_source AND t1.type_option = 'waybillSource'
                    LEFT JOIN `datamart_idexp.masterdata_mapping_source_finance_v2` s1 ON t1.option_name = s1.waybill_source AND s1.type_option = 'waybillSource'
                    LEFT JOIN `datamart_idexp.masterdata_mapping_source_finance_v2` s2 ON s2.parent_shipping_cleint = ww.parent_shipping_cleint AND s2.type_option = 'vipSeller' AND t1.option_name = 'VIP Customer Portal'
                    WHERE DATE(ww.update_time,'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'),INTERVAL 62 DAY)
                    AND (DATE(ww.pod_record_time,'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'),INTERVAL 62 DAY) -- backfill
                        OR DATE(ww.return_pod_record_time,'Asia/Jakarta') >= DATE_SUB(CURRENT_DATE('Asia/Jakarta'),INTERVAL 62 DAY))
                    AND (ww.payment_type = '00' OR ww.invoice_amount > 0)
                    GROUP BY 1,2,3,4,5,6,7
                )

                SELECT * FROM periodic UNION ALL
                SELECT * FROM cash_wallet