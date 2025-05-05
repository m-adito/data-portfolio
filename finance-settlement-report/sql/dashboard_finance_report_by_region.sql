WITH net as(
                    SELECT waybill_no,
                            standard_shipping_fee,
                            cod_amount,
                            net_shipping_fee,
                            return_shipping_fee,
                            -- IF(cod_amount>0, pre_tax_cod_fee + vat_cod_fee,0) as cod_fee,
                            net_cod_fee,
                            handling_fee,
                            other_fee,
                    FROM `datawarehouse_idexp.net_off_report_daily`
                    ),
                    waybill as(
                    SELECT ww.waybill_no,
                        s1.waybill_source AS waybill_source, --source
                        ww.vip_customer_name as vip_username, --seller
                        IF(s2.finance_source_category IS NOT NULL,s2.finance_source_category, s1.finance_source_category) AS source_category,
                        CASE WHEN ww.express_type = '06' AND s1.parent_source <> 'E-Commerce' THEN 'Cargo'
                        WHEN s2.parent_source IS NOT NULL THEN s2.parent_source
                        ELSE s1.parent_source END AS parent_source,
                        IF(ww.sender_province_name IS NOT NULL, 'INDONESIA','INDONESIA') AS country,
                        ww.sender_province_name as origin_province,
                        ww.sender_city_name as origin_city,
                        ww.pickup_branch_name as origin_branch,
                        DATE(ww.shipping_time,'Asia/Jakarta') AS shipping_time,
                        DATE(ww.pod_record_time,'Asia/Jakarta') AS pod_record_time,
                        DATE(ww.return_pod_record_time,'Asia/Jakarta') AS return_pod_record_time,
                    FROM `datawarehouse_idexp.waybill_waybill` ww
                    LEFT JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = ww.waybill_source AND t1.type_option = 'waybillSource'
                    LEFT JOIN `datamart_idexp.masterdata_mapping_source_finance_v2` s1 ON t1.option_name = s1.waybill_source AND s1.type_option = 'waybillSource'
                    LEFT JOIN `datamart_idexp.masterdata_mapping_source_finance_v2` s2 ON s2.parent_shipping_cleint = ww.parent_shipping_cleint AND s2.type_option = 'vipSeller' AND t1.option_name = 'VIP Customer Portal'
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
                    )
                    SELECT IF(t2.pod_record_time IS NOT NULL,t2.pod_record_time,t2.return_pod_record_time) AS pod_or_return_date,
                        t1.waybill_no,

                        t2.waybill_source,
                        t2.parent_source,
                        t2.source_category,
                        t2.vip_username,
                        t2.country,
                        t2.origin_province,
                        t2.origin_city,
                        t2.origin_branch,

                        t1.standard_shipping_fee,
                        t1.cod_amount,
                        t1.net_shipping_fee,
                        t1.return_shipping_fee,
                        t1.net_cod_fee,
                        t1.handling_fee,
                        t1.other_fee,

                    FROM net t1
                    LEFT OUTER JOIN waybill t2 ON t2.waybill_no = t1.waybill_no
                    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
                    ORDER BY 1 ASC