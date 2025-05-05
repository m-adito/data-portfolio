-- CREATE TABLE `datamart_idexp.finance_oms_data_aggregate` AS

                SELECT
                DATE(pod_record_time,'Asia/Jakarta') AS pod_date,
                t1.option_name AS waybill_source,
                IF(cod_amount > 0, 'COD', 'Non COD') AS cod_flag,
                COUNT(waybill_no) AS waybill,
                SUM(standard_shipping_fee) AS standard_shipping_fee,
                SUM(total_shipping_fee) AS total_shipping_fee,
                SUM(cod_amount) AS cod_amount

                FROM `datawarehouse_idexp.dm_waybill_waybill`
                LEFT JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = waybill_source AND t1.type_option = 'waybillSource'
                WHERE DATE(update_time,'Asia/Jakarta') >= DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -62 DAY)
                AND DATE(pod_record_time,'Asia/Jakarta') >= DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -62 DAY)
                GROUP BY 1,2,3
                -- ORDER BY 1 DESC