
-- CREATE TABLE `grand-sweep-324604.datamart_idexp.dashboard_va_monitoring`
-- PARTITION BY DATE(Tanggal_Pembayaran)
-- CLUSTER BY waybill_no, No_VA, Nama_TH
-- AS

WITH correction AS (
                        SELECT
                        correction_date,
                        branch_id,
                        SUM(correction_amount) AS correction_amount,
                        STRING_AGG(note) AS note,
                        -- STRING_AGG(correction_date) AS correction_date
                        FROM `datawarehouse_idexp.branch_virtual_account_correction`

                        GROUP BY 1,2
                        )
                , bank_code AS (
                        SELECT account_number, bank_code, status_message, created_at
                        FROM `datawarehouse_idexp.virtual_account_log`
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY account_number ORDER BY created_at DESC)=1

                        )

                , va_detail AS (SELECT
                        va.id,
                        va.account_number AS No_VA,
                        b2.bank_name AS Nama_Bank,
                        IF(va.agent_branch_name = '', t3.branch_name, va.agent_branch_name) AS Nama_Mitra,
                        IF(va.branch_name = '', t1.branch_name, va.branch_name) AS Nama_TH,
                        IF(t2.wilayah IS NULL, t4.wilayah, t2.wilayah) AS wilayah,
                        IF(t2.dm IS NULL, t4.dm, t2.dm) AS dm,
                        t1.branch_no AS Kode_TH,
                        va.create_va_date AS Tanggal_Tagihan_VA,
                        va.payment_date AS Tanggal_Pembayaran,
                        -- month,
                        CASE WHEN va.payment_status = '0' THEN 'Not Paid'
                            WHEN va.payment_status = '1' THEN 'Paid'
                            WHEN va.payment_status = '2' THEN 'Expired'
                            ELSE va.payment_status
                            END AS Status_Transaksi,
                        -- va.cod_amount + va.cash_settlement_amount AS Total_Nominal,
                          IF(va.cod_amount IS NULL,0,va.cod_amount) AS va_cod_amount,
                          IF(va.cash_settlement_amount IS NULL,0,va.cash_settlement_amount) AS va_cash_settlement_amount,
                        va.calculate_date AS Tanggal_Hitung,
                        CASE WHEN va.payment_status = '1' THEN 0
                                ELSE DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) END AS overdue,

                        CASE
                                WHEN va.payment_status = '1' THEN "00NO_OD"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) <= 0 THEN "00NO_OD"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) between 1 AND 7 THEN "P001_007"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) between 8 AND 30 THEN "P008_030"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) between 31 AND 60 THEN "P031_060"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) between 61 AND 90 THEN "P061_090"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) between 91 AND 120 THEN "P091_120"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) between 121 AND 150 THEN "P121_150"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) between 151 AND 180 THEN "P151_180"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) between 181 AND 210 THEN "P181_210"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) between 211 AND 240 THEN "P211_240"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) between 241 AND 270 THEN "P241_270"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) between 271 AND 300 THEN "P270_300"
                                WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), DATE(va.create_va_date), DAY) > 300 THEN "P300_UP"
                                END AS bucket_collection,
                        cr.correction_amount,
                        cr.note,
                        cr.branch_id,

                    FROM `datawarehouse_idexp.branch_virtual_account` va -- non join row : 241006
                --     LEFT JOIN `datawarehouse_idexp.branch_bank_preference` b1 ON b1.branch_id = va.branch_id
                    LEFT JOIN bank_code b1 ON b1.account_number = va.account_number
                    LEFT JOIN `datawarehouse_idexp.vendor_bank` b2 ON b2.bank_code = b1.bank_code
                    LEFT JOIN `datawarehouse_idexp.res_branch` t1 ON va.branch_id = t1.id
                    LEFT JOIN `datawarehouse_idexp.res_branch` t3 ON t1.agent_branch_id = t3.id --AND t3.branch_level = '01'
                    LEFT JOIN `datamart_idexp.masterdata_finance_dm_wilayah` t2 ON va.branch_name = t2.branch_name
                    LEFT JOIN `datamart_idexp.masterdata_finance_dm_wilayah` t4 ON t1.branch_name = t4.branch_name
                    LEFT JOIN correction cr ON cr.branch_id = va.branch_id AND va.calculate_date = cr.correction_date
                    -- WHERE t1.branch_id = 158 AND t1.calculate_date = '2024-06-20'

                    WHERE (DATE(va.payment_date) >= DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -62 DAY)
                    OR va.payment_status IN ('0','2'))-- BETWEEN '2023-05-01' AND '2023-05-21'
                    )

                    ,union_cte AS (
                            SELECT bva.*, bvad.waybill_no, bvad.cod_amount, 0 AS sf
                            FROM va_detail bva
                            LEFT JOIN `datawarehouse_idexp.branch_virtual_account_detail` bvad ON bva.id = bvad.branch_virtual_account_id
                            WHERE --bva.created_at >= '2023-06-09 17:00:00' and bva.created_at < '2023-06-10 17:00:00' and
                            bvad.cod_amount > 0

                    UNION ALL

                            SELECT bva.*, csd.waybill_no, 0 AS cod_amount, csd.total_shipping_fee-csd.other_fee AS sf
                            FROM va_detail bva
                            LEFT JOIN `datawarehouse_idexp.cash_settlement_detail` csd ON bva.id = csd.branch_virtual_account_id
                            WHERE --bva.created_at >= '2023-06-09 17:00:00'and bva.created_at < '2023-06-10 17:00:00' and
                            (total_shipping_fee > 0 OR insurance_amount > 0)
                    )

                        SELECT --union_cte.* EXCEPT(cod_amount, sf),
                                union_cte.id,
                                union_cte.No_VA,
                                union_cte.Nama_Bank,
                                union_cte.Nama_Mitra,
                                union_cte.Nama_TH,
                                union_cte.wilayah,
                                union_cte.dm,
                                union_cte.Kode_TH,
                                union_cte.Tanggal_Tagihan_VA,
                                CAST(union_cte.Tanggal_Pembayaran AS DATETIME) AS Tanggal_Pembayaran,
                                union_cte.Status_Transaksi,
                                -- union_cte.Total_Nominal,
                                -- union_cte.va_cod_amount,
                                -- union_cte.va_cash_settlement_amount,
                                IFNULL(union_cte.va_cod_amount,0)
                                  + IFNULL(union_cte.va_cash_settlement_amount,0)
                                  + IFNULL(union_cte.correction_amount,0) AS Total_Nominal,
                                union_cte.correction_amount AS Nominal_Koreksi,
                                union_cte.note,
                                union_cte.branch_id,
                                union_cte.Tanggal_Hitung,
                                union_cte.overdue,
                                union_cte.bucket_collection,
                                union_cte.waybill_no,
                                MAX(union_cte.cod_amount) AS COD_Amount,
                                MAX(union_cte.sf) AS Cash_Amount,
                                MAX(union_cte.cod_amount)+MAX(union_cte.sf) AS Total_Waybill_Amount
                        FROM union_cte
                        WHERE union_cte.Tanggal_Tagihan_VA IS NOT NULL
                          -- AND union_cte.branch_id = 158 AND union_cte.Tanggal_Hitung = '2024-06-20'
                        -- AND union_cte.waybill_no IN ('IDD903743504521', 'IDD901891907557')
                        -- AND union_cte.No_VA IN ('135026000003731881', '135026000003731880')
                        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
                        -- ORDER BY id DESC