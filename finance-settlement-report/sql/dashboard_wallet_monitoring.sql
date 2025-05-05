WITH non_batch AS (

SELECT
      t1.shipping_client,
      t2.tx_no,
      t3.process_name AS tx_status,
      DATETIME(t2.create_time) AS tx_time,
      DATETIME(oo.pickup_record_time,'Asia/Jakarta') AS pickup_time,
      -- t2.amount,
      t2.amount*-1 AS amount,
      t2.batch_id,
      IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0, ww.waybill_no, oo.waybill_no) AS waybill_no,
      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ww.standard_shipping_fee - oo.standard_shipping_fee
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN ww.standard_shipping_fee
            WHEN t2.tx_status = '04' THEN ww.standard_shipping_fee
            WHEN t2.tx_status = '06' THEN rr.return_standard_shipping_fee
            ELSE oo.standard_shipping_fee END AS standard_shipping_fee,

      -- IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ww.standard_shipping_fee - oo.standard_shipping_fee, oo.standard_shipping_fee) AS standard_shipping_fee,

      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ww.insurance_amount - oo.insurance_amount
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN ww.insurance_amount
            WHEN t2.tx_status = '04' THEN ww.insurance_amount
            WHEN t2.tx_status = '06' THEN rr.insurance_amount
            ELSE oo.insurance_amount END AS insurance_amount,

      -- IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ww.insurance_amount - oo.insurance_amount, oo.insurance_amount) AS insurance_amount,

      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ww.other_fee - oo.other_fee
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN ww.other_fee
            WHEN t2.tx_status = '04' THEN ww.other_fee
            WHEN t2.tx_status = '06' THEN rr.other_fee
            ELSE oo.other_fee END AS other_fee,

      -- IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ww.other_fee - oo.other_fee, oo.other_fee) AS other_fee,

      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ww.handling_fee - oo.handling_fee
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN ww.handling_fee
            WHEN t2.tx_status = '04' THEN ww.handling_fee
            WHEN t2.tx_status = '06' THEN rr.return_handling_fee
            ELSE oo.handling_fee END AS handling_fee,

      -- IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ww.handling_fee - oo.handling_fee, oo.handling_fee) AS handling_fee,

      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ww.total_shipping_fee - oo.total_shipping_fee
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN ww.total_shipping_fee
            WHEN t2.tx_status = '04' THEN ww.total_shipping_fee
            WHEN t2.tx_status = '06' THEN rr.return_shipping_fee
            ELSE oo.total_shipping_fee END AS total_shipping_fee,

      -- IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ww.total_shipping_fee - oo.total_shipping_fee, oo.total_shipping_fee) AS total_shipping_fee,

      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ((ABS(ww.total_shipping_fee - oo.total_shipping_fee)) - ABS(t2.amount)) / ABS(ww.standard_shipping_fee - oo.standard_shipping_fee)
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN (ABS(ww.total_shipping_fee) - ABS(t2.amount)) / ABS(ww.standard_shipping_fee)
            WHEN t2.tx_status = '04' THEN (ABS(ww.total_shipping_fee) - ABS(t2.amount)) / ABS(ww.standard_shipping_fee)
            WHEN t2.tx_status = '06' THEN (ABS(rr.return_shipping_fee) - ABS(t2.amount)) / ABS(rr.return_standard_shipping_fee)
            ELSE (ABS(oo.total_shipping_fee) - ABS(t2.amount)) / ABS(oo.standard_shipping_fee)
            END AS discount_rate,

      -- ((IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ABS(ww.total_shipping_fee - oo.total_shipping_fee), ABS(oo.total_shipping_fee)) - ABS(t2.amount)) / IF(t2.tx_status = '03'
      --       AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ABS(ww.standard_shipping_fee - oo.standard_shipping_fee), ABS(oo.standard_shipping_fee))) AS discount_rate,

      IF(t2.batch_id = '' OR t2.batch_id IS NULL, NULL, RANK() OVER (PARTITION BY t2.batch_id ORDER BY oo.waybill_no ASC)) AS batch_rank,
      t6.option_name AS express_type,
      t2.request_status,

      DATETIME(ww.pod_record_time,'Asia/Jakarta') AS pod_record_time,
      DATETIME(ww.return_pod_record_time,'Asia/Jakarta') AS return_pod_record_time
      -- CASE WHEN t6.batch_id IS NOT NULL THEN COUNT(t6.waybill_no)
      -- ELSE 1 END AS total_waybill

      FROM `datawarehouse_idexp.wallet` t1 --242
      LEFT JOIN `datawarehouse_idexp.wallet_history` t2 ON t1.id = t2.wallet_id
      LEFT JOIN `datawarehouse_idexp.wallet_status` t3 ON t2.tx_status = t3.code
      LEFT JOIN `datawarehouse_idexp.wallet_history_detail` t4 ON t2.id = t4.wallet_history_id
      LEFT JOIN `datawarehouse_idexp.order_order` oo ON t4.waybill_no = oo.waybill_no AND DATE(oo.input_time,'Asia/Jakarta') >= '2023-07-24'
      LEFT JOIN `datawarehouse_idexp.waybill_waybill` ww ON t4.waybill_no = ww.waybill_no AND DATE(ww.shipping_time,'Asia/Jakarta') >= '2023-07-24'
      LEFT JOIN `datawarehouse_idexp.waybill_return_bill` rr ON t4.waybill_no = rr.waybill_no AND rr.return_waybill_no = ww.return_waybill_no
      LEFT JOIN `datawarehouse_idexp.system_option` t6 ON t6.option_value = oo.express_type AND t6.type_option = 'expressType'
      -- LEFT JOIN `datawarehouse_idexp.vip_order` t6 ON t4.waybill_no = t6.waybill_no

      WHERE --DATE(t1.create_time,'Asia/Jakarta') >= '2023-07-24'
            --AND
  			t2.tx_no IS NOT NULL
            AND t2.batch_id = ''
            AND oo.waybill_no IS NOT NULL
)
, batch AS (
      SELECT
      t1.shipping_client,
      t2.tx_no,
      t3.process_name AS tx_status,
      DATETIME(t2.create_time) AS tx_time,
      DATETIME(oo.pickup_record_time,'Asia/Jakarta') AS pickup_time,
      -- t2.amount,
      t2.amount*-1 AS amount,
      t2.batch_id,
      IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0, ww.waybill_no, oo.waybill_no) AS waybill_no,
      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ww.standard_shipping_fee - oo.standard_shipping_fee
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN ww.standard_shipping_fee
            WHEN t2.tx_status = '04' THEN ww.standard_shipping_fee
            WHEN t2.tx_status = '06' THEN rr.return_standard_shipping_fee
            ELSE oo.standard_shipping_fee END AS standard_shipping_fee,

      -- IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ww.standard_shipping_fee - oo.standard_shipping_fee, oo.standard_shipping_fee) AS standard_shipping_fee,

      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ww.insurance_amount - oo.insurance_amount
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN ww.insurance_amount
            WHEN t2.tx_status = '04' THEN ww.insurance_amount
            WHEN t2.tx_status = '06' THEN rr.insurance_amount
            ELSE oo.insurance_amount END AS insurance_amount,

      -- IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ww.insurance_amount - oo.insurance_amount, oo.insurance_amount) AS insurance_amount,

      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ww.other_fee - oo.other_fee
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN ww.other_fee
            WHEN t2.tx_status = '04' THEN ww.other_fee
            WHEN t2.tx_status = '06' THEN rr.other_fee
            ELSE oo.other_fee END AS other_fee,

      -- IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ww.other_fee - oo.other_fee, oo.other_fee) AS other_fee,

      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ww.handling_fee - oo.handling_fee
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN ww.handling_fee
            WHEN t2.tx_status = '04' THEN ww.handling_fee
            WHEN t2.tx_status = '06' THEN rr.return_handling_fee
            ELSE oo.handling_fee END AS handling_fee,

      -- IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ww.handling_fee - oo.handling_fee, oo.handling_fee) AS handling_fee,

      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ww.total_shipping_fee - oo.total_shipping_fee
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN ww.total_shipping_fee
            WHEN t2.tx_status = '04' THEN ww.total_shipping_fee
            WHEN t2.tx_status = '06' THEN rr.return_shipping_fee
            ELSE oo.total_shipping_fee END AS total_shipping_fee,

      -- IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ww.total_shipping_fee - oo.total_shipping_fee, oo.total_shipping_fee) AS total_shipping_fee,

      CASE WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0 THEN ((ABS(ww.total_shipping_fee - oo.total_shipping_fee)) - ABS(t2.amount)) / ABS(ww.standard_shipping_fee - oo.standard_shipping_fee)
            WHEN t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) = 0 THEN (ABS(ww.total_shipping_fee) - ABS(t2.amount)) / ABS(ww.standard_shipping_fee)
            WHEN t2.tx_status = '04' THEN (ABS(ww.total_shipping_fee) - ABS(t2.amount)) / ABS(ww.standard_shipping_fee)
            WHEN t2.tx_status = '06' THEN (ABS(rr.return_shipping_fee) - ABS(t2.amount)) / ABS(rr.return_standard_shipping_fee)
            ELSE (ABS(oo.total_shipping_fee) - ABS(t2.amount)) / ABS(oo.standard_shipping_fee)
            END AS discount_rate,

      -- ((IF(t2.tx_status = '03' AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ABS(ww.total_shipping_fee - oo.total_shipping_fee), ABS(oo.total_shipping_fee)) - ABS(t2.amount)) / IF(t2.tx_status = '03'
      --       AND (ww.standard_shipping_fee - oo.standard_shipping_fee) != 0,ABS(ww.standard_shipping_fee - oo.standard_shipping_fee), ABS(oo.standard_shipping_fee))) AS discount_rate,

      IF(t2.batch_id = '' OR t2.batch_id IS NULL, NULL, RANK() OVER (PARTITION BY t2.batch_id ORDER BY oo.waybill_no ASC)) AS batch_rank,
      t6.option_name AS express_type,
      t2.request_status,


      DATETIME(ww.pod_record_time,'Asia/Jakarta') AS pod_record_time,
      DATETIME(ww.return_pod_record_time,'Asia/Jakarta') AS return_pod_record_time
      -- CASE WHEN t6.batch_id IS NOT NULL THEN COUNT(t6.waybill_no)
      -- ELSE 1 END AS total_waybill

      FROM `datawarehouse_idexp.wallet` t1 --242
      LEFT JOIN `datawarehouse_idexp.wallet_history` t2 ON t1.id = t2.wallet_id
      LEFT JOIN `datawarehouse_idexp.wallet_status` t3 ON t2.tx_status = t3.code
      -- LEFT JOIN `datawarehouse_idexp.wallet_history_detail` t4 ON t2.id = t4.wallet_history_id
      INNER JOIN `datawarehouse_idexp.vip_order` t4 ON t2.batch_id = t4.batch_id --AND DATE(t4.create_time,'Asia/Jakarta') >= '2023-07-24'
      LEFT JOIN `datawarehouse_idexp.order_order` oo ON t4.waybill_no = oo.waybill_no AND DATE(oo.input_time,'Asia/Jakarta') >= '2023-07-24'
      LEFT JOIN `datawarehouse_idexp.waybill_waybill` ww ON t4.waybill_no = ww.waybill_no AND DATE(ww.shipping_time,'Asia/Jakarta') >= '2023-07-24'
      LEFT JOIN `datawarehouse_idexp.waybill_return_bill` rr ON t4.waybill_no = rr.waybill_no AND rr.return_waybill_no = ww.return_waybill_no
      LEFT JOIN `datawarehouse_idexp.system_option` t6 ON t6.option_value = oo.express_type AND t6.type_option = 'expressType'

      WHERE --DATE(t1.create_time,'Asia/Jakarta') >= '2023-07-24'
            --AND
  			t2.tx_no IS NOT NULL
            AND t2.batch_id <> ''
            AND oo.waybill_no IS NOT NULL
            -- AND oo.waybill_no = 'IDV901515393931'

)
, union_awb AS (
      SELECT * FROM non_batch
      UNION ALL
      SELECT * FROM batch
)

, discount_rate AS (
SELECT

      shipping_client,
      tx_no,
      tx_status,
      tx_time,
      pickup_time,
      pod_record_time,
      return_pod_record_time,
      amount,
      IF(batch_rank > 1, NULL, amount) filtered_amount,
      batch_id,batch_rank,
      waybill_no,standard_shipping_fee,insurance_amount,other_fee,handling_fee,total_shipping_fee,express_type,
      request_status,
      CASE WHEN batch_rank > 1 THEN NULL
            WHEN batch_rank = 1 THEN SUM(standard_shipping_fee) OVER (PARTITION BY batch_id)
            ELSE standard_shipping_fee
            END AS batch_standard_shipping_fee,
	  CASE WHEN batch_rank > 1 THEN NULL
            WHEN batch_rank = 1 THEN SUM(total_shipping_fee) OVER (PARTITION BY batch_id)
            ELSE total_shipping_fee
            END AS batch_total_shipping_fee,
      CASE WHEN batch_rank >= 1 THEN ((SUM(ABS(total_shipping_fee)) OVER (PARTITION BY batch_id)) - ABS(amount)) / (SUM(ABS(standard_shipping_fee)) OVER (PARTITION BY batch_id))
            -- WHEN batch_rank > 1 THEN NULL
            ELSE ABS(discount_rate) END AS discount_rate,


FROM union_awb)

SELECT
      wallet.*,
      CASE WHEN wallet.waybill_no IS NOT NULL THEN IF(wallet.batch_rank >= 1, ABS(wallet.standard_shipping_fee) * wallet.discount_rate,
            ABS(wallet.total_shipping_fee) - ABS(wallet.filtered_amount))
          END AS discount_amount,
      CASE WHEN wallet.waybill_no IS NOT NULL AND wallet.total_shipping_fee < 0 AND wallet.batch_rank >= 1 THEN wallet.total_shipping_fee +
            (ABS(wallet.standard_shipping_fee) * wallet.discount_rate)
          WHEN wallet.waybill_no IS NOT NULL AND wallet.total_shipping_fee >= 0 AND wallet.batch_rank >= 1 THEN wallet.total_shipping_fee -
            (ABS(wallet.standard_shipping_fee) * wallet.discount_rate)
          WHEN wallet.waybill_no IS NOT NULL AND wallet.total_shipping_fee < 0 AND wallet.batch_rank IS NULL THEN wallet.total_shipping_fee +
            (ABS(wallet.total_shipping_fee) - ABS(wallet.filtered_amount))
          WHEN wallet.waybill_no IS NOT NULL AND wallet.total_shipping_fee >= 0 AND wallet.batch_rank IS NULL THEN wallet.total_shipping_fee -
            (ABS(wallet.total_shipping_fee) - ABS(wallet.filtered_amount))
          END AS net_shipping_fee,

FROM discount_rate wallet
-- WHERE batch_id = '03022024-SfUQ'
-- WHERE waybill_no = 'IDV903598593153' --'IDV901515393931'
-- WHERE waybill_no IN ( 'IDV902928933295', 'IDV902625527103') --sample case return, void