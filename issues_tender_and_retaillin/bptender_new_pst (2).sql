--{# {{ config(#}
--{#   schema='staging',#}
--{#   materialized='table',#}
--{#   tags = 'pst2',#}
--{#   properties = {#}
--{#     "format": "'PARQUET'",#}
--{#     "partitioning": "ARRAY['load_date_xml']",#}
--{#     "sorted_by": "ARRAY['rtl_txn_rk', 'load_ts_xml']"#}
--{#     }#}
--{# ) }}#}

{{ config(
  schema='staging_',
  materialized='incremental',
  incremental_strategy='append',
  tags = 'pst2'
) }}


WITH
    {% if is_incremental() %}
    changed_keys AS (

    select distinct *
    from (SELECT rtl_txn_rk
          FROM {{ source('raw_table',"raw_bpretaillineitem") }}
          where load_ts_xml > (SELECT COALESCE(MAX(load_ts_xml), CAST('1970-01-01' AS TIMESTAMP)) FROM {{ this }})
          AND load_date_xml >= (SELECT COALESCE(MAX(load_date_xml),  DATE '1970-01-01') FROM {{ this }})

          union all

          SELECT rtl_txn_rk
          FROM {{ source('raw_table',"raw_bptender") }}
          where load_ts_xml > (SELECT COALESCE(MAX(load_ts_xml), CAST('1970-01-01' AS TIMESTAMP)) FROM {{ this }})
          AND load_date_xml >= (SELECT COALESCE(MAX(load_date_xml),  DATE '1970-01-01') FROM {{ this }})
             ) t

),{% endif %}
tx as (select *
      from {{ source('raw_table','raw_bptransaction')}}
      {% if is_incremental() %}
      where load_ts_xml > (SELECT COALESCE(MAX(load_ts_xml), CAST('1970-01-01' AS TIMESTAMP)) FROM {{ this }})
      AND load_date_xml >= (SELECT COALESCE(MAX(load_date_xml),  DATE '1970-01-01') FROM {{ this }})
        {% endif %}
)
--bptender_corr as (
--    SELECT
--        xxhash64(
--             CAST(CONCAT_WS('|',
--              retailstoreid,
--              businessdaydate,
--              workstationid,
--              transnumber) AS VARBINARY)
--            ) AS rtl_txn_rk,
--        xxhash64(
--             CAST(CONCAT_WS('|',
--                retailstoreid,
--                businessdaydate,
--                workstationid,
--                transnumber,
--                tendernumber) AS VARBINARY)
--            ) AS rtl_txn_tender_rk,
--        transnumber,
--        retailstoreid,
--        DATE(date_parse(businessdaydate, '%Y%m%d')) AS businessdaydate,
--        transtypecode,
--        workstationid,
--        tenderamount,
--        tendercurrency,
--        tenderid,
--        CAST(tendernumber AS INT) AS tendernumber,
--        tendertypecode,
--        referenceid,
--        accountnumber,
--        load_ts,
--        date(now() - INTERVAL '1' HOUR) as b.load_date_xml,
--        (now() - INTERVAL '1' HOUR) as load_ts_xml,
--        true AS is_corr_receipt
--    FROM {{ source('staging','corr_receipts') }}
--    WHERE recordqualifier = 21
--)
SELECT
    c.rtl_txn_rk,
    xxhash64(
        CAST(CONCAT_WS('|',
            c.retailstoreid,
            CAST(c.businessdaydate AS VARCHAR),
            c.workstationid,
            c.transactionsequencenumber,
            '9'
        ) AS VARBINARY)
    ) AS rtl_txn_tender_rk,
    c.transactionsequencenumber,
    c.retailstoreid,
    c.businessdaydate,
    c.transactiontypecode,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM {{source('raw_table','raw_bptransactextensio')}} tt
            WHERE tt.rtl_txn_rk = c.rtl_txn_rk
              AND tt.fieldname  = 'SOURCE'
              AND tt.fieldvalue = 'vprok.express'
        )
        THEN '0000001002'
        ELSE c.workstationid
    END                                                 AS workstationid,
    (sales.sum_salesamount - tend.sum_tenderamount)     AS tenderamount,
    NULL                                                AS tendercurrency,
    NULL                                                AS tenderid,
    9                                                 AS tendersequencenumber,
    '3101'                                              AS tendertypecode,
    NULL                                                AS referenceid,
    NULL                                                AS accountnumber,
	c.load_ts,
    c.load_date_xml,
    c.load_ts_xml,
    false AS is_corr_receipt
FROM tx c
JOIN (
    SELECT t1.rtl_txn_rk, SUM(salesamount) AS sum_salesamount
    FROM {{ source('raw_table','raw_bpretaillineitem') }} t1
    {% if is_incremental() %}
    join changed_keys t2
    on t1.rtl_txn_rk = t2.rtl_txn_rk
    {% endif %}
    GROUP BY t1.rtl_txn_rk
) sales
    ON c.rtl_txn_rk = sales.rtl_txn_rk
JOIN (
    SELECT t1.rtl_txn_rk, SUM(tenderamount) AS sum_tenderamount
    FROM {{ source('raw_table','raw_bptender') }} t1
    {% if is_incremental() %}
    join changed_keys t2
    on t1.rtl_txn_rk = t2.rtl_txn_rk
    {% endif %}
    GROUP BY t1.rtl_txn_rk
) tend
    ON c.rtl_txn_rk = tend.rtl_txn_rk
    WHERE c.transactiontypecode = '1014'
        AND sales.sum_salesamount <> tend.sum_tenderamount

UNION ALL

-- 2) Чеки без оплат: создаём оплату целиком на сумму продаж с tenderseq = 8
SELECT
    t.rtl_txn_rk,
    xxhash64(
        CAST(CONCAT_WS('|',
            t.retailstoreid,
            CAST(t.businessdaydate AS VARCHAR),
            t.workstationid,
            t.transactionsequencenumber,
            '8'
        ) AS VARBINARY)
    ) AS rtl_txn_tender_rk,
    t.transactionsequencenumber,
    t.retailstoreid,
    t.businessdaydate,
    t.transactiontypecode,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM {{source('raw_table','raw_bptransactextensio')}} tt
            WHERE tt.rtl_txn_rk = t.rtl_txn_rk
              AND tt.fieldname  = 'SOURCE'
              AND tt.fieldvalue = 'vprok.express'
        )
        THEN '0000001002'
        ELSE t.workstationid
    END                                                 AS workstationid,
    s.sum_salesamount                                   AS tenderamount,
    NULL                                                AS tendercurrency,
    NULL                                                AS tenderid,
    8                                              AS tendersequencenumber,
    '3101'                                              AS tendertypecode,
    NULL                                                AS referenceid,
    NULL                                                AS accountnumber,
	t.load_ts,
    t.load_date_xml,
    t.load_ts_xml,
    false AS is_corr_receipt
FROM tx t
JOIN (
    SELECT t1.rtl_txn_rk, SUM(salesamount) AS sum_salesamount
    FROM {{ source('raw_table','raw_bpretaillineitem') }} t1
    {% if is_incremental() %}
    join changed_keys t2
    on t1.rtl_txn_rk = t2.rtl_txn_rk
    {% endif %}
    GROUP BY t1.rtl_txn_rk
) s
ON t.rtl_txn_rk = s.rtl_txn_rk
WHERE 1=1
  AND t.transactiontypecode = '1014'
  AND NOT EXISTS (
        SELECT 1
        FROM {{ source ('raw_table','raw_bptender') }} te
        WHERE te.rtl_txn_rk = t.rtl_txn_rk
        {% if is_incremental() %}
        AND load_ts_xml > (SELECT COALESCE(MAX(load_ts_xml), CAST('1970-01-01' AS TIMESTAMP)) FROM {{ this }})
        AND load_date_xml >= (SELECT COALESCE(MAX(load_date_xml),  DATE '1970-01-01') FROM {{ this }})
        {% endif %}
  )

UNION ALL

-- 3) Протягиваем существующие оплаты, мапим тип 3108->3123 для CERT_PARTY с RU02%
SELECT
    te.rtl_txn_rk,
    te.rtl_txn_tender_rk,
    te.transactionsequencenumber,
    te.retailstoreid,
    te.businessdaydate,
    te.transactiontypecode,
    te.workstationid,
    te.tenderamount,
    te.tendercurrency,
    te.tenderid,
    te.tendersequencenumber,
    CASE
        WHEN te.tendertypecode = '3108'
         AND EXISTS (
                SELECT 1
                FROM {{source('raw_table','raw_bptenderextensions')}} tex
                WHERE tex.rtl_txn_rk        = te.rtl_txn_rk
                  AND tex.rtl_txn_tender_rk = te.rtl_txn_tender_rk
                  AND tex.fieldname  = 'CERT_PARTY'
                  AND tex.fieldvalue LIKE 'RU02%'
        )
        THEN '3123'
        ELSE te.tendertypecode
    END                                                 AS tendertypecode,
    te.referenceid,
    te.accountnumber,
	te.load_ts,
    te.load_date_xml,
    te.load_ts_xml,
    false as is_corr_receipt
FROM {{ source ('raw_table','raw_bptender') }} te
WHERE 1=1
{% if is_incremental() %}
    AND load_ts_xml > (SELECT COALESCE(MAX(load_ts_xml), CAST('1970-01-01' AS TIMESTAMP)) FROM {{ this }})
    AND load_date_xml >= (SELECT COALESCE(MAX(load_date_xml),  DATE '1970-01-01') FROM {{ this }})
{% endif %}
--union all
--select *
--from bptender_corr
