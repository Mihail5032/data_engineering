--{# {{ config
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

--with bptransaction_corr as (
--    SELECT
--        xxhash64(
--            CAST(CONCAT_WS('|',
--                retailstoreid,
--                businessdaydate,
--                workstationid,
--                transnumber
--            ) AS VARBINARY)
--        ) AS rtl_txn_rk,
--        cast(date_parse(begintimestamp, '%Y%m%d%H%i%s') as timestamp) as begintimestamp,
--        DATE(date_parse(businessdaydate, '%Y%m%d')) AS businessdaydate,
--        cast(date_parse(endtimestamp, '%Y%m%d%H%i%s') as timestamp) as endtimestamp,
--        operatorid,
--        retailstoreid,
--        transcurrency,
--        transnumber,
--        transtypecode,
--        workstationid,
--        load_ts,
--        date(now() - INTERVAL '1' HOUR) as b.load_date_xml,
--        (now() - INTERVAL '1' HOUR) as load_ts_xml,
--        true AS is_corr_receipt
----    FROM {{ source('staging','corr_receipts') }}
----    WHERE recordqualifier = 1
--)
select
    t.rtl_txn_rk,
    t.begindatetimestamp,
  CASE 
      WHEN t.transactiontypecode LIKE '11%' 
          AND DATE(t.enddatetimestamp) <> t.businessdaydate
      THEN DATE(t.enddatetimestamp)
      ELSE t.businessdaydate
  END AS businessdaydate,
    t.enddatetimestamp,
    t.operatorid,
    t.retailstoreid,
    t.transactioncurrency,
    t.transactionsequencenumber,
    t.transactiontypecode,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM {{ source('raw_table','raw_bptransactextensio') }} tt
            WHERE tt.rtl_txn_rk = t.rtl_txn_rk
              AND tt.fieldname   = 'SOURCE'
              AND tt.fieldvalue  = 'vprok.express'
        )
        THEN '1002'
        ELSE t.workstationid
    END AS workstationid,
    load_ts,
    load_date_xml,
    load_ts_xml,
    false as is_corr_receipt
FROM (select *
      from {{ source('raw_table','raw_bptransaction') }}
    {% if is_incremental() %}
        WHERE load_ts_xml > (SELECT COALESCE(MAX(load_ts_xml), CAST('1970-01-01' AS TIMESTAMP)) FROM {{ this }})
        AND load_date_xml >= (SELECT COALESCE(MAX(load_date_xml),  DATE '1970-01-01') FROM {{ this }})
    {% endif %}
     )t
--union all
--select *
--from bptransaction_corr

