
{#{{ config(#}
{#  schema='staging',#}
{#  materialized='table',#}
{#  tags = 'pst',#}
{#  properties = {#}
{#    "format": "'PARQUET'",#}
{#    "partitioning": "ARRAY['load_date']"#}
{#    }#}
{#) }}#}
{{ config(
  schema='staging',
  materialized='incremental',
  incremental_strategy='append',
  tags = 'pst',
    pre_hook=[
    "DELETE FROM {{ this }} WHERE is_corr_receipt = true"
  ]
  )}}


WITH
    {% if is_incremental() %}
    changed_keys AS (

    select distinct *
    from (SELECT rtl_txn_rk
          FROM {{ ref("bpretaillineitem_pre") }}
          where load_ts > (select max(load_ts) from {{ this }})
            and load_date >= (select max(load_date) from {{ this }})

          union all

          SELECT rtl_txn_rk
          FROM {{ ref("bptender_pre") }}
          where load_ts > (select max(load_ts) from {{ this }})
            and load_date >= (select max(load_date) from {{ this }})
             ) t

),{% endif %}
tx as (select *
      from {{ ref('bptransaction_pre')}}
      {% if is_incremental() %}
        where load_ts > (select max (load_ts) from {{ this }})
        and load_date >= (select max (load_date) from {{ this }})
        {% endif %}
),
bptender_corr as (
    SELECT
        xxhash64(
             CAST(CONCAT_WS('|',
              retailstoreid,
              businessdaydate,
              workstationid,
              transnumber) AS VARBINARY)
            ) AS rtl_txn_rk,
        xxhash64(
             CAST(CONCAT_WS('|',
                retailstoreid,
                businessdaydate,
                workstationid,
                transnumber,
                tendernumber) AS VARBINARY)
            ) AS rtl_txn_tender_rk,
        transnumber,
        retailstoreid,
        DATE(date_parse(businessdaydate, '%Y%m%d')) AS businessdaydate,
        transtypecode,
        workstationid,
        tenderamount,
        tendercurrency,
        tenderid,
        CAST(tendernumber AS INT) AS tendernumber,
        tendertypecode,
        referenceid,
        accountnumber,
        load_ts,
        date(load_ts) as load_date,
        true AS is_corr_receipt
    FROM {{ source('staging','corr_receipts') }}
    WHERE recordqualifier = 21
)
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
            FROM {{ref('bptransactextensio_pre')}} tt
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
    c.load_date,
    false AS is_corr_receipt
FROM tx c
JOIN (
    SELECT t1.rtl_txn_rk, SUM(salesamount) AS sum_salesamount
    FROM {{ ref('bpretaillineitem_pre') }} t1
    {% if is_incremental() %}
    join changed_keys t2
    on t1.rtl_txn_rk = t2.rtl_txn_rk
    {% endif %}
    GROUP BY t1.rtl_txn_rk
) sales
    ON c.rtl_txn_rk = sales.rtl_txn_rk
JOIN (
    SELECT t1.rtl_txn_rk, SUM(tenderamount) AS sum_tenderamount
    FROM {{ ref('bptender_pre') }} t1
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
            FROM {{ref('bptransactextensio_pre')}} tt
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
    t.load_date,
    false AS is_corr_receipt
FROM tx t
JOIN (
    SELECT t1.rtl_txn_rk, SUM(salesamount) AS sum_salesamount
    FROM {{ ref('bpretaillineitem_pre') }} t1
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
        FROM {{ ref ('bptender_pre') }} te
        WHERE te.rtl_txn_rk = t.rtl_txn_rk
        {% if is_incremental() %}
            and load_ts > (select max (load_ts) from {{ this }})
            and load_date >= (select max (load_date) from {{ this }})
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
                FROM {{ref('bptenderextensions_pre')}} tex
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
    te.load_date,
    false as is_corr_receipt
FROM {{ ref ('bptender_pre') }} te
WHERE 1=1
{% if is_incremental() %}
and load_ts > (select max (load_ts) from {{ this }})
and load_date >= (select max (load_date) from {{ this }})
{% endif %}
union all
select *
from bptender_corr
