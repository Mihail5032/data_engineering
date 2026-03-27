
{#{{ config(#}
{#  schema='raw',#}
{#  materialized='table',#}
{#  tags = 'pre',#}
{#  properties = {#}
{#    "format": "'PARQUET'",#}
{#    "partitioning": "ARRAY['load_date']",#}
{#    "sorted_by": "ARRAY['load_ts']"#}
{#    }#}
{#) }}#}
{{ config(
  schema='raw',
  materialized='incremental',
  incremental_strategy='append',
  tags = 'pre'
) }}

SELECT
     xxhash64(
         CAST(CONCAT_WS('|',
          retailstoreid,
          businessdaydate,
          workstationid,
          transactionsequencenumber) AS VARBINARY)
        ) AS rtl_txn_rk,
    xxhash64(
         CAST(CONCAT_WS('|',
            retailstoreid,
            businessdaydate,
            workstationid,
            transactionsequencenumber,
            tendersequencenumber) AS VARBINARY)
        ) AS rtl_txn_tender_rk,
    DATE(CAST(businessdaydate AS TIMESTAMP)) AS businessdaydate,
    SUBSTRING(fieldgroup, 1, 5) as fieldgroup,
    SUBSTRING(fieldname, 1, 10) as fieldname,
    fieldvalue,
    retailstoreid,
    tendersequencenumber,
    transactionsequencenumber,
    transactiontypecode,
    workstationid,
    load_ts,
    date(load_ts) as load_date
    FROM {{ source('raw', 'raw_table_part') }}
    WHERE segment_name = 'E1BPTENDEREXTENSIONS'
{#    and {{ date_filter()  }}#}
        {% if is_incremental() %}
      and load_ts
        > (select max (load_ts) from {{ this }})
      and date (load_ts) >= (select max (load_date) from {{ this }})
        {% endif %}
