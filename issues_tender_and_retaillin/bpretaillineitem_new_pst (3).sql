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


-- =====================================================================
-- Оптимизация: вся логика marm_fm/mean_fm сведена к ОДНОМУ JOIN
-- вместо 6 повторных JOIN в оригинале.
-- split_part + lpad вычисляются ОДИН раз в CTE base_enriched.
-- correlated EXISTS заменён на LEFT JOIN (auto_md_flag).
-- =====================================================================

WITH 

-- =====================================================================
-- AUTO_MD флаг: LEFT JOIN вместо correlated EXISTS (быстрее)
-- =====================================================================
auto_md_flags AS (
    SELECT DISTINCT
        rtl_txn_rk,
        rtl_txn_item_rk
    FROM {{ source('raw_table','raw_bplineitemextensio') }}
    WHERE fieldgroup = 'POS'
      AND fieldname  = 'AUTO_MD'
      AND fieldvalue = 'Y'
),

ean_mapping AS (
    SELECT DISTINCT
        m.matnr,
        m.meinh,
        m.umrez,
        e.ean11,
        CASE
            WHEN e.ean11 IS NOT NULL AND e.ean11 <> '' THEN TRUE
            ELSE FALSE
        END AS has_ean
    FROM {{ source('dictionaries', 'marm_fm') }} m
    JOIN {{ source('dictionaries', 'mean_fm') }} e
      ON m.matnr = e.matnr
     AND m.meinh = e.meinh
    WHERE e.lfnum = '00001'
),

-- =====================================================================
-- base: применяем AUTO_MD → '2043' + предвычисляем matnr_padded/umrez
-- (Оптимизация: split_part + lpad вычисляются ОДИН раз)
-- =====================================================================

base_enriched AS (
    SELECT
        t.rtl_txn_rk,
        t.rtl_txn_item_rk,
        t.businessdaydate,
        t.itemid,
        t.itemidentrymethodcode,
        t.itemidqualifier,
        t.normalsalesamount,
        t.promotionid,
        t.retailquantity,
        t.retailsequencenumber,
        t.retailstoreid,
        CASE
            WHEN amd.rtl_txn_rk IS NOT NULL THEN '2043'
            ELSE t.retailtypecode
        END AS retailtypecode,
        t.retailreasoncode,
        t.salesamount,
        t.salesunitofmeasure,
        t.transactionsequencenumber,
        t.transactiontypecode,
        t.units,
        t.workstationid,
        t.load_ts,
        t.load_date_xml,
        t.load_ts_xml,
        t.matnr_padded,
        t.umrez_val,
        em.ean11,
        em.meinh,
        em.has_ean
    FROM {{ source('raw_table','raw_bpretaillineitem') }} AS t
    LEFT JOIN auto_md_flags amd
        ON amd.rtl_txn_rk = t.rtl_txn_rk
        AND amd.rtl_txn_item_rk = t.rtl_txn_item_rk
    LEFT JOIN ean_mapping em
        ON t.matnr_padded IS NOT NULL
        AND em.matnr = t.matnr_padded
        AND em.umrez = t.umrez_val
{% if is_incremental() %}
    WHERE load_ts_xml > (SELECT COALESCE(MAX(load_ts_xml), CAST('1970-01-01' AS TIMESTAMP)) FROM {{ this }})
    AND load_date_xml >= (SELECT COALESCE(MAX(load_date_xml),  DATE '1970-01-01') FROM {{ this }})
{% endif %}
)

-- =====================================================================
-- Присоединяем маппинг к base (один проход)
-- ====================================================================

-- =====================================================================
-- Корректировочные чеки
-- =====================================================================
--bpretaillineitem_corr AS (
--    SELECT
--        xxhash64(
--            CAST(concat_ws('|', retailstoreid, businessdaydate, workstationid, transnumber) AS VARBINARY)
--        ) AS rtl_txn_rk,
--        xxhash64(
--            CAST(concat_ws('|', retailstoreid, businessdaydate, workstationid, transnumber, retailnumber) AS VARBINARY)
--        ) AS rtl_txn_item_rk,
--        date_parse(businessdaydate, '%Y%m%d') AS businessdaydate,
--        itemid,
--        entrymethodcode,
--        CASE
--            WHEN retailtypecode = '2010' THEN NULL
--            ELSE itemidqualifier
--        END AS itemidqualifier,
--        CAST(normalsalesamt AS DECIMAL(32,2)) AS normalsalesamount,
--        promotionid,
--        CAST(retailquantity AS DECIMAL(32,3)) AS retailquantity,
--        CAST(retailnumber AS INTEGER) AS retailsequencenumber,
--        retailstoreid,
--        retailtypecode,
--        retailreasoncode,
--        CAST(salesamount AS DECIMAL(32,2)) AS salesamount,
--        salesuom,
--        transnumber,
--        transtypecode,
--        CAST(units AS DECIMAL(32,2)) AS units,
--        workstationid,
--        load_ts,
--        date(now() - INTERVAL '1' HOUR) as b.load_date_xml,
--        (now() - INTERVAL '1' HOUR) as load_ts_xml,
--        TRUE AS is_corr_receipt
--    FROM {{ source('staging','corr_receipts') }}
--    WHERE recordqualifier = 5
--)

-- =====================================================================
-- Финальный SELECT
-- Фильтрация delete: has_ean = false → исключаем (NOT has_ean = false)
-- Update: has_ean = true → подставляем ean11, meinh, itemidqualifier='1'
-- =====================================================================
SELECT
    b.rtl_txn_rk,
    b.rtl_txn_item_rk,
    b.businessdaydate,

    CASE
        WHEN b.transactiontypecode = '1014'
             AND COALESCE(CASE WHEN b.has_ean = TRUE THEN b.ean11 END, b.itemid) IS NULL
            THEN '0'
        ELSE COALESCE(CASE WHEN b.has_ean = TRUE THEN b.ean11 END, b.itemid)
    END AS itemid,

    b.itemidentrymethodcode,

    CASE
        WHEN b.transactiontypecode = '1014'
             AND COALESCE(CASE WHEN b.has_ean = TRUE THEN b.ean11 END, b.itemid) IS NULL
            THEN ''
        WHEN b.has_ean = TRUE THEN '1'
        ELSE b.itemidqualifier
    END AS itemidqualifier,

    b.normalsalesamount,
    b.promotionid,
    b.retailquantity,
    b.retailsequencenumber,
    b.retailstoreid,
    b.retailtypecode,
    b.retailreasoncode,
    b.salesamount,

    COALESCE(
        CASE WHEN b.has_ean = TRUE THEN b.meinh END,
        b.salesunitofmeasure
    ) AS salesunitofmeasure,

    b.transactionsequencenumber,
    b.transactiontypecode,
    b.units,
    b.workstationid,
    b.load_ts,
    b.load_date_xml,
    b.load_ts_xml,
    FALSE AS is_corr_receipt
FROM base_enriched b
WHERE b.has_ean IS NULL        -- нет маппинга вообще (не UP-товар, или нет в справочнике)
   OR b.has_ean = TRUE         -- есть маппинг с валидным EAN (update-кандидат)
                               -- has_ean = FALSE → delete-кандидат, исключается

--UNION ALL
--
--SELECT *
--FROM bpretaillineitem_corr
