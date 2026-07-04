{{
    config(
        materialized='incremental',
        unique_key='warehouse_id'
    )
}}

WITH stg_warehouses AS (
    SELECT * FROM {{ ref('stg_warehouses') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY warehouse_id 
            ORDER BY warehouse_id DESC
        ) AS row_num
    FROM stg_warehouses
),

transformed AS (
    SELECT
        UPPER(TRIM(warehouse_id)) AS warehouse_id,

        TRIM(city) AS city,
        TRIM(state) AS state,

        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed