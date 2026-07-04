{{
    config(
        materialized='incremental',
        unique_key='store_id'
    )
}}

WITH stg_stores_details AS (
    SELECT * FROM {{ ref('stg_stores_details') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY store_id 
            ORDER BY store_id DESC
        ) AS row_num
    FROM stg_stores_details
),

transformed AS (
    SELECT
        UPPER(TRIM(store_id)) AS store_id,

        TRIM(store_name) AS store_name,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(region) AS region,
        
        TO_DATE(TO_TIMESTAMP_NTZ(store_open_date / 1000000000)) AS store_open_date,

        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed