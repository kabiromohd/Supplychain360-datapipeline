{{
    config(
        materialized='incremental',
        unique_key='supplier_id'
    )
}}

WITH stg_suppliers AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY supplier_id 
            ORDER BY supplier_id DESC
        ) AS row_num
    FROM stg_suppliers
),

transformed AS (
    SELECT
        UPPER(TRIM(supplier_id)) AS supplier_id,

        TRIM(supplier_name) AS supplier_name,
        TRIM(category) AS category,
        TRIM(country) AS country,

        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed