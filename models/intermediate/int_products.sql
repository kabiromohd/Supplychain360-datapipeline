{{
    config(
        materialized='incremental',
        unique_key='product_id'
    )
}}

WITH stg_products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY product_id  -- fallback ordering
        ) AS row_num
    FROM stg_products
),

transformed AS (
    SELECT
        UPPER(TRIM(product_id)) AS product_id,
        UPPER(TRIM(supplier_id)) AS supplier_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(brand) AS brand,
        CAST(unit_price AS NUMERIC(12, 2)) AS unit_price,

        CASE 
            WHEN CAST(unit_price AS NUMERIC(12, 2)) > 500 THEN 'HIGH_VALUE'
            WHEN CAST(unit_price AS NUMERIC(12, 2)) > 100 THEN 'MEDIUM_VALUE'
            ELSE 'STANDARD_VALUE'
        END AS price_segment,

        CURRENT_TIMESTAMP AS _transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM transformed