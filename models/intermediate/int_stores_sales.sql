{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

WITH max_date AS (

    {% if is_incremental() %}
        SELECT MAX(file_date) AS max_file_date FROM {{ ref('stg_stores_sales') }}
    {% else %}
        SELECT NULL AS max_file_date
    {% endif %}

),

stg_stores_sales AS (

    SELECT s.*
    FROM {{ ref('stg_stores_sales') }} s
    CROSS JOIN max_date m

    {% if is_incremental() %}
    WHERE s.file_date > m.max_file_date
    {% endif %}

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY file_date DESC
        ) AS row_num
    FROM stg_stores_sales

),

transformed AS (

    SELECT
        UPPER(TRIM(transaction_id)) AS transaction_id,
        UPPER(TRIM(store_id)) AS store_id,
        UPPER(TRIM(product_id)) AS product_id,

        CAST(quantity_sold AS INTEGER) AS quantity_sold,
        CAST(unit_price AS NUMERIC(12, 2)) AS unit_price,
        
        COALESCE(CAST(discount_pct AS NUMERIC(5, 2)), 0) AS discount_pct,
        
        CAST(sale_amount AS NUMERIC(15, 2)) AS gross_sale_amount,
        
        CAST(
            sale_amount * (1 - COALESCE(discount_pct, 0)) 
            AS NUMERIC(15, 2)
        ) AS net_sale_amount,

        TO_TIMESTAMP_NTZ(transaction_timestamp / 1000000000) AS transaction_at,
        TO_DATE(TO_TIMESTAMP_NTZ(transaction_timestamp / 1000000000)) AS transaction_date,

        CURRENT_TIMESTAMP AS _transformed_at

    FROM deduplicated
    WHERE row_num = 1

)

SELECT * FROM transformed