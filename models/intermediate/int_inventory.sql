{{
    config(
        materialized='incremental',
        unique_key='inventory_sk'
    )
}}

WITH stg_inventory AS (

    SELECT * FROM {{ ref('stg_inventory') }}

    {% if is_incremental() %}
    WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{ this }})
    {% endif %}

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY warehouse_id, product_id, snapshot_date 
            ORDER BY snapshot_date DESC
        ) AS row_num
    FROM stg_inventory

),

transformed AS (

    SELECT
        {{ generate_surrogate_key(['warehouse_id', 'product_id', 'snapshot_date']) }} AS inventory_sk,

        UPPER(TRIM(warehouse_id)) AS warehouse_id,
        UPPER(TRIM(product_id)) AS product_id,

        quantity_available,
        reorder_threshold,
        snapshot_date,

        CASE 
            WHEN quantity_available = 0 THEN 'OUT_OF_STOCK'
            WHEN quantity_available <= reorder_threshold THEN 'LOW_STOCK'
            ELSE 'HEALTHY'
        END AS stock_status,

        CURRENT_TIMESTAMP AS _transformed_at

    FROM deduplicated
    WHERE row_num = 1

)

SELECT * FROM transformed