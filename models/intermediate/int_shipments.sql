{{
    config(
        materialized='incremental',
        unique_key='shipment_id'
    )
}}

WITH max_loaded AS (

    {% if is_incremental() %}
        SELECT MAX(file_date) AS max_file_date
        FROM {{ ref('stg_shipments') }}
    {% else %}
        SELECT NULL AS max_file_date
    {% endif %}

),

stg_shipments AS (

    SELECT s.*
    FROM {{ ref('stg_shipments') }} s
    LEFT JOIN max_loaded m ON 1=1

    {% if is_incremental() %}
    WHERE m.max_file_date IS NULL 
       OR s.file_date > m.max_file_date
    {% endif %}

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id 
            ORDER BY file_date DESC
        ) AS row_num
    FROM stg_shipments

),

cleaned AS (

    SELECT
        *,
        TO_DATE(TO_TIMESTAMP_NTZ(expected_delivery_date / 1000000000)) AS expected_dt,
        TO_DATE(TO_TIMESTAMP_NTZ(actual_delivery_date / 1000000000)) AS actual_dt
    FROM deduplicated
    WHERE row_num = 1

),

transformed AS (

    SELECT
        UPPER(TRIM(shipment_id)) AS shipment_id,
        UPPER(TRIM(warehouse_id)) AS warehouse_id,
        UPPER(TRIM(store_id)) AS store_id,
        UPPER(TRIM(product_id)) AS product_id,

        CAST(quantity_shipped AS INTEGER) AS quantity_shipped,
        TRIM(carrier) AS carrier,

        shipment_date,
        expected_dt AS expected_delivery_date,
        actual_dt AS actual_delivery_date,

        DATEDIFF(day, expected_dt, actual_dt) AS delivery_delay_days,
        
        CASE 
            WHEN actual_dt IS NULL THEN 'IN_TRANSIT'
            WHEN actual_dt <= expected_dt THEN 'ON_TIME'
            ELSE 'LATE'
        END AS delivery_status,

        CURRENT_TIMESTAMP AS _transformed_at

    FROM cleaned

)

SELECT * FROM transformed