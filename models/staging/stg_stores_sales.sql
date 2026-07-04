with source as (
    SELECT * FROM {{ source('raw', 'stores_sales') }}
),

renamed as (
    SELECT
        "transaction_id" as transaction_id,
        "store_id" as store_id,
        "product_id" as product_id,
        "quantity_sold" as quantity_sold,
        "unit_price" as unit_price,
        "discount_pct" as discount_pct,
        "sale_amount" as sale_amount,
        "file_date" as file_date,
        "transaction_timestamp" as transaction_timestamp,
        'ingestion_timestamp',
        'current_timestamp' as _ingested_at,
        'raw.stores_sales' as _source_file_path
    FROM source
)

SELECT * FROM renamed