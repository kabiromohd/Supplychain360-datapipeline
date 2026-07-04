with source as (
    SELECT * FROM {{ source('raw', 'products') }}
),

renamed as (
    SELECT
        "product_id" as product_id,
        "product_name" as product_name,
        "category" as category,
        "brand" as brand,
        "supplier_id" as supplier_id,
        "unit_price" as unit_price,
        'ingestion_timestamp',
        'current_timestamp' as _ingested_at,
        'raw.products' as _source_file_path
    FROM source
)

SELECT * FROM renamed