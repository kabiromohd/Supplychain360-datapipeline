with source as (
    SELECT * FROM {{ source('raw', 'inventory') }}
),

renamed as (
    SELECT
        "warehouse_id" as warehouse_id,
        "product_id" as product_id,
        "quantity_available" as quantity_available,
        "reorder_threshold" as reorder_threshold,
        "snapshot_date" as snapshot_date,
        'ingestion_timestamp',
        'current_timestamp' as _ingested_at,
        'raw.inventory' as _source_file_path
    FROM source
)

SELECT * FROM renamed