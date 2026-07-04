with source as (
    SELECT * FROM {{ source('raw', 'shipments') }}
),

renamed as (
    SELECT
        "shipment_id" as shipment_id,
        "warehouse_id" as warehouse_id,
        "store_id" as store_id,
        "product_id" as product_id,
        "quantity_shipped" as quantity_shipped,
        "shipment_date" as shipment_date,
        "expected_delivery_date" as expected_delivery_date,
        "actual_delivery_date" as actual_delivery_date,
        "carrier" as carrier,
        "file_date" as file_date,
        'ingestion_timestamp',
        'current_timestamp' as _ingested_at,
        'raw.shipments' as _source_file_path
    FROM source
)

SELECT * FROM renamed