with source as (
    SELECT * FROM {{ source('raw', 'suppliers') }}
),

renamed as (
    SELECT
        "supplier_id" as supplier_id,
        "supplier_name" as supplier_name,
        "category" as category,
        "country" as country,
        'ingestion_timestamp',
        'current_timestamp' as _ingested_at,
        'raw.suppliers' as _source_file_path
    FROM source
)

SELECT * FROM renamed