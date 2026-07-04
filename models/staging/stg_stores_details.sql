with source as (
    SELECT * FROM {{ source('raw', 'stores_details') }}
),

renamed as (
    SELECT
        "store_id" as store_id,
        "store_name" as store_name,
        "city" as city,
        "state" as state,
        "region" as region,
        "store_open_date" as store_open_date,
        'ingestion_timestamp',
        'current_timestamp' as _ingested_at,
        'raw.stores_details' as _source_file_path
    FROM source
)

SELECT * FROM renamed