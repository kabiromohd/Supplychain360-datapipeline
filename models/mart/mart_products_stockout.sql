WITH inventory AS (
    SELECT * FROM {{ ref('int_inventory') }}
),

products AS (
    SELECT * FROM {{ ref('int_products') }}
),

joined AS (
    SELECT
        i.inventory_sk,
        i.snapshot_date,
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        p.price_segment,
        i.warehouse_id,
        i.quantity_available,
        i.reorder_threshold,
        i.stock_status,
        
        CASE 
            WHEN i.stock_status = 'OUT_OF_STOCK' AND p.price_segment = 'HIGH_VALUE' THEN 'CRITICAL'
            WHEN i.stock_status = 'OUT_OF_STOCK' THEN 'URGENT'
            WHEN i.stock_status = 'LOW_STOCK' THEN 'WARNING'
            ELSE 'STABLE'
        END AS replenishment_priority
    FROM inventory i
    LEFT JOIN products p ON i.product_id = p.product_id
)

SELECT * FROM joined