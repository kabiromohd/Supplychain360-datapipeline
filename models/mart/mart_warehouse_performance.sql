WITH warehouses AS (
    SELECT * FROM {{ ref('int_warehouses') }}
),

inventories AS (
    SELECT * FROM {{ ref('int_inventory') }}
),

efficiency_agg AS (
    SELECT
        w.warehouse_id,
        w.city,
        w.state,
        inv.snapshot_date,
        COUNT(DISTINCT inv.product_id) AS unique_products_stored,
        SUM(inv.quantity_available) AS total_units_on_hand,
        AVG(inv.reorder_threshold) AS avg_reorder_point
    FROM warehouses w
    LEFT JOIN inventories inv ON w.warehouse_id = inv.warehouse_id
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM efficiency_agg