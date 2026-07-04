
WITH shipments AS (
    SELECT * FROM {{ ref('int_shipments') }}
),

products AS (
    SELECT * FROM {{ ref('int_products') }}
),

suppliers AS (
    SELECT * FROM {{ ref('int_suppliers') }}
),

performance_metrics AS (
    SELECT
        s.shipment_id,
        s.shipment_date,
        sup.supplier_id,
        sup.supplier_name,
        sup.category AS supplier_category,
        p.product_name,
        s.carrier,
        s.delivery_status,
        s.delivery_delay_days,
        
        CASE WHEN s.delivery_status = 'LATE' THEN 1 ELSE 0 END AS is_late_flag,
        CASE WHEN s.delivery_status = 'ON_TIME' THEN 1 ELSE 0 END AS is_on_time_flag
    FROM shipments s
    LEFT JOIN products p ON s.product_id = p.product_id
    LEFT JOIN suppliers sup ON p.supplier_id = sup.supplier_id
)

SELECT * FROM performance_metrics