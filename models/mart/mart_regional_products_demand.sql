
WITH sales AS (
    SELECT * FROM {{ ref('int_stores_sales') }}
),

stores AS (
    SELECT * FROM {{ ref('int_stores_details') }}
),

products AS (
    SELECT * FROM {{ ref('int_products') }}
),

regional_agg AS (
    SELECT
        sl.region,
        sl.state,
        sl.city,
        sl.store_name,
        s.transaction_date,
        p.category AS product_category,
        p.brand,
        SUM(s.quantity_sold) AS total_qty_sold,
        SUM(s.gross_sale_amount) AS total_gross_revenue,
        SUM(s.net_sale_amount) AS total_net_revenue
    FROM sales s
    LEFT JOIN stores sl ON s.store_id = sl.store_id
    LEFT JOIN products p ON s.product_id = p.product_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT * FROM regional_agg