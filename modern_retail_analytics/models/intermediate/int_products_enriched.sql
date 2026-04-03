WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE stock_code_type = 'product'
        AND is_cancelled = FALSE
        AND quantity > 0
),

sales_history AS (
    SELECT
        stock_code,
        MIN(invoice_at)                             AS first_sold_at,
        MAX(invoice_at)                             AS last_sold_at,
        SUM(quantity)                               AS total_units_sold,
        SUM(quantity * unit_price)                  AS total_revenue,
        COUNT(DISTINCT invoice_id)                  AS total_orders,
        SAFE_DIVIDE(SUM(quantity), COUNT(DISTINCT invoice_id)) AS avg_quantity_per_order
    FROM orders
    GROUP BY stock_code
),

dataset_max_date AS (
    SELECT DATE(MAX(invoice_at)) AS max_date
    FROM orders
),

enriched AS (
    SELECT
        p.stock_code,
        p.product_description,
        p.unit_price,
        p.stock_code_type,

        -- Historial de ventas --
        s.first_sold_at,
        s.last_sold_at,
        s.total_units_sold,
        s.total_revenue,
        s.total_orders,
        s.avg_quantity_per_order,
        DATE_DIFF(d.max_date, DATE(s.last_sold_at), DAY)       AS days_since_last_sale,
        COALESCE(
            DATE_DIFF(d.max_date, DATE(s.last_sold_at), DAY) <= 90,
            FALSE
        )                                                           AS is_active

    FROM products AS p
    LEFT JOIN sales_history AS s USING (stock_code)
    CROSS JOIN dataset_max_date AS d
)

SELECT * FROM enriched
