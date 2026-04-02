WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

joined AS (
    SELECT
        -- IDs --
        orders.invoice_id,
        orders.stock_code,
        orders.customer_id,
        orders.country,

        -- Product --
        products.product_description,

        -- Metricas --
        orders.quantity,
        orders.unit_price,
        orders.quantity * orders.unit_price AS line_revenue,

        -- Fechas --
        orders.invoice_at,

        -- Flags --
        orders.is_cancelled

    FROM orders
    INNER JOIN products
        ON orders.stock_code = products.stock_code
)

SELECT * FROM joined
