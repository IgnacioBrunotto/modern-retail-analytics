WITH orders AS (
    SELECT * FROM {{ ref('int_orders_with_items') }}
),

dim_customers AS (
    SELECT customer_id, customer_id_key
    FROM {{ ref('dim_customers') }}
),

dim_products AS (
    SELECT stock_code, product_id
    FROM {{ ref('dim_products') }}
),

invoice_stats AS (
    SELECT
        invoice_id,
        customer_id,
        DENSE_RANK() OVER (
            PARTITION BY customer_id
            ORDER BY MIN(invoice_at)
        )                                                   AS customer_order_sequence,
        DATE_DIFF(
            DATE(MIN(invoice_at)),
            DATE(LAG(MIN(invoice_at)) OVER (
                PARTITION BY customer_id
                ORDER BY MIN(invoice_at)
            )),
            DAY
        )                                                   AS days_since_previous_order
    FROM orders
    WHERE is_cancelled = FALSE
        AND customer_id IS NOT NULL
    GROUP BY invoice_id, customer_id
),

final AS (
    SELECT
        -- Surrogate key --
        {{ dbt_utils.generate_surrogate_key(['orders.invoice_id', 'orders.stock_code', 'orders.quantity', 'orders.unit_price', 'orders.invoice_at']) }} AS order_line_id,

        -- Foreign keys --
        dim_customers.customer_id_key,
        dim_products.product_id,
        CAST(FORMAT_DATE('%Y%m%d', DATE(orders.invoice_at)) AS INT64)   AS date_id,

        -- Degenerate dimensions --
        orders.invoice_id,
        orders.customer_id,
        orders.stock_code,
        orders.country,

        -- Medidas --
        orders.quantity,
        orders.unit_price,
        orders.line_revenue,
        SUM(orders.line_revenue) OVER (
            PARTITION BY orders.invoice_id
        )                                                               AS invoice_total,

        -- Clasificación de línea --
        CASE
            WHEN orders.is_cancelled        THEN 'cancellation'
            WHEN orders.quantity < 0        THEN 'return'
            ELSE                                 'sale'
        END                                                             AS quantity_type,

        -- Comportamiento del cliente --
        invoice_stats.customer_order_sequence,
        invoice_stats.days_since_previous_order,

        -- Fechas --
        orders.invoice_at,

        -- Flags --
        orders.is_cancelled,
        orders.customer_id IS NULL                                      AS is_guest

    FROM orders
    LEFT JOIN dim_customers
        ON orders.customer_id = dim_customers.customer_id
    LEFT JOIN dim_products
        ON orders.stock_code = dim_products.stock_code
    LEFT JOIN invoice_stats
        ON orders.invoice_id = invoice_stats.invoice_id
)

SELECT * FROM final
