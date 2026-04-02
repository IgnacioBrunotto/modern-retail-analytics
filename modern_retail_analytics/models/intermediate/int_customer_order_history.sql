WITH orders AS (
    SELECT * FROM {{ ref('int_orders_with_items') }}
),

aggregated AS (
    SELECT
        customer_id,

        -- Historial de compras (excluye cancelaciones) --
        MIN(CASE WHEN is_cancelled = FALSE THEN invoice_at END)     AS first_order_at,
        MAX(CASE WHEN is_cancelled = FALSE THEN invoice_at END)     AS last_order_at,
        COUNT(DISTINCT CASE WHEN is_cancelled = FALSE
            THEN invoice_id END)                                    AS total_orders,
        COUNT(CASE WHEN is_cancelled = FALSE THEN 1 END)            AS total_line_items,
        COUNT(DISTINCT CASE WHEN is_cancelled = FALSE
            THEN stock_code END)                                    AS total_unique_products,

        -- Revenue --
        SUM(CASE WHEN is_cancelled = FALSE
            THEN line_revenue ELSE 0 END)                           AS total_revenue_gross,
        SUM(line_revenue)                                           AS total_revenue_net,
        SUM(CASE WHEN is_cancelled = TRUE
            THEN line_revenue ELSE 0 END)                           AS total_revenue_cancelled,
        SUM(CASE WHEN is_cancelled = FALSE THEN line_revenue END)
            / NULLIF(COUNT(DISTINCT CASE WHEN is_cancelled = FALSE
            THEN invoice_id END), 0)                                AS avg_order_value,

        -- Engagement --
        DATE_DIFF(
            DATE(MAX(CASE WHEN is_cancelled = FALSE THEN invoice_at END)),
            DATE(MIN(CASE WHEN is_cancelled = FALSE THEN invoice_at END)),
            DAY
        )                                                           AS customer_lifespan_days,
        COUNT(DISTINCT CASE WHEN is_cancelled = FALSE
            THEN invoice_id END) > 1                                AS is_repeat_customer

    FROM orders
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
    HAVING COUNT(DISTINCT CASE WHEN is_cancelled = FALSE THEN invoice_id END) > 0
)

SELECT * FROM aggregated
