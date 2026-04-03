WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
),

daily AS (
    SELECT
        date_id,
        DATE(invoice_at)                            AS date_day,
        country,

        -- Volumen --
        COUNT(DISTINCT invoice_id)                  AS total_orders,
        COUNT(DISTINCT customer_id)                 AS total_customers,
        COUNT(*)                                    AS total_line_items,

        -- Revenue --
        SUM(CASE WHEN quantity_type = 'sale'
            THEN line_revenue ELSE 0 END)           AS revenue_gross,
        SUM(CASE WHEN quantity_type = 'cancellation'
            THEN line_revenue ELSE 0 END)           AS revenue_cancelled,
        SUM(line_revenue)                           AS revenue_net,

        -- Mix --
        COUNTIF(quantity_type = 'sale')             AS sale_lines,
        COUNTIF(quantity_type = 'return')           AS return_lines,
        COUNTIF(quantity_type = 'cancellation')     AS cancellation_lines,
        COUNTIF(is_guest = TRUE)                    AS guest_lines

    FROM orders
    GROUP BY date_id, DATE(invoice_at), country
)

SELECT * FROM daily
